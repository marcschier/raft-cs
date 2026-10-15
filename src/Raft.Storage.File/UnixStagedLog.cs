// Copyright (c) marcschier. Licensed under the MIT License.

using System.Runtime.InteropServices;
#if NET8_0_OR_GREATER
using System.Runtime.Versioning;
#endif
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace Raft.Storage.File;

/// <summary>Calls Linux statx for an open descriptor.</summary>
/// <param name="descriptor">The descriptor.</param>
/// <param name="buffer">The statx result.</param>
/// <returns>0 on success; otherwise the errno of the failure.</returns>
internal delegate int StatxCall(int descriptor, Span<byte> buffer);

/// <summary>Gives the staged log the log's access control list, or confirms that neither file has one.</summary>
/// <param name="log">The log's descriptor.</param>
/// <param name="staged">The staged log's descriptor.</param>
/// <returns>Whether the staged log's access control list is now the log's.</returns>
internal delegate bool AccessControlCopy(int log, int staged);

/// <summary>The owner, group, and mode of an open file.</summary>
internal readonly record struct UnixFileStatus(uint Uid, uint Gid, uint Mode);

/// <summary>
/// Creates the compacted log on Unix and gives it the log's access exactly, or reports that it cannot.
/// </summary>
/// <remarks>
/// <para>
/// The compacted log is created exclusively and owner-only, and the returned stream writes through the descriptor that
/// created it, so no file or symbolic link placed at the path is opened and no other user can open the new file.
/// Before any payload is written, <see cref="TryApplyLogAccess"/> reproduces the log's access on it: the owner must
/// already be the log's (it is never changed), the group is set to the log's, the access control list is copied, and
/// the mode is set; then the result is read back and compared. Each step leaves the file no more accessible than the
/// log. When any of them cannot be done exactly the log is not rewritten.
/// </para>
/// <para>
/// Linux copies the POSIX access control list; macOS rewrites only a log that has no extended ACL, and a staged log
/// that inherited none; other Unix systems, whose ACLs are not inspected, never rewrite the log. The owner, group, and
/// mode are read with statx on Linux. Where libc lacks statx (glibc before 2.28, musl before 1.2.5) or the kernel
/// refuses it (ENOSYS, or EPERM from a seccomp filter), and on other Unix systems, they are read with the runtime's
/// own fstat, whose result the runtime lays out identically for every libc and architecture.
/// </para>
/// </remarks>
#if NET8_0_OR_GREATER
[UnsupportedOSPlatform("windows")]
#endif
internal static unsafe class UnixStagedLog
{
    private const int OwnerReadWrite = 0x180;
    private const uint FileModeBits = 0xFFF;
    private const string AccessAclName = "system.posix_acl_access";

    private const int LinuxWriteOnly = 0x1;
    private const int LinuxCreate = 0x40;
    private const int LinuxExclusive = 0x80;
    private const int LinuxCloseOnExec = 0x80000;
    private const int LinuxEmptyPath = 0x1000;
    private const uint LinuxStatxRequired = 0x2 | 0x8 | 0x10;
    private const int LinuxStatxSize = 256;
    private const int LinuxStatxUidOffset = 20;
    private const int LinuxStatxGidOffset = 24;
    private const int LinuxStatxModeOffset = 28;
    private const int LinuxNotPermitted = 1;
    private const int LinuxRange = 34;
    private const int LinuxNotImplemented = 38;
    private const int LinuxNoData = 61;
    private const int LinuxNotSupported = 95;

    private const int MacNoEntry = 2;
    private const int MacInvalid = 22;
    private const int MacAclTypeExtended = 0x100;
    private const int MacAclFirstEntry = 0;

    // The runtime's FileStatus starts with int Flags, int Mode, uint Uid, uint Gid; the buffer leaves room for the
    // rest.
    private const int RuntimeStatusSize = 256;
    private const int RuntimeStatusModeOffset = 4;
    private const int RuntimeStatusUidOffset = 8;
    private const int RuntimeStatusGidOffset = 12;

    private static readonly byte[] AccessAclNameBytes = NullTerminated(AccessAclName);

    /// <summary>Creates an empty owner-only compacted log at <paramref name="path"/>, which must not exist.</summary>
    /// <param name="path">The path of the compacted log.</param>
    /// <returns>A stream that writes the compacted log.</returns>
    public static FileStream Create(string path)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return CreateOnLinux(path);
        }

#if NET8_0_OR_GREATER
        // CreateNew opens with O_CREAT | O_EXCL, which neither follows nor reuses an existing link or file.
        return new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.Write,
                Share = FileShare.None,
                UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite,
            });
#else
        return CreateWithTemporaryName(path);
#endif
    }

    /// <summary>
    /// Gives the empty compacted log exactly the log's owner, group, access control list, and mode.
    /// </summary>
    /// <param name="log">The open log.</param>
    /// <param name="staged">The open, empty compacted log.</param>
    /// <param name="statx">The statx call on Linux; <see langword="null"/> calls libc.</param>
    /// <param name="accessControl">The access control list step; <see langword="null"/> uses the platform's.</param>
    /// <returns>Whether the compacted log now has exactly the log's access.</returns>
    public static bool TryApplyLogAccess(
        SafeFileHandle log,
        SafeFileHandle staged,
        StatxCall? statx,
        AccessControlCopy? accessControl)
    {
        bool logAdded = false;
        bool stagedAdded = false;
        try
        {
            log.DangerousAddRef(ref logAdded);
            staged.DangerousAddRef(ref stagedAdded);
            return TryApplyDescriptorAccess(
                (int)log.DangerousGetHandle(),
                (int)staged.DangerousGetHandle(),
                statx ?? NativeStatx,
                accessControl ?? PlatformAccessControl());
        }
        finally
        {
            if (stagedAdded)
            {
                staged.DangerousRelease();
            }

            if (logAdded)
            {
                log.DangerousRelease();
            }
        }
    }

    /// <summary>Reads the owner, group, and mode of an open file.</summary>
    /// <param name="file">The open file.</param>
    /// <param name="path">The path of the file, for error messages.</param>
    /// <param name="statx">The statx call on Linux; <see langword="null"/> calls libc.</param>
    /// <returns>The owner, group, and mode.</returns>
    public static UnixFileStatus ReadStatus(SafeFileHandle file, string path, StatxCall? statx)
    {
        bool added = false;
        try
        {
            file.DangerousAddRef(ref added);
            if (!TryReadStatus((int)file.DangerousGetHandle(), statx ?? NativeStatx, out UnixFileStatus status))
            {
                throw new IOException($"The owner, group, and mode of '{path}' could not be read.");
            }

            return status;
        }
        finally
        {
            if (added)
            {
                file.DangerousRelease();
            }
        }
    }

    /// <summary>
    /// Creates the access control list step of a platform whose extended ACLs are not copied: it succeeds only when
    /// neither file has one.
    /// </summary>
    /// <param name="hasExtendedAcl">
    /// Reports whether a descriptor's file has an extended ACL; <see langword="null"/> when that cannot be told.
    /// </param>
    /// <returns>The access control list step.</returns>
    public static AccessControlCopy RequireNoExtendedAcl(Func<int, bool?> hasExtendedAcl)
    {
        return (log, staged) => hasExtendedAcl(log) == false && hasExtendedAcl(staged) == false;
    }

    private static bool TryApplyDescriptorAccess(
        int log,
        int staged,
        StatxCall statx,
        AccessControlCopy accessControl)
    {
        if (!TryReadStatus(log, statx, out UnixFileStatus logStatus)
            || !TryReadStatus(staged, statx, out UnixFileStatus stagedStatus)
            || stagedStatus.Uid != logStatus.Uid)
        {
            return false;
        }

        // The group changes while the file is owner-only and before the access control list, whose owning-group entry
        // then applies to the log's group.
        if (stagedStatus.Gid != logStatus.Gid && ChangeOwner(staged, uint.MaxValue, logStatus.Gid) != 0)
        {
            return false;
        }

        uint mode = logStatus.Mode & FileModeBits;
        if (!accessControl(log, staged) || ChangeMode(staged, mode) != 0)
        {
            return false;
        }

        return TryReadStatus(staged, statx, out UnixFileStatus applied)
            && applied.Uid == logStatus.Uid
            && applied.Gid == logStatus.Gid
            && (applied.Mode & FileModeBits) == mode
            && (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux) || HasSamePosixAcl(log, staged));
    }

    private static AccessControlCopy PlatformAccessControl()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return CopyPosixAcl;
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return RequireNoExtendedAcl(MacHasExtendedAcl);
        }

        return static (_, _) => false;
    }

    private static bool CopyPosixAcl(int log, int staged)
    {
        if (!TryGetPosixAcl(log, out byte[]? acl))
        {
            return false;
        }

        fixed (byte* name = AccessAclNameBytes)
        {
            if (acl is null)
            {
                return LinuxRemoveAttribute(staged, name) == 0 || IsWithoutAcl(Marshal.GetLastWin32Error());
            }

            fixed (byte* value = acl)
            {
                return LinuxSetAttribute(staged, name, value, (nuint)acl.Length, 0) == 0;
            }
        }
    }

    private static bool HasSamePosixAcl(int log, int staged)
    {
        return TryGetPosixAcl(log, out byte[]? logAcl)
            && TryGetPosixAcl(staged, out byte[]? stagedAcl)
            && (logAcl is null
                ? stagedAcl is null
                : stagedAcl is not null && logAcl.AsSpan().SequenceEqual(stagedAcl));
    }

    private static bool TryGetPosixAcl(int descriptor, out byte[]? acl)
    {
        acl = null;
        fixed (byte* name = AccessAclNameBytes)
        {
            while (true)
            {
                nint size = LinuxGetAttribute(descriptor, name, null, 0);
                if (size < 0)
                {
                    return IsWithoutAcl(Marshal.GetLastWin32Error());
                }

                byte[] value = new byte[(int)size];
                fixed (byte* valueBytes = value)
                {
                    nint read = LinuxGetAttribute(descriptor, name, valueBytes, (nuint)value.Length);
                    if (read >= 0)
                    {
                        acl = value.AsSpan(0, (int)read).ToArray();
                        return true;
                    }
                }

                int error = Marshal.GetLastWin32Error();
                if (error != LinuxRange)
                {
                    return IsWithoutAcl(error);
                }
            }
        }
    }

    private static bool IsWithoutAcl(int error)
    {
        return error is LinuxNoData or LinuxNotSupported;
    }

    private static bool? MacHasExtendedAcl(int descriptor)
    {
        nint acl = MacGetAcl(descriptor, MacAclTypeExtended);
        if (acl == 0)
        {
            return Marshal.GetLastWin32Error() == MacNoEntry ? false : null;
        }

        try
        {
            nint entry;
            if (MacGetAclEntry(acl, MacAclFirstEntry, &entry) == 0)
            {
                return true;
            }

            return Marshal.GetLastWin32Error() == MacInvalid ? false : null;
        }
        finally
        {
            _ = MacFreeAcl(acl);
        }
    }

    private static bool TryReadStatus(int descriptor, StatxCall statx, out UnixFileStatus status)
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return TryReadRuntimeStatus(descriptor, out status);
        }

        status = default;
        Span<byte> buffer = stackalloc byte[LinuxStatxSize];
        int error;
        try
        {
            error = statx(descriptor, buffer);
        }
        catch (EntryPointNotFoundException)
        {
            // libc predates statx (glibc 2.28, musl 1.2.5).
            return TryReadRuntimeStatus(descriptor, out status);
        }

        if (error is LinuxNotImplemented or LinuxNotPermitted)
        {
            // The kernel predates statx, or a seccomp filter refuses it; reading an open descriptor needs no access.
            return TryReadRuntimeStatus(descriptor, out status);
        }

        if (error != 0 || (MemoryMarshal.Read<uint>(buffer) & LinuxStatxRequired) != LinuxStatxRequired)
        {
            return false;
        }

        status = new UnixFileStatus(
            MemoryMarshal.Read<uint>(buffer.Slice(LinuxStatxUidOffset)),
            MemoryMarshal.Read<uint>(buffer.Slice(LinuxStatxGidOffset)),
            MemoryMarshal.Read<ushort>(buffer.Slice(LinuxStatxModeOffset)));
        return true;
    }

    private static int NativeStatx(int descriptor, Span<byte> buffer)
    {
        byte emptyPath = 0;
        fixed (byte* result = buffer)
        {
            return LinuxStatx(descriptor, &emptyPath, LinuxEmptyPath, LinuxStatxRequired, result) == 0
                ? 0
                : Marshal.GetLastWin32Error();
        }
    }

    private static bool TryReadRuntimeStatus(int descriptor, out UnixFileStatus status)
    {
        byte* buffer = stackalloc byte[RuntimeStatusSize];
        int result;
        try
        {
            result = RuntimeFileStatus(descriptor, buffer);
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException)
        {
            // Without the runtime's fstat the access cannot be compared, so the caller keeps the log and compacts
            // logically instead of rewriting it.
            status = default;
            return false;
        }

        if (result != 0)
        {
            status = default;
            return false;
        }

        status = new UnixFileStatus(
            *(uint*)(buffer + RuntimeStatusUidOffset),
            *(uint*)(buffer + RuntimeStatusGidOffset),
            *(uint*)(buffer + RuntimeStatusModeOffset));
        return true;
    }

    private static int ChangeOwner(int descriptor, uint owner, uint group)
    {
        return RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
            ? MacChangeOwner(descriptor, owner, group)
            : UnixChangeOwner(descriptor, owner, group);
    }

    private static int ChangeMode(int descriptor, uint mode)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return LinuxChangeMode(descriptor, mode);
        }

        // mode_t is 16 bits on macOS and the BSDs.
        return RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
            ? MacChangeMode(descriptor, (ushort)mode)
            : BsdChangeMode(descriptor, (ushort)mode);
    }

    private static FileStream CreateOnLinux(string path)
    {
        int flags = LinuxWriteOnly | LinuxCreate | LinuxExclusive | LinuxCloseOnExec | LinuxNoFollow();
        int descriptor;
        fixed (byte* pathBytes = NullTerminated(path))
        {
            descriptor = LinuxOpen(pathBytes, flags, OwnerReadWrite);
        }

        if (descriptor < 0)
        {
            throw CreateIOException("create", path, Marshal.GetLastWin32Error());
        }

        return StreamFor(descriptor);
    }

    private static int LinuxNoFollow()
    {
        // O_EXCL already refuses an existing link; O_NOFOLLOW states it too. Its value is architecture-specific.
        Architecture architecture = RuntimeInformation.ProcessArchitecture;
#if NET8_0_OR_GREATER
        if (architecture is Architecture.Armv6 or Architecture.Ppc64le)
        {
            return 0x8000;
        }
#endif
        return architecture is Architecture.Arm or Architecture.Arm64 ? 0x8000 : 0x20000;
    }

#if !NET8_0_OR_GREATER
    private static FileStream CreateWithTemporaryName(string path)
    {
        // mkstemp creates the file exclusively and owner-only without the variadic open(2), which cannot be called
        // portably; the new file then takes the staging name without replacing anything placed there.
        byte[] template = NullTerminated(path + ".XXXXXX");
        int descriptor;
        fixed (byte* templateBytes = template)
        {
            descriptor = RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
                ? MacMakeTemporary(templateBytes)
                : UnixMakeTemporary(templateBytes);
        }

        if (descriptor < 0)
        {
            throw CreateIOException("create", path, Marshal.GetLastWin32Error());
        }

        FileStream stream = StreamFor(descriptor);
        string temporaryPath = Encoding.UTF8.GetString(template, 0, template.Length - 1);
        try
        {
            System.IO.File.Move(temporaryPath, path);
        }
        catch
        {
            stream.Dispose();
            System.IO.File.Delete(temporaryPath);
            throw;
        }

        return stream;
    }
#endif

    private static FileStream StreamFor(int descriptor)
    {
        var handle = new SafeFileHandle((IntPtr)descriptor, ownsHandle: true);
        try
        {
            return new FileStream(handle, FileAccess.Write, bufferSize: 4096);
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    private static byte[] NullTerminated(string value)
    {
        byte[] bytes = new byte[Encoding.UTF8.GetByteCount(value) + 1];
        Encoding.UTF8.GetBytes(value, 0, value.Length, bytes, 0);
        return bytes;
    }

    private static IOException CreateIOException(string operation, string path, int error)
    {
        return new IOException($"Unable to {operation} compacted log '{path}' (errno {error}).");
    }

    [DllImport("libc", EntryPoint = "open", SetLastError = true)]
    private static extern int LinuxOpen(byte* path, int flags, int mode);

    [DllImport("libc", EntryPoint = "statx", SetLastError = true)]
    private static extern int LinuxStatx(int descriptor, byte* path, int flags, uint mask, byte* buffer);

    [DllImport("libc", EntryPoint = "fchmod", SetLastError = true)]
    private static extern int LinuxChangeMode(int descriptor, uint mode);

    [DllImport("libc", EntryPoint = "fgetxattr", SetLastError = true)]
    private static extern nint LinuxGetAttribute(int descriptor, byte* name, byte* value, nuint size);

    [DllImport("libc", EntryPoint = "fsetxattr", SetLastError = true)]
    private static extern int LinuxSetAttribute(int descriptor, byte* name, byte* value, nuint size, int flags);

    [DllImport("libc", EntryPoint = "fremovexattr", SetLastError = true)]
    private static extern int LinuxRemoveAttribute(int descriptor, byte* name);

    [DllImport("libc", EntryPoint = "fchown", SetLastError = true)]
    private static extern int UnixChangeOwner(int descriptor, uint owner, uint group);

    [DllImport("libc", EntryPoint = "fchmod", SetLastError = true)]
    private static extern int BsdChangeMode(int descriptor, ushort mode);

    [DllImport("libSystem.B.dylib", EntryPoint = "fchown", SetLastError = true)]
    private static extern int MacChangeOwner(int descriptor, uint owner, uint group);

    [DllImport("libSystem.B.dylib", EntryPoint = "fchmod", SetLastError = true)]
    private static extern int MacChangeMode(int descriptor, ushort mode);

    [DllImport("libSystem.B.dylib", EntryPoint = "acl_get_fd_np", SetLastError = true)]
    private static extern nint MacGetAcl(int descriptor, int type);

    [DllImport("libSystem.B.dylib", EntryPoint = "acl_get_entry", SetLastError = true)]
    private static extern int MacGetAclEntry(nint acl, int entryId, nint* entry);

    [DllImport("libSystem.B.dylib", EntryPoint = "acl_free", SetLastError = true)]
    private static extern int MacFreeAcl(nint acl);

    // The runtime's fstat (what File.GetUnixFileMode calls), built for the process's libc and architecture. The
    // canonical name lets NativeAOT bind it to the statically linked runtime library.
    [DllImport("System.Native", EntryPoint = "SystemNative_FStat", SetLastError = true)]
    private static extern int RuntimeFileStatus(nint descriptor, byte* status);

#if !NET8_0_OR_GREATER
    [DllImport("libc", EntryPoint = "mkstemp", SetLastError = true)]
    private static extern int UnixMakeTemporary(byte* template);

    [DllImport("libSystem.B.dylib", EntryPoint = "mkstemp", SetLastError = true)]
    private static extern int MacMakeTemporary(byte* template);
#endif
}
