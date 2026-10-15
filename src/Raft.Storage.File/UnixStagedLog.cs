// Copyright (c) marcschier. Licensed under the MIT License.

using System.Buffers.Binary;
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

/// <summary>The owner, group, and mode of an open file.</summary>
internal readonly record struct UnixFileStatus(uint Uid, uint Gid, uint Mode);

/// <summary>
/// Creates the compacted log on Unix and gives it the log's access controls before it replaces the log.
/// </summary>
/// <remarks>
/// <para>
/// The compacted log is created exclusively and owner-only, and the returned stream writes through the descriptor that
/// created it, so no file or symbolic link placed at the path is opened and no other user can open the new file. The
/// retained payloads are written while the file is owner-only; <see cref="ApplyLogAccess"/> then applies the log's
/// owner and group, access control list, and permission bits, in that order, so no step grants anyone access the log
/// does not grant. When the process may not assign the log's group, the compacted log withholds the group's access,
/// and the other class, which the log's group members then fall into, keeps only what that group was granted. The
/// owner the process may not assign stays the process, which already reads and writes the log.
/// </para>
/// <para>
/// Linux copies the POSIX access control list too; other Unix systems copy the owner, group, and permission bits but
/// not extended ACLs. The owner, group, and mode are read with statx on Linux. Where libc lacks statx (glibc before
/// 2.28, musl before 1.2.5) or the kernel refuses it (ENOSYS, or EPERM from a seccomp filter), and on other Unix
/// systems, they are read with the runtime's own fstat, whose result the runtime lays out identically for every libc
/// and architecture, rather than with a hand-laid struct stat.
/// </para>
/// </remarks>
#if NET8_0_OR_GREATER
[UnsupportedOSPlatform("windows")]
#endif
internal static unsafe class UnixStagedLog
{
    private const int OwnerReadWrite = 0x180;
    private const uint PermissionBits = 0x1FF;
    private const uint GroupAndOtherBits = 0x3F;
    private const uint GroupBits = 0x38;
    private const uint OtherBits = 0x7;
    private const int NotPermitted = 1;
    private const string AccessAclName = "system.posix_acl_access";
    private const int AclHeaderSize = 4;
    private const int AclEntrySize = 8;
    private const int AclVersion = 2;
    private const ushort AclGroupObject = 0x04;
    private const ushort AclMask = 0x10;
    private const ushort AclOther = 0x20;

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
    private const int LinuxRange = 34;
    private const int LinuxNotImplemented = 38;
    private const int LinuxNoData = 61;
    private const int LinuxNotSupported = 95;

    // The runtime's FileStatus starts with int Flags, int Mode, uint Uid, uint Gid; the buffer leaves room for the
    // rest.
    private const int RuntimeStatusSize = 256;
    private const int RuntimeStatusModeOffset = 4;
    private const int RuntimeStatusUidOffset = 8;
    private const int RuntimeStatusGidOffset = 12;

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
    /// Gives the compacted log the log's owner and group, access control list, and permission bits.
    /// </summary>
    /// <param name="log">The open log.</param>
    /// <param name="staged">The open compacted log.</param>
    /// <param name="stagedPath">The path of the compacted log, for error messages.</param>
    /// <param name="statx">The statx call on Linux; <see langword="null"/> calls libc.</param>
    public static void ApplyLogAccess(SafeFileHandle log, SafeFileHandle staged, string stagedPath, StatxCall? statx)
    {
        bool logAdded = false;
        bool stagedAdded = false;
        try
        {
            log.DangerousAddRef(ref logAdded);
            staged.DangerousAddRef(ref stagedAdded);
            ApplyDescriptorAccess(
                (int)log.DangerousGetHandle(),
                (int)staged.DangerousGetHandle(),
                stagedPath,
                statx ?? NativeStatx);
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
            return ReadDescriptorStatus((int)file.DangerousGetHandle(), statx ?? NativeStatx, path);
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
    /// Withholds the access of the log's group from a compacted log that cannot have that group: the owning group's
    /// access is removed, and the other class, which the log's group members fall into, keeps only what the log's
    /// group was granted.
    /// </summary>
    /// <param name="acl">
    /// The log's access control list, updated in place; <see langword="null"/> when it has none.
    /// </param>
    /// <param name="mode">The log's permission bits.</param>
    /// <param name="stagedPath">The path of the compacted log, for error messages.</param>
    /// <returns>The permission bits for the compacted log.</returns>
    public static uint WithholdLogGroupAccess(byte[]? acl, uint mode, string stagedPath)
    {
        if (acl is null)
        {
            uint groupAccess = (mode & GroupBits) >> 3;
            return (mode & ~GroupAndOtherBits) | (mode & OtherBits & groupAccess);
        }

        if (acl.Length < AclHeaderSize
            || (acl.Length - AclHeaderSize) % AclEntrySize != 0
            || BinaryPrimitives.ReadInt32LittleEndian(acl) != AclVersion)
        {
            throw InvalidAcl(stagedPath);
        }

        int groupOffset = -1;
        int otherOffset = -1;
        bool hasMask = false;
        for (int offset = AclHeaderSize; offset < acl.Length; offset += AclEntrySize)
        {
            switch (BinaryPrimitives.ReadUInt16LittleEndian(acl.AsSpan(offset)))
            {
                case AclGroupObject:
                    groupOffset = offset + 2;
                    break;
                case AclOther:
                    otherOffset = offset + 2;
                    break;
                case AclMask:
                    hasMask = true;
                    break;
            }
        }

        if (groupOffset < 0 || otherOffset < 0)
        {
            throw InvalidAcl(stagedPath);
        }

        ushort groupPermissions = BinaryPrimitives.ReadUInt16LittleEndian(acl.AsSpan(groupOffset));
        ushort otherPermissions = (ushort)(BinaryPrimitives.ReadUInt16LittleEndian(acl.AsSpan(otherOffset))
            & groupPermissions);
        BinaryPrimitives.WriteUInt16LittleEndian(acl.AsSpan(groupOffset), 0);
        BinaryPrimitives.WriteUInt16LittleEndian(acl.AsSpan(otherOffset), otherPermissions);

        // With a mask the group bits are the mask, which still bounds the named entries.
        uint groupBits = hasMask ? mode & GroupBits : 0;
        return (mode & ~GroupAndOtherBits) | groupBits | otherPermissions;
    }

    private static void ApplyDescriptorAccess(int log, int staged, string stagedPath, StatxCall statx)
    {
        UnixFileStatus logStatus = ReadDescriptorStatus(log, statx, stagedPath);
        UnixFileStatus stagedStatus = ReadDescriptorStatus(staged, statx, stagedPath);
        bool isLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);

        // The compacted log is owner-only until the access control list and the permission bits are applied, so the
        // owner and group change before they do.
        bool hasLogGroup = TryAssignOwnership(staged, logStatus, stagedStatus, stagedPath);
        byte[]? acl = isLinux ? GetAccessAcl(log, stagedPath) : null;
        uint mode = logStatus.Mode & PermissionBits;
        if (!hasLogGroup)
        {
            mode = WithholdLogGroupAccess(acl, mode, stagedPath);
        }

        if (isLinux)
        {
            if (acl is null)
            {
                RemoveAccessAcl(staged, stagedPath);
            }
            else
            {
                SetAccessAcl(staged, acl, stagedPath);
            }
        }

        // With an access control list the group bits are its mask, which the list just set to the same value.
        if (ChangeMode(staged, mode) != 0)
        {
            throw CreateIOException("set the permissions of", stagedPath, Marshal.GetLastWin32Error());
        }
    }

    private static bool TryAssignOwnership(
        int staged,
        UnixFileStatus log,
        UnixFileStatus stagedStatus,
        string stagedPath)
    {
        if (stagedStatus.Uid == log.Uid && stagedStatus.Gid == log.Gid)
        {
            return true;
        }

        if (ChangeOwner(staged, log.Uid, log.Gid) == 0)
        {
            return true;
        }

        int error = Marshal.GetLastWin32Error();
        if (error != NotPermitted)
        {
            throw CreateIOException("set the owner of", stagedPath, error);
        }

        // The process cannot give the file away; it keeps the owner, which already reads and writes the log.
        if (stagedStatus.Gid == log.Gid)
        {
            return true;
        }

        if (ChangeOwner(staged, uint.MaxValue, log.Gid) == 0)
        {
            return true;
        }

        error = Marshal.GetLastWin32Error();
        if (error != NotPermitted)
        {
            throw CreateIOException("set the group of", stagedPath, error);
        }

        return false;
    }

    private static UnixFileStatus ReadDescriptorStatus(int descriptor, StatxCall statx, string stagedPath)
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return ReadRuntimeStatus(descriptor, stagedPath);
        }

        Span<byte> buffer = stackalloc byte[LinuxStatxSize];
        int error;
        try
        {
            error = statx(descriptor, buffer);
        }
        catch (EntryPointNotFoundException)
        {
            // libc predates statx (glibc 2.28, musl 1.2.5).
            return ReadRuntimeStatus(descriptor, stagedPath);
        }

        if (error is LinuxNotImplemented or NotPermitted)
        {
            // The kernel predates statx, or a seccomp filter refuses it; reading an open descriptor needs no access.
            return ReadRuntimeStatus(descriptor, stagedPath);
        }

        if (error != 0)
        {
            throw CreateIOException("read the access of the log for", stagedPath, error);
        }

        if ((MemoryMarshal.Read<uint>(buffer) & LinuxStatxRequired) != LinuxStatxRequired)
        {
            throw new IOException($"The log's owner and permissions could not be read for '{stagedPath}'.");
        }

        return new UnixFileStatus(
            MemoryMarshal.Read<uint>(buffer.Slice(LinuxStatxUidOffset)),
            MemoryMarshal.Read<uint>(buffer.Slice(LinuxStatxGidOffset)),
            MemoryMarshal.Read<ushort>(buffer.Slice(LinuxStatxModeOffset)));
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

    private static UnixFileStatus ReadRuntimeStatus(int descriptor, string stagedPath)
    {
        byte* status = stackalloc byte[RuntimeStatusSize];
        if (RuntimeFileStatus(descriptor, status) != 0)
        {
            throw CreateIOException("read the access of the log for", stagedPath, Marshal.GetLastWin32Error());
        }

        return new UnixFileStatus(
            *(uint*)(status + RuntimeStatusUidOffset),
            *(uint*)(status + RuntimeStatusGidOffset),
            *(uint*)(status + RuntimeStatusModeOffset));
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

    private static byte[]? GetAccessAcl(int descriptor, string stagedPath)
    {
        fixed (byte* name = NullTerminated(AccessAclName))
        {
            while (true)
            {
                nint size = LinuxGetAttribute(descriptor, name, null, 0);
                if (size < 0)
                {
                    return NoAclOrThrow(Marshal.GetLastWin32Error(), stagedPath);
                }

                byte[] value = new byte[(int)size];
                fixed (byte* valueBytes = value)
                {
                    nint read = LinuxGetAttribute(descriptor, name, valueBytes, (nuint)value.Length);
                    if (read >= 0)
                    {
                        return value.AsSpan(0, (int)read).ToArray();
                    }
                }

                int error = Marshal.GetLastWin32Error();
                if (error != LinuxRange)
                {
                    return NoAclOrThrow(error, stagedPath);
                }
            }
        }
    }

    private static byte[]? NoAclOrThrow(int error, string stagedPath)
    {
        if (error is LinuxNoData or LinuxNotSupported)
        {
            return null;
        }

        throw CreateIOException("read the access control list of the log for", stagedPath, error);
    }

    private static void SetAccessAcl(int descriptor, byte[] acl, string stagedPath)
    {
        fixed (byte* name = NullTerminated(AccessAclName))
        fixed (byte* value = acl)
        {
            if (LinuxSetAttribute(descriptor, name, value, (nuint)acl.Length, 0) != 0)
            {
                throw CreateIOException("set the access control list of", stagedPath, Marshal.GetLastWin32Error());
            }
        }
    }

    private static void RemoveAccessAcl(int descriptor, string stagedPath)
    {
        fixed (byte* name = NullTerminated(AccessAclName))
        {
            if (LinuxRemoveAttribute(descriptor, name) == 0)
            {
                return;
            }
        }

        int error = Marshal.GetLastWin32Error();
        if (error is not (LinuxNoData or LinuxNotSupported))
        {
            throw CreateIOException("remove the access control list of", stagedPath, error);
        }
    }

    private static IOException InvalidAcl(string stagedPath)
    {
        return new IOException($"The log's access control list could not be applied to '{stagedPath}'.");
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

    // The runtime's fstat (what File.GetUnixFileMode calls), built for the process's libc and architecture.
    [DllImport("libSystem.Native", EntryPoint = "SystemNative_FStat", SetLastError = true)]
    private static extern int RuntimeFileStatus(nint descriptor, byte* status);

#if !NET8_0_OR_GREATER
    [DllImport("libc", EntryPoint = "mkstemp", SetLastError = true)]
    private static extern int UnixMakeTemporary(byte* template);

    [DllImport("libSystem.B.dylib", EntryPoint = "mkstemp", SetLastError = true)]
    private static extern int MacMakeTemporary(byte* template);
#endif
}
