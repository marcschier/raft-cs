// Copyright (c) marcschier. Licensed under the MIT License.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
#if NET8_0_OR_GREATER
using System.Runtime.Versioning;
#endif
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace Raft.Storage.File;

/// <summary>
/// Creates the compacted log on Unix and gives it the log's access controls before it replaces the log.
/// </summary>
/// <remarks>
/// The compacted log is created exclusively and owner-only, and the returned stream writes through the descriptor that
/// created it, so no file or symbolic link placed at the path is opened and no other user can open the new file. The
/// retained payloads are written while the file is owner-only; <see cref="ApplyLogAccess"/> then applies the log's
/// ownership, access control list, and permission bits, in that order, so no step grants anyone access the log does
/// not grant. On Linux all of them are copied (the log's owner is read with statx, glibc 2.28 or musl 1.2.5 and
/// later). On other Unix systems only the permission bits are copied (owner-only on netstandard builds, which cannot
/// read them), and extended ACLs and ownership are not.
/// </remarks>
#if NET8_0_OR_GREATER
[UnsupportedOSPlatform("windows")]
#endif
internal static unsafe class UnixStagedLog
{
    private const int OwnerReadWrite = 0x180;
    private const uint PermissionBits = 0x1FF;
    private const uint GroupPermissionBits = 0x38;
    private const string AccessAclName = "system.posix_acl_access";
    private const int AclHeaderSize = 4;
    private const int AclEntrySize = 8;
    private const int AclVersion = 2;
    private const ushort AclGroupObject = 0x04;
    private const ushort AclMask = 0x10;

    private const int LinuxWriteOnly = 0x1;
    private const int LinuxCreate = 0x40;
    private const int LinuxExclusive = 0x80;
    private const int LinuxCloseOnExec = 0x80000;
    private const int LinuxEmptyPath = 0x1000;
    private const uint LinuxStatMode = 0x2;
    private const uint LinuxStatUid = 0x8;
    private const uint LinuxStatGid = 0x10;
    private const int LinuxStatSize = 256;
    private const int LinuxStatUidOffset = 20;
    private const int LinuxStatGidOffset = 24;
    private const int LinuxStatModeOffset = 28;
    private const int LinuxNotPermitted = 1;
    private const int LinuxRange = 34;
    private const int LinuxNoData = 61;
    private const int LinuxNotSupported = 95;

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
    /// Gives the compacted log the log's ownership, access control list, and permission bits.
    /// </summary>
    /// <param name="log">The open log.</param>
    /// <param name="staged">The open compacted log.</param>
    /// <param name="stagedPath">The path of the compacted log, for error messages.</param>
    public static void ApplyLogAccess(SafeFileHandle log, SafeFileHandle staged, string stagedPath)
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
#if NET8_0_OR_GREATER
            System.IO.File.SetUnixFileMode(
                staged,
                System.IO.File.GetUnixFileMode(log) & (UnixFileMode)PermissionBits);
#endif
            return;
        }

        bool logAdded = false;
        bool stagedAdded = false;
        try
        {
            log.DangerousAddRef(ref logAdded);
            staged.DangerousAddRef(ref stagedAdded);
            ApplyLogAccessOnLinux(
                (int)log.DangerousGetHandle(),
                (int)staged.DangerousGetHandle(),
                stagedPath);
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

    private static void ApplyLogAccessOnLinux(int log, int staged, string stagedPath)
    {
        (uint logUid, uint logGid, uint logMode) = LinuxStat(log, stagedPath);
        (uint stagedUid, uint stagedGid, _) = LinuxStat(staged, stagedPath);

        // The compacted log is owner-only until the access control list and the permission bits are applied, so the
        // owning group changes before they do; a group that cannot be assigned gets none of the log's group access.
        bool hasLogGroup = TryChangeOwnership(staged, logUid, logGid, stagedUid, stagedGid, stagedPath);
        byte[]? acl = GetAccessAcl(log, stagedPath);
        uint mode = logMode & PermissionBits;
        if (!hasLogGroup && !RemoveOwningGroupAccess(acl, stagedPath))
        {
            mode &= ~GroupPermissionBits;
        }

        if (acl is null)
        {
            RemoveAccessAcl(staged, stagedPath);
        }
        else
        {
            SetAccessAcl(staged, acl, stagedPath);
        }

        // With an access control list the group bits are its mask, which the list just set to the same value.
        if (LinuxChangeMode(staged, mode) != 0)
        {
            throw CreateIOException("set the permissions of", stagedPath, Marshal.GetLastWin32Error());
        }
    }

    private static bool TryChangeOwnership(
        int staged,
        uint logUid,
        uint logGid,
        uint stagedUid,
        uint stagedGid,
        string stagedPath)
    {
        if (stagedUid == logUid && stagedGid == logGid)
        {
            return true;
        }

        if (LinuxChangeOwner(staged, logUid, logGid) == 0)
        {
            return true;
        }

        int error = Marshal.GetLastWin32Error();
        if (error != LinuxNotPermitted)
        {
            throw CreateIOException("set the owner of", stagedPath, error);
        }

        // The process cannot give the file away; it keeps the owner, which already reads and writes the log.
        if (stagedGid == logGid)
        {
            return true;
        }

        if (LinuxChangeOwner(staged, uint.MaxValue, logGid) == 0)
        {
            return true;
        }

        error = Marshal.GetLastWin32Error();
        if (error != LinuxNotPermitted)
        {
            throw CreateIOException("set the group of", stagedPath, error);
        }

        return false;
    }

    /// <summary>
    /// Removes the owning group's access from <paramref name="acl"/> and reports whether the group permission bits
    /// are the list's mask rather than the owning group's access.
    /// </summary>
    private static bool RemoveOwningGroupAccess(byte[]? acl, string stagedPath)
    {
        if (acl is null)
        {
            return false;
        }

        if (acl.Length < AclHeaderSize
            || (acl.Length - AclHeaderSize) % AclEntrySize != 0
            || BinaryPrimitives.ReadInt32LittleEndian(acl) != AclVersion)
        {
            throw new IOException($"The log's access control list could not be applied to '{stagedPath}'.");
        }

        bool hasMask = false;
        for (int offset = AclHeaderSize; offset < acl.Length; offset += AclEntrySize)
        {
            ushort tag = BinaryPrimitives.ReadUInt16LittleEndian(acl.AsSpan(offset));
            if (tag == AclGroupObject)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(acl.AsSpan(offset + 2), 0);
            }
            else if (tag == AclMask)
            {
                hasMask = true;
            }
        }

        return hasMask;
    }

    private static (uint Uid, uint Gid, uint Mode) LinuxStat(int descriptor, string stagedPath)
    {
        const uint required = LinuxStatMode | LinuxStatUid | LinuxStatGid;
        byte* buffer = stackalloc byte[LinuxStatSize];
        byte emptyPath = 0;
        if (LinuxStatx(descriptor, &emptyPath, LinuxEmptyPath, required, buffer) != 0)
        {
            throw CreateIOException("read the access of the log for", stagedPath, Marshal.GetLastWin32Error());
        }

        if ((*(uint*)buffer & required) != required)
        {
            throw new IOException($"The log's owner and permissions could not be read for '{stagedPath}'.");
        }

        return (
            *(uint*)(buffer + LinuxStatUidOffset),
            *(uint*)(buffer + LinuxStatGidOffset),
            *(ushort*)(buffer + LinuxStatModeOffset));
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

    [DllImport("libc", EntryPoint = "fchown", SetLastError = true)]
    private static extern int LinuxChangeOwner(int descriptor, uint owner, uint group);

    [DllImport("libc", EntryPoint = "fchmod", SetLastError = true)]
    private static extern int LinuxChangeMode(int descriptor, uint mode);

    [DllImport("libc", EntryPoint = "fgetxattr", SetLastError = true)]
    private static extern nint LinuxGetAttribute(int descriptor, byte* name, byte* value, nuint size);

    [DllImport("libc", EntryPoint = "fsetxattr", SetLastError = true)]
    private static extern int LinuxSetAttribute(int descriptor, byte* name, byte* value, nuint size, int flags);

    [DllImport("libc", EntryPoint = "fremovexattr", SetLastError = true)]
    private static extern int LinuxRemoveAttribute(int descriptor, byte* name);

#if !NET8_0_OR_GREATER
    [DllImport("libc", EntryPoint = "mkstemp", SetLastError = true)]
    private static extern int UnixMakeTemporary(byte* template);

    [DllImport("libSystem.B.dylib", EntryPoint = "mkstemp", SetLastError = true)]
    private static extern int MacMakeTemporary(byte* template);
#endif
}
