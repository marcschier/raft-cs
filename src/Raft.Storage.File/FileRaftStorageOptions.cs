// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Storage.File;

/// <summary>Options used to configure <see cref="FileRaftStorage"/>.</summary>
public sealed class FileRaftStorageOptions
{
    /// <summary>Initializes a new instance of the <see cref="FileRaftStorageOptions"/> class.</summary>
    /// <param name="directoryPath">The directory that stores the Raft log, hard state, and snapshot files.</param>
    public FileRaftStorageOptions(string directoryPath)
    {
        ThrowIfNull(directoryPath);

        DirectoryPath = directoryPath;
    }

    /// <summary>Gets the directory that stores the Raft log, hard state, and snapshot files.</summary>
    public string DirectoryPath { get; }

    /// <summary>
    /// Gets or sets a value indicating whether flushes should force data through the operating system cache.
    /// </summary>
    public bool Fsync { get; set; } = true;

    private static void ThrowIfNull(object? value)
    {
#if NET8_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(value);
#else
        if (value is null)
        {
            throw new ArgumentNullException(nameof(value));
        }
#endif
    }
}
