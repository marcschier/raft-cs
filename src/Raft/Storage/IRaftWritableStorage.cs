// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;

namespace Raft.Storage;

/// <summary>
/// A durable Raft store that the consensus driver can also write to: append entries, install/compact snapshots, and
/// persist hard and configuration state. Both <see cref="MemoryStorage"/> and the file store implement this.
/// </summary>
public interface IRaftWritableStorage : IRaftStorage
{
    /// <summary>Appends contiguous entries, truncating any conflicting suffix.</summary>
    /// <param name="entries">The entries to append.</param>
    void Append(IReadOnlyList<Entry> entries);

    /// <summary>Installs a snapshot, discarding the log it supersedes.</summary>
    /// <param name="snapshot">The snapshot to apply.</param>
    void ApplySnapshot(Snapshot snapshot);

    /// <summary>Compacts the log up to (and excluding) <paramref name="compactIndex"/>.</summary>
    /// <param name="compactIndex">The first index to retain.</param>
    void Compact(ulong compactIndex);

    /// <summary>Persists the hard state.</summary>
    /// <param name="hardState">The hard state to store.</param>
    void SetHardState(HardState hardState);

    /// <summary>Persists the committed configuration.</summary>
    /// <param name="confState">The configuration to store.</param>
    void SetConfState(ConfState confState);
}
