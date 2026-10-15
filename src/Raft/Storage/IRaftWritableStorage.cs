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

    /// <summary>
    /// Atomically persists a batch of unstable entries, the changed hard state, and any pending snapshot with a single
    /// durability sync, in the order snapshot, then entries, then hard state. This is the primary write path for the
    /// asynchronous storage-writes driver; <paramref name="hardState"/> and <paramref name="snapshot"/> may be
    /// <see langword="null"/> when unchanged or absent. The granular methods remain available for incremental use.
    /// </summary>
    /// <param name="entries">The unstable entries to append (may be empty).</param>
    /// <param name="hardState">The hard state to persist, or <see langword="null"/> when unchanged.</param>
    /// <param name="snapshot">The snapshot to install, or <see langword="null"/> when none.</param>
    void Write(IReadOnlyList<Entry> entries, HardState? hardState, Snapshot? snapshot);

    /// <summary>
    /// Persists the committed configuration together with the log index up to which it reflects every committed
    /// configuration change. On restart, a node replays only the configuration changes after that index.
    /// </summary>
    /// <param name="confState">The configuration to store.</param>
    /// <param name="index">
    /// The index of the last configuration-change entry <paramref name="confState"/> reflects, or zero for the
    /// bootstrap membership of a new cluster.
    /// </param>
    void SetConfState(ConfState confState, ulong index);

    /// <summary>
    /// Returns the log index up to which the stored configuration reflects every committed configuration change: the
    /// index passed to <see cref="SetConfState"/>, the index of an installed snapshot, or zero for the bootstrap
    /// membership.
    /// </summary>
    /// <returns>The index, or <see langword="null"/> when the store does not record it.</returns>
    ulong? ConfStateIndex();
}
