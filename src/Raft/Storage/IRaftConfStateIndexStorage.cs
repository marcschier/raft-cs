// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;

namespace Raft.Storage;

/// <summary>
/// An optional extension of <see cref="IRaftWritableStorage"/> for stores that record, with the committed
/// configuration, the log index up to which that configuration reflects every committed configuration change. A
/// <see cref="RaftNode"/> that restarts over such a store replays the committed configuration changes after the
/// recorded index, and can then reconstruct the membership at any index from it on; when it restores a snapshot at or
/// after the recorded index, it starts from the snapshot's membership instead. Over a store whose index is unknown the
/// node keeps the stored membership, also over a snapshot's, and replays none of the committed changes; over a store
/// that does not implement this interface it does the same, and persists each committed configuration with
/// <see cref="IRaftWritableStorage.SetConfState"/>. <see cref="MemoryStorage"/> and the file store implement it.
/// </summary>
public interface IRaftConfStateIndexStorage : IRaftWritableStorage
{
    /// <summary>
    /// Persists the committed configuration together with the log index up to which it reflects every committed
    /// configuration change.
    /// </summary>
    /// <param name="confState">The configuration to store.</param>
    /// <param name="index">
    /// The index of the last configuration-change entry <paramref name="confState"/> reflects, or zero for the
    /// bootstrap membership of a new cluster, before any log entry.
    /// </param>
    void SetConfState(ConfState confState, ulong index);

    /// <summary>
    /// Returns the log index up to which the stored configuration reflects every committed configuration change: the
    /// index passed to <see cref="SetConfState(ConfState, ulong)"/>, the index of an installed snapshot, or zero for
    /// the bootstrap membership of a new cluster.
    /// </summary>
    /// <returns>
    /// The index, or <see langword="null"/> when it is unknown: the configuration was stored with
    /// <see cref="IRaftWritableStorage.SetConfState"/>, which records no index.
    /// </returns>
    ulong? ConfStateIndex();
}
