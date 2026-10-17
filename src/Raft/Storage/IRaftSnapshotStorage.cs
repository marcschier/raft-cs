// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;

namespace Raft.Storage;

/// <summary>
/// A writable store that keeps snapshots of the application state. With it, a <see cref="RaftNode"/> records the
/// snapshots its application creates (<see cref="RaftNode.CreateSnapshotAsync"/>), sends the latest one to a follower
/// whose log ends before the compacted boundary, and hands it to the application again after a restart, before the
/// entries that follow it. <see cref="MemoryStorage"/> and the file store implement it.
/// </summary>
public interface IRaftSnapshotStorage : IRaftWritableStorage
{
    /// <summary>
    /// Records a snapshot of the application state at <paramref name="index"/>, which the log still holds. The entries
    /// after the index are kept; <see cref="IRaftWritableStorage.Compact"/> discards the ones up to it.
    /// </summary>
    /// <param name="index">The log index the application state corresponds to.</param>
    /// <param name="confState">The cluster membership at <paramref name="index"/>.</param>
    /// <param name="data">The application state; the store keeps its own copy.</param>
    /// <returns>The recorded snapshot.</returns>
    /// <exception cref="RaftStorageException">
    /// <see cref="RaftStorageError.SnapshotOutOfDate"/> when <paramref name="index"/> is not after the latest
    /// snapshot; <see cref="RaftStorageError.Compacted"/> or <see cref="RaftStorageError.Unavailable"/> when the log
    /// does not hold <paramref name="index"/>.
    /// </exception>
    Snapshot CreateSnapshot(ulong index, ConfState confState, ReadOnlyMemory<byte> data);

    /// <summary>
    /// Returns the latest snapshot recorded by <see cref="CreateSnapshot"/> or installed by
    /// <see cref="IRaftWritableStorage.ApplySnapshot"/>, or <see langword="null"/> when there is none.
    /// </summary>
    /// <returns>The latest snapshot, or <see langword="null"/>.</returns>
    Snapshot? LatestSnapshot();
}
