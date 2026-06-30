// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Messages;
using Raft.Storage;

namespace Raft;

/// <summary>
/// A unit of durable work produced by <see cref="RaftCore.TakeStorageWrite"/>: the unstable log entries, the changed
/// hard state, and any pending snapshot to persist together with a single sync, plus the response messages that must
/// not be released to peers until that write is durable.
/// </summary>
/// <remarks>
/// This is the request side of the asynchronous storage-writes model (mirroring etcd's <c>MsgStorageAppend</c>). The
/// driver sends the leader's outbound append/heartbeat messages (from <see cref="RaftCore.TakeSendNowMessages"/>)
/// <em>before</em> persisting this write, so the leader replicates to followers in parallel with writing to its own
/// disk; the leader counts its own entries toward commit only once <see cref="RaftCore.AckStorageWrite"/> confirms the
/// write is durable. The <see cref="Responses"/> (follower append acknowledgements and granted votes) are held back
/// until after the write completes, because a node must never acknowledge state it has not yet made durable.
/// </remarks>
public sealed class StorageWrite
{
    internal StorageWrite(
        IReadOnlyList<Entry> entries,
        HardState? hardState,
        Snapshot? snapshot,
        IReadOnlyList<Message> responses,
        ulong lastEntryIndex,
        ulong lastEntryTerm,
        ulong snapshotIndex)
    {
        Entries = entries;
        HardState = hardState;
        Snapshot = snapshot;
        Responses = responses;
        LastEntryIndex = lastEntryIndex;
        LastEntryTerm = lastEntryTerm;
        SnapshotIndex = snapshotIndex;
    }

    /// <summary>Gets the unstable log entries to append, in index order.</summary>
    public IReadOnlyList<Entry> Entries { get; }

    /// <summary>Gets the hard state to persist, or <see langword="null"/> when it is unchanged since the last write.</summary>
    public HardState? HardState { get; }

    /// <summary>Gets the snapshot to install, or <see langword="null"/> when none is pending.</summary>
    public Snapshot? Snapshot { get; }

    /// <summary>Gets the response messages to send once this write is durable.</summary>
    public IReadOnlyList<Message> Responses { get; }

    /// <summary>Gets the index of the last entry in <see cref="Entries"/>, or zero when there are none.</summary>
    public ulong LastEntryIndex { get; }

    /// <summary>Gets the term of the last entry in <see cref="Entries"/>, or zero when there are none.</summary>
    public ulong LastEntryTerm { get; }

    /// <summary>Gets the index of the pending <see cref="Snapshot"/>, or zero when none.</summary>
    public ulong SnapshotIndex { get; }

    /// <summary>Gets a value indicating whether this write carries durable state (entries, hard state, or a snapshot).</summary>
    public bool HasPersistentWork => Entries.Count > 0 || HardState is not null || Snapshot is not null;
}
