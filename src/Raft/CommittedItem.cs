// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Storage;

namespace Raft;

/// <summary>Identifies what a <see cref="CommittedItem"/> carries.</summary>
public enum CommittedItemKind
{
    /// <summary>A committed application command.</summary>
    Command,

    /// <summary>
    /// A snapshot of the application state: the state machine replaces its state with <see cref="CommittedItem.Data"/>,
    /// which holds every command up to <see cref="CommittedItem.Index"/>.
    /// </summary>
    Snapshot,
}

/// <summary>
/// An item of the apply stream <see cref="RaftNode.CommittedItems"/>: a committed application command, or a snapshot
/// that replaces the application state, together with its log position, so a state machine knows the log index its
/// state corresponds to.
/// </summary>
public sealed class CommittedItem
{
    internal CommittedItem(CommittedItemKind kind, ulong index, ulong term, ReadOnlyMemory<byte> data)
    {
        Kind = kind;
        Index = index;
        Term = term;
        Data = data;
    }

    /// <summary>Gets what this item carries.</summary>
    public CommittedItemKind Kind { get; }

    /// <summary>Gets the log index of the item; the state machine has applied the log up to it.</summary>
    public ulong Index { get; }

    /// <summary>Gets the term of the entry at <see cref="Index"/>.</summary>
    public ulong Term { get; }

    /// <summary>Gets the command payload, or the application state of a snapshot.</summary>
    public ReadOnlyMemory<byte> Data { get; }

    internal static CommittedItem ForSnapshot(Snapshot snapshot) =>
        new(CommittedItemKind.Snapshot, snapshot.Metadata.Index, snapshot.Metadata.Term, snapshot.Data);
}
