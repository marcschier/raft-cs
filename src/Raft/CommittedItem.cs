// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft;

/// <summary>Identifies what a <see cref="CommittedItem"/> carries.</summary>
public enum CommittedItemKind
{
    /// <summary>A committed application command.</summary>
    Command,
}

/// <summary>
/// An item of the apply stream <see cref="RaftNode.CommittedItems"/>: a committed application command together with
/// its log position, so a state machine knows the log index its state corresponds to.
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

    /// <summary>Gets the command payload.</summary>
    public ReadOnlyMemory<byte> Data { get; }
}
