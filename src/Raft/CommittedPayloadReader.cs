// Copyright (c) marcschier. Licensed under the MIT License.

using System.Threading.Channels;

namespace Raft;

/// <summary>
/// The <see cref="RaftNode.Committed"/> view of the apply stream: the command payloads of the items queued for
/// <see cref="RaftNode.CommittedItems"/>. Both readers take items from the same queue, so each command is delivered
/// once, to whichever reader takes it.
/// </summary>
internal sealed class CommittedPayloadReader : ChannelReader<ReadOnlyMemory<byte>>
{
    private readonly ChannelReader<CommittedItem> _items;

    internal CommittedPayloadReader(ChannelReader<CommittedItem> items) => _items = items;

    public override Task Completion => _items.Completion;

    public override bool CanCount => _items.CanCount;

    public override int Count => _items.Count;

    public override bool CanPeek => _items.CanPeek;

    public override bool TryRead(out ReadOnlyMemory<byte> item)
    {
        if (_items.TryRead(out CommittedItem? next))
        {
            item = next.Data;
            return true;
        }

        item = default;
        return false;
    }

    public override bool TryPeek(out ReadOnlyMemory<byte> item)
    {
        if (_items.TryPeek(out CommittedItem? next))
        {
            item = next.Data;
            return true;
        }

        item = default;
        return false;
    }

    public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default) =>
        _items.WaitToReadAsync(cancellationToken);
}
