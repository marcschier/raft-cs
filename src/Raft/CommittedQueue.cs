// Copyright (c) marcschier. Licensed under the MIT License.

using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;

namespace Raft;

/// <summary>
/// The apply stream: committed commands and snapshots in log order. <see cref="Items"/>
/// (<see cref="RaftNode.CommittedItems"/>) reads every item; <see cref="Payloads"/> (<see cref="RaftNode.Committed"/>)
/// reads the command payloads and skips snapshots. Both take items from the same queue, so each item is delivered once,
/// to whichever reader takes it.
/// </summary>
internal sealed class CommittedQueue
{
    private readonly Channel<CommittedItem> _channel = Channel.CreateUnbounded<CommittedItem>(
        new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = true,
        });

    // The snapshots in the queue, so the payload reader counts commands only.
    private int _pendingSnapshots;

    internal CommittedQueue()
    {
        Items = new ItemReader(this);
        Payloads = new PayloadReader(this);
    }

    internal ChannelReader<CommittedItem> Items { get; }

    internal ChannelReader<ReadOnlyMemory<byte>> Payloads { get; }

    internal void Write(CommittedItem item)
    {
        // Counted before it is queued, so a reader that takes it never sees it uncounted.
        bool snapshot = item.Kind == CommittedItemKind.Snapshot;
        if (snapshot)
        {
            Interlocked.Increment(ref _pendingSnapshots);
        }

        if (!_channel.Writer.TryWrite(item) && snapshot)
        {
            Interlocked.Decrement(ref _pendingSnapshots);
        }
    }

    internal void Complete(Exception? error = null) => _channel.Writer.TryComplete(error);

    private bool TryTake([MaybeNullWhen(false)] out CommittedItem item)
    {
        if (!_channel.Reader.TryRead(out item))
        {
            return false;
        }

        if (item.Kind == CommittedItemKind.Snapshot)
        {
            Interlocked.Decrement(ref _pendingSnapshots);
        }

        return true;
    }

    /// <summary>Reads every item of the queue.</summary>
    private sealed class ItemReader : ChannelReader<CommittedItem>
    {
        private readonly CommittedQueue _queue;

        internal ItemReader(CommittedQueue queue) => _queue = queue;

        public override Task Completion => _queue._channel.Reader.Completion;

        public override bool CanCount => true;

        public override int Count => _queue._channel.Reader.Count;

        public override bool CanPeek => true;

        public override bool TryRead([MaybeNullWhen(false)] out CommittedItem item) => _queue.TryTake(out item);

        public override bool TryPeek([MaybeNullWhen(false)] out CommittedItem item) =>
            _queue._channel.Reader.TryPeek(out item);

        public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default) =>
            _queue._channel.Reader.WaitToReadAsync(cancellationToken);
    }

    /// <summary>Reads the command payloads of the queue and skips its snapshots.</summary>
    private sealed class PayloadReader : ChannelReader<ReadOnlyMemory<byte>>
    {
        private readonly CommittedQueue _queue;

        internal PayloadReader(CommittedQueue queue) => _queue = queue;

        public override Task Completion => _queue._channel.Reader.Completion;

        public override bool CanCount => true;

        // The queued items without the queued snapshots. With concurrent readers it is a point-in-time value.
        public override int Count =>
            Math.Max(0, _queue._channel.Reader.Count - Volatile.Read(ref _queue._pendingSnapshots));

        public override bool CanPeek => false;

        public override bool TryRead(out ReadOnlyMemory<byte> item)
        {
            while (_queue.TryTake(out CommittedItem? next))
            {
                if (next.Kind == CommittedItemKind.Command)
                {
                    item = next.Data;
                    return true;
                }
            }

            item = default;
            return false;
        }

        public override bool TryPeek(out ReadOnlyMemory<byte> item)
        {
            item = default;
            return false;
        }

        public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default) =>
            _queue._channel.Reader.WaitToReadAsync(cancellationToken);
    }
}
