// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Storage;

namespace Raft.Tests;

public sealed class CommittedQueueTests
{
    [Test]
    public async Task PayloadCount_StaysZero_WhenOnlyASnapshotIsQueuedAndTaken()
    {
        var queue = new CommittedQueue();
        queue.Write(SnapshotItem(1));

        // No command is ever queued, so every observation of the payload count is zero, before and after the item
        // reader takes the snapshot.
        await Assert.That(queue.Payloads.Count).IsEqualTo(0);
        await Assert.That(queue.Items.Count).IsEqualTo(1);
        await Assert.That(queue.Items.TryRead(out CommittedItem? taken)).IsTrue();
        await Assert.That(taken!.Kind).IsEqualTo(CommittedItemKind.Snapshot);
        await Assert.That(queue.Payloads.Count).IsEqualTo(0);
        await Assert.That(queue.Items.Count).IsEqualTo(0);
        await Assert.That(queue.Payloads.TryRead(out _)).IsFalse();
        await Assert.That(queue.Payloads.Count).IsEqualTo(0);
    }

    [Test]
    public async Task PayloadCount_FollowsTheCommandsEitherReaderTakes()
    {
        var queue = new CommittedQueue();
        queue.Write(SnapshotItem(1));
        queue.Write(CommandItem(2));
        queue.Write(CommandItem(3));
        await Assert.That(queue.Payloads.Count).IsEqualTo(2);
        await Assert.That(queue.Items.Count).IsEqualTo(3);

        // The payload reader skips the snapshot and takes the first command.
        await Assert.That(queue.Payloads.TryRead(out ReadOnlyMemory<byte> first)).IsTrue();
        await Assert.That(first.ToArray().SequenceEqual(new byte[] { 2 })).IsTrue();
        await Assert.That(queue.Payloads.Count).IsEqualTo(1);
        await Assert.That(queue.Items.Count).IsEqualTo(1);

        // The item reader takes the last command.
        await Assert.That(queue.Items.TryRead(out CommittedItem? last)).IsTrue();
        await Assert.That(last!.Index).IsEqualTo(3UL);
        await Assert.That(queue.Payloads.Count).IsEqualTo(0);
        await Assert.That(queue.Items.Count).IsEqualTo(0);
    }

    private static CommittedItem SnapshotItem(ulong index) =>
        CommittedItem.ForSnapshot(new Snapshot(new SnapshotMetadata(index, 1, new ConfState()), new byte[] { 0 }));

    private static CommittedItem CommandItem(ulong index) =>
        new(CommittedItemKind.Command, index, 1, new[] { (byte)index });
}
