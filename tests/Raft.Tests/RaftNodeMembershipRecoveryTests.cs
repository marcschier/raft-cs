// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Messages;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class RaftNodeMembershipRecoveryTests
{
    private static readonly ConfState Initial = new(new ulong[] { 1, 2 });

    // Index 2 enters a joint configuration removing node 2, index 3 leaves it, and index 4 enters a joint configuration
    // adding node 3. Re-applying index 2 and 3 to the configuration after index 4 changes its outgoing voters.
    private static readonly ConfChangeV2 EnterJointRemove2 = new(
        new[] { new ConfChangeSingle(ConfChangeType.RemoveNode, 2) },
        useJoint: true);

    private static readonly ConfChangeV2 Leave = ConfChangeV2.LeaveJoint();

    private static readonly ConfChangeV2 EnterJointAdd3 = new(
        new[] { new ConfChangeSingle(ConfChangeType.AddNode, 3) },
        useJoint: true);

    [Test]
    public async Task CreateSnapshotAsync_WithoutMembershipChanges_UsesTheBootstrapMembership()
    {
        await using var network = new InMemoryNetwork();
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        storage.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, new byte[] { 1 }),
            new Entry(EntryType.Normal, 1, 2, new byte[] { 2 }),
        });
        storage.SetHardState(new HardState(1, 0, 2));

        RaftNode node = CreateSingleNode(network, storage);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await node.StartAsync(timeout.Token);

            SnapshotMetadata metadata = await node.CreateSnapshotAsync(1, new byte[] { 1 }, timeout.Token);

            await Assert.That(metadata.Index).IsEqualTo(1UL);
            await Assert.That(metadata.ConfState.Voters.SequenceEqual(new ulong[] { 1 })).IsTrue();
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task Restart_ReplaysACommittedMembershipChangeTheStoreDidNotRecord()
    {
        await using var network = new InMemoryNetwork();
        ConfState afterLeave = Changer.Apply(Changer.Apply(Initial, EnterJointRemove2), Leave);
        ConfState latest = Changer.Apply(afterLeave, EnterJointAdd3);
        MemoryStorage storage = CreateCommittedLog();

        // The process stopped after the commit of index 4 was durable but before its configuration was recorded.
        storage.SetConfState(afterLeave, 3);

        RaftNode node = CreateSingleNode(network, storage);
        try
        {
            await Assert.That(node.AppliedIndex).IsEqualTo(4UL);
            await Assert.That(node.Configuration.Equals(latest)).IsTrue();
            await Assert.That(storage.InitialState().ConfState.Equals(latest)).IsTrue();
            await Assert.That(storage.ConfStateIndex()).IsEqualTo((ulong?)4);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task Restart_DoesNotReapplyMembershipChangesTheStoreRecorded()
    {
        await using var network = new InMemoryNetwork();
        ConfState latest = Changer.Apply(
            Changer.Apply(Changer.Apply(Initial, EnterJointRemove2), Leave),
            EnterJointAdd3);
        MemoryStorage storage = CreateCommittedLog();
        storage.SetConfState(latest, 4);

        RaftNode node = CreateSingleNode(network, storage);
        try
        {
            await Assert.That(node.Configuration.Equals(latest)).IsTrue();
            await Assert.That(node.Configuration.VotersOutgoing.SequenceEqual(new ulong[] { 1 })).IsTrue();
            await Assert.That(storage.InitialState().ConfState.Equals(latest)).IsTrue();
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task Restart_WithTheMembershipAheadOfTheStoredCommit_DoesNotReapplyItsChangesLater()
    {
        await RestartWithTheMembershipAheadOfTheStoredCommit(withSnapshot: false);
    }

    [Test]
    public async Task Restart_WithTheMembershipAheadOfTheStoredCommitAndAnOlderSnapshot_KeepsTheNewerMembership()
    {
        await RestartWithTheMembershipAheadOfTheStoredCommit(withSnapshot: true);
    }

    private static async Task RestartWithTheMembershipAheadOfTheStoredCommit(bool withSnapshot)
    {
        await using var network = new InMemoryNetwork();
        ConfState latest = Changer.Apply(
            Changer.Apply(Changer.Apply(Initial, EnterJointRemove2), Leave),
            EnterJointAdd3);
        MemoryStorage storage = CreateCommittedLog();
        if (withSnapshot)
        {
            // The application state after index 1, with the membership at index 1.
            storage.CreateSnapshot(1, Initial, new byte[] { 1 });
        }

        // The membership after index 4 was recorded, but the process stopped before the commit of index 4 was durable.
        storage.SetHardState(new HardState(1, 0, 1));
        storage.SetConfState(latest, 4);

        RaftNode node = new(
            new RaftConfig { Id = 1, RandomizedElectionTimeout = 1000 },
            storage,
            network.CreateNode(1),
            new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
        IRaftTransport leader = network.CreateNode(3);
        try
        {
            await Assert.That(node.AppliedIndex).IsEqualTo(1UL);
            await Assert.That(node.Configuration.Equals(latest)).IsTrue();

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            if (withSnapshot)
            {
                // The application still restores the snapshot before the commands after it.
                CommittedItem snapshot = await node.CommittedItems.ReadAsync(timeout.Token);
                await Assert.That(snapshot.Kind).IsEqualTo(CommittedItemKind.Snapshot);
                await Assert.That(snapshot.Index).IsEqualTo(1UL);
            }

            await leader.StartAsync(timeout.Token);
            await node.StartAsync(timeout.Token);

            // The leader reports the commit of index 4: the node applies index 2 to 4 after its construction.
            var heartbeat = new Message { Type = MessageType.Heartbeat, From = 3, To = 1, Term = 2, Commit = 4 };
            byte[] frame = new byte[MessageCodec.EncodedLength(heartbeat)];
            MessageCodec.TryWrite(heartbeat, frame);
            await leader.SendAsync(1, frame, timeout.Token);
            while (node.AppliedIndex < 4)
            {
                await Task.Delay(10, timeout.Token);
            }

            await Assert.That(node.Configuration.Equals(latest)).IsTrue();
            await Assert.That(node.Configuration.VotersOutgoing.SequenceEqual(new ulong[] { 1 })).IsTrue();
            await Assert.That(storage.InitialState().ConfState.Equals(latest)).IsTrue();
            await Assert.That(storage.ConfStateIndex()).IsEqualTo((ulong?)4);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    private static MemoryStorage CreateCommittedLog()
    {
        var storage = new MemoryStorage(Initial);
        storage.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.ConfChangeV2, 1, 2, EnterJointRemove2.Encode()),
            new Entry(EntryType.ConfChangeV2, 1, 3, Leave.Encode()),
            new Entry(EntryType.ConfChangeV2, 1, 4, EnterJointAdd3.Encode()),
        });
        storage.SetHardState(new HardState(1, 0, 4));
        return storage;
    }

    private static RaftNode CreateSingleNode(InMemoryNetwork network, MemoryStorage storage)
    {
        return new RaftNode(
            new RaftConfig { Id = 1, RandomizedElectionTimeout = 5 },
            storage,
            network.CreateNode(1),
            new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
    }
}
