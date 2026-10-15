// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using Raft.Configuration;
using Raft.Messages;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

/// <summary>
/// A store written against <see cref="IRaftWritableStorage"/> alone, without <see cref="IRaftConfStateIndexStorage"/>,
/// still drives a <see cref="RaftNode"/>: the node persists membership with the one-argument
/// <see cref="IRaftWritableStorage.SetConfState"/> and recovers it without an index.
/// </summary>
public sealed class RaftNodeCustomStorageTests
{
    private static readonly ulong[] Learners = { 2 };

    // Index 2 enters a joint configuration removing node 2, index 3 leaves it, and index 4 enters a joint configuration
    // adding node 3.
    private static readonly ConfState Initial = new(new ulong[] { 1, 2 });

    private static readonly ConfChangeV2 EnterJointRemove2 = new(
        new[] { new ConfChangeSingle(ConfChangeType.RemoveNode, 2) },
        useJoint: true);

    private static readonly ConfChangeV2 Leave = ConfChangeV2.LeaveJoint();

    private static readonly ConfChangeV2 EnterJointAdd3 = new(
        new[] { new ConfChangeSingle(ConfChangeType.AddNode, 3) },
        useJoint: true);

    private static readonly ConfState Latest = Changer.Apply(
        Changer.Apply(Changer.Apply(Initial, EnterJointRemove2), Leave),
        EnterJointAdd3);

    [Test]
    public async Task WritableOnlyStorage_CommitsAndRestarts()
    {
        var storage = new WritableOnlyStorage(new ConfState(new ulong[] { 1 }));
        await Assert.That(((object)storage) is IRaftConfStateIndexStorage).IsFalse();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await using (var network = new InMemoryNetwork())
        {
            RaftNode node = CreateSingleNode(network, storage);
            try
            {
                await StartLeaderAsync(node, timeout.Token);
                List<string> commands = await CommitWithLearnerAsync(node, timeout.Token);
                await Assert.That(string.Join(",", commands)).IsEqualTo("a,b,c");
                await Assert.That(async () => await node.CreateSnapshotAsync(node.AppliedIndex, new byte[] { 1 }))
                    .Throws<NotSupportedException>();
            }
            finally
            {
                await node.DisposeAsync();
            }
        }

        await Assert.That(storage.SetConfStateCalls).IsEqualTo(1);
        await Assert.That(storage.InitialState().ConfState.Learners.SequenceEqual(Learners)).IsTrue();

        await using (var network = new InMemoryNetwork())
        {
            RaftNode restarted = CreateSingleNode(network, storage);
            try
            {
                await Assert.That(restarted.Configuration.Learners.SequenceEqual(Learners)).IsTrue();

                // The commands are applied again once their commit is known: from the stored hard state, or after the
                // restarted node is elected again.
                await StartLeaderAsync(restarted, timeout.Token);
                await Assert.That(await ReadCommandsAsync(restarted, 3, timeout.Token)).IsEqualTo("a,b,c");
                await restarted.ProposeAsync(Encoding.UTF8.GetBytes("d"), timeout.Token);
                await Assert.That(await ReadCommandsAsync(restarted, 1, timeout.Token)).IsEqualTo("d");
                await Assert.That(restarted.Configuration.Learners.SequenceEqual(Learners)).IsTrue();
            }
            finally
            {
                await restarted.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task SnapshotStorageWithoutConfStateIndex_CommitsSnapshotsAndRestarts()
    {
        var storage = new SnapshotStorageWithoutConfStateIndex(new ConfState(new ulong[] { 1 }));
        await Assert.That(((object)storage) is IRaftConfStateIndexStorage).IsFalse();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        ulong snapshotIndex;

        await using (var network = new InMemoryNetwork())
        {
            RaftNode node = CreateSingleNode(network, storage);
            try
            {
                await StartLeaderAsync(node, timeout.Token);
                List<string> commands = await CommitWithLearnerAsync(node, timeout.Token);
                snapshotIndex = node.AppliedIndex;
                SnapshotMetadata metadata = await node.CreateSnapshotAsync(
                    snapshotIndex, Encoding.UTF8.GetBytes(string.Join(",", commands)), timeout.Token);
                await Assert.That(metadata.ConfState.Learners.SequenceEqual(Learners)).IsTrue();
                await node.CompactAsync(snapshotIndex, timeout.Token);

                await node.ProposeAsync(Encoding.UTF8.GetBytes("d"), timeout.Token);
                await Assert.That(await ReadCommandsAsync(node, 1, timeout.Token)).IsEqualTo("d");
            }
            finally
            {
                await node.DisposeAsync();
            }
        }

        await Assert.That(storage.SetConfStateCalls).IsEqualTo(1);
        await Assert.That(storage.FirstIndex()).IsEqualTo(snapshotIndex + 1);

        await using (var network = new InMemoryNetwork())
        {
            RaftNode restarted = CreateSingleNode(network, storage);
            try
            {
                await Assert.That(restarted.Configuration.Learners.SequenceEqual(Learners)).IsTrue();
                CommittedItem snapshot = await restarted.CommittedItems.ReadAsync(timeout.Token);
                await Assert.That(snapshot.Kind).IsEqualTo(CommittedItemKind.Snapshot);
                await Assert.That(snapshot.Index).IsEqualTo(snapshotIndex);
                await Assert.That(Encoding.UTF8.GetString(snapshot.Data.ToArray())).IsEqualTo("a,b,c");

                await StartLeaderAsync(restarted, timeout.Token);
                await Assert.That(await ReadCommandsAsync(restarted, 1, timeout.Token)).IsEqualTo("d");
                await restarted.ProposeAsync(Encoding.UTF8.GetBytes("e"), timeout.Token);
                await Assert.That(await ReadCommandsAsync(restarted, 1, timeout.Token)).IsEqualTo("e");
                SnapshotMetadata next = await restarted.CreateSnapshotAsync(
                    restarted.AppliedIndex, Encoding.UTF8.GetBytes("a,b,c,d,e"), timeout.Token);
                await Assert.That(next.ConfState.Learners.SequenceEqual(Learners)).IsTrue();
            }
            finally
            {
                await restarted.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task SnapshotStorageWithoutConfStateIndex_KeepsTheStoredMembershipOverAnOlderSnapshot()
    {
        // Without a recorded index, the changes committed after the stored commit are applied again once their commit
        // is known, although the stored membership already reflects them: the behaviour of earlier versions.
        ConfState afterCommit = await RestartWithTheMembershipAheadOfTheCommitAsync(
            new SnapshotStorageWithoutConfStateIndex(Initial),
            storage => storage.SetConfState(Latest));
        ConfState reapplied = Changer.Apply(
            Changer.Apply(Changer.Apply(Latest, EnterJointRemove2), Leave),
            EnterJointAdd3);
        await Assert.That(afterCommit.Equals(reapplied)).IsTrue();
        await Assert.That(afterCommit.VotersOutgoing.OrderBy(id => id).SequenceEqual(new ulong[] { 1, 3 })).IsTrue();
    }

    [Test]
    public async Task SnapshotStorageWithAConfStateIndex_DoesNotReapplyTheRecordedChanges()
    {
        ConfState afterCommit = await RestartWithTheMembershipAheadOfTheCommitAsync(
            new MemoryStorage(Initial),
            storage => ((IRaftConfStateIndexStorage)storage).SetConfState(Latest, 4));
        await Assert.That(afterCommit.Equals(Latest)).IsTrue();
        await Assert.That(afterCommit.VotersOutgoing.SequenceEqual(new ulong[] { 1 })).IsTrue();
    }

    /// <summary>
    /// Stores the joint-change log with a snapshot at index 1, stores the membership after index 4 with
    /// <paramref name="storeMembership"/> while the stored commit is 1, restarts a node, and checks it keeps that
    /// membership over the snapshot's. Then a leader reports the commit of index 4; returns the membership after it.
    /// </summary>
    private static async Task<ConfState> RestartWithTheMembershipAheadOfTheCommitAsync(
        IRaftSnapshotStorage storage,
        Action<IRaftSnapshotStorage> storeMembership)
    {
        storage.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.ConfChangeV2, 1, 2, EnterJointRemove2.Encode()),
            new Entry(EntryType.ConfChangeV2, 1, 3, Leave.Encode()),
            new Entry(EntryType.ConfChangeV2, 1, 4, EnterJointAdd3.Encode()),
        });
        storage.CreateSnapshot(1, Initial, new byte[] { 1 });

        // The membership after index 4 was stored, but the process stopped before the commit of index 4 was durable.
        storeMembership(storage);
        storage.SetHardState(new HardState(1, 0, 1));

        await using var network = new InMemoryNetwork();
        var node = new RaftNode(
            new RaftConfig { Id = 1, RandomizedElectionTimeout = 1000 },
            storage,
            network.CreateNode(1),
            new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
        IRaftTransport leader = network.CreateNode(3);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            CommittedItem snapshot = await node.CommittedItems.ReadAsync(timeout.Token);
            await Assert.That(snapshot.Kind).IsEqualTo(CommittedItemKind.Snapshot);
            await Assert.That(snapshot.Index).IsEqualTo(1UL);
            await Assert.That(node.AppliedIndex).IsEqualTo(1UL);
            await Assert.That(node.Configuration.Equals(Latest)).IsTrue();

            await leader.StartAsync(timeout.Token);
            await node.StartAsync(timeout.Token);
            var heartbeat = new Message { Type = MessageType.Heartbeat, From = 3, To = 1, Term = 2, Commit = 4 };
            byte[] frame = new byte[MessageCodec.EncodedLength(heartbeat)];
            MessageCodec.TryWrite(heartbeat, frame);
            await leader.SendAsync(1, frame, timeout.Token);
            while (node.AppliedIndex < 4)
            {
                await Task.Delay(10, timeout.Token);
            }

            return node.Configuration;
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    /// <summary>Commits a, b, a configuration change adding learner 2, then c; returns the commands read.</summary>
    private static async Task<List<string>> CommitWithLearnerAsync(RaftNode node, CancellationToken ct)
    {
        await node.ProposeAsync(Encoding.UTF8.GetBytes("a"), ct);
        await node.ProposeAsync(Encoding.UTF8.GetBytes("b"), ct);
        await node.ChangeConfigurationAsync(ConfChangeV2.Single(ConfChangeType.AddLearnerNode, 2), ct);
        await node.ProposeAsync(Encoding.UTF8.GetBytes("c"), ct);
        var commands = new List<string>();
        while (commands.Count < 3)
        {
            CommittedItem item = await node.CommittedItems.ReadAsync(ct);
            commands.Add(Encoding.UTF8.GetString(item.Data.ToArray()));
        }

        while (!node.Configuration.Learners.SequenceEqual(Learners))
        {
            await Task.Delay(10, ct);
        }

        return commands;
    }

    private static async Task<string> ReadCommandsAsync(RaftNode node, int count, CancellationToken ct)
    {
        var commands = new List<string>();
        while (commands.Count < count)
        {
            CommittedItem item = await node.CommittedItems.ReadAsync(ct);
            commands.Add(Encoding.UTF8.GetString(item.Data.ToArray()));
        }

        return string.Join(",", commands);
    }

    private static RaftNode CreateSingleNode(InMemoryNetwork network, IRaftWritableStorage storage)
    {
        return new RaftNode(
            new RaftConfig { Id = 1, RandomizedElectionTimeout = 5 },
            storage,
            network.CreateNode(1),
            new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
    }

    private static async Task StartLeaderAsync(RaftNode node, CancellationToken ct)
    {
        await node.StartAsync(ct);
        await node.CampaignAsync(ct);
        while (!node.IsLeader)
        {
            await Task.Delay(10, ct);
        }
    }

    /// <summary>A third-party store that implements <see cref="IRaftWritableStorage"/> only.</summary>
    private class WritableOnlyStorage : IRaftWritableStorage
    {
        private int _setConfStateCalls;

        public WritableOnlyStorage(ConfState initialConfState)
        {
            Inner = new MemoryStorage(initialConfState);
        }

        public int SetConfStateCalls => Volatile.Read(ref _setConfStateCalls);

        protected MemoryStorage Inner { get; }

        public (HardState HardState, ConfState ConfState) InitialState() => Inner.InitialState();

        public IReadOnlyList<Entry> Entries(ulong low, ulong high, ulong maxBytes) => Inner.Entries(low, high, maxBytes);

        public ulong Term(ulong index) => Inner.Term(index);

        public ulong FirstIndex() => Inner.FirstIndex();

        public ulong LastIndex() => Inner.LastIndex();

        public Snapshot Snapshot(ulong requestIndex) => Inner.Snapshot(requestIndex);

        public void Append(IReadOnlyList<Entry> entries) => Inner.Append(entries);

        public void ApplySnapshot(Snapshot snapshot) => Inner.ApplySnapshot(snapshot);

        public void Compact(ulong compactIndex) => Inner.Compact(compactIndex);

        public void SetHardState(HardState hardState) => Inner.SetHardState(hardState);

        public void Write(IReadOnlyList<Entry> entries, HardState? hardState, Snapshot? snapshot) =>
            Inner.Write(entries, hardState, snapshot);

        public void SetConfState(ConfState confState)
        {
            Interlocked.Increment(ref _setConfStateCalls);
            Inner.SetConfState(confState);
        }
    }

    /// <summary>A third-party store that keeps snapshots but does not record the index of its configuration.</summary>
    private sealed class SnapshotStorageWithoutConfStateIndex : WritableOnlyStorage, IRaftSnapshotStorage
    {
        public SnapshotStorageWithoutConfStateIndex(ConfState initialConfState)
            : base(initialConfState)
        {
        }

        public Snapshot CreateSnapshot(ulong index, ConfState confState, ReadOnlyMemory<byte> data) =>
            Inner.CreateSnapshot(index, confState, data);

        public Snapshot? LatestSnapshot() => Inner.LatestSnapshot();
    }
}
