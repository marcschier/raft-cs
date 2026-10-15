// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using Raft.Configuration;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class RaftNodeSnapshotDeliveryTests
{
    [Test]
    public async Task CreateSnapshotAsync_StoresTheApplicationStateAtTheIndex()
    {
        await using var network = new InMemoryNetwork();
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        RaftNode node = CreateSingleNode(network, storage);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await StartLeaderAsync(node, timeout.Token);
            var state = new StateMachine();
            foreach (string command in new[] { "a", "b", "c" })
            {
                await node.ProposeAsync(Encoding.UTF8.GetBytes(command));
            }

            while (state.Applied < 4)
            {
                state.Apply(await node.CommittedItems.ReadAsync(timeout.Token));
            }

            SnapshotMetadata metadata = await node.CreateSnapshotAsync(state.Applied, state.Encode());

            Snapshot? stored = storage.LatestSnapshot();
            await Assert.That(stored).IsNotNull();
            await Assert.That(metadata.Index).IsEqualTo(4UL);
            await Assert.That(stored!.Metadata.Index).IsEqualTo(4UL);
            await Assert.That(stored.Metadata.Term).IsEqualTo(node.Term);
            await Assert.That(stored.Metadata.ConfState.Voters.SequenceEqual(new ulong[] { 1 })).IsTrue();
            await Assert.That(Encoding.UTF8.GetString(stored.Data.ToArray())).IsEqualTo("a,b,c");
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task CreateSnapshotAsync_PastTheAppliedIndex_Throws()
    {
        await using var network = new InMemoryNetwork();
        RaftNode node = CreateSingleNode(network, new MemoryStorage(new ConfState(new ulong[] { 1 })));
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await StartLeaderAsync(node, timeout.Token);

            await Assert.That(async () => await node.CreateSnapshotAsync(node.AppliedIndex + 1, new byte[] { 1 }))
                .Throws<ArgumentOutOfRangeException>();
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task CompactAsync_PastTheLatestSnapshot_Throws()
    {
        await using var network = new InMemoryNetwork();
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        RaftNode node = CreateSingleNode(network, storage);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await StartLeaderAsync(node, timeout.Token);
            var state = new StateMachine();
            for (int i = 0; i < 4; i++)
            {
                await node.ProposeAsync(Encoding.UTF8.GetBytes($"c{i}"));
            }

            while (state.Applied < 5)
            {
                state.Apply(await node.CommittedItems.ReadAsync(timeout.Token));
            }

            await node.CreateSnapshotAsync(3, Encoding.UTF8.GetBytes("c0,c1"));

            // Compacting past the snapshot would leave entries that neither the snapshot nor the log holds.
            await Assert.That(async () => await node.CompactAsync(4)).Throws<ArgumentOutOfRangeException>();

            await node.CompactAsync(3);
            await Assert.That(storage.FirstIndex()).IsEqualTo(4UL);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task CompactAsync_RechecksASnapshotQueuedBeforeIt()
    {
        await using var network = new InMemoryNetwork();
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        RaftNode node = CreateSingleNode(network, storage);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await StartLeaderAsync(node, timeout.Token);
            var state = new StateMachine();
            for (int i = 0; i < 4; i++)
            {
                await node.ProposeAsync(Encoding.UTF8.GetBytes($"c{i}"));
            }

            while (state.Applied < 5)
            {
                state.Apply(await node.CommittedItems.ReadAsync(timeout.Token));
            }

            Task<SnapshotMetadata> snapshot = node.CreateSnapshotAsync(3, new byte[] { 3 });
            Task compact;
            try
            {
                compact = node.CompactAsync(5);
            }
            catch (ArgumentOutOfRangeException ex)
            {
                // The loop recorded the snapshot before CompactAsync checked it: the caller-side check refuses.
                compact = Task.FromException(ex);
            }

            await snapshot;

            await Assert.That(async () => await compact).Throws<ArgumentOutOfRangeException>();
            await Assert.That(storage.FirstIndex()).IsEqualTo(1UL);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task Restart_RestoresSnapshotMembershipBeforeReplayingJointChange()
    {
        await using var network = new InMemoryNetwork();
        var initial = new ConfState(new ulong[] { 1, 2 });
        ConfChangeV2 change = new(
            new[] { new ConfChangeSingle(ConfChangeType.RemoveNode, 2) },
            useJoint: true);
        ConfState latest = Changer.Apply(initial, change);
        var storage = new MemoryStorage(initial);
        storage.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.ConfChangeV2, 1, 2, change.Encode()),
        });
        storage.SetHardState(new HardState(1, 0, 2));
        storage.CreateSnapshot(1, initial, new byte[] { 1 });
        storage.SetConfState(latest);

        RaftNode node = CreateSingleNode(network, storage);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await node.StartAsync(timeout.Token);
            while (node.AppliedIndex < 2)
            {
                await Task.Delay(10, timeout.Token);
            }

            await Assert.That(node.Configuration.Voters.SequenceEqual(new ulong[] { 1 })).IsTrue();
            await Assert.That(node.Configuration.VotersOutgoing.SequenceEqual(initial.Voters)).IsTrue();
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task Restart_ReplaysCommittedMembershipBeforeTheNodeStarts()
    {
        await using var network = new InMemoryNetwork();
        var initial = new ConfState(new ulong[] { 1, 2 });
        ConfChangeV2 change = new(
            new[] { new ConfChangeSingle(ConfChangeType.RemoveNode, 2) },
            useJoint: true);
        ConfState latest = Changer.Apply(initial, change);
        var storage = new MemoryStorage(initial);
        storage.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.ConfChangeV2, 1, 2, change.Encode()),
        });
        storage.SetHardState(new HardState(1, 0, 2));
        storage.CreateSnapshot(1, initial, new byte[] { 1 });
        storage.SetConfState(latest);

        RaftNode node = CreateSingleNode(network, storage);
        try
        {
            await Assert.That(node.AppliedIndex).IsEqualTo(2UL);
            await Assert.That(node.Configuration.Voters.SequenceEqual(new ulong[] { 1 })).IsTrue();
            await Assert.That(node.Configuration.VotersOutgoing.SequenceEqual(initial.Voters)).IsTrue();
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task CreateSnapshotAsync_RejectsMembershipBeforeTheRecoverableBaseline()
    {
        await using var network = new InMemoryNetwork();
        var initial = new ConfState(new ulong[] { 1, 2 });
        ConfChangeV2 change = new(
            new[] { new ConfChangeSingle(ConfChangeType.RemoveNode, 2) },
            useJoint: true);
        ConfState latest = Changer.Apply(initial, change);
        var storage = new MemoryStorage(initial);
        storage.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.ConfChangeV2, 1, 2, change.Encode()),
        });
        storage.SetHardState(new HardState(1, 0, 2));
        storage.SetConfState(latest);

        RaftNode node = CreateSingleNode(network, storage);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await node.StartAsync(timeout.Token);

            await Assert.That(async () => await node.CreateSnapshotAsync(1, new byte[] { 1 }, timeout.Token))
                .Throws<InvalidOperationException>();
            SnapshotMetadata metadata = await node.CreateSnapshotAsync(2, new byte[] { 2 }, timeout.Token);
            await Assert.That(metadata.ConfState.Equals(latest)).IsTrue();
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task LaggingFollower_ReceivesTheLeadersSnapshotOnCommittedItems()
    {
        await using var cluster = new LaggingFollowerCluster();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        StateMachine leaderState = await cluster.SnapshotPastTheFollowerAsync(timeout.Token);

        // The follower's state machine receives the leader's state, then the commands after it.
        var followerState = new StateMachine();
        CommittedItem snapshot = await ReadUntilAsync(
            cluster.Follower, followerState, item => item.Kind == CommittedItemKind.Snapshot, timeout.Token);
        await Assert.That(snapshot.Index).IsEqualTo(leaderState.Applied);
        await Assert.That(string.Join(",", followerState.Commands)).IsEqualTo("v0,v1,v2,v3");

        CommittedItem next = await ReadUntilAsync(
            cluster.Follower, followerState, item => item.Kind == CommittedItemKind.Command, timeout.Token);
        await Assert.That(next.Index).IsGreaterThan(snapshot.Index);
        await Assert.That(string.Join(",", followerState.Commands)).IsEqualTo("v0,v1,v2,v3,after-heal");
    }

    [Test]
    public async Task Committed_SkipsSnapshots()
    {
        await using var cluster = new LaggingFollowerCluster();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await cluster.SnapshotPastTheFollowerAsync(timeout.Token);

        // A consumer of the payload view sees commands only; the leader's snapshot is not one.
        ReadOnlyMemory<byte> first = await cluster.Follower.Committed.ReadAsync(timeout.Token);
        await Assert.That(Encoding.UTF8.GetString(first.ToArray())).IsEqualTo("after-heal");
    }

    [Test]
    public async Task Committed_DoesNotPeekOrDiscardSnapshots()
    {
        await using var network = new InMemoryNetwork();
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        storage.Append(new[] { new Entry(EntryType.Normal, 1, 1, default) });
        storage.CreateSnapshot(1, new ConfState(new ulong[] { 1 }), new byte[] { 1 });
        RaftNode node = CreateSingleNode(network, storage);
        try
        {
            await Assert.That(node.Committed.CanPeek).IsFalse();
            await Assert.That(node.Committed.TryPeek(out _)).IsFalse();

            CommittedItem snapshot = await node.CommittedItems.ReadAsync();
            await Assert.That(snapshot.Kind).IsEqualTo(CommittedItemKind.Snapshot);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    private static RaftNode CreateSingleNode(InMemoryNetwork network, MemoryStorage storage)
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

    private static async Task<RaftNode> WaitForLeaderAsync(IReadOnlyList<RaftNode> nodes, CancellationToken ct)
    {
        while (true)
        {
            foreach (RaftNode node in nodes)
            {
                if (node.IsLeader)
                {
                    return node;
                }
            }

            await Task.Delay(20, ct);
        }
    }

    private static async Task<CommittedItem> ReadUntilAsync(
        RaftNode node, StateMachine state, Func<CommittedItem, bool> predicate, CancellationToken ct)
    {
        while (true)
        {
            CommittedItem item = await node.CommittedItems.ReadAsync(ct);
            state.Apply(item);
            if (predicate(item))
            {
                return item;
            }
        }
    }

    /// <summary>
    /// Three voters with pre-vote, so the partitioned node 3 cannot disturb the leader when it rejoins. The leader
    /// commits commands while node 3 is cut off, snapshots its state machine, and compacts behind node 3.
    /// </summary>
    private sealed class LaggingFollowerCluster : IAsyncDisposable
    {
        private readonly InMemoryNetwork _network = new();
        private readonly List<RaftNode> _nodes = new();

        public LaggingFollowerCluster()
        {
            ulong[] ids = { 1, 2, 3 };
            foreach (ulong id in ids)
            {
                _nodes.Add(new RaftNode(
                    new RaftConfig
                    {
                        Id = id,
                        ElectionTick = 10,
                        HeartbeatTick = 1,
                        PreVote = true,
                        RandomizedElectionTimeout = id == 1 ? 6 : 30,
                    },
                    new MemoryStorage(new ConfState(ids)),
                    _network.CreateNode(id),
                    new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) }));
            }
        }

        public RaftNode Follower => _nodes[2];

        public async Task<StateMachine> SnapshotPastTheFollowerAsync(CancellationToken ct)
        {
            foreach (RaftNode node in _nodes)
            {
                await node.StartAsync(ct);
            }

            RaftNode leader = await WaitForLeaderAsync(_nodes.Take(2).ToList(), ct);
            var leaderState = new StateMachine();
            _network.SetPartition(new ulong[] { Follower.Id });
            for (int i = 0; i < 4; i++)
            {
                await leader.ProposeAsync(Encoding.UTF8.GetBytes($"v{i}"), ct);
            }

            while (leaderState.Commands.Count < 4)
            {
                leaderState.Apply(await leader.CommittedItems.ReadAsync(ct));
            }

            await leader.CreateSnapshotAsync(leaderState.Applied, leaderState.Encode(), ct);
            await leader.CompactAsync(leaderState.Applied, ct);
            _network.Heal();
            await leader.ProposeAsync(Encoding.UTF8.GetBytes("after-heal"), ct);
            return leaderState;
        }

        public async ValueTask DisposeAsync()
        {
            foreach (RaftNode node in _nodes)
            {
                await node.DisposeAsync();
            }

            await _network.DisposeAsync();
        }
    }

    /// <summary>A state machine that records the commands it applied and snapshots them as a comma list.</summary>
    private sealed class StateMachine
    {
        public List<string> Commands { get; } = new();

        public ulong Applied { get; private set; }

        public void Apply(CommittedItem item)
        {
            string text = Encoding.UTF8.GetString(item.Data.ToArray());
            if (item.Kind == CommittedItemKind.Snapshot)
            {
                Commands.Clear();
                Commands.AddRange(text.Length == 0 ? Array.Empty<string>() : text.Split(','));
            }
            else
            {
                Commands.Add(text);
            }

            Applied = item.Index;
        }

        public byte[] Encode() => Encoding.UTF8.GetBytes(string.Join(",", Commands));
    }
}
