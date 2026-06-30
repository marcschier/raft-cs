// Copyright (c) marcschier. Licensed under the MIT License.

using System.Threading.Channels;
using Raft.Configuration;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class RaftNodeObservationTests
{
    [Test]
    public async Task StateChanges_EmitsFollowerBaseline()
    {
        await using var network = new InMemoryNetwork();
        var node = new RaftNode(
            new RaftConfig { Id = 1, RandomizedElectionTimeout = 1000 },
            new MemoryStorage(new ConfState(new ulong[] { 1 })),
            network.CreateNode(1),
            new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
        try
        {
            await node.StartAsync();

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            RaftStateChange baseline = await node.StateChanges.ReadAsync(timeout.Token);

            await Assert.That(baseline.Role).IsEqualTo(RaftRole.Follower);
            await Assert.That(baseline.LeaderId).IsEqualTo((ulong)0);
            await Assert.That(baseline.IsLeader).IsFalse();
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task StateChanges_ObserveLeadershipAndFollowerLeader()
    {
        ulong[] ids = { 1, 2, 3 };
        await using var network = new InMemoryNetwork();
        var nodes = new List<RaftNode>();
        try
        {
            foreach (ulong id in ids)
            {
                nodes.Add(new RaftNode(
                    new RaftConfig
                    {
                        Id = id,
                        ElectionTick = 10,
                        HeartbeatTick = 1,
                        RandomizedElectionTimeout = id == 1 ? 6 : 18,
                    },
                    new MemoryStorage(new ConfState(ids)),
                    network.CreateNode(id),
                    new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) }));
            }

            foreach (RaftNode node in nodes)
            {
                await node.StartAsync();
            }

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));

            RaftStateChange leader = await ReadUntilAsync(
                nodes[0].StateChanges, change => change.IsLeader, timeout.Token);
            await Assert.That(leader.LeaderId).IsEqualTo((ulong)1);
            await Assert.That(leader.Role).IsEqualTo(RaftRole.Leader);
            await Assert.That(leader.Term).IsGreaterThanOrEqualTo((ulong)1);

            RaftStateChange followerView = await ReadUntilAsync(
                nodes[1].StateChanges, change => change.LeaderId == 1, timeout.Token);
            await Assert.That(followerView.Role).IsEqualTo(RaftRole.Follower);
        }
        finally
        {
            foreach (RaftNode node in nodes)
            {
                await node.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task CommittedConfigurations_EmitsBaselineAndMembershipChange()
    {
        await using var network = new InMemoryNetwork();
        var node = new RaftNode(
            new RaftConfig { Id = 1, RandomizedElectionTimeout = 5 },
            new MemoryStorage(new ConfState(new ulong[] { 1 })),
            network.CreateNode(1),
            new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
        try
        {
            await node.StartAsync();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));

            ConfState baseline = await node.CommittedConfigurations.ReadAsync(timeout.Token);
            await Assert.That(baseline.Voters).Contains((ulong)1);
            await Assert.That(baseline.Learners.Count).IsEqualTo(0);

            await node.CampaignAsync();
            await WaitUntilAsync(() => node.IsLeader, timeout.Token);

            await node.ChangeConfigurationAsync(ConfChangeV2.Single(ConfChangeType.AddLearnerNode, 2));

            ConfState changed = await ReadUntilAsync(
                node.CommittedConfigurations, state => state.Learners.Contains((ulong)2), timeout.Token);
            await Assert.That(changed.Voters).Contains((ulong)1);
            await Assert.That(node.Configuration.Learners).Contains((ulong)2);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task Disposal_CompletesObservationStreams()
    {
        await using var network = new InMemoryNetwork();
        var node = new RaftNode(
            new RaftConfig { Id = 1, RandomizedElectionTimeout = 1000 },
            new MemoryStorage(new ConfState(new ulong[] { 1 })),
            network.CreateNode(1),
            new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });

        await node.StartAsync();
        await node.DisposeAsync();

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        // Draining each stream must terminate now that the writers are completed on dispose.
        await foreach (RaftStateChange _ in node.StateChanges.ReadAllAsync(timeout.Token))
        {
        }

        await foreach (ConfState _ in node.CommittedConfigurations.ReadAllAsync(timeout.Token))
        {
        }

        await Assert.That(node.StateChanges.Completion.IsCompleted).IsTrue();
        await Assert.That(node.CommittedConfigurations.Completion.IsCompleted).IsTrue();
    }

    private static async Task<T> ReadUntilAsync<T>(
        ChannelReader<T> reader,
        Func<T, bool> predicate,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            T item = await reader.ReadAsync(cancellationToken);
            if (predicate(item))
            {
                return item;
            }
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        while (!condition())
        {
            await Task.Delay(10, cancellationToken);
        }
    }
}
