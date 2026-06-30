// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using Raft.Configuration;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class RaftNodeCompactionTests
{
    [Test]
    public async Task CompactAsync_DiscardsAppliedPrefix_AndNodeKeepsWorking()
    {
        await using var network = new InMemoryNetwork();
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        var node = new RaftNode(
            new RaftConfig { Id = 1, RandomizedElectionTimeout = 5 },
            storage,
            network.CreateNode(1),
            new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
        try
        {
            await node.StartAsync();
            await node.CampaignAsync();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await WaitUntilAsync(() => node.IsLeader, timeout.Token);

            for (int i = 0; i < 5; i++)
            {
                await node.ProposeAsync(Encoding.UTF8.GetBytes($"c{i}"));
            }

            for (int i = 0; i < 5; i++)
            {
                await node.Committed.ReadAsync(timeout.Token);
            }

            await WaitUntilAsync(() => node.AppliedIndex >= 6, timeout.Token);

            await node.CompactAsync(3);

            await Assert.That(storage.FirstIndex()).IsEqualTo((ulong)4);

            await node.ProposeAsync(Encoding.UTF8.GetBytes("after"));
            ReadOnlyMemory<byte> after = await node.Committed.ReadAsync(timeout.Token);
            await Assert.That(Encoding.UTF8.GetString(after.ToArray())).IsEqualTo("after");
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task CompactAsync_BeyondAppliedIndex_Throws()
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
            await node.CampaignAsync();
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await WaitUntilAsync(() => node.IsLeader, timeout.Token);

            await Assert.That(async () => await node.CompactAsync(ulong.MaxValue))
                .Throws<ArgumentOutOfRangeException>();
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task CompactAsync_OnHealthyCluster_DoesNotBreakReplication()
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
            RaftNode leader = await WaitForLeaderAsync(nodes, timeout.Token);

            await leader.ProposeAsync(Encoding.UTF8.GetBytes("x"));
            foreach (RaftNode node in nodes)
            {
                await ReadCommandAsync(node, "x", timeout.Token);
            }

            // Every node has applied "x"; compacting the leader's applied prefix must not break the cluster.
            await WaitUntilAsync(() => leader.AppliedIndex >= 2, timeout.Token);
            await leader.CompactAsync(leader.AppliedIndex);

            await leader.ProposeAsync(Encoding.UTF8.GetBytes("y"));
            foreach (RaftNode node in nodes)
            {
                await ReadCommandAsync(node, "y", timeout.Token);
            }
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
    public async Task CompactAsync_LaggingFollower_CatchesUpViaSnapshot()
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
                        RandomizedElectionTimeout = id == 1 ? 6 : 30,
                    },
                    new MemoryStorage(new ConfState(ids)),
                    network.CreateNode(id),
                    new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) }));
            }

            foreach (RaftNode node in nodes)
            {
                await node.StartAsync();
            }

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            RaftNode leader = await WaitForLeaderAsync(nodes, timeout.Token);
            RaftNode follower = nodes.First(n => n.Id == 3);

            // Cut node 3 off, commit + apply a batch on the {1,2} quorum, then compact past node 3's position.
            network.SetPartition(new ulong[] { 3 });
            for (int i = 0; i < 4; i++)
            {
                await leader.ProposeAsync(Encoding.UTF8.GetBytes($"v{i}"));
            }

            await WaitUntilAsync(() => leader.AppliedIndex >= 5, timeout.Token);
            await leader.CompactAsync(leader.AppliedIndex);

            // Heal: node 3 is now behind the compacted boundary and must be re-seeded with a snapshot.
            network.Heal();
            ulong target = leader.CommitIndex;
            await WaitUntilAsync(() => follower.CommitIndex >= target, timeout.Token);

            // A post-heal proposal commits everywhere, including the re-seeded follower.
            await leader.ProposeAsync(Encoding.UTF8.GetBytes("after-heal"));
            string command = await ReadCommandAsync(follower, "after-heal", timeout.Token);
            await Assert.That(command).IsEqualTo("after-heal");
        }
        finally
        {
            foreach (RaftNode node in nodes)
            {
                await node.DisposeAsync();
            }
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

    private static async Task<string> ReadCommandAsync(RaftNode node, string expected, CancellationToken ct)
    {
        while (true)
        {
            ReadOnlyMemory<byte> command = await node.Committed.ReadAsync(ct);
            string text = Encoding.UTF8.GetString(command.ToArray());
            if (text == expected)
            {
                return text;
            }
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken ct)
    {
        while (!condition())
        {
            await Task.Delay(10, ct);
        }
    }
}
