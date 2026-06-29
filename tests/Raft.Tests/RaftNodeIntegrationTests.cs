// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class RaftNodeIntegrationTests
{
    [Test]
    public async Task ThreeNodeCluster_ElectsLeaderAndReplicatesProposals()
    {
        ulong[] ids = { 1, 2, 3 };
        await using var network = new InMemoryNetwork();
        var nodes = new List<RaftNode>();
        try
        {
            foreach (ulong id in ids)
            {
                var storage = new MemoryStorage(new Configuration.ConfState(ids));
                var config = new RaftConfig
                {
                    Id = id,
                    ElectionTick = 10,
                    HeartbeatTick = 1,
                    RandomizedElectionTimeout = id == 1 ? 6 : 18,
                };
                var node = new RaftNode(
                    config,
                    storage,
                    network.CreateNode(id),
                    new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
                nodes.Add(node);
            }

            foreach (RaftNode node in nodes)
            {
                await node.StartAsync();
            }

            RaftNode leader = await WaitForLeaderAsync(nodes);
            await Assert.That(leader.Id).IsEqualTo((ulong)1);

            for (int i = 0; i < 5; i++)
            {
                await leader.ProposeAsync(Encoding.UTF8.GetBytes($"cmd{i}"));
            }

            foreach (RaftNode node in nodes)
            {
                var received = await ReadCommittedAsync(node, 5);
                await Assert.That(string.Join(",", received)).IsEqualTo("cmd0,cmd1,cmd2,cmd3,cmd4");
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

    private static async Task<RaftNode> WaitForLeaderAsync(IReadOnlyList<RaftNode> nodes)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!timeout.IsCancellationRequested)
        {
            foreach (RaftNode node in nodes)
            {
                if (node.IsLeader)
                {
                    return node;
                }
            }

            await Task.Delay(20, timeout.Token);
        }

        throw new TimeoutException("No leader was elected.");
    }

    private static async Task<List<string>> ReadCommittedAsync(RaftNode node, int count)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var result = new List<string>();
        while (result.Count < count)
        {
            ReadOnlyMemory<byte> command = await node.Committed.ReadAsync(timeout.Token);
            result.Add(Encoding.UTF8.GetString(command.ToArray()));
        }

        return result;
    }
}
