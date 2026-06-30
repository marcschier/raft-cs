// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using Raft.Configuration;
using Raft.Storage;
using Raft.Storage.File;
using Raft.Transport;

namespace Raft.Tests;

public sealed class RaftNodeAsyncStorageTests
{
    [Test]
    public async Task FollowerForwardsProposalToLeaderOverTransport()
    {
        ulong[] ids = { 1, 2, 3 };
        await using var network = new InMemoryNetwork();
        var nodes = new List<RaftNode>();
        try
        {
            foreach (ulong id in ids)
            {
                var node = new RaftNode(
                    new RaftConfig
                    {
                        Id = id,
                        ElectionTick = 10,
                        HeartbeatTick = 1,
                        RandomizedElectionTimeout = id == 1 ? 6 : 18,
                    },
                    new MemoryStorage(new ConfState(ids)),
                    network.CreateNode(id),
                    new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
                nodes.Add(node);
            }

            foreach (RaftNode node in nodes)
            {
                await node.StartAsync();
            }

            RaftNode leader = await WaitForLeaderAsync(nodes);
            RaftNode follower = nodes.First(n => !n.IsLeader);
            await WaitUntilAsync(() => follower.LeaderId == leader.Id);

            // Propose to a follower: it must internally forward the proposal to the leader.
            await follower.ProposeAsync(Encoding.UTF8.GetBytes("forwarded"));

            foreach (RaftNode node in nodes)
            {
                string command = await ReadOneCommittedAsync(node);
                await Assert.That(command).IsEqualTo("forwarded");
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
    public async Task SingleNodeWithFileStorage_CommitsAndRecoversDurably()
    {
        string directory = Path.Combine(Path.GetTempPath(), "raft-node-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            await using var network = new InMemoryNetwork();
            ulong commitIndex;

            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.SetConfState(new ConfState(new ulong[] { 1 }));
                var node = new RaftNode(
                    new RaftConfig { Id = 1, RandomizedElectionTimeout = 5 },
                    storage,
                    network.CreateNode(1),
                    new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });

                await node.StartAsync();
                await node.CampaignAsync();
                await WaitUntilAsync(() => node.IsLeader);

                for (int i = 0; i < 4; i++)
                {
                    await node.ProposeAsync(Encoding.UTF8.GetBytes($"v{i}"));
                }

                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                var received = new List<string>();
                while (received.Count < 4)
                {
                    ReadOnlyMemory<byte> command = await node.Committed.ReadAsync(timeout.Token);
                    received.Add(Encoding.UTF8.GetString(command.ToArray()));
                }

                await Assert.That(string.Join(",", received)).IsEqualTo("v0,v1,v2,v3");
                commitIndex = node.CommitIndex;
                await node.DisposeAsync();
            }

            // Reopen the durable store: the committed entries and commit index must have survived the async writes.
            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                (HardState hardState, _) = reopened.InitialState();
                await Assert.That(reopened.LastIndex()).IsGreaterThanOrEqualTo(commitIndex);
                await Assert.That(hardState.Commit).IsEqualTo(commitIndex);
            }
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
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

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!condition())
        {
            await Task.Delay(10, timeout.Token);
        }
    }

    private static async Task<string> ReadOneCommittedAsync(RaftNode node)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        ReadOnlyMemory<byte> command = await node.Committed.ReadAsync(timeout.Token);
        return Encoding.UTF8.GetString(command.ToArray());
    }
}
