// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using Raft.Configuration;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class RaftNodeCommittedItemsTests
{
    [Test]
    public async Task CommittedItems_CarryTheLogIndexAndTermOfEachCommand()
    {
        await using var network = new InMemoryNetwork();
        RaftNode node = CreateSingleNode(network);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await StartLeaderAsync(node, timeout.Token);

            string[] commands = { "a", "b", "c" };
            foreach (string command in commands)
            {
                await node.ProposeAsync(Encoding.UTF8.GetBytes(command));
            }

            var items = new List<CommittedItem>();
            while (items.Count < commands.Length)
            {
                items.Add(await node.CommittedItems.ReadAsync(timeout.Token));
            }

            // The leader's empty entry of term 1 is index 1, so the commands are indices 2, 3, and 4.
            for (int i = 0; i < commands.Length; i++)
            {
                await Assert.That(items[i].Kind).IsEqualTo(CommittedItemKind.Command);
                await Assert.That(items[i].Index).IsEqualTo((ulong)(i + 2));
                await Assert.That(items[i].Term).IsEqualTo(node.Term);
                await Assert.That(Encoding.UTF8.GetString(items[i].Data.ToArray())).IsEqualTo(commands[i]);
            }

            await WaitUntilAsync(() => node.AppliedIndex >= items[^1].Index, timeout.Token);
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task Committed_AndCommittedItems_ReadOneQueue()
    {
        await using var network = new InMemoryNetwork();
        RaftNode node = CreateSingleNode(network);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await StartLeaderAsync(node, timeout.Token);

            await node.ProposeAsync(Encoding.UTF8.GetBytes("first"));
            await node.ProposeAsync(Encoding.UTF8.GetBytes("second"));

            ReadOnlyMemory<byte> first = await node.Committed.ReadAsync(timeout.Token);
            CommittedItem second = await node.CommittedItems.ReadAsync(timeout.Token);

            // Each command is delivered once, through whichever reader takes it.
            await Assert.That(Encoding.UTF8.GetString(first.ToArray())).IsEqualTo("first");
            await Assert.That(Encoding.UTF8.GetString(second.Data.ToArray())).IsEqualTo("second");
            await Assert.That(second.Index).IsEqualTo((ulong)3);
            await Assert.That(node.Committed.TryRead(out _)).IsFalse();
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task AppliedIndex_CoversEveryItemAConsumerHasRead()
    {
        await using var network = new InMemoryNetwork();
        RaftNode node = CreateSingleNode(network);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await StartLeaderAsync(node, timeout.Token);
            const int count = 2000;
            Task reader = Task.Run(
                async () =>
                {
                    for (int i = 0; i < count; i++)
                    {
                        CommittedItem item = await node.CommittedItems.ReadAsync(timeout.Token);

                        // A consumer may snapshot or compact at the index it read, so it must be applied.
                        if (node.AppliedIndex < item.Index)
                        {
                            throw new InvalidOperationException(
                                $"Item {item.Index} was read while AppliedIndex was {node.AppliedIndex}.");
                        }
                    }
                },
                timeout.Token);

            for (int i = 0; i < count; i++)
            {
                await node.ProposeAsync(new byte[] { (byte)i });
                if (i % 16 == 0)
                {
                    await Task.Yield();
                }
            }

            await reader;
        }
        finally
        {
            await node.DisposeAsync();
        }
    }

    [Test]
    public async Task CommittedItems_GiveEveryReplicaTheSameIndex()
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

            var indices = new List<ulong>();
            foreach (RaftNode node in nodes)
            {
                CommittedItem item = await node.CommittedItems.ReadAsync(timeout.Token);
                await Assert.That(Encoding.UTF8.GetString(item.Data.ToArray())).IsEqualTo("x");
                indices.Add(item.Index);
            }

            await Assert.That(indices.Distinct().Count()).IsEqualTo(1);
            await Assert.That(indices[0]).IsEqualTo(leader.CommitIndex);
        }
        finally
        {
            foreach (RaftNode node in nodes)
            {
                await node.DisposeAsync();
            }
        }
    }

    private static RaftNode CreateSingleNode(InMemoryNetwork network)
    {
        return new RaftNode(
            new RaftConfig { Id = 1, RandomizedElectionTimeout = 5 },
            new MemoryStorage(new ConfState(new ulong[] { 1 })),
            network.CreateNode(1),
            new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
    }

    private static async Task StartLeaderAsync(RaftNode node, CancellationToken ct)
    {
        await node.StartAsync(ct);
        await node.CampaignAsync(ct);
        await WaitUntilAsync(() => node.IsLeader, ct);
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

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken ct)
    {
        while (!condition())
        {
            await Task.Delay(10, ct);
        }
    }
}
