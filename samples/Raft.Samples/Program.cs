// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using Raft;
using Raft.Configuration;
using Raft.Storage;
using Raft.Transport;

Console.WriteLine("Raft in-memory cluster sample: 3 nodes elect a leader and replicate commands.\n");

ulong[] ids = { 1, 2, 3 };
await using var network = new InMemoryNetwork();
var nodes = new List<RaftNode>();

foreach (ulong id in ids)
{
    var storage = new MemoryStorage(new ConfState(ids));
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
        new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(15) });
    nodes.Add(node);
}

foreach (RaftNode node in nodes)
{
    await node.StartAsync();
}

RaftNode leader = await waitForLeaderAsync(nodes);
Console.WriteLine($"Node {leader.Id} was elected leader for term {leader.Term}.\n");
string[] commands = { "set x = 1", "set y = 2", "delete x", "set z = 3" };
foreach (string command in commands)
{
    await leader.ProposeAsync(Encoding.UTF8.GetBytes(command));
}

foreach (RaftNode node in nodes)
{
    Console.WriteLine($"Node {node.Id} committed:");
    for (int i = 0; i < commands.Length; i++)
    {
        ReadOnlyMemory<byte> entry = await node.Committed.ReadAsync();
        Console.WriteLine($"  {i + 1}. {Encoding.UTF8.GetString(entry.ToArray())}");
    }

    Console.WriteLine();
}

foreach (RaftNode node in nodes)
{
    await node.DisposeAsync();
}

Console.WriteLine("All three replicas converged on the same committed log. Done.");

static async Task<RaftNode> waitForLeaderAsync(IReadOnlyList<RaftNode> nodes)
{
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
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
