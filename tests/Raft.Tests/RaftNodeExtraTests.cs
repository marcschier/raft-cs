// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using Raft.Configuration;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class RaftNodeExtraTests
{
    [Test]
    public async Task SingleNode_CampaignProposeAndConfChange()
    {
        await using var network = new InMemoryNetwork();
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        var node = new RaftNode(
            new RaftConfig { Id = 1, RandomizedElectionTimeout = 5 },
            storage,
            network.CreateNode(1),
            new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });

        await node.StartAsync();
        await node.CampaignAsync();

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (!node.IsLeader)
        {
            await Task.Delay(10, timeout.Token);
        }

        await node.ChangeConfigurationAsync(ConfChangeV2.Single(ConfChangeType.AddLearnerNode, 2));
        await node.ProposeAsync(Encoding.UTF8.GetBytes("hello"));

        ReadOnlyMemory<byte> committed = await node.Committed.ReadAsync(timeout.Token);
        await Assert.That(Encoding.UTF8.GetString(committed.ToArray())).IsEqualTo("hello");
        await Assert.That(node.Term).IsGreaterThanOrEqualTo((ulong)1);
        await Assert.That(node.CommitIndex).IsGreaterThan((ulong)0);

        await node.DisposeAsync();
        await node.DisposeAsync();
        await Assert.That(async () => await node.ProposeAsync(default))
            .Throws<ObjectDisposedException>();
    }
}
