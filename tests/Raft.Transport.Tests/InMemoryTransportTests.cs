// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Transport.Tests;

public sealed class InMemoryTransportTests
{
    [Test]
    public async Task Two_Nodes_Exchange_Frames()
    {
        await using var network = new InMemoryNetwork();
        IRaftTransport node1 = network.CreateNode(1);
        IRaftTransport node2 = network.CreateNode(2);
        byte[] received = [];
        node2.FrameReceived += frame => received = frame.ToArray();

        await node1.StartAsync();
        await node2.StartAsync();
        await node1.SendAsync(2, new byte[] { 1, 2, 3 });
        await network.DrainAsync();

        await Assert.That(received).IsEquivalentTo(new byte[] { 1, 2, 3 });
    }

    [Test]
    public async Task Broadcast_Delivers_To_All_Other_Nodes()
    {
        await using var network = new InMemoryNetwork();
        IRaftTransport node1 = network.CreateNode(1);
        IRaftTransport node2 = network.CreateNode(2);
        IRaftTransport node3 = network.CreateNode(3);
        IRaftTransport node4 = network.CreateNode(4);
        List<byte[]> received = [];
        node2.FrameReceived += frame => received.Add(frame.ToArray());
        node3.FrameReceived += frame => received.Add(frame.ToArray());
        node4.FrameReceived += frame => received.Add(frame.ToArray());

        await node1.StartAsync();
        await node2.StartAsync();
        await node3.StartAsync();
        await node4.StartAsync();
        await node1.SendAsync(0, new byte[] { 7, 8, 9 });
        await network.DrainAsync();

        await Assert.That(received.Count).IsEqualTo(3);
        foreach (byte[] frame in received)
        {
            await Assert.That(frame).IsEquivalentTo(new byte[] { 7, 8, 9 });
        }
    }

    [Test]
    public async Task Partition_Drops_Cross_Partition_Frames_And_Heal_Restores()
    {
        await using var network = new InMemoryNetwork();
        IRaftTransport node1 = network.CreateNode(1);
        IRaftTransport node2 = network.CreateNode(2);
        int received = 0;
        node2.FrameReceived += _ => received++;

        await node1.StartAsync();
        await node2.StartAsync();
        network.SetPartition([1]);
        await node1.SendAsync(2, new byte[] { 1 });
        await network.DrainAsync();
        await Assert.That(received).IsEqualTo(0);

        network.Heal();
        await node1.SendAsync(2, new byte[] { 2 });
        await network.DrainAsync();
        await Assert.That(received).IsEqualTo(1);
    }

    [Test]
    public async Task Drop_Predicate_Drops_Configured_Link()
    {
        await using var network = new InMemoryNetwork(new InMemoryNetworkOptions
        {
            DropPredicate = static (from, to) => from == 1 && to == 2,
        });
        IRaftTransport node1 = network.CreateNode(1);
        IRaftTransport node2 = network.CreateNode(2);
        int received = 0;
        node2.FrameReceived += _ => received++;

        await node1.StartAsync();
        await node2.StartAsync();
        await node1.SendAsync(2, new byte[] { 1 });
        await network.DrainAsync();

        await Assert.That(received).IsEqualTo(0);
    }
}
