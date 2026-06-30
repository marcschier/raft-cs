// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using Raft.Configuration;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class RaftNodeNonBatchTransportTests
{
    [Test]
    public async Task ReplicatesOverTransportThatDoesNotSupportBatching()
    {
        ulong[] ids = { 1, 2, 3 };
        await using var network = new InMemoryNetwork();
        var nodes = new List<RaftNode>();
        try
        {
            foreach (ulong id in ids)
            {
                // Wrap the batch-capable in-memory transport so the node only sees IRaftTransport, forcing the
                // driver's per-frame SendAsync fallback.
                IRaftTransport transport = new NonBatchTransport(network.CreateNode(id));
                var node = new RaftNode(
                    new RaftConfig
                    {
                        Id = id,
                        ElectionTick = 10,
                        HeartbeatTick = 1,
                        RandomizedElectionTimeout = id == 1 ? 6 : 18,
                    },
                    new MemoryStorage(new ConfState(ids)),
                    transport,
                    new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
                nodes.Add(node);
            }

            foreach (RaftNode node in nodes)
            {
                await node.StartAsync();
            }

            RaftNode leader = await WaitForLeaderAsync(nodes);
            await leader.ProposeAsync(Encoding.UTF8.GetBytes("nb"));

            foreach (RaftNode node in nodes)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                ReadOnlyMemory<byte> command = await node.Committed.ReadAsync(timeout.Token);
                await Assert.That(Encoding.UTF8.GetString(command.ToArray())).IsEqualTo("nb");
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

    /// <summary>An <see cref="IRaftTransport"/> wrapper that deliberately does not implement batch sending.</summary>
    private sealed class NonBatchTransport : IRaftTransport
    {
        private readonly IRaftTransport _inner;

        internal NonBatchTransport(IRaftTransport inner) => _inner = inner;

        public event Action<ReadOnlyMemory<byte>>? FrameReceived
        {
            add => _inner.FrameReceived += value;
            remove => _inner.FrameReceived -= value;
        }

        public ValueTask StartAsync(CancellationToken cancellationToken = default) =>
            _inner.StartAsync(cancellationToken);

        public ValueTask SendAsync(
            ulong recipient,
            ReadOnlyMemory<byte> frame,
            CancellationToken cancellationToken = default) =>
            _inner.SendAsync(recipient, frame, cancellationToken);

        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }
}
