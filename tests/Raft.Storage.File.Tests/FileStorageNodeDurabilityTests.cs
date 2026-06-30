// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using Raft.Configuration;
using Raft.Transport;

namespace Raft.Storage.File.Tests;

public sealed class FileStorageNodeDurabilityTests
{
    [Test]
    public async Task SingleNode_CommitsViaAsyncWritesAndRecoversDurably()
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

            // Reopen the durable store: the committed entries and commit index survive the async storage writes.
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

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!condition())
        {
            await Task.Delay(10, timeout.Token);
        }
    }
}
