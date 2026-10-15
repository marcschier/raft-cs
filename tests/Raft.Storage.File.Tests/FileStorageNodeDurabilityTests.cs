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
                storage.SetConfState(new ConfState(new ulong[] { 1 }), 0);
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

    [Test]
    public async Task Restart_DeliversTheSnapshotThenTheEntriesAfterIt()
    {
        string directory = Path.Combine(Path.GetTempPath(), "raft-node-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var before = new List<CommittedItem>();
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.SetConfState(new ConfState(new ulong[] { 1 }), 0);
                await using var network = new InMemoryNetwork();
                RaftNode node = CreateNode(storage, network);
                try
                {
                    await node.StartAsync();
                    await node.CampaignAsync();
                    await WaitUntilAsync(() => node.IsLeader);
                    for (int i = 0; i < 6; i++)
                    {
                        await node.ProposeAsync(Encoding.UTF8.GetBytes($"v{i}"));
                    }

                    while (before.Count < 6)
                    {
                        before.Add(await node.CommittedItems.ReadAsync(timeout.Token));
                    }

                    // Snapshot the state after v3, and keep only the log after it.
                    await node.CreateSnapshotAsync(before[3].Index, Encoding.UTF8.GetBytes("v0,v1,v2,v3"));
                    await node.CompactAsync(before[3].Index);
                }
                finally
                {
                    await node.DisposeAsync();
                }
            }

            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                await using var network = new InMemoryNetwork();
                RaftNode node = CreateNode(storage, network);
                try
                {
                    await node.StartAsync();
                    CommittedItem snapshot = await node.CommittedItems.ReadAsync(timeout.Token);
                    CommittedItem v4 = await node.CommittedItems.ReadAsync(timeout.Token);
                    CommittedItem v5 = await node.CommittedItems.ReadAsync(timeout.Token);

                    await Assert.That(snapshot.Kind).IsEqualTo(CommittedItemKind.Snapshot);
                    await Assert.That(snapshot.Index).IsEqualTo(before[3].Index);
                    await Assert.That(Encoding.UTF8.GetString(snapshot.Data.ToArray())).IsEqualTo("v0,v1,v2,v3");
                    await Assert.That(v4.Index).IsEqualTo(before[4].Index);
                    await Assert.That(Encoding.UTF8.GetString(v4.Data.ToArray())).IsEqualTo("v4");
                    await Assert.That(v5.Index).IsEqualTo(before[5].Index);
                    await Assert.That(Encoding.UTF8.GetString(v5.Data.ToArray())).IsEqualTo("v5");
                }
                finally
                {
                    await node.DisposeAsync();
                }
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

    [Test]
    public async Task Restart_RefusesALogCompactedPastTheSnapshot()
    {
        string directory = Path.Combine(Path.GetTempPath(), "raft-node-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                var entries = new Entry[10];
                for (int i = 0; i < entries.Length; i++)
                {
                    entries[i] = new Entry(EntryType.Normal, 1, (ulong)i + 1, new byte[] { (byte)i });
                }

                storage.SetConfState(new ConfState(new ulong[] { 1 }), 0);
                storage.Append(entries);
                storage.SetHardState(new HardState(1, 1, 10));
                storage.CreateSnapshot(4, new ConfState(new ulong[] { 1 }), new byte[] { 4 });
                storage.Compact(6);
            }

            // Entries 5 and 6 are in neither the snapshot nor the log, so the state machine cannot be rebuilt.
            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await using var network = new InMemoryNetwork();
            await Assert.That(() => CreateNode(reopened, network)).Throws<InvalidOperationException>();
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Test]
    public async Task Restart_ReplaysACommittedMembershipChangeAndRecordsIt()
    {
        string directory = Path.Combine(Path.GetTempPath(), "raft-node-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            ConfChangeV2 removeNode2 = ConfChangeV2.Single(ConfChangeType.RemoveNode, 2);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                // The commit of the change at index 2 is durable, but the process stopped before it was recorded.
                storage.SetConfState(new ConfState(new ulong[] { 1, 2 }), 0);
                storage.Write(
                    new[]
                    {
                        new Entry(EntryType.Normal, 1, 1, default),
                        new Entry(EntryType.ConfChangeV2, 1, 2, removeNode2.Encode()),
                    },
                    new HardState(1, 1, 2),
                    null);
            }

            await using var network = new InMemoryNetwork();
            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                RaftNode node = CreateNode(reopened, network);
                await Assert.That(node.Configuration.Voters.SequenceEqual(new ulong[] { 1 })).IsTrue();
                await node.DisposeAsync();
            }

            using var recovered = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(recovered.InitialState().ConfState.Voters.SequenceEqual(new ulong[] { 1 })).IsTrue();
            await Assert.That(recovered.ConfStateIndex()).IsEqualTo((ulong?)2);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    private static RaftNode CreateNode(FileRaftStorage storage, InMemoryNetwork network)
    {
        return new RaftNode(
            new RaftConfig { Id = 1, RandomizedElectionTimeout = 5 },
            storage,
            network.CreateNode(1),
            new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
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
