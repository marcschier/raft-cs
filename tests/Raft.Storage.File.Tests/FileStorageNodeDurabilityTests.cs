// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using Raft.Configuration;
using Raft.Transport;

namespace Raft.Storage.File.Tests;

public sealed class FileStorageNodeDurabilityTests
{
    private static readonly ConfState Initial = new(new ulong[] { 1, 2 });

    private static readonly ConfChangeV2 EnterJointRemove2 = new(
        new[] { new ConfChangeSingle(ConfChangeType.RemoveNode, 2) },
        useJoint: true);

    private static readonly ConfChangeV2 Leave = ConfChangeV2.LeaveJoint();

    private static readonly ConfChangeV2 EnterJointAdd3 = new(
        new[] { new ConfChangeSingle(ConfChangeType.AddNode, 3) },
        useJoint: true);

    private static readonly ConfState AfterJointChanges = Changer.Apply(
        Changer.Apply(Changer.Apply(Initial, EnterJointRemove2), Leave),
        EnterJointAdd3);

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
                storage.SetConfState(new ConfState(new ulong[] { 1 }));
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

                storage.SetConfState(new ConfState(new ulong[] { 1 }));
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

    [Test]
    public async Task Restart_FromALegacyStore_DoesNotReapplyTheRecordedMembershipChanges()
    {
        string directory = Path.Combine(Path.GetTempPath(), "raft-node-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            // The files an earlier version left after committing and applying index 1 to 4: its SetConfState stored the
            // membership after index 4 in a snapshot file at index zero.
            var entries = new List<Entry> { new(EntryType.Normal, 1, 1, default) };
            entries.AddRange(JointChangeEntries(2));
            new LegacyStoreFormat.Log()
                .Append(entries.ToArray())
                .HardState(new HardState(1, 1, 4))
                .WriteTo(directory);
            LegacyStoreFormat.WriteSnapshotFile(directory, 0, 0, AfterJointChanges);

            await using var network = new InMemoryNetwork();
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                RaftNode node = CreateNode(storage, network);
                try
                {
                    await Assert.That(node.AppliedIndex).IsEqualTo(4UL);
                    await Assert.That(node.Configuration.Equals(AfterJointChanges)).IsTrue();
                    await Assert.That(node.Configuration.VotersOutgoing.SequenceEqual(new ulong[] { 1 })).IsTrue();
                }
                finally
                {
                    await node.DisposeAsync();
                }
            }

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(reopened.InitialState().ConfState.Equals(AfterJointChanges)).IsTrue();
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
    public async Task Restart_FromALegacyFollowerWithASnapshot_AppliesItsMembershipChangesOnce()
    {
        string directory = Path.Combine(Path.GetTempPath(), "raft-node-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            // An earlier version installed a snapshot at index 4 from the leader, then committed and applied the
            // membership changes at index 5 to 7: its SetConfState rewrote the snapshot file with the membership after
            // index 7, keeping index 4.
            new LegacyStoreFormat.Log()
                .HardState(new HardState(1, 0, 4))
                .Append(JointChangeEntries(5))
                .HardState(new HardState(1, 0, 7))
                .WriteTo(directory);
            LegacyStoreFormat.WriteSnapshotFile(directory, 4, 1, AfterJointChanges);

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await using (var network = new InMemoryNetwork())
            {
                using var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
                RaftNode node = CreateNode(storage, network);
                try
                {
                    await Assert.That(node.AppliedIndex).IsEqualTo(7UL);
                    await Assert.That(node.Configuration.Equals(AfterJointChanges)).IsTrue();
                    await Assert.That(node.Configuration.VotersOutgoing.SequenceEqual(new ulong[] { 1 })).IsTrue();

                    // The first snapshot after the upgrade records the membership at its own index.
                    await node.StartAsync(timeout.Token);
                    SnapshotMetadata metadata = await node.CreateSnapshotAsync(7, new byte[] { 7 }, timeout.Token);
                    await Assert.That(metadata.ConfState.Equals(AfterJointChanges)).IsTrue();
                }
                finally
                {
                    await node.DisposeAsync();
                }
            }

            await using (var network = new InMemoryNetwork())
            {
                using var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
                RaftNode node = CreateNode(storage, network);
                try
                {
                    await node.StartAsync(timeout.Token);
                    CommittedItem snapshot = await node.CommittedItems.ReadAsync(timeout.Token);
                    await Assert.That(snapshot.Kind).IsEqualTo(CommittedItemKind.Snapshot);
                    await Assert.That(snapshot.Index).IsEqualTo(7UL);
                    await Assert.That(node.Configuration.Equals(AfterJointChanges)).IsTrue();
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
    public async Task Restart_FromALegacySnapshotWithState_DeliversItThenTheCommandsAfterIt()
    {
        string directory = Path.Combine(Path.GetTempPath(), "raft-node-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            // An earlier version installed a snapshot of the state after index 4, which replaced the log up to it, then
            // committed two commands; its SetConfState stored the latest membership in the snapshot file.
            new LegacyStoreFormat.Log()
                .HardState(new HardState(1, 0, 4))
                .Append(
                    new Entry(EntryType.Normal, 1, 5, Encoding.UTF8.GetBytes("v4")),
                    new Entry(EntryType.Normal, 1, 6, Encoding.UTF8.GetBytes("v5")))
                .HardState(new HardState(1, 0, 6))
                .WriteTo(directory);
            LegacyStoreFormat.WriteSnapshotFile(
                directory, 4, 1, AfterJointChanges, Encoding.UTF8.GetBytes("v0,v1,v2,v3"));

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await using var network = new InMemoryNetwork();
            using var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            RaftNode node = CreateNode(storage, network);
            try
            {
                CommittedItem snapshot = await node.CommittedItems.ReadAsync(timeout.Token);
                CommittedItem v4 = await node.CommittedItems.ReadAsync(timeout.Token);
                CommittedItem v5 = await node.CommittedItems.ReadAsync(timeout.Token);

                await Assert.That(snapshot.Kind).IsEqualTo(CommittedItemKind.Snapshot);
                await Assert.That(snapshot.Index).IsEqualTo(4UL);
                await Assert.That(Encoding.UTF8.GetString(snapshot.Data.ToArray())).IsEqualTo("v0,v1,v2,v3");
                await Assert.That(v4.Index).IsEqualTo(5UL);
                await Assert.That(Encoding.UTF8.GetString(v4.Data.ToArray())).IsEqualTo("v4");
                await Assert.That(v5.Index).IsEqualTo(6UL);
                await Assert.That(Encoding.UTF8.GetString(v5.Data.ToArray())).IsEqualTo("v5");

                // The membership is the stored one, not taken as the one at index 4, and its index stays unknown.
                await Assert.That(node.Configuration.Equals(AfterJointChanges)).IsTrue();
                await Assert.That(storage.ConfStateIndex()).IsNull();

                // Until the application records a new snapshot, a lagging follower is not sent this one.
                RaftStorageError? error = null;
                try
                {
                    storage.Snapshot(0);
                }
                catch (RaftStorageException ex)
                {
                    error = ex.Error;
                }

                await Assert.That(error).IsEqualTo(RaftStorageError.SnapshotTemporarilyUnavailable);
            }
            finally
            {
                await node.DisposeAsync();
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
    public async Task LeaderWithALegacySnapshot_CatchesUpAFollowerOnlyFromANewSnapshot()
    {
        string directory = Path.Combine(Path.GetTempPath(), "raft-node-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            // Node 1 kept a snapshot of the state after index 4 from an earlier version, then committed v4 and v5.
            var voters = new ConfState(new ulong[] { 1, 2 });
            new LegacyStoreFormat.Log()
                .HardState(new HardState(1, 0, 4))
                .Append(
                    new Entry(EntryType.Normal, 1, 5, Encoding.UTF8.GetBytes("v4")),
                    new Entry(EntryType.Normal, 1, 6, Encoding.UTF8.GetBytes("v5")))
                .HardState(new HardState(1, 0, 6))
                .WriteTo(directory);
            LegacyStoreFormat.WriteSnapshotFile(directory, 4, 1, voters, Encoding.UTF8.GetBytes("v0,v1,v2,v3"));

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await using var network = new InMemoryNetwork();
            using var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            RaftNode leader = new(
                new RaftConfig { Id = 1, ElectionTick = 10, HeartbeatTick = 1, RandomizedElectionTimeout = 6 },
                storage,
                network.CreateNode(1),
                new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
            RaftNode follower = new(
                new RaftConfig { Id = 2, ElectionTick = 10, HeartbeatTick = 1, RandomizedElectionTimeout = 30 },
                new MemoryStorage(voters),
                network.CreateNode(2),
                new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
            try
            {
                await leader.StartAsync(timeout.Token);
                await follower.StartAsync(timeout.Token);
                await WaitUntilAsync(() => leader.IsLeader);

                // The follower's log ends before the leader's, which starts after the legacy snapshot: the leader has
                // no snapshot it may send until its application records one.
                await Task.Delay(300, timeout.Token);
                await Assert.That(follower.CommittedItems.TryRead(out _)).IsFalse();

                await leader.CreateSnapshotAsync(6, Encoding.UTF8.GetBytes("v0,v1,v2,v3,v4,v5"), timeout.Token);
                CommittedItem snapshot = await follower.CommittedItems.ReadAsync(timeout.Token);

                await Assert.That(snapshot.Kind).IsEqualTo(CommittedItemKind.Snapshot);
                await Assert.That(snapshot.Index).IsEqualTo(6UL);
                await Assert.That(Encoding.UTF8.GetString(snapshot.Data.ToArray())).IsEqualTo("v0,v1,v2,v3,v4,v5");
            }
            finally
            {
                await follower.DisposeAsync();
                await leader.DisposeAsync();
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
    public async Task Restart_FromALegacySnapshotBehindTheStoredMembership_KeepsTheStoredMembership()
    {
        string directory = Path.Combine(Path.GetTempPath(), "raft-node-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            // An earlier version installed a snapshot at index 1 and appended the membership changes at index 2 to 4.
            // It applied them and stored the membership after index 4 in the snapshot file, but the process stopped
            // before the commit of index 4 was durable.
            new LegacyStoreFormat.Log()
                .HardState(new HardState(1, 0, 1))
                .Append(JointChangeEntries(2))
                .WriteTo(directory);
            LegacyStoreFormat.WriteSnapshotFile(directory, 1, 1, AfterJointChanges, new byte[] { 1 });

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await using var network = new InMemoryNetwork();
            using var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            RaftNode node = CreateNode(storage, network);
            try
            {
                CommittedItem snapshot = await node.CommittedItems.ReadAsync(timeout.Token);
                await Assert.That(snapshot.Kind).IsEqualTo(CommittedItemKind.Snapshot);
                await Assert.That(snapshot.Index).IsEqualTo(1UL);
                await Assert.That(snapshot.Data.ToArray().SequenceEqual(new byte[] { 1 })).IsTrue();
                await Assert.That(node.AppliedIndex).IsEqualTo(1UL);
                await Assert.That(node.Configuration.Equals(AfterJointChanges)).IsTrue();
            }
            finally
            {
                await node.DisposeAsync();
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
    public async Task Restart_AfterABootstrapAtIndexZero_AppliesAMembershipChangeCommittedBeforeTheCrash()
    {
        ConfState recovered = await RestartAfterTheFirstMembershipChangeCommits(bootstrapIndex: 0);
        await Assert.That(recovered.Equals(Changer.Apply(Initial, EnterJointRemove2))).IsTrue();
    }

    [Test]
    public async Task Restart_AfterABootstrapWithoutAnIndex_KeepsTheBootstrapMembership()
    {
        ConfState recovered = await RestartAfterTheFirstMembershipChangeCommits(bootstrapIndex: null);
        await Assert.That(recovered.Equals(Initial)).IsTrue();
    }

    /// <summary>
    /// Bootstraps a fresh store as the documentation shows, commits the first membership change, stops before the node
    /// records the new membership, and returns the membership a restarted node recovers.
    /// </summary>
    private static async Task<ConfState> RestartAfterTheFirstMembershipChangeCommits(ulong? bootstrapIndex)
    {
        string directory = Path.Combine(Path.GetTempPath(), "raft-node-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                if (storage.InitialState().ConfState.Voters.Count == 0)
                {
                    if (bootstrapIndex is { } index)
                    {
                        storage.SetConfState(Initial, index);
                    }
                    else
                    {
                        storage.SetConfState(Initial);
                    }
                }

                storage.Write(
                    new[]
                    {
                        new Entry(EntryType.Normal, 1, 1, default),
                        new Entry(EntryType.ConfChangeV2, 1, 2, EnterJointRemove2.Encode()),
                    },
                    new HardState(1, 1, 2),
                    null);
            }

            await using var network = new InMemoryNetwork();
            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                RaftNode node = CreateNode(reopened, network);
                await node.DisposeAsync();
            }

            using var recovered = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            return recovered.InitialState().ConfState;
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    /// <summary>
    /// Entries that enter a joint configuration removing node 2, leave it, and enter a joint configuration adding
    /// node 3. Re-applying the first two to the configuration after the third changes its outgoing voters.
    /// </summary>
    private static Entry[] JointChangeEntries(ulong first)
    {
        return new[]
        {
            new Entry(EntryType.ConfChangeV2, 1, first, EnterJointRemove2.Encode()),
            new Entry(EntryType.ConfChangeV2, 1, first + 1, Leave.Encode()),
            new Entry(EntryType.ConfChangeV2, 1, first + 2, EnterJointAdd3.Encode()),
        };
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
