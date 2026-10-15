// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using Raft.Configuration;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

/// <summary>
/// A store written against <see cref="IRaftWritableStorage"/> alone, without <see cref="IRaftConfStateIndexStorage"/>,
/// still drives a <see cref="RaftNode"/>: the node persists membership with the one-argument
/// <see cref="IRaftWritableStorage.SetConfState"/> and recovers it without an index.
/// </summary>
public sealed class RaftNodeCustomStorageTests
{
    private static readonly ulong[] Learners = { 2 };

    [Test]
    public async Task WritableOnlyStorage_CommitsAndRestarts()
    {
        var storage = new WritableOnlyStorage(new ConfState(new ulong[] { 1 }));
        await Assert.That(((object)storage) is IRaftConfStateIndexStorage).IsFalse();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await using (var network = new InMemoryNetwork())
        {
            RaftNode node = CreateSingleNode(network, storage);
            try
            {
                await StartLeaderAsync(node, timeout.Token);
                List<string> commands = await CommitWithLearnerAsync(node, timeout.Token);
                await Assert.That(string.Join(",", commands)).IsEqualTo("a,b,c");
                await Assert.That(async () => await node.CreateSnapshotAsync(node.AppliedIndex, new byte[] { 1 }))
                    .Throws<NotSupportedException>();
            }
            finally
            {
                await node.DisposeAsync();
            }
        }

        await Assert.That(storage.SetConfStateCalls).IsEqualTo(1);
        await Assert.That(storage.InitialState().ConfState.Learners.SequenceEqual(Learners)).IsTrue();

        await using (var network = new InMemoryNetwork())
        {
            RaftNode restarted = CreateSingleNode(network, storage);
            try
            {
                await Assert.That(restarted.Configuration.Learners.SequenceEqual(Learners)).IsTrue();

                // The commands are applied again once their commit is known: from the stored hard state, or after the
                // restarted node is elected again.
                await StartLeaderAsync(restarted, timeout.Token);
                await Assert.That(await ReadCommandsAsync(restarted, 3, timeout.Token)).IsEqualTo("a,b,c");
                await restarted.ProposeAsync(Encoding.UTF8.GetBytes("d"), timeout.Token);
                await Assert.That(await ReadCommandsAsync(restarted, 1, timeout.Token)).IsEqualTo("d");
                await Assert.That(restarted.Configuration.Learners.SequenceEqual(Learners)).IsTrue();
            }
            finally
            {
                await restarted.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task SnapshotStorageWithoutConfStateIndex_CommitsSnapshotsAndRestarts()
    {
        var storage = new SnapshotStorageWithoutConfStateIndex(new ConfState(new ulong[] { 1 }));
        await Assert.That(((object)storage) is IRaftConfStateIndexStorage).IsFalse();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        ulong snapshotIndex;

        await using (var network = new InMemoryNetwork())
        {
            RaftNode node = CreateSingleNode(network, storage);
            try
            {
                await StartLeaderAsync(node, timeout.Token);
                List<string> commands = await CommitWithLearnerAsync(node, timeout.Token);
                snapshotIndex = node.AppliedIndex;
                SnapshotMetadata metadata = await node.CreateSnapshotAsync(
                    snapshotIndex, Encoding.UTF8.GetBytes(string.Join(",", commands)), timeout.Token);
                await Assert.That(metadata.ConfState.Learners.SequenceEqual(Learners)).IsTrue();
                await node.CompactAsync(snapshotIndex, timeout.Token);

                await node.ProposeAsync(Encoding.UTF8.GetBytes("d"), timeout.Token);
                await Assert.That(await ReadCommandsAsync(node, 1, timeout.Token)).IsEqualTo("d");
            }
            finally
            {
                await node.DisposeAsync();
            }
        }

        await Assert.That(storage.SetConfStateCalls).IsEqualTo(1);
        await Assert.That(storage.FirstIndex()).IsEqualTo(snapshotIndex + 1);

        await using (var network = new InMemoryNetwork())
        {
            RaftNode restarted = CreateSingleNode(network, storage);
            try
            {
                await Assert.That(restarted.Configuration.Learners.SequenceEqual(Learners)).IsTrue();
                CommittedItem snapshot = await restarted.CommittedItems.ReadAsync(timeout.Token);
                await Assert.That(snapshot.Kind).IsEqualTo(CommittedItemKind.Snapshot);
                await Assert.That(snapshot.Index).IsEqualTo(snapshotIndex);
                await Assert.That(Encoding.UTF8.GetString(snapshot.Data.ToArray())).IsEqualTo("a,b,c");

                await StartLeaderAsync(restarted, timeout.Token);
                await Assert.That(await ReadCommandsAsync(restarted, 1, timeout.Token)).IsEqualTo("d");
                await restarted.ProposeAsync(Encoding.UTF8.GetBytes("e"), timeout.Token);
                await Assert.That(await ReadCommandsAsync(restarted, 1, timeout.Token)).IsEqualTo("e");
                SnapshotMetadata next = await restarted.CreateSnapshotAsync(
                    restarted.AppliedIndex, Encoding.UTF8.GetBytes("a,b,c,d,e"), timeout.Token);
                await Assert.That(next.ConfState.Learners.SequenceEqual(Learners)).IsTrue();
            }
            finally
            {
                await restarted.DisposeAsync();
            }
        }
    }

    /// <summary>Commits a, b, a configuration change adding learner 2, then c; returns the commands read.</summary>
    private static async Task<List<string>> CommitWithLearnerAsync(RaftNode node, CancellationToken ct)
    {
        await node.ProposeAsync(Encoding.UTF8.GetBytes("a"), ct);
        await node.ProposeAsync(Encoding.UTF8.GetBytes("b"), ct);
        await node.ChangeConfigurationAsync(ConfChangeV2.Single(ConfChangeType.AddLearnerNode, 2), ct);
        await node.ProposeAsync(Encoding.UTF8.GetBytes("c"), ct);
        var commands = new List<string>();
        while (commands.Count < 3)
        {
            CommittedItem item = await node.CommittedItems.ReadAsync(ct);
            commands.Add(Encoding.UTF8.GetString(item.Data.ToArray()));
        }

        while (!node.Configuration.Learners.SequenceEqual(Learners))
        {
            await Task.Delay(10, ct);
        }

        return commands;
    }

    private static async Task<string> ReadCommandsAsync(RaftNode node, int count, CancellationToken ct)
    {
        var commands = new List<string>();
        while (commands.Count < count)
        {
            CommittedItem item = await node.CommittedItems.ReadAsync(ct);
            commands.Add(Encoding.UTF8.GetString(item.Data.ToArray()));
        }

        return string.Join(",", commands);
    }

    private static RaftNode CreateSingleNode(InMemoryNetwork network, IRaftWritableStorage storage)
    {
        return new RaftNode(
            new RaftConfig { Id = 1, RandomizedElectionTimeout = 5 },
            storage,
            network.CreateNode(1),
            new RaftNodeOptions { TickInterval = TimeSpan.FromMilliseconds(10) });
    }

    private static async Task StartLeaderAsync(RaftNode node, CancellationToken ct)
    {
        await node.StartAsync(ct);
        await node.CampaignAsync(ct);
        while (!node.IsLeader)
        {
            await Task.Delay(10, ct);
        }
    }

    /// <summary>A third-party store that implements <see cref="IRaftWritableStorage"/> only.</summary>
    private class WritableOnlyStorage : IRaftWritableStorage
    {
        private int _setConfStateCalls;

        public WritableOnlyStorage(ConfState initialConfState)
        {
            Inner = new MemoryStorage(initialConfState);
        }

        public int SetConfStateCalls => Volatile.Read(ref _setConfStateCalls);

        protected MemoryStorage Inner { get; }

        public (HardState HardState, ConfState ConfState) InitialState() => Inner.InitialState();

        public IReadOnlyList<Entry> Entries(ulong low, ulong high, ulong maxBytes) => Inner.Entries(low, high, maxBytes);

        public ulong Term(ulong index) => Inner.Term(index);

        public ulong FirstIndex() => Inner.FirstIndex();

        public ulong LastIndex() => Inner.LastIndex();

        public Snapshot Snapshot(ulong requestIndex) => Inner.Snapshot(requestIndex);

        public void Append(IReadOnlyList<Entry> entries) => Inner.Append(entries);

        public void ApplySnapshot(Snapshot snapshot) => Inner.ApplySnapshot(snapshot);

        public void Compact(ulong compactIndex) => Inner.Compact(compactIndex);

        public void SetHardState(HardState hardState) => Inner.SetHardState(hardState);

        public void Write(IReadOnlyList<Entry> entries, HardState? hardState, Snapshot? snapshot) =>
            Inner.Write(entries, hardState, snapshot);

        public void SetConfState(ConfState confState)
        {
            Interlocked.Increment(ref _setConfStateCalls);
            Inner.SetConfState(confState);
        }
    }

    /// <summary>A third-party store that keeps snapshots but does not record the index of its configuration.</summary>
    private sealed class SnapshotStorageWithoutConfStateIndex : WritableOnlyStorage, IRaftSnapshotStorage
    {
        public SnapshotStorageWithoutConfStateIndex(ConfState initialConfState)
            : base(initialConfState)
        {
        }

        public Snapshot CreateSnapshot(ulong index, ConfState confState, ReadOnlyMemory<byte> data) =>
            Inner.CreateSnapshot(index, confState, data);

        public Snapshot? LatestSnapshot() => Inner.LatestSnapshot();
    }
}
