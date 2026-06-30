// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Storage;

namespace Raft.Tests;

public sealed class MemoryStorageTests
{
    [Test]
    public async Task AppendAndRead_RoundTrips()
    {
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        storage.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, new byte[] { 1 }),
            new Entry(EntryType.Normal, 1, 2, new byte[] { 2, 2 }),
            new Entry(EntryType.Normal, 2, 3, new byte[] { 3 }),
        });

        await Assert.That(storage.FirstIndex()).IsEqualTo((ulong)1);
        await Assert.That(storage.LastIndex()).IsEqualTo((ulong)3);
        await Assert.That(storage.Term(2)).IsEqualTo((ulong)1);
        await Assert.That(storage.Term(3)).IsEqualTo((ulong)2);
        await Assert.That(storage.Entries(1, 4, ulong.MaxValue).Count).IsEqualTo(3);
    }

    [Test]
    public async Task Append_TruncatesConflictingSuffix()
    {
        var storage = new MemoryStorage();
        storage.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.Normal, 1, 2, default),
            new Entry(EntryType.Normal, 1, 3, default),
        });
        storage.Append(new[] { new Entry(EntryType.Normal, 2, 2, default) });

        await Assert.That(storage.LastIndex()).IsEqualTo((ulong)2);
        await Assert.That(storage.Term(2)).IsEqualTo((ulong)2);
    }

    [Test]
    public async Task Compact_DropsPrefixAndRejectsOldReads()
    {
        var storage = new MemoryStorage();
        storage.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.Normal, 1, 2, default),
            new Entry(EntryType.Normal, 1, 3, default),
        });

        storage.Compact(2);

        await Assert.That(storage.FirstIndex()).IsEqualTo((ulong)3);
        await Assert.That(() => storage.Entries(1, 2, ulong.MaxValue))
            .Throws<RaftStorageException>();
    }

    [Test]
    public async Task Term_BelowCompactedBoundary_Throws()
    {
        var storage = new MemoryStorage();
        storage.Append(new[] { new Entry(EntryType.Normal, 1, 1, default) });
        await Assert.That(() => storage.Term(5)).Throws<RaftStorageException>();
    }

    [Test]
    public async Task ApplySnapshot_ResetsLogAndState()
    {
        var storage = new MemoryStorage();
        storage.Append(new[] { new Entry(EntryType.Normal, 1, 1, default) });
        var confState = new ConfState(new ulong[] { 1, 2, 3 });
        storage.ApplySnapshot(new Snapshot(new SnapshotMetadata(5, 3, confState), default));

        await Assert.That(storage.FirstIndex()).IsEqualTo((ulong)6);
        await Assert.That(storage.LastIndex()).IsEqualTo((ulong)5);
        await Assert.That(storage.Term(5)).IsEqualTo((ulong)3);
        (HardState hardState, ConfState restored) = storage.InitialState();
        await Assert.That(hardState.Commit).IsEqualTo((ulong)5);
        await Assert.That(string.Join(",", restored.Voters)).IsEqualTo("1,2,3");
    }

    [Test]
    public async Task ApplySnapshot_OlderThanCurrent_Throws()
    {
        var storage = new MemoryStorage();
        storage.ApplySnapshot(new Snapshot(new SnapshotMetadata(5, 3, new ConfState()), default));
        var older = new Snapshot(new SnapshotMetadata(3, 2, new ConfState()), default);
        await Assert.That(() => storage.ApplySnapshot(older)).Throws<RaftStorageException>();
    }

    [Test]
    public async Task Compact_OutOfRange_Throws()
    {
        var storage = new MemoryStorage();
        storage.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.Normal, 1, 2, default),
        });

        // At or below the compacted boundary.
        await Assert.That(() => storage.Compact(0)).Throws<RaftStorageException>();

        // Past the last index.
        await Assert.That(() => storage.Compact(5)).Throws<RaftStorageException>();
    }

    [Test]
    public async Task Entries_HighBeyondLastIndex_Throws()
    {
        var storage = new MemoryStorage();
        storage.Append(new[] { new Entry(EntryType.Normal, 1, 1, default) });
        await Assert.That(() => storage.Entries(1, 5, ulong.MaxValue)).Throws<RaftStorageException>();
    }

    [Test]
    public async Task Entries_MaxBytes_StopsAfterFirstOverBudget()
    {
        var storage = new MemoryStorage();
        storage.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, new byte[] { 1, 2, 3 }),
            new Entry(EntryType.Normal, 1, 2, new byte[] { 4, 5, 6 }),
            new Entry(EntryType.Normal, 1, 3, new byte[] { 7, 8, 9 }),
        });

        // The first entry is always returned; the second would exceed the 4-byte budget, so iteration stops.
        await Assert.That(storage.Entries(1, 4, 4).Count).IsEqualTo(1);
        await Assert.That(storage.Entries(1, 4, ulong.MaxValue).Count).IsEqualTo(3);
    }

    [Test]
    public async Task Snapshot_RequestBeyondCommitted_Throws()
    {
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        storage.Append(new[] { new Entry(EntryType.Normal, 1, 1, default) });

        // No hard state => committed index 0, so index 1 is not yet snapshottable.
        await Assert.That(() => storage.Snapshot(1)).Throws<RaftStorageException>();
    }

    [Test]
    public async Task Snapshot_AtCommittedIndex_ReturnsMetadata()
    {
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1, 2 }));
        storage.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.Normal, 2, 2, default),
        });
        storage.SetHardState(new HardState(2, 0, 2));

        Snapshot snapshot = storage.Snapshot(1);
        await Assert.That(snapshot.Metadata.Index).IsEqualTo((ulong)2);
        await Assert.That(snapshot.Metadata.Term).IsEqualTo((ulong)2);
        await Assert.That(string.Join(",", snapshot.Metadata.ConfState.Voters)).IsEqualTo("1,2");
    }
}
