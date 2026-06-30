// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Storage;

namespace Raft.Tests;

public sealed class RaftLogTests
{
    [Test]
    public async Task AppendAndCommit_TracksWatermarks()
    {
        var storage = new MemoryStorage();
        var log = new RaftLog(storage);
        Entry[] entries =
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.Normal, 1, 2, default),
        };
        log.Append(entries);

        await Assert.That(log.LastIndex()).IsEqualTo((ulong)2);
        await Assert.That(log.LastTerm()).IsEqualTo((ulong)1);
        await Assert.That(log.Term(2)).IsEqualTo((ulong)1);

        // Entries must be durable (persisted to storage and marked stable) before they may be applied.
        storage.Append(entries);
        log.StableEntriesTo(2, 1);
        log.CommitTo(2);
        await Assert.That(log.Committed).IsEqualTo((ulong)2);

        IReadOnlyList<Entry> next = log.NextEntries(ulong.MaxValue);
        await Assert.That(next.Count).IsEqualTo(2);
        log.AppliedTo(2);
        await Assert.That(log.HasNextEntries()).IsFalse();
    }

    [Test]
    public async Task TryMaybeAppend_MatchingPrefix_AppendsTail()
    {
        var log = new RaftLog(new MemoryStorage());
        log.Append(new[] { new Entry(EntryType.Normal, 1, 1, default) });

        bool ok = log.TryMaybeAppend(
            1,
            1,
            2,
            new[]
            {
                new Entry(EntryType.Normal, 1, 2, default),
                new Entry(EntryType.Normal, 1, 3, default),
            },
            out ulong lastNewIndex);

        await Assert.That(ok).IsTrue();
        await Assert.That(lastNewIndex).IsEqualTo((ulong)3);
        await Assert.That(log.Committed).IsEqualTo((ulong)2);
    }

    [Test]
    public async Task TryMaybeAppend_TermMismatch_Fails()
    {
        var log = new RaftLog(new MemoryStorage());
        log.Append(new[] { new Entry(EntryType.Normal, 1, 1, default) });

        bool ok = log.TryMaybeAppend(1, 5, 1, Array.Empty<Entry>(), out _);
        await Assert.That(ok).IsFalse();
    }

    [Test]
    public async Task FindConflict_DetectsDivergentTerm()
    {
        var log = new RaftLog(new MemoryStorage());
        log.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.Normal, 1, 2, default),
        });

        ulong conflict = log.FindConflict(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.Normal, 2, 2, default),
        });

        await Assert.That(conflict).IsEqualTo((ulong)2);
    }

    [Test]
    public async Task IsUpToDate_ComparesTermThenIndex()
    {
        var log = new RaftLog(new MemoryStorage());
        log.Append(new[] { new Entry(EntryType.Normal, 2, 1, default) });

        await Assert.That(log.IsUpToDate(1, 2)).IsTrue();
        await Assert.That(log.IsUpToDate(5, 1)).IsFalse();
        await Assert.That(log.IsUpToDate(1, 3)).IsTrue();
    }

    [Test]
    public async Task TryRestore_AdoptsSnapshot()
    {
        var log = new RaftLog(new MemoryStorage());
        var snapshot = new Snapshot(new SnapshotMetadata(8, 3, new ConfState(new ulong[] { 1, 2 })), default);

        bool restored = log.TryRestore(snapshot);

        await Assert.That(restored).IsTrue();
        await Assert.That(log.Committed).IsEqualTo((ulong)8);
        await Assert.That(log.LastIndex()).IsEqualTo((ulong)8);
        await Assert.That(log.UnstableSnapshot).IsNotNull();
    }

    [Test]
    public async Task TryMaybeAppend_ConflictWithCommitted_Throws()
    {
        var log = new RaftLog(new MemoryStorage());
        log.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.Normal, 1, 2, default),
        });
        log.CommitTo(2);

        // An entry at index 1 (term 2) conflicts with the already-committed entry there, which is illegal.
        Entry[] conflicting = { new Entry(EntryType.Normal, 2, 1, default) };
        await Assert.That(() => log.TryMaybeAppend(0, 0, 2, conflicting, out _))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task CommitTo_BeyondLastIndex_Throws()
    {
        var log = new RaftLog(new MemoryStorage());
        log.Append(new[] { new Entry(EntryType.Normal, 1, 1, default) });
        await Assert.That(() => log.CommitTo(5)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task AppliedTo_Zero_IsNoOp_AndBeyondCommittedThrows()
    {
        var log = new RaftLog(new MemoryStorage());
        log.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.Normal, 1, 2, default),
        });

        log.AppliedTo(0);
        await Assert.That(log.Applied).IsEqualTo((ulong)0);

        // Applying past the committed watermark is out of range.
        await Assert.That(() => log.AppliedTo(2)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task TryRestore_OlderThanCommitted_ReturnsFalse()
    {
        var log = new RaftLog(new MemoryStorage());
        log.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.Normal, 1, 2, default),
            new Entry(EntryType.Normal, 1, 3, default),
        });
        log.CommitTo(3);

        var stale = new Snapshot(new SnapshotMetadata(2, 1, new ConfState(new ulong[] { 1 })), default);
        await Assert.That(log.TryRestore(stale)).IsFalse();
        await Assert.That(log.Committed).IsEqualTo((ulong)3);
    }

    [Test]
    public async Task TryRestore_MatchingTerm_FastForwardsCommitWithoutRestoring()
    {
        var log = new RaftLog(new MemoryStorage());
        log.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.Normal, 2, 2, default),
            new Entry(EntryType.Normal, 2, 3, default),
        });

        // The snapshot point matches the local log term, so the commit fast-forwards without an unstable restore.
        var snapshot = new Snapshot(new SnapshotMetadata(2, 2, new ConfState(new ulong[] { 1 })), default);
        await Assert.That(log.TryRestore(snapshot)).IsFalse();
        await Assert.That(log.Committed).IsEqualTo((ulong)2);
    }
}
