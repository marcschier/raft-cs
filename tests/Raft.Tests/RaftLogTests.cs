// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Storage;

namespace Raft.Tests;

public sealed class RaftLogTests
{
    [Test]
    public async Task AppendAndCommit_TracksWatermarks()
    {
        var log = new RaftLog(new MemoryStorage());
        log.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.Normal, 1, 2, default),
        });

        await Assert.That(log.LastIndex()).IsEqualTo((ulong)2);
        await Assert.That(log.LastTerm()).IsEqualTo((ulong)1);
        await Assert.That(log.Term(2)).IsEqualTo((ulong)1);

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
}
