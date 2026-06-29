// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Storage;

namespace Raft;

/// <summary>
/// The Raft log: a view over persisted <see cref="IRaftStorage"/> plus an in-memory <see cref="Unstable"/> tail,
/// tracking the committed and applied watermarks. Mirrors the raft-rs <c>RaftLog</c>.
/// </summary>
internal sealed class RaftLog
{
    private const ulong NoLimit = ulong.MaxValue;

    private readonly IRaftStorage _store;
    private readonly Unstable _unstable;

    internal RaftLog(IRaftStorage store)
    {
        _store = store;
        ulong lastIndex = store.LastIndex();
        _unstable = new Unstable(lastIndex + 1);
        Committed = store.FirstIndex() - 1;
        Applied = store.FirstIndex() - 1;
    }

    /// <summary>Gets the highest index known to be committed.</summary>
    internal ulong Committed { get; private set; }

    /// <summary>Gets the highest index applied to the state machine.</summary>
    internal ulong Applied { get; private set; }

    internal Snapshot? UnstableSnapshot => _unstable.Snapshot;

    internal IReadOnlyList<Entry> UnstableEntries => _unstable.Entries;

    internal ulong FirstIndex()
    {
        if (_unstable.TryFirstIndex(out ulong index))
        {
            return index;
        }

        return _store.FirstIndex();
    }

    internal ulong LastIndex()
    {
        if (_unstable.TryLastIndex(out ulong index))
        {
            return index;
        }

        return _store.LastIndex();
    }

    internal ulong Term(ulong index)
    {
        ulong dummyIndex = FirstIndex() - 1;
        if (index < dummyIndex || index > LastIndex())
        {
            return 0;
        }

        if (_unstable.TryTerm(index, out ulong term))
        {
            return term;
        }

        try
        {
            return _store.Term(index);
        }
        catch (RaftStorageException ex) when (
            ex.Error is RaftStorageError.Compacted or RaftStorageError.Unavailable)
        {
            return 0;
        }
    }

    internal ulong LastTerm() => Term(LastIndex());

    internal bool MatchTerm(ulong index, ulong term) => Term(index) == term;

    internal bool IsUpToDate(ulong lastIndex, ulong term)
    {
        ulong myTerm = LastTerm();
        return term > myTerm || (term == myTerm && lastIndex >= LastIndex());
    }

    internal ulong FindConflict(IReadOnlyList<Entry> entries)
    {
        foreach (Entry entry in entries)
        {
            if (!MatchTerm(entry.Index, entry.Term))
            {
                return entry.Index;
            }
        }

        return 0;
    }

    /// <summary>Appends leader entries to the unstable tail and returns the new last index.</summary>
    internal ulong Append(IReadOnlyList<Entry> entries)
    {
        if (entries.Count == 0)
        {
            return LastIndex();
        }

        _unstable.TruncateAndAppend(entries);
        return LastIndex();
    }

    /// <summary>Appends follower entries after a matched prefix; returns the new last index or 0 on mismatch.</summary>
    internal bool TryMaybeAppend(
        ulong index,
        ulong logTerm,
        ulong committed,
        IReadOnlyList<Entry> entries,
        out ulong lastNewIndex)
    {
        lastNewIndex = 0;
        if (!MatchTerm(index, logTerm))
        {
            return false;
        }

        ulong lastIndexOfEntries = index + (ulong)entries.Count;
        ulong conflictIndex = FindConflict(entries);
        if (conflictIndex != 0 && conflictIndex <= Committed)
        {
            throw new InvalidOperationException(
                $"entry {conflictIndex} conflicts with committed entry committed={Committed}");
        }

        if (conflictIndex != 0)
        {
            ulong start = conflictIndex - (index + 1);
            var toAppend = new List<Entry>((int)((ulong)entries.Count - start));
            for (ulong i = start; i < (ulong)entries.Count; i++)
            {
                toAppend.Add(entries[(int)i]);
            }

            Append(toAppend);
        }

        lastNewIndex = lastIndexOfEntries;
        CommitTo(Math.Min(committed, lastNewIndex));
        return true;
    }

    internal void CommitTo(ulong commit)
    {
        if (commit <= Committed)
        {
            return;
        }

        if (commit > LastIndex())
        {
            throw new InvalidOperationException(
                $"to-commit {commit} is out of range last index {LastIndex()}");
        }

        Committed = commit;
    }

    internal bool TryMaybeCommit(ulong maxIndex, ulong term)
    {
        if (maxIndex > Committed && Term(maxIndex) == term)
        {
            CommitTo(maxIndex);
            return true;
        }

        return false;
    }

    internal void AppliedTo(ulong index)
    {
        if (index == 0)
        {
            return;
        }

        if (Committed < index || index < Applied)
        {
            throw new InvalidOperationException(
                $"applied {index} is out of range [prev {Applied}, committed {Committed}]");
        }

        Applied = index;
    }

    internal void StableEntriesTo(ulong index, ulong term) => _unstable.StableTo(index, term);

    internal void StableSnapshotTo(ulong index) => _unstable.StableSnapshotTo(index);

    internal IReadOnlyList<Entry> Slice(ulong low, ulong high, ulong maxBytes)
    {
        if (low == high)
        {
            return Array.Empty<Entry>();
        }

        var result = new List<Entry>();
        ulong unstableOffset = _unstable.TryFirstIndex(out _) || _unstable.Entries.Count > 0
            ? UnstableOffset()
            : ulong.MaxValue;

        if (low < unstableOffset)
        {
            ulong storedHigh = Math.Min(high, unstableOffset);
            IReadOnlyList<Entry> stored = _store.Entries(low, storedHigh, maxBytes);
            result.AddRange(stored);
            if ((ulong)stored.Count < storedHigh - low)
            {
                return result;
            }
        }

        if (high > unstableOffset)
        {
            ulong unstableLow = Math.Max(low, unstableOffset);
            result.AddRange(_unstable.Slice(unstableLow, high));
        }

        return result;
    }

    internal IReadOnlyList<Entry> Entries(ulong index, ulong maxBytes)
    {
        ulong last = LastIndex();
        if (index > last)
        {
            return Array.Empty<Entry>();
        }

        return Slice(index, last + 1, maxBytes);
    }

    internal IReadOnlyList<Entry> AllEntries() => Entries(FirstIndex(), NoLimit);

    /// <summary>Returns committed-but-not-yet-applied entries for the state machine to apply.</summary>
    internal IReadOnlyList<Entry> NextEntries(ulong maxBytes)
    {
        ulong offset = Math.Max(Applied + 1, FirstIndex());
        if (Committed + 1 > offset)
        {
            return Slice(offset, Committed + 1, maxBytes);
        }

        return Array.Empty<Entry>();
    }

    internal bool HasNextEntries()
    {
        ulong offset = Math.Max(Applied + 1, FirstIndex());
        return Committed + 1 > offset;
    }

    internal Snapshot SnapshotForRequest(ulong requestIndex)
    {
        if (_unstable.Snapshot is { } snap && snap.Metadata.Index >= requestIndex)
        {
            return snap;
        }

        return _store.Snapshot(requestIndex);
    }

    internal bool TryRestore(Snapshot snapshot)
    {
        ulong index = snapshot.Metadata.Index;
        if (index <= Committed)
        {
            return false;
        }

        if (MatchTerm(index, snapshot.Metadata.Term))
        {
            CommitTo(index);
            return false;
        }

        _unstable.Restore(snapshot);
        Committed = index;
        Applied = index;
        return true;
    }

    private ulong UnstableOffset()
    {
        if (_unstable.Entries.Count > 0)
        {
            return _unstable.Entries[0].Index;
        }

        if (_unstable.TryLastIndex(out ulong last))
        {
            return last + 1;
        }

        return LastIndex() + 1;
    }
}
