// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Storage;

namespace Raft;

/// <summary>
/// The tail of the Raft log that has not yet been persisted to <see cref="IRaftStorage"/>: entries appended since the
/// last stable point and an optional snapshot pending application. Mirrors the raft-rs <c>Unstable</c>.
/// </summary>
internal sealed class Unstable
{
    private readonly List<Entry> _entries = new();

    internal Unstable(ulong offset)
    {
        Offset = offset;
    }

    /// <summary>Gets the index of the first entry in <see cref="Entries"/>.</summary>
    internal ulong Offset { get; private set; }

    /// <summary>Gets the snapshot pending application, if any.</summary>
    internal Snapshot? Snapshot { get; private set; }

    /// <summary>Gets the unstable entries.</summary>
    internal IReadOnlyList<Entry> Entries => _entries;

    internal bool TryFirstIndex(out ulong index)
    {
        if (Snapshot is not null)
        {
            index = Snapshot.Metadata.Index + 1;
            return true;
        }

        index = 0;
        return false;
    }

    internal bool TryLastIndex(out ulong index)
    {
        if (_entries.Count > 0)
        {
            index = Offset + (ulong)_entries.Count - 1;
            return true;
        }

        if (Snapshot is not null)
        {
            index = Snapshot.Metadata.Index;
            return true;
        }

        index = 0;
        return false;
    }

    internal bool TryTerm(ulong index, out ulong term)
    {
        if (index < Offset)
        {
            if (Snapshot is not null && Snapshot.Metadata.Index == index)
            {
                term = Snapshot.Metadata.Term;
                return true;
            }

            term = 0;
            return false;
        }

        if (TryLastIndex(out ulong last) && index <= last && _entries.Count > 0)
        {
            term = _entries[(int)(index - Offset)].Term;
            return true;
        }

        term = 0;
        return false;
    }

    internal void StableTo(ulong index, ulong term)
    {
        if (!TryTerm(index, out ulong t) || t != term || index < Offset)
        {
            return;
        }

        int removeCount = (int)(index + 1 - Offset);
        _entries.RemoveRange(0, removeCount);
        Offset = index + 1;
    }

    internal void StableSnapshotTo(ulong index)
    {
        if (Snapshot is not null && Snapshot.Metadata.Index == index)
        {
            Snapshot = null;
        }
    }

    internal void Restore(Snapshot snapshot)
    {
        _entries.Clear();
        Offset = snapshot.Metadata.Index + 1;
        Snapshot = snapshot;
    }

    internal void TruncateAndAppend(IReadOnlyList<Entry> entries)
    {
        if (entries.Count == 0)
        {
            return;
        }

        ulong after = entries[0].Index;
        if (after == Offset + (ulong)_entries.Count)
        {
            _entries.AddRange(entries);
        }
        else if (after <= Offset)
        {
            Offset = after;
            _entries.Clear();
            _entries.AddRange(entries);
        }
        else
        {
            int keep = (int)(after - Offset);
            if (keep < _entries.Count)
            {
                _entries.RemoveRange(keep, _entries.Count - keep);
            }

            _entries.AddRange(entries);
        }
    }

    internal IReadOnlyList<Entry> Slice(ulong low, ulong high)
    {
        int start = (int)(low - Offset);
        int end = (int)(high - Offset);
        var result = new List<Entry>(Math.Max(0, end - start));
        for (int i = start; i < end; i++)
        {
            result.Add(_entries[i]);
        }

        return result;
    }
}
