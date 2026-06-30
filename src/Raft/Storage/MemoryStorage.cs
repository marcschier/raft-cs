// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;

namespace Raft.Storage;

/// <summary>
/// A volatile, thread-safe <see cref="IRaftStorage"/> kept entirely in memory. It mirrors the raft-rs
/// <c>MemStorage</c>: a sentinel entry at index 0 of the buffer represents the compacted snapshot boundary.
/// </summary>
public sealed class MemoryStorage : IRaftWritableStorage
{
    private readonly object _gate = new();
    private readonly List<Entry> _entries = new() { default };
    private HardState _hardState;
    private SnapshotMetadata _snapshotMetadata;

    /// <summary>Initializes a new instance of the <see cref="MemoryStorage"/> class with no initial members.</summary>
    public MemoryStorage()
        : this(new ConfState())
    {
    }

    /// <summary>Initializes a new instance of the <see cref="MemoryStorage"/> class.</summary>
    /// <param name="initialConfState">The bootstrap cluster membership.</param>
    public MemoryStorage(ConfState initialConfState)
    {
        _snapshotMetadata = new SnapshotMetadata(0, 0, initialConfState ?? new ConfState());
    }

    /// <inheritdoc/>
    public (HardState HardState, ConfState ConfState) InitialState()
    {
        lock (_gate)
        {
            return (_hardState, _snapshotMetadata.ConfState);
        }
    }

    /// <summary>Overwrites the persisted hard state.</summary>
    /// <param name="hardState">The hard state to store.</param>
    public void SetHardState(HardState hardState)
    {
        lock (_gate)
        {
            _hardState = hardState;
        }
    }

    /// <summary>Overwrites the persisted committed configuration.</summary>
    /// <param name="confState">The configuration to store.</param>
    public void SetConfState(ConfState confState)
    {
        lock (_gate)
        {
            _snapshotMetadata = new SnapshotMetadata(
                _snapshotMetadata.Index,
                _snapshotMetadata.Term,
                confState ?? new ConfState());
        }
    }

    /// <inheritdoc/>
    public IReadOnlyList<Entry> Entries(ulong low, ulong high, ulong maxBytes)
    {
        lock (_gate)
        {
            ulong offset = _entries[0].Index;
            if (low <= offset)
            {
                throw new RaftStorageException(RaftStorageError.Compacted);
            }

            ulong lastIndex = LastIndexLocked();
            if (high > lastIndex + 1)
            {
                throw new RaftStorageException(
                    RaftStorageError.Other,
                    $"entries' high {high} is out of bound last index {lastIndex}");
            }

            int start = (int)(low - offset);
            int end = (int)(high - offset);
            var result = new List<Entry>(Math.Max(0, end - start));
            ulong total = 0;
            for (int i = start; i < end; i++)
            {
                Entry entry = _entries[i];
                ulong size = (ulong)entry.Data.Length;
                if (result.Count > 0 && total + size > maxBytes)
                {
                    break;
                }

                total += size;
                result.Add(entry);
            }

            return result;
        }
    }

    /// <inheritdoc/>
    public ulong Term(ulong index)
    {
        lock (_gate)
        {
            ulong offset = _entries[0].Index;
            if (index < offset)
            {
                throw new RaftStorageException(RaftStorageError.Compacted);
            }

            int i = (int)(index - offset);
            if (i >= _entries.Count)
            {
                throw new RaftStorageException(RaftStorageError.Unavailable);
            }

            return _entries[i].Term;
        }
    }

    /// <inheritdoc/>
    public ulong FirstIndex()
    {
        lock (_gate)
        {
            return _entries[0].Index + 1;
        }
    }

    /// <inheritdoc/>
    public ulong LastIndex()
    {
        lock (_gate)
        {
            return LastIndexLocked();
        }
    }

    /// <inheritdoc/>
    public Snapshot Snapshot(ulong requestIndex)
    {
        lock (_gate)
        {
            ulong appliedIndex = _hardState.Commit;
            if (requestIndex > appliedIndex)
            {
                throw new RaftStorageException(RaftStorageError.SnapshotTemporarilyUnavailable);
            }

            ulong term = _entries[(int)(appliedIndex - _entries[0].Index)].Term;
            var metadata = new SnapshotMetadata(appliedIndex, term, _snapshotMetadata.ConfState);
            return new Snapshot(metadata, ReadOnlyMemory<byte>.Empty);
        }
    }

    /// <summary>Appends entries to the log, truncating any conflicting suffix.</summary>
    /// <param name="entries">The contiguous entries to append.</param>
    public void Append(IReadOnlyList<Entry> entries)
    {
        Internal.Check.NotNull(entries);

        if (entries.Count == 0)
        {
            return;
        }

        lock (_gate)
        {
            ulong first = _entries[0].Index + 1;
            ulong last = entries[entries.Count - 1].Index;
            if (last < first)
            {
                return;
            }

            int skip = first > entries[0].Index ? (int)(first - entries[0].Index) : 0;
            ulong appendStart = entries[skip].Index;
            int truncateAt = (int)(appendStart - _entries[0].Index);
            if (truncateAt < _entries.Count)
            {
                _entries.RemoveRange(truncateAt, _entries.Count - truncateAt);
            }

            for (int i = skip; i < entries.Count; i++)
            {
                _entries.Add(entries[i]);
            }
        }
    }

    /// <summary>Installs a snapshot, discarding the log it supersedes.</summary>
    /// <param name="snapshot">The snapshot to apply.</param>
    public void ApplySnapshot(Snapshot snapshot)
    {
        Internal.Check.NotNull(snapshot);

        lock (_gate)
        {
            ulong index = snapshot.Metadata.Index;
            if (index <= _snapshotMetadata.Index)
            {
                throw new RaftStorageException(RaftStorageError.SnapshotOutOfDate);
            }

            _snapshotMetadata = snapshot.Metadata;
            _entries.Clear();
            _entries.Add(new Entry(EntryType.Normal, snapshot.Metadata.Term, snapshot.Metadata.Index, default));
            _hardState = new HardState(
                Math.Max(_hardState.Term, snapshot.Metadata.Term),
                _hardState.Vote,
                index);
        }
    }

    /// <summary>Compacts the in-memory log up to (and excluding) <paramref name="compactIndex"/>.</summary>
    /// <param name="compactIndex">The first index to retain.</param>
    public void Compact(ulong compactIndex)
    {
        lock (_gate)
        {
            ulong offset = _entries[0].Index;
            if (compactIndex <= offset)
            {
                throw new RaftStorageException(RaftStorageError.Compacted);
            }

            if (compactIndex > LastIndexLocked())
            {
                throw new RaftStorageException(RaftStorageError.Unavailable);
            }

            int cut = (int)(compactIndex - offset);
            Entry boundary = _entries[cut];
            _entries.RemoveRange(0, cut);
            _entries[0] = new Entry(EntryType.Normal, boundary.Term, boundary.Index, default);
        }
    }

    /// <summary>
    /// Atomically persists a snapshot, a batch of entries, and the hard state. For the volatile in-memory store this
    /// simply forwards to the granular methods in snapshot/entries/hard-state order.
    /// </summary>
    /// <param name="entries">The entries to append (may be empty).</param>
    /// <param name="hardState">The hard state to persist, or <see langword="null"/> when unchanged.</param>
    /// <param name="snapshot">The snapshot to install, or <see langword="null"/> when none.</param>
    public void Write(IReadOnlyList<Entry> entries, HardState? hardState, Snapshot? snapshot)
    {
        if (snapshot is not null)
        {
            ApplySnapshot(snapshot);
        }

        if (entries is { Count: > 0 })
        {
            Append(entries);
        }

        if (hardState is { } hs)
        {
            SetHardState(hs);
        }
    }

    private ulong LastIndexLocked() => _entries[0].Index + (ulong)_entries.Count - 1;
}
