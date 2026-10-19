// Copyright (c) marcschier. Licensed under the MIT License.

using System.Buffers.Binary;
using System.Runtime.InteropServices;
#if NET8_0_OR_GREATER
using System.Runtime.Versioning;
#endif
using System.Security.AccessControl;
using Raft.Configuration;

namespace Raft.Storage.File;

/// <summary>A simple crash-safe file-backed implementation of <see cref="IRaftStorage"/>.</summary>
public sealed class FileRaftStorage : IRaftSnapshotStorage, IRaftConfStateIndexStorage, IDisposable
{
    private const string LogFileName = "log";
    private const string CompactingLogFileName = "log.compact";
    private const string HardStateFileName = "hardstate";
    private const string SnapshotFileName = "snapshot";
    private const string NewSnapshotFileName = "snapshot.new";
    private const string NewLogFileName = "log.new";
    private const byte EntryRecord = 0;
    private const byte TruncateRecord = 1;
    private const byte CompactRecord = 2;
    private const byte HardStateRecord = 3;
    private const byte BaseRecord = 4;
    private const byte ConfStateIndexRecord = 5;
    private const byte LogGenerationRecord = 6;
    private const byte ConfStateRecord = 7;
    private const int LengthPrefixSize = 4;
    private const int EntryHeaderSize = 1 + 1 + 8 + 8 + 4;
    private const int HardStateRecordSize = 1 + (3 * 8);
    private const int BaseRecordSize = 1 + (2 * 8);
    private const int UInt64Size = 8;

    // A compaction index no log reaches. Earlier readers fail on a compaction record with it; this store skips it.
    private const ulong RollbackFenceIndex = ulong.MaxValue;

    // Ends a snapshot file whose membership is the one at the snapshot's index. Earlier versions wrote no marker and
    // rewrote the snapshot file with the latest membership on every SetConfState, keeping the snapshot's index.
    private static readonly byte[] PointInTimeMembershipMarker =
    {
        (byte)'R', (byte)'S', (byte)'P', (byte)'M', 0, 0, 0, 1,
    };

    // Ends a snapshot file that, beyond the membership at its index, records before the marker the generation of the
    // log that continues it. The snapshot supersedes every log of an earlier generation.
    private static readonly byte[] LogGenerationMarker =
    {
        (byte)'R', (byte)'S', (byte)'P', (byte)'M', 0, 0, 0, 2,
    };

    private readonly object _gate = new();
    private readonly string _logPath;
    private readonly string _compactingLogPath;
    private readonly string _hardStatePath;
    private readonly string _snapshotPath;
    private readonly string _newSnapshotPath;
    private readonly string _newLogPath;
    private readonly string _directoryPath;
    private readonly bool _fsync;
    private readonly Action<string> _syncDirectory;
    private readonly StatxCall? _statx;
    private readonly AccessControlCopy? _accessControl;
    private readonly List<Entry> _entries = new() { default };
    private FileStream _logStream;
    private HardState _hardState;
    private Snapshot _snapshot = new(new SnapshotMetadata(0, 0, new ConfState()), ReadOnlyMemory<byte>.Empty);
    private bool _logWritable;
    private bool _replacementNeedsDirectorySync;
    private bool _expectsLegacyReaderRejectionRecord;
    private ConfState _confState = new();
    private ulong? _confStateIndex = 0;
    private ulong? _pendingConfStateIndex;

    // The generation of the log file: zero for a log written before generations were recorded.
    private ulong _logGeneration;

    // The generation of the log that continues the snapshot, or null when the snapshot file records none.
    private ulong? _snapshotGeneration;

    // Whether the log holds a rollback fence, which precedes its first configuration record.
    private bool _logHasRollbackFence;
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="FileRaftStorage"/> class.</summary>
    /// <param name="options">The storage options.</param>
    public FileRaftStorage(FileRaftStorageOptions options)
        : this(options, SyncDirectoryDurably)
    {
    }

    internal FileRaftStorage(
        FileRaftStorageOptions options,
        Action<string> syncDirectory,
        StatxCall? statx = null,
        AccessControlCopy? accessControl = null)
    {
        ThrowIfNull(options);
        ThrowIfNull(syncDirectory);

        // Resolve once: a later change of the working directory must not redirect a relative store's files.
        string directory = Path.GetFullPath(options.DirectoryPath);
        Directory.CreateDirectory(directory);
        _directoryPath = directory;
        _fsync = options.Fsync;
        _syncDirectory = syncDirectory;
        _statx = statx;
        _accessControl = accessControl;
        _logPath = Path.Combine(directory, LogFileName);
        _compactingLogPath = Path.Combine(directory, CompactingLogFileName);
        _hardStatePath = Path.Combine(directory, HardStateFileName);
        _snapshotPath = Path.Combine(directory, SnapshotFileName);
        _newLogPath = Path.Combine(directory, NewLogFileName);
        _newSnapshotPath = Path.Combine(directory, NewSnapshotFileName);

        // A compacted log that was still being written when the process stopped is incomplete; the log it was to
        // replace is intact.
        System.IO.File.Delete(_compactingLogPath);

        // A snapshot or log that was still being written beside the current one when the process stopped was never
        // published; the current one is intact.
        System.IO.File.Delete(_newSnapshotPath);
        System.IO.File.Delete(_newLogPath);

        LoadSnapshot();
        LoadHardState();
        ulong logGeneration = ReadLogGeneration();
        _logGeneration = logGeneration;
        if (_snapshotGeneration is { } snapshotGeneration && logGeneration != snapshotGeneration)
        {
            if (logGeneration > snapshotGeneration)
            {
                throw new InvalidDataException(
                    $"The log file '{_logPath}' continues a newer snapshot than the one in '{_snapshotPath}'.");
            }

            // The snapshot was installed, but the process stopped before the log it supersedes was replaced. Of that
            // log only the hard state (term and vote) still holds; the installation is completed.
            ReplayLog(hardStateOnly: true);
            _logStream = OpenLog();
            InitializeLogWrites();
            try
            {
                InstallSnapshot(_snapshot, snapshotGeneration);
            }
            catch
            {
                _logStream.Dispose();
                throw;
            }

            return;
        }

        long intactLength = ReplayLog(hardStateOnly: false);
        _logStream = OpenLog();
        InitializeLogWrites();
        if (_logStream.Length > intactLength)
        {
            // The bytes after the intact log are the torn last write: an incomplete record, or an index record whose
            // configuration record is missing. Drop them, so the next record starts where a replay reads it and an
            // index record is not paired with a configuration later written without an index.
            try
            {
                _logStream.SetLength(intactLength);
                FlushLog();
                _logStream.Seek(0, SeekOrigin.End);
            }
            catch
            {
                _logStream.Dispose();
                throw;
            }
        }
    }

    private void InitializeLogWrites()
    {
        if (_fsync)
        {
            // A previous replacement may still need its directory entry made durable before writes are acknowledged.
            try
            {
                SyncLogDirectory();
            }
            catch
            {
                _logStream.Dispose();
                throw;
            }
        }

        _logWritable = true;
    }

    /// <inheritdoc/>
    public (HardState HardState, ConfState ConfState) InitialState()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            return (_hardState, _confState);
        }
    }

    /// <inheritdoc/>
    public IReadOnlyList<Entry> Entries(ulong low, ulong high, ulong maxBytes)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
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
            ThrowIfDisposed();
            return TermLocked(index);
        }
    }

    /// <inheritdoc/>
    public ulong FirstIndex()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            return _entries[0].Index + 1;
        }
    }

    /// <inheritdoc/>
    public ulong LastIndex()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            return LastIndexLocked();
        }
    }

    /// <inheritdoc/>
    public Snapshot Snapshot(ulong requestIndex)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (!_snapshot.IsEmpty)
            {
                // A snapshot kept from an earlier version holds the application state but not the membership at its
                // index, and the log before it is gone: a follower can be caught up only from a newer snapshot.
                if (!_snapshot.MembershipIsAtIndex)
                {
                    throw new RaftStorageException(RaftStorageError.SnapshotTemporarilyUnavailable);
                }

                if (requestIndex <= _snapshot.Metadata.Index)
                {
                    return _snapshot;
                }

                // A snapshot without the application state would skip the commands after this one.
                throw new RaftStorageException(RaftStorageError.SnapshotTemporarilyUnavailable);
            }

            // Without an application snapshot, the store has no application state to send: only the position.
            ulong appliedIndex = _hardState.Commit;
            if (requestIndex > appliedIndex)
            {
                throw new RaftStorageException(RaftStorageError.SnapshotTemporarilyUnavailable);
            }

            ulong term = TermLocked(appliedIndex);
            var metadata = new SnapshotMetadata(appliedIndex, term, _confState);
            return new Snapshot(metadata, ReadOnlyMemory<byte>.Empty);
        }
    }

    /// <summary>Appends entries to the log, truncating any conflicting suffix.</summary>
    /// <param name="entries">The contiguous entries to append.</param>
    public void Append(IReadOnlyList<Entry> entries)
    {
        ThrowIfNull(entries);

        if (entries.Count == 0)
        {
            return;
        }

        lock (_gate)
        {
            ThrowIfDisposed();
            EnsureLogWritable();
            if (AppendToLogLocked(entries))
            {
                FlushLog();
            }
        }
    }

    /// <summary>
    /// Atomically appends entries, persists the (changed) hard state, and installs any snapshot with a single
    /// durability sync covering the log. The common path (entries plus hard state, no snapshot) costs one fsync.
    /// </summary>
    /// <param name="entries">The entries to append (may be empty).</param>
    /// <param name="hardState">The hard state to persist, or <see langword="null"/> when unchanged.</param>
    /// <param name="snapshot">The snapshot to install, or <see langword="null"/> when none.</param>
    public void Write(IReadOnlyList<Entry> entries, HardState? hardState, Snapshot? snapshot)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            EnsureLogWritable();

            if (snapshot is not null)
            {
                ApplySnapshotLocked(snapshot);
            }

            bool wroteLog = false;
            if (entries is { Count: > 0 } && AppendToLogLocked(entries))
            {
                wroteLog = true;
            }

            if (hardState is { } hs)
            {
                _hardState = hs;
                WriteHardStateRecord(hs);
                wroteLog = true;
            }

            if (wroteLog)
            {
                FlushLog();
            }
        }
    }

    private bool AppendToLogLocked(IReadOnlyList<Entry> entries)
    {
        ulong first = _entries[0].Index + 1;
        ulong last = entries[entries.Count - 1].Index;
        if (last < first)
        {
            return false;
        }

        int skip = first > entries[0].Index ? (int)(first - entries[0].Index) : 0;
        ulong appendStart = entries[skip].Index;
        int truncateAt = (int)(appendStart - _entries[0].Index);
        WriteTruncateRecord(appendStart);
        for (int i = skip; i < entries.Count; i++)
        {
            WriteEntryRecord(entries[i]);
        }

        if (truncateAt < _entries.Count)
        {
            _entries.RemoveRange(truncateAt, _entries.Count - truncateAt);
        }

        for (int i = skip; i < entries.Count; i++)
        {
            _entries.Add(entries[i]);
        }

        return true;
    }

    /// <summary>Installs a snapshot, discarding the log it supersedes.</summary>
    /// <param name="snapshot">The snapshot to apply.</param>
    public void ApplySnapshot(Snapshot snapshot)
    {
        ThrowIfNull(snapshot);

        lock (_gate)
        {
            ThrowIfDisposed();
            EnsureLogWritable();
            ApplySnapshotLocked(snapshot);
        }
    }

    private void ApplySnapshotLocked(Snapshot snapshot)
    {
        ulong index = snapshot.Metadata.Index;
        if (index <= _snapshot.Metadata.Index)
        {
            throw new RaftStorageException(RaftStorageError.SnapshotOutOfDate);
        }

        // Earlier readers ignore the generation the snapshot names, so until the log is replaced they would replay
        // the superseded log after the new snapshot: the log is fenced before the snapshot is published.
        if (!_logHasRollbackFence)
        {
            WriteRollbackFence();
            FlushLog();
            _logHasRollbackFence = true;
        }

        // The snapshot names the next log generation and is published first: a reopen after a crash before the log
        // it supersedes is replaced completes the installation.
        ulong generation = _logGeneration + 1;
        WriteSnapshot(snapshot, generation);
        InstallSnapshot(CopySnapshot(snapshot), generation);
    }

    /// <summary>
    /// Takes over a published snapshot that supersedes the log, and replaces the log with one of
    /// <paramref name="generation"/> that holds only the state the snapshot leaves.
    /// </summary>
    private void InstallSnapshot(Snapshot snapshot, ulong generation)
    {
        ulong index = snapshot.Metadata.Index;
        _snapshot = snapshot;
        _snapshotGeneration = generation;
        _confState = snapshot.Metadata.ConfState;
        _confStateIndex = index;
        _entries.Clear();
        _entries.Add(new Entry(EntryType.Normal, snapshot.Metadata.Term, index, default));
        _hardState = new HardState(Math.Max(_hardState.Term, snapshot.Metadata.Term), _hardState.Vote, index);
        ReplaceLog(generation, index);
    }

    /// <summary>
    /// Writes a log of <paramref name="generation"/> beside the current one and swaps it in, so after a crash either
    /// the former or the new log is in place. A log written whole starts with its generation record, which is read
    /// before the log is replayed, and holds the membership after a rollback fence; any rewrite of the log keeps both.
    /// </summary>
    private void ReplaceLog(ulong generation, ulong confStateIndex)
    {
        FileStream current = _logStream;
        using (var next = new FileStream(_newLogPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            _logStream = next;
            try
            {
                WriteLogGenerationRecord(generation);
                WriteRollbackFence();
                WriteConfStateRecords(_confState, confStateIndex);
                WriteHardStateRecord(_hardState);
                FlushLog();
            }
            finally
            {
                _logStream = current;
            }
        }

        current.Dispose();
        try
        {
            PublishFile(_newLogPath, _logPath, "log");
        }
        finally
        {
            _logStream = OpenLog();
        }

        _logGeneration = generation;
        _logHasRollbackFence = true;
    }

    private FileStream OpenLog()
    {
        var stream = new FileStream(_logPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        stream.Seek(0, SeekOrigin.End);
        return stream;
    }

    /// <inheritdoc/>
    public Snapshot CreateSnapshot(ulong index, ConfState confState, ReadOnlyMemory<byte> data)
    {
        ThrowIfNull(confState);

        lock (_gate)
        {
            ThrowIfDisposed();
            EnsureLogWritable();
            if (index <= _snapshot.Metadata.Index)
            {
                throw new RaftStorageException(RaftStorageError.SnapshotOutOfDate);
            }

            ulong term = TermLocked(index);
            var snapshot = new Snapshot(new SnapshotMetadata(index, term, confState), data.ToArray());
            if (!_snapshot.MembershipIsAtIndex)
            {
                // The current membership is stored only in the snapshot file this one replaces; keep it in the log.
                AppendConfStateRecords(_confState, _confStateIndex);
                FlushLog();
            }

            // The log continues after this snapshot, so the snapshot names the log's own generation.
            WriteSnapshot(snapshot, _logGeneration);
            _snapshot = snapshot;
            _snapshotGeneration = _logGeneration;
            return snapshot;
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// A snapshot kept from an earlier version of this store holds the latest membership the store recorded, not
    /// necessarily the one at its index.
    /// </remarks>
    public Snapshot? LatestSnapshot()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            return _snapshot.IsEmpty ? null : _snapshot;
        }
    }

    /// <summary>
    /// Compacts the log up to (and excluding) <paramref name="compactIndex"/>. When the log file's owner, group, and
    /// access control can be reproduced exactly on a new file, the log file is rewritten so it holds only the retained
    /// entries: the compacted log is written next to the log and then replaces it, so after a crash either the former
    /// or the compacted log is in place, and both describe the retained entries. Otherwise the compaction is recorded
    /// in the log file, which keeps its access and does not shrink.
    /// </summary>
    /// <param name="compactIndex">The first index to retain.</param>
    public void Compact(ulong compactIndex)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            EnsureLogWritable();
            List<Entry> compactedEntries = CreateCompactedEntries(compactIndex);
            if (!TryRewriteLog(compactedEntries))
            {
                WriteCompactRecord(_logStream, compactIndex);
                FlushLog();
                _entries.Clear();
                _entries.AddRange(compactedEntries);
                SkippedLogRewrites++;
            }
        }
    }

    /// <summary>Gets the number of compactions recorded in the log file because it could not be rewritten.</summary>
    internal int SkippedLogRewrites { get; private set; }

    /// <summary>Overwrites the persisted hard state.</summary>
    /// <param name="hardState">The hard state to store.</param>
    public void SetHardState(HardState hardState)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            EnsureLogWritable();
            _hardState = hardState;
            WriteHardStateRecord(hardState);
            FlushLog();
        }
    }

    /// <summary>
    /// Overwrites the persisted committed configuration. The index it reflects becomes unknown:
    /// <see cref="ConfStateIndex"/> returns <see langword="null"/>, also after the store is reopened, until the next
    /// <see cref="SetConfState(ConfState, ulong)"/> or snapshot installation.
    /// </summary>
    /// <param name="confState">The configuration to store.</param>
    public void SetConfState(ConfState confState)
    {
        ThrowIfNull(confState);

        lock (_gate)
        {
            ThrowIfDisposed();
            EnsureLogWritable();
            _confState = confState;
            _confStateIndex = null;
            AppendConfStateRecords(confState, index: null);
            FlushLog();
        }
    }

    /// <inheritdoc/>
    public void SetConfState(ConfState confState, ulong index)
    {
        ThrowIfNull(confState);

        lock (_gate)
        {
            ThrowIfDisposed();
            EnsureLogWritable();
            _confState = confState;
            _confStateIndex = index;
            AppendConfStateRecords(confState, index);
            FlushLog();
        }
    }

    /// <inheritdoc/>
    public ulong? ConfStateIndex()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            return _confStateIndex;
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _logStream.Dispose();
            _disposed = true;
        }
    }

    private static Snapshot CopySnapshot(Snapshot snapshot)
    {
        byte[] data = snapshot.Data.ToArray();
        return new Snapshot(snapshot.Metadata, data);
    }

    private static int ConfStateLength(ConfState confState)
    {
        return 1
            + 4 + (confState.Voters.Count * UInt64Size)
            + 4 + (confState.Learners.Count * UInt64Size)
            + 4 + (confState.VotersOutgoing.Count * UInt64Size)
            + 4 + (confState.LearnersNext.Count * UInt64Size);
    }

    private static void WriteConfState(Span<byte> destination, ref int offset, ConfState confState)
    {
        destination[offset++] = confState.AutoLeave ? (byte)1 : (byte)0;
        WriteIds(destination, ref offset, confState.Voters);
        WriteIds(destination, ref offset, confState.Learners);
        WriteIds(destination, ref offset, confState.VotersOutgoing);
        WriteIds(destination, ref offset, confState.LearnersNext);
    }

    private static bool TryReadConfState(ReadOnlySpan<byte> source, ref int offset, out ConfState? confState)
    {
        confState = null;
        if (source.Length < offset + 1)
        {
            return false;
        }

        bool autoLeave = source[offset++] != 0;
        if (!TryReadIds(source, ref offset, out ulong[]? voters)
            || !TryReadIds(source, ref offset, out ulong[]? learners)
            || !TryReadIds(source, ref offset, out ulong[]? votersOutgoing)
            || !TryReadIds(source, ref offset, out ulong[]? learnersNext))
        {
            return false;
        }

        confState = new ConfState(voters, learners, votersOutgoing, learnersNext, autoLeave);
        return true;
    }

    private static void WriteIds(Span<byte> destination, ref int offset, IReadOnlyList<ulong> ids)
    {
        BinaryPrimitives.WriteInt32BigEndian(destination.Slice(offset), ids.Count);
        offset += 4;
        for (int i = 0; i < ids.Count; i++)
        {
            WriteUInt64(destination, ref offset, ids[i]);
        }
    }

    private static bool TryReadIds(ReadOnlySpan<byte> source, ref int offset, out ulong[]? ids)
    {
        ids = null;
        if (source.Length < offset + 4)
        {
            return false;
        }

        int count = BinaryPrimitives.ReadInt32BigEndian(source.Slice(offset));
        offset += 4;
        if (count < 0 || source.Length < offset + (count * UInt64Size))
        {
            return false;
        }

        var result = new ulong[count];
        for (int i = 0; i < count; i++)
        {
            result[i] = ReadUInt64(source, ref offset);
        }

        ids = result;
        return true;
    }

    private static void WriteUInt64(Span<byte> destination, ref int offset, ulong value)
    {
        BinaryPrimitives.WriteUInt64BigEndian(destination.Slice(offset), value);
        offset += UInt64Size;
    }

    private static ulong ReadUInt64(ReadOnlySpan<byte> source, ref int offset)
    {
        ulong value = BinaryPrimitives.ReadUInt64BigEndian(source.Slice(offset));
        offset += UInt64Size;
        return value;
    }

    private static void ThrowIfNull(object? value)
    {
#if NET8_0_OR_GREATER
        ArgumentNullException.ThrowIfNull(value);
#else
        if (value is null)
        {
            throw new ArgumentNullException(nameof(value));
        }
#endif
    }

    private void LoadSnapshot()
    {
        if (!System.IO.File.Exists(_snapshotPath))
        {
            return;
        }

        // The snapshot file is replaced as a whole, never written in place, so a damaged one is not a torn write. It
        // holds the application state, so it is refused rather than ignored.
        byte[] data = System.IO.File.ReadAllBytes(_snapshotPath);
        if (data.Length < 8 + 8 + 4)
        {
            throw DamagedSnapshot();
        }

        int offset = 0;
        ulong index = ReadUInt64(data, ref offset);
        ulong term = ReadUInt64(data, ref offset);
        int dataLength = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset));
        offset += 4;
        if (dataLength < 0 || data.Length < offset + dataLength)
        {
            throw DamagedSnapshot();
        }

        byte[] snapshotData = data.AsSpan(offset, dataLength).ToArray();
        offset += dataLength;
        if (!TryReadConfState(data, ref offset, out ConfState? confState) || confState is null)
        {
            throw DamagedSnapshot();
        }

        // Without a marker, an earlier version wrote the file: its membership is the latest one the store recorded,
        // not the one at the snapshot's index, so the index that membership reflects is unknown. Only the second
        // marker records the generation of the log that continues the snapshot.
        ReadOnlySpan<byte> trailer = data.AsSpan(offset);
        bool membershipIsAtIndex = !trailer.IsEmpty;
        if (trailer.Length == UInt64Size + LogGenerationMarker.Length
            && trailer.Slice(UInt64Size).SequenceEqual(LogGenerationMarker))
        {
            int generationOffset = 0;
            _snapshotGeneration = ReadUInt64(trailer, ref generationOffset);
        }
        else if (membershipIsAtIndex && !trailer.SequenceEqual(PointInTimeMembershipMarker))
        {
            throw DamagedSnapshot();
        }

        _snapshot = new Snapshot(new SnapshotMetadata(index, term, confState), snapshotData, membershipIsAtIndex);
        _confState = confState;
        _confStateIndex = membershipIsAtIndex ? index : null;
        _entries.Clear();
        _entries.Add(new Entry(EntryType.Normal, term, index, default));
    }

    private InvalidDataException DamagedSnapshot() =>
        new($"The snapshot file '{_snapshotPath}' is damaged.");

    private void LoadHardState()
    {
        if (!System.IO.File.Exists(_hardStatePath))
        {
            return;
        }

        byte[] data = System.IO.File.ReadAllBytes(_hardStatePath);
        if (data.Length < 24)
        {
            return;
        }

        int offset = 0;
        ulong term = ReadUInt64(data, ref offset);
        ulong vote = ReadUInt64(data, ref offset);
        ulong commit = ReadUInt64(data, ref offset);
        _hardState = new HardState(term, vote, commit);
    }

    /// <summary>
    /// Returns the generation the log file records in its first record, or zero when it records none: the log was
    /// written before generations were recorded, or never replaced by an installed snapshot.
    /// </summary>
    private ulong ReadLogGeneration()
    {
        if (!System.IO.File.Exists(_logPath))
        {
            return 0;
        }

        using var stream = new FileStream(_logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var record = new byte[LengthPrefixSize + 1 + UInt64Size];
        if (!ReadExactly(stream, record, 0, record.Length)
            || BinaryPrimitives.ReadInt32BigEndian(record) != 1 + UInt64Size
            || record[LengthPrefixSize] != LogGenerationRecord)
        {
            return 0;
        }

        int offset = LengthPrefixSize + 1;
        return ReadUInt64(record, ref offset);
    }

    /// <summary>
    /// Replays the log records into memory, or only its hard state records when <paramref name="hardStateOnly"/>;
    /// returns the length of the intact log: the end of the last complete record, before a trailing index record whose
    /// configuration record is missing.
    /// </summary>
    private long ReplayLog(bool hardStateOnly)
    {
        if (!System.IO.File.Exists(_logPath))
        {
            return 0;
        }

        using var stream = new FileStream(_logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        long fileLength = stream.Length;
        var lengthBuffer = new byte[LengthPrefixSize];
        long intactLength = 0;
        while (ReadExactly(stream, lengthBuffer, 0, lengthBuffer.Length))
        {
            // A record that does not fit in the file is the torn last write, and so may be its length prefix.
            int length = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);
            if (length <= 0 || length > fileLength - stream.Position)
            {
                break;
            }

            var payload = new byte[length];
            if (!ReadExactly(stream, payload, 0, payload.Length))
            {
                break;
            }

            if (!hardStateOnly || payload[0] == HardStateRecord)
            {
                ReplayRecord(payload);
            }

            // An index record is intact only together with the configuration record written right after it.
            if (_pendingConfStateIndex is null)
            {
                intactLength = stream.Position;
            }
        }

        _pendingConfStateIndex = null;
        return intactLength;
    }

    private static bool ReadExactly(Stream stream, byte[] buffer, int offset, int count)
    {
        int read = 0;
        while (read < count)
        {
            int n = stream.Read(buffer, offset + read, count - read);
            if (n == 0)
            {
                return false;
            }

            read += n;
        }

        return true;
    }

    private void ReplayRecord(ReadOnlySpan<byte> payload)
    {
        if (_expectsLegacyReaderRejectionRecord
            && (payload[0] != CompactRecord || payload.Length != 1 + UInt64Size))
        {
            _expectsLegacyReaderRejectionRecord = false;
        }

        // An index record belongs to the configuration record written right after it.
        ulong? confStateIndex = _pendingConfStateIndex;
        _pendingConfStateIndex = null;
        switch (payload[0])
        {
            case EntryRecord:
                ReplayEntryRecord(payload);
                break;
            case TruncateRecord:
                if (payload.Length == 1 + UInt64Size)
                {
                    int offset = 1;
                    TruncateForAppend(ReadUInt64(payload, ref offset));
                }

                break;
            case BaseRecord:
                if (payload.Length == BaseRecordSize)
                {
                    int offset = 1;
                    ulong index = ReadUInt64(payload, ref offset);
                    ulong term = ReadUInt64(payload, ref offset);
                    _expectsLegacyReaderRejectionRecord = true;

                    // A newer snapshot already covers a base at or below its index.
                    if (index > _entries[0].Index)
                    {
                        _entries.Clear();
                        _entries.Add(new Entry(EntryType.Normal, term, index, default));
                    }
                }

                break;
            case CompactRecord:
                if (payload.Length == 1 + UInt64Size)
                {
                    int offset = 1;
                    ulong compactIndex = ReadUInt64(payload, ref offset);
                    if (_expectsLegacyReaderRejectionRecord && compactIndex == 0)
                    {
                        _expectsLegacyReaderRejectionRecord = false;
                        break;
                    }

                    _expectsLegacyReaderRejectionRecord = false;
                    if (compactIndex == RollbackFenceIndex)
                    {
                        _logHasRollbackFence = true;
                        break;
                    }

                    // A snapshot recorded after this compaction already covers it.
                    if (compactIndex > _entries[0].Index)
                    {
                        Entry boundary = CompactCore(compactIndex);
                        _entries[0] = new Entry(EntryType.Normal, boundary.Term, boundary.Index, default);
                    }
                }

                break;
            case HardStateRecord:
                if (payload.Length == HardStateRecordSize)
                {
                    int offset = 1;
                    ulong term = ReadUInt64(payload, ref offset);
                    ulong vote = ReadUInt64(payload, ref offset);
                    ulong commit = ReadUInt64(payload, ref offset);
                    _hardState = new HardState(term, vote, commit);
                }

                break;
            case ConfStateRecord:
                int confStateOffset = 1;
                if (TryReadConfState(payload, ref confStateOffset, out ConfState? confState)
                    && confState is not null
                    && confStateOffset == payload.Length)
                {
                    // A configuration without an index record right before it was stored with an unknown index.
                    _confState = confState;
                    _confStateIndex = confStateIndex;
                }

                break;
            case ConfStateIndexRecord:
                if (payload.Length == 1 + UInt64Size)
                {
                    int offset = 1;
                    _pendingConfStateIndex = ReadUInt64(payload, ref offset);
                }

                break;
        }
    }

    private void ReplayEntryRecord(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < EntryHeaderSize)
        {
            return;
        }

        var type = (EntryType)payload[1];
        int offset = 2;
        ulong term = ReadUInt64(payload, ref offset);
        ulong index = ReadUInt64(payload, ref offset);
        int dataLength = BinaryPrimitives.ReadInt32BigEndian(payload.Slice(offset));
        offset += 4;
        if (dataLength < 0 || payload.Length != offset + dataLength)
        {
            return;
        }

        AppendCore(new Entry(type, term, index, payload.Slice(offset, dataLength).ToArray()));
    }

    private void WriteTruncateRecord(ulong index)
    {
        Span<byte> payload = stackalloc byte[1 + UInt64Size];
        payload[0] = TruncateRecord;
        int offset = 1;
        WriteUInt64(payload, ref offset, index);
        WriteRecord(_logStream, payload);
    }

    private static void WriteBaseRecord(Stream stream, Entry boundary)
    {
        Span<byte> payload = stackalloc byte[BaseRecordSize];
        payload[0] = BaseRecord;
        int offset = 1;
        WriteUInt64(payload, ref offset, boundary.Index);
        WriteUInt64(payload, ref offset, boundary.Term);
        WriteRecord(stream, payload);
    }

    private void WriteHardStateRecord(HardState hardState) => WriteHardStateRecord(_logStream, hardState);

    private static void WriteHardStateRecord(Stream stream, HardState hardState)
    {
        Span<byte> payload = stackalloc byte[HardStateRecordSize];
        payload[0] = HardStateRecord;
        int offset = 1;
        WriteUInt64(payload, ref offset, hardState.Term);
        WriteUInt64(payload, ref offset, hardState.Vote);
        WriteUInt64(payload, ref offset, hardState.Commit);
        WriteRecord(stream, payload);
    }

    private void WriteRollbackFence()
    {
        // Readers before this format ignore the configuration records that follow; they fail on this compaction
        // record instead of opening the store with an earlier membership.
        WriteCompactRecord(_logStream, RollbackFenceIndex);
    }

    /// <summary>Appends configuration records to the log, after a rollback fence when the log holds none yet.</summary>
    private void AppendConfStateRecords(ConfState confState, ulong? index)
    {
        if (!_logHasRollbackFence)
        {
            WriteRollbackFence();
            _logHasRollbackFence = true;
        }

        WriteConfStateRecords(confState, index);
    }

    /// <summary>
    /// Writes the index record, when the index is known, then the configuration record it belongs to. A configuration
    /// record without an index record right before it has an unknown index; an index record without its configuration
    /// record is dropped on open.
    /// </summary>
    private void WriteConfStateRecords(ConfState confState, ulong? index) =>
        WriteConfStateRecords(_logStream, confState, index);

    private static void WriteConfStateRecords(Stream stream, ConfState confState, ulong? index)
    {
        if (index is { } knownIndex)
        {
            Span<byte> indexPayload = stackalloc byte[1 + UInt64Size];
            indexPayload[0] = ConfStateIndexRecord;
            int indexOffset = 1;
            WriteUInt64(indexPayload, ref indexOffset, knownIndex);
            WriteRecord(stream, indexPayload);
        }

        byte[] payload = new byte[1 + ConfStateLength(confState)];
        payload[0] = ConfStateRecord;
        int offset = 1;
        WriteConfState(payload, ref offset, confState);
        WriteRecord(stream, payload);
    }

    private void WriteEntryRecord(Entry entry) => WriteEntryRecord(_logStream, entry);

    private static void WriteEntryRecord(Stream stream, Entry entry)
    {
        int length = EntryHeaderSize + entry.Data.Length;
        byte[] payload = new byte[length];
        payload[0] = EntryRecord;
        payload[1] = (byte)entry.Type;
        int offset = 2;
        WriteUInt64(payload, ref offset, entry.Term);
        WriteUInt64(payload, ref offset, entry.Index);
        BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(offset), entry.Data.Length);
        offset += 4;
        entry.Data.Span.CopyTo(payload.AsSpan(offset));
        WriteRecord(stream, payload);
    }

    private static void WriteRecord(Stream stream, ReadOnlySpan<byte> payload)
    {
        Span<byte> lengthBuffer = stackalloc byte[LengthPrefixSize];
        BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, payload.Length);
        Write(stream, lengthBuffer);
        Write(stream, payload);
    }

    private static void Write(Stream stream, ReadOnlySpan<byte> buffer)
    {
        byte[] data = buffer.ToArray();
        stream.Write(data, 0, data.Length);
    }

    private void FlushLog()
    {
        _logStream.Flush(_fsync);
    }

    /// <summary>
    /// Replaces the log file with one that holds only the retained log: the compaction boundary, the entries after it,
    /// and the hard state. Any other state the log records, and any record a log must start with, is carried into the
    /// compacted log as well; the guard record for older readers directly follows the compaction boundary.
    /// </summary>
    /// <returns>Whether the log file was replaced; not when its access cannot be reproduced on a new file.</returns>
    private bool TryRewriteLog(List<Entry> compactedEntries)
    {
        FileStream? staged = CreateCompactingLog();
        if (staged is null)
        {
            return false;
        }

        using (FileStream compacted = staged)
        {
            WriteLogGenerationRecord(compacted, _logGeneration);
            WriteBaseRecord(compacted, compactedEntries[0]);
            WriteLegacyReaderRejectionRecord(compacted);
            WriteCompactRecord(compacted, RollbackFenceIndex);
            WriteConfStateRecords(compacted, _confState, _confStateIndex);
            for (int i = 1; i < compactedEntries.Count; i++)
            {
                WriteEntryRecord(compacted, compactedEntries[i]);
            }

            WriteHardStateRecord(compacted, _hardState);
            compacted.Flush(_fsync);
        }

        _logStream.Dispose();
        _logWritable = false;
        try
        {
            System.IO.File.Replace(_compactingLogPath, _logPath, destinationBackupFileName: null);
        }
        catch (Exception replacementException)
        {
            try
            {
                _logStream = OpenLog();
                _logWritable = true;
            }
            catch (Exception reopenException)
            {
                throw new InvalidOperationException(
                    "The log replacement failed and the original log could not be reopened.",
                    new AggregateException(replacementException, reopenException));
            }

            throw;
        }

        _entries.Clear();
        _entries.AddRange(compactedEntries);
        _logHasRollbackFence = true;

        if (_fsync)
        {
            _replacementNeedsDirectorySync = true;
            SyncLogDirectory();
            _replacementNeedsDirectorySync = false;
        }

        try
        {
            _logStream = OpenLog();
            _logWritable = true;
        }
        catch (Exception reopenException)
        {
            throw new InvalidOperationException(
                "The compacted log was replaced but could not be reopened for writes.",
                reopenException);
        }

        return true;
    }

    /// <summary>
    /// Creates an empty compacted log with exactly the log's access, which is never broader than the log's while it is
    /// created: on Windows the file is created with the log's access rules, and on Unix it is created exclusively and
    /// owner-only and then gets the log's owner, group, access control list, and mode before any payload.
    /// </summary>
    /// <returns>The compacted log; <see langword="null"/> when the log's access cannot be reproduced exactly.</returns>
    private FileStream? CreateCompactingLog()
    {
        // A file that already exists keeps its own permissions when reopened, so the compacted log is always created.
        System.IO.File.Delete(_compactingLogPath);
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return CreateCompactingLogOnWindows();
        }

        FileStream staged = UnixStagedLog.Create(_compactingLogPath);
        bool hasLogAccess = false;
        try
        {
            hasLogAccess = UnixStagedLog.TryApplyLogAccess(
                _logStream.SafeFileHandle,
                staged.SafeFileHandle,
                _statx,
                _accessControl);
        }
        finally
        {
            if (!hasLogAccess)
            {
                staged.Dispose();
                System.IO.File.Delete(_compactingLogPath);
            }
        }

        return hasLogAccess ? staged : null;
    }

    /// <summary>
    /// Creates the compacted log with the log's access rules in the same call that creates the file, so the retained
    /// payloads are never readable under the directory's inherited rules.
    /// </summary>
#if NET8_0_OR_GREATER
    [SupportedOSPlatform("windows")]
#endif
    private FileStream CreateCompactingLogOnWindows()
    {
        var logSecurity = new FileSecurity(_logPath, AccessControlSections.Access);
        return new FileInfo(_compactingLogPath).Create(
            FileMode.CreateNew,
            FileSystemRights.Write,
            FileShare.None,
            bufferSize: 4096,
            FileOptions.None,
            logSecurity);
    }

    private static void WriteLegacyReaderRejectionRecord(Stream stream)
    {
        // Older readers ignore BaseRecord. They interpret this otherwise-invalid compaction index as an error rather
        // than replaying retained entries with incorrect indexes.
        WriteCompactRecord(stream, 0);
    }

    private static void WriteCompactRecord(Stream stream, ulong compactIndex)
    {
        Span<byte> payload = stackalloc byte[1 + UInt64Size];
        payload[0] = CompactRecord;
        int offset = 1;
        WriteUInt64(payload, ref offset, compactIndex);
        WriteRecord(stream, payload);
    }

    private void EnsureLogWritable()
    {
        if (_logWritable)
        {
            return;
        }

        if (_replacementNeedsDirectorySync)
        {
            SyncLogDirectory();
            _replacementNeedsDirectorySync = false;
        }

        _logStream = OpenLog();
        _logWritable = true;
    }

    private void SyncLogDirectory()
    {
        _syncDirectory(Path.GetDirectoryName(_logPath)!);
    }

    private static void SyncDirectoryDurably(string directoryPath)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            // Windows exposes no supported directory fsync equivalent through the .NET file APIs.
            return;
        }

        DirectorySync.Sync(directoryPath);
    }

    private void WriteLogGenerationRecord(ulong generation) => WriteLogGenerationRecord(_logStream, generation);

    private static void WriteLogGenerationRecord(Stream stream, ulong generation)
    {
        Span<byte> payload = stackalloc byte[1 + UInt64Size];
        payload[0] = LogGenerationRecord;
        int offset = 1;
        WriteUInt64(payload, ref offset, generation);
        WriteRecord(stream, payload);
    }

    private void WriteSnapshot(Snapshot snapshot, ulong logGeneration)
    {
        int length = 8 + 8 + 4 + snapshot.Data.Length + ConfStateLength(snapshot.Metadata.ConfState)
            + UInt64Size + LogGenerationMarker.Length;
        byte[] data = new byte[length];
        int offset = 0;
        WriteUInt64(data, ref offset, snapshot.Metadata.Index);
        WriteUInt64(data, ref offset, snapshot.Metadata.Term);
        BinaryPrimitives.WriteInt32BigEndian(data.AsSpan(offset), snapshot.Data.Length);
        offset += 4;
        snapshot.Data.Span.CopyTo(data.AsSpan(offset));
        offset += snapshot.Data.Length;
        WriteConfState(data, ref offset, snapshot.Metadata.ConfState);

        // After every field an earlier version reads, which ignores the bytes that follow.
        WriteUInt64(data, ref offset, logGeneration);
        LogGenerationMarker.CopyTo(data, offset);

        // Written beside the snapshot and then swapped in, so after a crash either the former or the new snapshot is
        // in place.
        using (var stream = new FileStream(_newSnapshotPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            stream.Write(data, 0, data.Length);
            stream.Flush(_fsync);
        }

        PublishFile(_newSnapshotPath, _snapshotPath, "snapshot");
    }

    /// <summary>Replaces <paramref name="path"/> with the durable file <paramref name="newPath"/>.</summary>
    private void PublishFile(string newPath, string path, string kind)
    {
        if (_fsync && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            if (!MoveFileEx(newPath, path, MoveFileReplaceExisting | MoveFileWriteThrough))
            {
                throw new IOException($"Could not publish {kind} durably (error {Marshal.GetLastWin32Error()}).");
            }
        }
        else if (System.IO.File.Exists(path))
        {
            System.IO.File.Replace(newPath, path, destinationBackupFileName: null);
        }
        else
        {
            System.IO.File.Move(newPath, path);
        }

        FlushDirectory();
    }

    private void FlushDirectory()
    {
        if (!_fsync)
        {
            return;
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            // MoveFileEx with MOVEFILE_WRITE_THROUGH publishes the rename synchronously on Windows, where
            // FlushFileBuffers rejects directory handles.
            return;
        }

        int directory = Open(_directoryPath, OpenReadOnly);
        if (directory < 0)
        {
            throw new IOException($"Could not open store directory for syncing (error {Marshal.GetLastWin32Error()}).");
        }

        int closeResult = 0;
        try
        {
            if (Fsync(directory) != 0)
            {
                throw new IOException($"Could not sync store directory (error {Marshal.GetLastWin32Error()}).");
            }
        }
        finally
        {
            closeResult = Close(directory);
        }

        if (closeResult != 0)
        {
            throw new IOException(
                $"Could not close store directory after syncing (error {Marshal.GetLastWin32Error()}).");
        }
    }

    private const uint MoveFileReplaceExisting = 0x00000001;
    private const uint MoveFileWriteThrough = 0x00000008;
    private const int OpenReadOnly = 0;

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool MoveFileEx(string existingFileName, string newFileName, uint flags);

    [DllImport("libc", EntryPoint = "open", SetLastError = true)]
    private static extern int Open(string path, int flags);

    [DllImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static extern int Fsync(int fileDescriptor);

    [DllImport("libc", EntryPoint = "close", SetLastError = true)]
    private static extern int Close(int fileDescriptor);

    private ulong TermLocked(ulong index)
    {
        ulong offset = _entries[0].Index;
        if (index < offset)
        {
            throw new RaftStorageException(RaftStorageError.Compacted);
        }

        // Checked before the narrowing cast below, which would wrap for an index far past the log.
        if (index > LastIndexLocked())
        {
            throw new RaftStorageException(RaftStorageError.Unavailable);
        }

        int i = (int)(index - offset);
        if (i >= _entries.Count)
        {
            throw new RaftStorageException(RaftStorageError.Unavailable);
        }

        return _entries[i].Term;
    }

    private ulong LastIndexLocked() => _entries[0].Index + (ulong)_entries.Count - 1;

    private void AppendCore(Entry entry)
    {
        ulong first = _entries[0].Index + 1;
        if (entry.Index < first)
        {
            return;
        }

        TruncateForAppend(entry.Index);
        _entries.Add(entry);
    }

    private void TruncateForAppend(ulong appendStart)
    {
        if (appendStart <= _entries[0].Index)
        {
            _entries.RemoveRange(1, _entries.Count - 1);
            return;
        }

        int truncateAt = (int)(appendStart - _entries[0].Index);
        if (truncateAt < _entries.Count)
        {
            _entries.RemoveRange(truncateAt, _entries.Count - truncateAt);
        }
    }

    private static class DirectorySync
    {
        private const int ReadOnly = 0;

        public static void Sync(string path)
        {
            int descriptor = RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
                ? OpenMac(path, ReadOnly)
                : OpenUnix(path, ReadOnly);
            if (descriptor < 0)
            {
                throw CreateIOException("open", path, Marshal.GetLastWin32Error());
            }

            int syncResult = RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
                ? FsyncMac(descriptor)
                : FsyncUnix(descriptor);
            int syncError = Marshal.GetLastWin32Error();
            int closeResult = RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
                ? CloseMac(descriptor)
                : CloseUnix(descriptor);
            int closeError = Marshal.GetLastWin32Error();
            if (syncResult != 0)
            {
                throw CreateIOException("fsync", path, syncError);
            }

            if (closeResult != 0)
            {
                throw CreateIOException("close", path, closeError);
            }
        }

        private static IOException CreateIOException(string operation, string path, int error)
        {
            return new IOException(
                $"Unable to {operation} log directory '{path}' (errno {error}).");
        }

        [DllImport("libc", EntryPoint = "open", SetLastError = true)]
        private static extern int OpenUnix(string path, int flags);

        [DllImport("libc", EntryPoint = "fsync", SetLastError = true)]
        private static extern int FsyncUnix(int descriptor);

        [DllImport("libc", EntryPoint = "close", SetLastError = true)]
        private static extern int CloseUnix(int descriptor);

        [DllImport("libSystem.B.dylib", EntryPoint = "open", SetLastError = true)]
        private static extern int OpenMac(string path, int flags);

        [DllImport("libSystem.B.dylib", EntryPoint = "fsync", SetLastError = true)]
        private static extern int FsyncMac(int descriptor);

        [DllImport("libSystem.B.dylib", EntryPoint = "close", SetLastError = true)]
        private static extern int CloseMac(int descriptor);
    }

    private List<Entry> CreateCompactedEntries(ulong compactIndex)
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
        var compactedEntries = _entries.GetRange(cut, _entries.Count - cut);
        compactedEntries[0] = new Entry(EntryType.Normal, boundary.Term, boundary.Index, default);
        return compactedEntries;
    }

    private Entry CompactCore(ulong compactIndex)
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
        return boundary;
    }

    private void ThrowIfDisposed()
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(FileRaftStorage));
        }
#endif
    }
}
