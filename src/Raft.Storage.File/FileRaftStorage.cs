// Copyright (c) marcschier. Licensed under the MIT License.

using System.Buffers.Binary;
using Raft.Configuration;

namespace Raft.Storage.File;

/// <summary>A simple crash-safe file-backed implementation of <see cref="IRaftStorage"/>.</summary>
public sealed class FileRaftStorage : IRaftWritableStorage, IDisposable
{
    private const string LogFileName = "log";
    private const string HardStateFileName = "hardstate";
    private const string SnapshotFileName = "snapshot";
    private const byte EntryRecord = 0;
    private const byte TruncateRecord = 1;
    private const byte CompactRecord = 2;
    private const byte HardStateRecord = 3;
    private const int LengthPrefixSize = 4;
    private const int EntryHeaderSize = 1 + 1 + 8 + 8 + 4;
    private const int HardStateRecordSize = 1 + (3 * 8);
    private const int UInt64Size = 8;

    private readonly object _gate = new();
    private readonly string _hardStatePath;
    private readonly string _snapshotPath;
    private readonly bool _fsync;
    private readonly List<Entry> _entries = new() { default };
    private readonly FileStream _logStream;
    private HardState _hardState;
    private Snapshot _snapshot = new(new SnapshotMetadata(0, 0, new ConfState()), ReadOnlyMemory<byte>.Empty);
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="FileRaftStorage"/> class.</summary>
    /// <param name="options">The storage options.</param>
    public FileRaftStorage(FileRaftStorageOptions options)
    {
        ThrowIfNull(options);

        Directory.CreateDirectory(options.DirectoryPath);
        _fsync = options.Fsync;
        string logPath = Path.Combine(options.DirectoryPath, LogFileName);
        _hardStatePath = Path.Combine(options.DirectoryPath, HardStateFileName);
        _snapshotPath = Path.Combine(options.DirectoryPath, SnapshotFileName);

        LoadSnapshot();
        LoadHardState();
        ReplayLog(logPath);
        _logStream = new FileStream(logPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        _logStream.Seek(0, SeekOrigin.End);
    }

    /// <inheritdoc/>
    public (HardState HardState, ConfState ConfState) InitialState()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            return (_hardState, _snapshot.Metadata.ConfState);
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
            if (!_snapshot.IsEmpty && requestIndex <= _snapshot.Metadata.Index)
            {
                return _snapshot;
            }

            ulong appliedIndex = _hardState.Commit;
            if (requestIndex > appliedIndex)
            {
                throw new RaftStorageException(RaftStorageError.SnapshotTemporarilyUnavailable);
            }

            ulong term = TermLocked(appliedIndex);
            var metadata = new SnapshotMetadata(appliedIndex, term, _snapshot.Metadata.ConfState);
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

        WriteSnapshot(snapshot);
        ResetLogFile();
        _snapshot = CopySnapshot(snapshot);
        _entries.Clear();
        _entries.Add(new Entry(EntryType.Normal, snapshot.Metadata.Term, snapshot.Metadata.Index, default));
        _hardState = new HardState(Math.Max(_hardState.Term, snapshot.Metadata.Term), _hardState.Vote, index);
        WriteHardStateRecord(_hardState);
        FlushLog();
    }

    /// <summary>Compacts the in-memory log up to (and excluding) <paramref name="compactIndex"/>.</summary>
    /// <param name="compactIndex">The first index to retain.</param>
    public void Compact(ulong compactIndex)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            Entry boundary = CompactCore(compactIndex);
            WriteCompactRecord(compactIndex);
            FlushLog();
            _entries[0] = new Entry(EntryType.Normal, boundary.Term, boundary.Index, default);
        }
    }

    /// <summary>Overwrites the persisted hard state.</summary>
    /// <param name="hardState">The hard state to store.</param>
    public void SetHardState(HardState hardState)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            _hardState = hardState;
            WriteHardStateRecord(hardState);
            FlushLog();
        }
    }

    /// <summary>Overwrites the persisted committed configuration.</summary>
    /// <param name="confState">The configuration to store.</param>
    public void SetConfState(ConfState confState)
    {
        ThrowIfNull(confState);

        lock (_gate)
        {
            ThrowIfDisposed();
            SnapshotMetadata metadata = _snapshot.Metadata;
            _snapshot = new Snapshot(new SnapshotMetadata(metadata.Index, metadata.Term, confState), _snapshot.Data);
            WriteSnapshot(_snapshot);
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

        byte[] data = System.IO.File.ReadAllBytes(_snapshotPath);
        if (data.Length < 8 + 8 + 4)
        {
            return;
        }

        int offset = 0;
        ulong index = ReadUInt64(data, ref offset);
        ulong term = ReadUInt64(data, ref offset);
        int dataLength = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset));
        offset += 4;
        if (dataLength < 0 || data.Length < offset + dataLength)
        {
            return;
        }

        byte[] snapshotData = data.AsSpan(offset, dataLength).ToArray();
        offset += dataLength;
        if (!TryReadConfState(data, ref offset, out ConfState? confState) || confState is null)
        {
            return;
        }

        _snapshot = new Snapshot(new SnapshotMetadata(index, term, confState), snapshotData);
        _entries.Clear();
        _entries.Add(new Entry(EntryType.Normal, term, index, default));
    }

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

    private void ReplayLog(string logPath)
    {
        if (!System.IO.File.Exists(logPath))
        {
            return;
        }

        using var stream = new FileStream(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var lengthBuffer = new byte[LengthPrefixSize];
        while (ReadExactly(stream, lengthBuffer, 0, lengthBuffer.Length))
        {
            int length = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);
            if (length <= 0)
            {
                break;
            }

            var payload = new byte[length];
            if (!ReadExactly(stream, payload, 0, payload.Length))
            {
                break;
            }

            ReplayRecord(payload);
        }
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
            case CompactRecord:
                if (payload.Length == 1 + UInt64Size)
                {
                    int offset = 1;
                    Entry boundary = CompactCore(ReadUInt64(payload, ref offset));
                    _entries[0] = new Entry(EntryType.Normal, boundary.Term, boundary.Index, default);
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
        WriteRecord(payload);
    }

    private void WriteCompactRecord(ulong index)
    {
        Span<byte> payload = stackalloc byte[1 + UInt64Size];
        payload[0] = CompactRecord;
        int offset = 1;
        WriteUInt64(payload, ref offset, index);
        WriteRecord(payload);
    }

    private void WriteHardStateRecord(HardState hardState)
    {
        Span<byte> payload = stackalloc byte[HardStateRecordSize];
        payload[0] = HardStateRecord;
        int offset = 1;
        WriteUInt64(payload, ref offset, hardState.Term);
        WriteUInt64(payload, ref offset, hardState.Vote);
        WriteUInt64(payload, ref offset, hardState.Commit);
        WriteRecord(payload);
    }

    private void WriteEntryRecord(Entry entry)
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
        WriteRecord(payload);
    }

    private void WriteRecord(ReadOnlySpan<byte> payload)
    {
        Span<byte> lengthBuffer = stackalloc byte[LengthPrefixSize];
        BinaryPrimitives.WriteInt32BigEndian(lengthBuffer, payload.Length);
        Write(_logStream, lengthBuffer);
        Write(_logStream, payload);
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

    private void ResetLogFile()
    {
        _logStream.SetLength(0);
        _logStream.Seek(0, SeekOrigin.Begin);
        FlushLog();
    }

    private void WriteSnapshot(Snapshot snapshot)
    {
        int length = 8 + 8 + 4 + snapshot.Data.Length + ConfStateLength(snapshot.Metadata.ConfState);
        byte[] data = new byte[length];
        int offset = 0;
        WriteUInt64(data, ref offset, snapshot.Metadata.Index);
        WriteUInt64(data, ref offset, snapshot.Metadata.Term);
        BinaryPrimitives.WriteInt32BigEndian(data.AsSpan(offset), snapshot.Data.Length);
        offset += 4;
        snapshot.Data.Span.CopyTo(data.AsSpan(offset));
        offset += snapshot.Data.Length;
        WriteConfState(data, ref offset, snapshot.Metadata.ConfState);
        WriteAllBytesDurable(_snapshotPath, data);
    }

    private void WriteAllBytesDurable(string path, byte[] data)
    {
        using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        stream.Write(data, 0, data.Length);
        stream.Flush(_fsync);
    }

    private ulong TermLocked(ulong index)
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
