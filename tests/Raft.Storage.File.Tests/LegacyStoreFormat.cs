// Copyright (c) marcschier. Licensed under the MIT License.

using System.Buffers.Binary;
using Raft.Configuration;

namespace Raft.Storage.File.Tests;

/// <summary>
/// Writes and reads store files byte for byte as the file store did before configuration records and the snapshot
/// format marker existed (upstream df44fde). All integers are big-endian.
/// <list type="bullet">
/// <item>
/// <c>snapshot</c>: index (u64), term (u64), data length (i32), data, then the membership: auto-leave (byte) and the
/// voters, learners, outgoing voters, and next learners, each a count (i32) followed by the ids (u64). Nothing follows.
/// <c>SetConfState</c> rewrote this file with the latest membership and the snapshot's index, which is zero when no
/// snapshot was installed.
/// </item>
/// <item>
/// <c>log</c>: records of a payload length (i32) and the payload. An append wrote a truncate record (kind 1, the first
/// appended index) and an entry record per entry (kind 0, entry type, term, index, data length, data); a hard state
/// record is kind 3 with the term, vote, and commit. Installing a snapshot emptied the log and wrote a hard state
/// record.
/// </item>
/// </list>
/// </summary>
internal static class LegacyStoreFormat
{
    internal static void WriteSnapshotFile(
        string directory,
        ulong index,
        ulong term,
        ConfState confState,
        byte[]? data = null)
    {
        data ??= Array.Empty<byte>();
        var bytes = new List<byte>();
        AddUInt64(bytes, index);
        AddUInt64(bytes, term);
        AddInt32(bytes, data.Length);
        bytes.AddRange(data);
        bytes.Add(confState.AutoLeave ? (byte)1 : (byte)0);
        AddIds(bytes, confState.Voters);
        AddIds(bytes, confState.Learners);
        AddIds(bytes, confState.VotersOutgoing);
        AddIds(bytes, confState.LearnersNext);
        System.IO.File.WriteAllBytes(Path.Combine(directory, "snapshot"), bytes.ToArray());
    }

    /// <summary>
    /// Reads the snapshot file the way df44fde loaded it on open. It returns <see langword="false"/> where df44fde
    /// ignored the file and opened without a snapshot; it never read the bytes after the membership.
    /// </summary>
    internal static bool TryReadSnapshotFile(
        string directory,
        out ulong index,
        out ulong term,
        out byte[] data,
        out ConfState? confState)
    {
        index = 0;
        term = 0;
        data = Array.Empty<byte>();
        confState = null;
        byte[] file = System.IO.File.ReadAllBytes(Path.Combine(directory, "snapshot"));
        if (file.Length < 8 + 8 + 4)
        {
            return false;
        }

        int offset = 0;
        ulong readIndex = BinaryPrimitives.ReadUInt64BigEndian(file.AsSpan(offset));
        offset += 8;
        ulong readTerm = BinaryPrimitives.ReadUInt64BigEndian(file.AsSpan(offset));
        offset += 8;
        int dataLength = BinaryPrimitives.ReadInt32BigEndian(file.AsSpan(offset));
        offset += 4;
        if (dataLength < 0 || file.Length < offset + dataLength)
        {
            return false;
        }

        byte[] readData = file.AsSpan(offset, dataLength).ToArray();
        offset += dataLength;
        if (file.Length < offset + 1)
        {
            return false;
        }

        bool autoLeave = file[offset++] != 0;
        if (!TryReadIds(file, ref offset, out ulong[]? voters)
            || !TryReadIds(file, ref offset, out ulong[]? learners)
            || !TryReadIds(file, ref offset, out ulong[]? votersOutgoing)
            || !TryReadIds(file, ref offset, out ulong[]? learnersNext))
        {
            return false;
        }

        index = readIndex;
        term = readTerm;
        data = readData;
        confState = new ConfState(voters, learners, votersOutgoing, learnersNext, autoLeave);
        return true;
    }

    /// <summary>Builds a df44fde log file record by record.</summary>
    internal sealed class Log
    {
        private readonly List<byte> _bytes = new();

        /// <summary>Records an append of <paramref name="entries"/>, as df44fde wrote it.</summary>
        internal Log Append(params Entry[] entries)
        {
            var truncate = new List<byte> { 1 };
            AddUInt64(truncate, entries[0].Index);
            AddRecord(truncate);

            foreach (Entry entry in entries)
            {
                var record = new List<byte> { 0, (byte)entry.Type };
                AddUInt64(record, entry.Term);
                AddUInt64(record, entry.Index);
                AddInt32(record, entry.Data.Length);
                record.AddRange(entry.Data.ToArray());
                AddRecord(record);
            }

            return this;
        }

        internal Log HardState(HardState hardState)
        {
            var record = new List<byte> { 3 };
            AddUInt64(record, hardState.Term);
            AddUInt64(record, hardState.Vote);
            AddUInt64(record, hardState.Commit);
            AddRecord(record);
            return this;
        }

        internal void WriteTo(string directory) =>
            System.IO.File.WriteAllBytes(Path.Combine(directory, "log"), _bytes.ToArray());

        private void AddRecord(List<byte> payload)
        {
            AddInt32(_bytes, payload.Count);
            _bytes.AddRange(payload);
        }
    }

    private static bool TryReadIds(byte[] source, ref int offset, out ulong[]? ids)
    {
        ids = null;
        if (source.Length < offset + 4)
        {
            return false;
        }

        int count = BinaryPrimitives.ReadInt32BigEndian(source.AsSpan(offset));
        offset += 4;
        if (count < 0 || source.Length < offset + (count * 8))
        {
            return false;
        }

        var result = new ulong[count];
        for (int i = 0; i < count; i++)
        {
            result[i] = BinaryPrimitives.ReadUInt64BigEndian(source.AsSpan(offset));
            offset += 8;
        }

        ids = result;
        return true;
    }

    private static void AddIds(List<byte> bytes, IReadOnlyList<ulong> ids)
    {
        AddInt32(bytes, ids.Count);
        foreach (ulong id in ids)
        {
            AddUInt64(bytes, id);
        }
    }

    private static void AddInt32(List<byte> bytes, int value)
    {
        byte[] buffer = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        bytes.AddRange(buffer);
    }

    private static void AddUInt64(List<byte> bytes, ulong value)
    {
        byte[] buffer = new byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(buffer, value);
        bytes.AddRange(buffer);
    }
}
