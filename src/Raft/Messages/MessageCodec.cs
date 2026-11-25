// Copyright (c) marcschier. Licensed under the MIT License.

using System.Buffers.Binary;
using Raft.Configuration;
using Raft.Storage;

namespace Raft.Messages;

/// <summary>
/// A compact big-endian binary codec for <see cref="Message"/>. Encoding writes directly into a caller buffer
/// via <see cref="System.Span{T}"/>/<see cref="BinaryPrimitives"/>; decoding validates lengths before allocating.
/// </summary>
public static class MessageCodec
{
    private const byte FlagReject = 0x01;
    private const byte FlagSnapshot = 0x02;
    private const int FixedHeader = 1 + (8 * 8) + 1 + 4; // type + 8 u64 + flags + entryCount
    private const int EntryHeader = 1 + 8 + 8 + 4;
    private const int SnapshotHeader = 8 + 8 + 4;
    private const int ConfStateHeader = 1 + (4 * sizeof(int));

    /// <summary>Computes the exact encoded size of <paramref name="message"/>.</summary>
    /// <param name="message">The message to measure.</param>
    /// <returns>The number of bytes <see cref="TryWrite"/> will produce.</returns>
    /// <exception cref="OverflowException">The length cannot be represented as an <see cref="int"/>.</exception>
    public static int EncodedLength(Message message)
    {
        Internal.Check.NotNull(message);

        int length = FixedHeader;
        IReadOnlyList<Entry> entries = message.Entries;
        for (int i = 0; i < entries.Count; i++)
        {
            length = checked(length + EntryHeader + entries[i].Data.Length);
        }

        if (message.Snapshot is { } snapshot)
        {
            length = checked(length + SnapshotHeader + snapshot.Data.Length
                + ConfStateLength(snapshot.Metadata.ConfState));
        }

        return length;
    }

    /// <summary>Encodes <paramref name="message"/> into <paramref name="destination"/>.</summary>
    /// <param name="message">The message to encode.</param>
    /// <param name="destination">The destination span.</param>
    /// <returns><see langword="true"/> when the message was written.</returns>
    public static bool TryWrite(Message message, Span<byte> destination)
    {
        Internal.Check.NotNull(message);

        if (destination.Length < EncodedLength(message))
        {
            return false;
        }

        destination[0] = (byte)message.Type;
        int offset = 1;
        WriteUInt64(destination, ref offset, message.From);
        WriteUInt64(destination, ref offset, message.To);
        WriteUInt64(destination, ref offset, message.Term);
        WriteUInt64(destination, ref offset, message.LogTerm);
        WriteUInt64(destination, ref offset, message.Index);
        WriteUInt64(destination, ref offset, message.Commit);
        WriteUInt64(destination, ref offset, message.RejectHint);
        WriteUInt64(destination, ref offset, message.Context);

        byte flags = 0;
        if (message.Reject)
        {
            flags |= FlagReject;
        }

        if (message.Snapshot is not null)
        {
            flags |= FlagSnapshot;
        }

        destination[offset++] = flags;

        IReadOnlyList<Entry> entries = message.Entries;
        BinaryPrimitives.WriteInt32BigEndian(destination.Slice(offset), entries.Count);
        offset += 4;
        for (int i = 0; i < entries.Count; i++)
        {
            Entry entry = entries[i];
            destination[offset++] = (byte)entry.Type;
            WriteUInt64(destination, ref offset, entry.Term);
            WriteUInt64(destination, ref offset, entry.Index);
            BinaryPrimitives.WriteInt32BigEndian(destination.Slice(offset), entry.Data.Length);
            offset += 4;
            entry.Data.Span.CopyTo(destination.Slice(offset));
            offset += entry.Data.Length;
        }

        if (message.Snapshot is { } snapshot)
        {
            WriteUInt64(destination, ref offset, snapshot.Metadata.Index);
            WriteUInt64(destination, ref offset, snapshot.Metadata.Term);
            BinaryPrimitives.WriteInt32BigEndian(destination.Slice(offset), snapshot.Data.Length);
            offset += 4;
            snapshot.Data.Span.CopyTo(destination.Slice(offset));
            offset += snapshot.Data.Length;
            WriteConfState(destination, ref offset, snapshot.Metadata.ConfState);
        }

        return true;
    }

    /// <summary>Decodes a <see cref="Message"/> from <paramref name="source"/>.</summary>
    /// <param name="source">The encoded message bytes.</param>
    /// <param name="message">The decoded message.</param>
    /// <returns><see langword="true"/> when decoding succeeded.</returns>
    public static bool TryParse(ReadOnlySpan<byte> source, out Message? message)
    {
        message = null;
        if (source.Length < FixedHeader || source[0] > (byte)MessageType.TimeoutNow)
        {
            return false;
        }

        var result = new Message { Type = (MessageType)source[0] };
        int offset = 1;
        result.From = ReadUInt64(source, ref offset);
        result.To = ReadUInt64(source, ref offset);
        result.Term = ReadUInt64(source, ref offset);
        result.LogTerm = ReadUInt64(source, ref offset);
        result.Index = ReadUInt64(source, ref offset);
        result.Commit = ReadUInt64(source, ref offset);
        result.RejectHint = ReadUInt64(source, ref offset);
        result.Context = ReadUInt64(source, ref offset);

        byte flags = source[offset++];
        if ((flags & ~(FlagReject | FlagSnapshot)) != 0)
        {
            return false;
        }

        result.Reject = (flags & FlagReject) != 0;
        bool hasSnapshot = (flags & FlagSnapshot) != 0;
        int snapshotMinimum = hasSnapshot ? SnapshotHeader + ConfStateHeader : 0;

        int entryCount = BinaryPrimitives.ReadInt32BigEndian(source.Slice(offset));
        offset += 4;
        int remaining = source.Length - offset - snapshotMinimum;
        if (remaining < 0
            || entryCount < 0
            || entryCount > remaining / EntryHeader
            || (entryCount != 0 && result.Type is not (MessageType.Append or MessageType.Propose))
            || hasSnapshot != (result.Type == MessageType.Snapshot))
        {
            return false;
        }

        if (entryCount > 0)
        {
            var entries = new Entry[entryCount];
            for (int i = 0; i < entryCount; i++)
            {
                int headersAfterThis = ((entryCount - i - 1) * EntryHeader) + snapshotMinimum;
                if (source.Length - offset - headersAfterThis < EntryHeader)
                {
                    return false;
                }

                var type = (EntryType)source[offset++];
                ulong term = ReadUInt64(source, ref offset);
                ulong index = ReadUInt64(source, ref offset);
                int dataLen = BinaryPrimitives.ReadInt32BigEndian(source.Slice(offset));
                offset += 4;
                if (type > EntryType.ConfChangeV2
                    || dataLen < 0
                    || dataLen > source.Length - offset - headersAfterThis)
                {
                    return false;
                }

                entries[i] = new Entry(type, term, index, source.Slice(offset, dataLen).ToArray());
                offset += dataLen;
            }

            result.Entries = entries;
        }

        if (hasSnapshot)
        {
            ulong snapIndex = ReadUInt64(source, ref offset);
            ulong snapTerm = ReadUInt64(source, ref offset);
            int dataLen = BinaryPrimitives.ReadInt32BigEndian(source.Slice(offset));
            offset += 4;
            if (dataLen < 0 || dataLen > source.Length - offset - ConfStateHeader)
            {
                return false;
            }

            ReadOnlySpan<byte> data = source.Slice(offset, dataLen);
            offset += dataLen;
            if (!TryReadConfState(source, ref offset, out ConfState? confState) || confState is null)
            {
                return false;
            }

            result.Snapshot = new Snapshot(new SnapshotMetadata(snapIndex, snapTerm, confState), data.ToArray());
        }

        if (offset != source.Length)
        {
            return false;
        }

        message = result;
        return true;
    }

    private static int ConfStateLength(ConfState confState)
    {
        return checked(ConfStateHeader
            + (confState.Voters.Count * sizeof(ulong))
            + (confState.Learners.Count * sizeof(ulong))
            + (confState.VotersOutgoing.Count * sizeof(ulong))
            + (confState.LearnersNext.Count * sizeof(ulong)));
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
        if (source.Length - offset < ConfStateHeader || source[offset] > 1)
        {
            return false;
        }

        bool autoLeave = source[offset++] != 0;
        if (!TryReadIds(source, ref offset, 3 * sizeof(int), out ulong[]? voters)
            || !TryReadIds(source, ref offset, 2 * sizeof(int), out ulong[]? learners)
            || !TryReadIds(source, ref offset, sizeof(int), out ulong[]? votersOutgoing)
            || !TryReadIds(source, ref offset, 0, out ulong[]? learnersNext))
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

    private static bool TryReadIds(ReadOnlySpan<byte> source, ref int offset, int reservedBytes, out ulong[]? ids)
    {
        ids = null;
        if (source.Length - offset < sizeof(int) + reservedBytes)
        {
            return false;
        }

        int count = BinaryPrimitives.ReadInt32BigEndian(source.Slice(offset));
        offset += 4;
        if (count < 0 || count > (source.Length - offset - reservedBytes) / sizeof(ulong))
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
        offset += 8;
    }

    private static ulong ReadUInt64(ReadOnlySpan<byte> source, ref int offset)
    {
        ulong value = BinaryPrimitives.ReadUInt64BigEndian(source.Slice(offset));
        offset += 8;
        return value;
    }
}
