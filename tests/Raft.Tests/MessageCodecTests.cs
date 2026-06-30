// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Messages;
using Raft.Storage;

namespace Raft.Tests;

public sealed class MessageCodecTests
{
    [Test]
    public async Task AppendMessageWithEntries_RoundTrips()
    {
        var message = new Message
        {
            Type = MessageType.Append,
            From = 3,
            To = 7,
            Term = 9,
            LogTerm = 8,
            Index = 42,
            Commit = 40,
            Entries = new[]
            {
                new Entry(EntryType.Normal, 9, 43, new byte[] { 1, 2, 3 }),
                new Entry(EntryType.ConfChangeV2, 9, 44, new byte[] { 9, 8 }),
            },
        };

        Message decoded = RoundTrip(message);

        await Assert.That(decoded.Type).IsEqualTo(MessageType.Append);
        await Assert.That(decoded.From).IsEqualTo((ulong)3);
        await Assert.That(decoded.To).IsEqualTo((ulong)7);
        await Assert.That(decoded.Index).IsEqualTo((ulong)42);
        await Assert.That(decoded.Entries.Count).IsEqualTo(2);
        await Assert.That(string.Join(",", decoded.Entries[0].Data.ToArray())).IsEqualTo("1,2,3");
        await Assert.That(decoded.Entries[1].Type).IsEqualTo(EntryType.ConfChangeV2);
    }

    [Test]
    public async Task VoteMessage_RoundTrips()
    {
        var message = new Message
        {
            Type = MessageType.RequestVoteResponse,
            From = 2,
            To = 1,
            Term = 5,
            Reject = true,
        };

        Message decoded = RoundTrip(message);

        await Assert.That(decoded.Type).IsEqualTo(MessageType.RequestVoteResponse);
        await Assert.That(decoded.Reject).IsTrue();
        await Assert.That(decoded.Term).IsEqualTo((ulong)5);
    }

    [Test]
    public async Task SnapshotMessage_RoundTrips()
    {
        var confState = new ConfState(new ulong[] { 1, 2, 3 }, new ulong[] { 4 });
        var snapshot = new Snapshot(new SnapshotMetadata(10, 4, confState), new byte[] { 7, 7, 7 });
        var message = new Message
        {
            Type = MessageType.Snapshot,
            From = 1,
            To = 4,
            Term = 4,
            Snapshot = snapshot,
        };

        Message decoded = RoundTrip(message);

        await Assert.That(decoded.Snapshot).IsNotNull();
        await Assert.That(decoded.Snapshot!.Metadata.Index).IsEqualTo((ulong)10);
        await Assert.That(string.Join(",", decoded.Snapshot.Data.ToArray())).IsEqualTo("7,7,7");
        await Assert.That(string.Join(",", decoded.Snapshot.Metadata.ConfState.Voters)).IsEqualTo("1,2,3");
        await Assert.That(string.Join(",", decoded.Snapshot.Metadata.ConfState.Learners)).IsEqualTo("4");
    }

    [Test]
    public async Task TryParse_ShortBuffer_ReturnsFalse()
    {
        await Assert.That(MessageCodec.TryParse(new byte[8], out _)).IsFalse();
    }

    [Test]
    public async Task TryParse_TruncatedEntries_ReturnsFalse()
    {
        var message = new Message
        {
            Type = MessageType.Append,
            From = 1,
            To = 2,
            Term = 3,
            Entries = new[] { new Entry(EntryType.Normal, 3, 1, new byte[] { 1, 2, 3, 4 }) },
        };
        var buffer = new byte[MessageCodec.EncodedLength(message)];
        _ = MessageCodec.TryWrite(message, buffer);

        // Chop the trailing entry payload so decoding must reject the frame.
        await Assert.That(MessageCodec.TryParse(buffer.AsSpan(0, buffer.Length - 2), out _)).IsFalse();
    }

    [Test]
    public async Task TryParse_TruncatedSnapshot_ReturnsFalse()
    {
        var snapshot = new Snapshot(
            new SnapshotMetadata(10, 4, new ConfState(new ulong[] { 1, 2 })), new byte[] { 7, 7, 7 });
        var message = new Message
        {
            Type = MessageType.Snapshot,
            From = 1,
            To = 2,
            Term = 4,
            Snapshot = snapshot,
        };
        var buffer = new byte[MessageCodec.EncodedLength(message)];
        _ = MessageCodec.TryWrite(message, buffer);

        // Chop the trailing ConfState so the snapshot decode must fail.
        await Assert.That(MessageCodec.TryParse(buffer.AsSpan(0, buffer.Length - 3), out _)).IsFalse();
    }

    [Test]
    public async Task TryWrite_DestinationTooSmall_ReturnsFalse()
    {
        var message = new Message { Type = MessageType.Heartbeat, From = 1, To = 2, Term = 1 };
        var tooSmall = new byte[MessageCodec.EncodedLength(message) - 1];
        await Assert.That(MessageCodec.TryWrite(message, tooSmall)).IsFalse();
    }

    private static Message RoundTrip(Message message)
    {
        var buffer = new byte[MessageCodec.EncodedLength(message)];
        _ = MessageCodec.TryWrite(message, buffer);
        _ = MessageCodec.TryParse(buffer, out Message? decoded);
        return decoded ?? throw new InvalidOperationException("decode failed");
    }
}
