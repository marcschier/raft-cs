// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Storage;
using Raft.Transport;

namespace Raft.Tests;

public sealed class ValueTypesAndCodecTests
{
    [Test]
    public async Task Entry_Equality()
    {
        var a = new Entry(EntryType.Normal, 1, 1, new byte[] { 1, 2 });
        var b = new Entry(EntryType.Normal, 1, 1, new byte[] { 1, 2 });
        var c = new Entry(EntryType.Normal, 2, 1, new byte[] { 1, 2 });

        await Assert.That(a == b).IsTrue();
        await Assert.That(a != c).IsTrue();
        await Assert.That(a.Equals((object)b)).IsTrue();
        await Assert.That(a.GetHashCode()).IsEqualTo(b.GetHashCode());
    }

    [Test]
    public async Task HardAndSoftState_Equality()
    {
        await Assert.That(new HardState(1, 2, 3) == new HardState(1, 2, 3)).IsTrue();
        await Assert.That(new HardState(1, 2, 3) != new HardState(1, 2, 4)).IsTrue();
        await Assert.That(new SoftState(1, RaftRole.Leader) == new SoftState(1, RaftRole.Leader)).IsTrue();
        await Assert.That(new SoftState(1, RaftRole.Leader) != new SoftState(2, RaftRole.Leader)).IsTrue();
        await Assert.That(new SoftState(1, RaftRole.Leader).Equals((object)new SoftState(1, RaftRole.Leader))).IsTrue();
    }

    [Test]
    public async Task ConfState_EqualityAndJoint()
    {
        var a = new ConfState(new ulong[] { 1, 2 }, new ulong[] { 3 });
        var b = new ConfState(new ulong[] { 2, 1 }, new ulong[] { 3 });
        var joint = new ConfState(new ulong[] { 1, 2 }, votersOutgoing: new ulong[] { 3 });

        await Assert.That(a.Equals(b)).IsTrue();
        await Assert.That(a.Equals(joint)).IsFalse();
        await Assert.That(joint.IsJoint).IsTrue();
        await Assert.That(a.GetHashCode()).IsEqualTo(b.GetHashCode());
    }

    [Test]
    public async Task RaftConfig_Validate_RejectsBadValues()
    {
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        await Assert.That(() => new RaftCore(new RaftConfig { Id = 0 }, storage)).Throws<ArgumentException>();
        await Assert.That(() => new RaftCore(
            new RaftConfig { Id = 1, ElectionTick = 1, HeartbeatTick = 5 }, storage)).Throws<ArgumentException>();
    }

    [Test]
    public async Task FrameCodec_RoundTripsAndRejectsPartial()
    {
        var payload = new byte[] { 9, 8, 7, 6 };
        var buffer = new byte[FrameCodec.FrameLength(payload.Length)];
        await Assert.That(FrameCodec.TryWriteFrame(payload, buffer)).IsTrue();

        bool read = FrameCodec.TryReadFrame(buffer, out ReadOnlySpan<byte> decoded, out int consumed);
        byte[] decodedBytes = decoded.ToArray();
        bool partialRejected = !FrameCodec.TryReadFrame(buffer.AsSpan(0, 2), out _, out _);

        await Assert.That(read).IsTrue();
        await Assert.That(consumed).IsEqualTo(buffer.Length);
        await Assert.That(string.Join(",", decodedBytes)).IsEqualTo("9,8,7,6");
        await Assert.That(partialRejected).IsTrue();
    }
}
