// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Transport.Tests;

public sealed class FrameCodecTests
{
    [Test]
    public async Task FrameCodec_Roundtrips_Random_Payloads()
    {
        var random = new Random(12345);
        for (int i = 0; i < 100; i++)
        {
            byte[] payload = new byte[random.Next(0, 1024)];
            random.NextBytes(payload);
            byte[] frame = new byte[FrameCodec.FrameLength(payload.Length)];

            bool wrote = FrameCodec.TryWriteFrame(payload, frame);
            bool read = FrameCodec.TryReadFrame(frame, out ReadOnlySpan<byte> decoded, out int consumed);
            byte[] decodedBytes = decoded.ToArray();

            await Assert.That(wrote).IsTrue();
            await Assert.That(read).IsTrue();
            await Assert.That(consumed).IsEqualTo(frame.Length);
            await Assert.That(decodedBytes).IsEquivalentTo(payload);
        }
    }

    [Test]
    public async Task TryReadFrame_Returns_False_For_Partial_Frame()
    {
        byte[] frame = new byte[FrameCodec.FrameLength(3)];
        bool wrote = FrameCodec.TryWriteFrame([1, 2, 3], frame);

        bool read = FrameCodec.TryReadFrame(frame.AsSpan(0, frame.Length - 1), out _, out int consumed);

        await Assert.That(wrote).IsTrue();
        await Assert.That(read).IsFalse();
        await Assert.That(consumed).IsEqualTo(0);
    }
}
