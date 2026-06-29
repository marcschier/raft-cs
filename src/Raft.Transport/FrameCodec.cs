// Copyright (c) marcschier. Licensed under the MIT License.

using System.Buffers.Binary;

namespace Raft.Transport;

/// <summary>Encodes and decodes opaque payloads with a four-byte big-endian length prefix.</summary>
public static class FrameCodec
{
    /// <summary>Calculates the encoded frame length for a payload length.</summary>
    /// <param name="payload">The payload length in bytes.</param>
    /// <returns>The length-prefixed frame length in bytes.</returns>
    public static int FrameLength(int payload) => checked(payload + sizeof(int));

    /// <summary>Attempts to write a length-prefixed frame.</summary>
    /// <param name="payload">The opaque frame payload.</param>
    /// <param name="dest">The destination buffer.</param>
    /// <returns>
    /// <see langword="true"/> when the frame fits in <paramref name="dest"/>; otherwise <see langword="false"/>.
    /// </returns>
    public static bool TryWriteFrame(ReadOnlySpan<byte> payload, Span<byte> dest)
    {
        int length = FrameLength(payload.Length);
        if (dest.Length < length)
        {
            return false;
        }

        BinaryPrimitives.WriteInt32BigEndian(dest, payload.Length);
        payload.CopyTo(dest.Slice(sizeof(int)));
        return true;
    }

    /// <summary>Attempts to read one complete length-prefixed frame from a buffer.</summary>
    /// <param name="buf">The buffer containing zero or more bytes from a framed stream.</param>
    /// <param name="payload">The decoded payload when a full frame is available.</param>
    /// <param name="consumed">The number of bytes consumed when a full frame is available.</param>
    /// <returns><see langword="true"/> when a full frame is available; otherwise <see langword="false"/>.</returns>
    public static bool TryReadFrame(
        ReadOnlySpan<byte> buf,
        out ReadOnlySpan<byte> payload,
        out int consumed)
    {
        payload = default;
        consumed = 0;
        if (buf.Length < sizeof(int))
        {
            return false;
        }

        int length = BinaryPrimitives.ReadInt32BigEndian(buf);
        if (length < 0 || buf.Length - sizeof(int) < length)
        {
            return false;
        }

        payload = buf.Slice(sizeof(int), length);
        consumed = sizeof(int) + length;
        return true;
    }
}
