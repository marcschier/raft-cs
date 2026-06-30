// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Transport;

/// <summary>A single encoded frame destined for one peer node.</summary>
public readonly struct OutboundFrame : IEquatable<OutboundFrame>
{
    /// <summary>Initializes a new instance of the <see cref="OutboundFrame"/> struct.</summary>
    /// <param name="recipient">The recipient node id (a hint; broadcast transports filter the frame).</param>
    /// <param name="frame">The encoded message frame.</param>
    public OutboundFrame(ulong recipient, ReadOnlyMemory<byte> frame)
    {
        Recipient = recipient;
        Frame = frame;
    }

    /// <summary>Gets the recipient node id.</summary>
    public ulong Recipient { get; }

    /// <summary>Gets the encoded message frame.</summary>
    public ReadOnlyMemory<byte> Frame { get; }

    /// <summary>Compares two frames for equality.</summary>
    /// <param name="left">The left frame.</param>
    /// <param name="right">The right frame.</param>
    /// <returns><see langword="true"/> when the frames are equal.</returns>
    public static bool operator ==(OutboundFrame left, OutboundFrame right) => left.Equals(right);

    /// <summary>Compares two frames for inequality.</summary>
    /// <param name="left">The left frame.</param>
    /// <param name="right">The right frame.</param>
    /// <returns><see langword="true"/> when the frames differ.</returns>
    public static bool operator !=(OutboundFrame left, OutboundFrame right) => !left.Equals(right);

    /// <inheritdoc/>
    public bool Equals(OutboundFrame other) => Recipient == other.Recipient && Frame.Equals(other.Frame);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is OutboundFrame other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine(Recipient, Frame.Length);
}

/// <summary>
/// An optional extension of <see cref="IRaftTransport"/> for transports that can send several frames in a single call.
/// The asynchronous driver coalesces all of a ready cycle's outbound frames into one
/// <see cref="SendManyAsync"/> invocation, reducing the number of synchronized network operations per cycle. Drivers
/// fall back to per-frame <see cref="IRaftTransport.SendAsync"/> when a transport does not implement this interface.
/// </summary>
public interface IRaftBatchTransport : IRaftTransport
{
    /// <summary>Sends a batch of encoded frames to their peer nodes.</summary>
    /// <param name="frames">The frames to send, in order.</param>
    /// <param name="cancellationToken">A token that cancels the send operation.</param>
    /// <returns>A value task that completes when the frames have been queued or sent.</returns>
    ValueTask SendManyAsync(IReadOnlyList<OutboundFrame> frames, CancellationToken cancellationToken = default);
}
