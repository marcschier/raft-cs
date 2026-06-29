// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Transport;

/// <summary>
/// The replaceable network layer for a Raft node. Implementations deliver encoded message frames between nodes; the
/// consensus driver serializes each <see cref="Raft.Messages.Message"/> with
/// <see cref="Raft.Messages.MessageCodec"/> and routes it by recipient node id.
/// </summary>
public interface IRaftTransport : IAsyncDisposable
{
    /// <summary>Raised when a complete encoded message frame is received from a peer.</summary>
    event Action<ReadOnlyMemory<byte>>? FrameReceived;

    /// <summary>Starts the transport and any receive loops it owns.</summary>
    /// <param name="cancellationToken">A token that cancels the start operation.</param>
    /// <returns>A value task that completes when the transport has started.</returns>
    ValueTask StartAsync(CancellationToken cancellationToken = default);

    /// <summary>Sends an encoded message frame to a peer node.</summary>
    /// <param name="recipient">The recipient node id (a hint; broadcast transports filter the frame).</param>
    /// <param name="frame">The encoded message frame.</param>
    /// <param name="cancellationToken">A token that cancels the send operation.</param>
    /// <returns>A value task that completes when the frame has been queued or sent.</returns>
    ValueTask SendAsync(ulong recipient, ReadOnlyMemory<byte> frame, CancellationToken cancellationToken = default);
}
