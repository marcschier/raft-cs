// Copyright (c) marcschier. Licensed under the MIT License.

using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Raft.Transport;

/// <summary>Configures admission, framing and certificate policy for <see cref="TlsRaftTransport"/>.</summary>
public sealed class TlsRaftTransportOptions
{
    /// <summary>Gets the maximum encoded message size, excluding the four-byte framing prefix.</summary>
    public int MaxFrameLength { get; init; } = 4 * 1024 * 1024;

    /// <summary>Gets the maximum number of queued frames per peer, excluding one active send.</summary>
    public int SendQueueCapacity { get; init; } = 8;

    /// <summary>
    /// Gets the maximum number of inbound TLS handshakes in progress. At least two are required so one
    /// stalled handshake cannot prevent another from starting. Also used as the TCP listener backlog.
    /// </summary>
    public int MaxPendingConnections { get; init; } = 16;

    /// <summary>
    /// Gets the deadline for each inbound handshake, outbound connect plus handshake, complete frame read, or
    /// complete frame write. A connection with no inbound frame before this deadline is closed.
    /// </summary>
    public TimeSpan IoTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets an optional additional certificate policy, called with the pinned member id, leaf, platform chain
    /// and TLS policy errors. It cannot override a pin mismatch, an expired certificate or the absence of a
    /// certificate. Without it, trust is by pin and validity period, not by host name or platform chain.
    /// It must return promptly, be thread-safe and not retain or dispose the borrowed certificate or chain.
    /// </summary>
    public Func<ulong, X509Certificate2, X509Chain?, SslPolicyErrors, bool>? ValidatePeerCertificate { get; init; }
}
