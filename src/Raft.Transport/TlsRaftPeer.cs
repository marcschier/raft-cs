// Copyright (c) marcschier. Licensed under the MIT License.

using System.Net;
using System.Security.Cryptography.X509Certificates;

namespace Raft.Transport;

/// <summary>Associates a Raft member and dial address with its pinned TLS leaf certificate.</summary>
public sealed class TlsRaftPeer
{
    /// <summary>Initializes a peer with a statically configured certificate identity.</summary>
    /// <param name="memberId">The nonzero remote Raft member id.</param>
    /// <param name="endPoint">The remote TCP endpoint.</param>
    /// <param name="certificate">The remote leaf certificate (no private key needed).</param>
    public TlsRaftPeer(ulong memberId, IPEndPoint endPoint, X509Certificate2 certificate)
    {
        MemberId = memberId;
        EndPoint = endPoint ?? throw new ArgumentNullException(nameof(endPoint));
        Certificate = certificate ?? throw new ArgumentNullException(nameof(certificate));
    }

    /// <summary>Gets the remote Raft member id.</summary>
    public ulong MemberId { get; }

    /// <summary>Gets the remote TCP endpoint, copied by the transport at construction.</summary>
    public IPEndPoint EndPoint { get; }

    /// <summary>Gets the leaf certificate used to create the pin at transport construction.</summary>
    public X509Certificate2 Certificate { get; }
}
