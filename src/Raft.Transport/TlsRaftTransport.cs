// Copyright (c) marcschier. Licensed under the MIT License.

using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Channels;

namespace Raft.Transport;

/// <summary>A bounded, mutually authenticated TCP/TLS transport for a statically configured Raft member.</summary>
public sealed class TlsRaftTransport : IRaftTransport
{
    // The MessageCodec wire header contains the routing fields before any variable-length data.
    private const int MessageHeaderLength = 1 + (8 * sizeof(ulong)) + 1 + sizeof(int);
    private readonly TcpListener _listener;
    private readonly X509Certificate2 _localCertificate;
    private readonly TlsRaftTransportOptions _options;
    private readonly Dictionary<ulong, Peer> _peersById = new();
    private readonly Dictionary<string, Peer> _peersByPin = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private readonly CancellationTokenSource _stop = new();
    private Task? _workers;
    private int _started;
    private int _disposed;

    /// <summary>Initializes a transport without starting its listener.</summary>
    /// <param name="memberId">The nonzero local Raft member id.</param>
    /// <param name="listenEndPoint">The local TCP endpoint. Port zero selects an available port.</param>
    /// <param name="localCertificate">
    /// The local certificate with private key for both TLS client and server authentication. The caller retains
    /// ownership and must keep it usable until transport disposal completes.
    /// </param>
    /// <param name="peers">Remote members with unique ids and certificate keys, excluding the local member.</param>
    /// <param name="options">Optional transport limits and additional certificate policy.</param>
    public TlsRaftTransport(
        ulong memberId,
        IPEndPoint listenEndPoint,
        X509Certificate2 localCertificate,
        IEnumerable<TlsRaftPeer> peers,
        TlsRaftTransportOptions? options = null)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfZero(memberId);
#else
        if (memberId == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(memberId));
        }
#endif

        if (listenEndPoint is null)
        {
            throw new ArgumentNullException(nameof(listenEndPoint));
        }

        if (peers is null)
        {
            throw new ArgumentNullException(nameof(peers));
        }

        _localCertificate = localCertificate ?? throw new ArgumentNullException(nameof(localCertificate));
        if (!_localCertificate.HasPrivateKey)
        {
            throw new ArgumentException("The local TLS certificate needs a private key.", nameof(localCertificate));
        }

        _options = options ?? new TlsRaftTransportOptions();
        ValidateOptions(_options);
        MemberId = memberId;
        _listener = new TcpListener(CopyEndPoint(listenEndPoint));

        var keys = new HashSet<string>(StringComparer.Ordinal) { PublicKey(_localCertificate) };
        foreach (TlsRaftPeer peer in peers)
        {
            if (peer is null || peer.MemberId == 0 || peer.MemberId == memberId || peer.EndPoint.Port == 0
                || _peersById.ContainsKey(peer.MemberId) || !keys.Add(PublicKey(peer.Certificate)))
            {
                throw new ArgumentException("Peers need distinct ids, nonzero ports and distinct TLS keys.",
                    nameof(peers));
            }

            var state = new Peer(peer.MemberId, CopyEndPoint(peer.EndPoint), _options.SendQueueCapacity);
            _peersById.Add(peer.MemberId, state);
            _peersByPin.Add(CertificatePin(peer.Certificate), state);
        }
    }

    /// <summary>Gets the nonzero local Raft member id.</summary>
    public ulong MemberId { get; }

    /// <summary>Gets the listening port, or zero before a successful start.</summary>
    public int BoundPort { get; private set; }

    /// <summary>
    /// Raised with an owned frame after TLS authentication and routing checks. Handlers can run concurrently
    /// for different peers; they must return promptly and must not wait for transport disposal.
    /// </summary>
    public event Action<ReadOnlyMemory<byte>>? FrameReceived;

    /// <inheritdoc/>
    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            if (_started != 0)
            {
                return;
            }

            _listener.Start(_options.MaxPendingConnections);
            BoundPort = ((IPEndPoint)_listener.LocalEndpoint).Port;
            CancellationToken stop = _stop.Token;
            var workers = new List<Task>();
            foreach (Peer peer in _peersById.Values)
            {
                workers.Add(Task.Run(() => SendLoopAsync(peer, stop), CancellationToken.None));
                workers.Add(Task.Run(() => ReceiveLoopAsync(peer, stop), CancellationToken.None));
            }

            for (int i = 0; i < _options.MaxPendingConnections; i++)
            {
                workers.Add(Task.Run(() => AcceptLoopAsync(stop), CancellationToken.None));
            }

            _workers = Task.WhenAll(workers);
            Volatile.Write(ref _started, 1);
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    /// <summary>
    /// Copies and queues a message for its configured recipient without waiting for network I/O. A connection
    /// failure drops the active send; Raft is responsible for retransmission.
    /// </summary>
    /// <param name="recipient">The configured remote member id, matching the encoded recipient.</param>
    /// <param name="frame">A message encoded with <see cref="Messages.MessageCodec"/>.</param>
    /// <param name="cancellationToken">A token that cancels queue admission, not a queued send.</param>
    /// <returns>A completed value task when the frame has been queued.</returns>
    /// <exception cref="ArgumentException">The frame length or routing is invalid, or the peer is unknown.</exception>
    /// <exception cref="IOException">The recipient's send queue is full.</exception>
    public ValueTask SendAsync(
        ulong recipient,
        ReadOnlyMemory<byte> frame,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        if (Volatile.Read(ref _started) == 0)
        {
            throw new InvalidOperationException("The TLS transport has not been started.");
        }

        if (!_peersById.TryGetValue(recipient, out Peer? peer))
        {
            throw new ArgumentException("The recipient is not a configured peer.", nameof(recipient));
        }

        if (!IsFrameLengthAllowed(frame.Length))
        {
            throw new ArgumentException("The encoded message length is outside transport limits.", nameof(frame));
        }

        peer.Enqueue(frame, MemberId);
        return default;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await _lifecycle.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed != 0)
            {
                return;
            }

            Volatile.Write(ref _disposed, 1);
            using CancellationTokenSource stop = _stop;
            foreach (Peer peer in _peersById.Values)
            {
                peer.Complete();
            }

            stop.Cancel();
            _listener.Stop();
            if (_workers is not null)
            {
                await _workers.ConfigureAwait(false);
            }
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    private static void ValidateOptions(TlsRaftTransportOptions options)
    {
        if (options.MaxFrameLength is < MessageHeaderLength or > int.MaxValue - sizeof(int)
            || options.SendQueueCapacity <= 0
            || options.MaxPendingConnections < 2
            || options.IoTimeout < TimeSpan.FromMilliseconds(1)
            || options.IoTimeout > TimeSpan.FromMilliseconds(int.MaxValue))
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Invalid TLS transport limits.");
        }
    }

    private static IPEndPoint CopyEndPoint(IPEndPoint value)
    {
        IPAddress address = value.Address;
        IPAddress copy = address.AddressFamily == AddressFamily.InterNetworkV6
            ? new IPAddress(address.GetAddressBytes(), address.ScopeId)
            : new IPAddress(address.GetAddressBytes());
        return new IPEndPoint(copy, value.Port);
    }

    private static string PublicKey(X509Certificate certificate) => Convert.ToBase64String(certificate.GetPublicKey());

    private static string CertificatePin(X509Certificate certificate)
    {
#if NET8_0_OR_GREATER
        return Convert.ToBase64String(certificate.GetCertHash(HashAlgorithmName.SHA256));
#else
        using SHA256 sha256 = SHA256.Create();
        return Convert.ToBase64String(sha256.ComputeHash(certificate.GetRawCertData()));
#endif
    }

    private static bool HasRoute(ReadOnlySpan<byte> frame, ulong sender, ulong recipient) =>
        BinaryPrimitives.ReadUInt64BigEndian(frame.Slice(1)) == sender
        && BinaryPrimitives.ReadUInt64BigEndian(frame.Slice(1 + sizeof(ulong))) == recipient;

    private static bool IsConnectionFailure(Exception exception) =>
        exception is IOException or SocketException or AuthenticationException
            or InvalidOperationException or OperationCanceledException;

    private static async Task<bool> ReadExactlyAsync(
        SslStream stream,
        byte[] buffer,
        CancellationToken cancellationToken)
    {
        int offset = 0;
        while (offset < buffer.Length)
        {
#if NETSTANDARD2_0
            int read = await stream.ReadAsync(buffer, offset, buffer.Length - offset, cancellationToken)
                .ConfigureAwait(false);
#else
            int read = await stream.ReadAsync(buffer.AsMemory(offset), cancellationToken).ConfigureAwait(false);
#endif
            if (read == 0)
            {
                return false;
            }

            offset += read;
        }

        return true;
    }

#if NET8_0_OR_GREATER
    private static X509ChainPolicy CreateChainPolicy() => new()
    {
        DisableCertificateDownloads = true,
        RevocationMode = X509RevocationMode.NoCheck,
    };
#endif

    private async Task AcceptLoopAsync(CancellationToken stop)
    {
        while (!stop.IsCancellationRequested)
        {
            TcpClient? client;
            try
            {
#if NET8_0_OR_GREATER
                client = await _listener.AcceptTcpClientAsync(stop).ConfigureAwait(false);
#else
                client = await _listener.AcceptTcpClientAsync().ConfigureAwait(false);
#endif
            }
            catch (Exception ex) when (stop.IsCancellationRequested
                && ex is SocketException or InvalidOperationException or OperationCanceledException)
            {
                return;
            }
            catch (SocketException)
            {
                try
                {
                    await Task.Delay(_options.IoTimeout, stop).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    return;
                }

                continue;
            }

            Connection? connection = null;
            try
            {
                connection = await AuthenticateAsync(client, null, stop).ConfigureAwait(false);
                if (connection.Peer.TryAdmit(connection))
                {
                    client = null;
                    connection = null;
                }
            }
            catch (Exception ex) when (IsConnectionFailure(ex))
            {
            }
            finally
            {
                if (connection is null)
                {
                    client?.Dispose();
                }
                else
                {
                    connection.Dispose();
                }
            }
        }
    }

    private async Task ReceiveLoopAsync(Peer peer, CancellationToken stop)
    {
        try
        {
            while (await peer.Inbound.Reader.WaitToReadAsync(stop).ConfigureAwait(false))
            {
                if (peer.Inbound.Reader.TryRead(out Connection? connection))
                {
                    try
                    {
                        await ReceiveFramesAsync(connection, stop).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (IsConnectionFailure(ex))
                    {
                    }
                    finally
                    {
                        connection.Dispose();
                        peer.ReleaseInbound();
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
        }
        finally
        {
            if (peer.Inbound.Reader.TryRead(out Connection? connection))
            {
                connection.Dispose();
            }
        }
    }

    private async Task ReceiveFramesAsync(Connection connection, CancellationToken stop)
    {
        byte[] prefix = new byte[sizeof(int)];
        while (!stop.IsCancellationRequested)
        {
            byte[]? frame = await ReadFrameAsync(connection, prefix, stop).ConfigureAwait(false);
            if (frame is null || !HasRoute(frame, connection.Peer.Id, MemberId) || stop.IsCancellationRequested)
            {
                return;
            }

            try
            {
                FrameReceived?.Invoke(frame);
            }
            catch (Exception)
            {
                // A failed consumer must not leave the peer's receive slot occupied.
                return;
            }
        }
    }

    private async Task<byte[]?> ReadFrameAsync(Connection connection, byte[] prefix, CancellationToken stop)
    {
        using CancellationTokenSource deadline = CreateDeadline(stop);
        using CancellationTokenRegistration abort = AbortOnCancel(connection.Client, deadline.Token);
        if (!await ReadExactlyAsync(connection.Stream, prefix, deadline.Token).ConfigureAwait(false))
        {
            return null;
        }

        int length = BinaryPrimitives.ReadInt32BigEndian(prefix);
        if (!IsFrameLengthAllowed(length))
        {
            return null;
        }

        var frame = new byte[length];
        if (!await ReadExactlyAsync(connection.Stream, frame, deadline.Token).ConfigureAwait(false))
        {
            return null;
        }

        deadline.Token.ThrowIfCancellationRequested();
        return frame;
    }

    private async Task SendLoopAsync(Peer peer, CancellationToken stop)
    {
        Connection? connection = null;
        try
        {
            while (await peer.Outbound.Reader.WaitToReadAsync(stop).ConfigureAwait(false))
            {
                if (!peer.TryDequeue(out byte[]? frame))
                {
                    continue;
                }

                try
                {
                    stop.ThrowIfCancellationRequested();
                    if (connection is not null && connection.HasPendingRead)
                    {
                        connection.Dispose();
                        connection = null;
                    }

                    connection ??= await ConnectAsync(peer, stop).ConfigureAwait(false);
                    await WriteFrameAsync(connection, frame, stop).ConfigureAwait(false);
                }
                catch (Exception ex) when (IsConnectionFailure(ex))
                {
                    connection?.Dispose();
                    connection = null;
                }
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
        }
        finally
        {
            connection?.Dispose();
            while (peer.TryDequeue(out _))
            {
            }
        }
    }

    private async Task WriteFrameAsync(Connection connection, byte[] frame, CancellationToken stop)
    {
        using CancellationTokenSource deadline = CreateDeadline(stop);
        using CancellationTokenRegistration abort = AbortOnCancel(connection.Client, deadline.Token);
#if NETSTANDARD2_0
        await connection.Stream.WriteAsync(frame, 0, frame.Length, deadline.Token).ConfigureAwait(false);
#else
        await connection.Stream.WriteAsync(frame.AsMemory(), deadline.Token).ConfigureAwait(false);
#endif
    }

    private async Task<Connection> ConnectAsync(Peer peer, CancellationToken stop)
    {
        var client = new TcpClient(peer.EndPoint.AddressFamily);
        try
        {
            return await AuthenticateAsync(client, peer, stop).ConfigureAwait(false);
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    private async Task<Connection> AuthenticateAsync(TcpClient client, Peer? expectedPeer, CancellationToken stop)
    {
        using CancellationTokenSource deadline = CreateDeadline(stop);
        using CancellationTokenRegistration abort = AbortOnCancel(client, deadline.Token);
        deadline.Token.ThrowIfCancellationRequested();
        if (expectedPeer is not null)
        {
#if NET8_0_OR_GREATER
            await client.ConnectAsync(expectedPeer.EndPoint, deadline.Token).ConfigureAwait(false);
#else
            await client.ConnectAsync(expectedPeer.EndPoint.Address, expectedPeer.EndPoint.Port).ConfigureAwait(false);
#endif
        }

        deadline.Token.ThrowIfCancellationRequested();
        client.NoDelay = true;
        Peer? authenticatedPeer = null;
        var stream = new SslStream(client.GetStream(), leaveInnerStreamOpen: true, (_, certificate, chain, errors) =>
        {
            Peer? peer = MatchPeer(certificate, chain, errors, expectedPeer ?? authenticatedPeer);
            if (peer is null)
            {
                return false;
            }

            authenticatedPeer = peer;
            return true;
        });
        try
        {
            if (expectedPeer is null)
            {
                await AuthenticateServerAsync(stream, deadline.Token).ConfigureAwait(false);
            }
            else
            {
                await AuthenticateClientAsync(stream, expectedPeer, deadline.Token).ConfigureAwait(false);
            }

            deadline.Token.ThrowIfCancellationRequested();
            if (authenticatedPeer is null || !stream.IsMutuallyAuthenticated || !stream.IsEncrypted)
            {
                throw new AuthenticationException("TLS did not authenticate the configured Raft peer.");
            }

            return new Connection(client, stream, authenticatedPeer);
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    private Task AuthenticateServerAsync(SslStream stream, CancellationToken cancellationToken)
    {
#if NET8_0_OR_GREATER
        return stream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
        {
            ServerCertificate = _localCertificate,
            ClientCertificateRequired = true,
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
            AllowRenegotiation = false,
            AllowTlsResume = false,
            CertificateChainPolicy = CreateChainPolicy(),
        }, cancellationToken);
#else
        cancellationToken.ThrowIfCancellationRequested();
        return stream.AuthenticateAsServerAsync(_localCertificate, true, SslProtocols.Tls12, false);
#endif
    }

    private Task AuthenticateClientAsync(SslStream stream, Peer peer, CancellationToken cancellationToken)
    {
        var certificates = new X509CertificateCollection { _localCertificate };
        string targetHost = peer.EndPoint.Address.ToString();
#if NET8_0_OR_GREATER
        return stream.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
        {
            TargetHost = targetHost,
            ClientCertificates = certificates,
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
            AllowRenegotiation = false,
            AllowTlsResume = false,
            CertificateChainPolicy = CreateChainPolicy(),
        }, cancellationToken);
#else
        cancellationToken.ThrowIfCancellationRequested();
        return stream.AuthenticateAsClientAsync(targetHost, certificates, SslProtocols.Tls12, false);
#endif
    }

    private Peer? MatchPeer(X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors, Peer? expectedPeer)
    {
        if (certificate is null || !_peersByPin.TryGetValue(CertificatePin(certificate), out Peer? peer)
            || (expectedPeer is not null && !ReferenceEquals(peer, expectedPeer)))
        {
            return null;
        }

        // SslStream normally supplies X509Certificate2; a legacy provider can supply its base type.
        using X509Certificate2? copy = certificate is X509Certificate2 ? null : ToCertificate2(certificate);
        var leaf = copy ?? (X509Certificate2)certificate;
        DateTime now = DateTime.UtcNow;
        if (now < leaf.NotBefore.ToUniversalTime() || now > leaf.NotAfter.ToUniversalTime())
        {
            return null;
        }

        try
        {
            return _options.ValidatePeerCertificate?.Invoke(peer.Id, leaf, chain, errors) != false ? peer : null;
        }
        catch (Exception)
        {
            return null;
        }
    }

    private static X509Certificate2 ToCertificate2(X509Certificate certificate)
    {
#if NET9_0_OR_GREATER
        return X509CertificateLoader.LoadCertificate(certificate.GetRawCertData());
#else
        return new X509Certificate2(certificate);
#endif
    }

    private CancellationTokenSource CreateDeadline(CancellationToken stop)
    {
        var deadline = CancellationTokenSource.CreateLinkedTokenSource(stop);
        deadline.CancelAfter(_options.IoTimeout);
        return deadline;
    }

    private static CancellationTokenRegistration AbortOnCancel(TcpClient client, CancellationToken cancellationToken) =>
        cancellationToken.Register(static state => ((TcpClient)state!).Dispose(), client);

    private bool IsFrameLengthAllowed(int length) =>
        length >= MessageHeaderLength && length <= _options.MaxFrameLength;

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(TlsRaftTransport));
        }
    }

    private sealed class Connection(TcpClient client, SslStream stream, Peer peer) : IDisposable
    {
        private readonly Socket _socket = client.Client;

        public TcpClient Client { get; } = client;

        public SslStream Stream { get; } = stream;

        public Peer Peer { get; } = peer;

        public bool HasPendingRead => _socket.Poll(0, SelectMode.SelectRead);

        public void Dispose()
        {
            Client.Dispose();
            Stream.Dispose();
        }
    }

    private sealed class Peer(ulong id, IPEndPoint endPoint, int capacity)
    {
        private int _queued;
        private int _inboundOccupied;

        public ulong Id { get; } = id;

        public IPEndPoint EndPoint { get; } = endPoint;

        public Channel<byte[]> Outbound { get; } = Channel.CreateBounded<byte[]>(new BoundedChannelOptions(capacity)
        {
            SingleReader = true,
        });

        public Channel<Connection> Inbound { get; } = Channel.CreateBounded<Connection>(new BoundedChannelOptions(1)
        {
            SingleReader = true,
        });

        public void Enqueue(ReadOnlyMemory<byte> frame, ulong sender)
        {
            // Reserve capacity before copying so concurrent callers cannot allocate beyond the queue limit.
            int queuedCount;
            do
            {
                queuedCount = Volatile.Read(ref _queued);
                if (queuedCount == capacity)
                {
                    throw new IOException("The Raft peer's outbound queue is full.");
                }
            }
            while (Interlocked.CompareExchange(ref _queued, queuedCount + 1, queuedCount) != queuedCount);

            bool queued = false;
            try
            {
                var framed = new byte[FrameCodec.FrameLength(frame.Length)];
                FrameCodec.TryWriteFrame(frame.Span, framed);
                if (!HasRoute(framed.AsSpan(sizeof(int)), sender, Id))
                {
                    throw new ArgumentException("The message sender or recipient does not match the route.",
                        nameof(frame));
                }

                queued = Outbound.Writer.TryWrite(framed);
                if (!queued)
                {
                    throw new ObjectDisposedException(nameof(TlsRaftTransport));
                }
            }
            finally
            {
                if (!queued)
                {
                    Interlocked.Decrement(ref _queued);
                }
            }
        }

        public bool TryDequeue([NotNullWhen(true)] out byte[]? frame)
        {
            if (!Outbound.Reader.TryRead(out frame))
            {
                return false;
            }

            Interlocked.Decrement(ref _queued);
            return true;
        }

        public bool TryAdmit(Connection connection)
        {
            if (Interlocked.CompareExchange(ref _inboundOccupied, 1, 0) != 0)
            {
                return false;
            }

            if (Inbound.Writer.TryWrite(connection))
            {
                return true;
            }

            ReleaseInbound();
            return false;
        }

        public void ReleaseInbound() => Volatile.Write(ref _inboundOccupied, 0);

        public void Complete()
        {
            Outbound.Writer.TryComplete();
            Inbound.Writer.TryComplete();
        }
    }
}
