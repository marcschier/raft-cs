// Copyright (c) marcschier. Licensed under the MIT License.

using NanoMsg;

namespace Raft.Transport.NanoMsg;

/// <summary>
/// A Raft transport that broadcasts opaque encoded message frames over the nanomsg/NNG BUS protocol.
/// </summary>
/// <remarks>
/// BUS delivers each sent frame to directly connected peers and does not echo a node's own sends. The Raft
/// consensus layer is responsible for filtering by recipient id; <see cref="SendAsync"/> ignores its routing hint.
/// </remarks>
public sealed class NanoMsgBusTransport : IRaftTransport
{
    private readonly NanoMsgBusTransportOptions _options;
    private readonly object _gate = new();
    private readonly List<string> _pendingPeers = [];
    private readonly CancellationTokenSource _stop = new();
    private BusSocket? _socket;
    private Task? _receiveLoop;
    private int _started;
    private int _disposed;
    private int _boundPort = -1;

    /// <summary>Initializes a NanoMsg BUS transport.</summary>
    /// <param name="options">The transport options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">No bind address or peers are configured, or a value is invalid.</exception>
    public NanoMsgBusTransport(NanoMsgBusTransportOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));

        if (string.IsNullOrWhiteSpace(_options.BindAddress) && _options.Peers.Count == 0)
        {
            throw new ArgumentException(
                "At least one of BindAddress or Peers must be configured.", nameof(options));
        }

        if (_options.MaxFrameLength <= 0)
        {
            throw new ArgumentException("MaxFrameLength must be positive.", nameof(options));
        }
    }

    /// <inheritdoc/>
    public event Action<ReadOnlyMemory<byte>>? FrameReceived;

    /// <summary>
    /// Gets the resolved local TCP port assigned after <see cref="StartAsync"/> binds a <c>tcp</c>
    /// endpoint, or <c>-1</c> before binding (and for non-tcp transports, which report <c>0</c>).
    /// </summary>
    public int BoundPort => Volatile.Read(ref _boundPort);

    /// <summary>Adds a peer endpoint to dial. May be called before or after <see cref="StartAsync"/>.</summary>
    /// <param name="address">The peer endpoint address, for example <c>tcp://10.0.0.2:5560</c>.</param>
    public void AddPeer(string address)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            throw new ArgumentException("The peer address must be non-empty.", nameof(address));
        }

        BusSocket? socket;
        lock (_gate)
        {
            socket = _socket;
            if (socket is null)
            {
                _pendingPeers.Add(address);
                return;
            }
        }

        socket.Connect(address);
    }

    /// <summary>Adds multiple peer endpoints to dial.</summary>
    /// <param name="addresses">The peer endpoint addresses.</param>
    public void AddPeers(IEnumerable<string> addresses)
    {
        foreach (string address in addresses ?? throw new ArgumentNullException(nameof(addresses)))
        {
            AddPeer(address);
        }
    }

    /// <inheritdoc/>
    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (Interlocked.Exchange(ref _started, 1) == 1)
        {
            return;
        }

        var socket = new BusSocket(_options.SocketOptions ?? new NanoSocketOptions());
        if (!string.IsNullOrWhiteSpace(_options.BindAddress))
        {
            int port = await socket.BindAsync(_options.BindAddress!, cancellationToken).ConfigureAwait(false);
            Volatile.Write(ref _boundPort, port);
        }

        string[] peers;
        lock (_gate)
        {
            _socket = socket;
            peers = [.. _options.Peers, .. _pendingPeers];
            _pendingPeers.Clear();
        }

        foreach (string peer in peers)
        {
            socket.Connect(peer);
        }

        _receiveLoop = ReceiveLoopAsync(socket, _stop.Token);
    }

    /// <inheritdoc/>
    public async ValueTask SendAsync(
        ulong recipient,
        ReadOnlyMemory<byte> frame,
        CancellationToken cancellationToken = default)
    {
        _ = recipient;
        if (frame.Length > _options.MaxFrameLength)
        {
            throw new ArgumentException(
                "The frame is larger than the maximum frame length.", nameof(frame));
        }

        BusSocket? socket = Volatile.Read(ref _socket);
        if (socket is null)
        {
            throw new InvalidOperationException("The transport has not started.");
        }

        try
        {
            await socket.SendAsync(frame, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested && IsTransientFault(ex))
        {
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _stop.Cancel();

        BusSocket? socket = Volatile.Read(ref _socket);
        if (socket is not null)
        {
            try
            {
                await socket.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex) when (IsExpectedFault(ex) || IsTransientFault(ex))
            {
            }
        }

        if (_receiveLoop is not null)
        {
            try
            {
                await _receiveLoop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        _stop.Dispose();
    }

    private async Task ReceiveLoopAsync(BusSocket socket, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            NanoMessage message;
            try
            {
                message = await socket.ReceiveAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (IsExpectedFault(ex))
            {
                return;
            }
            catch (Exception ex) when (IsTransientFault(ex))
            {
                await Task.Delay(10, CancellationToken.None).ConfigureAwait(false);
                continue;
            }

            using (message)
            {
                byte[] frame = message.Payload.ToArray();
                if (frame.Length <= _options.MaxFrameLength)
                {
                    FrameReceived?.Invoke(frame);
                }
            }
        }
    }

    private static bool IsExpectedFault(Exception ex) =>
        ex is OperationCanceledException or ObjectDisposedException;

    private static bool IsTransientFault(Exception ex) =>
        ex is InvalidOperationException or IOException;
}
