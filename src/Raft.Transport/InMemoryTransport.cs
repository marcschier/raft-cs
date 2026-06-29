// Copyright (c) marcschier. Licensed under the MIT License.

using System.Threading.Channels;

namespace Raft.Transport;

/// <summary>An in-process transport bound to a node in an <see cref="InMemoryNetwork"/>.</summary>
public sealed class InMemoryTransport : IRaftTransport
{
    private readonly InMemoryNetwork _network;
    private readonly Channel<byte[]> _inbox;
    private readonly CancellationTokenSource _stop = new();
    private Task? _receiveLoop;
    private int _pendingFrames;
    private int _started;
    private int _disposed;

    internal InMemoryTransport(InMemoryNetwork network, ulong id)
    {
        _network = network;
        Id = id;
        _inbox = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
    }

    /// <summary>Gets the node id this transport is registered as.</summary>
    public ulong Id { get; }

    /// <inheritdoc/>
    public event Action<ReadOnlyMemory<byte>>? FrameReceived;

    internal int PendingFrames => Volatile.Read(ref _pendingFrames);

    /// <inheritdoc/>
    public ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        if (Interlocked.Exchange(ref _started, 1) == 1)
        {
            return default;
        }

        _receiveLoop = ReceiveLoopAsync(_stop.Token);
        return default;
    }

    /// <inheritdoc/>
    public ValueTask SendAsync(
        ulong recipient,
        ReadOnlyMemory<byte> frame,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        _network.Send(Id, recipient, frame);
        return default;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _network.Unregister(Id, this);
        _inbox.Writer.TryComplete();
        _stop.Cancel();
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

    internal void Enqueue(ReadOnlyMemory<byte> frame)
    {
        Interlocked.Increment(ref _pendingFrames);
        if (!_inbox.Writer.TryWrite(frame.ToArray()))
        {
            Interlocked.Decrement(ref _pendingFrames);
            throw new InvalidOperationException("The in-memory transport is closed.");
        }
    }

    private async Task ReceiveLoopAsync(CancellationToken cancellationToken)
    {
        await foreach (byte[] frame in _inbox.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            try
            {
                FrameReceived?.Invoke(frame);
            }
            finally
            {
                Interlocked.Decrement(ref _pendingFrames);
            }
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) == 1)
        {
            throw new ObjectDisposedException(nameof(InMemoryTransport));
        }
    }
}
