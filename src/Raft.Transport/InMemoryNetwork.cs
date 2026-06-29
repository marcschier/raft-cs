// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Transport;

/// <summary>Provides an in-process, node-id keyed hub of <see cref="InMemoryTransport"/> peers.</summary>
public sealed class InMemoryNetwork : IAsyncDisposable
{
    private readonly object _gate = new();
    private readonly Dictionary<ulong, InMemoryTransport> _transports = new();
    private readonly InMemoryNetworkOptions _options;
    private readonly Random? _random;
    private HashSet<ulong>? _partition;
    private bool _disposed;

    /// <summary>Initializes an in-memory network without injected faults.</summary>
    public InMemoryNetwork()
        : this(new InMemoryNetworkOptions())
    {
    }

    /// <summary>Initializes an in-memory network with optional deterministic fault injection.</summary>
    /// <param name="options">The network options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <see cref="InMemoryNetworkOptions.DropRate"/> is invalid.
    /// </exception>
    public InMemoryNetwork(InMemoryNetworkOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        if (_options.DropRate is < 0 or > 1 || double.IsNaN(_options.DropRate))
        {
            throw new ArgumentOutOfRangeException(nameof(options), "DropRate must be between 0 and 1.");
        }

        if (_options.DropRate > 0)
        {
            _random = _options.Seed.HasValue ? new Random(_options.Seed.Value) : new Random();
        }
    }

    /// <summary>Creates and registers a transport bound to <paramref name="id"/>.</summary>
    /// <param name="id">The node id.</param>
    /// <returns>The registered transport.</returns>
    /// <exception cref="InvalidOperationException">The network is disposed or the id is already registered.</exception>
    public IRaftTransport CreateNode(ulong id)
    {
        var transport = new InMemoryTransport(this, id);
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_transports.ContainsKey(id))
            {
                throw new InvalidOperationException("A transport is already registered for the node id.");
            }

            _transports.Add(id, transport);
        }

        return transport;
    }

    /// <summary>
    /// Partitions the network into <paramref name="groupA"/> and all other registered nodes. Frames crossing the
    /// partition boundary are dropped until <see cref="Heal"/> is called.
    /// </summary>
    /// <param name="groupA">The first partition group.</param>
    /// <exception cref="ArgumentNullException"><paramref name="groupA"/> is <see langword="null"/>.</exception>
    public void SetPartition(IReadOnlyCollection<ulong> groupA)
    {
        if (groupA is null)
        {
            throw new ArgumentNullException(nameof(groupA));
        }

        lock (_gate)
        {
            ThrowIfDisposed();
            _partition = new HashSet<ulong>(groupA);
        }
    }

    /// <summary>Removes any active network partition.</summary>
    public void Heal()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            _partition = null;
        }
    }

    /// <summary>Waits until all currently queued in-memory frames have been pumped.</summary>
    /// <param name="cancellationToken">A token that cancels the wait.</param>
    /// <returns>A task that completes once the network is idle.</returns>
    public async ValueTask DrainAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            InMemoryTransport[] transports = Snapshot();
            bool idle = true;
            foreach (InMemoryTransport transport in transports)
            {
                idle &= transport.PendingFrames == 0;
            }

            if (idle)
            {
                return;
            }

            await Task.Yield();
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        InMemoryTransport[] transports;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            transports = SnapshotLocked();
            _transports.Clear();
            _partition = null;
        }

        foreach (InMemoryTransport transport in transports)
        {
            await transport.DisposeAsync().ConfigureAwait(false);
        }
    }

    internal void Unregister(ulong id, InMemoryTransport transport)
    {
        lock (_gate)
        {
            if (_transports.TryGetValue(id, out InMemoryTransport? registered)
                && ReferenceEquals(registered, transport))
            {
                _transports.Remove(id);
            }
        }
    }

    internal void Send(ulong from, ulong to, ReadOnlyMemory<byte> frame)
    {
        InMemoryTransport[] targets;
        lock (_gate)
        {
            ThrowIfDisposed();
            if (to == 0)
            {
                targets = BroadcastTargetsLocked(from, frame);
            }
            else if (_transports.TryGetValue(to, out InMemoryTransport? target)
                && ShouldDeliverLocked(from, to))
            {
                targets = [target];
            }
            else
            {
                targets = [];
            }
        }

        foreach (InMemoryTransport target in targets)
        {
            target.Enqueue(frame);
        }
    }

    private InMemoryTransport[] BroadcastTargetsLocked(ulong from, ReadOnlyMemory<byte> frame)
    {
        var targets = new List<InMemoryTransport>(_transports.Count);
        foreach (KeyValuePair<ulong, InMemoryTransport> entry in _transports)
        {
            if (entry.Key != from && ShouldDeliverLocked(from, entry.Key))
            {
                targets.Add(entry.Value);
            }
        }

        return [.. targets];
    }

    private bool ShouldDeliverLocked(ulong from, ulong to)
    {
        if (_partition is not null && _partition.Contains(from) != _partition.Contains(to))
        {
            return false;
        }

        if (_options.DropPredicate?.Invoke(from, to) == true)
        {
            return false;
        }

        return _random is null || _random.NextDouble() >= _options.DropRate;
    }

    private InMemoryTransport[] Snapshot()
    {
        lock (_gate)
        {
            return SnapshotLocked();
        }
    }

    private InMemoryTransport[] SnapshotLocked() => [.. _transports.Values];

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(InMemoryNetwork));
        }
    }
}
