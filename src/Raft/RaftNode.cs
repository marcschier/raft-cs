// Copyright (c) marcschier. Licensed under the MIT License.

using System.Threading.Channels;
using Raft.Configuration;
using Raft.Messages;
using Raft.Storage;

namespace Raft;

/// <summary>
/// The asynchronous Raft node: a self-contained replica that owns a <see cref="RaftCore"/>, a durable
/// <see cref="IRaftWritableStorage"/>, an <see cref="Raft.Transport.IRaftTransport"/>, and a tick timer. All access to
/// the (single-threaded) core is funneled through one driver loop, so the public API is fully thread-safe.
/// </summary>
public sealed class RaftNode : IAsyncDisposable
{
    private readonly RaftCore _core;
    private readonly IRaftWritableStorage _storage;
    private readonly Raft.Transport.IRaftTransport _transport;
    private readonly RaftNodeOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly Channel<object> _inbox;
    private readonly Channel<ReadOnlyMemory<byte>> _committed;
    private readonly CancellationTokenSource _stop = new();
    private readonly SemaphoreSlim _startGate = new(1, 1);
    private readonly object _tickSignal = new();

    private ConfState _confState;
    private Task? _loop;
    private ITimer? _tickTimer;
    private bool _started;
    private bool _disposed;
    private volatile uint _packedState;
    private long _leaderId;
    private long _term;
    private long _commitIndex;

    /// <summary>Initializes a new instance of the <see cref="RaftNode"/> class.</summary>
    /// <param name="config">The Raft configuration.</param>
    /// <param name="storage">The durable backing store.</param>
    /// <param name="transport">The network transport.</param>
    /// <param name="options">The driver options, or <see langword="null"/> for defaults.</param>
    /// <param name="timeProvider">The time provider used for the tick timer.</param>
    public RaftNode(
        RaftConfig config,
        IRaftWritableStorage storage,
        Raft.Transport.IRaftTransport transport,
        RaftNodeOptions? options = null,
        TimeProvider? timeProvider = null)
    {
        Internal.Check.NotNull(config);
        Internal.Check.NotNull(storage);
        Internal.Check.NotNull(transport);

        _storage = storage;
        _transport = transport;
        _options = options ?? new RaftNodeOptions();
        _timeProvider = timeProvider ?? TimeProvider.System;
        _core = new RaftCore(config, storage);
        _confState = storage.InitialState().ConfState;
        _inbox = Channel.CreateUnbounded<object>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        _committed = Channel.CreateUnbounded<ReadOnlyMemory<byte>>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = true,
        });
    }

    /// <summary>Gets this node's id.</summary>
    public ulong Id => _core.Id;

    /// <summary>Gets the id of the leader this node recognizes, or zero when unknown.</summary>
    public ulong LeaderId => (ulong)Interlocked.Read(ref _leaderId);

    /// <summary>Gets this node's current term.</summary>
    public ulong Term => (ulong)Interlocked.Read(ref _term);

    /// <summary>Gets this node's highest committed index.</summary>
    public ulong CommitIndex => (ulong)Interlocked.Read(ref _commitIndex);

    /// <summary>Gets the current role.</summary>
    public RaftRole Role => (RaftRole)(byte)_packedState;

    /// <summary>Gets a value indicating whether this node currently believes itself to be the leader.</summary>
    public bool IsLeader => Role == RaftRole.Leader;

    /// <summary>Gets a reader over committed application command payloads, in log order.</summary>
    public ChannelReader<ReadOnlyMemory<byte>> Committed => _committed.Reader;

    /// <summary>Starts the transport, the driver loop, and the tick timer.</summary>
    /// <param name="cancellationToken">A token that cancels startup.</param>
    /// <returns>A value task that completes when the node has started.</returns>
    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await _startGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_started)
            {
                return;
            }

            _transport.FrameReceived += OnFrameReceived;
            await _transport.StartAsync(cancellationToken).ConfigureAwait(false);
            _loop = Task.Run(() => RunLoopAsync(_stop.Token), CancellationToken.None);
            _tickTimer = _timeProvider.CreateTimer(
                static state => ((RaftNode)state!).EnqueueTick(),
                this,
                _options.TickInterval,
                _options.TickInterval);
            _started = true;
        }
        finally
        {
            _startGate.Release();
        }
    }

    /// <summary>Proposes an opaque application command to be replicated and committed.</summary>
    /// <param name="command">The command payload.</param>
    /// <param name="cancellationToken">A token that cancels the enqueue.</param>
    /// <returns>A value task that completes when the proposal is accepted into the driver.</returns>
    public ValueTask ProposeAsync(ReadOnlyMemory<byte> command, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var message = new Message
        {
            From = _core.Id,
            Type = MessageType.Propose,
            Entries = new[] { new Entry(EntryType.Normal, 0, 0, command) },
        };
        return _inbox.Writer.WriteAsync(message, cancellationToken);
    }

    /// <summary>Proposes a cluster membership change.</summary>
    /// <param name="change">The configuration change.</param>
    /// <param name="cancellationToken">A token that cancels the enqueue.</param>
    /// <returns>A value task that completes when the change is accepted into the driver.</returns>
    public ValueTask ChangeConfigurationAsync(ConfChangeV2 change, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        Internal.Check.NotNull(change);
        var message = new Message
        {
            From = _core.Id,
            Type = MessageType.Propose,
            Entries = new[] { new Entry(EntryType.ConfChangeV2, 0, 0, change.Encode()) },
        };
        return _inbox.Writer.WriteAsync(message, cancellationToken);
    }

    /// <summary>Requests leadership transfer to <paramref name="targetId"/> (only effective on a leader).</summary>
    /// <param name="targetId">The transfer target node id.</param>
    /// <param name="cancellationToken">A token that cancels the enqueue.</param>
    /// <returns>A value task that completes when the request is accepted into the driver.</returns>
    public ValueTask TransferLeadershipAsync(ulong targetId, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var message = new Message { From = targetId, Type = MessageType.TransferLeader };
        return _inbox.Writer.WriteAsync(message, cancellationToken);
    }

    /// <summary>Forces this node to start an election campaign immediately.</summary>
    /// <param name="cancellationToken">A token that cancels the enqueue.</param>
    /// <returns>A value task that completes when the request is accepted into the driver.</returns>
    public ValueTask CampaignAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var message = new Message { From = _core.Id, Type = MessageType.Hup };
        return _inbox.Writer.WriteAsync(message, cancellationToken);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _stop.Cancel();
        _inbox.Writer.TryComplete();
        if (_tickTimer is not null)
        {
            await _tickTimer.DisposeAsync().ConfigureAwait(false);
        }

        if (_loop is not null)
        {
            try
            {
                await _loop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        _transport.FrameReceived -= OnFrameReceived;
        await _transport.DisposeAsync().ConfigureAwait(false);
        _committed.Writer.TryComplete();
        _startGate.Dispose();
        _stop.Dispose();
    }

    private void EnqueueTick() => _inbox.Writer.TryWrite(_tickSignal);

    private void OnFrameReceived(ReadOnlyMemory<byte> frame)
    {
        if (MessageCodec.TryParse(frame.Span, out Message? message) && message is not null && message.To == _core.Id)
        {
            _inbox.Writer.TryWrite(message);
        }
    }

    private async Task RunLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _inbox.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (_inbox.Reader.TryRead(out object? item))
                {
                    if (ReferenceEquals(item, _tickSignal))
                    {
                        _core.Tick();
                    }
                    else if (item is Message message)
                    {
                        _core.Step(message);
                    }
                }

                await ProcessReadyAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task ProcessReadyAsync(CancellationToken cancellationToken)
    {
        Snapshot? snapshot = _core.UnstableSnapshot();
        if (snapshot is not null && !snapshot.IsEmpty)
        {
            _storage.ApplySnapshot(snapshot);
            _confState = snapshot.Metadata.ConfState;
            _core.StableSnapshotTo(snapshot.Metadata.Index);
        }

        IReadOnlyList<Entry> unstable = _core.UnstableEntries();
        if (unstable.Count > 0)
        {
            _storage.Append(unstable);
            Entry last = unstable[unstable.Count - 1];
            _core.StableTo(last.Index, last.Term);
        }

        _storage.SetHardState(_core.HardState);

        IReadOnlyList<Message> messages = _core.TakeMessages();
        for (int i = 0; i < messages.Count; i++)
        {
            await SendAsync(messages[i], cancellationToken).ConfigureAwait(false);
        }

        IReadOnlyList<Entry> toApply = _core.NextEntriesToApply();
        if (toApply.Count > 0)
        {
            ulong appliedTo = 0;
            foreach (Entry entry in toApply)
            {
                ApplyEntry(entry);
                appliedTo = entry.Index;
            }

            _core.AppliedTo(appliedTo);

            if (_core.ShouldAutoLeaveJoint())
            {
                _inbox.Writer.TryWrite(new Message
                {
                    From = _core.Id,
                    Type = MessageType.Propose,
                    Entries = new[]
                    {
                        new Entry(EntryType.ConfChangeV2, 0, 0, ConfChangeV2.LeaveJoint().Encode()),
                    },
                });
            }
        }

        PublishState();
    }

    private void ApplyEntry(Entry entry)
    {
        switch (entry.Type)
        {
            case EntryType.Normal:
                if (!entry.IsEmpty)
                {
                    _committed.Writer.TryWrite(entry.Data);
                }

                break;

            case EntryType.ConfChange:
            case EntryType.ConfChangeV2:
                ConfChangeV2 change = ConfChangeV2.Decode(entry.Data.Span);
                ConfState next = Changer.Apply(_confState, change);
                _confState = next;
                _core.ApplyConfChange(next);
                _storage.SetConfState(next);
                break;
        }
    }

    private async ValueTask SendAsync(Message message, CancellationToken cancellationToken)
    {
        if (message.To == 0 || message.To == _core.Id)
        {
            return;
        }

        int length = MessageCodec.EncodedLength(message);
        byte[] frame = new byte[length];
        if (!MessageCodec.TryWrite(message, frame))
        {
            return;
        }

        try
        {
            await _transport.SendAsync(message.To, frame, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private void PublishState()
    {
        Interlocked.Exchange(ref _leaderId, (long)_core.LeaderId);
        Interlocked.Exchange(ref _term, (long)_core.Term);
        Interlocked.Exchange(ref _commitIndex, (long)_core.CommitIndex);
        _packedState = (byte)_core.Role;
    }

    private void ThrowIfDisposed()
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RaftNode));
        }
#endif
    }
}
