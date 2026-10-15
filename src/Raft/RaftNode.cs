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
    private readonly IRaftSnapshotStorage? _snapshotStorage;
    private readonly Raft.Transport.IRaftTransport _transport;
    private readonly Raft.Transport.IRaftBatchTransport? _batchTransport;
    private readonly RaftNodeOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly Channel<object> _inbox;
    private readonly Channel<CommittedItem> _committed;
    private readonly CommittedPayloadReader _committedPayloads;
    private readonly Channel<RaftStateChange> _stateChanges;
    private readonly Channel<ConfState> _confChanges;
    private readonly CancellationTokenSource _stop = new();
    private readonly SemaphoreSlim _startGate = new(1, 1);
    private readonly object _tickSignal = new();

    // The committed configurations since the latest snapshot, keyed by the index of the entry that committed them, so
    // a snapshot records the membership at its own index.
    private readonly List<KeyValuePair<ulong, ConfState>> _confHistory = new();

    private ConfState _confState;
    private volatile ConfState _publishedConfState;
    private Task? _loop;
    private Task? _appendTask;
    private ITimer? _tickTimer;
    private bool _started;
    private bool _disposed;
    private bool _appendInFlight;
    private bool _stateEverPublished;
    private ulong _lastEmittedLeaderId;
    private RaftRole _lastEmittedRole;
    private volatile uint _packedState;
    private long _leaderId;
    private long _term;
    private long _commitIndex;
    private long _appliedIndex;

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
        _snapshotStorage = storage as IRaftSnapshotStorage;
        _transport = transport;
        _batchTransport = transport as Raft.Transport.IRaftBatchTransport;
        _options = options ?? new RaftNodeOptions();
        _timeProvider = timeProvider ?? TimeProvider.System;
        _core = new RaftCore(config, storage);
        _confState = storage.InitialState().ConfState;
        Snapshot? restored = RestoreLatestSnapshot();
        _appliedIndex = (long)_core.Applied;
        _confHistory.Add(new KeyValuePair<ulong, ConfState>(_core.Applied, _confState));
        _inbox = Channel.CreateUnbounded<object>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
        });
        _committed = Channel.CreateUnbounded<CommittedItem>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = true,
        });
        _committedPayloads = new CommittedPayloadReader(_committed.Reader);

        // The application restores the latest snapshot before the entries after it are applied.
        if (restored is not null)
        {
            _committed.Writer.TryWrite(CommittedItem.ForSnapshot(restored));
        }

        int capacity = _options.StateObservationCapacity;
        if (capacity <= 0)
        {
            throw new ArgumentException("StateObservationCapacity must be positive.", nameof(options));
        }

        var observationOptions = new BoundedChannelOptions(capacity)
        {
            SingleReader = false,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.DropOldest,
        };
        _stateChanges = Channel.CreateBounded<RaftStateChange>(observationOptions);
        _confChanges = Channel.CreateBounded<ConfState>(observationOptions);

        // Seed the committed-configuration stream and poll property with the recovered baseline membership so a
        // stream-only consumer observes the starting configuration before any change.
        _publishedConfState = _confState;
        _confChanges.Writer.TryWrite(_confState);
    }

    /// <summary>Gets this node's id.</summary>
    public ulong Id => _core.Id;

    /// <summary>Gets the id of the leader this node recognizes, or zero when unknown.</summary>
    public ulong LeaderId => (ulong)Interlocked.Read(ref _leaderId);

    /// <summary>Gets this node's current term.</summary>
    public ulong Term => (ulong)Interlocked.Read(ref _term);

    /// <summary>Gets this node's highest committed index.</summary>
    public ulong CommitIndex => (ulong)Interlocked.Read(ref _commitIndex);

    /// <summary>
    /// Gets the highest log index the node has applied: the commands up to it are queued on
    /// <see cref="CommittedItems"/>, not necessarily processed by the consumer yet. A state machine takes its own
    /// position from <see cref="CommittedItem.Index"/>.
    /// </summary>
    public ulong AppliedIndex => (ulong)Interlocked.Read(ref _appliedIndex);

    /// <summary>Gets the current role.</summary>
    public RaftRole Role => (RaftRole)(byte)_packedState;

    /// <summary>Gets a value indicating whether this node currently believes itself to be the leader.</summary>
    public bool IsLeader => Role == RaftRole.Leader;

    /// <summary>
    /// Gets a reader over committed application command payloads, in log order. This is a view of
    /// <see cref="CommittedItems"/> without the log positions and without snapshots: both read the same queue, so a
    /// consumer reads one of them.
    /// </summary>
    public ChannelReader<ReadOnlyMemory<byte>> Committed => _committedPayloads;

    /// <summary>
    /// Gets a reader over the apply stream, in log order: committed application commands, each with its log index and
    /// term, and snapshots that replace the application state (<see cref="CommittedItemKind.Snapshot"/>). A snapshot
    /// comes first after a restart when the storage holds one (<see cref="IRaftSnapshotStorage"/>), and when this node
    /// installs a snapshot from the leader. The index of the last item a state machine has applied is the log position
    /// its state corresponds to. This reads the same queue as <see cref="Committed"/>, so a consumer reads one of them.
    /// </summary>
    public ChannelReader<CommittedItem> CommittedItems => _committed.Reader;

    /// <summary>
    /// Gets a reader over leadership/role transitions. The current state is emitted as the first item; thereafter a
    /// new <see cref="RaftStateChange"/> is published whenever the recognized leader id or the role changes. The
    /// stream drops the oldest buffered item when a consumer falls behind (see
    /// <see cref="RaftNodeOptions.StateObservationCapacity"/>), so the latest state is always retained.
    /// </summary>
    public ChannelReader<RaftStateChange> StateChanges => _stateChanges.Reader;

    /// <summary>
    /// Gets a reader over committed cluster configurations. The current configuration is emitted as the first item;
    /// thereafter the new <see cref="ConfState"/> is published each time a membership change commits. Lets a consumer
    /// reconcile membership against committed state rather than relying on the node's internal optimistic apply.
    /// </summary>
    public ChannelReader<ConfState> CommittedConfigurations => _confChanges.Reader;

    /// <summary>Gets the most recently committed cluster configuration.</summary>
    public ConfState Configuration => _publishedConfState;

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

    /// <summary>
    /// Discards the applied log prefix up to (and excluding) <paramref name="index"/> to bound the on-disk log. The
    /// compaction runs on the driver loop, serialized with all other storage access. A follower whose next index falls
    /// below the compacted boundary is automatically re-seeded with the latest snapshot, so for full state transfer to
    /// a far-behind follower the application records its state with <see cref="CreateSnapshotAsync"/> before it
    /// compacts. When the storage holds a snapshot, the log is not compacted past it: the snapshot and the log after it
    /// must together hold every applied entry, for a restart and for a lagging follower.
    /// </summary>
    /// <param name="index">The first index to retain; must not exceed <see cref="AppliedIndex"/>.</param>
    /// <param name="cancellationToken">A token that cancels the wait (the compaction itself still completes).</param>
    /// <returns>A task that completes when the log has been compacted.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="index"/> is past the applied index or past the latest snapshot.
    /// </exception>
    public Task CompactAsync(ulong index, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ulong applied = AppliedIndex;
        if (index > applied)
        {
            throw new ArgumentOutOfRangeException(
                nameof(index), index, $"Cannot compact past the applied index {applied}.");
        }

        // The latest snapshot only moves forward, so a compaction allowed here stays allowed on the loop.
        if (_snapshotStorage?.LatestSnapshot() is { IsEmpty: false } latest && index > latest.Metadata.Index)
        {
            throw new ArgumentOutOfRangeException(
                nameof(index), index, $"Cannot compact past the latest snapshot at index {latest.Metadata.Index}.");
        }

        return EnqueueRequest(new CompactRequest(index), cancellationToken);
    }

    /// <summary>
    /// Records a snapshot of the application state at <paramref name="index"/> in the storage, with the cluster
    /// membership at that index. The leader sends the latest snapshot to a follower whose log ends before the compacted
    /// boundary, and a node hands it to its application on <see cref="CommittedItems"/> after a restart. Afterwards,
    /// <see cref="CompactAsync"/> may discard the log up to <paramref name="index"/>.
    /// </summary>
    /// <param name="index">
    /// The log index the state corresponds to: the <see cref="CommittedItem.Index"/> of the last item the application
    /// applied. Must not exceed <see cref="AppliedIndex"/>.
    /// </param>
    /// <param name="data">The application state; the node keeps its own copy.</param>
    /// <param name="cancellationToken">A token that cancels the wait (the snapshot itself is still recorded).</param>
    /// <returns>The metadata of the recorded snapshot.</returns>
    /// <exception cref="NotSupportedException">
    /// The storage does not implement <see cref="IRaftSnapshotStorage"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="index"/> is zero or past the applied index.
    /// </exception>
    /// <exception cref="RaftStorageException">
    /// The storage refused the snapshot, for example because it is not newer than the latest one.
    /// </exception>
    public Task<SnapshotMetadata> CreateSnapshotAsync(
        ulong index,
        ReadOnlyMemory<byte> data,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (_snapshotStorage is null)
        {
            throw new NotSupportedException(
                $"The storage of this node does not keep snapshots; it must implement {nameof(IRaftSnapshotStorage)}.");
        }

        ulong applied = AppliedIndex;
        if (index == 0 || index > applied)
        {
            throw new ArgumentOutOfRangeException(
                nameof(index), index, $"A snapshot index must be between 1 and the applied index {applied}.");
        }

        return EnqueueRequest(new SnapshotRequest(index, data.ToArray()), cancellationToken);
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

        // Await any in-flight storage write so the worker has released the store before the caller disposes it.
        if (_appendTask is not null)
        {
            try
            {
                await _appendTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        _transport.FrameReceived -= OnFrameReceived;
        await _transport.DisposeAsync().ConfigureAwait(false);
        _committed.Writer.TryComplete();
        _stateChanges.Writer.TryComplete();
        _confChanges.Writer.TryComplete();
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
                    else if (item is AppendComplete complete)
                    {
                        await OnAppendCompleteAsync(complete, cancellationToken).ConfigureAwait(false);
                    }
                    else if (item is CompactRequest compact)
                    {
                        HandleCompact(compact);
                    }
                    else if (item is SnapshotRequest snapshot)
                    {
                        HandleCreateSnapshot(snapshot);
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
        // 1. Send the immediately sendable messages (a leader's appends/heartbeats, vote requests, snapshots) before
        //    persisting below, so replication to followers overlaps the local disk write ("writing to the leader's
        //    disk in parallel").
        IReadOnlyList<Message> sendNow = _core.TakeSendNowMessages();
        if (sendNow.Count > 0)
        {
            await SendBatchAsync(sendNow, cancellationToken).ConfigureAwait(false);
        }

        // 2. Hand the next durable write to a background worker. At most one write is outstanding at a time, so entries
        //    appended while a write is in flight are batched into the following write.
        if (!_appendInFlight)
        {
            StorageWrite? write = _core.TakeStorageWrite();
            if (write is not null)
            {
                if (write.Snapshot is not null)
                {
                    _confState = write.Snapshot.Metadata.ConfState;
                }

                _appendInFlight = true;
                _appendTask = Task.Run(() => RunStorageWrite(write), CancellationToken.None);
            }
        }

        // 3. Apply committed-and-durable entries to the state machine (conf changes on the loop for ordering).
        ApplyCommitted();

        // 4. Publish observable state.
        PublishState();
    }

    private async Task OnAppendCompleteAsync(AppendComplete complete, CancellationToken cancellationToken)
    {
        _appendInFlight = false;
        StorageWrite write = complete.Write;

        // A snapshot from the leader is durable now: it replaces the application state before any entry after it.
        if (write.Snapshot is not null)
        {
            DeliverInstalledSnapshot(write.Snapshot);
        }

        // Responses (follower append acknowledgements and granted votes) are released only now that the write is
        // durable, because a node must never acknowledge state it has not yet persisted.
        if (write.Responses.Count > 0)
        {
            await SendBatchAsync(write.Responses, cancellationToken).ConfigureAwait(false);
        }

        _core.AckStorageWrite(write);
    }

    private void RunStorageWrite(StorageWrite write)
    {
        try
        {
            _storage.Write(write.Entries, write.HardState, write.Snapshot);
        }
        catch (Exception ex)
        {
            // A storage write failure is unrecoverable for consensus: surface it to the committed-command consumer and
            // stop the driver loop.
            _committed.Writer.TryComplete(ex);
            _stop.Cancel();
            return;
        }

        _inbox.Writer.TryWrite(new AppendComplete(write));
    }

    private void ApplyCommitted()
    {
        IReadOnlyList<Entry> toApply = _core.NextEntriesToApply();
        if (toApply.Count == 0)
        {
            return;
        }

        ulong appliedTo = 0;
        List<CommittedItem>? commands = null;
        foreach (Entry entry in toApply)
        {
            if (ApplyEntry(entry) is { } command)
            {
                (commands ??= new List<CommittedItem>(toApply.Count)).Add(command);
            }

            appliedTo = entry.Index;
        }

        _core.AppliedTo(appliedTo);

        // AppliedIndex covers every item before a consumer can read it, so the consumer may compact at that index.
        Interlocked.Exchange(ref _appliedIndex, (long)_core.Applied);
        if (commands is not null)
        {
            foreach (CommittedItem command in commands)
            {
                _committed.Writer.TryWrite(command);
            }
        }

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

    private void HandleCompact(CompactRequest request)
    {
        try
        {
            if (_snapshotStorage?.LatestSnapshot() is { IsEmpty: false } latest
                && request.Index > latest.Metadata.Index)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(request),
                    request.Index,
                    $"Cannot compact past the latest snapshot at index {latest.Metadata.Index}.");
            }

            // No-op when the prefix is already compacted; otherwise discard the applied prefix on the loop thread,
            // serialized with every other storage access.
            if (request.Index >= _storage.FirstIndex())
            {
                _storage.Compact(request.Index);
            }

            request.Completion.TrySetResult(true);
        }
        catch (Exception ex)
        {
            request.Completion.TrySetException(ex);
        }
    }

    private void HandleCreateSnapshot(SnapshotRequest request)
    {
        try
        {
            Snapshot snapshot = _snapshotStorage!.CreateSnapshot(
                request.Index, ConfStateAt(request.Index), request.Data);

            // Only the configuration in force at the snapshot and the ones committed after it are still needed.
            int keepFrom = _confHistory.FindLastIndex(pair => pair.Key <= request.Index);
            if (keepFrom > 0)
            {
                _confHistory.RemoveRange(0, keepFrom);
            }

            request.Completion.TrySetResult(snapshot.Metadata);
        }
        catch (Exception ex)
        {
            request.Completion.TrySetException(ex);
        }
    }

    private ConfState ConfStateAt(ulong index)
    {
        for (int i = _confHistory.Count - 1; i > 0; i--)
        {
            if (_confHistory[i].Key <= index)
            {
                return _confHistory[i].Value;
            }
        }

        return _confHistory[0].Value;
    }

    /// <summary>
    /// Validates the latest snapshot in the storage on start and marks the log applied up to it, so the application
    /// restores the snapshot and then applies only the entries after it.
    /// </summary>
    private Snapshot? RestoreLatestSnapshot()
    {
        if (_snapshotStorage?.LatestSnapshot() is not { IsEmpty: false } snapshot)
        {
            return null;
        }

        ulong compacted = _storage.FirstIndex() - 1;
        if (snapshot.Metadata.Index < compacted)
        {
            throw new InvalidOperationException(
                $"The log is compacted to index {compacted}, past the latest snapshot at index "
                + $"{snapshot.Metadata.Index}: the entries between them are in neither, so the application state "
                + "cannot be restored.");
        }

        _confState = snapshot.Metadata.ConfState;
        _core.ApplyConfChange(_confState);
        _core.RestoreAppliedTo(snapshot.Metadata.Index);
        return snapshot;
    }

    private void DeliverInstalledSnapshot(Snapshot snapshot)
    {
        ConfState confState = snapshot.Metadata.ConfState;
        _confHistory.Clear();
        _confHistory.Add(new KeyValuePair<ulong, ConfState>(snapshot.Metadata.Index, confState));
        _committed.Writer.TryWrite(CommittedItem.ForSnapshot(snapshot));

        _publishedConfState = confState;
        _confChanges.Writer.TryWrite(confState);
    }

    /// <summary>Applies one committed entry; returns the item to queue for a non-empty command.</summary>
    private CommittedItem? ApplyEntry(Entry entry)
    {
        switch (entry.Type)
        {
            case EntryType.Normal:
                if (!entry.IsEmpty)
                {
                    return new CommittedItem(CommittedItemKind.Command, entry.Index, entry.Term, entry.Data);
                }

                break;

            case EntryType.ConfChange:
            case EntryType.ConfChangeV2:
                ConfChangeV2 change = ConfChangeV2.Decode(entry.Data.Span);
                ConfState next = Changer.Apply(_confState, change);
                _confState = next;
                _confHistory.Add(new KeyValuePair<ulong, ConfState>(entry.Index, next));
                _core.ApplyConfChange(next);
                _storage.SetConfState(next);

                // Surface the committed membership so consumers can reconcile against it.
                _publishedConfState = next;
                _confChanges.Writer.TryWrite(next);
                break;
        }

        return null;
    }

    private async ValueTask SendBatchAsync(IReadOnlyList<Message> messages, CancellationToken cancellationToken)
    {
        if (messages.Count == 0)
        {
            return;
        }

        if (_batchTransport is not null)
        {
            List<Raft.Transport.OutboundFrame>? frames = null;
            for (int i = 0; i < messages.Count; i++)
            {
                if (TryEncodeFrame(messages[i], out Raft.Transport.OutboundFrame frame))
                {
                    frames ??= new List<Raft.Transport.OutboundFrame>(messages.Count);
                    frames.Add(frame);
                }
            }

            if (frames is not null)
            {
                try
                {
                    await _batchTransport.SendManyAsync(frames, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception)
                {
                    // Best-effort: a failed batch send is a dropped message (Raft retransmits) and must
                    // never fault the driver loop — e.g. a peer's transport closing during cluster shutdown.
                }
            }

            return;
        }

        for (int i = 0; i < messages.Count; i++)
        {
            await SendAsync(messages[i], cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask SendAsync(Message message, CancellationToken cancellationToken)
    {
        if (!TryEncodeFrame(message, out Raft.Transport.OutboundFrame frame))
        {
            return;
        }

        try
        {
            await _transport.SendAsync(frame.Recipient, frame.Frame, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception)
        {
            // A failed send is a dropped message, which Raft recovers from by retransmission. A peer whose
            // transport has already closed (e.g. during cluster shutdown) must never fault the driver loop
            // and surface out of DisposeAsync.
        }
    }

    private bool TryEncodeFrame(Message message, out Raft.Transport.OutboundFrame frame)
    {
        frame = default;
        if (message.To == 0 || message.To == _core.Id)
        {
            return false;
        }

        int length = MessageCodec.EncodedLength(message);
        byte[] bytes = new byte[length];
        if (!MessageCodec.TryWrite(message, bytes))
        {
            return false;
        }

        frame = new Raft.Transport.OutboundFrame(message.To, bytes);
        return true;
    }

    private void PublishState()
    {
        ulong leaderId = _core.LeaderId;
        ulong term = _core.Term;
        RaftRole role = _core.Role;

        Interlocked.Exchange(ref _leaderId, (long)leaderId);
        Interlocked.Exchange(ref _term, (long)term);
        Interlocked.Exchange(ref _commitIndex, (long)_core.CommitIndex);
        Interlocked.Exchange(ref _appliedIndex, (long)_core.Applied);
        _packedState = (byte)role;

        // Publish a leadership/role transition only when the recognized leader or the role actually changes; the term
        // travels in the payload as context but does not by itself trigger an event. The first publish always emits,
        // giving stream-only consumers a baseline. DropOldest means TryWrite never blocks the consensus loop.
        if (!_stateEverPublished || leaderId != _lastEmittedLeaderId || role != _lastEmittedRole)
        {
            _stateEverPublished = true;
            _lastEmittedLeaderId = leaderId;
            _lastEmittedRole = role;
            _stateChanges.Writer.TryWrite(new RaftStateChange(term, leaderId, role));
        }
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

    /// <summary>Loop signal posted by the storage worker once a <see cref="StorageWrite"/> is durable.</summary>
    private sealed class AppendComplete
    {
        internal AppendComplete(StorageWrite write) => Write = write;

        internal StorageWrite Write { get; }
    }

    private Task<TResult> EnqueueRequest<TResult>(LoopRequest<TResult> request, CancellationToken cancellationToken)
    {
        if (cancellationToken.CanBeCanceled)
        {
            CancellationTokenRegistration registration = cancellationToken.Register(
                static state => ((LoopRequest<TResult>)state!).Completion.TrySetCanceled(), request);
            _ = request.Completion.Task.ContinueWith(
                static (_, state) => ((CancellationTokenRegistration)state!).Dispose(),
                registration,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        if (!_inbox.Writer.TryWrite(request))
        {
            request.Completion.TrySetException(new ObjectDisposedException(nameof(RaftNode)));
        }

        return request.Completion.Task;
    }

    /// <summary>A request the driver loop runs, serialized with all other storage access.</summary>
    private abstract class LoopRequest<TResult>
    {
        internal TaskCompletionSource<TResult> Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>Loop request to compact the log up to (and excluding) <see cref="Index"/>.</summary>
    private sealed class CompactRequest : LoopRequest<bool>
    {
        internal CompactRequest(ulong index) => Index = index;

        internal ulong Index { get; }
    }

    /// <summary>Loop request to record a snapshot of the application state at <see cref="Index"/>.</summary>
    private sealed class SnapshotRequest : LoopRequest<SnapshotMetadata>
    {
        internal SnapshotRequest(ulong index, byte[] data)
        {
            Index = index;
            Data = data;
        }

        internal ulong Index { get; }

        internal byte[] Data { get; }
    }
}
