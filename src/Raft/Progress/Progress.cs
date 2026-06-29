// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Progress;

/// <summary>The replication state the leader maintains for a single follower or learner.</summary>
public enum ProgressState : byte
{
    /// <summary>Probing for the follower's match index (one in-flight append at a time).</summary>
    Probe = 0,

    /// <summary>Steadily streaming entries; multiple appends may be in flight.</summary>
    Replicate = 1,

    /// <summary>A snapshot is being installed; replication is paused until it completes.</summary>
    Snapshot = 2,
}

/// <summary>A bounded sliding window of in-flight append message tail indices, used for flow control.</summary>
public sealed class Inflights
{
    private readonly ulong[] _buffer;
    private int _start;
    private int _count;

    /// <summary>Initializes a new instance of the <see cref="Inflights"/> class.</summary>
    /// <param name="capacity">The maximum number of in-flight appends.</param>
    public Inflights(int capacity)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
#else
        if (capacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(capacity));
        }
#endif

        _buffer = new ulong[capacity];
    }

    /// <summary>Gets a value indicating whether the window is full.</summary>
    public bool IsFull => _count == _buffer.Length;

    /// <summary>Gets the number of in-flight appends.</summary>
    public int Count => _count;

    /// <summary>Records a new in-flight append whose last entry has index <paramref name="inflight"/>.</summary>
    /// <param name="inflight">The last index in the append message.</param>
    public void Add(ulong inflight)
    {
        if (IsFull)
        {
            throw new InvalidOperationException("cannot add into a full inflights");
        }

        int next = _start + _count;
        if (next >= _buffer.Length)
        {
            next -= _buffer.Length;
        }

        _buffer[next] = inflight;
        _count++;
    }

    /// <summary>Frees all in-flight appends up to and including the one ending at <paramref name="to"/>.</summary>
    /// <param name="to">The acknowledged last index.</param>
    public void FreeLatestEntries(ulong to)
    {
        int freed = 0;
        int index = _start;
        for (; freed < _count; freed++)
        {
            if (to < _buffer[index])
            {
                break;
            }

            index++;
            if (index >= _buffer.Length)
            {
                index -= _buffer.Length;
            }
        }

        _count -= freed;
        _start = index;
    }

    /// <summary>Frees the first in-flight append (used after a successful single probe).</summary>
    public void FreeFirstOne()
    {
        if (_count == 0)
        {
            return;
        }

        FreeLatestEntries(_buffer[_start]);
    }

    /// <summary>Resets the window to empty.</summary>
    public void Reset()
    {
        _start = 0;
        _count = 0;
    }
}

/// <summary>The leader's replication bookkeeping for one peer (follower or learner).</summary>
public sealed class Progress
{
    /// <summary>Initializes a new instance of the <see cref="Progress"/> class.</summary>
    /// <param name="nextIndex">The next log index to send to the peer.</param>
    /// <param name="maxInflight">The flow-control window capacity.</param>
    public Progress(ulong nextIndex, int maxInflight)
    {
        NextIndex = nextIndex;
        Inflights = new Inflights(maxInflight);
        State = ProgressState.Probe;
    }

    /// <summary>Gets or sets the highest log index known to be replicated on the peer.</summary>
    public ulong MatchIndex { get; set; }

    /// <summary>Gets or sets the next log index to send to the peer.</summary>
    public ulong NextIndex { get; set; }

    /// <summary>Gets or sets the replication state.</summary>
    public ProgressState State { get; set; }

    /// <summary>Gets or sets a value indicating whether the peer is a non-voting learner.</summary>
    public bool IsLearner { get; set; }

    /// <summary>Gets or sets a value indicating whether the peer responded since the last check-quorum.</summary>
    public bool RecentActive { get; set; }

    /// <summary>Gets or sets the pending snapshot index when in <see cref="ProgressState.Snapshot"/>.</summary>
    public ulong PendingSnapshot { get; set; }

    /// <summary>Gets or sets a value indicating whether sending is paused (probe in flight or window full).</summary>
    public bool ProbeSent { get; set; }

    /// <summary>Gets the in-flight append window.</summary>
    public Inflights Inflights { get; }

    /// <summary>Transitions the peer into probe state.</summary>
    public void BecomeProbe()
    {
        if (State == ProgressState.Snapshot)
        {
            ulong pendingSnapshot = PendingSnapshot;
            ResetState(ProgressState.Probe);
            NextIndex = Math.Max(MatchIndex + 1, pendingSnapshot + 1);
        }
        else
        {
            ResetState(ProgressState.Probe);
            NextIndex = MatchIndex + 1;
        }
    }

    /// <summary>Transitions the peer into replicate state.</summary>
    public void BecomeReplicate()
    {
        ResetState(ProgressState.Replicate);
        NextIndex = MatchIndex + 1;
    }

    /// <summary>Transitions the peer into snapshot state.</summary>
    /// <param name="snapshotIndex">The index of the snapshot being installed.</param>
    public void BecomeSnapshot(ulong snapshotIndex)
    {
        ResetState(ProgressState.Snapshot);
        PendingSnapshot = snapshotIndex;
    }

    /// <summary>Advances the match/next indices on a successful append, returning whether anything changed.</summary>
    /// <param name="index">The acknowledged last index.</param>
    /// <returns><see langword="true"/> when the match index advanced.</returns>
    public bool MaybeUpdate(ulong index)
    {
        bool updated = false;
        if (MatchIndex < index)
        {
            MatchIndex = index;
            updated = true;
            ProbeSent = false;
        }

        if (NextIndex < index + 1)
        {
            NextIndex = index + 1;
        }

        return updated;
    }

    /// <summary>Decrements the next index on a rejected append, returning whether it changed.</summary>
    /// <param name="rejected">The rejected index.</param>
    /// <param name="matchHint">The follower's last index hint.</param>
    /// <returns><see langword="true"/> when the next index regressed.</returns>
    public bool MaybeDecrTo(ulong rejected, ulong matchHint)
    {
        if (State == ProgressState.Replicate)
        {
            if (rejected <= MatchIndex)
            {
                return false;
            }

            NextIndex = MatchIndex + 1;
            return true;
        }

        if (NextIndex == 0 || NextIndex - 1 != rejected)
        {
            return false;
        }

        NextIndex = Math.Min(rejected, matchHint + 1);
        if (NextIndex < 1)
        {
            NextIndex = 1;
        }

        ProbeSent = false;
        return true;
    }

    /// <summary>Returns whether the leader is currently throttled from sending to this peer.</summary>
    /// <returns><see langword="true"/> when sending should be paused.</returns>
    public bool IsPaused()
    {
        return State switch
        {
            ProgressState.Probe => ProbeSent,
            ProgressState.Replicate => Inflights.IsFull,
            ProgressState.Snapshot => true,
            _ => true,
        };
    }

    private void ResetState(ProgressState state)
    {
        ProbeSent = false;
        PendingSnapshot = 0;
        State = state;
        Inflights.Reset();
    }
}
