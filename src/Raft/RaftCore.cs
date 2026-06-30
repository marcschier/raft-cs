// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Messages;
using Raft.Progress;
using Raft.Storage;

namespace Raft;

/// <summary>
/// The deterministic Raft consensus state machine. It performs no I/O: callers feed it ticks and messages via
/// <see cref="Tick"/> / <see cref="Step"/>, drain the resulting outbound messages, persist the log/hard-state, apply
/// committed entries, and signal stability back. <see cref="RaftNode"/> is the async driver that wires this to a
/// transport, storage, and timers. Mirrors the raft-rs <c>Raft</c>/<c>RawNode</c>.
/// </summary>
public sealed partial class RaftCore
{
    internal const ulong None = 0;
    internal const ulong CampaignTransfer = ulong.MaxValue;

    private readonly RaftLog _log;
    private readonly ProgressTracker _tracker;
    private readonly List<Message> _msgs = new();
    private readonly List<Message> _msgsAfterAppend = new();
    private readonly Random _rng;
    private readonly ulong _maxSizePerMessage;
    private readonly ulong _maxUncommittedSize;
    private readonly bool _disableProposalForwarding;
    private ulong _uncommittedSize;

    private int _electionElapsed;
    private int _heartbeatElapsed;
    private int _randomizedElectionTimeout;
    private readonly int _fixedElectionTimeout;
    private HardState _prevHardState;

    /// <summary>Initializes a new instance of the <see cref="RaftCore"/> class.</summary>
    /// <param name="config">The node configuration.</param>
    /// <param name="storage">The durable backing store.</param>
    public RaftCore(RaftConfig config, IRaftStorage storage)
    {
        Internal.Check.NotNull(config);
        Internal.Check.NotNull(storage);
        config.Validate();

        Id = config.Id;
        ElectionTimeout = config.ElectionTick;
        HeartbeatTimeout = config.HeartbeatTick;
        PreVote = config.PreVote;
        CheckQuorum = config.CheckQuorum;
        _maxSizePerMessage = config.MaxSizePerMessage;
        _maxUncommittedSize = config.MaxUncommittedEntriesSize;
        _disableProposalForwarding = config.DisableProposalForwarding;
        _rng = new Random(unchecked((int)(config.Id * 2654435761u)));
        _log = new RaftLog(storage);
        _tracker = new ProgressTracker(config.MaxInflightMessages, config.MaxInflightBytes);

        (HardState hardState, ConfState confState) = storage.InitialState();
        _tracker.ApplyConf(confState, 1);

        Term = hardState.Term;
        Vote = hardState.Vote;
        if (hardState.Commit > 0)
        {
            _log.CommitTo(hardState.Commit);
        }

        _fixedElectionTimeout = config.RandomizedElectionTimeout;
        BecomeFollower(Term, None);
        if (_fixedElectionTimeout > 0)
        {
            _randomizedElectionTimeout = _fixedElectionTimeout;
        }

        _prevHardState = HardState;
    }

    /// <summary>Gets this node's id.</summary>
    public ulong Id { get; }

    /// <summary>Gets the current term.</summary>
    public ulong Term { get; private set; }

    /// <summary>Gets the candidate voted for in the current term, or zero.</summary>
    public ulong Vote { get; private set; }

    /// <summary>Gets the id of the leader this node recognizes, or zero when unknown.</summary>
    public ulong LeaderId { get; private set; }

    /// <summary>Gets the current role.</summary>
    public RaftRole Role { get; private set; }

    /// <summary>Gets the election timeout in ticks.</summary>
    public int ElectionTimeout { get; }

    /// <summary>Gets the heartbeat timeout in ticks.</summary>
    public int HeartbeatTimeout { get; }

    /// <summary>Gets a value indicating whether pre-vote is enabled.</summary>
    public bool PreVote { get; }

    /// <summary>Gets a value indicating whether check-quorum is enabled.</summary>
    public bool CheckQuorum { get; }

    /// <summary>Gets the leadership-transfer target, or zero when no transfer is in progress.</summary>
    public ulong LeadTransferee { get; private set; }

    /// <summary>Gets the replication progress tracker.</summary>
    public ProgressTracker Tracker => _tracker;

    internal RaftLog Log => _log;

    internal ulong PendingConfIndex { get; private set; }

    internal ulong UncommittedSize => _uncommittedSize;

    /// <summary>Gets the highest committed log index.</summary>
    public ulong CommitIndex => _log.Committed;

    /// <summary>Gets this node's current observable soft state.</summary>
    public SoftState SoftState => new(LeaderId, Role);

    /// <summary>Gets this node's durable hard state.</summary>
    public HardState HardState => new(Term, Vote, _log.Committed);

    /// <summary>
    /// Removes and returns the messages queued for transmission since the last drain, including both immediately
    /// sendable messages and durability-gated responses. This is the synchronous-driver entry point; callers that
    /// persist before draining (so responses are released only after the write is durable) use this. Asynchronous
    /// drivers instead pair <see cref="TakeSendNowMessages"/> with <see cref="TakeStorageWrite"/>.
    /// </summary>
    /// <returns>The outbound messages.</returns>
    public IReadOnlyList<Message> TakeMessages()
    {
        if (_msgs.Count == 0 && _msgsAfterAppend.Count == 0)
        {
            return Array.Empty<Message>();
        }

        var taken = new List<Message>(_msgs.Count + _msgsAfterAppend.Count);
        taken.AddRange(_msgs);
        taken.AddRange(_msgsAfterAppend);
        _msgs.Clear();
        _msgsAfterAppend.Clear();
        return taken;
    }

    /// <summary>
    /// Removes and returns only the messages that may be sent immediately, before the unstable log and hard state are
    /// made durable (leader appends/heartbeats, vote requests, snapshots, timeout-now). Durability-gated responses are
    /// carried by <see cref="TakeStorageWrite"/> instead.
    /// </summary>
    /// <returns>The immediately sendable messages.</returns>
    public IReadOnlyList<Message> TakeSendNowMessages()
    {
        if (_msgs.Count == 0)
        {
            return Array.Empty<Message>();
        }

        var taken = _msgs.ToArray();
        _msgs.Clear();
        return taken;
    }

    /// <summary>
    /// Drains the next durable write: the unstable entries to append, the hard state (when changed since the last
    /// write), the pending snapshot, and the responses to release once the write is durable. Returns
    /// <see langword="null"/> when there is nothing to persist or release. The driver persists the returned write and
    /// then calls <see cref="AckStorageWrite"/>. At most one write should be outstanding at a time.
    /// </summary>
    /// <returns>The pending storage write, or <see langword="null"/>.</returns>
    public StorageWrite? TakeStorageWrite()
    {
        IReadOnlyList<Entry> entries = _log.UnstableEntries;
        Snapshot? snapshot = _log.UnstableSnapshot;
        HardState current = HardState;
        bool hardChanged = current != _prevHardState;

        if (entries.Count == 0 && snapshot is null && !hardChanged && _msgsAfterAppend.Count == 0)
        {
            return null;
        }

        Message[] responses = _msgsAfterAppend.Count == 0
            ? Array.Empty<Message>()
            : _msgsAfterAppend.ToArray();
        _msgsAfterAppend.Clear();

        Entry[] toPersist = entries.Count == 0 ? Array.Empty<Entry>() : new Entry[entries.Count];
        for (int i = 0; i < entries.Count; i++)
        {
            toPersist[i] = entries[i];
        }

        ulong lastIndex = toPersist.Length > 0 ? toPersist[toPersist.Length - 1].Index : 0;
        ulong lastTerm = toPersist.Length > 0 ? toPersist[toPersist.Length - 1].Term : 0;
        ulong snapshotIndex = snapshot?.Metadata.Index ?? 0;
        HardState? hardState = hardChanged ? current : null;
        if (hardChanged)
        {
            _prevHardState = current;
        }

        return new StorageWrite(toPersist, hardState, snapshot, responses, lastIndex, lastTerm, snapshotIndex);
    }

    /// <summary>
    /// Acknowledges that a previously taken <see cref="StorageWrite"/> is durable, advancing the stable log and (for a
    /// leader) its own match index, which may in turn advance the commit index and trigger further replication.
    /// </summary>
    /// <param name="write">The write that has been persisted.</param>
    public void AckStorageWrite(StorageWrite write)
    {
        Internal.Check.NotNull(write);
        if (write.SnapshotIndex != 0)
        {
            StableSnapshotTo(write.SnapshotIndex);
        }

        if (write.LastEntryIndex != 0)
        {
            StableTo(write.LastEntryIndex, write.LastEntryTerm);
        }
    }

    /// <summary>Returns the entries appended since the last stable point that must be persisted.</summary>
    /// <returns>The unstable entries.</returns>
    public IReadOnlyList<Entry> UnstableEntries() => _log.UnstableEntries;

    /// <summary>Returns the snapshot pending persistence, if any.</summary>
    /// <returns>The unstable snapshot, or <see langword="null"/>.</returns>
    public Snapshot? UnstableSnapshot() => _log.UnstableSnapshot;

    /// <summary>Returns committed-but-unapplied entries for the state machine to apply.</summary>
    /// <returns>The next entries to apply.</returns>
    public IReadOnlyList<Entry> NextEntriesToApply() => _log.NextEntries(_maxSizePerMessage);

    /// <summary>Marks the log persisted up to <paramref name="index"/> at <paramref name="term"/>.</summary>
    /// <param name="index">The highest persisted index.</param>
    /// <param name="term">The term at <paramref name="index"/>.</param>
    public void StableTo(ulong index, ulong term)
    {
        _log.StableEntriesTo(index, term);

        // The leader counts its own log entries toward commit only once they are durable on its disk. This is what
        // lets the driver replicate to followers in parallel with the leader's local write (etcd's "writing to the
        // leader's disk in parallel").
        if (Role == RaftRole.Leader && index != 0)
        {
            Raft.Progress.Progress? self = _tracker.GetProgress(Id);
            if (self is not null && self.MaybeUpdate(index) && MaybeCommit())
            {
                BcastAppend();
            }
        }
    }

    /// <summary>Marks the pending snapshot persisted.</summary>
    /// <param name="index">The snapshot index.</param>
    public void StableSnapshotTo(ulong index) => _log.StableSnapshotTo(index);

    /// <summary>Records that the state machine has applied entries up to <paramref name="index"/>.</summary>
    /// <param name="index">The highest applied index.</param>
    public void AppliedTo(ulong index) => _log.AppliedTo(index);

    /// <summary>Advances the logical clock by one tick.</summary>
    public void Tick()
    {
        if (Role == RaftRole.Leader)
        {
            TickHeartbeat();
        }
        else
        {
            TickElection();
        }
    }

    /// <summary>Feeds one message into the state machine.</summary>
    /// <param name="message">The message to process.</param>
    public void Step(Message message)
    {
        Internal.Check.NotNull(message);

        if (message.Term == 0)
        {
            // Local signal.
        }
        else if (message.Term > Term)
        {
            if (!StepHigherTerm(message))
            {
                return;
            }
        }
        else if (message.Term < Term)
        {
            StepLowerTerm(message);
            return;
        }

        switch (message.Type)
        {
            case MessageType.Hup:
                Hup();
                return;
            case MessageType.RequestVote:
            case MessageType.RequestPreVote:
                HandleVoteRequest(message);
                return;
        }

        switch (Role)
        {
            case RaftRole.Leader:
                StepLeader(message);
                break;
            case RaftRole.Candidate:
            case RaftRole.PreCandidate:
                StepCandidate(message);
                break;
            case RaftRole.Follower:
                StepFollower(message);
                break;
        }
    }

    private bool StepHigherTerm(Message message)
    {
        bool isVoteRequest = message.Type is MessageType.RequestVote or MessageType.RequestPreVote;
        if (isVoteRequest)
        {
            bool force = message.Context == CampaignTransfer;
            bool inLease = CheckQuorum && LeaderId != None && _electionElapsed < _randomizedElectionTimeout;
            if (!force && inLease)
            {
                return false;
            }
        }

        if (message.Type == MessageType.RequestPreVote)
        {
            return true;
        }

        if (message.Type == MessageType.RequestPreVoteResponse && !message.Reject)
        {
            return true;
        }

        if (message.Type is MessageType.Append or MessageType.Heartbeat or MessageType.Snapshot)
        {
            BecomeFollower(message.Term, message.From);
        }
        else
        {
            BecomeFollower(message.Term, None);
        }

        return true;
    }

    private void StepLowerTerm(Message message)
    {
        if ((CheckQuorum || PreVote)
            && message.Type is MessageType.Heartbeat or MessageType.Append)
        {
            Send(new Message { To = message.From, Type = MessageType.AppendResponse });
        }
        else if (message.Type == MessageType.RequestPreVote)
        {
            Send(new Message
            {
                To = message.From,
                Term = Term,
                Type = MessageType.RequestPreVoteResponse,
                Reject = true,
            });
        }
    }

    private void HandleVoteRequest(Message message)
    {
        bool isPreVote = message.Type == MessageType.RequestPreVote;
        bool canVote =
            Vote == message.From
            || (Vote == None && LeaderId == None)
            || (isPreVote && message.Term > Term);

        bool upToDate = _log.IsUpToDate(message.Index, message.LogTerm);
        var response = new Message
        {
            To = message.From,
            Term = isPreVote ? message.Term : Term,
            Type = isPreVote ? MessageType.RequestPreVoteResponse : MessageType.RequestVoteResponse,
        };

        if (canVote && upToDate)
        {
            response.Reject = false;
            if (!isPreVote)
            {
                _electionElapsed = 0;
                Vote = message.From;
            }
        }
        else
        {
            response.Reject = true;
        }

        Send(response);
    }

    private void Hup()
    {
        if (Role == RaftRole.Leader)
        {
            return;
        }

        if (!_tracker.IsVoter(Id))
        {
            return;
        }

        Campaign(PreVote ? CampaignType.PreElection : CampaignType.Election);
    }

    private void Campaign(CampaignType campaignType)
    {
        ulong term;
        MessageType voteType;
        if (campaignType == CampaignType.PreElection)
        {
            BecomePreCandidate();
            voteType = MessageType.RequestPreVote;
            term = Term + 1;
        }
        else
        {
            BecomeCandidate();
            voteType = MessageType.RequestVote;
            term = Term;
        }

        _tracker.RecordVote(Id, true);
        (_, _, VoteResult selfResult) = _tracker.TallyVotes();
        if (selfResult == VoteResult.Won)
        {
            if (campaignType == CampaignType.PreElection)
            {
                Campaign(CampaignType.Election);
            }
            else
            {
                BecomeLeader();
            }

            return;
        }

        ulong lastIndex = _log.LastIndex();
        ulong lastTerm = _log.LastTerm();
        foreach (ulong id in _tracker.Voters.Ids())
        {
            if (id == Id)
            {
                continue;
            }

            Send(new Message
            {
                To = id,
                Term = term,
                Type = voteType,
                Index = lastIndex,
                LogTerm = lastTerm,
                Context = campaignType == CampaignType.Transfer ? CampaignTransfer : None,
            });
        }
    }

    internal void BecomeFollower(ulong term, ulong leaderId)
    {
        Reset(term);
        LeaderId = leaderId;
        Role = RaftRole.Follower;
    }

    internal void BecomePreCandidate()
    {
        _tracker.ResetVotes();
        Role = RaftRole.PreCandidate;
        LeaderId = None;
    }

    internal void BecomeCandidate()
    {
        Reset(Term + 1);
        Vote = Id;
        Role = RaftRole.Candidate;
    }

    internal void BecomeLeader()
    {
        Reset(Term);
        LeaderId = Id;
        Role = RaftRole.Leader;
        _uncommittedSize = 0;

        PendingConfIndex = _log.LastIndex();
        var noop = new Entry(EntryType.Normal, Term, _log.LastIndex() + 1, ReadOnlyMemory<byte>.Empty);
        AppendEntries(new[] { noop });
    }

    private void Reset(ulong term)
    {
        if (Term != term)
        {
            Term = term;
            Vote = None;
        }

        LeaderId = None;
        _electionElapsed = 0;
        _heartbeatElapsed = 0;
        ResetRandomizedElectionTimeout();
        LeadTransferee = None;
        _tracker.ResetVotes();

        ulong nextIndex = _log.LastIndex() + 1;
        foreach (ulong id in EnumeratePeers())
        {
            Raft.Progress.Progress? progress = _tracker.GetProgress(id);
            if (progress is null)
            {
                continue;
            }

            progress.NextIndex = nextIndex;
            progress.MatchIndex = 0;
            progress.Inflights.Reset();
            progress.ProbeSent = false;
            progress.State = ProgressState.Probe;
            progress.RecentActive = false;
            if (id == Id)
            {
                progress.MatchIndex = _log.LastIndex();
                progress.RecentActive = true;
            }
        }
    }

    private HashSet<ulong> EnumeratePeers()
    {
        var ids = _tracker.Voters.Ids();
        ids.UnionWith(_tracker.Learners);
        ids.UnionWith(_tracker.LearnersNext);
        return ids;
    }

    private void TickElection()
    {
        _electionElapsed++;
        if (_tracker.IsVoter(Id) && _electionElapsed >= _randomizedElectionTimeout)
        {
            _electionElapsed = 0;
            Step(new Message { From = Id, Type = MessageType.Hup });
        }
    }

    private void TickHeartbeat()
    {
        _heartbeatElapsed++;
        _electionElapsed++;

        if (_electionElapsed >= ElectionTimeout)
        {
            _electionElapsed = 0;
            if (CheckQuorum && !_tracker.QuorumRecentlyActive(Id))
            {
                BecomeFollower(Term, None);
            }

            _tracker.ResetRecentActive(Id);
            if (Role != RaftRole.Leader)
            {
                return;
            }

            if (LeadTransferee != None)
            {
                LeadTransferee = None;
            }
        }

        if (Role != RaftRole.Leader)
        {
            return;
        }

        if (_heartbeatElapsed >= HeartbeatTimeout)
        {
            _heartbeatElapsed = 0;
            Step(new Message { From = Id, Type = MessageType.Beat });
        }
    }

    private void ResetRandomizedElectionTimeout()
    {
        _randomizedElectionTimeout = _fixedElectionTimeout > 0
            ? _fixedElectionTimeout
            : ElectionTimeout + _rng.Next(ElectionTimeout);
    }

    internal void Send(Message message)
    {
        if (message.From == None)
        {
            message.From = Id;
        }

        // Responses that reflect newly durable state (a follower acknowledging appended entries, or a node granting a
        // vote) must not be released until the unstable log and hard state are persisted. They are queued separately
        // and carried by TakeStorageWrite so the asynchronous driver can hold them back until the write completes.
        if (message.Type is MessageType.AppendResponse
            or MessageType.RequestVoteResponse
            or MessageType.RequestPreVoteResponse)
        {
            _msgsAfterAppend.Add(message);
        }
        else
        {
            _msgs.Add(message);
        }
    }

    private enum CampaignType
    {
        PreElection,
        Election,
        Transfer,
    }
}
