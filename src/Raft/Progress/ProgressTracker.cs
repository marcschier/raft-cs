// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;

namespace Raft.Progress;

/// <summary>
/// Tracks per-peer <see cref="Progress"/>, the current joint voter/learner configuration, and the votes cast in the
/// current election. Mirrors the raft-rs <c>ProgressTracker</c>.
/// </summary>
public sealed class ProgressTracker
{
    private readonly Dictionary<ulong, Progress> _progress = new();
    private readonly Dictionary<ulong, bool> _votes = new();
    private readonly int _maxInflight;
    private readonly ulong _maxInflightBytes;

    /// <summary>Initializes a new instance of the <see cref="ProgressTracker"/> class.</summary>
    /// <param name="maxInflight">The per-peer flow-control window capacity (message count).</param>
    /// <param name="maxInflightBytes">The per-peer flow-control window capacity in payload bytes.</param>
    public ProgressTracker(int maxInflight, ulong maxInflightBytes = ulong.MaxValue)
    {
        _maxInflight = maxInflight;
        _maxInflightBytes = maxInflightBytes;
        Voters = new JointConfig();
        Learners = new HashSet<ulong>();
        LearnersNext = new HashSet<ulong>();
    }

    /// <summary>Gets the joint voter configuration.</summary>
    public JointConfig Voters { get; private set; }

    /// <summary>Gets the learner ids.</summary>
    public HashSet<ulong> Learners { get; }

    /// <summary>Gets learners that become voters when a joint change leaves.</summary>
    public HashSet<ulong> LearnersNext { get; }

    /// <summary>Gets or sets a value indicating whether the leader should auto-leave the joint configuration.</summary>
    public bool AutoLeave { get; set; }

    /// <summary>Gets the per-peer progress map.</summary>
    public IReadOnlyDictionary<ulong, Progress> Peers => _progress;

    /// <summary>Gets the per-peer flow-control window capacity.</summary>
    public int MaxInflight => _maxInflight;

    /// <summary>Returns the progress for <paramref name="id"/>, or <see langword="null"/> when absent.</summary>
    /// <param name="id">The peer id.</param>
    /// <returns>The progress, or <see langword="null"/>.</returns>
    public Progress? GetProgress(ulong id) => _progress.TryGetValue(id, out Progress? p) ? p : null;

    /// <summary>Returns whether <paramref name="id"/> is a voter.</summary>
    /// <param name="id">The node id.</param>
    /// <returns><see langword="true"/> when the node votes.</returns>
    public bool IsVoter(ulong id) => Voters.Contains(id);

    /// <summary>Returns the highest log index committed by the current joint quorum.</summary>
    /// <returns>The committed index.</returns>
    public ulong Committed()
    {
        return Voters.CommittedIndex(id => _progress.TryGetValue(id, out Progress? p) ? p.MatchIndex : 0);
    }

    /// <summary>Clears recorded votes for a new election.</summary>
    public void ResetVotes() => _votes.Clear();

    /// <summary>Records a vote from <paramref name="id"/>.</summary>
    /// <param name="id">The voter id.</param>
    /// <param name="granted">Whether the vote was granted.</param>
    public void RecordVote(ulong id, bool granted)
    {
        if (!_votes.ContainsKey(id))
        {
            _votes[id] = granted;
        }
    }

    /// <summary>Tallies the recorded votes.</summary>
    /// <returns>The granted count, rejected count, and combined result.</returns>
    public (int Granted, int Rejected, VoteResult Result) TallyVotes()
    {
        int granted = 0;
        int rejected = 0;
        foreach (KeyValuePair<ulong, bool> vote in _votes)
        {
            if (!Voters.Contains(vote.Key))
            {
                continue;
            }

            if (vote.Value)
            {
                granted++;
            }
            else
            {
                rejected++;
            }
        }

        return (granted, rejected, Voters.TallyVotes(_votes));
    }

    /// <summary>Returns whether a quorum of voters has been recently active (for check-quorum).</summary>
    /// <returns><see langword="true"/> when the leader still has quorum contact.</returns>
    public bool QuorumRecentlyActive(ulong selfId)
    {
        var active = new Dictionary<ulong, bool>();
        foreach (KeyValuePair<ulong, Progress> peer in _progress)
        {
            if (!Voters.Contains(peer.Key))
            {
                continue;
            }

            active[peer.Key] = peer.Key == selfId || peer.Value.RecentActive;
        }

        return Voters.TallyVotes(active) == VoteResult.Won;
    }

    /// <summary>Clears the recent-active flag on every peer (called each check-quorum interval).</summary>
    public void ResetRecentActive(ulong selfId)
    {
        foreach (KeyValuePair<ulong, Progress> peer in _progress)
        {
            peer.Value.RecentActive = peer.Key == selfId;
        }
    }

    /// <summary>Replaces the configuration and per-peer progress, preserving match/next where possible.</summary>
    /// <param name="confState">The new configuration.</param>
    /// <param name="nextIndex">The leader's next index used to seed new peers.</param>
    public void ApplyConf(ConfState confState, ulong nextIndex)
    {
        Voters = new JointConfig(
            new MajorityConfig(confState.Voters),
            new MajorityConfig(confState.VotersOutgoing));
        Learners.Clear();
        foreach (ulong id in confState.Learners)
        {
            Learners.Add(id);
        }

        LearnersNext.Clear();
        foreach (ulong id in confState.LearnersNext)
        {
            LearnersNext.Add(id);
        }

        AutoLeave = confState.AutoLeave;

        var ids = Voters.Ids();
        ids.UnionWith(Learners);
        ids.UnionWith(LearnersNext);

        foreach (ulong id in ids)
        {
            if (!_progress.TryGetValue(id, out Progress? progress))
            {
                progress = new Progress(nextIndex, _maxInflight, _maxInflightBytes);
                _progress[id] = progress;
            }

            progress.IsLearner = Learners.Contains(id) && !Voters.Contains(id);
        }

        var stale = new List<ulong>();
        foreach (ulong id in _progress.Keys)
        {
            if (!ids.Contains(id))
            {
                stale.Add(id);
            }
        }

        foreach (ulong id in stale)
        {
            _progress.Remove(id);
        }
    }

    /// <summary>Builds the <see cref="ConfState"/> describing the current configuration.</summary>
    /// <returns>The configuration state.</returns>
    public ConfState ToConfState()
    {
        return new ConfState(
            new List<ulong>(Voters.Incoming.Voters),
            new List<ulong>(Learners),
            new List<ulong>(Voters.Outgoing.Voters),
            new List<ulong>(LearnersNext),
            AutoLeave);
    }
}
