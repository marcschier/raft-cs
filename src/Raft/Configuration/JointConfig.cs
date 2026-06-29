// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Configuration;

/// <summary>The outcome of tallying votes for an election.</summary>
public enum VoteResult : byte
{
    /// <summary>Not enough votes have been cast to decide.</summary>
    Pending = 0,

    /// <summary>A quorum granted the vote.</summary>
    Won = 1,

    /// <summary>A quorum rejected the vote.</summary>
    Lost = 2,
}

/// <summary>A single set of voters forming one majority quorum.</summary>
public sealed class MajorityConfig
{
    /// <summary>Initializes a new instance of the <see cref="MajorityConfig"/> class.</summary>
    /// <param name="voters">The voter ids.</param>
    public MajorityConfig(IEnumerable<ulong>? voters = null)
    {
        Voters = voters is null ? new HashSet<ulong>() : new HashSet<ulong>(voters);
    }

    /// <summary>Gets the voter ids.</summary>
    public HashSet<ulong> Voters { get; }

    /// <summary>Returns the highest index that a majority of voters has reached.</summary>
    /// <param name="matchIndex">A function returning a voter's match index (0 when unknown).</param>
    /// <returns>The committed index, or <see cref="ulong.MaxValue"/> for the empty set.</returns>
    public ulong CommittedIndex(Func<ulong, ulong> matchIndex)
    {
        if (Voters.Count == 0)
        {
            return ulong.MaxValue;
        }

        var matched = new ulong[Voters.Count];
        int i = 0;
        foreach (ulong id in Voters)
        {
            matched[i++] = matchIndex(id);
        }

        Array.Sort(matched);
        Array.Reverse(matched);
        int quorum = (matched.Length / 2) + 1;
        return matched[quorum - 1];
    }

    /// <summary>Tallies votes among this config's voters.</summary>
    /// <param name="votes">The cast votes keyed by voter id (true granted, false rejected).</param>
    /// <returns>The vote result.</returns>
    public VoteResult TallyVotes(IReadOnlyDictionary<ulong, bool> votes)
    {
        if (Voters.Count == 0)
        {
            return VoteResult.Won;
        }

        int granted = 0;
        int rejected = 0;
        foreach (ulong id in Voters)
        {
            if (votes.TryGetValue(id, out bool granted2))
            {
                if (granted2)
                {
                    granted++;
                }
                else
                {
                    rejected++;
                }
            }
        }

        int quorum = (Voters.Count / 2) + 1;
        if (granted >= quorum)
        {
            return VoteResult.Won;
        }

        if (rejected >= quorum)
        {
            return VoteResult.Lost;
        }

        return VoteResult.Pending;
    }
}

/// <summary>A joint quorum: the AND of an incoming and an outgoing <see cref="MajorityConfig"/>.</summary>
public sealed class JointConfig
{
    /// <summary>Initializes a new instance of the <see cref="JointConfig"/> class.</summary>
    /// <param name="incoming">The incoming (current) majority configuration.</param>
    /// <param name="outgoing">The outgoing majority configuration (empty when not joint).</param>
    public JointConfig(MajorityConfig? incoming = null, MajorityConfig? outgoing = null)
    {
        Incoming = incoming ?? new MajorityConfig();
        Outgoing = outgoing ?? new MajorityConfig();
    }

    /// <summary>Gets the incoming (current) majority configuration.</summary>
    public MajorityConfig Incoming { get; }

    /// <summary>Gets the outgoing majority configuration (empty when not joint).</summary>
    public MajorityConfig Outgoing { get; }

    /// <summary>Gets a value indicating whether the configuration is in the joint state.</summary>
    public bool IsJoint => Outgoing.Voters.Count > 0;

    /// <summary>Returns all distinct voter ids across both configurations.</summary>
    /// <returns>The set of voter ids.</returns>
    public HashSet<ulong> Ids()
    {
        var ids = new HashSet<ulong>(Incoming.Voters);
        ids.UnionWith(Outgoing.Voters);
        return ids;
    }

    /// <summary>Returns whether <paramref name="id"/> is a voter in either configuration.</summary>
    /// <param name="id">The node id.</param>
    /// <returns><see langword="true"/> when the node votes.</returns>
    public bool Contains(ulong id) => Incoming.Voters.Contains(id) || Outgoing.Voters.Contains(id);

    /// <summary>Returns the highest index committed by both configurations.</summary>
    /// <param name="matchIndex">A function returning a voter's match index.</param>
    /// <returns>The joint committed index.</returns>
    public ulong CommittedIndex(Func<ulong, ulong> matchIndex)
    {
        return Math.Min(Incoming.CommittedIndex(matchIndex), Outgoing.CommittedIndex(matchIndex));
    }

    /// <summary>Tallies votes across both configurations.</summary>
    /// <param name="votes">The cast votes.</param>
    /// <returns>The combined vote result.</returns>
    public VoteResult TallyVotes(IReadOnlyDictionary<ulong, bool> votes)
    {
        VoteResult incoming = Incoming.TallyVotes(votes);
        VoteResult outgoing = Outgoing.TallyVotes(votes);
        if (incoming == outgoing)
        {
            return incoming;
        }

        if (incoming == VoteResult.Lost || outgoing == VoteResult.Lost)
        {
            return VoteResult.Lost;
        }

        return VoteResult.Pending;
    }
}
