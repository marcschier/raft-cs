// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Messages;
using Raft.Storage;

namespace Raft;

public sealed partial class RaftCore
{
    // The log and election code use one-past-the-end positions.
    private const ulong MaxLogIndex = ulong.MaxValue - 1;
    private const ulong MaxTerm = ulong.MaxValue - 1;

    internal bool HasValidSnapshotMetadata(SnapshotMetadata metadata)
    {
        return metadata.Index is > 0 and <= MaxLogIndex
            && metadata.Term is > 0 and <= MaxTerm
            && IsValidConfiguration(metadata.ConfState);
    }

    internal bool AcceptsSnapshotData(Snapshot snapshot)
    {
        try
        {
            return _snapshotValidator?.Invoke(snapshot) ?? true;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return false;
        }
    }

    private bool AcceptLocalMessage(Message message)
    {
        if (message.Term != None || message.Snapshot is not null)
        {
            return false;
        }

        return message.Type switch
        {
            MessageType.Hup or MessageType.Beat => (message.From == None || message.From == Id)
                && message.Entries is { Count: 0 },
            MessageType.TransferLeader => message.From != None && message.Entries is { Count: 0 },
            MessageType.Propose => (message.From == None || message.From == Id)
                && ValidProposalEntries(message, allowConfigurationChanges: Role == RaftRole.Leader),
            _ => false,
        };
    }

    private bool AcceptPeerMessage(Message message)
    {
        if (message.To != Id || message.From == None || message.From == Id || message.Entries is null)
        {
            return false;
        }

        if (message.Type == MessageType.Propose)
        {
            return message.Term == None && Role == RaftRole.Leader && message.Snapshot is null
                && _tracker.GetProgress(message.From) is not null
                && ValidProposalEntries(message, allowConfigurationChanges: false);
        }

        if (message.Term is 0 or > MaxTerm
            || message.Index > MaxLogIndex || message.Commit > MaxLogIndex || message.RejectHint > MaxLogIndex
            || (message.Type != MessageType.Append && message.Entries.Count != 0)
            || (message.Type != MessageType.Snapshot && message.Snapshot is not null)
            || (message.Reject && message.Type is not (MessageType.AppendResponse
                or MessageType.RequestVoteResponse or MessageType.RequestPreVoteResponse)))
        {
            return false;
        }

        return message.Type switch
        {
            MessageType.Append => CanReceiveLeaderMessage(message) && ValidAppend(message),
            MessageType.Heartbeat => CanReceiveLeaderMessage(message),
            MessageType.Snapshot => message.Term >= Term && CanReceiveLeaderMessage(message)
                && message.Snapshot is { } snapshot && AcceptPeerSnapshot(message, snapshot),
            MessageType.AppendResponse => _tracker.GetProgress(message.From) is not null
                && (message.Term != Term || Role != RaftRole.Leader || message.Index <= _log.LastIndex()),
            MessageType.HeartbeatResponse => _tracker.GetProgress(message.From) is not null,
            MessageType.RequestVote or MessageType.RequestPreVote => _tracker.IsVoter(Id)
                && _tracker.IsVoter(message.From) && ValidLogPosition(message),
            MessageType.RequestVoteResponse => _tracker.IsVoter(message.From),
            MessageType.RequestPreVoteResponse => _tracker.IsVoter(message.From)
                && (message.Reject || (Role == RaftRole.PreCandidate && message.Term == Term + 1)),
            MessageType.TimeoutNow => Role == RaftRole.Follower && message.Term == Term && message.From == LeaderId
                && _tracker.IsVoter(Id) && _tracker.IsVoter(message.From),
            _ => false,
        };
    }

    private bool CanReceiveLeaderMessage(Message message)
    {
        return _tracker.IsVoter(message.From)
            && (message.Term != Term || LeaderId == None || LeaderId == message.From);
    }

    private bool AcceptPeerSnapshot(Message message, Snapshot snapshot)
    {
        SnapshotMetadata metadata = snapshot.Metadata;
        if (!HasValidSnapshotMetadata(metadata) || metadata.Term > message.Term)
        {
            return false;
        }

        // A stale snapshot is acknowledged and a matching log only advances commit; neither replaces membership.
        // An installation must preserve the leader's eligibility for the following append, but need not name us.
        return metadata.Index <= _log.Committed
            || (metadata.Term >= _log.Term(_log.Committed)
                && (_log.MatchTerm(metadata.Index, metadata.Term)
                    || (SnapshotRespectsRecoveredConfiguration(metadata)
                        && SnapshotContainsVoter(snapshot, message.From)
                        && AcceptsSnapshotData(snapshot))));
    }

    private bool SnapshotRespectsRecoveredConfiguration(SnapshotMetadata metadata) =>
        metadata.Index > _recoveredConfigurationIndex
        || (metadata.Index == _recoveredConfigurationIndex
            && _tracker.ToConfState().Equals(metadata.ConfState));

    private static bool SnapshotContainsVoter(Snapshot snapshot, ulong voter) =>
        snapshot.Metadata.ConfState is { } configuration
        && (configuration.Voters.Contains(voter) || configuration.VotersOutgoing.Contains(voter));

    private bool IsValidConfiguration(ConfState? confState)
    {
        if (confState is null || confState.Voters.Count == 0
            || confState.Voters.Count > _maxClusterMembers || confState.Learners.Count > _maxClusterMembers
            || confState.VotersOutgoing.Count > _maxClusterMembers || confState.LearnersNext.Count > _maxClusterMembers
            || (!confState.IsJoint && (confState.AutoLeave || confState.LearnersNext.Count != 0)))
        {
            return false;
        }

        var voters = new HashSet<ulong>(confState.Voters);
        var outgoing = new HashSet<ulong>(confState.VotersOutgoing);
        var learners = new HashSet<ulong>(confState.Learners);
        var learnersNext = new HashSet<ulong>(confState.LearnersNext);
        if (voters.Count != confState.Voters.Count || outgoing.Count != confState.VotersOutgoing.Count
            || learners.Count != confState.Learners.Count || learnersNext.Count != confState.LearnersNext.Count
            || voters.Contains(None) || outgoing.Contains(None) || learners.Contains(None)
            || learners.Overlaps(voters) || learners.Overlaps(outgoing)
            || learnersNext.Overlaps(voters) || !learnersNext.IsSubsetOf(outgoing))
        {
            return false;
        }

        voters.UnionWith(outgoing);
        return voters.Count <= _maxClusterMembers - learners.Count;
    }

    private static bool ValidLogPosition(Message message)
    {
        return message.LogTerm <= message.Term && (message.Index == None) == (message.LogTerm == None);
    }

    private static bool ValidAppend(Message message)
    {
        IReadOnlyList<Entry> entries = message.Entries;
        if (!ValidLogPosition(message) || (ulong)entries.Count > MaxLogIndex - message.Index)
        {
            return false;
        }

        ulong index = message.Index;
        ulong previousTerm = message.LogTerm;
        foreach (Entry entry in entries)
        {
            if (entry.Index != ++index || entry.Term == None || entry.Term < previousTerm
                || entry.Term > message.Term || !ValidEntryPayload(entry))
            {
                return false;
            }

            previousTerm = entry.Term;
        }

        return true;
    }

    private static bool ValidProposalEntries(Message message, bool allowConfigurationChanges)
    {
        if (message.Entries is not { Count: > 0 } entries)
        {
            return false;
        }

        foreach (Entry entry in entries)
        {
            if (entry.Type != EntryType.Normal && (!allowConfigurationChanges || !ValidEntryPayload(entry)))
            {
                return false;
            }
        }

        return true;
    }

    private static bool ValidEntryPayload(Entry entry) => entry.Type switch
    {
        EntryType.Normal => true,
        EntryType.ConfChange or EntryType.ConfChangeV2 => ConfChangeV2.IsValidEncoding(entry.Data.Span),
        _ => false,
    };
}
