// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Messages;
using Raft.Storage;

namespace Raft;

/// <summary>Follower and candidate behavior for <see cref="RaftCore"/>: snapshots and conf-change apply.</summary>
public sealed partial class RaftCore
{
    private void StepCandidate(Message message)
    {
        switch (message.Type)
        {
            case MessageType.Append:
                BecomeFollower(message.Term, message.From);
                HandleAppendEntries(message);
                return;
            case MessageType.Heartbeat:
                BecomeFollower(message.Term, message.From);
                HandleHeartbeat(message);
                return;
            case MessageType.Snapshot:
                BecomeFollower(message.Term, message.From);
                HandleSnapshot(message);
                return;
            case MessageType.RequestVoteResponse when Role == RaftRole.Candidate:
            case MessageType.RequestPreVoteResponse when Role == RaftRole.PreCandidate:
                HandleVoteResponse(message);
                return;
        }
    }

    private void StepFollower(Message message)
    {
        switch (message.Type)
        {
            case MessageType.Append:
                _electionElapsed = 0;
                LeaderId = message.From;
                HandleAppendEntries(message);
                return;
            case MessageType.Heartbeat:
                _electionElapsed = 0;
                LeaderId = message.From;
                HandleHeartbeat(message);
                return;
            case MessageType.Snapshot:
                _electionElapsed = 0;
                LeaderId = message.From;
                HandleSnapshot(message);
                return;
            case MessageType.TimeoutNow:
                Campaign(CampaignType.Transfer);
                return;
        }
    }

    private void HandleVoteResponse(Message message)
    {
        _tracker.RecordVote(message.From, !message.Reject);
        (_, _, VoteResult result) = _tracker.TallyVotes();
        switch (result)
        {
            case VoteResult.Won:
                if (Role == RaftRole.PreCandidate)
                {
                    Campaign(CampaignType.Election);
                }
                else
                {
                    BecomeLeader();
                    BcastAppend();
                }

                break;
            case VoteResult.Lost:
                BecomeFollower(Term, None);
                break;
        }
    }

    private void HandleAppendEntries(Message message)
    {
        if (message.Index < _log.Committed)
        {
            Send(new Message { To = message.From, Type = MessageType.AppendResponse, Index = _log.Committed });
            return;
        }

        if (_log.TryMaybeAppend(
            message.Index, message.LogTerm, message.Commit, message.Entries, out ulong lastNewIndex))
        {
            Send(new Message { To = message.From, Type = MessageType.AppendResponse, Index = lastNewIndex });
        }
        else
        {
            Send(new Message
            {
                To = message.From,
                Type = MessageType.AppendResponse,
                Index = message.Index,
                Reject = true,
                RejectHint = _log.LastIndex(),
            });
        }
    }

    private void HandleHeartbeat(Message message)
    {
        _log.CommitTo(Math.Min(message.Commit, _log.LastIndex()));
        Send(new Message
        {
            To = message.From,
            Type = MessageType.HeartbeatResponse,
            Context = message.Context,
        });
    }

    private void HandleSnapshot(Message message)
    {
        Snapshot? snapshot = message.Snapshot;
        if (snapshot is null)
        {
            return;
        }

        if (Restore(snapshot))
        {
            Send(new Message { To = message.From, Type = MessageType.AppendResponse, Index = _log.LastIndex() });
        }
        else
        {
            Send(new Message { To = message.From, Type = MessageType.AppendResponse, Index = _log.Committed });
        }
    }

    private bool Restore(Snapshot snapshot)
    {
        if (snapshot.Metadata.Index <= _log.Committed)
        {
            return false;
        }

        if (!_log.TryRestore(snapshot))
        {
            return false;
        }

        _tracker.ApplyConf(snapshot.Metadata.ConfState, _log.LastIndex() + 1);
        return true;
    }

    /// <summary>
    /// Applies a committed configuration to the tracker. The caller computes the <see cref="ConfState"/> (via
    /// <see cref="Changer"/>) when it applies a conf-change entry, then passes it here so the consensus state agrees.
    /// </summary>
    /// <param name="confState">The new committed configuration.</param>
    public void ApplyConfChange(ConfState confState)
    {
        Internal.Check.NotNull(confState);

        _tracker.ApplyConf(confState, _log.LastIndex() + 1);
        Progress.Progress? self = _tracker.GetProgress(Id);
        if (Role == RaftRole.Leader && self is not null)
        {
            self.MatchIndex = _log.LastIndex();
            self.RecentActive = true;
            MaybeCommit();
        }

        if (LeadTransferee != None && !_tracker.IsVoter(LeadTransferee))
        {
            LeadTransferee = None;
        }
    }

    /// <summary>Returns whether the leader should propose an automatic leave-joint change.</summary>
    /// <returns><see langword="true"/> when an auto-leave conf change is due.</returns>
    internal bool ShouldAutoLeaveJoint()
    {
        return Role == RaftRole.Leader
            && _tracker.Voters.IsJoint
            && _tracker.AutoLeave
            && _log.Applied >= PendingConfIndex;
    }
}
