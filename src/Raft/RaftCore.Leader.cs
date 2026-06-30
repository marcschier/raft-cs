// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Messages;
using Raft.Progress;
using Raft.Storage;

namespace Raft;

/// <summary>Leader-role behavior for <see cref="RaftCore"/>: proposal, replication, commit, and transfer.</summary>
public sealed partial class RaftCore
{
    /// <summary>Appends entries to the local log as the leader, stamping term and contiguous indices.</summary>
    /// <param name="entries">The entries to append (term/index are assigned here).</param>
    /// <returns><see langword="true"/> when accepted, or <see langword="false"/> when the cap rejects it.</returns>
    internal bool AppendEntries(IReadOnlyList<Entry> entries)
    {
        ulong lastIndex = _log.LastIndex();
        var stamped = new Entry[entries.Count];
        for (int i = 0; i < entries.Count; i++)
        {
            Entry source = entries[i];
            stamped[i] = new Entry(source.Type, Term, lastIndex + 1 + (ulong)i, source.Data);
        }

        if (!IncreaseUncommittedSize(stamped))
        {
            // Drop the proposal: the uncommitted tail of the log is at its configured byte cap.
            return false;
        }

        _log.Append(stamped);

        // NOTE: the leader's own match index is advanced in StableTo, once these entries are durable, rather than here
        // at append time. This gates the leader's commit contribution on its local disk write completing.
        MaybeCommit();
        return true;
    }

    private bool MaybeCommit()
    {
        ulong oldCommit = _log.Committed;
        ulong mci = _tracker.Committed();
        if (!_log.TryMaybeCommit(mci, Term))
        {
            return false;
        }

        if (_uncommittedSize > 0)
        {
            // Entries that just committed no longer count against the uncommitted-log byte cap.
            IReadOnlyList<Entry> committed = _log.Slice(oldCommit + 1, _log.Committed + 1, ulong.MaxValue);
            ReduceUncommittedSize(committed);
        }

        return true;
    }

    /// <summary>
    /// Tracks the payload bytes of newly proposed entries against the uncommitted-log cap. Returns
    /// <see langword="false"/> (drop the proposal) when admitting these entries would exceed the cap, except that a
    /// proposal is always admitted when no uncommitted entries are currently outstanding.
    /// </summary>
    /// <param name="entries">The entries being appended.</param>
    /// <returns><see langword="true"/> when the entries are admitted.</returns>
    private bool IncreaseUncommittedSize(IReadOnlyList<Entry> entries)
    {
        ulong size = PayloadsSize(entries);
        if (_uncommittedSize > 0 && size > 0 && _uncommittedSize + size > _maxUncommittedSize)
        {
            return false;
        }

        _uncommittedSize += size;
        return true;
    }

    /// <summary>Reduces the uncommitted-log byte tally as entries commit, saturating at zero.</summary>
    /// <param name="entries">The entries leaving the uncommitted tail.</param>
    private void ReduceUncommittedSize(IReadOnlyList<Entry> entries)
    {
        ulong size = PayloadsSize(entries);
        if (size > _uncommittedSize)
        {
            _uncommittedSize = 0;
        }
        else
        {
            _uncommittedSize -= size;
        }
    }

    private void StepLeader(Message message)
    {
        switch (message.Type)
        {
            case MessageType.Beat:
                BcastHeartbeat();
                return;
            case MessageType.Propose:
                HandlePropose(message);
                return;
            case MessageType.AppendResponse:
                HandleAppendResponse(message);
                return;
            case MessageType.HeartbeatResponse:
                HandleHeartbeatResponse(message);
                return;
            case MessageType.TransferLeader:
                HandleTransferLeader(message);
                return;
        }
    }

    private void HandlePropose(Message message)
    {
        if (message.Entries.Count == 0 || LeadTransferee != None)
        {
            return;
        }

        var toAppend = new List<Entry>(message.Entries.Count);
        foreach (Entry entry in message.Entries)
        {
            Entry candidate = entry;
            if (entry.Type is EntryType.ConfChange or EntryType.ConfChangeV2)
            {
                if (PendingConfIndex > _log.Applied)
                {
                    // A configuration change is already in flight; drop this one to preserve safety.
                    candidate = new Entry(EntryType.Normal, 0, 0, ReadOnlyMemory<byte>.Empty);
                }
                else
                {
                    PendingConfIndex = _log.LastIndex() + (ulong)toAppend.Count + 1;
                }
            }

            toAppend.Add(candidate);
        }

        if (AppendEntries(toAppend))
        {
            BcastAppend();
        }
    }

    private void BcastAppend()
    {
        foreach (ulong id in EnumeratePeers())
        {
            if (id == Id)
            {
                continue;
            }

            SendAppend(id);
        }
    }

    private void BcastHeartbeat()
    {
        foreach (ulong id in EnumeratePeers())
        {
            if (id == Id)
            {
                continue;
            }

            SendHeartbeat(id);
        }
    }

    private void SendHeartbeat(ulong to)
    {
        Raft.Progress.Progress? progress = _tracker.GetProgress(to);
        if (progress is null)
        {
            return;
        }

        ulong commit = Math.Min(progress.MatchIndex, _log.Committed);
        Send(new Message
        {
            To = to,
            Type = MessageType.Heartbeat,
            Term = Term,
            Commit = commit,
        });
    }

    private void SendAppend(ulong to)
    {
        Raft.Progress.Progress? progress = _tracker.GetProgress(to);
        if (progress is null || progress.IsPaused())
        {
            return;
        }

        ulong prevIndex = progress.NextIndex - 1;
        ulong firstIndex = _log.FirstIndex();
        if (progress.NextIndex < firstIndex)
        {
            SendSnapshot(to, progress);
            return;
        }

        ulong prevTerm = _log.Term(prevIndex);
        IReadOnlyList<Entry> entries = _log.Entries(progress.NextIndex, _maxSizePerMessage);
        var message = new Message
        {
            To = to,
            Type = MessageType.Append,
            Term = Term,
            Index = prevIndex,
            LogTerm = prevTerm,
            Commit = _log.Committed,
            Entries = entries,
        };

        if (entries.Count > 0)
        {
            switch (progress.State)
            {
                case ProgressState.Replicate:
                    ulong last = entries[entries.Count - 1].Index;
                    progress.NextIndex = last + 1;
                    progress.Inflights.Add(last, PayloadsSize(entries));
                    break;
                case ProgressState.Probe:
                    progress.ProbeSent = true;
                    break;
            }
        }

        Send(message);
    }

    private static ulong PayloadsSize(IReadOnlyList<Entry> entries)
    {
        ulong total = 0;
        for (int i = 0; i < entries.Count; i++)
        {
            total += (ulong)entries[i].Data.Length;
        }

        return total;
    }

    private void SendSnapshot(ulong to, Raft.Progress.Progress progress)
    {
        Snapshot snapshot;
        try
        {
            snapshot = _log.SnapshotForRequest(0);
        }
        catch (RaftStorageException ex) when (ex.Error == RaftStorageError.SnapshotTemporarilyUnavailable)
        {
            return;
        }

        if (snapshot.IsEmpty)
        {
            return;
        }

        progress.BecomeSnapshot(snapshot.Metadata.Index);
        Send(new Message
        {
            To = to,
            Type = MessageType.Snapshot,
            Term = Term,
            Snapshot = snapshot,
        });
    }

    private void HandleAppendResponse(Message message)
    {
        Raft.Progress.Progress? progress = _tracker.GetProgress(message.From);
        if (progress is null)
        {
            return;
        }

        progress.RecentActive = true;

        if (message.Reject)
        {
            if (progress.MaybeDecrTo(message.Index, message.RejectHint))
            {
                if (progress.State == ProgressState.Replicate)
                {
                    progress.BecomeProbe();
                }

                SendAppend(message.From);
            }

            return;
        }

        if (!progress.MaybeUpdate(message.Index))
        {
            if (progress.State == ProgressState.Replicate)
            {
                progress.Inflights.FreeLatestEntries(message.Index);
            }

            return;
        }

        switch (progress.State)
        {
            case ProgressState.Probe:
                progress.BecomeReplicate();
                break;
            case ProgressState.Replicate:
                progress.Inflights.FreeLatestEntries(message.Index);
                break;
            case ProgressState.Snapshot:
                if (progress.MatchIndex >= progress.PendingSnapshot)
                {
                    progress.BecomeProbe();
                    progress.BecomeReplicate();
                }

                break;
        }

        if (MaybeCommit())
        {
            BcastAppend();
        }
        else if (progress.MatchIndex < _log.LastIndex() || progress.State != ProgressState.Replicate)
        {
            SendAppend(message.From);
        }

        if (LeadTransferee == message.From && progress.MatchIndex == _log.LastIndex())
        {
            SendTimeoutNow(message.From);
        }
    }

    private void HandleHeartbeatResponse(Message message)
    {
        Raft.Progress.Progress? progress = _tracker.GetProgress(message.From);
        if (progress is null)
        {
            return;
        }

        progress.RecentActive = true;
        progress.ProbeSent = false;
        if (progress.State == ProgressState.Replicate && progress.Inflights.IsFull)
        {
            progress.Inflights.FreeFirstOne();
        }

        if (progress.MatchIndex < _log.LastIndex())
        {
            SendAppend(message.From);
        }
    }

    private void HandleTransferLeader(Message message)
    {
        ulong transferee = message.From;
        if (transferee == Id)
        {
            return;
        }

        if (!_tracker.IsVoter(transferee))
        {
            return;
        }

        LeadTransferee = transferee;
        Raft.Progress.Progress? progress = _tracker.GetProgress(transferee);
        if (progress is not null && progress.MatchIndex == _log.LastIndex())
        {
            SendTimeoutNow(transferee);
        }
        else
        {
            SendAppend(transferee);
        }
    }

    private void SendTimeoutNow(ulong to)
    {
        Send(new Message { To = to, Type = MessageType.TimeoutNow, Term = Term });
    }
}
