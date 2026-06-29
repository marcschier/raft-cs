// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Messages;

/// <summary>Identifies a Raft protocol message; local driver signals share the wire enum.</summary>
public enum MessageType : byte
{
    /// <summary>Local signal: begin a (real) election campaign.</summary>
    Hup = 0,

    /// <summary>Local signal: the leader should broadcast a heartbeat.</summary>
    Beat = 1,

    /// <summary>Local signal: append the carried entries as the leader.</summary>
    Propose = 2,

    /// <summary>Leader-to-follower log replication (AppendEntries).</summary>
    Append = 3,

    /// <summary>Follower-to-leader replication acknowledgement.</summary>
    AppendResponse = 4,

    /// <summary>Candidate vote request.</summary>
    RequestVote = 5,

    /// <summary>Vote request response.</summary>
    RequestVoteResponse = 6,

    /// <summary>Pre-candidate pre-vote request.</summary>
    RequestPreVote = 7,

    /// <summary>Pre-vote request response.</summary>
    RequestPreVoteResponse = 8,

    /// <summary>Leader heartbeat.</summary>
    Heartbeat = 9,

    /// <summary>Heartbeat acknowledgement.</summary>
    HeartbeatResponse = 10,

    /// <summary>Leader-to-follower snapshot install.</summary>
    Snapshot = 11,

    /// <summary>Leader signal handing leadership to a target.</summary>
    TransferLeader = 12,

    /// <summary>Leader instruction telling a follower to start an election immediately.</summary>
    TimeoutNow = 13,
}

/// <summary>A Raft protocol message exchanged between nodes (and a few node-local driver signals).</summary>
public sealed class Message
{
    /// <summary>Gets or sets the message type.</summary>
    public MessageType Type { get; set; }

    /// <summary>Gets or sets the sender node id.</summary>
    public ulong From { get; set; }

    /// <summary>Gets or sets the recipient node id.</summary>
    public ulong To { get; set; }

    /// <summary>Gets or sets the sender term (zero for local signals).</summary>
    public ulong Term { get; set; }

    /// <summary>Gets or sets the term of the entry preceding <see cref="Index"/> (log/last-log term).</summary>
    public ulong LogTerm { get; set; }

    /// <summary>Gets or sets the log index (prev-log index, last-log index, or vote/transfer target).</summary>
    public ulong Index { get; set; }

    /// <summary>Gets or sets the leader commit index.</summary>
    public ulong Commit { get; set; }

    /// <summary>Gets or sets a value indicating whether the request was rejected.</summary>
    public bool Reject { get; set; }

    /// <summary>Gets or sets the rejection hint index (follower's last index on append rejection).</summary>
    public ulong RejectHint { get; set; }

    /// <summary>Gets or sets an opaque context value (leadership transfer / read-only correlation).</summary>
    public ulong Context { get; set; }

    /// <summary>Gets or sets the carried log entries.</summary>
    public IReadOnlyList<Entry> Entries { get; set; } = Array.Empty<Entry>();

    /// <summary>Gets or sets the carried snapshot, if any.</summary>
    public Storage.Snapshot? Snapshot { get; set; }
}
