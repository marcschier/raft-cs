// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft;

/// <summary>Identifies the role a Raft node currently plays in its term.</summary>
public enum RaftRole : byte
{
    /// <summary>A passive replica that redirects proposals and grants votes.</summary>
    Follower = 0,

    /// <summary>A node soliciting pre-votes before incrementing its term.</summary>
    PreCandidate = 1,

    /// <summary>A node campaigning for leadership in an incremented term.</summary>
    Candidate = 2,

    /// <summary>The elected coordinator that accepts proposals and replicates entries.</summary>
    Leader = 3,
}

/// <summary>Identifies the payload class carried by a <see cref="Entry"/>.</summary>
public enum EntryType : byte
{
    /// <summary>An opaque application command.</summary>
    Normal = 0,

    /// <summary>A legacy single-step configuration change.</summary>
    ConfChange = 1,

    /// <summary>A joint-consensus configuration change (one or more transitions).</summary>
    ConfChangeV2 = 2,
}

/// <summary>A single replicated Raft log entry.</summary>
public readonly struct Entry : IEquatable<Entry>
{
    /// <summary>Initializes a new instance of the <see cref="Entry"/> struct.</summary>
    /// <param name="type">The entry payload class.</param>
    /// <param name="term">The leader term in which the entry was created.</param>
    /// <param name="index">The 1-based position of the entry in the log.</param>
    /// <param name="data">The opaque entry payload.</param>
    public Entry(EntryType type, ulong term, ulong index, ReadOnlyMemory<byte> data)
    {
        Type = type;
        Term = term;
        Index = index;
        Data = data;
    }

    /// <summary>Gets the entry payload class.</summary>
    public EntryType Type { get; }

    /// <summary>Gets the leader term in which the entry was created.</summary>
    public ulong Term { get; }

    /// <summary>Gets the 1-based position of the entry in the log.</summary>
    public ulong Index { get; }

    /// <summary>Gets the opaque entry payload.</summary>
    public ReadOnlyMemory<byte> Data { get; }

    /// <summary>Gets a value indicating whether the entry carries an empty payload.</summary>
    public bool IsEmpty => Data.Length == 0;

    /// <summary>Compares two entries for equality.</summary>
    /// <param name="left">The left entry.</param>
    /// <param name="right">The right entry.</param>
    /// <returns><see langword="true"/> when the entries are equal.</returns>
    public static bool operator ==(Entry left, Entry right) => left.Equals(right);

    /// <summary>Compares two entries for inequality.</summary>
    /// <param name="left">The left entry.</param>
    /// <param name="right">The right entry.</param>
    /// <returns><see langword="true"/> when the entries differ.</returns>
    public static bool operator !=(Entry left, Entry right) => !left.Equals(right);

    /// <inheritdoc/>
    public bool Equals(Entry other) =>
        Type == other.Type && Term == other.Term && Index == other.Index && Data.Span.SequenceEqual(other.Data.Span);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Entry other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine((byte)Type, Term, Index, Data.Length);
}

/// <summary>The durable, crash-safe portion of a node's Raft state.</summary>
public readonly struct HardState : IEquatable<HardState>
{
    /// <summary>Initializes a new instance of the <see cref="HardState"/> struct.</summary>
    /// <param name="term">The current term.</param>
    /// <param name="vote">The candidate voted for in the current term, or zero.</param>
    /// <param name="commit">The highest log index known to be committed.</param>
    public HardState(ulong term, ulong vote, ulong commit)
    {
        Term = term;
        Vote = vote;
        Commit = commit;
    }

    /// <summary>Gets the current term.</summary>
    public ulong Term { get; }

    /// <summary>Gets the candidate voted for in the current term, or zero when none.</summary>
    public ulong Vote { get; }

    /// <summary>Gets the highest log index known to be committed.</summary>
    public ulong Commit { get; }

    /// <summary>Compares two hard states for equality.</summary>
    /// <param name="left">The left hard state.</param>
    /// <param name="right">The right hard state.</param>
    /// <returns><see langword="true"/> when the hard states are equal.</returns>
    public static bool operator ==(HardState left, HardState right) => left.Equals(right);

    /// <summary>Compares two hard states for inequality.</summary>
    /// <param name="left">The left hard state.</param>
    /// <param name="right">The right hard state.</param>
    /// <returns><see langword="true"/> when the hard states differ.</returns>
    public static bool operator !=(HardState left, HardState right) => !left.Equals(right);

    /// <inheritdoc/>
    public bool Equals(HardState other) => Term == other.Term && Vote == other.Vote && Commit == other.Commit;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is HardState other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine(Term, Vote, Commit);
}

/// <summary>The volatile, in-memory portion of a node's observable Raft state.</summary>
public readonly struct SoftState : IEquatable<SoftState>
{
    /// <summary>Initializes a new instance of the <see cref="SoftState"/> struct.</summary>
    /// <param name="leaderId">The id of the current leader, or zero when unknown.</param>
    /// <param name="role">The current role.</param>
    public SoftState(ulong leaderId, RaftRole role)
    {
        LeaderId = leaderId;
        Role = role;
    }

    /// <summary>Gets the id of the current leader, or zero when unknown.</summary>
    public ulong LeaderId { get; }

    /// <summary>Gets the current role.</summary>
    public RaftRole Role { get; }

    /// <summary>Compares two soft states for equality.</summary>
    /// <param name="left">The left soft state.</param>
    /// <param name="right">The right soft state.</param>
    /// <returns><see langword="true"/> when the soft states are equal.</returns>
    public static bool operator ==(SoftState left, SoftState right) => left.Equals(right);

    /// <summary>Compares two soft states for inequality.</summary>
    /// <param name="left">The left soft state.</param>
    /// <param name="right">The right soft state.</param>
    /// <returns><see langword="true"/> when the soft states differ.</returns>
    public static bool operator !=(SoftState left, SoftState right) => !left.Equals(right);

    /// <inheritdoc/>
    public bool Equals(SoftState other) => LeaderId == other.LeaderId && Role == other.Role;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is SoftState other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine(LeaderId, (byte)Role);
}
