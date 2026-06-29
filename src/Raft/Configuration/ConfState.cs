// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Configuration;

/// <summary>The committed cluster membership: voters and learners, including any in-progress joint state.</summary>
public sealed class ConfState : IEquatable<ConfState>
{
    /// <summary>Initializes a new instance of the <see cref="ConfState"/> class.</summary>
    /// <param name="voters">The voters in the current (incoming) configuration.</param>
    /// <param name="learners">The non-voting learners.</param>
    /// <param name="votersOutgoing">The voters in the outgoing configuration during a joint change.</param>
    /// <param name="learnersNext">Learners that become voters when the joint change leaves.</param>
    /// <param name="autoLeave">Whether the leader auto-leaves the joint configuration once committed.</param>
    public ConfState(
        IReadOnlyList<ulong>? voters = null,
        IReadOnlyList<ulong>? learners = null,
        IReadOnlyList<ulong>? votersOutgoing = null,
        IReadOnlyList<ulong>? learnersNext = null,
        bool autoLeave = false)
    {
        Voters = voters ?? Array.Empty<ulong>();
        Learners = learners ?? Array.Empty<ulong>();
        VotersOutgoing = votersOutgoing ?? Array.Empty<ulong>();
        LearnersNext = learnersNext ?? Array.Empty<ulong>();
        AutoLeave = autoLeave;
    }

    /// <summary>Gets the voters in the current (incoming) configuration.</summary>
    public IReadOnlyList<ulong> Voters { get; }

    /// <summary>Gets the non-voting learners.</summary>
    public IReadOnlyList<ulong> Learners { get; }

    /// <summary>Gets the voters in the outgoing configuration during a joint change.</summary>
    public IReadOnlyList<ulong> VotersOutgoing { get; }

    /// <summary>Gets the learners that become voters when the joint change leaves.</summary>
    public IReadOnlyList<ulong> LearnersNext { get; }

    /// <summary>Gets a value indicating whether the leader auto-leaves the joint configuration.</summary>
    public bool AutoLeave { get; }

    /// <summary>Gets a value indicating whether this configuration is in the joint (two-phase) state.</summary>
    public bool IsJoint => VotersOutgoing.Count > 0;

    /// <inheritdoc/>
    public bool Equals(ConfState? other)
    {
        return other is not null
            && SetEquals(Voters, other.Voters)
            && SetEquals(Learners, other.Learners)
            && SetEquals(VotersOutgoing, other.VotersOutgoing)
            && SetEquals(LearnersNext, other.LearnersNext)
            && AutoLeave == other.AutoLeave;
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as ConfState);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        // Order-independent so equal (set-based) configurations hash identically.
        ulong accumulator = 0;
        foreach (ulong id in Voters)
        {
            accumulator ^= id;
        }

        return HashCode.Combine(accumulator, Voters.Count, Learners.Count, VotersOutgoing.Count, AutoLeave);
    }

    private static bool SetEquals(IReadOnlyList<ulong> left, IReadOnlyList<ulong> right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        var set = new HashSet<ulong>(left);
        foreach (ulong id in right)
        {
            if (!set.Contains(id))
            {
                return false;
            }
        }

        return true;
    }
}
