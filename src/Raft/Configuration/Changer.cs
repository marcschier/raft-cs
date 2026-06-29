// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Configuration;

/// <summary>Computes the next <see cref="ConfState"/> produced by applying a <see cref="ConfChangeV2"/>.</summary>
public static class Changer
{
    /// <summary>Applies a membership change to the current configuration.</summary>
    /// <param name="current">The current configuration.</param>
    /// <param name="change">The change to apply.</param>
    /// <returns>The resulting configuration.</returns>
    public static ConfState Apply(ConfState current, ConfChangeV2 change)
    {
        Internal.Check.NotNull(current);
        Internal.Check.NotNull(change);

        if (change.IsLeaveJoint)
        {
            return LeaveJoint(current);
        }

        bool joint = change.UseJoint || current.IsJoint;
        if (joint && !current.IsJoint)
        {
            return EnterJoint(current, change);
        }

        return SimpleOrJointApply(current, change, current.IsJoint);
    }

    private static ConfState EnterJoint(ConfState current, ConfChangeV2 change)
    {
        var voters = new HashSet<ulong>(current.Voters);
        var learners = new HashSet<ulong>(current.Learners);
        var learnersNext = new HashSet<ulong>(current.LearnersNext);
        var outgoing = new List<ulong>(current.Voters);

        foreach (ConfChangeSingle single in change.Changes)
        {
            ApplySingle(single, voters, learners, learnersNext, isJoint: true, outgoing);
        }

        return new ConfState(
            new List<ulong>(voters),
            new List<ulong>(learners),
            outgoing,
            new List<ulong>(learnersNext),
            change.AutoLeave);
    }

    private static ConfState LeaveJoint(ConfState current)
    {
        var learners = new HashSet<ulong>(current.Learners);
        learners.UnionWith(current.LearnersNext);
        return new ConfState(
            new List<ulong>(current.Voters),
            new List<ulong>(learners),
            Array.Empty<ulong>(),
            Array.Empty<ulong>(),
            false);
    }

    private static ConfState SimpleOrJointApply(ConfState current, ConfChangeV2 change, bool isJoint)
    {
        var voters = new HashSet<ulong>(current.Voters);
        var learners = new HashSet<ulong>(current.Learners);
        var learnersNext = new HashSet<ulong>(current.LearnersNext);
        var outgoing = new List<ulong>(current.VotersOutgoing);

        foreach (ConfChangeSingle single in change.Changes)
        {
            ApplySingle(single, voters, learners, learnersNext, isJoint, outgoing);
        }

        return new ConfState(
            new List<ulong>(voters),
            new List<ulong>(learners),
            outgoing,
            new List<ulong>(learnersNext),
            isJoint && current.AutoLeave);
    }

    private static void ApplySingle(
        ConfChangeSingle single,
        HashSet<ulong> voters,
        HashSet<ulong> learners,
        HashSet<ulong> learnersNext,
        bool isJoint,
        IReadOnlyList<ulong> outgoing)
    {
        ulong id = single.NodeId;
        switch (single.Type)
        {
            case ConfChangeType.AddNode:
                voters.Add(id);
                learners.Remove(id);
                learnersNext.Remove(id);
                break;

            case ConfChangeType.AddLearnerNode:
                if (isJoint && outgoing.Contains(id))
                {
                    // The node is still a voter in the outgoing config; stage the demotion.
                    voters.Remove(id);
                    learners.Remove(id);
                    learnersNext.Add(id);
                }
                else
                {
                    voters.Remove(id);
                    learnersNext.Remove(id);
                    learners.Add(id);
                }

                break;

            case ConfChangeType.RemoveNode:
                voters.Remove(id);
                learners.Remove(id);
                learnersNext.Remove(id);
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(single));
        }
    }
}
