// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;

namespace Raft.Tests;

public sealed class ConfChangeTests
{
    [Test]
    public async Task ConfChangeV2_EncodeDecode_RoundTrips()
    {
        var change = new ConfChangeV2(
            new[]
            {
                new ConfChangeSingle(ConfChangeType.AddNode, 4),
                new ConfChangeSingle(ConfChangeType.RemoveNode, 2),
            },
            useJoint: true,
            autoLeave: true);

        ConfChangeV2 decoded = ConfChangeV2.Decode(change.Encode());

        await Assert.That(decoded.UseJoint).IsTrue();
        await Assert.That(decoded.AutoLeave).IsTrue();
        await Assert.That(decoded.Changes.Count).IsEqualTo(2);
        await Assert.That(decoded.Changes[0].Type).IsEqualTo(ConfChangeType.AddNode);
        await Assert.That(decoded.Changes[0].NodeId).IsEqualTo((ulong)4);
        await Assert.That(decoded.Changes[1].NodeId).IsEqualTo((ulong)2);
    }

    [Test]
    public async Task Changer_SimpleAdd_AddsVoter()
    {
        var current = new ConfState(new ulong[] { 1, 2 });
        ConfState next = Changer.Apply(current, ConfChangeV2.Single(ConfChangeType.AddNode, 3));

        await Assert.That(Sorted(next.Voters)).IsEqualTo("1,2,3");
        await Assert.That(next.IsJoint).IsFalse();
    }

    [Test]
    public async Task Changer_JointThenLeave_FoldsLearnersNext()
    {
        var current = new ConfState(new ulong[] { 1, 2, 3 });
        ConfState joint = Changer.Apply(
            current,
            new ConfChangeV2(new[] { new ConfChangeSingle(ConfChangeType.RemoveNode, 3) }, useJoint: true));

        await Assert.That(joint.IsJoint).IsTrue();
        await Assert.That(Sorted(joint.VotersOutgoing)).IsEqualTo("1,2,3");

        ConfState left = Changer.Apply(joint, ConfChangeV2.LeaveJoint());
        await Assert.That(left.IsJoint).IsFalse();
        await Assert.That(Sorted(left.Voters)).IsEqualTo("1,2");
    }

    private static string Sorted(IReadOnlyList<ulong> ids)
    {
        var copy = new List<ulong>(ids);
        copy.Sort();
        return string.Join(",", copy);
    }
}
