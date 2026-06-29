// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Tests.Support;

namespace Raft.Tests;

public sealed class MembershipTests
{
    [Test]
    public async Task RemoveNode_ShrinksVoterSet()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);

        harness.ChangeConf(1, ConfChangeV2.Single(ConfChangeType.RemoveNode, 3));

        await Assert.That(Voters(harness, 1)).IsEqualTo("1,2");
        await Assert.That(harness.Core(1).Tracker.Voters.IsJoint).IsFalse();

        harness.Propose(1, "after-remove");
        await Assert.That(harness.Committed(2)).Contains("after-remove");
    }

    [Test]
    public async Task JointChange_EntersAndAutoLeavesJoint()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);

        harness.ChangeConf(1, new ConfChangeV2(
            new[] { new ConfChangeSingle(ConfChangeType.RemoveNode, 3) },
            useJoint: true,
            autoLeave: true));

        await Assert.That(harness.Core(1).Tracker.Voters.IsJoint).IsFalse();
        await Assert.That(Voters(harness, 1)).IsEqualTo("1,2");
    }

    [Test]
    public async Task DemoteVoterToLearner_RemovesFromQuorum()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);

        harness.ChangeConf(1, ConfChangeV2.Single(ConfChangeType.AddLearnerNode, 3));

        await Assert.That(Voters(harness, 1)).IsEqualTo("1,2");
        await Assert.That(harness.Core(1).Tracker.Learners.Contains((ulong)3)).IsTrue();
    }

    private static string Voters(RaftHarness harness, ulong id)
    {
        var voters = new List<ulong>(harness.Core(id).Tracker.Voters.Incoming.Voters);
        voters.Sort();
        return string.Join(",", voters);
    }
}
