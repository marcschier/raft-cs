// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Messages;
using Raft.Tests.Support;

namespace Raft.Tests;

public sealed class ProposalForwardingTests
{
    [Test]
    public async Task Follower_ForwardsProposalToLeader()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);

        harness.Propose(2, "x=1");

        await Assert.That(string.Join(",", harness.Committed(1))).IsEqualTo("x=1");
        await Assert.That(string.Join(",", harness.Committed(2))).IsEqualTo("x=1");
        await Assert.That(string.Join(",", harness.Committed(3))).IsEqualTo("x=1");
    }

    [Test]
    public async Task LearnerForwardsAnOrdinaryProposal()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);
        harness.ChangeConf(1, ConfChangeV2.Single(ConfChangeType.AddLearnerNode, 3));
        await Assert.That(harness.Core(3).Tracker.IsVoter(3)).IsFalse();

        harness.Propose(3, "learner-write");

        await Assert.That(harness.Committed(1)).Contains("learner-write");
        await Assert.That(harness.Committed(3)).Contains("learner-write");
    }

    [Test]
    [Arguments(2UL)]
    [Arguments(3UL)]
    public async Task ConfigurationProposalsRequireTheLeadersLocalPath(ulong sender)
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);
        harness.ChangeConf(1, ConfChangeV2.Single(ConfChangeType.AddLearnerNode, 3));
        RaftCore leader = harness.Core(1);
        ConfChangeV2 change = ConfChangeV2.Single(ConfChangeType.AddLearnerNode, 4);
        ulong lastIndex = leader.Log.LastIndex();

        var proposal = new Message
        {
            Type = MessageType.Propose,
            From = sender,
            Entries = new[] { new Entry(EntryType.ConfChangeV2, 0, 0, change.Encode()) },
        };
        RaftCore follower = harness.Core(sender);
        follower.Step(proposal);
        await Assert.That(follower.TakeMessages().Count).IsEqualTo(0);

        proposal.To = 1;
        leader.Step(proposal);
        await Assert.That(leader.Log.LastIndex()).IsEqualTo(lastIndex);

        harness.ChangeConf(1, change);
        await Assert.That(leader.Tracker.Learners.Contains(4)).IsTrue();
    }

    [Test]
    public async Task DisableProposalForwarding_DropsFollowerProposal()
    {
        var harness = new RaftHarness(
            new ulong[] { 1, 2, 3 },
            (_, config) => config.DisableProposalForwarding = true);
        harness.Campaign(1);

        harness.Propose(2, "x=1");

        await Assert.That(harness.Committed(1).Count).IsEqualTo(0);
        await Assert.That(harness.Committed(2).Count).IsEqualTo(0);
    }

    [Test]
    public async Task Follower_WithoutKnownLeader_DropsProposal()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });

        harness.Propose(2, "x=1");

        await Assert.That(harness.Committed(1).Count).IsEqualTo(0);
        await Assert.That(harness.Committed(2).Count).IsEqualTo(0);
    }

    [Test]
    public async Task Leader_AcceptsDirectProposal()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);

        harness.Propose(1, "x=1");

        await Assert.That(string.Join(",", harness.Committed(1))).IsEqualTo("x=1");
    }
}
