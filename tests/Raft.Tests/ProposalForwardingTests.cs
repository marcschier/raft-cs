// Copyright (c) marcschier. Licensed under the MIT License.

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
