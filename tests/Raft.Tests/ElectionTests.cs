// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Tests.Support;

namespace Raft.Tests;

public sealed class ElectionTests
{
    [Test]
    public async Task SingleNode_BecomesLeaderOnCampaign()
    {
        var harness = new RaftHarness(new ulong[] { 1 });
        harness.Campaign(1);

        await Assert.That(harness.Leader()).IsEqualTo((ulong?)1);
        await Assert.That(harness.Core(1).Role).IsEqualTo(RaftRole.Leader);
    }

    [Test]
    public async Task ThreeNodes_ElectSingleLeaderOnCampaign()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);

        await Assert.That(harness.Leader()).IsEqualTo((ulong?)1);
        await Assert.That(harness.Core(2).Role).IsEqualTo(RaftRole.Follower);
        await Assert.That(harness.Core(3).Role).IsEqualTo(RaftRole.Follower);
        await Assert.That(harness.Core(2).LeaderId).IsEqualTo((ulong)1);
    }

    [Test]
    public async Task ElectionTimeout_LowestTimeoutNodeWins()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 }, (id, config) =>
            config.RandomizedElectionTimeout = id == 2 ? 6 : 12);
        harness.TickAll(6);

        await Assert.That(harness.Leader()).IsEqualTo((ulong?)2);
    }

    [Test]
    public async Task PreVote_ElectsLeaderWithoutDisruptingTerm()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 }, (_, config) => config.PreVote = true);
        harness.Campaign(1);

        await Assert.That(harness.Leader()).IsEqualTo((ulong?)1);
        await Assert.That(harness.Core(1).Term).IsEqualTo((ulong)1);
    }

    [Test]
    public async Task LeaderFailover_RemainingNodesElectNewLeader()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);
        await Assert.That(harness.Leader()).IsEqualTo((ulong?)1);

        harness.Isolate(1);
        for (int i = 0; i < 13; i++)
        {
            harness.Tick(2);
            harness.Tick(3);
        }

        ulong? newLeader = harness.Leader();
        await Assert.That(newLeader == 2 || newLeader == 3).IsTrue();
        await Assert.That(harness.Core(newLeader!.Value).Term > 1).IsTrue();
    }
}
