// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Tests.Support;

namespace Raft.Tests;

public sealed class CheckQuorumTests
{
    [Test]
    public async Task Leader_StepsDownToFollower_WhenQuorumContactLost()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 }, (_, config) => config.CheckQuorum = true);
        harness.Campaign(1);
        await Assert.That(harness.Core(1).Role).IsEqualTo(RaftRole.Leader);

        harness.Isolate(1);
        for (int i = 0; i < 21; i++)
        {
            harness.Tick(1);
        }

        await Assert.That(harness.Core(1).Role).IsEqualTo(RaftRole.Follower);
        await Assert.That(harness.Core(1).LeaderId).IsEqualTo((ulong)0);
    }

    [Test]
    public async Task Leader_RemainsLeader_WhenQuorumStaysActive()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 }, (_, config) => config.CheckQuorum = true);
        harness.Campaign(1);

        for (int i = 0; i < 30; i++)
        {
            harness.TickAll();
        }

        await Assert.That(harness.Core(1).Role).IsEqualTo(RaftRole.Leader);
    }

    [Test]
    public async Task Leader_WithoutCheckQuorum_DoesNotStepDownWhenIsolated()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);

        harness.Isolate(1);
        for (int i = 0; i < 20; i++)
        {
            harness.Tick(1);
        }

        await Assert.That(harness.Core(1).Role).IsEqualTo(RaftRole.Leader);
    }
}
