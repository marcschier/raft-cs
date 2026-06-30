// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Tests.Support;

namespace Raft.Tests;

public sealed class ReplicationTests
{
    [Test]
    public async Task LeaderReplicatesCommandsToAllFollowers()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);

        harness.Propose(1, "a");
        harness.Propose(1, "b");
        harness.Propose(1, "c");

        foreach (ulong id in new ulong[] { 1, 2, 3 })
        {
            await Assert.That(string.Join(",", harness.Committed(id))).IsEqualTo("a,b,c");
        }
    }

    [Test]
    public async Task CommandsCommitWithOneFollowerPartitioned()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);
        harness.Isolate(3);

        harness.Propose(1, "x");
        harness.Propose(1, "y");

        await Assert.That(string.Join(",", harness.Committed(1))).IsEqualTo("x,y");
        await Assert.That(string.Join(",", harness.Committed(2))).IsEqualTo("x,y");

        harness.Heal();
        for (int i = 0; i < 2; i++)
        {
            harness.Tick(1);
        }

        await Assert.That(string.Join(",", harness.Committed(3))).IsEqualTo("x,y");
    }

    [Test]
    public async Task ProposalsOnNonLeaderWithoutForwardingAreNotCommitted()
    {
        var harness = new RaftHarness(
            new ulong[] { 1, 2, 3 },
            (_, config) => config.DisableProposalForwarding = true);
        harness.Campaign(1);

        harness.Propose(2, "ignored");

        await Assert.That(harness.Committed(1).Count).IsEqualTo(0);
    }

    [Test]
    public async Task RejoiningFollowerCatchesUpViaProbe()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);
        harness.Isolate(3);
        harness.Propose(1, "1");
        harness.Propose(1, "2");
        harness.Propose(1, "3");

        harness.Heal();
        for (int i = 0; i < 3; i++)
        {
            harness.Tick(1);
        }

        await Assert.That(string.Join(",", harness.Committed(3))).IsEqualTo("1,2,3");
    }
}
