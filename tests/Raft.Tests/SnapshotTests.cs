// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Tests.Support;

namespace Raft.Tests;

public sealed class SnapshotTests
{
    [Test]
    public async Task LaggingFollowerCaughtUpViaSnapshot()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);
        harness.Isolate(3);

        harness.Propose(1, "1");
        harness.Propose(1, "2");
        harness.Propose(1, "3");
        harness.Propose(1, "4");
        harness.Propose(1, "5");

        // Compact the leader's log so the lagging follower can no longer be served by AppendEntries.
        harness.Compact(1, 3);

        harness.Heal();
        for (int i = 0; i < 4; i++)
        {
            harness.Tick(1);
        }

        // After the snapshot installs, the follower's commit index catches up to the leader.
        await Assert.That(harness.Core(3).CommitIndex >= harness.Core(1).CommitIndex).IsTrue();

        harness.Propose(1, "post-snapshot");
        await Assert.That(harness.Committed(3)).Contains("post-snapshot");
    }
}

public sealed class LeaderTransferTests
{
    [Test]
    public async Task TransferLeadership_MovesLeadershipToTarget()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);
        harness.Propose(1, "warmup");

        harness.TransferLeadership(1, 2);

        await Assert.That(harness.Leader()).IsEqualTo((ulong?)2);
        await Assert.That(harness.Core(1).Role).IsEqualTo(RaftRole.Follower);
    }
}
