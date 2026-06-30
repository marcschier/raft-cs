// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Tests.Support;

namespace Raft.Tests;

public sealed class UncommittedSizeTests
{
    [Test]
    public async Task Proposals_AdmittedUpToCap_ThenRejected()
    {
        var harness = new RaftHarness(
            new ulong[] { 1, 2, 3 },
            (_, config) => config.MaxUncommittedEntriesSize = 6);
        harness.Campaign(1);
        harness.Isolate(1);

        harness.Propose(1, "aaaa"); // first: uncommitted 0 -> admit -> 4
        await Assert.That(harness.Core(1).UncommittedSize).IsEqualTo((ulong)4);

        harness.Propose(1, "bbbb"); // 4 + 4 = 8 > 6 -> reject
        await Assert.That(harness.Core(1).UncommittedSize).IsEqualTo((ulong)4);
    }

    [Test]
    public async Task FirstProposal_AlwaysAdmitted_EvenWhenLargerThanCap()
    {
        var harness = new RaftHarness(
            new ulong[] { 1, 2, 3 },
            (_, config) => config.MaxUncommittedEntriesSize = 2);
        harness.Campaign(1);
        harness.Isolate(1);

        harness.Propose(1, "aaaaaaaa"); // 8 bytes, but uncommitted tail empty -> admit
        await Assert.That(harness.Core(1).UncommittedSize).IsEqualTo((ulong)8);

        harness.Propose(1, "b"); // 8 + 1 = 9 > 2 -> reject
        await Assert.That(harness.Core(1).UncommittedSize).IsEqualTo((ulong)8);
    }

    [Test]
    public async Task CommitReducesUncommittedSize()
    {
        var harness = new RaftHarness(
            new ulong[] { 1, 2, 3 },
            (_, config) => config.MaxUncommittedEntriesSize = 64);
        harness.Campaign(1);
        harness.Isolate(1);

        harness.Propose(1, "aaaa");
        harness.Propose(1, "bbbb");
        await Assert.That(harness.Core(1).UncommittedSize).IsEqualTo((ulong)8);

        harness.Heal(); // replicate + commit
        for (int i = 0; i < 3; i++)
        {
            harness.Tick(1);
        }

        await Assert.That(harness.Core(1).UncommittedSize).IsEqualTo((ulong)0);
        await Assert.That(string.Join(",", harness.Committed(1))).IsEqualTo("aaaa,bbbb");
    }

    [Test]
    public async Task UnlimitedCap_AdmitsManyProposals()
    {
        var harness = new RaftHarness(new ulong[] { 1, 2, 3 });
        harness.Campaign(1);
        harness.Isolate(1);

        for (int i = 0; i < 50; i++)
        {
            harness.Propose(1, "payload");
        }

        await Assert.That(harness.Core(1).UncommittedSize).IsEqualTo((ulong)(50 * 7));
    }
}
