// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;

namespace Raft.Tests;

public sealed class QuorumTests
{
    [Test]
    public async Task MajorityConfig_CommittedIndex_IsMedianFromTop()
    {
        var config = new MajorityConfig(new ulong[] { 1, 2, 3 });
        var match = new Dictionary<ulong, ulong> { [1] = 5, [2] = 4, [3] = 2 };

        await Assert.That(config.CommittedIndex(id => match[id])).IsEqualTo((ulong)4);
    }

    [Test]
    public async Task MajorityConfig_EmptySet_IsMaxValue()
    {
        var config = new MajorityConfig();
        await Assert.That(config.CommittedIndex(_ => 0)).IsEqualTo(ulong.MaxValue);
    }

    [Test]
    public async Task MajorityConfig_TallyVotes_DecidesQuorum()
    {
        var config = new MajorityConfig(new ulong[] { 1, 2, 3 });

        await Assert.That(config.TallyVotes(new Dictionary<ulong, bool> { [1] = true, [2] = true }))
            .IsEqualTo(VoteResult.Won);
        await Assert.That(config.TallyVotes(new Dictionary<ulong, bool> { [1] = false, [2] = false }))
            .IsEqualTo(VoteResult.Lost);
        await Assert.That(config.TallyVotes(new Dictionary<ulong, bool> { [1] = true }))
            .IsEqualTo(VoteResult.Pending);
    }

    [Test]
    public async Task JointConfig_CommittedIndex_IsMinOfBoth()
    {
        var joint = new JointConfig(
            new MajorityConfig(new ulong[] { 1, 2, 3 }),
            new MajorityConfig(new ulong[] { 3, 4, 5 }));
        var match = new Dictionary<ulong, ulong>
        {
            [1] = 9,
            [2] = 9,
            [3] = 6,
            [4] = 2,
            [5] = 2,
        };

        await Assert.That(joint.IsJoint).IsTrue();
        await Assert.That(joint.CommittedIndex(id => match[id])).IsEqualTo((ulong)2);
    }

    [Test]
    public async Task JointConfig_TallyVotes_RequiresBoth()
    {
        var joint = new JointConfig(
            new MajorityConfig(new ulong[] { 1, 2, 3 }),
            new MajorityConfig(new ulong[] { 3, 4, 5 }));

        var votes = new Dictionary<ulong, bool>
        {
            [1] = true,
            [2] = true,
            [3] = true,
            [4] = true,
            [5] = true,
        };
        await Assert.That(joint.TallyVotes(votes)).IsEqualTo(VoteResult.Won);

        var partial = new Dictionary<ulong, bool> { [1] = true, [2] = true };
        await Assert.That(joint.TallyVotes(partial)).IsEqualTo(VoteResult.Pending);
    }
}
