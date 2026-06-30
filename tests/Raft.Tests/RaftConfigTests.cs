// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Storage;

namespace Raft.Tests;

public sealed class RaftConfigTests
{
    private static RaftCore Build(RaftConfig config) =>
        new(config, new MemoryStorage(new ConfState(new ulong[] { 1 })));

    [Test]
    public async Task MaxInflightBytes_Zero_Throws()
    {
        var config = new RaftConfig { Id = 1, MaxInflightBytes = 0 };
        await Assert.That(() => Build(config)).Throws<ArgumentException>();
    }

    [Test]
    public async Task MaxUncommittedEntriesSize_Zero_Throws()
    {
        var config = new RaftConfig { Id = 1, MaxUncommittedEntriesSize = 0 };
        await Assert.That(() => Build(config)).Throws<ArgumentException>();
    }

    [Test]
    public async Task MaxInflightMessages_NonPositive_Throws()
    {
        var config = new RaftConfig { Id = 1, MaxInflightMessages = 0 };
        await Assert.That(() => Build(config)).Throws<ArgumentException>();
    }

    [Test]
    public async Task Defaults_AreRaftRsCompatible()
    {
        var config = new RaftConfig { Id = 1 };
        await Assert.That(config.MaxInflightBytes).IsEqualTo(ulong.MaxValue);
        await Assert.That(config.MaxUncommittedEntriesSize).IsEqualTo(ulong.MaxValue);
        await Assert.That(config.DisableProposalForwarding).IsFalse();

        // Valid config constructs without throwing.
        RaftCore core = Build(config);
        await Assert.That(core.Id).IsEqualTo((ulong)1);
    }

    [Test]
    public async Task HeartbeatTick_NonPositive_Throws()
    {
        // ElectionTick keeps its default (10) so the ElectionTick<=HeartbeatTick guard passes and the
        // HeartbeatTick<=0 guard is the one exercised.
        var config = new RaftConfig { Id = 1, HeartbeatTick = 0 };
        await Assert.That(() => Build(config)).Throws<ArgumentException>();
    }
}
