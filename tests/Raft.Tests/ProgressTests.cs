// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Progress;
using ProgressPeer = Raft.Progress.Progress;

namespace Raft.Tests;

public sealed class ProgressTests
{
    [Test]
    public async Task Inflights_FillsAndFrees()
    {
        var inflights = new Inflights(3);
        inflights.Add(1);
        inflights.Add(2);
        inflights.Add(3);

        await Assert.That(inflights.IsFull).IsTrue();
        inflights.FreeLatestEntries(2);
        await Assert.That(inflights.Count).IsEqualTo(1);
        await Assert.That(inflights.IsFull).IsFalse();

        inflights.Add(4);
        inflights.FreeFirstOne();
        await Assert.That(inflights.Count).IsEqualTo(1);

        inflights.Reset();
        await Assert.That(inflights.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Inflights_AddWhenFull_Throws()
    {
        var inflights = new Inflights(1);
        inflights.Add(1);
        await Assert.That(() => inflights.Add(2)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Progress_MaybeUpdate_AdvancesMatchAndNext()
    {
        var progress = new ProgressPeer(1, 8);
        await Assert.That(progress.MaybeUpdate(5)).IsTrue();
        await Assert.That(progress.MatchIndex).IsEqualTo((ulong)5);
        await Assert.That(progress.NextIndex).IsEqualTo((ulong)6);
        await Assert.That(progress.MaybeUpdate(3)).IsFalse();
    }

    [Test]
    public async Task Progress_MaybeDecrTo_RegressesProbe()
    {
        var progress = new ProgressPeer(10, 8);
        await Assert.That(progress.MaybeDecrTo(9, 4)).IsTrue();
        await Assert.That(progress.NextIndex).IsEqualTo((ulong)5);
    }

    [Test]
    public async Task Progress_StateTransitions()
    {
        var progress = new ProgressPeer(1, 8);
        progress.MaybeUpdate(4);
        progress.BecomeReplicate();
        await Assert.That(progress.State).IsEqualTo(ProgressState.Replicate);

        progress.BecomeSnapshot(9);
        await Assert.That(progress.State).IsEqualTo(ProgressState.Snapshot);
        await Assert.That(progress.IsPaused()).IsTrue();

        progress.BecomeProbe();
        await Assert.That(progress.State).IsEqualTo(ProgressState.Probe);
    }
}
