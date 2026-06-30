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
    public async Task Inflights_ByteWindow_FullByBytesBeforeCount()
    {
        var inflights = new Inflights(10, maxBytes: 100);
        inflights.Add(1, 60);
        await Assert.That(inflights.IsFull).IsFalse();

        inflights.Add(2, 60);
        await Assert.That(inflights.Count).IsEqualTo(2);
        await Assert.That(inflights.IsFull).IsTrue();

        inflights.FreeLatestEntries(1);
        await Assert.That(inflights.IsFull).IsFalse();

        inflights.Add(3, 60);
        await Assert.That(inflights.IsFull).IsTrue();
    }

    [Test]
    public async Task Inflights_ByteWindow_Unlimited_NeverFullByBytes()
    {
        var inflights = new Inflights(3);
        inflights.Add(1, ulong.MaxValue / 2);
        inflights.Add(2, ulong.MaxValue / 2);
        await Assert.That(inflights.IsFull).IsFalse();
        inflights.Add(3, 1);
        await Assert.That(inflights.IsFull).IsTrue();
    }

    [Test]
    public async Task Inflights_ByteWindow_FreeResetsBytes()
    {
        var inflights = new Inflights(4, maxBytes: 50);
        inflights.Add(1, 30);
        inflights.Add(2, 20);
        await Assert.That(inflights.IsFull).IsTrue();

        inflights.FreeLatestEntries(2);
        await Assert.That(inflights.Count).IsEqualTo(0);
        await Assert.That(inflights.IsFull).IsFalse();

        inflights.Add(3, 40);
        await Assert.That(inflights.IsFull).IsFalse();
    }

    [Test]
    public async Task Progress_IsPaused_WhenByteWindowFull()
    {
        var progress = new ProgressPeer(1, 16, maxInflightBytes: 100);
        progress.MaybeUpdate(0);
        progress.BecomeReplicate();
        progress.Inflights.Add(1, 100);
        await Assert.That(progress.IsPaused()).IsTrue();
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

    [Test]
    public async Task Progress_MaybeDecrTo_InReplicate_ResetsToMatchPlusOne()
    {
        var progress = new ProgressPeer(1, 8);
        progress.MaybeUpdate(10);
        progress.BecomeReplicate();

        // A stale rejection at or below the match index is ignored.
        await Assert.That(progress.MaybeDecrTo(5, 0)).IsFalse();
        await Assert.That(progress.NextIndex).IsEqualTo((ulong)11);

        // A rejection above the match index snaps NextIndex back to MatchIndex + 1.
        progress.NextIndex = 20;
        await Assert.That(progress.MaybeDecrTo(15, 0)).IsTrue();
        await Assert.That(progress.NextIndex).IsEqualTo((ulong)11);
    }

    [Test]
    public async Task Progress_MaybeDecrTo_InProbe_RejectsStaleAndClamps()
    {
        // NextIndex - 1 does not match the rejected index: no regression.
        var stale = new ProgressPeer(10, 8);
        await Assert.That(stale.MaybeDecrTo(3, 0)).IsFalse();
        await Assert.That(stale.NextIndex).IsEqualTo((ulong)10);

        // NextIndex == 0: nothing to decrement.
        var atZero = new ProgressPeer(0, 8);
        await Assert.That(atZero.MaybeDecrTo(5, 0)).IsFalse();

        // The match hint pulls NextIndex below 1, so it is clamped back up to 1.
        var clamp = new ProgressPeer(1, 8);
        await Assert.That(clamp.MaybeDecrTo(0, 5)).IsTrue();
        await Assert.That(clamp.NextIndex).IsEqualTo((ulong)1);
    }

    [Test]
    public async Task Progress_BecomeProbe_FromSnapshot_SetsNextAbovePending()
    {
        var progress = new ProgressPeer(1, 8);
        progress.MaybeUpdate(4);
        progress.BecomeSnapshot(9);
        progress.BecomeProbe();

        await Assert.That(progress.State).IsEqualTo(ProgressState.Probe);
        await Assert.That(progress.NextIndex).IsEqualTo((ulong)10);
        await Assert.That(progress.PendingSnapshot).IsEqualTo((ulong)0);
    }

    [Test]
    public async Task Inflights_NonPositiveCapacity_Throws()
    {
        await Assert.That(() => new Inflights(0)).Throws<ArgumentOutOfRangeException>();
    }
}
