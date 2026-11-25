// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Messages;
using Raft.Storage;

namespace Raft.Tests;

public sealed class RaftCoreIngressTests
{
    [Test]
    public async Task VoteRequestsUseBothVoterSetsButNotLearners()
    {
        var configuration = new ConfState(
            voters: new ulong[] { 1, 2 }, learners: new ulong[] { 3 }, votersOutgoing: new ulong[] { 1, 4 });
        var core = new RaftCore(new RaftConfig { Id = 1 }, new MemoryStorage(configuration));

        core.Step(new Message { Type = MessageType.RequestVote, From = 3, To = 1, Term = 2 });
        await Assert.That(core.Term).IsEqualTo(0UL);
        await Assert.That(core.TakeMessages().Count).IsEqualTo(0);

        core.Step(new Message { Type = MessageType.RequestVote, From = 4, To = 1, Term = 2 });
        await Assert.That(core.Vote).IsEqualTo(4UL);
        Message response = core.TakeMessages().Single();
        await Assert.That(response.Term).IsEqualTo(2UL);
        await Assert.That(response.Reject).IsFalse();
    }

    [Test]
    public async Task AppendResponsesCannotAdvanceBeyondTheLeadersLog()
    {
        var configuration = new ConfState(new ulong[] { 1 }, learners: new ulong[] { 2 });
        var core = new RaftCore(new RaftConfig { Id = 1 }, new MemoryStorage(configuration));
        core.Step(new Message { Type = MessageType.Hup });
        await Assert.That(core.Role).IsEqualTo(RaftRole.Leader);

        core.Step(new Message { Type = MessageType.AppendResponse, From = 3, To = 1, Term = 2 });
        await Assert.That(core.Role).IsEqualTo(RaftRole.Leader);

        core.Step(new Message { Type = MessageType.AppendResponse, From = 2, To = 1, Term = 1, Index = 2 });
        await Assert.That(core.Tracker.GetProgress(2)!.MatchIndex).IsEqualTo(0UL);
        await Assert.That(core.Log.LastIndex()).IsEqualTo(1UL);

        core.Step(new Message { Type = MessageType.AppendResponse, From = 2, To = 1, Term = 1, Index = 1 });
        await Assert.That(core.Tracker.GetProgress(2)!.MatchIndex).IsEqualTo(1UL);
    }

    [Test]
    public async Task AppendRejectsAnIncompleteConfigurationEntryBeforePersistence()
    {
        var core = new RaftCore(new RaftConfig { Id = 2 }, new MemoryStorage(new ConfState(new ulong[] { 1, 2 })));
        byte[] membership = ConfChangeV2.Single(ConfChangeType.AddNode, 3).Encode();
        var append = new Message
        {
            Type = MessageType.Append,
            From = 1,
            To = 2,
            Term = 1,
            Commit = 1,
            Entries = new[] { new Entry(EntryType.ConfChangeV2, 1, 1, membership.AsMemory(0, membership.Length - 1)) },
        };
        core.Step(append);
        await Assert.That(core.TakeStorageWrite()).IsNull();

        append.Entries = new[] { new Entry(EntryType.ConfChangeV2, 1, 1, membership) };
        core.Step(append);
        StorageWrite write = core.TakeStorageWrite()!;
        await Assert.That(write.Entries.Single().Index).IsEqualTo(1UL);
        await Assert.That(write.Responses.Single().Term).IsEqualTo(1UL);
    }

    [Test]
    public async Task AppendRequiresContiguousPositions()
    {
        var core = new RaftCore(new RaftConfig { Id = 2 }, new MemoryStorage(new ConfState(new ulong[] { 1, 2 })));
        var append = new Message
        {
            Type = MessageType.Append,
            From = 1,
            To = 2,
            Term = 1,
            Entries = new[] { new Entry(EntryType.Normal, 1, 2, default) },
        };
        core.Step(append);
        await Assert.That(core.TakeStorageWrite()).IsNull();
        await Assert.That(core.Log.LastIndex()).IsEqualTo(0UL);
    }

    [Test]
    public async Task SnapshotUsesDistinctMembershipSizeIncludingJointVoters()
    {
        var membership = new ConfState(voters: new ulong[] { 1, 2 });
        var core = new RaftCore(new RaftConfig { Id = 1, MaxClusterMembers = 2 }, new MemoryStorage(membership));
        var joint = new ConfState(
            voters: new ulong[] { 1 }, votersOutgoing: new ulong[] { 1, 2 }, learnersNext: new ulong[] { 2 });
        var larger = new ConfState(
            voters: joint.Voters, learners: new ulong[] { 3 },
            votersOutgoing: joint.VotersOutgoing, learnersNext: joint.LearnersNext);
        var message = new Message
        {
            Type = MessageType.Snapshot,
            From = 2,
            To = 1,
            Term = 1,
            Snapshot = new Snapshot(new SnapshotMetadata(1, 1, larger), default),
        };
        core.Step(message);
        await Assert.That(core.TakeStorageWrite()).IsNull();

        message.Snapshot = new Snapshot(new SnapshotMetadata(1, 1, joint), default);
        core.Step(message);
        await Assert.That(core.TakeStorageWrite()?.Snapshot?.Metadata.ConfState).IsEqualTo(joint);
    }

    [Test]
    [Arguments(MessageType.Append, ConfChangeType.RemoveNode)]
    [Arguments(MessageType.Heartbeat, ConfChangeType.AddLearnerNode)]
    [Arguments(MessageType.Snapshot, ConfChangeType.RemoveNode)]
    public async Task LeaderTrafficStopsWhenOutgoingVoterLeaves(MessageType type, ConfChangeType transition)
    {
        var initial = new ConfState(new ulong[] { 1, 2, 3 });
        var joint = Changer.Apply(initial, new ConfChangeV2(
            new[] { new ConfChangeSingle(transition, 1) }, useJoint: true));
        var core = new RaftCore(new RaftConfig { Id = 2 }, new MemoryStorage(joint));
        core.Step(new Message { Type = MessageType.Heartbeat, From = 1, To = 2, Term = 1 });
        await Assert.That(core.LeaderId).IsEqualTo(1UL);
        _ = core.TakeStorageWrite();
        _ = core.TakeMessages();
        core.ApplyConfChange(Changer.Apply(joint, ConfChangeV2.LeaveJoint()));

        Snapshot? snapshot = type == MessageType.Snapshot
            ? new Snapshot(new SnapshotMetadata(1, 1, joint), default)
            : null;
        core.Step(new Message { Type = type, From = 1, To = 2, Term = 2, Snapshot = snapshot });
        await Assert.That(core.Term).IsEqualTo(1UL);
        await Assert.That(core.CommitIndex).IsEqualTo(0UL);
        await Assert.That(core.TakeStorageWrite()).IsNull();
        await Assert.That(core.TakeMessages().Count).IsEqualTo(0);

        core.Step(new Message { Type = MessageType.Heartbeat, From = 3, To = 2, Term = 2 });
        await Assert.That(core.LeaderId).IsEqualTo(3UL);
    }

    [Test]
    public async Task SnapshotRequiresTrustedSeedAndPreservesTheSendersVoterRole()
    {
        var historical = new ConfState(new ulong[] { 1 });
        var active = new ConfState(new ulong[] { 1, 2 });
        var snapshot = new Snapshot(new SnapshotMetadata(2, 1, active), default);
        var message = new Message { Type = MessageType.Snapshot, From = 2, To = 3, Term = 2, Snapshot = snapshot };
        var unseeded = new RaftCore(new RaftConfig { Id = 3 }, new MemoryStorage());
        unseeded.Step(message);
        await Assert.That(unseeded.TakeStorageWrite()).IsNull();

        var core = new RaftCore(new RaftConfig { Id = 3 }, new MemoryStorage(active));
        message.Snapshot = new Snapshot(new SnapshotMetadata(1, 1, historical), default);
        core.Step(message);
        await Assert.That(core.TakeStorageWrite()).IsNull();

        message.Snapshot = snapshot;
        core.Step(message);
        await Assert.That(core.TakeStorageWrite()?.Snapshot?.Metadata.Index).IsEqualTo((ulong?)2);
    }

    [Test]
    public async Task SeededLearnerAcceptsHistoricalSnapshotAndItsMembershipSuffix()
    {
        var historical = new ConfState(new ulong[] { 1, 2 });
        var storage = new MemoryStorage(historical);
        var core = new RaftCore(new RaftConfig { Id = 3 }, storage);
        var snapshot = new Snapshot(new SnapshotMetadata(2, 1, historical), new byte[] { 1 });
        core.Step(new Message { Type = MessageType.Snapshot, From = 2, To = 3, Term = 2, Snapshot = snapshot });
        StorageWrite install = core.TakeStorageWrite()!;
        await Assert.That(install.Snapshot?.Metadata.Index).IsEqualTo((ulong?)2);
        storage.Write(install.Entries, install.HardState, install.Snapshot);
        core.AckStorageWrite(install);

        core.Step(new Message
        {
            Type = MessageType.Append,
            From = 2,
            To = 3,
            Term = 2,
            Index = 2,
            LogTerm = 1,
            Commit = 3,
            Entries = new[]
            {
                new Entry(EntryType.ConfChangeV2, 2, 3, ConfChangeV2.Single(ConfChangeType.AddLearnerNode, 3).Encode()),
            },
        });
        StorageWrite suffix = core.TakeStorageWrite()!;
        await Assert.That(suffix.Entries.Single().Index).IsEqualTo(3UL);
        await Assert.That(core.CommitIndex).IsEqualTo(3UL);
        await Assert.That(core.LeaderId).IsEqualTo(2UL);
    }

    [Test]
    public async Task SnapshotCannotReplaceANewerTrustedSeed()
    {
        var historical = new ConfState(new ulong[] { 1, 2, 3 });
        ConfState current = Changer.Apply(historical, ConfChangeV2.Single(ConfChangeType.RemoveNode, 3));
        var storage = new MemoryStorage();
        storage.SetConfState(current, 2);
        var core = new RaftCore(new RaftConfig { Id = 4 }, storage);
        var message = new Message
        {
            Type = MessageType.Snapshot,
            From = 2,
            To = 4,
            Term = 1,
            Snapshot = new Snapshot(new SnapshotMetadata(1, 1, historical), default),
        };
        core.Step(message);
        await Assert.That(core.TakeStorageWrite()).IsNull();
        await Assert.That(core.Tracker.IsVoter(3)).IsFalse();

        message.Snapshot = new Snapshot(new SnapshotMetadata(2, 1, historical), default);
        core.Step(message);
        await Assert.That(core.TakeStorageWrite()).IsNull();

        message.Snapshot = new Snapshot(new SnapshotMetadata(2, 1, current), default);
        core.Step(message);
        await Assert.That(core.TakeStorageWrite()?.Snapshot?.Metadata.Index).IsEqualTo((ulong?)2);
    }

    [Test]
    public async Task LeaderRetriesAfterTakingAnEligibleSnapshot()
    {
        var historical = new ConfState(new ulong[] { 1 });
        ConfChangeV2 addVoter = ConfChangeV2.Single(ConfChangeType.AddNode, 2);
        ConfChangeV2 addLearner = ConfChangeV2.Single(ConfChangeType.AddLearnerNode, 3);
        ConfState current = Changer.Apply(Changer.Apply(historical, addVoter), addLearner);
        var storage = new MemoryStorage(historical);
        storage.Append(new[]
        {
            new Entry(EntryType.Normal, 1, 1, default),
            new Entry(EntryType.ConfChangeV2, 1, 2, addVoter.Encode()),
            new Entry(EntryType.ConfChangeV2, 1, 3, addLearner.Encode()),
        });
        storage.SetConfState(current, 3);
        storage.SetHardState(new HardState(1, 0, 3));
        storage.CreateSnapshot(1, historical, default);
        storage.Compact(1);
        var core = new RaftCore(new RaftConfig { Id = 2 }, storage);
        core.Step(new Message { Type = MessageType.Hup });
        core.Step(new Message { Type = MessageType.RequestVoteResponse, From = 1, To = 2, Term = 2 });
        await Assert.That(core.Role).IsEqualTo(RaftRole.Leader);
        _ = core.TakeMessages();

        core.Step(new Message
        {
            Type = MessageType.AppendResponse,
            From = 3,
            To = 2,
            Term = 2,
            Index = 3,
            Reject = true,
        });
        await Assert.That(core.TakeMessages().Count).IsEqualTo(0);

        storage.CreateSnapshot(3, current, default);
        core.Step(new Message { Type = MessageType.HeartbeatResponse, From = 3, To = 2, Term = 2 });
        Message install = core.TakeMessages().Single();
        await Assert.That(install.Type).IsEqualTo(MessageType.Snapshot);
        await Assert.That(install.Snapshot?.Metadata.Index).IsEqualTo((ulong?)3);
    }
}
