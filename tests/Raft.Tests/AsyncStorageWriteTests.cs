// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;
using Raft.Messages;
using Raft.Storage;

namespace Raft.Tests;

public sealed class AsyncStorageWriteTests
{
    private static RaftCore NewCore(ulong id, params ulong[] cluster)
    {
        var storage = new MemoryStorage(new ConfState(cluster));
        var config = new RaftConfig
        {
            Id = id,
            ElectionTick = 10,
            HeartbeatTick = 1,
            RandomizedElectionTimeout = 10,
        };
        return new RaftCore(config, storage);
    }

    private static void Persist(MemoryStorage storage, StorageWrite write)
    {
        if (write.Snapshot is not null)
        {
            storage.ApplySnapshot(write.Snapshot);
        }

        if (write.Entries.Count > 0)
        {
            storage.Append(write.Entries);
        }

        if (write.HardState is { } hardState)
        {
            storage.SetHardState(hardState);
        }
    }

    [Test]
    public async Task Leader_DoesNotCommit_UntilStorageWriteAcknowledged()
    {
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        var core = new RaftCore(
            new RaftConfig { Id = 1, ElectionTick = 10, HeartbeatTick = 1, RandomizedElectionTimeout = 10 },
            storage);

        core.Step(new Message { From = 1, Type = MessageType.Hup });

        await Assert.That(core.Role).IsEqualTo(RaftRole.Leader);
        await Assert.That(core.CommitIndex).IsEqualTo((ulong)0);

        StorageWrite? write = core.TakeStorageWrite();
        await Assert.That(write).IsNotNull();
        await Assert.That(write!.Entries.Count).IsEqualTo(1);
        await Assert.That(write.HasPersistentWork).IsTrue();

        // Still not committed: the leader's own match is gated on durability.
        await Assert.That(core.CommitIndex).IsEqualTo((ulong)0);

        Persist(storage, write);
        core.AckStorageWrite(write);

        await Assert.That(core.CommitIndex).IsEqualTo((ulong)1);
    }

    [Test]
    public async Task Follower_AppendAck_IsHeldInStorageWriteResponses()
    {
        var core = NewCore(2, 1, 2);

        core.Step(new Message
        {
            From = 1,
            To = 2,
            Type = MessageType.Append,
            Term = 1,
            Index = 0,
            LogTerm = 0,
            Commit = 0,
            Entries = new[] { new Entry(EntryType.Normal, 1, 1, new byte[] { 42 }) },
        });

        // The acknowledgement must not be sendable before the append is durable.
        IReadOnlyList<Message> sendNow = core.TakeSendNowMessages();
        await Assert.That(sendNow.Count).IsEqualTo(0);

        StorageWrite? write = core.TakeStorageWrite();
        await Assert.That(write).IsNotNull();
        await Assert.That(write!.Entries.Count).IsEqualTo(1);
        await Assert.That(write.Responses.Count).IsEqualTo(1);
        await Assert.That(write.Responses[0].Type).IsEqualTo(MessageType.AppendResponse);
        await Assert.That(write.Responses[0].To).IsEqualTo((ulong)1);
    }

    [Test]
    public async Task TakeStorageWrite_ReturnsNull_WhenNothingPending()
    {
        var core = NewCore(1, 1, 2, 3);
        await Assert.That(core.TakeStorageWrite()).IsNull();
    }

    [Test]
    public async Task CommitAdvance_PersistedAsHardStateOnly_ThenNothingPending()
    {
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        var core = new RaftCore(
            new RaftConfig { Id = 1, ElectionTick = 10, HeartbeatTick = 1, RandomizedElectionTimeout = 10 },
            storage);

        core.Step(new Message { From = 1, Type = MessageType.Hup });
        StorageWrite first = core.TakeStorageWrite()!;
        await Assert.That(first.HardState).IsNotNull(); // term/vote changed on election
        Persist(storage, first);
        core.AckStorageWrite(first); // commits the no-op (single voter), advancing the commit index

        // The advanced commit index must be persisted, but carries no new entries.
        StorageWrite? second = core.TakeStorageWrite();
        await Assert.That(second).IsNotNull();
        await Assert.That(second!.Entries.Count).IsEqualTo(0);
        await Assert.That(second.HardState).IsNotNull();
        Persist(storage, second);
        core.AckStorageWrite(second);

        // Now fully quiescent: nothing left to persist.
        await Assert.That(core.TakeStorageWrite()).IsNull();
    }
}
