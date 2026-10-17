// Copyright (c) marcschier. Licensed under the MIT License.

using System.Buffers.Binary;

namespace Raft.Storage.File.Tests;

public sealed class FileRaftStorageTests
{
    private static readonly ulong[] TwoVoters = { 1UL, 2UL };
    private static readonly ulong[] OneVoter = { 1UL };
    private static readonly ulong[] OneLearner = { 3UL };

    [Test]
    public async Task RoundTrip_AppendsAndReadsEntries()
    {
        string directory = CreateDirectory();
        try
        {
            using var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            Entry[] entries = CreateEntries(1, 3);

            storage.Append(entries);

            await Assert.That(storage.FirstIndex()).IsEqualTo(1UL);
            await Assert.That(storage.LastIndex()).IsEqualTo(3UL);
            await Assert.That(storage.Term(2)).IsEqualTo(2UL);
            await AssertEntriesEqual(storage.Entries(1, 4, ulong.MaxValue), entries);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Reopen_ReplaysPersistedState()
    {
        string directory = CreateDirectory();
        try
        {
            var hardState = new HardState(4, 2, 2);
            var confState = new Configuration.ConfState(TwoVoters, OneLearner);
            Entry[] entries = CreateEntries(1, 3);

            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(entries);
                storage.SetHardState(hardState);
                storage.SetConfState(confState, 2);
            }

            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                (HardState loadedHardState, Configuration.ConfState loadedConfState) = reopened.InitialState();
                await Assert.That(loadedHardState).IsEqualTo(hardState);
                await Assert.That(loadedConfState.Equals(confState)).IsTrue();
                await Assert.That(reopened.ConfStateIndex()).IsEqualTo((ulong?)2);
                await Assert.That(reopened.FirstIndex()).IsEqualTo(1UL);
                await Assert.That(reopened.LastIndex()).IsEqualTo(3UL);
                await AssertEntriesEqual(reopened.Entries(1, 4, ulong.MaxValue), entries);
            }
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Compact_AdvancesFirstIndexAndRejectsOldEntries()
    {
        string directory = CreateDirectory();
        try
        {
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 5));

                storage.Compact(3);

                await Assert.That(storage.FirstIndex()).IsEqualTo(4UL);
                await Assert.That(storage.Term(3)).IsEqualTo(3UL);
                await AssertStorageError(() => storage.Entries(2, 3, ulong.MaxValue), RaftStorageError.Compacted);
            }

            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                await Assert.That(reopened.FirstIndex()).IsEqualTo(4UL);
                await Assert.That(reopened.Term(3)).IsEqualTo(3UL);
                await AssertStorageError(() => reopened.Entries(2, 3, ulong.MaxValue), RaftStorageError.Compacted);
            }
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task CreateSnapshot_PastTheLastIndex_IsRefusedWithoutWrappingTheIndex()
    {
        string directory = CreateDirectory();
        try
        {
            var confState = new Configuration.ConfState(TwoVoters);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 1));

                // 2^32 + 1 would wrap to entry 1 in a narrowing index cast.
                await AssertStorageError(
                    () => storage.CreateSnapshot((1UL << 32) + 1, confState, new byte[] { 1 }),
                    RaftStorageError.Unavailable);
                await AssertStorageError(
                    () => storage.CreateSnapshot(2, confState, new byte[] { 1 }),
                    RaftStorageError.Unavailable);
                await Assert.That(storage.LatestSnapshot()).IsNull();
            }

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(reopened.LatestSnapshot()).IsNull();
            await Assert.That(reopened.FirstIndex()).IsEqualTo(1UL);
            await Assert.That(reopened.LastIndex()).IsEqualTo(1UL);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task CreateSnapshot_KeepsTheEntriesAfterIt_AndSurvivesReopen()
    {
        string directory = CreateDirectory();
        try
        {
            var confState = new Configuration.ConfState(TwoVoters);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 10));
                storage.SetHardState(new HardState(10, 1, 10));

                Snapshot created = storage.CreateSnapshot(6, confState, new byte[] { 1, 2, 3 });

                await Assert.That(created.Metadata.Index).IsEqualTo(6UL);
                await Assert.That(created.Metadata.Term).IsEqualTo(6UL);
                await Assert.That(storage.LatestSnapshot()).IsSameReferenceAs(created);
                await Assert.That(storage.Snapshot(0)).IsSameReferenceAs(created);
                await AssertEntriesEqual(storage.Entries(7, 11, ulong.MaxValue), CreateEntries(7, 4));
                await AssertStorageError(
                    () => storage.CreateSnapshot(6, confState, new byte[] { 4 }),
                    RaftStorageError.SnapshotOutOfDate);
            }

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            Snapshot? latest = reopened.LatestSnapshot();
            await Assert.That(latest).IsNotNull();
            await Assert.That(latest!.Metadata.Index).IsEqualTo(6UL);
            await Assert.That(latest.Metadata.Term).IsEqualTo(6UL);
            await Assert.That(latest.Data.ToArray().SequenceEqual(new byte[] { 1, 2, 3 })).IsTrue();
            await Assert.That(reopened.FirstIndex()).IsEqualTo(7UL);
            await Assert.That(reopened.LastIndex()).IsEqualTo(10UL);
            await AssertEntriesEqual(reopened.Entries(7, 11, ulong.MaxValue), CreateEntries(7, 4));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Snapshot_PreservesItsMembershipWhenTheCurrentMembershipChanges()
    {
        string directory = CreateDirectory();
        try
        {
            var snapshotConfState = new Configuration.ConfState(TwoVoters);
            var currentConfState = new Configuration.ConfState(OneVoter, OneLearner);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 4));
                storage.CreateSnapshot(2, snapshotConfState, new byte[] { 2 });
                storage.SetConfState(currentConfState);
            }

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            Snapshot? snapshot = reopened.LatestSnapshot();
            (HardState _, Configuration.ConfState restoredCurrent) = reopened.InitialState();
            await Assert.That(snapshot).IsNotNull();
            await Assert.That(snapshot!.Metadata.ConfState.Equals(snapshotConfState)).IsTrue();
            await Assert.That(restoredCurrent.Equals(currentConfState)).IsTrue();
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Snapshot_FallbackUsesThePersistedCurrentMembership()
    {
        string directory = CreateDirectory();
        try
        {
            var confState = new Configuration.ConfState(TwoVoters, OneLearner);
            using var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            storage.Append(CreateEntries(1, 2));
            storage.SetHardState(new HardState(2, 1, 2));
            storage.SetConfState(confState);

            Snapshot fallback = storage.Snapshot(2);

            await Assert.That(fallback.Data.IsEmpty).IsTrue();
            await Assert.That(fallback.Metadata.ConfState.Equals(confState)).IsTrue();
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Snapshot_AboveTheApplicationSnapshot_IsTemporarilyUnavailable()
    {
        string directory = CreateDirectory();
        try
        {
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 10));
                storage.SetHardState(new HardState(10, 1, 10));
                storage.CreateSnapshot(6, new Configuration.ConfState(TwoVoters), new byte[] { 6 });
            }

            // A position-only snapshot at the commit index would skip the commands after the application snapshot.
            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(reopened.Snapshot(6).Metadata.Index).IsEqualTo(6UL);
            await AssertStorageError(() => reopened.Snapshot(7), RaftStorageError.SnapshotTemporarilyUnavailable);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task ConfStateIndex_FollowsSetConfStateAndSnapshots_AndSurvivesReopen()
    {
        string directory = CreateDirectory();
        try
        {
            var snapshotConfState = new Configuration.ConfState(TwoVoters);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                await Assert.That(storage.ConfStateIndex()).IsEqualTo((ulong?)0);
                storage.Append(CreateEntries(1, 4));
                storage.SetConfState(new Configuration.ConfState(OneVoter), 3);
                await Assert.That(storage.ConfStateIndex()).IsEqualTo((ulong?)3);
            }

            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                await Assert.That(reopened.ConfStateIndex()).IsEqualTo((ulong?)3);
                reopened.ApplySnapshot(new Snapshot(new SnapshotMetadata(6, 6, snapshotConfState), new byte[] { 6 }));
                await Assert.That(reopened.ConfStateIndex()).IsEqualTo((ulong?)6);
            }

            using var afterSnapshot = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(afterSnapshot.ConfStateIndex()).IsEqualTo((ulong?)6);
            await Assert.That(afterSnapshot.InitialState().ConfState.Equals(snapshotConfState)).IsTrue();
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task ConfStateRecordWithoutIndex_ReopensWithAnUnknownIndex()
    {
        string directory = CreateDirectory();
        try
        {
            // A configuration record as written before the index was recorded: voters { 1 }, no learners.
            byte[] payload = new byte[1 + 1 + 4 + 8 + 4 + 4 + 4];
            payload[0] = 7;
            BinaryPrimitives.WriteInt32BigEndian(payload.AsSpan(2), 1);
            BinaryPrimitives.WriteUInt64BigEndian(payload.AsSpan(6), 1);
            byte[] record = new byte[4 + payload.Length];
            BinaryPrimitives.WriteInt32BigEndian(record, payload.Length);
            payload.CopyTo(record, 4);
            System.IO.File.WriteAllBytes(Path.Combine(directory, "log"), record);

            using var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(storage.InitialState().ConfState.Voters.SequenceEqual(OneVoter)).IsTrue();
            await Assert.That(storage.ConfStateIndex()).IsNull();
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task TornConfStateRecord_KeepsThePreviousConfigurationAndIndex()
    {
        string directory = CreateDirectory();
        try
        {
            var bootstrap = new Configuration.ConfState(TwoVoters);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.SetConfState(bootstrap, 0);
                storage.SetConfState(new Configuration.ConfState(OneVoter), 5);
            }

            // The process stopped while the configuration after the index record was being written.
            string logPath = Path.Combine(directory, "log");
            using (var log = new FileStream(logPath, FileMode.Open, FileAccess.Write))
            {
                log.SetLength(log.Length - 1);
            }

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(reopened.InitialState().ConfState.Equals(bootstrap)).IsTrue();
            await Assert.That(reopened.ConfStateIndex()).IsEqualTo((ulong?)0);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task SetConfState_WithoutIndex_LeavesTheIndexUnknown_AcrossReopen()
    {
        string directory = CreateDirectory();
        try
        {
            var one = new Configuration.ConfState(OneVoter);
            var two = new Configuration.ConfState(TwoVoters);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.SetConfState(two, 3);
                storage.SetConfState(one);
                await Assert.That(storage.ConfStateIndex()).IsNull();
            }

            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                await Assert.That(reopened.InitialState().ConfState.Equals(one)).IsTrue();
                await Assert.That(reopened.ConfStateIndex()).IsNull();
                reopened.SetConfState(two, 5);
            }

            using (var indexed = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                await Assert.That(indexed.InitialState().ConfState.Equals(two)).IsTrue();
                await Assert.That(indexed.ConfStateIndex()).IsEqualTo((ulong?)5);
                indexed.SetConfState(one);
            }

            using var unknown = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(unknown.InitialState().ConfState.Equals(one)).IsTrue();
            await Assert.That(unknown.ConfStateIndex()).IsNull();
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task TornConfStateRecord_ThenSetConfStateWithoutIndex_ReopensWithAnUnknownIndex()
    {
        string directory = CreateDirectory();
        try
        {
            var bootstrap = new Configuration.ConfState(TwoVoters);
            var one = new Configuration.ConfState(OneVoter);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.SetConfState(bootstrap, 0);
                storage.SetConfState(one, 5);
            }

            // The process stopped while the configuration after the index record was being written.
            string logPath = Path.Combine(directory, "log");
            using (var log = new FileStream(logPath, FileMode.Open, FileAccess.Write))
            {
                log.SetLength(log.Length - 1);
            }

            // The orphaned index record must not be paired with the next configuration, stored without an index.
            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                reopened.SetConfState(one);
                await Assert.That(reopened.ConfStateIndex()).IsNull();
            }

            using var recovered = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(recovered.InitialState().ConfState.Equals(one)).IsTrue();
            await Assert.That(recovered.ConfStateIndex()).IsNull();
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task LegacySnapshotFileAtIndexZero_ReopensWithAnUnknownIndex()
    {
        string directory = CreateDirectory();
        try
        {
            // SetConfState of earlier versions stored the membership in a snapshot file at index zero.
            var confState = new Configuration.ConfState(OneVoter, OneLearner);
            LegacyStoreFormat.WriteSnapshotFile(directory, 0, 0, confState);

            using var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(storage.InitialState().ConfState.Equals(confState)).IsTrue();
            await Assert.That(storage.ConfStateIndex()).IsNull();
            await Assert.That(storage.LatestSnapshot()).IsNull();
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task LegacySnapshotFile_KeepsItsState_WithAnUnknownMembershipIndex_AndIsNotSent()
    {
        string directory = CreateDirectory();
        try
        {
            // An earlier version installed a snapshot at index 4 and later stored the membership after index 6 in it.
            var latest = new Configuration.ConfState(OneVoter, OneLearner);
            new LegacyStoreFormat.Log()
                .HardState(new HardState(4, 0, 4))
                .Append(CreateEntries(5, 2))
                .HardState(new HardState(6, 0, 6))
                .WriteTo(directory);
            LegacyStoreFormat.WriteSnapshotFile(directory, 4, 4, latest, new byte[] { 4, 4 });

            using var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(storage.InitialState().ConfState.Equals(latest)).IsTrue();
            await Assert.That(storage.ConfStateIndex()).IsNull();
            await Assert.That(storage.FirstIndex()).IsEqualTo(5UL);
            await AssertEntriesEqual(storage.Entries(5, 7, ulong.MaxValue), CreateEntries(5, 2));

            // The application state up to index 4 is still restored on start.
            Snapshot? latestSnapshot = storage.LatestSnapshot();
            await Assert.That(latestSnapshot).IsNotNull();
            await Assert.That(latestSnapshot!.Metadata.Index).IsEqualTo(4UL);
            await Assert.That(latestSnapshot.Data.ToArray().SequenceEqual(new byte[] { 4, 4 })).IsTrue();

            // Its membership is not the one at index 4, so a follower waits for a newer snapshot.
            await AssertStorageError(() => storage.Snapshot(0), RaftStorageError.SnapshotTemporarilyUnavailable);
            await AssertStorageError(() => storage.Snapshot(6), RaftStorageError.SnapshotTemporarilyUnavailable);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task CreateSnapshot_ReplacesALegacySnapshotFile_AndKeepsTheCurrentMembership()
    {
        string directory = CreateDirectory();
        try
        {
            var latest = new Configuration.ConfState(OneVoter, OneLearner);
            var atSix = new Configuration.ConfState(TwoVoters);
            new LegacyStoreFormat.Log()
                .HardState(new HardState(4, 0, 4))
                .Append(CreateEntries(5, 3))
                .HardState(new HardState(7, 0, 7))
                .WriteTo(directory);
            LegacyStoreFormat.WriteSnapshotFile(directory, 4, 4, latest);

            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                Snapshot created = storage.CreateSnapshot(6, atSix, new byte[] { 6 });
                await Assert.That(storage.LatestSnapshot()).IsSameReferenceAs(created);
                await Assert.That(storage.Snapshot(0)).IsSameReferenceAs(created);
            }

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            Snapshot? snapshot = reopened.LatestSnapshot();
            await Assert.That(snapshot).IsNotNull();
            await Assert.That(snapshot!.Metadata.Index).IsEqualTo(6UL);
            await Assert.That(snapshot.Metadata.ConfState.Equals(atSix)).IsTrue();
            await Assert.That(reopened.InitialState().ConfState.Equals(latest)).IsTrue();
            await Assert.That(reopened.ConfStateIndex()).IsNull();
            await AssertEntriesEqual(reopened.Entries(7, 8, ulong.MaxValue), CreateEntries(7, 1));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task ApplySnapshot_ReplacesALegacySnapshotFile()
    {
        string directory = CreateDirectory();
        try
        {
            var installed = new Configuration.ConfState(TwoVoters, OneLearner);
            new LegacyStoreFormat.Log()
                .HardState(new HardState(4, 0, 4))
                .Append(CreateEntries(5, 2))
                .WriteTo(directory);
            LegacyStoreFormat.WriteSnapshotFile(directory, 4, 4, new Configuration.ConfState(OneVoter));

            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.ApplySnapshot(new Snapshot(new SnapshotMetadata(9, 9, installed), new byte[] { 9 }));
            }

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            Snapshot? snapshot = reopened.LatestSnapshot();
            await Assert.That(snapshot).IsNotNull();
            await Assert.That(snapshot!.Metadata.Index).IsEqualTo(9UL);
            await Assert.That(snapshot.Metadata.ConfState.Equals(installed)).IsTrue();
            await Assert.That(reopened.InitialState().ConfState.Equals(installed)).IsTrue();
            await Assert.That(reopened.ConfStateIndex()).IsEqualTo((ulong?)9);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task SnapshotFile_ReadsUnchangedWithTheEarlierReader()
    {
        string directory = CreateDirectory();
        try
        {
            var confState = new Configuration.ConfState(TwoVoters, OneLearner, OneVoter, Array.Empty<ulong>(), true);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 4));
                storage.CreateSnapshot(3, confState, new byte[] { 1, 2, 3 });
            }

            bool read = LegacyStoreFormat.TryReadSnapshotFile(
                directory,
                out ulong index,
                out ulong term,
                out byte[] data,
                out Configuration.ConfState? legacyConfState);

            await Assert.That(read).IsTrue();
            await Assert.That(index).IsEqualTo(3UL);
            await Assert.That(term).IsEqualTo(3UL);
            await Assert.That(data.SequenceEqual(new byte[] { 1, 2, 3 })).IsTrue();
            await Assert.That(legacyConfState!.Equals(confState)).IsTrue();
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Reopen_RefusesASnapshotWithAnUnknownFormatMarker()
    {
        string directory = CreateDirectory();
        try
        {
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 4));
                storage.CreateSnapshot(3, new Configuration.ConfState(TwoVoters), new byte[] { 3 });
            }

            string snapshotPath = Path.Combine(directory, "snapshot");
            byte[] file = System.IO.File.ReadAllBytes(snapshotPath);
            file[file.Length - 1]++;
            System.IO.File.WriteAllBytes(snapshotPath, file);

            await Assert.That(() => new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
                .Throws<InvalidDataException>();
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task TornLastRecord_OfEveryKind_IsDroppedBeforeTheNextWrite()
    {
        var baselineHardState = new HardState(2, 1, 3);
        var baselineConfState = new Configuration.ConfState(TwoVoters);
        var tornWrites = new (string Kind, Action<FileRaftStorage> Write)[]
        {
            ("entry", storage => storage.Append(CreateEntries(4, 1))),
            ("hard state", storage => storage.SetHardState(new HardState(5, 1, 4))),
            ("compaction", storage => storage.Compact(2)),
            ("indexed configuration", storage => storage.SetConfState(new Configuration.ConfState(OneVoter), 3)),
            ("configuration without index", storage => storage.SetConfState(new Configuration.ConfState(OneVoter))),
        };

        foreach ((string kind, Action<FileRaftStorage> tornWrite) in tornWrites)
        {
            string directory = CreateDirectory();
            try
            {
                string logPath = Path.Combine(directory, "log");
                using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
                {
                    storage.Append(CreateEntries(1, 3));
                    storage.SetHardState(baselineHardState);
                    storage.SetConfState(baselineConfState, 2);
                }

                byte[] intact = System.IO.File.ReadAllBytes(logPath);
                using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
                {
                    tornWrite(storage);
                }

                byte[] written = System.IO.File.ReadAllBytes(logPath);
                for (int cut = intact.Length + 1; cut < written.Length; cut++)
                {
                    try
                    {
                        // The process stopped after the first cut bytes of the write reached the disk.
                        System.IO.File.WriteAllBytes(logPath, written.AsSpan(0, cut).ToArray());
                        long intactLength = IntactLength(written, intact.Length, cut);
                        await ReopenTornLogAndWriteAgain(
                            directory, intactLength, baselineHardState, baselineConfState);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Torn {kind} record cut at byte {cut}.", ex);
                    }
                }
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }
    }

    [Test]
    public async Task ApplySnapshot_StoppedBeforeTheLogIsReplaced_CompletesTheInstallationOnReopen()
    {
        string directory = CreateDirectory();
        try
        {
            var installed = new Configuration.ConfState(TwoVoters);
            string logPath = Path.Combine(directory, "log");
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 5));
                storage.SetHardState(new HardState(9, 2, 5));
                storage.SetConfState(new Configuration.ConfState(OneVoter, OneLearner));
            }

            byte[] supersededLog = System.IO.File.ReadAllBytes(logPath);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.ApplySnapshot(new Snapshot(new SnapshotMetadata(8, 4, installed), new byte[] { 8 }));
            }

            // The snapshot was published, but the process stopped while the log replacing the superseded one, which
            // still holds a configuration stored without an index, was being written beside it.
            System.IO.File.WriteAllBytes(logPath, supersededLog);
            System.IO.File.WriteAllBytes(Path.Combine(directory, "log.new"), new byte[] { 0, 0 });

            for (int reopen = 0; reopen < 2; reopen++)
            {
                using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
                Snapshot? snapshot = reopened.LatestSnapshot();
                await Assert.That(snapshot).IsNotNull();
                await Assert.That(snapshot!.Metadata.Index).IsEqualTo(8UL);
                await Assert.That(snapshot.Data.ToArray().SequenceEqual(new byte[] { 8 })).IsTrue();
                await Assert.That(reopened.InitialState().ConfState.Equals(installed)).IsTrue();
                await Assert.That(reopened.ConfStateIndex()).IsEqualTo((ulong?)8);

                // The term and vote of the superseded log still hold; the commit is the snapshot's.
                await Assert.That(reopened.InitialState().HardState).IsEqualTo(new HardState(9, 2, 8));
                await Assert.That(reopened.FirstIndex()).IsEqualTo(9UL);
                await Assert.That(reopened.LastIndex()).IsEqualTo(8UL);
            }

            await Assert.That(System.IO.File.Exists(Path.Combine(directory, "log.new"))).IsFalse();
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task ApplySnapshot_StoppedBeforeTheSnapshotIsPublished_KeepsTheFormerState()
    {
        string directory = CreateDirectory();
        try
        {
            var former = new Configuration.ConfState(OneVoter, OneLearner);
            string logPath = Path.Combine(directory, "log");
            string snapshotPath = Path.Combine(directory, "snapshot");
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 5));
                storage.SetHardState(new HardState(9, 2, 5));
                storage.SetConfState(former);
            }

            byte[] formerLog = System.IO.File.ReadAllBytes(logPath);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.ApplySnapshot(
                    new Snapshot(new SnapshotMetadata(8, 4, new Configuration.ConfState(TwoVoters)), new byte[] { 8 }));
            }

            // The process stopped while the snapshot was being written beside the former state, which is intact.
            System.IO.File.Move(snapshotPath, Path.Combine(directory, "snapshot.new"));
            System.IO.File.WriteAllBytes(logPath, formerLog);

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(reopened.LatestSnapshot()).IsNull();
            await Assert.That(reopened.InitialState().ConfState.Equals(former)).IsTrue();
            await Assert.That(reopened.ConfStateIndex()).IsNull();
            await Assert.That(reopened.InitialState().HardState).IsEqualTo(new HardState(9, 2, 5));
            await AssertEntriesEqual(reopened.Entries(1, 6, ulong.MaxValue), CreateEntries(1, 5));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task CreateSnapshot_KeepsTheLogItContinues_AcrossReopen()
    {
        string directory = CreateDirectory();
        try
        {
            var current = new Configuration.ConfState(OneVoter, OneLearner);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 6));
                storage.SetHardState(new HardState(6, 1, 6));
                storage.SetConfState(current);
                storage.CreateSnapshot(4, new Configuration.ConfState(TwoVoters), new byte[] { 4 });
            }

            // A created snapshot supersedes no log: the configuration stored before it is still the current one.
            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(reopened.LatestSnapshot()!.Metadata.Index).IsEqualTo(4UL);
            await Assert.That(reopened.InitialState().ConfState.Equals(current)).IsTrue();
            await Assert.That(reopened.ConfStateIndex()).IsNull();
            await Assert.That(reopened.InitialState().HardState).IsEqualTo(new HardState(6, 1, 6));
            await AssertEntriesEqual(reopened.Entries(5, 7, ulong.MaxValue), CreateEntries(5, 2));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Reopen_RefusesALogThatContinuesANewerSnapshot()
    {
        string directory = CreateDirectory();
        try
        {
            string snapshotPath = Path.Combine(directory, "snapshot");
            byte[] olderSnapshot;
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.ApplySnapshot(
                    new Snapshot(new SnapshotMetadata(5, 5, new Configuration.ConfState(TwoVoters)), new byte[] { 5 }));
                olderSnapshot = System.IO.File.ReadAllBytes(snapshotPath);
                storage.ApplySnapshot(
                    new Snapshot(new SnapshotMetadata(7, 7, new Configuration.ConfState(TwoVoters)), new byte[] { 7 }));
            }

            // The log was replaced for the snapshot at index 7, but the snapshot file is the one before it.
            System.IO.File.WriteAllBytes(snapshotPath, olderSnapshot);

            await Assert.That(() => new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
                .Throws<InvalidDataException>();
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task SnapshotFileWithoutALogGeneration_ReadsWithItsMembershipAtItsIndex()
    {
        string directory = CreateDirectory();
        try
        {
            // A snapshot file written with the first marker, which records no log generation.
            var confState = new Configuration.ConfState(TwoVoters);
            new LegacyStoreFormat.Log().Append(CreateEntries(5, 2)).WriteTo(directory);
            LegacyStoreFormat.WriteSnapshotFile(directory, 4, 4, confState, new byte[] { 4 });
            string snapshotPath = Path.Combine(directory, "snapshot");
            byte[] firstMarker = { (byte)'R', (byte)'S', (byte)'P', (byte)'M', 0, 0, 0, 1 };
            System.IO.File.WriteAllBytes(
                snapshotPath, System.IO.File.ReadAllBytes(snapshotPath).Concat(firstMarker).ToArray());

            using var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(storage.LatestSnapshot()!.Metadata.Index).IsEqualTo(4UL);
            await Assert.That(storage.ConfStateIndex()).IsEqualTo((ulong?)4);
            await AssertEntriesEqual(storage.Entries(5, 7, ulong.MaxValue), CreateEntries(5, 2));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task CreateSnapshot_WithFsync_PublishesItDurably()
    {
        string directory = CreateDirectory();
        try
        {
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = true }))
            {
                storage.Append(CreateEntries(1, 2));
                storage.CreateSnapshot(2, new Configuration.ConfState(TwoVoters), new byte[] { 2 });
            }

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = true });
            Snapshot? snapshot = reopened.LatestSnapshot();
            await Assert.That(snapshot).IsNotNull();
            await Assert.That(snapshot!.Metadata.Index).IsEqualTo(2UL);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Reopen_AfterASnapshotAboveAnEarlierCompaction()
    {
        string directory = CreateDirectory();
        try
        {
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 10));
                storage.Compact(3);
                storage.CreateSnapshot(6, new Configuration.ConfState(TwoVoters), new byte[] { 6 });
            }

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(reopened.FirstIndex()).IsEqualTo(7UL);
            await AssertEntriesEqual(reopened.Entries(7, 11, ulong.MaxValue), CreateEntries(7, 4));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Reopen_RefusesADamagedSnapshot()
    {
        string directory = CreateDirectory();
        try
        {
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 4));
                storage.CreateSnapshot(3, new Configuration.ConfState(TwoVoters), new byte[] { 1, 2, 3, 4 });
            }

            string snapshotPath = Path.Combine(directory, "snapshot");
            using (var stream = new FileStream(snapshotPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
            {
                stream.SetLength(stream.Length - 3);
            }

            // The snapshot holds the application state; opening without it would lose that state silently.
            await Assert.That(() => new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
                .Throws<InvalidDataException>();
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task ApplySnapshot_InstallsBoundaryAndMatchesMemoryStorage()
    {
        string directory = CreateDirectory();
        try
        {
            using var file = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            var memory = new MemoryStorage();
            Snapshot snapshot = new(
                new SnapshotMetadata(4, 7, new Configuration.ConfState(TwoVoters)),
                new byte[] { 9, 8, 7 });

            file.Append(CreateEntries(1, 4));
            memory.Append(CreateEntries(1, 4));
            file.ApplySnapshot(snapshot);
            memory.ApplySnapshot(snapshot);

            await AssertStoresEqual(memory, file);
            await Assert.That(file.Term(4)).IsEqualTo(7UL);
            await AssertStorageError(() => file.Entries(4, 5, ulong.MaxValue), RaftStorageError.Compacted);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Differential_RandomOperationsMatchMemoryStorage()
    {
        string directory = CreateDirectory();
        try
        {
            using var file = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            var memory = new MemoryStorage();
            var random = new Random(12345);

            for (int step = 0; step < 200; step++)
            {
                int operation = random.Next(4);
                if (operation == 0)
                {
                    ulong start = Math.Max(1, memory.FirstIndex()) + (ulong)random.Next(0, 4);
                    int count = random.Next(1, 5);
                    Entry[] entries = CreateEntries(start, count);
                    memory.Append(entries);
                    file.Append(entries);
                }
                else if (operation == 1)
                {
                    ulong first = memory.FirstIndex();
                    ulong last = memory.LastIndex();
                    if (first <= last)
                    {
                        ulong compactIndex = first + (ulong)random.Next(0, (int)(last - first + 1));
                        ApplyBoth(() => memory.Compact(compactIndex), () => file.Compact(compactIndex));
                    }
                }
                else if (operation == 2)
                {
                    ulong index = memory.LastIndex() + 1 + (ulong)random.Next(0, 3);
                    var snapshot = new Snapshot(
                        new SnapshotMetadata(index, index + 10, new Configuration.ConfState(new[] { index + 1 })),
                        new[] { (byte)step });
                    ApplyBoth(() => memory.ApplySnapshot(snapshot), () => file.ApplySnapshot(snapshot));
                }
                else
                {
                    var hardState = new HardState((ulong)step, (ulong)random.Next(0, 5), memory.LastIndex());
                    memory.SetHardState(hardState);
                    file.SetHardState(hardState);
                }

                await AssertStoresEqual(memory, file);
            }
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Reopen_IgnoresTornFinalRecord()
    {
        string directory = CreateDirectory();
        try
        {
            Entry[] intact = CreateEntries(1, 2);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(intact);
                storage.Append(CreateEntries(3, 1));
            }

            string logPath = Path.Combine(directory, "log");
            using (var stream = new FileStream(logPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read))
            {
                stream.SetLength(stream.Length - 5);
            }

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(reopened.LastIndex()).IsEqualTo(2UL);
            await AssertEntriesEqual(reopened.Entries(1, 3, ulong.MaxValue), intact);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Write_PersistsEntriesAndHardState_AndReplays()
    {
        string directory = CreateDirectory();
        try
        {
            var hardState = new HardState(7, 1, 3);
            Entry[] entries = CreateEntries(1, 3);

            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Write(entries, hardState, null);

                await Assert.That(storage.LastIndex()).IsEqualTo(3UL);
                (HardState hs, _) = storage.InitialState();
                await Assert.That(hs).IsEqualTo(hardState);
            }

            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                (HardState hs, _) = reopened.InitialState();
                await Assert.That(hs).IsEqualTo(hardState);
                await Assert.That(reopened.LastIndex()).IsEqualTo(3UL);
                await AssertEntriesEqual(reopened.Entries(1, 4, ulong.MaxValue), entries);
            }
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Write_WithSnapshotEntriesAndHardState_MatchesMemoryStorage()
    {
        string directory = CreateDirectory();
        try
        {
            using var file = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            var memory = new MemoryStorage();
            var snapshot = new Snapshot(
                new SnapshotMetadata(2, 5, new Configuration.ConfState(TwoVoters)),
                new byte[] { 1, 2 });
            Entry[] tail = CreateEntries(3, 2);
            var hardState = new HardState(5, 2, 3);

            file.Write(tail, hardState, snapshot);
            memory.Write(tail, hardState, snapshot);

            await AssertStoresEqual(memory, file);
            await Assert.That(file.LastIndex()).IsEqualTo(4UL);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Write_HardStateOnly_SurvivesReopen()
    {
        string directory = CreateDirectory();
        try
        {
            var hardState = new HardState(9, 3, 0);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Write(Array.Empty<Entry>(), hardState, null);
            }

            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                (HardState hs, _) = reopened.InitialState();
                await Assert.That(hs).IsEqualTo(hardState);
            }
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task ConfigurationRecords_FollowOneRollbackFence_AcrossReopenAndSnapshotInstallation()
    {
        string directory = CreateDirectory();
        string logPath = Path.Combine(directory, "log");
        try
        {
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 3));
                await Assert.That(string.Join(",", LogRecordKinds(logPath))).IsEqualTo("1,0,0,0");
                storage.SetConfState(new Configuration.ConfState(TwoVoters), 3);
            }

            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                reopened.SetConfState(new Configuration.ConfState(OneVoter));
            }

            // Earlier readers skip the configuration records but fail on the fence before them: a compaction past the
            // end of every log.
            await Assert.That(string.Join(",", LogRecordKinds(logPath))).IsEqualTo("1,0,0,0,fence,5,7,7");

            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                await Assert.That(reopened.InitialState().ConfState.Equals(new Configuration.ConfState(OneVoter)))
                    .IsTrue();
                await Assert.That(reopened.ConfStateIndex()).IsNull();
                await AssertEntriesEqual(reopened.Entries(1, 4, ulong.MaxValue), CreateEntries(1, 3));
                reopened.ApplySnapshot(
                    new Snapshot(new SnapshotMetadata(5, 5, new Configuration.ConfState(TwoVoters)), new byte[] { 5 }));
                reopened.SetConfState(new Configuration.ConfState(OneVoter), 6);
            }

            // The installed log starts with its generation, then the fence and the membership at the snapshot.
            await Assert.That(string.Join(",", LogRecordKinds(logPath))).IsEqualTo("6,fence,5,7,3,5,7");
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task ApplySnapshot_OverALegacyLog_FencesItBeforePublishingTheSnapshot()
    {
        string directory = CreateDirectory();
        string logPath = Path.Combine(directory, "log");
        string newLogPath = Path.Combine(directory, "log.new");
        try
        {
            new LegacyStoreFormat.Log()
                .Append(CreateEntries(1, 5))
                .HardState(new HardState(5, 1, 5))
                .WriteTo(directory);
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                // The process stops after the snapshot is published, before the log that replaces the superseded one
                // is written: here the replacement cannot be created.
                Directory.CreateDirectory(newLogPath);
                var installed = new Snapshot(
                    new SnapshotMetadata(8, 8, new Configuration.ConfState(TwoVoters)), new byte[] { 8 });
                await Assert.That(Capture(() => storage.ApplySnapshot(installed))).IsNotNull();
            }

            Directory.Delete(newLogPath);

            // An earlier reader, which ignores the generation the published snapshot names, fails on the fence instead
            // of replaying the superseded entries after the snapshot.
            await Assert.That(string.Join(",", LogRecordKinds(logPath))).IsEqualTo("1,0,0,0,0,0,3,fence");

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(reopened.LatestSnapshot()!.Metadata.Index).IsEqualTo(8UL);
            await Assert.That(reopened.FirstIndex()).IsEqualTo(9UL);
            await Assert.That(reopened.InitialState().HardState).IsEqualTo(new HardState(8, 1, 8));
            await Assert.That(string.Join(",", LogRecordKinds(logPath))).IsEqualTo("6,fence,5,7,3");
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Reopen_RefusesALogRewrittenByACompactionItDoesNotRead()
    {
        string directory = CreateDirectory();
        try
        {
            // A compaction-base record (kind 4: index and term) renumbers the entries after it.
            byte[] record = new byte[4 + 1 + 8 + 8];
            BinaryPrimitives.WriteInt32BigEndian(record, 1 + 8 + 8);
            record[4] = 4;
            BinaryPrimitives.WriteUInt64BigEndian(record.AsSpan(5), 3);
            BinaryPrimitives.WriteUInt64BigEndian(record.AsSpan(13), 3);
            System.IO.File.WriteAllBytes(Path.Combine(directory, "log"), record);

            await Assert.That(() => new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
                .Throws<InvalidDataException>();
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    /// <summary>
    /// Returns the kind of each record in the log file, in file order; a compaction record at the index no log reaches
    /// is "fence".
    /// </summary>
    private static List<string> LogRecordKinds(string logPath)
    {
        byte[] log;
        using (var stream = new FileStream(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            log = new byte[stream.Length];
            stream.ReadExactly(log);
        }

        var kinds = new List<string>();
        int offset = 0;
        while (offset + 4 <= log.Length)
        {
            int length = BinaryPrimitives.ReadInt32BigEndian(log.AsSpan(offset));
            ReadOnlySpan<byte> payload = log.AsSpan(offset + 4, length);
            bool fence = payload.Length == 1 + 8 && payload[0] == 2
                && BinaryPrimitives.ReadUInt64BigEndian(payload.Slice(1)) == ulong.MaxValue;
            kinds.Add(fence ? "fence" : payload[0].ToString(System.Globalization.CultureInfo.InvariantCulture));
            offset += 4 + length;
        }

        return kinds;
    }

    /// <summary>
    /// Returns the end of the last complete record of <paramref name="log"/> within its first <paramref name="cut"/>
    /// bytes, from <paramref name="start"/> on; an index record counts only with the configuration record after it.
    /// </summary>
    private static long IntactLength(byte[] log, int start, int cut)
    {
        const byte confStateIndexRecord = 5;
        long intactLength = start;
        int offset = start;
        while (offset + 4 <= cut)
        {
            int length = BinaryPrimitives.ReadInt32BigEndian(log.AsSpan(offset));
            if (offset + 4 + length > cut)
            {
                break;
            }

            if (log[offset + 4] != confStateIndexRecord)
            {
                intactLength = offset + 4 + length;
            }

            offset += 4 + length;
        }

        return intactLength;
    }

    /// <summary>
    /// Reopens a store whose log ends in a torn write, checks that only the intact log remains and the torn write
    /// changed nothing, writes again, and checks that the new writes survive the next reopen.
    /// </summary>
    private static async Task ReopenTornLogAndWriteAgain(
        string directory,
        long intactLength,
        HardState baselineHardState,
        Configuration.ConfState baselineConfState)
    {
        var acknowledgedHardState = new HardState(7, 1, 5);
        var acknowledgedConfState = new Configuration.ConfState(TwoVoters, OneLearner);
        using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
        {
            await Assert.That(new FileInfo(Path.Combine(directory, "log")).Length).IsEqualTo(intactLength);
            await Assert.That(reopened.InitialState().HardState).IsEqualTo(baselineHardState);
            await Assert.That(reopened.InitialState().ConfState.Equals(baselineConfState)).IsTrue();
            await Assert.That(reopened.ConfStateIndex()).IsEqualTo((ulong?)2);
            await Assert.That(reopened.FirstIndex()).IsEqualTo(1UL);
            await Assert.That(reopened.LastIndex()).IsEqualTo(3UL);
            await AssertEntriesEqual(reopened.Entries(1, 4, ulong.MaxValue), CreateEntries(1, 3));

            reopened.Append(CreateEntries(4, 2));
            reopened.SetHardState(acknowledgedHardState);
            reopened.SetConfState(acknowledgedConfState, 5);
        }

        using var recovered = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
        await Assert.That(recovered.InitialState().HardState).IsEqualTo(acknowledgedHardState);
        await Assert.That(recovered.InitialState().ConfState.Equals(acknowledgedConfState)).IsTrue();
        await Assert.That(recovered.ConfStateIndex()).IsEqualTo((ulong?)5);
        await Assert.That(recovered.FirstIndex()).IsEqualTo(1UL);
        await Assert.That(recovered.LastIndex()).IsEqualTo(5UL);
        await AssertEntriesEqual(recovered.Entries(1, 6, ulong.MaxValue), CreateEntries(1, 5));
    }

    private static Entry[] CreateEntries(ulong start, int count)
    {
        var entries = new Entry[count];
        for (int i = 0; i < count; i++)
        {
            ulong index = start + (ulong)i;
            entries[i] = new Entry(EntryType.Normal, index, index, new[] { (byte)index });
        }

        return entries;
    }

    private static void ApplyBoth(Action memoryAction, Action fileAction)
    {
        Exception? memoryException = Capture(memoryAction);
        Exception? fileException = Capture(fileAction);
        if (memoryException is RaftStorageException memoryStorageException
            && fileException is RaftStorageException fileStorageException
            && memoryStorageException.Error == fileStorageException.Error)
        {
            return;
        }

        if (memoryException is not null || fileException is not null)
        {
            throw new InvalidOperationException("Storage operations produced different exceptions.");
        }
    }

    private static Exception? Capture(Action action)
    {
        try
        {
            action();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private static async Task AssertStoresEqual(MemoryStorage expected, FileRaftStorage actual)
    {
        (HardState expectedHardState, Configuration.ConfState expectedConfState) = expected.InitialState();
        (HardState actualHardState, Configuration.ConfState actualConfState) = actual.InitialState();
        await Assert.That(actualHardState).IsEqualTo(expectedHardState);
        await Assert.That(actualConfState.Equals(expectedConfState)).IsTrue();
        await Assert.That(actual.ConfStateIndex()).IsEqualTo(expected.ConfStateIndex());
        await Assert.That(actual.FirstIndex()).IsEqualTo(expected.FirstIndex());
        await Assert.That(actual.LastIndex()).IsEqualTo(expected.LastIndex());

        ulong offset = expected.FirstIndex() - 1;
        await Assert.That(actual.Term(offset)).IsEqualTo(expected.Term(offset));
        if (expected.FirstIndex() <= expected.LastIndex())
        {
            await AssertEntriesEqual(
                actual.Entries(expected.FirstIndex(), expected.LastIndex() + 1, ulong.MaxValue),
                expected.Entries(expected.FirstIndex(), expected.LastIndex() + 1, ulong.MaxValue));
        }
    }

    private static async Task AssertEntriesEqual(IReadOnlyList<Entry> actual, IReadOnlyList<Entry> expected)
    {
        await Assert.That(actual.Count).IsEqualTo(expected.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            await Assert.That(actual[i].Equals(expected[i])).IsTrue();
        }
    }

    private static async Task AssertStorageError(Action action, RaftStorageError error)
    {
        Exception? exception = Capture(action);
        var storageException = exception as RaftStorageException;
        await Assert.That(storageException is not null).IsTrue();
        await Assert.That(storageException!.Error).IsEqualTo(error);
    }

    private static string CreateDirectory()
    {
        string directory = Path.Combine(Path.GetTempPath(), "raft-file-storage-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        return directory;
    }

    private static void DeleteDirectory(string directory)
    {
        if (Directory.Exists(directory))
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
