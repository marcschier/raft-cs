// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Storage.File.Tests;

public sealed class FileRaftStorageTests
{
    private static readonly ulong[] TwoVoters = { 1UL, 2UL };
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
                storage.SetConfState(confState);
            }

            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                (HardState loadedHardState, Configuration.ConfState loadedConfState) = reopened.InitialState();
                await Assert.That(loadedHardState).IsEqualTo(hardState);
                await Assert.That(loadedConfState.Equals(confState)).IsTrue();
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
