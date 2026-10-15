// Copyright (c) marcschier. Licensed under the MIT License.

using System.Buffers.Binary;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;

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
    public async Task Compact_ShrinksTheLogFile_AndReopensToTheSameState()
    {
        string directory = CreateDirectory();
        string logPath = Path.Combine(directory, "log");
        try
        {
            var hardState = new HardState(7, 2, 150);
            long sizeBefore;
            long sizeAfter;
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 200, payloadLength: 256));
                storage.SetHardState(hardState);
                sizeBefore = new FileInfo(logPath).Length;

                storage.Compact(150);
                sizeAfter = new FileInfo(logPath).Length;

                // The appends after a compaction go to the compacted file.
                storage.Append(CreateEntries(201, 5, payloadLength: 256));
            }

            // 50 of 200 entries remain, so the file holds about a quarter of its former size.
            await Assert.That(sizeAfter).IsLessThan(sizeBefore / 3);

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(reopened.FirstIndex()).IsEqualTo(151UL);
            await Assert.That(reopened.LastIndex()).IsEqualTo(205UL);
            await Assert.That(reopened.Term(150)).IsEqualTo(150UL);
            await Assert.That(reopened.InitialState().HardState).IsEqualTo(hardState);
            await AssertEntriesEqual(
                reopened.Entries(151, 206, ulong.MaxValue),
                CreateEntries(151, 55, payloadLength: 256));
            await AssertStorageError(() => reopened.Entries(150, 151, ulong.MaxValue), RaftStorageError.Compacted);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Compact_PreviousReaderRejectsCompactedLog()
    {
        string directory = CreateDirectory();
        string logPath = Path.Combine(directory, "log");
        try
        {
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 10));
                storage.Compact(3);
            }

            using (var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                await Assert.That(reopened.FirstIndex()).IsEqualTo(4UL);
                await Assert.That(reopened.LastIndex()).IsEqualTo(10UL);
                await Assert.That(reopened.Term(3)).IsEqualTo(3UL);
            }

            Exception? exception = Capture(() => PreviousFileRaftStorageReader.Open(logPath));
            var storageException = exception as RaftStorageException;
            await Assert.That(storageException is not null).IsTrue();
            await Assert.That(storageException!.Error).IsEqualTo(RaftStorageError.Compacted);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Compact_KeepsTheLogFileBoundedOverManyRounds()
    {
        string directory = CreateDirectory();
        string logPath = Path.Combine(directory, "log");
        try
        {
            var sizes = new List<long>();
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                for (int round = 0; round < 10; round++)
                {
                    ulong start = storage.LastIndex() + 1;
                    storage.Append(CreateEntries(start, 100, payloadLength: 64));
                    storage.SetHardState(new HardState(1, 1, storage.LastIndex()));
                    storage.Compact(storage.LastIndex() - 10);
                    sizes.Add(new FileInfo(logPath).Length);
                }
            }

            // Every round retains the same ten entries' worth of log, so the file does not grow.
            await Assert.That(sizes.Max()).IsEqualTo(sizes.Min());

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(reopened.FirstIndex()).IsEqualTo(991UL);
            await Assert.That(reopened.LastIndex()).IsEqualTo(1000UL);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Reopen_DiscardsAnInterruptedCompaction()
    {
        string directory = CreateDirectory();
        try
        {
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 5));
            }

            // A crash while the compacted log was being written leaves a partial file next to the intact log.
            string compactingPath = Path.Combine(directory, "log.compact");
            System.IO.File.WriteAllBytes(compactingPath, new byte[] { 0, 0, 0, 9, 4, 1 });

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            await Assert.That(System.IO.File.Exists(compactingPath)).IsFalse();
            await Assert.That(reopened.FirstIndex()).IsEqualTo(1UL);
            await AssertEntriesEqual(reopened.Entries(1, 6, ulong.MaxValue), CreateEntries(1, 5));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Compact_RewriteFailurePreservesEntriesAndCanRetry()
    {
        string directory = CreateDirectory();
        string compactingPath = Path.Combine(directory, "log.compact");
        try
        {
            using var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            storage.Append(CreateEntries(1, 5));

            Directory.CreateDirectory(compactingPath);
            Exception? failure = Capture(() => storage.Compact(3));

            await Assert.That(failure is not null).IsTrue();
            await Assert.That(storage.FirstIndex()).IsEqualTo(1UL);
            await Assert.That(storage.LastIndex()).IsEqualTo(5UL);
            await AssertEntriesEqual(storage.Entries(1, 6, ulong.MaxValue), CreateEntries(1, 5));

            Directory.Delete(compactingPath);
            storage.Compact(3);

            await Assert.That(storage.FirstIndex()).IsEqualTo(4UL);
            await Assert.That(storage.LastIndex()).IsEqualTo(5UL);
            await AssertEntriesEqual(storage.Entries(4, 6, ulong.MaxValue), CreateEntries(4, 2));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Compact_WithFsync_PersistsReplacementBeforeSubsequentAppend()
    {
        string directory = CreateDirectory();
        try
        {
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = true }))
            {
                storage.Append(CreateEntries(1, 5));
                storage.Compact(3);
                storage.Append(CreateEntries(6, 2));
            }

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = true });
            await Assert.That(reopened.FirstIndex()).IsEqualTo(4UL);
            await Assert.That(reopened.LastIndex()).IsEqualTo(7UL);
            await Assert.That(reopened.Term(3)).IsEqualTo(3UL);
            await AssertEntriesEqual(reopened.Entries(4, 8, ulong.MaxValue), CreateEntries(4, 4));
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Reopen_WithFsync_SyncsDirectoryLeftUnsyncedByAnInterruptedReplacement()
    {
        string directory = CreateDirectory();
        try
        {
            bool failDirectorySync = false;
            using (var storage = new FileRaftStorage(
                new FileRaftStorageOptions(directory) { Fsync = true },
                _ =>
                {
                    if (failDirectorySync)
                    {
                        throw new IOException("The process stopped before the directory sync.");
                    }
                }))
            {
                storage.Append(CreateEntries(1, 5));
                failDirectorySync = true;
                await Assert.That(Capture(() => storage.Compact(3)) is IOException).IsTrue();
            }

            var syncedDirectories = new List<string>();
            using var reopened = new FileRaftStorage(
                new FileRaftStorageOptions(directory) { Fsync = true },
                syncedDirectories.Add);

            await Assert.That(syncedDirectories.Count).IsEqualTo(1);
            await Assert.That(Path.GetFullPath(syncedDirectories[0])).IsEqualTo(Path.GetFullPath(directory));
            await Assert.That(reopened.FirstIndex()).IsEqualTo(4UL);

            reopened.Append(CreateEntries(6, 1));
            await Assert.That(reopened.LastIndex()).IsEqualTo(6UL);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Reopen_WithFsync_RefusesWritesWhenTheDirectorySyncFails()
    {
        string directory = CreateDirectory();
        try
        {
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 5));
                storage.Compact(3);
            }

            Exception? failure = Capture(() => new FileRaftStorage(
                new FileRaftStorageOptions(directory) { Fsync = true },
                _ => throw new IOException("The directory sync failed.")).Dispose());
            await Assert.That(failure is IOException).IsTrue();

            // The failed open released the log, so a later open can sync the directory and take writes.
            var syncedDirectories = new List<string>();
            using var reopened = new FileRaftStorage(
                new FileRaftStorageOptions(directory) { Fsync = true },
                syncedDirectories.Add);
            reopened.Append(CreateEntries(6, 1));

            await Assert.That(syncedDirectories.Count).IsEqualTo(1);
            await Assert.That(reopened.FirstIndex()).IsEqualTo(4UL);
            await Assert.That(reopened.LastIndex()).IsEqualTo(6UL);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    public async Task Reopen_WithoutFsync_DoesNotSyncDirectory()
    {
        string directory = CreateDirectory();
        try
        {
            var syncedDirectories = new List<string>();
            using (var storage = new FileRaftStorage(
                new FileRaftStorageOptions(directory) { Fsync = false },
                syncedDirectories.Add))
            {
                storage.Append(CreateEntries(1, 5));
                storage.Compact(3);
            }

            using var reopened = new FileRaftStorage(
                new FileRaftStorageOptions(directory) { Fsync = false },
                syncedDirectories.Add);

            await Assert.That(syncedDirectories.Count).IsEqualTo(0);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    [Arguments(0b110_000_000)]
    [Arguments(0b110_100_000)]
    public async Task Compact_OnUnix_KeepsTheLogPermissions(int mode)
    {
        if (OperatingSystem.IsWindows())
        {
            // Windows log files carry ACLs that File.Replace keeps; Unix permission bits do not apply.
            return;
        }

        string directory = CreateDirectory();
        try
        {
            var permissions = (UnixFileMode)mode;
            string logPath = Path.Combine(directory, "log");
            string compactingPath = Path.Combine(directory, "log.compact");
            using var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            storage.Append(CreateEntries(1, 5));
            System.IO.File.SetUnixFileMode(logPath, permissions);

            // A leftover compacted log with broader permissions must not be reused for the retained payloads.
            System.IO.File.WriteAllBytes(compactingPath, Array.Empty<byte>());
            System.IO.File.SetUnixFileMode(
                compactingPath,
                permissions | UnixFileMode.GroupRead | UnixFileMode.OtherRead);

            storage.Compact(3);

            await Assert.That(System.IO.File.GetUnixFileMode(logPath)).IsEqualTo(permissions);
            await Assert.That(System.IO.File.Exists(compactingPath)).IsFalse();
            await Assert.That(storage.FirstIndex()).IsEqualTo(4UL);
        }
        finally
        {
            DeleteDirectory(directory);
        }
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task Compact_OnWindows_KeepsTheLogAccessRules(bool restrictLog)
    {
        if (!OperatingSystem.IsWindows())
        {
            // Unix log files carry permission bits; Windows access rules do not apply.
            return;
        }

        string directory = CreateDirectory();
        try
        {
            string logPath = Path.Combine(directory, "log");
            string compactingPath = Path.Combine(directory, "log.compact");
            using (var storage = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false }))
            {
                storage.Append(CreateEntries(1, 5));
            }

            if (restrictLog)
            {
                var restricted = new FileSecurity();
                restricted.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
                restricted.AddAccessRule(new FileSystemAccessRule(
                    WindowsIdentity.GetCurrent().User!,
                    FileSystemRights.FullControl,
                    AccessControlType.Allow));
                new FileInfo(logPath).SetAccessControl(restricted);
            }

            string expected = AccessRules(logPath);
            await Assert.That(expected.StartsWith("protected=True;", StringComparison.Ordinal)).IsEqualTo(restrictLog);

            // A leftover compacted log with the directory's inherited rules must not be reused for the retained payloads.
            System.IO.File.WriteAllBytes(compactingPath, Array.Empty<byte>());

            using var reopened = new FileRaftStorage(new FileRaftStorageOptions(directory) { Fsync = false });
            reopened.Append(CreateEntries(6, 1));
            using (new FileStream(logPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                // The open handle refuses the replacement, so the staged compacted log stays for inspection.
                await Assert.That(Capture(() => reopened.Compact(3)) is not null).IsTrue();
            }

            await Assert.That(AccessRules(compactingPath)).IsEqualTo(expected);

            reopened.Compact(3);

            await Assert.That(AccessRules(logPath)).IsEqualTo(expected);
            await Assert.That(System.IO.File.Exists(compactingPath)).IsFalse();
            await Assert.That(reopened.FirstIndex()).IsEqualTo(4UL);
            await AssertEntriesEqual(reopened.Entries(4, 7, ulong.MaxValue), CreateEntries(4, 3));
        }
        finally
        {
            DeleteDirectory(directory);
        }

        [SupportedOSPlatform("windows")]
        static string AccessRules(string path)
        {
            FileSecurity security = new FileInfo(path).GetAccessControl();

            // File.Replace stores inherited rules as explicit copies, so the comparison covers the access granted.
            var rules = new SortedSet<string>(StringComparer.Ordinal);
            foreach (FileSystemAccessRule rule in security.GetAccessRules(
                includeExplicit: true,
                includeInherited: true,
                typeof(SecurityIdentifier)))
            {
                rules.Add(string.Join(
                    ",",
                    rule.IdentityReference.Value,
                    rule.AccessControlType,
                    rule.FileSystemRights,
                    rule.InheritanceFlags,
                    rule.PropagationFlags));
            }

            return $"protected={security.AreAccessRulesProtected};{string.Join(";", rules)}";
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

    private static Entry[] CreateEntries(ulong start, int count, int payloadLength = 1)
    {
        var entries = new Entry[count];
        for (int i = 0; i < count; i++)
        {
            ulong index = start + (ulong)i;
            byte[] payload = new byte[payloadLength];
            payload.AsSpan().Fill((byte)index);
            entries[i] = new Entry(EntryType.Normal, index, index, payload);
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

    private sealed class PreviousFileRaftStorageReader
    {
        private const byte EntryRecord = 0;
        private const byte TruncateRecord = 1;
        private const byte CompactRecord = 2;
        private const int EntryHeaderSize = 1 + 1 + 8 + 8 + 4;
        private const int UInt64Size = 8;

        private readonly List<Entry> _entries = new() { default };

        public static PreviousFileRaftStorageReader Open(string path)
        {
            var reader = new PreviousFileRaftStorageReader();
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var lengthBuffer = new byte[sizeof(int)];
            while (ReadExactly(stream, lengthBuffer))
            {
                int length = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);
                if (length <= 0)
                {
                    break;
                }

                var payload = new byte[length];
                if (!ReadExactly(stream, payload))
                {
                    break;
                }

                reader.ReplayRecord(payload);
            }

            return reader;
        }

        private void ReplayRecord(ReadOnlySpan<byte> payload)
        {
            switch (payload[0])
            {
                case EntryRecord:
                    ReplayEntryRecord(payload);
                    break;
                case TruncateRecord:
                    if (payload.Length == 1 + UInt64Size)
                    {
                        TruncateForAppend(ReadUInt64(payload.Slice(1)));
                    }

                    break;
                case CompactRecord:
                    if (payload.Length == 1 + UInt64Size)
                    {
                        CompactCore(ReadUInt64(payload.Slice(1)));
                    }

                    break;
            }
        }

        private void ReplayEntryRecord(ReadOnlySpan<byte> payload)
        {
            if (payload.Length < EntryHeaderSize)
            {
                return;
            }

            int offset = 2;
            ulong term = ReadUInt64(payload.Slice(offset));
            offset += UInt64Size;
            ulong index = ReadUInt64(payload.Slice(offset));
            offset += UInt64Size;
            int dataLength = BinaryPrimitives.ReadInt32BigEndian(payload.Slice(offset));
            offset += sizeof(int);
            if (dataLength < 0 || payload.Length != offset + dataLength)
            {
                return;
            }

            AppendCore(new Entry((EntryType)payload[1], term, index, payload.Slice(offset, dataLength).ToArray()));
        }

        private void AppendCore(Entry entry)
        {
            ulong first = _entries[0].Index + 1;
            if (entry.Index < first)
            {
                return;
            }

            TruncateForAppend(entry.Index);
            _entries.Add(entry);
        }

        private void TruncateForAppend(ulong appendStart)
        {
            if (appendStart <= _entries[0].Index)
            {
                _entries.RemoveRange(1, _entries.Count - 1);
                return;
            }

            int truncateAt = (int)(appendStart - _entries[0].Index);
            if (truncateAt < _entries.Count)
            {
                _entries.RemoveRange(truncateAt, _entries.Count - truncateAt);
            }
        }

        private void CompactCore(ulong compactIndex)
        {
            ulong offset = _entries[0].Index;
            if (compactIndex <= offset)
            {
                throw new RaftStorageException(RaftStorageError.Compacted);
            }

            ulong lastIndex = offset + (ulong)_entries.Count - 1;
            if (compactIndex > lastIndex)
            {
                throw new RaftStorageException(RaftStorageError.Unavailable);
            }

            _entries.RemoveRange(0, (int)(compactIndex - offset));
        }

        private static ulong ReadUInt64(ReadOnlySpan<byte> source) =>
            BinaryPrimitives.ReadUInt64BigEndian(source);

        private static bool ReadExactly(Stream stream, byte[] buffer)
        {
            int read = 0;
            while (read < buffer.Length)
            {
                int count = stream.Read(buffer, read, buffer.Length - read);
                if (count == 0)
                {
                    return false;
                }

                read += count;
            }

            return true;
        }
    }
}
