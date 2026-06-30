// Copyright (c) marcschier. Licensed under the MIT License.

using BenchmarkDotNet.Attributes;
using Raft.Configuration;
using Raft.Messages;
using Raft.Progress;
using Raft.Storage;

namespace Raft.Benchmarks;

[MemoryDiagnoser]
public class MessageCodecBenchmarks
{
    private readonly byte[] _buffer = new byte[64 * 1024];
    private Message _message = new();
    private byte[] _encoded = [];

    [Params(1, 8, 64)]
    public int EntryCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var entries = new Entry[EntryCount];
        var payload = new byte[64];
        new Random(1).NextBytes(payload);
        for (int i = 0; i < EntryCount; i++)
        {
            entries[i] = new Entry(EntryType.Normal, 3, (ulong)(i + 1), payload);
        }

        _message = new Message
        {
            Type = MessageType.Append,
            From = 1,
            To = 2,
            Term = 3,
            LogTerm = 3,
            Index = 0,
            Commit = 0,
            Entries = entries,
        };
        _encoded = new byte[MessageCodec.EncodedLength(_message)];
        _ = MessageCodec.TryWrite(_message, _encoded);
    }

    [Benchmark]
    public int Encode()
    {
        _ = MessageCodec.TryWrite(_message, _buffer);
        return MessageCodec.EncodedLength(_message);
    }

    [Benchmark]
    public int Parse()
    {
        _ = MessageCodec.TryParse(_encoded, out Message? decoded);
        return decoded!.Entries.Count;
    }
}

[MemoryDiagnoser]
public class ReplicationBenchmarks
{
    [Params(1000)]
    public int Commands { get; set; }

    [Benchmark]
    public ulong SingleNodeProposeAndCommit()
    {
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        var core = new RaftCore(new RaftConfig { Id = 1, RandomizedElectionTimeout = 10 }, storage);
        core.Step(new Message { From = 1, Type = MessageType.Hup });
        Drain(core, storage);

        var payload = new byte[32];
        for (int i = 0; i < Commands; i++)
        {
            core.Step(new Message
            {
                From = 1,
                Type = MessageType.Propose,
                Entries = new[] { new Entry(EntryType.Normal, 0, 0, payload) },
            });
            Drain(core, storage);
        }

        return core.CommitIndex;
    }

    private static void Drain(RaftCore core, MemoryStorage storage)
    {
        IReadOnlyList<Entry> unstable = core.UnstableEntries();
        if (unstable.Count > 0)
        {
            storage.Append(unstable);
            Entry last = unstable[unstable.Count - 1];
            core.StableTo(last.Index, last.Term);
        }

        storage.SetHardState(core.HardState);
        _ = core.TakeMessages();

        IReadOnlyList<Entry> toApply = core.NextEntriesToApply();
        if (toApply.Count > 0)
        {
            core.AppliedTo(toApply[toApply.Count - 1].Index);
        }
    }
}

[MemoryDiagnoser]
public class AsyncStorageWriteBenchmarks
{
    [Params(1000)]
    public int Commands { get; set; }

    [Benchmark]
    public ulong ProposeAndCommitViaStorageWrites()
    {
        var storage = new MemoryStorage(new ConfState(new ulong[] { 1 }));
        var core = new RaftCore(new RaftConfig { Id = 1, RandomizedElectionTimeout = 10 }, storage);
        core.Step(new Message { From = 1, Type = MessageType.Hup });
        DrainViaStorageWrites(core, storage);

        var payload = new byte[32];
        for (int i = 0; i < Commands; i++)
        {
            core.Step(new Message
            {
                From = 1,
                Type = MessageType.Propose,
                Entries = new[] { new Entry(EntryType.Normal, 0, 0, payload) },
            });
            DrainViaStorageWrites(core, storage);
        }

        return core.CommitIndex;
    }

    private static void DrainViaStorageWrites(RaftCore core, MemoryStorage storage)
    {
        _ = core.TakeSendNowMessages();
        StorageWrite? write = core.TakeStorageWrite();
        while (write is not null)
        {
            storage.Write(write.Entries, write.HardState, write.Snapshot);
            core.AckStorageWrite(write);

            IReadOnlyList<Entry> toApply = core.NextEntriesToApply();
            if (toApply.Count > 0)
            {
                core.AppliedTo(toApply[toApply.Count - 1].Index);
            }

            _ = core.TakeSendNowMessages();
            write = core.TakeStorageWrite();
        }
    }
}

[MemoryDiagnoser]
public class FlowControlBenchmarks
{
    [Params(256)]
    public int Window { get; set; }

    [Benchmark]
    public int InflightsAddAndFreeCycle()
    {
        var inflights = new Inflights(Window, (ulong)Window * 64);
        for (ulong i = 1; i <= (ulong)Window; i++)
        {
            inflights.Add(i, 64);
        }

        inflights.FreeLatestEntries((ulong)Window);
        return inflights.Count;
    }
}
