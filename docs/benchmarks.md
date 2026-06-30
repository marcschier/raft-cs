# Benchmarks

[BenchmarkDotNet](https://benchmarkdotnet.org/) results for the Raft hot paths. The project lives in [`tests/Raft.Benchmarks`](../tests/Raft.Benchmarks) and every benchmark uses `[MemoryDiagnoser]`.

Reproduce with:

```shell
dotnet run --project tests/Raft.Benchmarks -c Release -f net10.0 -- --filter '*'
```

## Environment

```
BenchmarkDotNet v0.15.8, Windows
.NET 10.0 (X64 RyuJIT AVX-512)
Job=ShortRun (quick run; absolute numbers are noisy — the Allocated column is the headline)
```

Numbers below are machine-specific and from a `--job Short` run, so timings carry wide error margins. The point is that message **encoding is zero-allocation** (it writes directly into a caller buffer) and the single-node propose/commit path stays tight.

## Message codec — `MessageCodec`

| Method | EntryCount | Mean      | Allocated |
| ------ | ---------- | --------- | --------- |
| Encode | 1          | 52 ns     | 0 B       |
| Parse  | 1          | 129 ns    | 256 B     |
| Encode | 8          | 178 ns    | 0 B       |
| Parse  | 8          | 358 ns    | 1152 B    |
| Encode | 64         | 1.14 µs   | 0 B       |
| Parse  | 64         | 2.63 µs   | 8320 B    |

`Encode` is allocation-free. `Parse` necessarily materializes a `Message` plus its entry array and payload copies.

## Replication — single-node propose & commit

| Method                     | Commands | Mean     | Allocated |
| -------------------------- | -------- | -------- | --------- |
| SingleNodeProposeAndCommit | 1000     | 760 µs   | 976 KB    |

Drives a single-node `RaftCore` through 1000 propose → append → commit → apply cycles (≈0.76 µs per command).

## Other suites

The project also includes `AsyncStorageWriteBenchmarks` (propose/commit driven through the asynchronous
storage-writes path with `TakeStorageWrite`/`AckStorageWrite`) and `FlowControlBenchmarks` (the byte-windowed
`Inflights` add/free cycle). Run a single suite with `--filter '*AsyncStorageWrite*'`.

CI runs the same suite as a `--job Dry` smoke (build + execute once) so it never gates on timings.
