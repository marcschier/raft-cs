# 🧭 Raft

[![CI](https://github.com/marcschier/raft-cs/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/marcschier/raft-cs/actions/workflows/ci.yml) [![NuGet](https://img.shields.io/nuget/v/RaftCs?logo=nuget&label=NuGet)](https://www.nuget.org/packages/RaftCs) [![GitHub Packages](https://img.shields.io/badge/GitHub%20Packages-RaftCs-2088FF?logo=github&logoColor=white)](https://github.com/marcschier/raft-cs/pkgs/nuget/RaftCs)

A pure-managed, **NativeAOT-ready** implementation of the **[Raft consensus algorithm](https://raft.github.io/)** for modern .NET. No native dependency.

`Raft` is a customizable **core consensus module** — modeled on [tikv/raft-rs](https://github.com/tikv/raft-rs) — with **bring-your-own log, state machine, and transport**. The deterministic state machine (`RaftCore`) is wrapped by an asynchronous `RaftNode` that owns timers, transport, and storage, so you get a working replica out of the box and can swap any piece.

## ✨ Why Raft

- **Complete consensus**: leader election with **pre-vote**, log replication and commit, **snapshots** with log compaction, **joint-consensus** membership changes with learners, and **leadership transfer**.
- **Low-latency replication**: **optimistic pipelining**, per-peer **flow control** (message- and byte-windowed), **batched** network sends, and **asynchronous storage writes** that let the leader replicate to followers in parallel with persisting to its own disk.
- **Robust under failure**: optional **check-quorum** step-down when a leader loses contact with a majority, an **uncommitted-log byte cap** that protects against unbounded log growth when quorum is lost, and internal **proposal forwarding** from followers to the leader.
- **Observable**: push streams for **leadership/role changes** and **committed membership** (plus poll properties), so applications react to elections and reconcile configuration against committed state without polling.
- **Replaceable everything**: the `IRaftStorage` and `IRaftTransport` abstractions ship with an in-memory + file (WAL) store and an in-memory + NanoMsg (NNG/nanomsg) transport.
- **Fast**: `readonly struct`/`readonly ref struct` building blocks, `Span<byte>`/`BinaryPrimitives`/`stackalloc` codecs, `ArrayPool` framing, an `Inflights` ring buffer, and no LINQ on hot paths.
- **NativeAOT & trimming clean** on .NET 8/9/10 — the library is annotated `IsAotCompatible`, and the test suite itself runs as a NativeAOT binary.
- **Broad reach**: targets `netstandard2.0`, `netstandard2.1`, `net8.0`, `net9.0`, `net10.0` (polyfilled on older runtimes).
- **Verified against raft-rs**: a behavioral-parity harness drives both implementations through identical scenarios and compares the committed logs.

## 📦 Install

```shell
dotnet add package RaftCs
```

Replaceable transport and storage ship as separate opt-in packages:

```shell
dotnet add package RaftCs.Transport            # IRaftTransport + in-memory transport
dotnet add package RaftCs.Transport.NanoMsg    # NNG/nanomsg (BUS) transport
dotnet add package RaftCs.Storage.File         # crash-safe file (WAL) IRaftStorage
```

## 🚀 Quick start

```csharp
using Raft;
using Raft.Configuration;
using Raft.Storage;
using Raft.Transport;

ulong[] cluster = { 1, 2, 3 };
await using var network = new InMemoryNetwork();

var node = new RaftNode(
    new RaftConfig { Id = 1, ElectionTick = 10, HeartbeatTick = 1, PreVote = true },
    new MemoryStorage(new ConfState(cluster)),
    network.CreateNode(1));

await node.StartAsync();
await node.ProposeAsync(System.Text.Encoding.UTF8.GetBytes("set x = 1"));

await foreach (var command in node.Committed.ReadAllAsync())
{
    Console.WriteLine(System.Text.Encoding.UTF8.GetString(command.Span));
}
```

See [`samples/Raft.Samples`](samples/Raft.Samples) for a runnable three-node demo.

## 🛠️ Build & test

```shell
dotnet build Raft.slnx -c Release
dotnet test Raft.slnx -c Release
```

## 📚 Documentation

- [Getting started](docs/getting-started.md)
- [Architecture](docs/architecture.md)
- [Wire format](docs/wire-format.md)
- [API reference](docs/api.md)

## 🧪 Samples

- [In-memory cluster](samples/Raft.Samples) — three nodes elect a leader and replicate commands over the in-memory transport; runs anywhere.

## 🔬 Interop

- [raft-rs behavioral parity](interop/README.md) — a Rust harness wrapping tikv/raft-rs produces golden traces that the .NET implementation must reproduce.

## 📄 License

[MIT](./LICENSE)
