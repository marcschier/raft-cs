# 🧭 Raft

A pure-managed, **NativeAOT-ready** implementation of the **[Raft consensus algorithm](https://raft.github.io/)** — reliable, leader-based replicated state — for modern .NET. No native dependency.

Raft lets a cluster of nodes agree on an ordered log of commands and survive the failure of a minority of its members: a single elected leader accepts proposals, replicates them to followers, and commits an entry once a quorum has stored it. After applying the same committed log, every replica reaches the same state. This library implements the **core consensus module** (modeled on [tikv/raft-rs](https://github.com/tikv/raft-rs)) and lets you bring your own log, state machine, and transport — while shipping batteries-included in-memory and file storage and in-memory and NanoMsg transports.

## Documentation

- [Getting started](getting-started.md) — install, run a cluster, propose commands.
- [Architecture](architecture.md) — core, node driver, storage, transport, and membership internals.
- [Wire format](wire-format.md) — the message set and its binary encoding.
- [API reference](api.md) — `RaftNode`, `RaftCore`, options, storage, and transport.
- [Benchmarks](benchmarks.md) — codec and replication micro-benchmarks.

## At a glance

| Feature | Status |
| --- | --- |
| Leader election + terms, with **pre-vote** | ✅ |
| Log replication, commit, and flow control | ✅ |
| Persistent storage (`IRaftStorage`: in-memory + file/WAL) | ✅ |
| Snapshots + log compaction | ✅ |
| Single-node + **joint-consensus** membership changes, learners | ✅ |
| Leadership transfer | ✅ |
| Replaceable transport (in-memory + NanoMsg) | ✅ |
| TFMs: `netstandard2.0`, `netstandard2.1`, `net8.0`, `net9.0`, `net10.0` | ✅ |
| NativeAOT (net8+) | ✅ |
| Behavioral parity vs tikv/raft-rs | ✅ |

```shell
dotnet add package Raft
```
