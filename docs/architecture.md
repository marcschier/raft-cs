# Architecture

Raft separates a **deterministic consensus core** from the **I/O that drives it**, mirroring tikv/raft-rs. Everything that touches the network, disk, or clock lives outside the core, which makes the protocol testable, deterministic, and easy to embed.

```
                 ┌──────────────────────────────────────────────┐
   timer ticks   │                  RaftNode                    │
   ───────────▶  │  (async driver: owns transport + storage)    │
   transport in  │   ┌────────────────────────────────────────┐ │
   ───────────▶  │   │               RaftCore                 │ │  committed
   proposals     │   │  Follower / PreCandidate / Candidate / │ │  commands
   ───────────▶  │   │  Leader  +  RaftLog  +  ProgressTracker │ │  ─────────▶
                 │   └────────────────────────────────────────┘ │
                 │      persist │            │ send              │
                 │   IRaftStorage          IRaftTransport        │
                 └──────────────────────────────────────────────┘
```

## RaftCore — the deterministic state machine

`RaftCore` performs no I/O. You feed it logical `Tick()`s and inbound `Step(Message)`s; it mutates its in-memory state and produces outputs that the driver drains:

- **roles** — `Follower`, `PreCandidate`, `Candidate`, `Leader`, with term and vote bookkeeping.
- **election** — randomized (or pinned) election timeouts, optional **pre-vote** to avoid disrupting a stable leader's term, and optional check-quorum.
- **replication** — the leader appends entries, tracks each peer with a `Progress` (probe/replicate/snapshot state + an `Inflights` ring buffer), and advances the commit index once a quorum's match index passes an entry of the current term.
- **outputs** — `TakeMessages()`, `UnstableEntries()`/`UnstableSnapshot()` (to persist), `NextEntriesToApply()` (to apply), and `HardState`/`SoftState`. The driver persists, sends, applies, then calls `StableTo`/`AppliedTo` to advance.

## RaftLog and storage

`RaftLog` is a view over persisted `IRaftStorage` plus an in-memory `Unstable` tail (entries and an optional snapshot not yet flushed). It tracks the `Committed` and `Applied` watermarks and handles append, conflict resolution, commit, and snapshot restore.

`IRaftStorage` is the read side (the raft-rs `Storage` trait analog); `IRaftWritableStorage` adds append/compact/snapshot/hard-state writes used by the driver. Two implementations ship:

- **`MemoryStorage`** — volatile, with a sentinel entry marking the compacted boundary.
- **`FileRaftStorage`** (package `Raft.Storage.File`) — a crash-safe append-only WAL plus hard-state and snapshot files, replayed on open and tolerant of a torn trailing record.

## Membership — joint consensus

Configuration is a `JointConfig` of an incoming and outgoing `MajorityConfig`; a commit requires a quorum in **both**. `Changer` computes the next `ConfState` for single-step or joint changes (including learner promotion/demotion), and the leader auto-leaves the joint configuration once the transition commits.

## RaftNode — the async driver

`RaftNode` is the public, thread-safe facade. A single loop consumes ticks, inbound frames, and proposals from one channel, advances the core, then runs a ready cycle: persist the unstable snapshot/entries and hard state, send outbound messages over the transport (encoded with `MessageCodec`), apply committed entries to the `Committed` channel (applying conf-change entries via `Changer`), and advance the stable/applied watermarks.

## Transport

`IRaftTransport` delivers opaque, encoded message frames between nodes. Implementations:

- **`InMemoryNetwork`/`InMemoryTransport`** (package `Raft.Transport`) — a deterministic in-process bus with optional loss and partition injection for tests.
- **`NanoMsgBusTransport`** (package `Raft.Transport.NanoMsg`) — a BUS-topology transport over NanoMsgSharp (managed nanomsg/NNG over tcp, tls, ipc, ws, inproc). The consensus layer filters frames by recipient id.

## Performance

The codec and hot paths use `readonly struct`/`readonly ref struct` types, `Span<byte>`/`BinaryPrimitives`/`stackalloc`, `ArrayPool` framing, and an `Inflights` ring buffer, with no LINQ on hot paths. The library is `IsAotCompatible` on net8+ and the test suite runs as a NativeAOT binary.
