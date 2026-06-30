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
- **`FileRaftStorage`** (package `RaftCs.Storage.File`) — a crash-safe append-only WAL plus hard-state and snapshot files, replayed on open and tolerant of a torn trailing record.

## Membership — joint consensus

Configuration is a `JointConfig` of an incoming and outgoing `MajorityConfig`; a commit requires a quorum in **both**. `Changer` computes the next `ConfState` for single-step or joint changes (including learner promotion/demotion), and the leader auto-leaves the joint configuration once the transition commits.

## RaftNode — the async driver

`RaftNode` is the public, thread-safe facade. A single loop consumes ticks, inbound frames, proposals, and storage-completion signals from one channel and advances the deterministic core, so all core state stays single-threaded while I/O happens off the loop.

Each ready cycle uses the **asynchronous storage-writes** model (mirroring etcd's `AsyncStorageWrites`), which is what lets the leader write to its own disk in parallel with replicating to followers:

1. **Send first.** Immediately sendable messages (a leader's `Append`/`Heartbeat`, vote requests, snapshots) are drained with `TakeSendNowMessages()` and dispatched — batched through `IRaftBatchTransport.SendManyAsync` when the transport supports it, else per-frame. Followers therefore start persisting while the leader's own write is still in flight.
2. **Persist off the loop.** `TakeStorageWrite()` produces a `StorageWrite` — the unstable entries, the changed hard state, any pending snapshot, and the responses to release once durable — which a background worker persists with a single `IRaftWritableStorage.Write` (one fsync covering entries + hard state). At most one write is outstanding; entries appended meanwhile are batched into the next write.
3. **Acknowledge.** When the write completes, the worker posts a signal back onto the loop. The loop releases the held-back responses (a follower's append acknowledgement, a granted vote — never sent before the state they reflect is durable) and calls `AckStorageWrite`, which advances the stable log and, for a leader, its own match index (and hence the commit index). Gating the leader's match on durability is the safety counterpart of step 1's parallelism.
4. **Apply.** Committed entries up to the **durable** stable index are applied to the `Committed` channel (conf-change entries are applied on the loop, via `Changer`, for ordering); the applied watermark advances. Commit may run ahead of the local disk, so apply never passes what has been persisted.

Besides the `Committed` command stream, the driver surfaces two observation streams so consumers need not poll: `StateChanges` publishes a `RaftStateChange` whenever the recognized leader or role changes (raft-rs `SoftState` semantics, with the term carried as context), and `CommittedConfigurations` publishes the new `ConfState` each time a membership change commits (so external membership reconcile sees committed state rather than relying on the node's internal apply). Both emit the current value as a baseline first, are written only on the loop thread, and are bounded drop-oldest channels — they never block consensus and never grow without bound when unobserved. The latest configuration is also available synchronously via the `Configuration` property.

## Transport

`IRaftTransport` delivers opaque, encoded message frames between nodes. The optional `IRaftBatchTransport` adds `SendManyAsync` so a cycle's outbound frames coalesce into one call. Implementations:

- **`InMemoryNetwork`/`InMemoryTransport`** (package `RaftCs.Transport`) — a deterministic in-process bus with optional loss and partition injection for tests.
- **`NanoMsgBusTransport`** (package `RaftCs.Transport.NanoMsg`) — a BUS-topology transport over NanoMsgSharp (managed nanomsg/NNG over tcp, tls, ipc, ws, inproc). The consensus layer filters frames by recipient id.

## Flow control and overload protection

The leader tracks each peer with a `Progress` whose `Inflights` ring bounds in-flight appends by both message count (`MaxInflightMessages`) and payload bytes (`MaxInflightBytes`), so a slow follower cannot force unbounded buffering. A separate **uncommitted-log byte cap** (`MaxUncommittedEntriesSize`) bounds the leader's accepted-but-not-yet-committed tail: when a quorum is lost and nothing commits, the tail fills and further proposals are dropped instead of growing the log without bound (a single proposal is always admitted when the tail is empty). With `CheckQuorum`, a leader that stops hearing from a majority steps down to follower. A follower with `DisableProposalForwarding` left off redirects client proposals to the leader it recognizes. All four default to raft-rs-compatible settings (forwarding on; check-quorum off; both caps unbounded).

## Performance

The codec and hot paths use `readonly struct`/`readonly ref struct` types, `Span<byte>`/`BinaryPrimitives`/`stackalloc`, `ArrayPool` framing, and an `Inflights` ring buffer, with no LINQ on hot paths. The library is `IsAotCompatible` on net8+ and the test suite runs as a NativeAOT binary.
