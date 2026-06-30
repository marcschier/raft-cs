# Adoption guide

This guide covers what you need to run `Raft` in a real application: building a state machine, bootstrapping and restarting a cluster, snapshots and log compaction, cross-machine transports, the threading model, and operations. It assumes you have read [Getting started](getting-started.md).

## Mental model

A `RaftNode` is one replica. It owns a deterministic consensus core, a durable `IRaftWritableStorage`, an `IRaftTransport`, and a single driver loop. You give it opaque command bytes via `ProposeAsync`; once a quorum has stored a command it is *committed*, and every replica surfaces committed commands through the `Committed` channel **in the same order**. Your job is to apply those commands to your own state machine. Raft guarantees the order and the agreement; the meaning of the bytes is entirely yours.

## Building a state machine

A state machine is just a fold over the committed command stream. Three rules make it correct across restarts:

1. **Apply in order, from `Committed`.** Never apply a command before it appears on `Committed`.
2. **Track the applied index durably**, alongside your state. On restart you must not re-apply commands you already applied (the log replays from the last snapshot/compaction point, which may be behind your state machine).
3. **Make apply idempotent or index-guarded**, because after a crash you may see a command you already applied.

`RaftNode` does not tag each committed payload with its log index (the `Committed` stream is raw bytes). The simplest robust pattern is to make the **index part of your command** (or derive your applied index from your own state) so you can skip already-applied commands:

```csharp
long applied = LoadAppliedIndexFromMyStore(); // durable, next to your state

await foreach (ReadOnlyMemory<byte> payload in node.Committed.ReadAllAsync(ct))
{
    Command command = Decode(payload.Span);
    if (command.Index <= applied)
    {
        continue; // already applied before the crash
    }

    ApplyToMyState(command);          // mutate your state machine
    applied = command.Index;
    PersistMyStateAndAppliedIndex();  // durably, together
}
```

If you do not want to stamp indices into commands, you can instead consume `Committed` and persist your applied state and a monotonically increasing counter together, treating the counter as your applied watermark.

The node's own watermarks are available for coordination: `node.CommitIndex` (committed) and `node.AppliedIndex` (the highest index the node has surfaced through `Committed`).

## Bootstrapping and restarting

Every voter needs the **same initial membership**, given as a `ConfState` of node ids.

- **`MemoryStorage`** takes the membership in its constructor: `new MemoryStorage(new ConfState(new ulong[] {1,2,3}))`.
- **`FileRaftStorage`** has no `ConfState` constructor. For a *fresh* cluster, seed it once (see below); on *restart* it recovers membership, log, and hard state from disk — do not re-seed.

```csharp
using var storage = new FileRaftStorage(new FileRaftStorageOptions("/var/lib/raft/node1"));
if (storage.InitialState().ConfState.Voters.Count == 0)
{
    storage.SetConfState(new ConfState(new ulong[] { 1, 2, 3 }));
}

await using var node = new RaftNode(config, storage, transport);
await node.StartAsync();
```

Add or remove members at runtime with `ChangeConfigurationAsync` (single or joint changes, including learners). Watch `CommittedConfigurations` to learn when a change commits.

## Threading model and thread-safety

Everything funnels through one driver loop, so the **public `RaftNode` API is fully thread-safe** — call `ProposeAsync`, `ChangeConfigurationAsync`, `CompactAsync`, etc. from any thread. Consequences:

- **Drain the streams.** `Committed`, `StateChanges`, and `CommittedConfigurations` are channels. `Committed` is unbounded and must be consumed, or it grows; the two observation streams are bounded drop-oldest (they never grow and always retain the latest value). All three complete when the node is disposed, so a `ReadAllAsync` loop ends cleanly.
- **You never run on the consensus loop.** You only touch the node through its async API and channels, so your code cannot stall consensus — but if a single thread both proposes and drains `Committed`, keep them decoupled (e.g. a separate consumer task) so you cannot self-deadlock under backpressure.
- **Dispose** with `await node.DisposeAsync()`; it stops the loop, completes the streams, and disposes the transport.

## Transports: running across machines

The in-memory transport is for tests on one process. For a real cluster use `NanoMsgBusTransport` (package `RaftCs.Transport.NanoMsg`), a managed nanomsg/NNG **BUS** transport over tcp/tls/ipc/ws/inproc. Each node binds a local endpoint and dials its peers; the consensus layer filters frames by recipient id.

```csharp
using Raft.Transport.NanoMsg;

var options = new NanoMsgBusTransportOptions { BindAddress = "tcp://0.0.0.0:5560" };
options.Peers.Add("tcp://10.0.0.2:5560");
options.Peers.Add("tcp://10.0.0.3:5560");

await using var transport = new NanoMsgBusTransport(options);
await using var node = new RaftNode(config, storage, transport);
await node.StartAsync();
```

Set `BindAddress` to `tcp://*:0` for an OS-assigned port and read it back from `transport.BoundPort`. `SocketOptions` configures TLS, timeouts, and message-size limits; `MaxFrameLength` caps accepted frames. To implement a different transport, implement `IRaftTransport` (and optionally `IRaftBatchTransport` for batched sends).

## Snapshots and log compaction

Without compaction the log grows forever. Compaction discards the already-applied prefix; a follower that then falls behind the compacted boundary is caught up by an **install-snapshot** message instead of a log replay.

**What the library does:** `RaftNode.CompactAsync(index)` discards the applied prefix on the driver loop (thread-safe), and the leader automatically sends a snapshot (produced by `IRaftStorage.Snapshot()`) to any follower whose next index falls below the compacted boundary. A follower installs that snapshot via `IRaftWritableStorage.ApplySnapshot`.

**What you must provide:** the snapshot must carry your *application* state, and only your code knows that state. The built-in `MemoryStorage` and `FileRaftStorage` are **log stores** — their `Snapshot()` returns no application data, so they cannot bring a far-behind follower's state machine up to date. For production compaction you have two options:

1. **Keep voters caught up before compacting** (e.g. compact only well behind the slowest follower's match index) so no follower ever needs a data-carrying snapshot. Simplest, works with the built-in stores.
2. **Implement a snapshotting `IRaftStorage`** whose `Snapshot()` serializes your state machine and whose `ApplySnapshot()` restores it (the raft-rs/etcd model, where storage and state machine are integrated). This is the robust path for clusters where a follower can fall arbitrarily behind.

A safe compaction loop:

```csharp
// Periodically, after persisting your state machine up to node.AppliedIndex:
await node.CompactAsync(node.AppliedIndex);
```

> Application-facing delivery of snapshot *data* through the node (so even a generic log store could hand a follower's state machine the bytes) is a planned enhancement; today it requires a snapshotting `IRaftStorage` as in option 2.

## Operations

- **Storage failures are fatal.** If a durable write fails, the node surfaces the exception by **faulting the `Committed` channel** and stops its loop. Treat a faulted `Committed` reader as "this replica is down" and restart the process (it recovers from disk).
- **Backpressure.** A slow `Committed` consumer does not block consensus, but the unbounded channel buffers committed commands until you read them. Keep up, or apply in batches.
- **Tuning.** `ElectionTick`/`HeartbeatTick` are in logical ticks; `RaftNodeOptions.TickInterval` maps ticks to wall clock (default 50 ms, so the default election timeout is ~0.5 s). Use `ElectionTick ≈ 10 × HeartbeatTick`. Enable `PreVote` to avoid term inflation from flapping nodes, and `CheckQuorum` to make a partitioned leader step down. `MaxInflightMessages`/`MaxInflightBytes` bound per-follower replication; `MaxUncommittedEntriesSize` caps the uncommitted tail when quorum is lost.

## See also

- [Architecture](architecture.md) — how the core, driver, storage, and transport fit together.
- [API reference](api.md) — the full member and options tables.
- [Wire format](wire-format.md) — the message encoding for custom transports.
