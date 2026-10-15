# Adoption guide

This guide covers what you need to run `Raft` in a real application: building a state machine, bootstrapping and restarting a cluster, snapshots and log compaction, cross-machine transports, the threading model, and operations. It assumes you have read [Getting started](getting-started.md).

## Mental model

A `RaftNode` is one replica. It owns a deterministic consensus core, a durable `IRaftWritableStorage`, an `IRaftTransport`, and a single driver loop. You give it opaque command bytes via `ProposeAsync`; once a quorum has stored a command it is *committed*, and every replica surfaces committed commands through the `Committed` channel **in the same order**. Your job is to apply those commands to your own state machine. Raft guarantees the order and the agreement; the meaning of the bytes is entirely yours.

## Building a state machine

A state machine is just a fold over the committed command stream. Three rules make it correct across restarts:

1. **Apply in order, from `CommittedItems`** (or `Committed`). Never apply a command before it appears there.
2. **Track the applied index durably**, alongside your state. On restart you must not re-apply commands you already applied (the log replays from the last snapshot/compaction point, which may be behind your state machine).
3. **Make apply idempotent or index-guarded**, because after a crash you may see a command you already applied.

`CommittedItems` delivers each committed command with its log index (`CommittedItem.Index`) and term, so the index of the last item you applied is the log position your state corresponds to. It also delivers snapshots (`CommittedItemKind.Snapshot`) that replace your state; see [Snapshots and log compaction](#snapshots-and-log-compaction). Guard with the index to skip already-applied items:

```csharp
ulong applied = LoadAppliedIndexFromMyStore(); // durable, next to your state

await foreach (CommittedItem item in node.CommittedItems.ReadAllAsync(ct))
{
    if (item.Index <= applied)
    {
        continue; // already applied before the crash
    }

    if (item.Kind == CommittedItemKind.Snapshot)
    {
        RestoreMyState(item.Data.Span);   // replaces the whole state
    }
    else
    {
        ApplyToMyState(item.Data.Span);   // mutate your state machine
    }

    applied = item.Index;
    PersistMyStateAndAppliedIndex();  // durably, together
}
```

`Committed` is the same stream without the positions (only the payloads); both read one queue, so consume one of them.

The node's own watermarks are available for coordination: `node.CommitIndex` (committed) and `node.AppliedIndex` (the highest index the node has applied and queued on `CommittedItems`, which your consumer may not have processed yet).

## Bootstrapping and restarting

Every voter needs the **same initial membership**, given as a `ConfState` of node ids.

- **`MemoryStorage`** takes the membership in its constructor: `new MemoryStorage(new ConfState(new ulong[] {1,2,3}))`.
- **`FileRaftStorage`** has no `ConfState` constructor. For a *fresh* cluster, seed it once with `SetConfState(initial, 0)` (see below); on *restart* it recovers membership, log, and hard state from disk — do not re-seed.

```csharp
using var storage = new FileRaftStorage(new FileRaftStorageOptions("/var/lib/raft/node1"));
if (storage.InitialState().ConfState.Voters.Count == 0)
{
    storage.SetConfState(new ConfState(new ulong[] { 1, 2, 3 }), 0); // bootstrap once, at index 0
}

await using var node = new RaftNode(config, storage, transport);
await node.StartAsync();
```

Index 0 (the `IRaftConfStateIndexStorage` overload) records that the membership precedes every log entry, so a node that crashes after a membership change commits, but before the new membership is recorded, applies the change on restart. The one-argument `SetConfState(confState)` is the compatible form: it leaves the index unknown, and a restarting node then keeps the stored membership as it is.

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

The snapshot carries your *application* state, which only your code knows, so your state machine creates it and the node does the rest:

- **Create.** `RaftNode.CreateSnapshotAsync(index, data)` records your serialized state at `index` (the `CommittedItem.Index` of the last item you applied) in the storage, with the cluster membership at that index. The storage must implement `IRaftSnapshotStorage`; `MemoryStorage` and `FileRaftStorage` do.
- **Compact.** `RaftNode.CompactAsync(index)` then discards the log up to `index` on the driver loop. It refuses to compact past the latest snapshot, because the snapshot and the log after it must together hold every applied command. `FileRaftStorage` drops the compacted entries from memory and appends a compaction record to its log file; in this version compaction does not shrink the file.
- **Lagging followers.** The leader sends the latest snapshot to any follower whose next index falls below the compacted boundary. Once the follower has stored it, its `CommittedItems` stream delivers it as a `CommittedItemKind.Snapshot` item: replace your state with `item.Data`, then apply the commands after it. A snapshot `FileRaftStorage` kept from an earlier version is restored on restart but never sent, because it does not record the membership at its index; until your application records a new snapshot with `CreateSnapshotAsync`, a follower behind it waits.
- **Restart.** When the storage holds a snapshot, `CommittedItems` starts with it, followed by the commands after it. A storage whose log was compacted past its latest snapshot is refused (`InvalidOperationException`), because the commands in between are lost.

A compaction loop inside your state machine's apply loop:

```csharp
await foreach (CommittedItem item in node.CommittedItems.ReadAllAsync(ct))
{
    if (item.Kind == CommittedItemKind.Snapshot)
    {
        RestoreMyState(item.Data.Span);   // replaces the whole state
    }
    else
    {
        ApplyToMyState(item.Data.Span);
    }

    if (++sinceSnapshot >= 1024)
    {
        await node.CreateSnapshotAsync(item.Index, SerializeMyState(), ct);
        await node.CompactAsync(item.Index, ct);
        sinceSnapshot = 0;
    }
}
```

With snapshots in the storage, the snapshot is the durable copy of your state, so you need not persist the state and applied index yourself: after a restart the node hands you the snapshot and replays the commands after it. `Committed` (payloads only) skips snapshot items; use `CommittedItems` when you compact.

## Operations

- **Storage failures are fatal.** If a durable write fails, the node surfaces the exception by **faulting the `Committed` channel** and stops its loop. Treat a faulted `Committed` reader as "this replica is down" and restart the process (it recovers from disk).
- **Backpressure.** A slow `Committed` consumer does not block consensus, but the unbounded channel buffers committed commands until you read them. Keep up, or apply in batches.
- **Tuning.** `ElectionTick`/`HeartbeatTick` are in logical ticks; `RaftNodeOptions.TickInterval` maps ticks to wall clock (default 50 ms, so the default election timeout is ~0.5 s). Use `ElectionTick ≈ 10 × HeartbeatTick`. Enable `PreVote` to avoid term inflation from flapping nodes, and `CheckQuorum` to make a partitioned leader step down. `MaxInflightMessages`/`MaxInflightBytes` bound per-follower replication; `MaxUncommittedEntriesSize` caps the uncommitted tail when quorum is lost.

## See also

- [Architecture](architecture.md) — how the core, driver, storage, and transport fit together.
- [API reference](api.md) — the full member and options tables.
- [Wire format](wire-format.md) — the message encoding for custom transports.
