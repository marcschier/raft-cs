# Getting started

## Install

```shell
dotnet add package RaftCs
dotnet add package RaftCs.Transport            # in-memory transport + IRaftTransport
dotnet add package RaftCs.Transport.NanoMsg    # optional: NNG/nanomsg transport
dotnet add package RaftCs.Storage.File         # optional: crash-safe file (WAL) storage
```

## Run a three-node cluster

Each node is a `RaftNode` wired to an `IRaftWritableStorage` (here `MemoryStorage`) and an `IRaftTransport` (here a shared `InMemoryNetwork`). Give every node the same initial membership via `ConfState`.

```csharp
using Raft;
using Raft.Configuration;
using Raft.Storage;
using Raft.Transport;

ulong[] cluster = { 1, 2, 3 };
await using var network = new InMemoryNetwork();
var nodes = new List<RaftNode>();

foreach (ulong id in cluster)
{
    var node = new RaftNode(
        new RaftConfig { Id = id, ElectionTick = 10, HeartbeatTick = 1, PreVote = true },
        new MemoryStorage(new ConfState(cluster)),
        network.CreateNode(id));
    nodes.Add(node);
    await node.StartAsync();
}
```

## Propose and apply commands

Proposals are opaque bytes. Only the leader accepts them; once committed, every node surfaces them through its `Committed` channel in log order.

```csharp
RaftNode leader = nodes.First(n => n.IsLeader);
await leader.ProposeAsync(System.Text.Encoding.UTF8.GetBytes("set x = 1"));

await foreach (var command in nodes[0].Committed.ReadAllAsync())
{
    Apply(System.Text.Encoding.UTF8.GetString(command.Span));
}
```

## Change membership

Single-node and joint-consensus changes are proposed like commands and applied when committed.

```csharp
await leader.ChangeConfigurationAsync(ConfChangeV2.Single(ConfChangeType.AddLearnerNode, 4));
await leader.ChangeConfigurationAsync(ConfChangeV2.Single(ConfChangeType.AddNode, 4)); // promote
```

## Transfer leadership

```csharp
await leader.TransferLeadershipAsync(targetId: 2);
```

## Observe leadership and membership

Besides the `Committed` command stream, a node pushes leadership and membership changes so you do not have to poll:

```csharp
// React to becoming / losing leadership (baseline state is delivered first).
_ = Task.Run(async () =>
{
    await foreach (RaftStateChange change in node.StateChanges.ReadAllAsync())
    {
        Console.WriteLine(change.IsLeader ? $"became leader (term {change.Term})" : $"following {change.LeaderId}");
    }
});

// Reconcile against committed membership.
await foreach (ConfState membership in node.CommittedConfigurations.ReadAllAsync())
{
    Console.WriteLine($"voters: {string.Join(",", membership.Voters)}");
}
```

The current values are also available synchronously: `node.IsLeader`, `node.LeaderId`, `node.Term`, `node.Role`, `node.CommitIndex`, `node.AppliedIndex`, and `node.Configuration`.

## Bound the log (compaction)

The log grows with every committed command. Snapshot your state machine at the index of the last item it applied, then discard the log up to that index so the log the node keeps in memory stays bounded (`FileRaftStorage` does not shrink its file on compaction in this version):

```csharp
// appliedIndex is the CommittedItem.Index of the last item your state machine applied.
await node.CreateSnapshotAsync(appliedIndex, SerializeMyState());
await node.CompactAsync(appliedIndex);
```

Both run on the node's driver loop, so they are safe to call from any thread. A follower that falls behind the compacted boundary receives the snapshot, and its `CommittedItems` stream hands it to the state machine — see [Snapshots and compaction](adoption.md#snapshots-and-log-compaction).

## Persist across restarts

Swap `MemoryStorage` for `FileRaftStorage` (package `RaftCs.Storage.File`) to durably persist the log, hard state, and snapshots:

```csharp
using Raft.Storage.File;

using var storage = new FileRaftStorage(new FileRaftStorageOptions("/var/lib/raft/node1") { Fsync = true });
```

Unlike `MemoryStorage(ConfState)`, a *fresh* `FileRaftStorage` starts with **empty membership** — there is no `ConfState` constructor. When you bootstrap a brand-new cluster, seed the initial membership once (on every node, with the same set) before constructing the `RaftNode`, or the node never knows it is a voter and never campaigns:

```csharp
using var storage = new FileRaftStorage(new FileRaftStorageOptions("/var/lib/raft/node1"));
if (storage.InitialState().ConfState.Voters.Count == 0)
{
    storage.SetConfState(new ConfState(new ulong[] { 1, 2, 3 })); // bootstrap once
}

var node = new RaftNode(config, storage, transport);
```

On a **restart**, the membership, log, and hard state are recovered from disk automatically — do not re-seed; just construct the node.

## Determinism for testing

Set `RaftConfig.RandomizedElectionTimeout` to pin a node's election timeout (the smallest one wins), which makes election outcomes deterministic in tests and the behavioral-parity harness.

## Next steps

For running a real cluster — building a state machine, bootstrapping, snapshots and log compaction, cross-machine transports, the threading model, and operational guidance — see the **[Adoption guide](adoption.md)**.
