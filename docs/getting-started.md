# Getting started

## Install

```shell
dotnet add package Raft
dotnet add package Raft.Transport            # in-memory transport + IRaftTransport
dotnet add package Raft.Transport.NanoMsg    # optional: NNG/nanomsg transport
dotnet add package Raft.Storage.File         # optional: crash-safe file (WAL) storage
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

## Persist across restarts

Swap `MemoryStorage` for `FileRaftStorage` (package `Raft.Storage.File`) to durably persist the log, hard state, and snapshots:

```csharp
using var storage = new FileRaftStorage(new FileRaftStorageOptions { Directory = "/var/lib/raft/node1" });
var node = new RaftNode(config, storage, transport);
```

## Determinism for testing

Set `RaftConfig.RandomizedElectionTimeout` to pin a node's election timeout (the smallest one wins), which makes election outcomes deterministic in tests and the behavioral-parity harness.
