# API reference

The shipping surface is the async `RaftNode` facade plus the deterministic `RaftCore` for advanced/embedded use, with replaceable storage and transport abstractions. All async types are `IAsyncDisposable`.

## RaftNode

| Member | Description |
| --- | --- |
| `new RaftNode(RaftConfig, IRaftWritableStorage, IRaftTransport, RaftNodeOptions?, TimeProvider?)` | Constructs a replica. |
| `ValueTask StartAsync(CancellationToken)` | Starts the transport, driver loop, and tick timer. |
| `ValueTask ProposeAsync(ReadOnlyMemory<byte>, CancellationToken)` | Proposes an application command (accepted only on the leader). |
| `ValueTask ChangeConfigurationAsync(ConfChangeV2, CancellationToken)` | Proposes a membership change. |
| `ValueTask TransferLeadershipAsync(ulong targetId, CancellationToken)` | Requests leadership transfer (leader only). |
| `ValueTask CampaignAsync(CancellationToken)` | Forces an immediate election campaign. |
| `Task CompactAsync(ulong index, CancellationToken)` | Discards the applied log prefix before `index` (on the loop; thread-safe). |
| `ChannelReader<ReadOnlyMemory<byte>> Committed` | Committed application commands, in log order. |
| `ChannelReader<RaftStateChange> StateChanges` | Leadership/role transitions (baseline first, then on leader-id/role change). |
| `ChannelReader<ConfState> CommittedConfigurations` | Committed cluster configurations (baseline first, then on each membership change). |
| `ConfState Configuration` | The most recently committed cluster configuration. |
| `ulong Id`, `ulong LeaderId`, `ulong Term`, `ulong CommitIndex`, `ulong AppliedIndex`, `RaftRole Role`, `bool IsLeader` | Observable state. |

## RaftConfig

| Property | Default | Meaning |
| --- | --- | --- |
| `Id` | — | This node's unique non-zero id. |
| `ElectionTick` | `10` | Ticks before an election timeout. |
| `HeartbeatTick` | `1` | Ticks between leader heartbeats. |
| `PreVote` | `false` | Enable the disruption-free pre-vote protocol. |
| `CheckQuorum` | `false` | Step down a leader without quorum contact. |
| `MaxSizePerMessage` | `1 MiB` | Soft cap on append payload bytes. |
| `MaxInflightMessages` | `256` | Per-peer in-flight append window (message count). |
| `MaxInflightBytes` | `ulong.MaxValue` | Per-peer in-flight append window (payload bytes; max = unbounded). |
| `MaxUncommittedEntriesSize` | `ulong.MaxValue` | Cap on uncommitted-log payload bytes a leader accepts (max = unbounded); guards against unbounded log growth when quorum is lost. |
| `DisableProposalForwarding` | `false` | When `false`, a follower forwards proposals to the leader it recognizes. |
| `RandomizedElectionTimeout` | `0` | Pin the election timeout (0 = randomized); set to make elections deterministic. |

## RaftNodeOptions

| Property | Default | Meaning |
| --- | --- | --- |
| `TickInterval` | `50 ms` | Wall-clock interval between logical ticks. |
| `MaxApplyBytes` | `16 MiB` | Soft cap on committed bytes applied per cycle. |
| `StateObservationCapacity` | `256` | Buffer for the `StateChanges`/`CommittedConfigurations` streams; oldest items drop when a consumer falls behind, so the latest value is always retained. |

## RaftCore

The deterministic engine, for embedding in a custom driver (the raft-rs `RawNode` analog). Feed it `Tick()` and `Step(Message)`. A custom driver can drive it two ways:

- **Synchronous** — drain `TakeMessages()`, `UnstableEntries()`, `UnstableSnapshot()`, and `NextEntriesToApply()`; persist; then call `StableTo`, `StableSnapshotTo`, and `AppliedTo`.
- **Asynchronous storage writes** (what `RaftNode` uses) — send `TakeSendNowMessages()` immediately, persist the `TakeStorageWrite()` batch off the loop with `IRaftWritableStorage.Write` and release its responses + call `AckStorageWrite` once durable, and apply `NextEntriesToApply()` (which never returns past the durable stable index). This lets a leader replicate in parallel with its own disk write.

Apply committed conf-change entries by computing the new `ConfState` (via `Changer`) and calling `ApplyConfChange`.

## Storage

| Type | Description |
| --- | --- |
| `IRaftStorage` | Read side: `InitialState`, `Entries`, `Term`, `FirstIndex`, `LastIndex`, `Snapshot`. |
| `IRaftWritableStorage` | Adds `Append`, `ApplySnapshot`, `Compact`, `SetHardState`, `SetConfState`. |
| `MemoryStorage` | Volatile in-memory store (package `RaftCs`). |
| `FileRaftStorage` / `FileRaftStorageOptions` | Crash-safe file (WAL) store (package `RaftCs.Storage.File`). |

## Transport

| Type | Description |
| --- | --- |
| `IRaftTransport` | `StartAsync`, `SendAsync(recipient, frame)`, `FrameReceived`. |
| `InMemoryNetwork` / `InMemoryTransport` | In-process bus with loss/partition injection (package `RaftCs.Transport`). |
| `NanoMsgBusTransport` / `NanoMsgBusTransportOptions` | NNG/nanomsg BUS transport (package `RaftCs.Transport.NanoMsg`). |

## Membership

| Type | Description |
| --- | --- |
| `ConfChangeV2` / `ConfChangeSingle` / `ConfChangeType` | A batch of `AddNode`/`AddLearnerNode`/`RemoveNode` changes, optionally joint. |
| `ConfState` | Voters, learners, and any in-progress joint state. |
| `Changer` | Computes the next `ConfState` for a change. |
