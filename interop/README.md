# Raft behavioral-parity interop

These tests compare this library's Raft behavior with `tikv/raft-rs` through a neutral JSON scenario protocol. They do
not assert byte-level compatibility. The goal is outcome parity: both implementations drive the same logical scenario to
quiescence and produce the same normalized committed-command trace.

## Harness

The Rust harness lives in `interop/raft-rs-harness` and wraps `raft = "0.7"` with in-memory `MemStorage` and a
deterministic message bus. Each scenario configures one `RawNode` per node id, sets fixed election timeouts, executes
logical steps, and prints a normalized JSON trace:

```json
{
  "name": "three-node-replicate",
  "leader": 1,
  "term": 1,
  "nodes": [
    { "id": 1, "committed": ["x=1", "y=2", "z=3"] },
    { "id": 2, "committed": ["x=1", "y=2", "z=3"] },
    { "id": 3, "committed": ["x=1", "y=2", "z=3"] }
  ]
}
```

Empty leader no-op entries and conf-change entries are omitted. Nodes are sorted by id.

## Running

Install a Rust toolchain, then run either one scenario or the full golden-generation self-test:

```powershell
cd interop\raft-rs-harness
cargo run --release -- run ..\scenarios\three-node-replicate.json
cargo run --release -- self-test
```

`self-test` runs every `interop/scenarios/*.json` except `schema.json` and `*.expected.json`, verifies that each
scenario quiesces and all nodes have identical committed command lists, and rewrites `<name>.expected.json` beside the
scenarios.

## Scenario format

See `interop/scenarios/schema.json` for the complete schema. A scenario includes:

- `name`: stable scenario and golden-file name.
- `nodes`: non-zero node ids.
- `election_ticks`: per-node deterministic logical election timeouts. Distinct values force the lowest-timeout node to
  campaign first when `tick_all` is used.
- `heartbeat_ticks`: logical heartbeat interval.
- `steps`: operations executed in order.

Supported steps:

```json
{ "op": "campaign", "node": 1 }
{ "op": "tick", "node": 1, "count": 11 }
{ "op": "tick_all", "count": 11 }
{ "op": "propose", "node": 1, "command": "x=1" }
{ "op": "deliver" }
{ "op": "isolate", "nodes": [3] }
{ "op": "heal" }
```

`deliver` drains in-flight messages and readies to quiescence. `isolate` drops messages crossing the
isolated/non-isolated boundary; messages within each side continue to flow. `heal` removes the partition.

## .NET tests

`tests/Raft.Interop.Tests` includes `interop/scenarios/**/*.json` as test data. The .NET parity tests should run the
same scenario JSON through the managed Raft implementation and compare the resulting normalized trace with the
generated `*.expected.json` files.
