# Wire format

Raft messages are exchanged as opaque, length-agnostic frames produced by `Raft.Messages.MessageCodec` — a compact, allocation-free big-endian binary codec written directly into caller buffers with `Span<byte>`/`BinaryPrimitives`. (Behavioral parity with tikv/raft-rs is verified at the *outcome* level, not the wire level, so this encoding is the library's own and is not eraftpb/protobuf compatible.)

## Message

Every `Message` encodes a fixed header followed by optional entries and an optional snapshot:

| Field | Bytes | Notes |
| --- | --- | --- |
| Type | 1 | `MessageType` (Append, Vote, PreVote, Heartbeat, Snapshot, TimeoutNow, …) |
| From / To | 16 | sender and recipient node ids (`ulong`) |
| Term | 8 | sender term |
| LogTerm | 8 | term of the entry preceding `Index` (or last-log term for votes) |
| Index | 8 | prev-log / last-log / vote-or-transfer target index |
| Commit | 8 | leader commit index |
| RejectHint | 8 | follower's last index on an append rejection |
| Context | 8 | opaque correlation (leadership transfer) |
| Flags | 1 | bit0 = reject, bit1 = snapshot present |
| EntryCount | 4 | number of entries that follow |

Each entry is `Type` (1) · `Term` (8) · `Index` (8) · `DataLen` (4) · `Data`. When the snapshot flag is set, the snapshot follows as `Index` (8) · `Term` (8) · `DataLen` (4) · `Data` · `ConfState`, where `ConfState` is `AutoLeave` (1) and four length-prefixed id arrays (voters, learners, outgoing voters, next learners).

## Codec usage

```csharp
using Raft.Messages;

var buffer = new byte[MessageCodec.EncodedLength(message)];
MessageCodec.TryWrite(message, buffer);

if (MessageCodec.TryParse(buffer, out var decoded))
{
    // dispatch decoded
}
```

Decoding validates every length and rejects truncated input. Transports frame these payloads themselves; the in-memory transport delivers them whole, and stream/datagram transports can use `Raft.Transport.FrameCodec` for 4-byte big-endian length prefixing.
