// Copyright (c) marcschier. Licensed under the MIT License.

using System.Buffers.Binary;

namespace Raft.Configuration;

/// <summary>The kind of a single membership change.</summary>
public enum ConfChangeType : byte
{
    /// <summary>Add a voter (or promote a learner to voter).</summary>
    AddNode = 0,

    /// <summary>Add a non-voting learner (or demote a voter to learner).</summary>
    AddLearnerNode = 1,

    /// <summary>Remove a node entirely.</summary>
    RemoveNode = 2,
}

/// <summary>A single atomic membership change applied to one node.</summary>
public readonly struct ConfChangeSingle
{
    /// <summary>Initializes a new instance of the <see cref="ConfChangeSingle"/> struct.</summary>
    /// <param name="type">The change kind.</param>
    /// <param name="nodeId">The affected node id.</param>
    public ConfChangeSingle(ConfChangeType type, ulong nodeId)
    {
        Type = type;
        NodeId = nodeId;
    }

    /// <summary>Gets the change kind.</summary>
    public ConfChangeType Type { get; }

    /// <summary>Gets the affected node id.</summary>
    public ulong NodeId { get; }
}

/// <summary>A batch of membership changes applied atomically, optionally via joint consensus.</summary>
public sealed class ConfChangeV2
{
    /// <summary>Initializes a new instance of the <see cref="ConfChangeV2"/> class.</summary>
    /// <param name="changes">The atomic changes.</param>
    /// <param name="useJoint">Whether to transition through the joint configuration.</param>
    /// <param name="autoLeave">Whether the leader auto-leaves the joint configuration once committed.</param>
    public ConfChangeV2(IReadOnlyList<ConfChangeSingle> changes, bool useJoint = false, bool autoLeave = false)
    {
        Changes = changes ?? Array.Empty<ConfChangeSingle>();
        UseJoint = useJoint;
        AutoLeave = autoLeave;
    }

    /// <summary>Gets the atomic changes.</summary>
    public IReadOnlyList<ConfChangeSingle> Changes { get; }

    /// <summary>Gets a value indicating whether the change transitions through the joint configuration.</summary>
    public bool UseJoint { get; }

    /// <summary>Gets whether the leader auto-leaves the joint configuration when committed.</summary>
    public bool AutoLeave { get; }

    /// <summary>Gets a value indicating whether this is an empty change that leaves the joint configuration.</summary>
    public bool IsLeaveJoint => Changes.Count == 0 && !UseJoint;

    /// <summary>Creates a single-node, non-joint change.</summary>
    /// <param name="type">The change kind.</param>
    /// <param name="nodeId">The affected node id.</param>
    /// <returns>The change.</returns>
    public static ConfChangeV2 Single(ConfChangeType type, ulong nodeId) =>
        new(new[] { new ConfChangeSingle(type, nodeId) });

    /// <summary>Creates the empty change that leaves an active joint configuration.</summary>
    /// <returns>The leave-joint change.</returns>
    public static ConfChangeV2 LeaveJoint() => new(Array.Empty<ConfChangeSingle>());

    /// <summary>Encodes this change into a compact binary form for a log entry payload.</summary>
    /// <returns>The encoded bytes.</returns>
    public byte[] Encode()
    {
        var buffer = new byte[2 + (Changes.Count * 9)];
        buffer[0] = (byte)((UseJoint ? 1 : 0) | (AutoLeave ? 2 : 0));
        buffer[1] = checked((byte)Changes.Count);
        int offset = 2;
        for (int i = 0; i < Changes.Count; i++)
        {
            buffer[offset++] = (byte)Changes[i].Type;
            BinaryPrimitives.WriteUInt64BigEndian(buffer.AsSpan(offset), Changes[i].NodeId);
            offset += 8;
        }

        return buffer;
    }

    /// <summary>Decodes a change from a log entry payload.</summary>
    /// <param name="data">The encoded bytes.</param>
    /// <returns>The decoded change.</returns>
    public static ConfChangeV2 Decode(ReadOnlySpan<byte> data)
    {
        if (data.Length < 2)
        {
            return new ConfChangeV2(Array.Empty<ConfChangeSingle>());
        }

        byte flags = data[0];
        int count = data[1];
        var changes = new ConfChangeSingle[count];
        int offset = 2;
        for (int i = 0; i < count; i++)
        {
            var type = (ConfChangeType)data[offset++];
            ulong nodeId = BinaryPrimitives.ReadUInt64BigEndian(data.Slice(offset));
            offset += 8;
            changes[i] = new ConfChangeSingle(type, nodeId);
        }

        return new ConfChangeV2(changes, (flags & 1) != 0, (flags & 2) != 0);
    }
}
