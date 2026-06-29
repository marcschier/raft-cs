// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Configuration;

namespace Raft.Storage;

/// <summary>Metadata describing the log position and membership captured by a <see cref="Snapshot"/>.</summary>
public readonly struct SnapshotMetadata
{
    /// <summary>Initializes a new instance of the <see cref="SnapshotMetadata"/> struct.</summary>
    /// <param name="index">The log index the snapshot replaces up to and including.</param>
    /// <param name="term">The term of the entry at <paramref name="index"/>.</param>
    /// <param name="confState">The cluster membership captured by the snapshot.</param>
    public SnapshotMetadata(ulong index, ulong term, ConfState confState)
    {
        Index = index;
        Term = term;
        ConfState = confState;
    }

    /// <summary>Gets the log index the snapshot replaces up to and including.</summary>
    public ulong Index { get; }

    /// <summary>Gets the term of the entry at <see cref="Index"/>.</summary>
    public ulong Term { get; }

    /// <summary>Gets the cluster membership captured by the snapshot.</summary>
    public ConfState ConfState { get; }
}

/// <summary>A point-in-time state-machine snapshot used to bootstrap or catch up a replica.</summary>
public sealed class Snapshot
{
    /// <summary>Initializes a new instance of the <see cref="Snapshot"/> class.</summary>
    /// <param name="metadata">The snapshot metadata.</param>
    /// <param name="data">The opaque state-machine bytes.</param>
    public Snapshot(SnapshotMetadata metadata, ReadOnlyMemory<byte> data)
    {
        Metadata = metadata;
        Data = data;
    }

    /// <summary>Gets the snapshot metadata.</summary>
    public SnapshotMetadata Metadata { get; }

    /// <summary>Gets the opaque state-machine bytes.</summary>
    public ReadOnlyMemory<byte> Data { get; }

    /// <summary>Gets a value indicating whether the snapshot carries no position (an empty snapshot).</summary>
    public bool IsEmpty => Metadata.Index == 0;
}
