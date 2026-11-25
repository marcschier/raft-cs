// Copyright (c) marcschier. Licensed under the MIT License.

using Raft.Storage;

namespace Raft;

/// <summary>Options that govern the <see cref="RaftNode"/> driver loop.</summary>
public sealed class RaftNodeOptions
{
    /// <summary>Gets or sets the wall-clock interval between logical ticks fed to the core (default 50 ms).</summary>
    public TimeSpan TickInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Gets or sets the soft maximum number of committed entries applied per ready cycle.</summary>
    public ulong MaxApplyBytes { get; set; } = 16 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the maximum encoded peer message size in bytes (default 16 MiB), including snapshot data.
    /// This bound is checked before decoding and must be positive. Coordinate it with the transport's frame limit.
    /// </summary>
    public int MaxIncomingMessageBytes { get; set; } = 16 * 1024 * 1024;

    /// <summary>
    /// Gets or sets an optional synchronous, side-effect-free check of application snapshot data. It runs before a
    /// peer snapshot is installed, a local snapshot is created, or a stored snapshot is delivered at startup.
    /// Returning <see langword="false"/> or throwing an exception other than resource exhaustion rejects the snapshot.
    /// Peer rejection leaves the node running; startup fails if the stored snapshot cannot be restored.
    /// </summary>
    public Func<Snapshot, bool>? SnapshotValidator { get; set; }

    /// <summary>
    /// Gets or sets the per-stream buffer capacity for the <see cref="RaftNode.StateChanges"/> and
    /// <see cref="RaftNode.CommittedConfigurations"/> observation streams. The streams drop the oldest buffered item
    /// when a consumer falls this far behind, so the most recent value is always retained and an unobserved stream
    /// never grows without bound. Must be positive.
    /// </summary>
    public int StateObservationCapacity { get; set; } = 256;
}
