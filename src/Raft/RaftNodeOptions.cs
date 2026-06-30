// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft;

/// <summary>Options that govern the <see cref="RaftNode"/> driver loop.</summary>
public sealed class RaftNodeOptions
{
    /// <summary>Gets or sets the wall-clock interval between logical ticks fed to the core (default 50 ms).</summary>
    public TimeSpan TickInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Gets or sets the soft maximum number of committed entries applied per ready cycle.</summary>
    public ulong MaxApplyBytes { get; set; } = 16 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the per-stream buffer capacity for the <see cref="RaftNode.StateChanges"/> and
    /// <see cref="RaftNode.CommittedConfigurations"/> observation streams. The streams drop the oldest buffered item
    /// when a consumer falls this far behind, so the most recent value is always retained and an unobserved stream
    /// never grows without bound. Must be positive.
    /// </summary>
    public int StateObservationCapacity { get; set; } = 256;
}
