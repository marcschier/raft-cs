// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Transport;

/// <summary>Configures deterministic fault injection for <see cref="InMemoryNetwork"/>.</summary>
public sealed class InMemoryNetworkOptions
{
    /// <summary>
    /// Gets or sets a predicate that returns <see langword="true"/> when a frame from the first node id to
    /// the second node id should be dropped.
    /// </summary>
    public Func<ulong, ulong, bool>? DropPredicate { get; set; }

    /// <summary>Gets or sets the independent probability that any deliverable frame is dropped.</summary>
    public double DropRate { get; set; }

    /// <summary>Gets or sets the deterministic random seed used with <see cref="DropRate"/>.</summary>
    public int? Seed { get; set; }
}
