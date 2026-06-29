// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft;

/// <summary>Tuning parameters for a <see cref="RaftCore"/> / <see cref="RaftNode"/>.</summary>
public sealed class RaftConfig
{
    /// <summary>Gets or sets this node's unique, non-zero id.</summary>
    public ulong Id { get; set; }

    /// <summary>Gets or sets the number of <see cref="RaftNode"/> ticks between election timeouts.</summary>
    public int ElectionTick { get; set; } = 10;

    /// <summary>Gets or sets the number of ticks between leader heartbeats.</summary>
    public int HeartbeatTick { get; set; } = 1;

    /// <summary>Gets or sets a value indicating whether the disruption-free pre-vote protocol is enabled.</summary>
    public bool PreVote { get; set; }

    /// <summary>Gets or sets a value indicating whether a leader steps down without quorum contact.</summary>
    public bool CheckQuorum { get; set; }

    /// <summary>Gets or sets the soft maximum total payload bytes per append message.</summary>
    public ulong MaxSizePerMessage { get; set; } = 1024 * 1024;

    /// <summary>Gets or sets the per-peer in-flight append window capacity.</summary>
    public int MaxInflightMessages { get; set; } = 256;

    /// <summary>
    /// Gets or sets a fixed randomized election timeout (in ticks). When zero, the timeout is chosen deterministically
    /// from <see cref="ElectionTick"/> seeded by <see cref="Id"/>. Tests pin this to force a deterministic leader.
    /// </summary>
    public int RandomizedElectionTimeout { get; set; }

    internal void Validate()
    {
        if (Id == 0)
        {
            throw new ArgumentException("Raft node id must be non-zero.", nameof(Id));
        }

        if (ElectionTick <= HeartbeatTick)
        {
            throw new ArgumentException("ElectionTick must be greater than HeartbeatTick.", nameof(ElectionTick));
        }

        if (HeartbeatTick <= 0)
        {
            throw new ArgumentException("HeartbeatTick must be positive.", nameof(HeartbeatTick));
        }

        if (MaxInflightMessages <= 0)
        {
            throw new ArgumentException("MaxInflightMessages must be positive.", nameof(MaxInflightMessages));
        }
    }
}
