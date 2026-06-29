// Copyright (c) marcschier. Licensed under the MIT License.

namespace Raft.Storage;

/// <summary>Identifies a recoverable storage condition surfaced by <see cref="IRaftStorage"/>.</summary>
public enum RaftStorageError
{
    /// <summary>The requested log index precedes the compacted prefix.</summary>
    Compacted,

    /// <summary>The requested log index has not yet been written.</summary>
    Unavailable,

    /// <summary>A proposed snapshot is older than the current state.</summary>
    SnapshotOutOfDate,

    /// <summary>A snapshot was requested but is not ready yet; retry later.</summary>
    SnapshotTemporarilyUnavailable,

    /// <summary>An implementation-specific storage failure.</summary>
    Other,
}

/// <summary>The exception thrown by <see cref="IRaftStorage"/> implementations for recoverable conditions.</summary>
public sealed class RaftStorageException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="RaftStorageException"/> class.</summary>
    /// <param name="error">The storage condition.</param>
    /// <param name="message">An optional human-readable message.</param>
    public RaftStorageException(RaftStorageError error, string? message = null)
        : base(message ?? error.ToString())
    {
        Error = error;
    }

    /// <summary>Gets the storage condition.</summary>
    public RaftStorageError Error { get; }
}

/// <summary>
/// The replaceable durable backing store for a Raft node: the log, hard state, and snapshots. Implementations are the
/// Raft equivalent of the raft-rs <c>Storage</c> trait — the consensus core reads from this interface only.
/// </summary>
public interface IRaftStorage
{
    /// <summary>Returns the persisted hard state and the latest committed cluster membership.</summary>
    /// <returns>The initial hard state and configuration state.</returns>
    (HardState HardState, Configuration.ConfState ConfState) InitialState();

    /// <summary>Returns the log entries in the half-open range <c>[low, high)</c>, capped by a byte budget.</summary>
    /// <param name="low">The inclusive lower bound (must exceed the compacted index).</param>
    /// <param name="high">The exclusive upper bound (must not exceed <see cref="LastIndex"/> + 1).</param>
    /// <param name="maxBytes">The soft maximum total payload size; at least one entry is always returned.</param>
    /// <returns>The requested entries.</returns>
    IReadOnlyList<Entry> Entries(ulong low, ulong high, ulong maxBytes);

    /// <summary>Returns the term of the entry at <paramref name="index"/>.</summary>
    /// <param name="index">The log index, which may equal the compacted boundary.</param>
    /// <returns>The term at <paramref name="index"/>.</returns>
    ulong Term(ulong index);

    /// <summary>Returns the index of the first entry that may still be requested via <see cref="Entries"/>.</summary>
    /// <returns>The first available index (compacted index + 1).</returns>
    ulong FirstIndex();

    /// <summary>Returns the index of the last entry in the log.</summary>
    /// <returns>The last index (the compacted index when the log is empty).</returns>
    ulong LastIndex();

    /// <summary>Returns a snapshot that includes at least <paramref name="requestIndex"/>.</summary>
    /// <param name="requestIndex">The minimum index the snapshot must cover.</param>
    /// <returns>The snapshot.</returns>
    Snapshot Snapshot(ulong requestIndex);
}
