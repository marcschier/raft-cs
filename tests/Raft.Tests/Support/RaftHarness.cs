// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using Raft.Configuration;
using Raft.Messages;
using Raft.Storage;

namespace Raft.Tests.Support;

/// <summary>
/// A deterministic, single-threaded harness that drives a set of <see cref="RaftCore"/> instances through a manual
/// message bus. It reproduces the <see cref="RaftNode"/> ready/advance cycle synchronously so consensus behavior can
/// be tested without timers, threads, or randomness.
/// </summary>
internal sealed class RaftHarness
{
    private readonly Dictionary<ulong, Node> _nodes = new();
    private readonly List<HashSet<ulong>> _partitions = new();

    internal RaftHarness(IEnumerable<ulong> nodeIds, Action<ulong, RaftConfig>? configure = null)
    {
        var ids = new List<ulong>(nodeIds);
        var confState = new ConfState(ids);
        foreach (ulong id in ids)
        {
            var storage = new MemoryStorage(confState);
            var config = new RaftConfig
            {
                Id = id,
                ElectionTick = 10,
                HeartbeatTick = 1,
                RandomizedElectionTimeout = 10,
            };
            configure?.Invoke(id, config);
            var core = new RaftCore(config, storage);
            _nodes[id] = new Node(core, storage, confState);
        }
    }

    internal RaftCore Core(ulong id) => _nodes[id].Core;

    internal void Compact(ulong id, ulong index)
    {
        _nodes[id].Storage.Compact(index);
    }

    internal IReadOnlyList<string> Committed(ulong id) => _nodes[id].Committed;

    internal ulong? Leader()
    {
        ulong? leader = null;
        ulong bestTerm = 0;
        foreach (KeyValuePair<ulong, Node> node in _nodes)
        {
            if (node.Value.Core.Role == RaftRole.Leader && node.Value.Core.Term >= bestTerm)
            {
                bestTerm = node.Value.Core.Term;
                leader = node.Key;
            }
        }

        return leader;
    }

    internal void Tick(ulong id)
    {
        _nodes[id].Core.Tick();
        Deliver();
    }

    internal void TickAll(int count = 1)
    {
        for (int i = 0; i < count; i++)
        {
            foreach (Node node in _nodes.Values)
            {
                node.Core.Tick();
            }
        }

        Deliver();
    }

    internal void Campaign(ulong id)
    {
        _nodes[id].Core.Step(new Message { From = id, Type = MessageType.Hup });
        Deliver();
    }

    internal void Propose(ulong id, string command)
    {
        _nodes[id].Core.Step(new Message
        {
            From = id,
            Type = MessageType.Propose,
            Entries = new[] { new Entry(EntryType.Normal, 0, 0, Encoding.UTF8.GetBytes(command)) },
        });
        Deliver();
    }

    internal void ChangeConf(ulong id, ConfChangeV2 change)
    {
        _nodes[id].Core.Step(new Message
        {
            From = id,
            Type = MessageType.Propose,
            Entries = new[] { new Entry(EntryType.ConfChangeV2, 0, 0, change.Encode()) },
        });
        Deliver();
    }

    internal void TransferLeadership(ulong from, ulong target)
    {
        _nodes[from].Core.Step(new Message { From = target, Type = MessageType.TransferLeader });
        Deliver();
    }

    internal void Isolate(params ulong[] group)
    {
        var isolated = new HashSet<ulong>(group);
        var rest = new HashSet<ulong>();
        foreach (ulong id in _nodes.Keys)
        {
            if (!isolated.Contains(id))
            {
                rest.Add(id);
            }
        }

        _partitions.Clear();
        _partitions.Add(isolated);
        _partitions.Add(rest);
    }

    internal void Heal()
    {
        _partitions.Clear();
        Deliver();
    }

    private bool CanDeliver(ulong from, ulong to)
    {
        if (_partitions.Count == 0)
        {
            return true;
        }

        foreach (HashSet<ulong> group in _partitions)
        {
            if (group.Contains(from))
            {
                return group.Contains(to);
            }
        }

        return true;
    }

    private void Deliver()
    {
        for (int iteration = 0; iteration < 10_000; iteration++)
        {
            var outbox = new List<Message>();
            foreach (Node node in _nodes.Values)
            {
                ProcessNode(node, outbox);
            }

            if (outbox.Count == 0)
            {
                return;
            }

            foreach (Message message in outbox)
            {
                if (_nodes.TryGetValue(message.To, out Node? target) && CanDeliver(message.From, message.To))
                {
                    target.Core.Step(message);
                }
            }
        }

        throw new InvalidOperationException("RaftHarness did not reach quiescence.");
    }

    private static void ProcessNode(Node node, List<Message> outbox)
    {
        RaftCore core = node.Core;

        Snapshot? snapshot = core.UnstableSnapshot();
        if (snapshot is not null && !snapshot.IsEmpty)
        {
            node.Storage.ApplySnapshot(snapshot);
            node.ConfState = snapshot.Metadata.ConfState;
            core.StableSnapshotTo(snapshot.Metadata.Index);
        }

        IReadOnlyList<Entry> unstable = core.UnstableEntries();
        if (unstable.Count > 0)
        {
            node.Storage.Append(unstable);
            Entry last = unstable[unstable.Count - 1];
            core.StableTo(last.Index, last.Term);
        }

        node.Storage.SetHardState(core.HardState);
        outbox.AddRange(core.TakeMessages());

        IReadOnlyList<Entry> toApply = core.NextEntriesToApply();
        if (toApply.Count == 0)
        {
            return;
        }

        ulong appliedTo = 0;
        foreach (Entry entry in toApply)
        {
            if (entry.Type == EntryType.Normal && !entry.IsEmpty)
            {
                node.Committed.Add(Encoding.UTF8.GetString(entry.Data.ToArray()));
            }
            else if (entry.Type is EntryType.ConfChange or EntryType.ConfChangeV2)
            {
                ConfChangeV2 change = ConfChangeV2.Decode(entry.Data.Span);
                ConfState next = Changer.Apply(node.ConfState, change);
                node.ConfState = next;
                core.ApplyConfChange(next);
                node.Storage.SetConfState(next);
            }

            appliedTo = entry.Index;
        }

        core.AppliedTo(appliedTo);

        if (core.ShouldAutoLeaveJoint())
        {
            core.Step(new Message
            {
                From = core.Id,
                Type = MessageType.Propose,
                Entries = new[]
                {
                    new Entry(EntryType.ConfChangeV2, 0, 0, ConfChangeV2.LeaveJoint().Encode()),
                },
            });
        }
    }

    private sealed class Node
    {
        internal Node(RaftCore core, MemoryStorage storage, ConfState confState)
        {
            Core = core;
            Storage = storage;
            ConfState = confState;
        }

        internal RaftCore Core { get; }

        internal MemoryStorage Storage { get; }

        internal ConfState ConfState { get; set; }

        internal List<string> Committed { get; } = new();
    }
}
