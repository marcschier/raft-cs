// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text;
using System.Text.Json;
using Raft.Configuration;
using Raft.Messages;
using Raft.Storage;

namespace Raft.Interop.Tests;

/// <summary>
/// Drives a set of <see cref="RaftCore"/> instances through a behavioral-parity scenario (the same JSON the Rust
/// raft-rs harness consumes) and produces a normalized outcome trace: the elected leader, its term, and each node's
/// committed application commands. Uses <see cref="JsonDocument"/> so it is reflection- and AOT-clean.
/// </summary>
internal sealed class ScenarioRunner
{
    private readonly Dictionary<ulong, Node> _nodes = new();
    private readonly List<HashSet<ulong>> _partitions = new();

    private ScenarioRunner(IReadOnlyList<ulong> ids, IReadOnlyDictionary<ulong, int> electionTicks, int heartbeatTicks)
    {
        var confState = new ConfState(ids);
        foreach (ulong id in ids)
        {
            var storage = new MemoryStorage(confState);
            var config = new RaftConfig
            {
                Id = id,
                ElectionTick = electionTicks[id],
                HeartbeatTick = heartbeatTicks,
                RandomizedElectionTimeout = electionTicks[id],
            };
            _nodes[id] = new Node(new RaftCore(config, storage), storage, confState);
        }
    }

    internal static ScenarioTrace Run(string scenarioJson)
    {
        using JsonDocument document = JsonDocument.Parse(scenarioJson);
        JsonElement root = document.RootElement;

        var ids = new List<ulong>();
        foreach (JsonElement node in root.GetProperty("nodes").EnumerateArray())
        {
            ids.Add(node.GetUInt64());
        }

        var electionTicks = new Dictionary<ulong, int>();
        foreach (JsonProperty property in root.GetProperty("election_ticks").EnumerateObject())
        {
            electionTicks[ulong.Parse(property.Name)] = property.Value.GetInt32();
        }

        int heartbeatTicks = root.GetProperty("heartbeat_ticks").GetInt32();
        var runner = new ScenarioRunner(ids, electionTicks, heartbeatTicks);

        foreach (JsonElement step in root.GetProperty("steps").EnumerateArray())
        {
            runner.Execute(step);
        }

        runner.Pump();
        return runner.BuildTrace(root.GetProperty("name").GetString() ?? string.Empty);
    }

    private void Execute(JsonElement step)
    {
        string op = step.GetProperty("op").GetString() ?? string.Empty;
        switch (op)
        {
            case "campaign":
                Step(step.GetProperty("node").GetUInt64(), new Message { Type = MessageType.Hup });
                break;
            case "tick":
                Tick(step.GetProperty("node").GetUInt64(), step.GetProperty("count").GetInt32());
                break;
            case "tick_all":
                int count = step.GetProperty("count").GetInt32();
                for (int i = 0; i < count; i++)
                {
                    foreach (Node node in _nodes.Values)
                    {
                        node.Core.Tick();
                    }
                }

                break;
            case "propose":
                Propose(step.GetProperty("node").GetUInt64(), step.GetProperty("command").GetString() ?? string.Empty);
                break;
            case "deliver":
                Pump();
                break;
            case "isolate":
                Isolate(step.GetProperty("nodes"));
                break;
            case "heal":
                _partitions.Clear();
                break;
        }
    }

    private void Tick(ulong id, int count)
    {
        for (int i = 0; i < count; i++)
        {
            _nodes[id].Core.Tick();
        }
    }

    private void Step(ulong id, Message message)
    {
        message.From = id;
        _nodes[id].Core.Step(message);
    }

    private void Propose(ulong id, string command)
    {
        Step(id, new Message
        {
            Type = MessageType.Propose,
            Entries = new[] { new Entry(EntryType.Normal, 0, 0, Encoding.UTF8.GetBytes(command)) },
        });
    }

    private void Isolate(JsonElement nodes)
    {
        var isolated = new HashSet<ulong>();
        foreach (JsonElement node in nodes.EnumerateArray())
        {
            isolated.Add(node.GetUInt64());
        }

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

    private void Pump()
    {
        for (int iteration = 0; iteration < 100_000; iteration++)
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

        throw new InvalidOperationException("Scenario did not reach quiescence.");
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
    }

    private ScenarioTrace BuildTrace(string name)
    {
        ulong leader = 0;
        ulong term = 0;
        foreach (KeyValuePair<ulong, Node> node in _nodes)
        {
            if (node.Value.Core.Role == RaftRole.Leader && node.Value.Core.Term >= term)
            {
                term = node.Value.Core.Term;
                leader = node.Key;
            }
        }

        var nodes = new List<(ulong Id, IReadOnlyList<string> Committed)>();
        foreach (ulong id in SortedIds())
        {
            nodes.Add((id, _nodes[id].Committed));
        }

        return new ScenarioTrace(name, leader, term, nodes);
    }

    private List<ulong> SortedIds()
    {
        var ids = new List<ulong>(_nodes.Keys);
        ids.Sort();
        return ids;
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

/// <summary>The normalized outcome of a behavioral-parity scenario.</summary>
internal sealed class ScenarioTrace
{
    internal ScenarioTrace(
        string name,
        ulong leader,
        ulong term,
        IReadOnlyList<(ulong Id, IReadOnlyList<string> Committed)> nodes)
    {
        Name = name;
        Leader = leader;
        Term = term;
        Nodes = nodes;
    }

    internal string Name { get; }

    internal ulong Leader { get; }

    internal ulong Term { get; }

    internal IReadOnlyList<(ulong Id, IReadOnlyList<string> Committed)> Nodes { get; }
}
