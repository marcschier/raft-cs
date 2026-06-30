// Copyright (c) marcschier. Licensed under the MIT License.

using System.Text.Json;

namespace Raft.Interop.Tests;

/// <summary>
/// Behavioral-parity tests: each scenario is run through the .NET Raft core and its normalized outcome is asserted to
/// match the golden trace produced by the Rust raft-rs harness (interop/scenarios/*.expected.json). The cluster-safety
/// invariant — every node commits an identical command sequence — is also checked directly.
/// </summary>
public sealed class ScenarioParityTests
{
    [Test]
    [Arguments("single-node-election")]
    [Arguments("three-node-election")]
    [Arguments("three-node-replicate")]
    [Arguments("leader-failover")]
    [Arguments("forward-proposal")]
    [Arguments("check-quorum-step-down")]
    public async Task Scenario_MatchesRaftRsGoldenTrace(string name)
    {
        string directory = Path.Combine(AppContext.BaseDirectory, "scenarios");
        string scenarioJson = await File.ReadAllTextAsync(Path.Combine(directory, name + ".json"));
        string goldenJson = await File.ReadAllTextAsync(Path.Combine(directory, name + ".expected.json"));

        ScenarioTrace actual = ScenarioRunner.Run(scenarioJson);

        // Cluster safety: every node commits the same sequence.
        string? reference = null;
        foreach ((ulong _, IReadOnlyList<string> committed) in actual.Nodes)
        {
            string joined = string.Join("|", committed);
            reference ??= joined;
            await Assert.That(joined).IsEqualTo(reference);
        }

        using JsonDocument golden = JsonDocument.Parse(goldenJson);
        JsonElement root = golden.RootElement;

        await Assert.That(actual.Leader).IsEqualTo(root.GetProperty("leader").GetUInt64());
        await Assert.That(actual.Term).IsEqualTo(root.GetProperty("term").GetUInt64());

        var expectedCommitted = new Dictionary<ulong, string>();
        foreach (JsonElement node in root.GetProperty("nodes").EnumerateArray())
        {
            var commands = new List<string>();
            foreach (JsonElement command in node.GetProperty("committed").EnumerateArray())
            {
                commands.Add(command.GetString() ?? string.Empty);
            }

            expectedCommitted[node.GetProperty("id").GetUInt64()] = string.Join("|", commands);
        }

        foreach ((ulong id, IReadOnlyList<string> committed) in actual.Nodes)
        {
            await Assert.That(string.Join("|", committed)).IsEqualTo(expectedCommitted[id]);
        }
    }
}
