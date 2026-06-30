// Copyright (c) marcschier. Licensed under the MIT License.

use raft::eraftpb::{Entry, EntryType, Message};
use raft::prelude::ConfState;
use raft::raw_node::{RawNode, Ready};
use raft::storage::MemStorage;
use raft::{Config, StateRole};
use serde::{Deserialize, Serialize};
use slog::{o, Discard, Logger};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

const QUIESCENCE_LIMIT: usize = 10_000;

type Result<T> = std::result::Result<T, HarnessError>;

#[derive(Debug, Error)]
enum HarnessError {
    #[error("usage: raft-rs-harness run <scenario.json> | raft-rs-harness self-test")]
    Usage,
    #[error("scenario error: {0}")]
    Scenario(String),
    #[error("raft error: {0}")]
    Raft(#[from] raft::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Deserialize)]
struct Scenario {
    name: String,
    nodes: Vec<u64>,
    election_ticks: BTreeMap<String, usize>,
    heartbeat_ticks: usize,
    #[serde(default)]
    check_quorum: bool,
    steps: Vec<Step>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Step {
    Campaign { node: u64 },
    Tick { node: u64, count: usize },
    TickAll { count: usize },
    Propose { node: u64, command: String },
    Deliver,
    Isolate { nodes: Vec<u64> },
    Heal,
}

#[derive(Debug, Serialize)]
struct Trace {
    name: String,
    leader: u64,
    term: u64,
    nodes: Vec<NodeTrace>,
}

#[derive(Debug, Serialize)]
struct NodeTrace {
    id: u64,
    committed: Vec<String>,
}

struct Harness {
    name: String,
    node_ids: Vec<u64>,
    nodes: BTreeMap<u64, RawNode<MemStorage>>,
    inboxes: BTreeMap<u64, VecDeque<Message>>,
    isolated: BTreeSet<u64>,
    committed: BTreeMap<u64, Vec<String>>,
}

impl Harness {
    fn new(scenario: &Scenario) -> Result<Self> {
        validate_scenario(scenario)?;

        let logger = Logger::root(Discard, o!());
        let mut node_ids = scenario.nodes.clone();
        node_ids.sort_unstable();

        let mut conf_state = ConfState::default();
        conf_state.voters = node_ids.clone();

        let mut nodes = BTreeMap::new();
        let mut inboxes = BTreeMap::new();
        let mut committed = BTreeMap::new();

        for id in &node_ids {
            let election_tick = election_tick_for(scenario, *id)?;
            let storage = MemStorage::new_with_conf_state(conf_state.clone());
            let config = Config {
                id: *id,
                election_tick,
                heartbeat_tick: scenario.heartbeat_ticks,
                min_election_tick: election_tick,
                max_election_tick: election_tick + 1,
                max_size_per_msg: 1_048_576,
                max_inflight_msgs: 256,
                check_quorum: scenario.check_quorum,
                pre_vote: false,
                ..Config::default()
            };
            let mut node = RawNode::new(&config, storage, &logger)?;
            node.raft.set_randomized_election_timeout(election_tick);
            nodes.insert(*id, node);
            inboxes.insert(*id, VecDeque::new());
            committed.insert(*id, Vec::new());
        }

        Ok(Self {
            name: scenario.name.clone(),
            node_ids,
            nodes,
            inboxes,
            isolated: BTreeSet::new(),
            committed,
        })
    }

    fn run(mut self, steps: &[Step]) -> Result<Trace> {
        for step in steps {
            match step {
                Step::Campaign { node } => {
                    self.node_mut(*node)?.campaign()?;
                    self.drain_readies()?;
                }
                Step::Tick { node, count } => {
                    for _ in 0..*count {
                        self.node_mut(*node)?.tick();
                        self.drain_readies()?;
                    }
                }
                Step::TickAll { count } => {
                    for _ in 0..*count {
                        for id in self.node_ids.clone() {
                            self.node_mut(id)?.tick();
                            self.drain_readies()?;
                        }
                    }
                }
                Step::Propose { node, command } => {
                    self.node_mut(*node)?
                        .propose(Vec::new(), command.as_bytes().to_vec())?;
                    self.drain_readies()?;
                }
                Step::Deliver => self.deliver_to_quiescence()?,
                Step::Isolate { nodes } => {
                    self.isolated = nodes.iter().copied().collect();
                }
                Step::Heal => self.isolated.clear(),
            }
        }

        self.deliver_to_quiescence()?;
        Ok(self.trace())
    }

    fn node_mut(&mut self, id: u64) -> Result<&mut RawNode<MemStorage>> {
        self.nodes
            .get_mut(&id)
            .ok_or_else(|| HarnessError::Scenario(format!("unknown node {id}")))
    }

    fn deliver_to_quiescence(&mut self) -> Result<()> {
        for _ in 0..QUIESCENCE_LIMIT {
            let mut made_progress = false;

            made_progress |= self.drain_readies()?;

            for id in self.node_ids.clone() {
                if self.nodes.get(&id).map_or(false, RawNode::has_ready) {
                    continue;
                }

                let message = self.inboxes.get_mut(&id).and_then(VecDeque::pop_front);
                if let Some(message) = message {
                    self.node_mut(id)?.step(message)?;
                    made_progress = true;
                }
            }

            if !made_progress {
                return Ok(());
            }
        }

        Err(HarnessError::Scenario(format!(
            "scenario '{}' did not quiesce within {QUIESCENCE_LIMIT} iterations",
            self.name
        )))
    }

    fn drain_readies(&mut self) -> Result<bool> {
        let mut made_progress = false;

        for _ in 0..QUIESCENCE_LIMIT {
            let mut found_ready = false;

            for id in self.node_ids.clone() {
                while self.nodes.get(&id).map_or(false, RawNode::has_ready) {
                    self.process_ready(id)?;
                    made_progress = true;
                    found_ready = true;
                }
            }

            if !found_ready {
                return Ok(made_progress);
            }
        }

        Err(HarnessError::Scenario(format!(
            "scenario '{}' produced readies without quiescing within {QUIESCENCE_LIMIT} iterations",
            self.name
        )))
    }

    fn process_ready(&mut self, id: u64) -> Result<()> {
        let mut ready = self.node_mut(id)?.ready();
        self.persist_ready(id, &ready)?;
        self.record_entries(id, ready.committed_entries());
        self.enqueue_messages(ready.take_messages());
        self.enqueue_messages(ready.take_persisted_messages());

        let mut light = self.node_mut(id)?.advance(ready);
        self.record_entries(id, light.committed_entries());
        self.enqueue_messages(light.take_messages());
        self.node_mut(id)?.advance_apply();
        Ok(())
    }

    fn persist_ready(&mut self, id: u64, ready: &Ready) -> Result<()> {
        let store = self.node_mut(id)?.mut_store().clone();
        if !ready.snapshot().is_empty() {
            store.wl().apply_snapshot(ready.snapshot().clone())?;
        }

        store.wl().append(ready.entries())?;

        if let Some(hard_state) = ready.hs() {
            store.wl().set_hardstate(hard_state.clone());
        }

        Ok(())
    }

    fn enqueue_messages<I>(&mut self, messages: I)
    where
        I: IntoIterator<Item = Message>,
    {
        for message in messages {
            if self.should_drop(&message) {
                continue;
            }

            if let Some(inbox) = self.inboxes.get_mut(&message.to) {
                inbox.push_back(message);
            }
        }
    }

    fn should_drop(&self, message: &Message) -> bool {
        let from_isolated = self.isolated.contains(&message.from);
        let to_isolated = self.isolated.contains(&message.to);
        from_isolated != to_isolated
    }

    fn record_entries(&mut self, id: u64, entries: &[Entry]) {
        let target = self.committed.entry(id).or_default();
        for entry in entries {
            if entry.get_entry_type() != EntryType::EntryNormal || entry.get_data().is_empty() {
                continue;
            }

            target.push(String::from_utf8_lossy(entry.get_data()).into_owned());
        }
    }

    fn trace(&self) -> Trace {
        let mut leader = 0;
        let mut term = 0;

        for id in &self.node_ids {
            let raft = &self.nodes[id].raft;
            if raft.term > term {
                term = raft.term;
            }

            if raft.state == StateRole::Leader && (leader == 0 || raft.term >= term) {
                leader = *id;
                term = raft.term;
            }
        }

        Trace {
            name: self.name.clone(),
            leader,
            term,
            nodes: self
                .node_ids
                .iter()
                .map(|id| NodeTrace {
                    id: *id,
                    committed: self.committed.get(id).cloned().unwrap_or_default(),
                })
                .collect(),
        }
    }
}

fn validate_scenario(scenario: &Scenario) -> Result<()> {
    if scenario.name.trim().is_empty() {
        return Err(HarnessError::Scenario("name must not be empty".to_owned()));
    }

    if scenario.nodes.is_empty() {
        return Err(HarnessError::Scenario("nodes must not be empty".to_owned()));
    }

    let unique: BTreeSet<u64> = scenario.nodes.iter().copied().collect();
    if unique.len() != scenario.nodes.len() || unique.contains(&0) {
        return Err(HarnessError::Scenario(
            "nodes must contain unique non-zero ids".to_owned(),
        ));
    }

    for id in &scenario.nodes {
        let election_tick = election_tick_for(scenario, *id)?;
        if election_tick <= scenario.heartbeat_ticks {
            return Err(HarnessError::Scenario(format!(
                "node {id} election tick {election_tick} must be greater than heartbeat tick {}",
                scenario.heartbeat_ticks
            )));
        }
    }

    Ok(())
}

fn election_tick_for(scenario: &Scenario, id: u64) -> Result<usize> {
    scenario
        .election_ticks
        .get(&id.to_string())
        .copied()
        .ok_or_else(|| HarnessError::Scenario(format!("missing election tick for node {id}")))
}

fn read_scenario(path: &Path) -> Result<Scenario> {
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

fn run_scenario_file(path: &Path) -> Result<Trace> {
    let scenario = read_scenario(path)?;
    Harness::new(&scenario)?.run(&scenario.steps)
}

fn scenario_dir() -> Result<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .map(|path| path.join("scenarios"))
        .ok_or_else(|| HarnessError::Scenario("cannot locate interop/scenarios".to_owned()))
}

fn write_expected(trace: &Trace, scenarios_dir: &Path) -> Result<()> {
    let expected_path = scenarios_dir.join(format!("{}.expected.json", trace.name));
    let json = serde_json::to_string_pretty(trace)?;
    fs::write(expected_path, format!("{json}\n"))?;
    Ok(())
}

fn self_test() -> Result<()> {
    let scenarios_dir = scenario_dir()?;
    let mut scenario_paths = Vec::new();

    for entry in fs::read_dir(&scenarios_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }

        let file_name = path.file_name().and_then(|value| value.to_str()).unwrap_or_default();
        if file_name == "schema.json" || file_name.ends_with(".expected.json") {
            continue;
        }

        scenario_paths.push(path);
    }

    scenario_paths.sort();
    let mut failed = false;

    for path in scenario_paths {
        match run_scenario_file(&path) {
            Ok(trace) => {
                let invariant = committed_lists_identical(&trace);
                write_expected(&trace, &scenarios_dir)?;
                println!(
                    "{} {}",
                    if invariant { "PASS" } else { "FAIL" },
                    trace.name
                );
                println!("{}", serde_json::to_string(&trace)?);
                failed |= !invariant;
            }
            Err(error) => {
                failed = true;
                println!("FAIL {}: {error}", path.display());
            }
        }
    }

    if failed {
        return Err(HarnessError::Scenario(
            "one or more scenarios failed self-test".to_owned(),
        ));
    }

    Ok(())
}

fn committed_lists_identical(trace: &Trace) -> bool {
    let Some(first) = trace.nodes.first() else {
        return true;
    };

    trace
        .nodes
        .iter()
        .all(|node| node.committed == first.committed)
}

fn real_main() -> Result<()> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("run") => {
            let path = args.next().ok_or(HarnessError::Usage)?;
            if args.next().is_some() {
                return Err(HarnessError::Usage);
            }

            let trace = run_scenario_file(Path::new(&path))?;
            println!("{}", serde_json::to_string_pretty(&trace)?);
            Ok(())
        }
        Some("self-test") => {
            if args.next().is_some() {
                return Err(HarnessError::Usage);
            }

            self_test()
        }
        _ => Err(HarnessError::Usage),
    }
}

fn main() {
    if let Err(error) = real_main() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
