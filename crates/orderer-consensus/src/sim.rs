//! Deterministic cluster simulator (`spec/REPLICATION.md` §10.2).
//!
//! A single thread drives N [`Node`]s over a simulated network and disk with
//! a seeded PRNG controlling message loss, duplication, reordering and
//! delay, network partitions, node crashes with torn disk tails, and slow
//! fsync. After every step it checks the protocol's safety properties. A
//! failure returns the seed and tick, and re-running that seed reproduces it
//! exactly.

use crate::core::*;
use crate::rng::Rng;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub nodes: usize,
    /// Ticks with faults and proposals.
    pub chaos_ticks: u64,
    /// Fault-free ticks afterwards, in which the cluster must converge.
    pub heal_ticks: u64,
    pub drop_permille: u32,
    pub dup_permille: u32,
    pub max_delay: u64,
    pub fsync_max: u64,
    /// Per tick, chance that one running node crashes.
    pub crash_permille: u32,
    /// Per tick, chance of a client proposal.
    pub propose_permille: u32,
    /// Network partitions are reshuffled this often (0 = never).
    pub partition_every: u64,
    pub node_cfg: Config,
}

impl Default for SimConfig {
    fn default() -> Self {
        SimConfig {
            nodes: 3,
            chaos_ticks: 3000,
            heal_ticks: 1500,
            drop_permille: 100,
            dup_permille: 30,
            max_delay: 6,
            fsync_max: 4,
            crash_permille: 4,
            propose_permille: 300,
            partition_every: 150,
            node_cfg: Config::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Report {
    pub committed: usize,
    pub elections: u64,
    pub crashes: u64,
    pub proposals: u64,
    pub max_term: Term,
}

struct SimDisk {
    disk: Disk,
    pending: VecDeque<(u64, Persist)>,
    last_done: u64,
}

struct Sim {
    cfg: SimConfig,
    seed: u64,
    rng: Rng,
    now: u64,
    nodes: Vec<Option<Node>>,
    disks: Vec<SimDisk>,
    down_until: Vec<u64>,
    net: BTreeMap<(u64, u64), (NodeId, NodeId, Msg)>,
    net_seq: u64,
    /// `blocked[from][to]`
    blocked: Vec<Vec<bool>>,
    /// The agreed sequence of committed entries, built from every `Apply`.
    global: Vec<Entry>,
    seen: HashSet<u64>,
    proposed: HashSet<u64>,
    applied_len: Vec<Index>,
    leaders: HashMap<Term, NodeId>,
    next_id: u64,
    chaos: bool,
    report: Report,
}

type R = Result<(), String>;

/// Run one seeded scenario. `Err` carries the violated property.
pub fn run(seed: u64, cfg: &SimConfig) -> Result<Report, String> {
    let n = cfg.nodes;
    let peers: Vec<NodeId> = (0..n as NodeId).collect();
    let mut sim = Sim {
        cfg: cfg.clone(),
        seed,
        rng: Rng::new(seed),
        now: 0,
        nodes: Vec::new(),
        disks: (0..n)
            .map(|_| SimDisk {
                disk: Disk::default(),
                pending: VecDeque::new(),
                last_done: 0,
            })
            .collect(),
        down_until: vec![0; n],
        net: BTreeMap::new(),
        net_seq: 0,
        blocked: vec![vec![false; n]; n],
        global: Vec::new(),
        seen: HashSet::new(),
        proposed: HashSet::new(),
        applied_len: vec![0; n],
        leaders: HashMap::new(),
        next_id: 1,
        chaos: true,
        report: Report::default(),
    };
    for i in 0..n {
        let node = Node::new(
            i as NodeId,
            peers.clone(),
            cfg.node_cfg.clone(),
            seed.wrapping_add(i as u64),
        );
        sim.nodes.push(Some(node));
    }

    for _ in 0..cfg.chaos_ticks {
        sim.step().map_err(|e| sim.fail(&e))?;
    }
    let chaos_committed = sim.global.len();

    // Heal: no faults, everything restarted, fully connected.
    sim.chaos = false;
    sim.blocked = vec![vec![false; n]; n];
    for i in 0..n {
        if sim.nodes[i].is_none() {
            sim.restart(i);
        }
    }
    let quiet_from = sim.now + cfg.heal_ticks.saturating_sub(400);
    for _ in 0..cfg.heal_ticks {
        let propose = sim.now < quiet_from;
        sim.step_with(propose).map_err(|e| sim.fail(&e))?;
    }

    sim.converged(chaos_committed).map_err(|e| sim.fail(&e))?;
    sim.report.committed = sim.global.len();
    Ok(sim.report)
}

impl Sim {
    fn fail(&self, msg: &str) -> String {
        format!("seed {} tick {}: {}", self.seed, self.now, msg)
    }

    fn step(&mut self) -> R {
        self.step_with(true)
    }

    fn step_with(&mut self, propose: bool) -> R {
        self.now += 1;
        if self.chaos {
            self.inject_faults();
        }
        self.restart_due();
        self.complete_disk()?;
        self.deliver_net()?;
        for i in 0..self.nodes.len() {
            if let Some(n) = self.nodes[i].as_mut() {
                n.tick();
                self.drain(i)?;
            }
        }
        if propose && self.rng.permille(self.cfg.propose_permille) {
            self.propose();
        }
        self.check()
    }

    // ---- faults ----------------------------------------------------------

    fn inject_faults(&mut self) {
        if self.cfg.partition_every > 0 && self.now % self.cfg.partition_every == 0 {
            self.reshuffle_partition();
        }
        if self.rng.permille(self.cfg.crash_permille) {
            let up: Vec<usize> = (0..self.nodes.len())
                .filter(|&i| self.nodes[i].is_some())
                .collect();
            if !up.is_empty() {
                let i = up[self.rng.below(up.len() as u64) as usize];
                self.crash(i);
            }
        }
    }

    fn reshuffle_partition(&mut self) {
        let n = self.nodes.len();
        self.blocked = vec![vec![false; n]; n];
        // One time in three the network is whole; otherwise cut a random side.
        if self.rng.below(3) == 0 {
            return;
        }
        let side: Vec<bool> = (0..n).map(|_| self.rng.below(2) == 0).collect();
        for a in 0..n {
            for b in 0..n {
                if side[a] != side[b] {
                    self.blocked[a][b] = true;
                }
            }
        }
    }

    fn crash(&mut self, i: usize) {
        self.report.crashes += 1;
        self.nodes[i] = None;
        self.applied_len[i] = 0;
        // Writes are atomic and in order, so a crash leaves a prefix of the
        // ones in flight: that is the torn tail.
        let pending: Vec<_> = self.disks[i].pending.drain(..).collect();
        let keep = self.rng.below(pending.len() as u64 + 1) as usize;
        for (_, p) in pending.into_iter().take(keep) {
            self.disks[i].disk.apply(&p);
        }
        self.down_until[i] = self.now + 5 + self.rng.below(80);
    }

    fn restart_due(&mut self) {
        for i in 0..self.nodes.len() {
            if self.nodes[i].is_none() && self.down_until[i] <= self.now {
                self.restart(i);
            }
        }
    }

    fn restart(&mut self, i: usize) {
        let peers: Vec<NodeId> = (0..self.nodes.len() as NodeId).collect();
        let seed = self.rng.next_u64();
        self.disks[i].last_done = self.now;
        self.nodes[i] = Some(Node::restart(
            i as NodeId,
            peers,
            self.cfg.node_cfg.clone(),
            seed,
            self.disks[i].disk.clone(),
        ));
    }

    // ---- environment -----------------------------------------------------

    fn complete_disk(&mut self) -> R {
        for i in 0..self.nodes.len() {
            if self.nodes[i].is_none() {
                continue;
            }
            while self.disks[i]
                .pending
                .front()
                .is_some_and(|&(t, _)| t <= self.now)
            {
                let (_, p) = self.disks[i]
                    .pending
                    .pop_front()
                    .expect("checked non-empty");
                self.disks[i].disk.apply(&p);
                if let Some(n) = self.nodes[i].as_mut() {
                    n.persisted(p.token);
                }
                self.drain(i)?;
            }
        }
        Ok(())
    }

    fn deliver_net(&mut self) -> R {
        loop {
            let key = match self.net.keys().next() {
                Some(&k) if k.0 <= self.now => k,
                _ => return Ok(()),
            };
            let (from, to, msg) = self.net.remove(&key).expect("key just seen");
            let t = to as usize;
            if let Some(n) = self.nodes[t].as_mut() {
                n.step(from, msg);
                self.drain(t)?;
            }
        }
    }

    fn send(&mut self, from: NodeId, to: NodeId, msg: Msg) {
        if self.blocked[from as usize][to as usize] {
            return;
        }
        if self.chaos && self.rng.permille(self.cfg.drop_permille) {
            return;
        }
        let copies = if self.chaos && self.rng.permille(self.cfg.dup_permille) {
            2
        } else {
            1
        };
        for _ in 0..copies {
            let at = self.now + 1 + self.rng.below(self.cfg.max_delay);
            self.net_seq += 1;
            self.net.insert((at, self.net_seq), (from, to, msg.clone()));
        }
    }

    fn drain(&mut self, i: usize) -> R {
        let effects = match self.nodes[i].as_mut() {
            Some(n) => n.take_effects(),
            None => return Ok(()),
        };
        for e in effects {
            match e {
                Effect::Send { to, msg } => self.send(i as NodeId, to, msg),
                Effect::Persist(p) => {
                    let d = &mut self.disks[i];
                    let done = d
                        .last_done
                        .max(self.now + 1 + self.rng.below(self.cfg.fsync_max));
                    d.last_done = done;
                    d.pending.push_back((done, p));
                }
                Effect::Apply { index, entry } => self.on_apply(i, index, entry)?,
                Effect::Rejected { .. } => {}
            }
        }
        Ok(())
    }

    fn propose(&mut self) {
        let up: Vec<usize> = (0..self.nodes.len())
            .filter(|&i| self.nodes[i].is_some())
            .collect();
        if up.is_empty() {
            return;
        }
        let i = up[self.rng.below(up.len() as u64) as usize];
        let id = self.next_id;
        self.next_id += 1;
        // Remember what was offered; it is only an error if something never
        // offered gets committed.
        self.proposed.insert(id);
        self.report.proposals += 1;
        if let Some(n) = self.nodes[i].as_mut() {
            n.propose(id.to_le_bytes().to_vec());
        }
        let _ = self.drain(i);
    }

    // ---- checks ----------------------------------------------------------

    fn on_apply(&mut self, i: usize, index: Index, entry: Entry) -> R {
        if index != self.applied_len[i] + 1 {
            return Err(format!(
                "node {i} applied index {index} after {}",
                self.applied_len[i]
            ));
        }
        self.applied_len[i] = index;
        match self.global.get(index as usize - 1) {
            Some(g) if *g == entry => {}
            Some(g) => {
                return Err(format!(
                    "committed entry {index} differs: node {i} applied {entry:?}, earlier {g:?}"
                ))
            }
            None => {
                if index as usize != self.global.len() + 1 {
                    return Err(format!(
                        "node {i} applied {index}, skipping ahead of {}",
                        self.global.len()
                    ));
                }
                if !entry.data.is_empty() {
                    let id =
                        u64::from_le_bytes(entry.data[..8].try_into().map_err(|_| "bad payload")?);
                    if !self.proposed.contains(&id) {
                        return Err(format!("committed entry {index} was never proposed"));
                    }
                    if !self.seen.insert(id) {
                        return Err(format!("command {id} committed twice"));
                    }
                }
                self.global.push(entry);
            }
        }
        Ok(())
    }

    fn check(&mut self) -> R {
        for i in 0..self.nodes.len() {
            let Some(n) = self.nodes[i].as_ref() else {
                continue;
            };
            if let Some(f) = n.fault() {
                return Err(format!("node {i}: {f}"));
            }
            if n.role() != Role::Leader {
                continue;
            }
            match self.leaders.get(&n.term()) {
                Some(&l) if l as usize != i => {
                    return Err(format!("two leaders in term {}: {l} and {i}", n.term()));
                }
                Some(_) => {}
                None => {
                    // Leader completeness: a new leader holds every entry
                    // already committed.
                    let log = n.log();
                    if log.len() < self.global.len() || log[..self.global.len()] != self.global[..]
                    {
                        return Err(format!(
                            "leader {i} of term {} lacks committed entries (log {}, committed {})",
                            n.term(),
                            log.len(),
                            self.global.len()
                        ));
                    }
                    self.leaders.insert(n.term(), i as NodeId);
                    self.report.elections += 1;
                    self.report.max_term = self.report.max_term.max(n.term());
                }
            }
        }
        if self.now % 25 == 0 {
            self.log_matching()?;
        }
        Ok(())
    }

    fn log_matching(&self) -> R {
        let logs: Vec<(usize, &[Entry])> = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(i, n)| n.as_ref().map(|n| (i, n.log())))
            .collect();
        for (x, &(a, la)) in logs.iter().enumerate() {
            for &(b, lb) in &logs[x + 1..] {
                let m = la.len().min(lb.len());
                if let Some(k) = (0..m).rev().find(|&k| la[k].term == lb[k].term) {
                    if la[..=k] != lb[..=k] {
                        return Err(format!(
                            "log matching violated between nodes {a} and {b} at index {}",
                            k + 1
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn converged(&self, committed_before: usize) -> R {
        if !self
            .nodes
            .iter()
            .flatten()
            .any(|n| n.role() == Role::Leader)
        {
            return Err("no leader after healing".into());
        }
        if self.global.len() <= committed_before {
            return Err(format!(
                "no progress after healing: committed {} before, {} after",
                committed_before,
                self.global.len()
            ));
        }
        for (i, &len) in self.applied_len.iter().enumerate() {
            if len as usize != self.global.len() {
                return Err(format!(
                    "node {i} applied {len} of {} committed entries after healing",
                    self.global.len()
                ));
            }
        }
        Ok(())
    }
}
