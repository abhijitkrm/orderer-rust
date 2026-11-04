//! Shared harness: real replicas over real TCP and real files.
#![allow(dead_code)]

use orderer_cluster::sha256::sha256;
use orderer_cluster::{ProposeError, Replica, ReplicaConfig, ReplicaHandle, StateMachine};
use orderer_consensus::{Config, Entry, Index, NodeId, Role};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

type Applied = Arc<Mutex<Vec<(Index, Vec<u8>)>>>;

/// Records every applied non-empty entry; snapshots are its serialization.
#[derive(Clone, Default)]
pub struct Recorder {
    pub applied: Applied,
    /// Corrupt the state at this index (to provoke divergence).
    pub corrupt_at: Option<Index>,
}

impl Recorder {
    pub fn payloads(&self) -> Vec<Vec<u8>> {
        self.applied
            .lock()
            .unwrap()
            .iter()
            .map(|(_, d)| d.clone())
            .collect()
    }
}

fn serialize(v: &[(Index, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, d) in v {
        out.extend_from_slice(&i.to_le_bytes());
        out.extend_from_slice(&(d.len() as u32).to_le_bytes());
        out.extend_from_slice(d);
    }
    out
}

impl StateMachine for Recorder {
    fn apply(&mut self, index: Index, entry: &Entry) {
        if entry.data.is_empty() {
            return;
        }
        let mut data = entry.data.clone();
        if self.corrupt_at == Some(index) {
            data[0] ^= 0xff;
        }
        self.applied.lock().unwrap().push((index, data));
    }
    fn snapshot(&mut self, _index: Index) -> Vec<u8> {
        serialize(&self.applied.lock().unwrap())
    }
    fn restore(&mut self, _index: Index, data: &[u8]) -> Result<(), String> {
        let mut v = Vec::new();
        let mut at = 0;
        while at < data.len() {
            let i = u64::from_le_bytes(data[at..at + 8].try_into().unwrap());
            let n = u32::from_le_bytes(data[at + 8..at + 12].try_into().unwrap()) as usize;
            v.push((i, data[at + 12..at + 12 + n].to_vec()));
            at += 12 + n;
        }
        *self.applied.lock().unwrap() = v;
        Ok(())
    }
    fn digest(&mut self, _index: Index) -> Option<[u8; 32]> {
        Some(sha256(&serialize(&self.applied.lock().unwrap())))
    }
}

pub fn free_addrs(n: usize) -> Vec<SocketAddr> {
    let ls: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    ls.iter().map(|l| l.local_addr().unwrap()).collect()
}

pub struct Cluster {
    pub dir: PathBuf,
    pub members: Vec<(NodeId, SocketAddr)>,
    pub nodes: Vec<Option<Replica>>,
    pub recorders: Vec<Recorder>,
    pub tune: Arc<dyn Fn(&mut ReplicaConfig) + Send + Sync>,
    /// Wrap every replica's state machine in `DedupSm`.
    pub dedup: bool,
}

pub fn temp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("orderer-cluster-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

impl Cluster {
    pub fn new(
        name: &str,
        n: usize,
        tune: impl Fn(&mut ReplicaConfig) + Send + Sync + 'static,
    ) -> Cluster {
        Cluster::build(name, n, tune, false)
    }

    pub fn with_dedup(name: &str, n: usize) -> Cluster {
        Cluster::build(name, n, |_| {}, true)
    }

    fn build(
        name: &str,
        n: usize,
        tune: impl Fn(&mut ReplicaConfig) + Send + Sync + 'static,
        dedup: bool,
    ) -> Cluster {
        let members = free_addrs(n)
            .into_iter()
            .enumerate()
            .map(|(i, a)| (i as NodeId, a))
            .collect();
        let mut c = Cluster {
            dir: temp(name),
            members,
            nodes: (0..n).map(|_| None).collect(),
            recorders: (0..n).map(|_| Recorder::default()).collect(),
            tune: Arc::new(tune),
            dedup,
        };
        for i in 0..n {
            c.start(i);
        }
        c
    }

    pub fn config(&self, i: usize) -> ReplicaConfig {
        let mut cfg = ReplicaConfig::new(
            i as NodeId,
            self.members.clone(),
            self.dir.join(format!("n{i}")),
        );
        cfg.tick = Duration::from_millis(5);
        cfg.consensus = Config {
            election_ticks: 20,
            heartbeat_ticks: 2,
            max_batch: 256,
            ..Config::default()
        };
        cfg.fsync = false;
        cfg.propose_timeout = Duration::from_secs(3);
        (self.tune)(&mut cfg);
        cfg
    }

    pub fn start(&mut self, i: usize) {
        self.start_with(i, Recorder::default());
    }

    pub fn start_with(&mut self, i: usize, rec: Recorder) {
        assert!(self.nodes[i].is_none());
        self.recorders[i] = rec.clone();
        let cfg = self.config(i);
        let started = if self.dedup {
            Replica::start(cfg, orderer_cluster::dedup::DedupSm::new(rec))
        } else {
            Replica::start(cfg, rec)
        };
        self.nodes[i] = Some(started.unwrap_or_else(|e| panic!("start {i}: {e}")));
    }

    pub fn handle(&self, i: usize) -> Option<ReplicaHandle> {
        self.nodes[i].as_ref().map(|r| r.handle())
    }

    pub fn crash(&mut self, i: usize) {
        self.nodes[i].take().expect("node running").crash();
    }

    pub fn stop(&mut self, i: usize) {
        self.nodes[i].take().expect("node running").stop();
    }

    pub fn leader(&self) -> Option<usize> {
        let mut best: Option<(u32, usize)> = None;
        for (i, n) in self.nodes.iter().enumerate() {
            if let Some(n) = n {
                let s = n.handle().status();
                if s.role == Role::Leader && best.map_or(true, |(t, _)| s.term > t) {
                    best = Some((s.term, i));
                }
            }
        }
        best.map(|b| b.1)
    }

    pub fn wait_leader(&self, within: Duration) -> usize {
        let end = Instant::now() + within;
        loop {
            if let Some(l) = self.leader() {
                return l;
            }
            assert!(Instant::now() < end, "no leader within {within:?}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Propose via whichever node is leader, retrying on redirects. Returns
    /// the committed index. `Err(Timeout)` is returned as is: the caller
    /// must treat that outcome as unknown.
    pub fn propose(&self, payload: &[u8]) -> Result<Index, ProposeError> {
        let end = Instant::now() + Duration::from_secs(10);
        loop {
            let l = match self.leader() {
                Some(l) => l,
                None => {
                    assert!(Instant::now() < end, "no leader to propose to");
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
            };
            match self.handle(l).unwrap().propose(payload.to_vec()) {
                Err(ProposeError::NotLeader(_)) | Err(ProposeError::Busy) => {
                    assert!(Instant::now() < end, "proposal kept being redirected");
                    std::thread::sleep(Duration::from_millis(5));
                }
                other => return other,
            }
        }
    }

    pub fn wait_applied(&self, nodes: &[usize], at_least: usize, within: Duration) {
        let end = Instant::now() + within;
        loop {
            let ok = nodes
                .iter()
                .all(|&i| self.recorders[i].applied.lock().unwrap().len() >= at_least);
            if ok {
                return;
            }
            assert!(
                Instant::now() < end,
                "nodes {nodes:?} did not apply {at_least}: {:?}",
                nodes
                    .iter()
                    .map(|&i| self.recorders[i].applied.lock().unwrap().len())
                    .collect::<Vec<_>>()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Wait until every running node has applied the same, stable sequence.
    pub fn wait_converged(&self, within: Duration) -> Vec<Vec<u8>> {
        let end = Instant::now() + within;
        loop {
            let seqs: Vec<Vec<Vec<u8>>> = self
                .nodes
                .iter()
                .enumerate()
                .filter(|(_, n)| n.is_some())
                .map(|(i, _)| self.recorders[i].payloads())
                .collect();
            if seqs.windows(2).all(|w| w[0] == w[1]) {
                // Stable for a moment, to rule out an in-flight commit.
                std::thread::sleep(Duration::from_millis(100));
                let again: Vec<Vec<Vec<u8>>> = self
                    .nodes
                    .iter()
                    .enumerate()
                    .filter(|(_, n)| n.is_some())
                    .map(|(i, _)| self.recorders[i].payloads())
                    .collect();
                if again == seqs {
                    return seqs.into_iter().next().unwrap_or_default();
                }
            }
            assert!(Instant::now() < end, "replicas did not converge");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in self.nodes.iter_mut() {
            if let Some(n) = n.take() {
                n.crash();
            }
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
