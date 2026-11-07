//! orderer-node — one replica of a replicated orderer cluster, as a process.
//!
//!   orderer-node --id N --members 0=HOST:PORT,1=HOST:PORT,... --client HOST:PORT
//!                --dir DIR [--partitions P] [--compact-every N] [--digest-every N]
//!                [--fsync on|off|relaxed[:MS]] [--audit] [--sm pipeline|recorder]
//!
//! `--audit` appends `(log index, first order id)` for every applied batch to
//! `DIR/applied.log` (truncated at start, so it reflects this incarnation's
//! replay), which `orderer-load verify` checks after a failover run.

use orderer::{Metrics, PartitionMap, Pipeline};
use orderer_cluster::client::ClientServer;
use orderer_cluster::codec::decode_batch;
use orderer_cluster::pipeline_sm::PipelineSm;
use orderer_cluster::{Replica, ReplicaConfig, StateMachine};
use orderer_consensus::{Entry, Index, NodeId};
use orderer_core::*;
use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::process::exit;

fn die(msg: &str) -> ! {
    eprintln!("orderer-node: {msg}");
    exit(2)
}

struct Audited<S: StateMachine> {
    inner: S,
    log: Option<File>,
}

impl<S: StateMachine> StateMachine for Audited<S> {
    fn apply(&mut self, index: Index, entry: &Entry) {
        if let Some(f) = self.log.as_mut() {
            if let Ok(cmds) = decode_batch(&entry.data) {
                if let Some((_, c)) = cmds.first() {
                    let id = match c {
                        Command::New { order_id, .. }
                        | Command::Cancel { order_id }
                        | Command::Replace { order_id, .. } => *order_id,
                    };
                    let mut rec = [0u8; 16];
                    rec[..8].copy_from_slice(&index.to_le_bytes());
                    rec[8..].copy_from_slice(&id.to_le_bytes());
                    // Straight to the OS: survives kill -9 of this process.
                    let _ = f.write_all(&rec);
                }
            }
        }
        self.inner.apply(index, entry);
    }
    fn snapshot(&mut self, index: Index) -> Vec<u8> {
        self.inner.snapshot(index)
    }
    fn restore(&mut self, index: Index, data: &[u8]) -> Result<(), String> {
        self.inner.restore(index, data)
    }
    fn digest(&mut self, index: Index) -> Option<[u8; 32]> {
        self.inner.digest(index)
    }
    fn deep_digest(&mut self, index: Index) -> Option<[u8; 32]> {
        self.inner.deep_digest(index)
    }
}

fn main() {
    let (mut id, mut members, mut client, mut dir) = (
        None::<NodeId>,
        Vec::new(),
        None::<SocketAddr>,
        String::new(),
    );
    let (mut partitions, mut compact_every, mut digest_every, mut fsync, mut audit) =
        (2u32, 0u64, 0u64, true, false);
    let mut fsync_interval: Option<std::time::Duration> = None;
    let mut sm_kind = String::from("pipeline");
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut v = |n: &str| {
            args.next()
                .unwrap_or_else(|| die(&format!("{n} needs a value")))
        };
        match a.as_str() {
            "--id" => id = Some(v("--id").parse().unwrap_or_else(|_| die("--id"))),
            "--members" => {
                for m in v("--members").split(',') {
                    let (i, addr) = m
                        .split_once('=')
                        .unwrap_or_else(|| die("--members wants ID=HOST:PORT,..."));
                    members.push((
                        i.parse::<NodeId>().unwrap_or_else(|_| die("member id")),
                        addr.parse::<SocketAddr>()
                            .unwrap_or_else(|_| die("member address")),
                    ));
                }
            }
            "--client" => client = Some(v("--client").parse().unwrap_or_else(|_| die("--client"))),
            "--dir" => dir = v("--dir"),
            "--partitions" => {
                partitions = v("--partitions")
                    .parse()
                    .unwrap_or_else(|_| die("--partitions"))
            }
            "--compact-every" => {
                compact_every = v("--compact-every")
                    .parse()
                    .unwrap_or_else(|_| die("--compact-every"))
            }
            "--digest-every" => {
                digest_every = v("--digest-every")
                    .parse()
                    .unwrap_or_else(|_| die("--digest-every"))
            }
            "--fsync" => {
                let m = v("--fsync");
                match m.as_str() {
                    "on" | "off" => fsync = m == "on",
                    _ => match m.strip_prefix("relaxed") {
                        // Commits wait for the write, not the flush; a background
                        // thread flushes about every MS (default 5).
                        Some(rest) => {
                            let ms = match rest {
                                "" => 5,
                                r => r
                                    .strip_prefix(':')
                                    .and_then(|x| x.parse::<u64>().ok())
                                    .unwrap_or_else(|| {
                                        die("--fsync relaxed:MS wants milliseconds")
                                    }),
                            };
                            fsync = true;
                            fsync_interval = Some(std::time::Duration::from_millis(ms));
                        }
                        None => die("--fsync wants on, off or relaxed[:MS]"),
                    },
                }
            }
            "--audit" => audit = true,
            "--sm" => sm_kind = v("--sm"),
            other => die(&format!("unknown argument {other}")),
        }
    }
    let id = id.unwrap_or_else(|| die("--id is required"));
    let client = client.unwrap_or_else(|| die("--client is required"));
    if members.is_empty() || dir.is_empty() {
        die("--members and --dir are required");
    }
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| die(&format!("{dir}: {e}")));

    let mut cfg = ReplicaConfig::new(id, members, format!("{dir}/state"));
    cfg.fsync = fsync;
    cfg.fsync_interval = fsync_interval;
    cfg.compact_every = compact_every;
    cfg.digest_every = digest_every;
    cfg.max_pending = 1 << 16;
    // Pre-shared cluster key from the environment (not argv, which `ps` shows).
    let key = std::env::var("ORDERER_KEY")
        .ok()
        .filter(|k| !k.is_empty())
        .map(String::into_bytes);
    cfg.auth_key = key.clone();
    let log = audit.then(|| {
        File::create(format!("{dir}/applied.log"))
            .unwrap_or_else(|e| die(&format!("audit log: {e}")))
    });
    let inner: Box<dyn StateMachine> = match sm_kind.as_str() {
        "recorder" => Box::new(orderer_cluster::recorder::RecorderSm::new()),
        "pipeline" => Box::new(PipelineSm::<FifoCore>::new(
            BookConfig::default(),
            PartitionMap::hash(partitions).unwrap_or_else(|e| die(&e.to_string())),
            move |init, digest| {
                let (c, _h) = Metrics::new(0);
                let mut b = Pipeline::<FifoCore>::builder()
                    .partitions(partitions)
                    .egress(c)
                    .egress(digest)
                    .timestamps(false);
                if let Some(i) = init {
                    b = b.initial(i);
                }
                b.build().expect("pipeline")
            },
        )),
        other => die(&format!("--sm {other}: want pipeline or recorder")),
    };
    let sm = orderer_cluster::dedup::DedupSm::new(Audited { inner, log });
    let replica = Replica::start(cfg, sm).unwrap_or_else(|e| die(&format!("start: {e}")));
    let _server = ClientServer::start_with_key(client, replica.handle(), key)
        .unwrap_or_else(|e| die(&format!("client port: {e}")));
    println!("orderer-node {id} ready (client port {client})");
    loop {
        std::thread::sleep(std::time::Duration::from_secs(3600));
    }
}
