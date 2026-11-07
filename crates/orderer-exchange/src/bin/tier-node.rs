//! tier-node: one replica of the matching group (`MatchingTierSm`) with its
//! client port (status, state hash) and exchange port (forward, cursor, read).
//!
//!   tier-node --id N --members 0=HOST:PORT,... --client HOST:PORT --exchange HOST:PORT
//!             --dir DIR [--partitions P] [--source-base B] [--retain N]
//!             [--compact-every N] [--digest-every N] [--fsync on|off|relaxed[:MS]]
//!
//! The cluster key, if any, is read from `ORDERER_KEY` (not argv). The exchange
//! port is unauthenticated: private network only.

use orderer::{PartitionMap, Pipeline};
use orderer_cluster::client::ClientServer;
use orderer_cluster::{Replica, ReplicaConfig};
use orderer_consensus::NodeId;
use orderer_core::{BookConfig, FifoCore};
use orderer_exchange::{ExchangeServer, MatchingTierSm, TierConfig};
use std::net::SocketAddr;
use std::process::exit;
use std::time::Duration;

fn die(msg: &str) -> ! {
    eprintln!("tier-node: {msg}");
    exit(2)
}

fn main() {
    let (mut id, mut members, mut client, mut exchange, mut dir) = (
        None::<NodeId>,
        Vec::new(),
        None::<SocketAddr>,
        None::<SocketAddr>,
        String::new(),
    );
    let (mut partitions, mut source_base, mut retain) = (4u32, 0u32, 1usize << 20);
    let (mut compact_every, mut digest_every, mut fsync) = (0u64, 0u64, true);
    let mut fsync_interval = None;
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
            "--exchange" => {
                exchange = Some(
                    v("--exchange")
                        .parse()
                        .unwrap_or_else(|_| die("--exchange")),
                )
            }
            "--dir" => dir = v("--dir"),
            "--partitions" => {
                partitions = v("--partitions")
                    .parse()
                    .unwrap_or_else(|_| die("--partitions"))
            }
            "--source-base" => {
                source_base = v("--source-base")
                    .parse()
                    .unwrap_or_else(|_| die("--source-base"))
            }
            "--retain" => retain = v("--retain").parse().unwrap_or_else(|_| die("--retain")),
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
                        Some(rest) => {
                            let ms = match rest {
                                "" => 5,
                                r => r
                                    .strip_prefix(':')
                                    .and_then(|x| x.parse::<u64>().ok())
                                    .unwrap_or_else(|| die("--fsync relaxed:MS")),
                            };
                            fsync = true;
                            fsync_interval = Some(Duration::from_millis(ms));
                        }
                        None => die("--fsync wants on, off or relaxed[:MS]"),
                    },
                }
            }
            other => die(&format!("unknown argument {other}")),
        }
    }
    let id = id.unwrap_or_else(|| die("--id is required"));
    let client = client.unwrap_or_else(|| die("--client is required"));
    let exchange = exchange.unwrap_or_else(|| die("--exchange is required"));
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
    let key = std::env::var("ORDERER_KEY")
        .ok()
        .filter(|k| !k.is_empty())
        .map(String::into_bytes);
    cfg.auth_key = key.clone();

    let (sm, view) = MatchingTierSm::<FifoCore>::new(
        BookConfig::default(),
        PartitionMap::hash(partitions).unwrap_or_else(|e| die(&e.to_string())),
        TierConfig {
            source_base,
            retain,
        },
        move |init, plugs| {
            let mut b = Pipeline::<FifoCore>::builder()
                .partitions(partitions)
                .egress(plugs.digest)
                .egress(plugs.archive)
                .timestamps(false);
            if let Some(i) = init {
                b = b.initial(i);
            }
            b.build().expect("pipeline")
        },
    );
    let replica = Replica::start(cfg, sm).unwrap_or_else(|e| die(&format!("start: {e}")));
    let _client = ClientServer::start_with_key(client, replica.handle(), key)
        .unwrap_or_else(|e| die(&format!("client port: {e}")));
    let _server = ExchangeServer::start(exchange, replica.handle(), view)
        .unwrap_or_else(|e| die(&format!("exchange port: {e}")));
    println!("tier-node {id} ready (client {client}, exchange {exchange})");
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}
