//! ledger-node: one replica of a ledger shard (`LedgerNodeSm`) with its client
//! port (status, state hash) and gateway port (commands and reads).
//!
//!   ledger-node --shard S --id N --members 0=HOST:PORT,... --client HOST:PORT
//!               --gateway HOST:PORT --dir DIR --config FILE [--compact-every N]
//!               [--digest-every N] [--fsync on|off|relaxed[:MS]]
//!
//! Reference data is read from the config file (`config.rs`). The key, if any, is
//! read from `ORDERER_KEY` (not argv); it authenticates the client port and the
//! gateway port. The replica transport uses a key derived from it and the config
//! checksum, so a replica started with a different config cannot join the group.

use orderer_cluster::client::ClientServer;
use orderer_cluster::{Replica, ReplicaConfig};
use orderer_consensus::NodeId;
use orderer_exchange::cli::Setup;
use orderer_exchange::{GatewayServer, LedgerNodeSm};
use std::net::SocketAddr;
use std::process::exit;
use std::time::Duration;

fn die(msg: &str) -> ! {
    eprintln!("ledger-node: {msg}");
    exit(2)
}

fn main() {
    let (mut shard, mut id, mut members, mut client, mut gateway, mut dir) = (
        None::<u16>,
        None::<NodeId>,
        Vec::new(),
        None::<SocketAddr>,
        None::<SocketAddr>,
        String::new(),
    );
    let mut config = String::new();
    let (mut compact_every, mut digest_every, mut fsync) = (0u64, 0u64, true);
    let mut fsync_interval = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut v = |n: &str| {
            args.next()
                .unwrap_or_else(|| die(&format!("{n} needs a value")))
        };
        match a.as_str() {
            "--shard" => shard = Some(v("--shard").parse().unwrap_or_else(|_| die("--shard"))),
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
            "--gateway" => {
                gateway = Some(v("--gateway").parse().unwrap_or_else(|_| die("--gateway")))
            }
            "--dir" => dir = v("--dir"),
            "--config" => config = v("--config"),
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
    let shard = shard.unwrap_or_else(|| die("--shard is required"));
    let id = id.unwrap_or_else(|| die("--id is required"));
    let client = client.unwrap_or_else(|| die("--client is required"));
    let gateway = gateway.unwrap_or_else(|| die("--gateway is required"));
    if members.is_empty() || dir.is_empty() || config.is_empty() {
        die("--members, --dir and --config are required");
    }
    let setup = Setup::load(&config).unwrap_or_else(|e| die(&e));
    if shard >= setup.cfg.ledger_shards {
        die(&format!(
            "--shard {shard} but the config has {} ledger shards",
            setup.cfg.ledger_shards
        ));
    }
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| die(&format!("{dir}: {e}")));

    let mut cfg = ReplicaConfig::new(id, members, format!("{dir}/state"));
    cfg.fsync = fsync;
    cfg.fsync_interval = fsync_interval;
    cfg.compact_every = compact_every;
    cfg.digest_every = digest_every;
    cfg.max_pending = 1 << 16;
    // Ledger entries are small and the apply is cheap, but a digest is O(state).
    let key = setup.auth.key.clone();
    cfg.auth_key = setup.auth.replica_key();

    let (sm, view) = LedgerNodeSm::new(setup.cfg.ledger_config(shard));
    let replica = Replica::start(cfg, sm).unwrap_or_else(|e| die(&format!("start: {e}")));
    let _client = ClientServer::start_with_key(client, replica.handle(), key)
        .unwrap_or_else(|e| die(&format!("client port: {e}")));
    let _gw = GatewayServer::start_with(gateway, replica.handle(), view, setup.auth.clone())
        .unwrap_or_else(|e| die(&format!("gateway port: {e}")));
    println!(
        "ledger-node shard {shard} id {id} ready (client {client}, gateway {gateway}, config {})",
        &orderer_cluster::sha256::hex(&setup.cfg.checksum())[..16]
    );
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}
