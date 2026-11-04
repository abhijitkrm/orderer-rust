//! Exactly-once submission: retries after unknown outcomes take effect once.

mod common;

use common::*;
use orderer_cluster::client::{Client, ClientServer};
use orderer_cluster::codec::{decode_batch, encode_session_batch};
use orderer_core::*;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

fn cmd(id: u64) -> Vec<(Symbol, Command)> {
    vec![(1, Command::new(id, Side::Bid, 10_000, 1, Tif::Gtc))]
}

fn ids(c: &Cluster, node: usize) -> Vec<u64> {
    c.recorders[node]
        .payloads()
        .iter()
        .flat_map(|p| decode_batch(p).unwrap())
        .map(|(_, c)| match c {
            Command::New { order_id, .. } => order_id,
            _ => unreachable!(),
        })
        .collect()
}

#[test]
fn the_same_session_batch_committed_twice_is_applied_once() {
    let c = Cluster::with_dedup("sess-dup", 3);
    c.wait_leader(Duration::from_secs(5));
    let mut p = Vec::new();
    encode_session_batch(7, 1, &cmd(100), &mut p);
    let a = c.propose(&p).unwrap();
    let b = c.propose(&p).unwrap(); // the retry: a second log entry
    assert!(b > a);
    let mut p2 = Vec::new();
    encode_session_batch(7, 2, &cmd(101), &mut p2);
    c.propose(&p2).unwrap();
    c.wait_converged(Duration::from_secs(5));
    for n in 0..3 {
        assert_eq!(ids(&c, n), vec![100, 101], "node {n}");
    }
}

#[test]
fn a_session_client_survives_leader_kills_without_a_single_duplicate_or_loss() {
    let mut c = Cluster::new("sess-failover", 3, |cfg| cfg.max_pending = 100_000);
    // Rebuild with dedup state machines (Cluster::new has them off).
    for i in 0..3 {
        c.stop(i);
    }
    c.dedup = true;
    for i in 0..3 {
        c.start(i);
    }
    c.wait_leader(Duration::from_secs(5));
    // One client-facing server per node, re-created after a restart.
    let addrs: Vec<SocketAddr> = free_addrs(3);
    let mut servers: Vec<Option<ClientServer>> = (0..3)
        .map(|i| Some(ClientServer::start(addrs[i], c.handle(i).unwrap()).unwrap()))
        .collect();

    let pairs: Vec<(u8, SocketAddr)> = addrs
        .iter()
        .copied()
        .enumerate()
        .map(|(i, a)| (i as u8, a))
        .collect();
    let writer = std::thread::spawn(move || {
        let mut client = Client::new(pairs).with_session(5);
        for id in 1..=150u64 {
            client
                .propose_session(&cmd(id))
                .unwrap_or_else(|e| panic!("batch {id}: {e}"));
        }
    });
    for _ in 0..3 {
        std::thread::sleep(Duration::from_millis(250));
        let l = c.wait_leader(Duration::from_secs(10));
        servers[l].take().unwrap().shutdown();
        c.crash(l);
        std::thread::sleep(Duration::from_millis(200));
        c.start(l);
        servers[l] = Some(ClientServer::start(addrs[l], c.handle(l).unwrap()).unwrap());
    }
    writer.join().unwrap();
    c.wait_converged(Duration::from_secs(20));
    for n in 0..3 {
        let got = ids(&c, n);
        let set: HashSet<u64> = got.iter().copied().collect();
        assert_eq!(got.len(), set.len(), "node {n}: a batch took effect twice");
        assert_eq!(
            got,
            (1..=150).collect::<Vec<_>>(),
            "node {n}: lost or reordered batches"
        );
    }
}

#[test]
fn a_client_without_a_session_cannot_use_propose_session() {
    let mut client = Client::new(vec![(0, free_addrs(1)[0])]);
    assert!(matches!(
        client.propose_session(&cmd(1)),
        Err(orderer_cluster::client::ClientError::Protocol(_))
    ));
}
