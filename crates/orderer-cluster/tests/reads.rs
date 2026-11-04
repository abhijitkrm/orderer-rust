//! Linearizable reads through a log barrier.

mod common;

use common::*;
use orderer_cluster::client::{ClientServer, Conn};
use orderer_cluster::ProposeError;
use std::time::Duration;

fn payload(i: u64) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

#[test]
fn a_barrier_reflects_every_acknowledged_write() {
    let c = Cluster::new("reads-basic", 3, |_| {});
    let l = c.wait_leader(Duration::from_secs(5));
    let h = c.handle(l).unwrap();
    for i in 0..50 {
        c.propose(&payload(i)).unwrap();
        // After a write is acknowledged, a barrier on the leader must see it.
        h.barrier().unwrap();
        assert_eq!(
            c.recorders[l].payloads().len() as u64,
            i + 1,
            "stale read after write {i}"
        );
    }
}

#[test]
fn followers_cannot_serve_a_barrier() {
    let c = Cluster::new("reads-follower", 3, |_| {});
    let l = c.wait_leader(Duration::from_secs(5));
    let f = (0..3).find(|&i| i != l).unwrap();
    assert!(matches!(
        c.handle(f).unwrap().barrier(),
        Err(ProposeError::NotLeader(_))
    ));
}

#[test]
fn a_deposed_leader_cannot_serve_a_stale_read() {
    let c = Cluster::new("reads-stale", 3, |cfg| {
        cfg.propose_timeout = Duration::from_millis(600)
    });
    let old = c.wait_leader(Duration::from_secs(5));
    c.propose(&payload(1)).unwrap();
    c.wait_applied(&[0, 1, 2], 1, Duration::from_secs(5));

    // Cut the leader off. It still believes it leads for a while.
    let all: Vec<u8> = (0..3).collect();
    c.nodes[old]
        .as_ref()
        .unwrap()
        .filter
        .isolate(old as u8, &all);
    // The majority moves on: a new leader commits a write the old one never sees.
    let end = std::time::Instant::now() + Duration::from_secs(10);
    let newl = loop {
        if let Some(l) = (0..3)
            .filter(|&i| i != old)
            .find(|&i| c.handle(i).unwrap().barrier().is_ok())
        {
            break l;
        }
        assert!(
            std::time::Instant::now() < end,
            "majority never elected a leader"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    let h_new = c.handle(newl).unwrap();
    h_new.propose(payload(2)).unwrap();
    h_new.barrier().unwrap();
    assert_eq!(c.recorders[newl].payloads().len(), 2);

    // The old leader's state still lacks write 2. It must refuse to answer a
    // linearizable read rather than return that stale state.
    assert_eq!(c.recorders[old].payloads().len(), 1);
    let r = c.handle(old).unwrap().barrier();
    assert!(r.is_err(), "a deposed leader served a barrier: {r:?}");
}

#[test]
fn the_barrier_works_over_the_client_protocol() {
    let c = Cluster::new("reads-client", 3, |_| {});
    let l = c.wait_leader(Duration::from_secs(5));
    let addrs = free_addrs(3);
    let _servers: Vec<_> = (0..3)
        .map(|i| ClientServer::start(addrs[i], c.handle(i).unwrap()).unwrap())
        .collect();
    c.propose(&payload(7)).unwrap();
    let mut leader = Conn::connect(addrs[l]).unwrap();
    assert!(leader.barrier().unwrap().is_some());
    assert_eq!(c.recorders[l].payloads().len(), 1);
    let f = (0..3).find(|&i| i != l).unwrap();
    assert_eq!(Conn::connect(addrs[f]).unwrap().barrier().unwrap(), None);
}
