//! End-to-end: real replicas over TCP with real files.

mod common;

use common::*;
use orderer_cluster::ProposeError;
use orderer_consensus::Role;
use std::collections::HashSet;
use std::io::Write;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn payload(i: u64) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

fn ids(c: &Cluster) -> Vec<u8> {
    c.members.iter().map(|m| m.0).collect()
}

#[test]
fn elects_a_leader_and_replicates_in_order() {
    let c = Cluster::new("basic", 3, |_| {});
    c.wait_leader(Duration::from_secs(5));
    for i in 0..500 {
        c.propose(&payload(i)).unwrap();
    }
    let seq = c.wait_converged(Duration::from_secs(10));
    assert_eq!(seq, (0..500).map(payload).collect::<Vec<_>>());
}

#[test]
fn single_node_cluster_works() {
    let c = Cluster::new("single", 1, |_| {});
    c.wait_leader(Duration::from_secs(5));
    for i in 0..100 {
        c.propose(&payload(i)).unwrap();
    }
    assert_eq!(c.wait_converged(Duration::from_secs(5)).len(), 100);
}

#[test]
fn followers_refuse_proposals_and_point_to_the_leader() {
    let c = Cluster::new("redirect", 3, |_| {});
    let l = c.wait_leader(Duration::from_secs(5));
    c.propose(&payload(0)).unwrap();
    c.wait_applied(&[0, 1, 2], 1, Duration::from_secs(5));
    let f = (0..3).find(|&i| i != l).unwrap();
    match c.handle(f).unwrap().propose(payload(1)) {
        Err(ProposeError::NotLeader(Some(h))) => assert_eq!(h as usize, l),
        other => panic!("expected a redirect, got {other:?}"),
    }
}

#[test]
fn leader_crash_loses_no_acknowledged_command() {
    let mut c = Cluster::new("failover", 3, |_| {});
    c.wait_leader(Duration::from_secs(5));
    let mut acked = HashSet::new();
    let mut maybe = HashSet::new();
    for i in 0..200u64 {
        match c.propose(&payload(i)) {
            Ok(_) => {
                acked.insert(i);
            }
            Err(_) => {
                maybe.insert(i);
            }
        }
    }
    // Kill the leader while more traffic is in flight.
    let old = c.leader().unwrap();
    c.crash(old);
    for i in 200..600u64 {
        match c.propose(&payload(i)) {
            Ok(_) => {
                acked.insert(i);
            }
            Err(_) => {
                maybe.insert(i);
            }
        }
    }
    // The old leader returns and catches up from its disk.
    c.start(old);
    let seq = c.wait_converged(Duration::from_secs(15));
    let got: Vec<u64> = seq
        .iter()
        .map(|p| u64::from_le_bytes(p[..8].try_into().unwrap()))
        .collect();
    let got_set: HashSet<u64> = got.iter().copied().collect();
    assert_eq!(got.len(), got_set.len(), "a command was applied twice");
    for a in &acked {
        assert!(got_set.contains(a), "acknowledged command {a} was lost");
    }
    for g in &got_set {
        assert!(
            acked.contains(g) || maybe.contains(g),
            "command {g} was never proposed"
        );
    }
    // Order within one client's stream is preserved.
    assert!(got.windows(2).all(|w| w[0] < w[1]), "commands reordered");
}

#[test]
fn repeated_leader_kills_under_concurrent_load() {
    use orderer_cluster::ReplicaHandle;
    use std::sync::Mutex;

    let mut c = Cluster::new("storm", 3, |cfg| {
        cfg.max_pending = 100_000;
        cfg.fsync = true;
    });
    c.wait_leader(Duration::from_secs(5));
    // Writers look handles up fresh for every attempt, so they follow restarts.
    let handles: Arc<Mutex<Vec<Option<ReplicaHandle>>>> =
        Arc::new(Mutex::new((0..3).map(|i| c.handle(i)).collect()));
    let next = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(Mutex::new(Vec::<u64>::new()));
    let maybe = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let writers: Vec<_> = (0..3)
        .map(|_| {
            let (handles, next, stop, acked, maybe) = (
                handles.clone(),
                next.clone(),
                stop.clone(),
                acked.clone(),
                maybe.clone(),
            );
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let id = next.fetch_add(1, Ordering::SeqCst) as u64;
                    let hs: Vec<_> = handles.lock().unwrap().iter().flatten().cloned().collect();
                    let mut outcome = None;
                    for h in &hs {
                        match h.propose(payload(id)) {
                            Ok(_) => {
                                outcome = Some(true);
                                break;
                            }
                            Err(ProposeError::NotLeader(_))
                            | Err(ProposeError::Busy)
                            | Err(ProposeError::Stopped) => {}
                            Err(_) => {
                                outcome = Some(false); // unknown: never retried
                                break;
                            }
                        }
                    }
                    match outcome {
                        Some(true) => acked.lock().unwrap().push(id),
                        Some(false) => {
                            maybe.lock().unwrap().insert(id);
                        }
                        None => std::thread::sleep(Duration::from_millis(5)),
                    }
                }
            })
        })
        .collect();

    for _round in 0..4 {
        std::thread::sleep(Duration::from_millis(500));
        let l = c.wait_leader(Duration::from_secs(10));
        handles.lock().unwrap()[l] = None;
        c.crash(l);
        std::thread::sleep(Duration::from_millis(300));
        c.start(l);
        handles.lock().unwrap()[l] = c.handle(l);
    }
    std::thread::sleep(Duration::from_millis(500));
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }

    let seq = c.wait_converged(Duration::from_secs(30));
    let got: Vec<u64> = seq
        .iter()
        .map(|p| u64::from_le_bytes(p[..8].try_into().unwrap()))
        .collect();
    let got_set: HashSet<u64> = got.iter().copied().collect();
    assert_eq!(got.len(), got_set.len(), "a command was applied twice");
    let acked = acked.lock().unwrap();
    assert!(
        acked.len() > 100,
        "load generator barely ran: {} acks",
        acked.len()
    );
    for a in acked.iter() {
        assert!(got_set.contains(a), "acknowledged command {a} was lost");
    }
    let maybe = maybe.lock().unwrap();
    for g in &got_set {
        assert!(
            acked.contains(g) || maybe.contains(g),
            "command {g} was never proposed"
        );
    }
}

#[test]
fn isolated_leader_cannot_commit_and_its_uncommitted_entries_vanish() {
    let mut c = Cluster::new("partition", 3, |cfg| {
        cfg.propose_timeout = Duration::from_millis(700)
    });
    let old = c.wait_leader(Duration::from_secs(5));
    for i in 0..20 {
        c.propose(&payload(i)).unwrap();
    }
    c.wait_applied(&[0, 1, 2], 20, Duration::from_secs(5));

    let all = ids(&c);
    c.nodes[old]
        .as_ref()
        .unwrap()
        .filter
        .isolate(old as u8, &all);
    // The isolated leader still believes it leads for a while, but cannot commit.
    let r = c.handle(old).unwrap().propose(payload(1000));
    assert!(
        matches!(
            r,
            Err(ProposeError::Timeout)
                | Err(ProposeError::NotLeader(_))
                | Err(ProposeError::Superseded)
        ),
        "an isolated leader must not acknowledge: {r:?}"
    );
    // The majority elects a new leader and keeps committing.
    let end = std::time::Instant::now() + Duration::from_secs(10);
    let newl = loop {
        let l = (0..3).filter(|&i| i != old).find(|&i| {
            c.handle(i)
                .map(|h| h.status().role == Role::Leader)
                .unwrap_or(false)
        });
        if let Some(l) = l {
            break l;
        }
        assert!(
            std::time::Instant::now() < end,
            "majority did not elect a leader"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    for i in 20..60 {
        loop {
            match c.handle(newl).unwrap().propose(payload(i)) {
                Ok(_) => break,
                Err(ProposeError::Busy) | Err(ProposeError::NotLeader(_)) => {
                    std::thread::sleep(Duration::from_millis(5))
                }
                Err(e) => panic!("majority side failed to commit: {e}"),
            }
        }
    }
    // Heal: the old leader steps down and drops its uncommitted entry.
    c.nodes[old].as_ref().unwrap().filter.heal();
    let seq = c.wait_converged(Duration::from_secs(15));
    let got: Vec<u64> = seq
        .iter()
        .map(|p| u64::from_le_bytes(p[..8].try_into().unwrap()))
        .collect();
    assert_eq!(
        got,
        (0..60).collect::<Vec<_>>(),
        "entry 1000 must not survive; 0..60 in order"
    );
    c.stop(old);
}

#[test]
fn lagging_node_catches_up_by_snapshot() {
    let mut c = Cluster::new("snapcatch", 3, |cfg| cfg.compact_every = 40);
    let l = c.wait_leader(Duration::from_secs(5));
    let lag = (0..3).find(|&i| i != l).unwrap();
    for i in 0..10 {
        c.propose(&payload(i)).unwrap();
    }
    c.wait_applied(&[0, 1, 2], 10, Duration::from_secs(5));
    c.stop(lag);
    for i in 10..400 {
        c.propose(&payload(i)).unwrap();
    }
    // The leader's log no longer reaches back to what `lag` needs.
    let end = std::time::Instant::now() + Duration::from_secs(10);
    while c.handle(l).unwrap().status().base == 0 {
        assert!(std::time::Instant::now() < end, "leader never compacted");
        std::thread::sleep(Duration::from_millis(20));
    }
    c.start(lag);
    let seq = c.wait_converged(Duration::from_secs(20));
    assert_eq!(seq, (0..400).map(payload).collect::<Vec<_>>());
    assert!(
        c.handle(lag).unwrap().status().base > 0,
        "caught up without a snapshot install"
    );
}

#[test]
fn whole_cluster_restart_recovers_from_disk() {
    let mut c = Cluster::new("restart", 3, |cfg| {
        cfg.compact_every = 50;
        cfg.fsync = true;
    });
    c.wait_leader(Duration::from_secs(5));
    for i in 0..300 {
        c.propose(&payload(i)).unwrap();
    }
    c.wait_converged(Duration::from_secs(10));
    for i in 0..3 {
        c.stop(i);
    }
    for i in 0..3 {
        c.start(i);
    }
    c.wait_leader(Duration::from_secs(10));
    for i in 300..400 {
        c.propose(&payload(i)).unwrap();
    }
    let seq = c.wait_converged(Duration::from_secs(15));
    assert_eq!(seq, (0..400).map(payload).collect::<Vec<_>>());
}

#[test]
fn crash_with_a_torn_wal_tail_recovers() {
    let mut c = Cluster::new("torn", 3, |cfg| cfg.fsync = true);
    let l = c.wait_leader(Duration::from_secs(5));
    for i in 0..100 {
        c.propose(&payload(i)).unwrap();
    }
    c.wait_converged(Duration::from_secs(10));
    let f = (0..3).find(|&i| i != l).unwrap();
    c.crash(f);
    // Append half a frame of garbage, as a torn write would leave.
    let wal = c.dir.join(format!("n{f}")).join("wal").join("raft.wal");
    let mut file = std::fs::OpenOptions::new().append(true).open(&wal).unwrap();
    file.write_all(&[0x40, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef, 1, 2, 3])
        .unwrap();
    drop(file);
    c.start(f);
    for i in 100..150 {
        c.propose(&payload(i)).unwrap();
    }
    let seq = c.wait_converged(Duration::from_secs(10));
    assert_eq!(seq, (0..150).map(payload).collect::<Vec<_>>());
}

#[test]
fn a_corrupt_wal_refuses_to_start() {
    let mut c = Cluster::new("corrupt", 3, |cfg| cfg.fsync = true);
    let l = c.wait_leader(Duration::from_secs(5));
    for i in 0..60 {
        c.propose(&payload(i)).unwrap();
    }
    c.wait_converged(Duration::from_secs(10));
    let f = (0..3).find(|&i| i != l).unwrap();
    c.stop(f);
    let wal = c.dir.join(format!("n{f}")).join("wal").join("raft.wal");
    let mut data = std::fs::read(&wal).unwrap();
    data[40] ^= 0xff; // early, with valid frames after it
    std::fs::write(&wal, data).unwrap();
    let r = orderer_cluster::Replica::start(c.config(f), Recorder::default());
    assert!(r.is_err(), "a node with mid-log damage must not join");
}

#[test]
fn state_divergence_halts_the_odd_node_out() {
    let mut c = Cluster::new("diverge", 3, |cfg| cfg.digest_every = 16);
    // Node 2's state machine mis-applies one entry.
    c.stop(2);
    c.start_with(
        2,
        Recorder {
            corrupt_at: Some(5),
            ..Recorder::default()
        },
    );
    c.wait_leader(Duration::from_secs(5));
    // If the corrupted node happens to lead, it halts itself mid-run and
    // proposals in flight to it have no definite answer; the majority carries on.
    let acked = (0..200).filter(|&i| c.propose(&payload(i)).is_ok()).count();
    assert!(acked >= 150, "only {acked} of 200 acknowledged");
    let end = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(why) = c.handle(2).unwrap().status().halted {
            assert!(why.contains("diverged"), "{why}");
            break;
        }
        assert!(
            std::time::Instant::now() < end,
            "divergence went undetected"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    // The healthy majority is unaffected and agrees.
    let a = c.handle(0).unwrap().state_hash();
    let b = c.handle(1).unwrap().state_hash();
    assert!(a.is_some() && b.is_some());
    assert_eq!(c.handle(0).unwrap().status().halted, None);
    assert_eq!(c.handle(1).unwrap().status().halted, None);
}

#[test]
fn matching_state_passes_digest_checks() {
    let c = Cluster::new("digest-ok", 3, |cfg| cfg.digest_every = 16);
    c.wait_leader(Duration::from_secs(5));
    for i in 0..200 {
        c.propose(&payload(i)).unwrap();
    }
    c.wait_converged(Duration::from_secs(10));
    let end = std::time::Instant::now() + Duration::from_secs(5);
    while c.handle(0).unwrap().status().digests_verified == 0 {
        assert!(
            std::time::Instant::now() < end,
            "no digest was ever verified"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    for i in 0..3 {
        assert_eq!(c.handle(i).unwrap().status().halted, None);
    }
}

#[test]
fn hostile_connections_do_not_disturb_the_cluster() {
    let c = Cluster::new("hostile", 3, |_| {});
    c.wait_leader(Duration::from_secs(5));
    let target = c.members[0].1;
    // Garbage, an oversized length, a wrong-cluster hello, an abrupt close.
    for junk in [
        vec![0xffu8; 64],
        vec![0xff, 0xff, 0xff, 0x7f, 0, 0, 0, 0, 1],
        {
            let mut b = vec![10, 0, 0, 0];
            b.extend_from_slice(&[0; 4]);
            b.extend_from_slice(&[6, 9, 1, 2, 3, 4, 5, 6, 7, 8]);
            b
        },
        vec![],
    ] {
        if let Ok(mut s) = TcpStream::connect(target) {
            let _ = s.write_all(&junk);
        }
    }
    for i in 0..100 {
        c.propose(&payload(i)).unwrap();
    }
    assert_eq!(c.wait_converged(Duration::from_secs(10)).len(), 100);
}

#[test]
fn five_nodes_survive_two_failures() {
    let mut c = Cluster::new("five", 5, |_| {});
    c.wait_leader(Duration::from_secs(5));
    for i in 0..100 {
        c.propose(&payload(i)).unwrap();
    }
    let l = c.leader().unwrap();
    c.crash(l);
    let other = (0..5).find(|&i| i != l && c.nodes[i].is_some()).unwrap();
    c.crash(other);
    c.wait_leader(Duration::from_secs(15));
    for i in 100..200 {
        c.propose(&payload(i)).unwrap();
    }
    let seq = c.wait_converged(Duration::from_secs(15));
    assert_eq!(seq, (0..200).map(payload).collect::<Vec<_>>());
}

#[test]
fn three_nodes_cannot_commit_with_two_down() {
    let mut c = Cluster::new("noquorum", 3, |cfg| {
        cfg.propose_timeout = Duration::from_millis(500)
    });
    let l = c.wait_leader(Duration::from_secs(5));
    c.propose(&payload(0)).unwrap();
    let others: Vec<usize> = (0..3).filter(|&i| i != l).collect();
    for o in others {
        c.crash(o);
    }
    let r = c.handle(l).unwrap().propose(payload(1));
    assert!(
        matches!(r, Err(ProposeError::Timeout)),
        "no quorum, no ack: {r:?}"
    );
    assert_eq!(c.recorders[l].payloads(), vec![payload(0)]);
}

/// A slow state machine on the leader must not stall consensus. If applying
/// blocked the consensus thread, a burst of proposals would starve its ticks
/// and heartbeats, the followers would time out, and leadership would move.
#[test]
fn a_slow_leader_state_machine_does_not_cost_it_leadership() {
    use orderer_cluster::StateMachine;
    use orderer_consensus::{Entry, Index};

    struct Slow(Recorder);
    impl StateMachine for Slow {
        fn apply(&mut self, i: Index, e: &Entry) {
            std::thread::sleep(Duration::from_millis(700));
            self.0.apply(i, e);
        }
        fn snapshot(&mut self, i: Index) -> Vec<u8> {
            self.0.snapshot(i)
        }
        fn restore(&mut self, i: Index, d: &[u8]) -> Result<(), String> {
            self.0.restore(i, d)
        }
    }

    let mut c = Cluster::new("slow-leader", 3, |_| {});
    for i in 0..3 {
        c.stop(i);
    }
    // Node 0 wins the first election (short timeout) and has the slow machine.
    for i in 0..3 {
        let mut cfg = c.config(i);
        cfg.max_pending = 100_000;
        cfg.propose_timeout = Duration::from_secs(30);
        cfg.consensus.election_ticks = if i == 0 { 8 } else { 40 };
        let rec = Recorder::default();
        c.recorders[i] = rec.clone();
        c.nodes[i] = Some(if i == 0 {
            orderer_cluster::Replica::start(cfg, Slow(rec)).unwrap()
        } else {
            orderer_cluster::Replica::start(cfg, rec).unwrap()
        });
    }
    let l = c.wait_leader(Duration::from_secs(5));
    assert_eq!(l, 0, "node 0 should have won the first election");
    let term0 = c.handle(0).unwrap().status().term;

    // Each apply on node 0 outlasts the followers' election timeout (200-400
    // ms), so if applying ran on the consensus thread its heartbeats would stop.
    let h = c.handle(0).unwrap();
    let rxs: Vec<_> = (0..3u64).map(|i| h.propose_async(payload(i))).collect();
    for rx in rxs {
        rx.recv().unwrap().expect("burst proposal acknowledged");
    }
    let s = h.status();
    assert_eq!(
        (s.role, s.term, s.halted),
        (Role::Leader, term0, None),
        "leadership was lost while the state machine was slow"
    );
    // Followers apply promptly; the slow node eventually catches up.
    c.wait_applied(&[1, 2], 3, Duration::from_secs(5));
    c.wait_applied(&[0], 3, Duration::from_secs(30));
    assert_eq!(c.wait_converged(Duration::from_secs(30)).len(), 3);
}
