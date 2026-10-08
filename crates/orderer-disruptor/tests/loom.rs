//! Model-checked ring protocol (plan §5.2 `loom_model`). Explores every
//! legal interleaving (and weak-memory reordering loom models) of tiny
//! rings, so the Release/Acquire pairs in `ring.rs`'s protocol are checked
//! exhaustively rather than sampled.
//!
//!   RUSTFLAGS="--cfg loom" cargo test -p orderer-disruptor --test loom --release

#![cfg(loom)]

use loom::thread;
use orderer_disruptor::*;

/// Two producers race to claim and publish on a 2-slot ring; one consumer
/// must see both payloads, each exactly once, never a torn/stale slot.
#[test]
fn two_producers_one_consumer() {
    loom::model(|| {
        let mut b = RingBuilder::<u64>::new(2);
        b.consumer(&[]);
        let (p, mut cs) = b.build_multi();
        let mut c = cs.pop().unwrap();
        let p2 = p.clone();
        let t1 = thread::spawn(move || {
            p.publish(|s| *s = 11).unwrap();
        });
        let t2 = thread::spawn(move || {
            p2.publish(|s| *s = 22).unwrap();
        });
        let mut seen = Vec::new();
        while seen.len() < 2 {
            if c.poll(|v, _, _| seen.push(*v)) == 0 {
                thread::yield_now();
            }
        }
        t1.join().unwrap();
        t2.join().unwrap();
        seen.sort();
        assert_eq!(seen, vec![11, 22]);
    });
}

/// Single producer laps a 2-slot ring three times over: the consumer must
/// read every value in order, and the producer must never overwrite a slot
/// the consumer has not released.
#[test]
fn single_producer_wraps_safely() {
    loom::model(|| {
        let mut b = RingBuilder::<u64>::new(2);
        b.consumer(&[]);
        let (mut p, mut cs) = b.build_single();
        let mut c = cs.pop().unwrap();
        let t = thread::spawn(move || {
            for i in 1..=3u64 {
                p.publish(|s| *s = i).unwrap();
            }
        });
        let mut seen = Vec::new();
        while seen.len() < 3 {
            if c.poll(|v, _, _| seen.push(*v)) == 0 {
                thread::yield_now();
            }
        }
        t.join().unwrap();
        assert_eq!(seen, vec![1, 2, 3]);
    });
}

/// Diamond: stage B may only read what stage A has released.
#[test]
fn dependent_consumer_never_overtakes() {
    loom::model(|| {
        let mut b = RingBuilder::<u64>::new(2);
        let a = b.consumer(&[]);
        let _bb = b.consumer(&[a]);
        let (mut p, cs) = b.build_single();
        let mut cs = cs.into_iter();
        let (mut ca, mut cb) = (cs.next().unwrap(), cs.next().unwrap());
        let a_seq = ca.sequence();
        let tp = thread::spawn(move || {
            p.publish(|s| *s = 5).unwrap();
            p.publish(|s| *s = 6).unwrap();
        });
        let ta = thread::spawn(move || {
            let mut n = 0;
            while n < 2 {
                n += ca.poll(|_, _, _| {});
                thread::yield_now();
            }
        });
        let mut seen = Vec::new();
        while seen.len() < 2 {
            cb.poll(|v, seq, _| {
                assert!(a_seq.get() >= seq);
                seen.push(*v);
            });
            thread::yield_now();
        }
        tp.join().unwrap();
        ta.join().unwrap();
        assert_eq!(seen, vec![5, 6]);
    });
}
