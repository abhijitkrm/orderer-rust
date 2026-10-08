//! Ring protocol tests (plan §5.2): wrap, multi-producer integrity, batch
//! claims, gating/backpressure, try-publish CAS path, stalled producers,
//! barrier dependencies, wait strategies, multicast, clean shutdown.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use orderer_disruptor::*;

/// Spin-poll a consumer until it has delivered `n` events or `timeout`.
fn drain_n<T>(c: &mut Consumer<T>, n: usize, mut f: impl FnMut(&T, i64, bool)) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut got = 0;
    while got < n {
        got += c.poll(&mut f);
        assert!(Instant::now() < deadline, "timed out after {got}/{n}");
        if got < n {
            thread::yield_now();
        }
    }
}

#[test]
fn spsc_wrap() {
    let mut b = RingBuilder::<u64>::new(8);
    b.consumer(&[]);
    let (mut p, mut cs) = b.build_single();
    let mut c = cs.pop().unwrap();
    const N: u64 = 100_000;
    let reader = thread::spawn(move || {
        let mut expect = 0u64;
        drain_n(&mut c, N as usize, |v, seq, _| {
            assert_eq!(*v, expect);
            assert_eq!(seq as u64, expect);
            expect += 1;
        });
    });
    for i in 0..N {
        p.publish(|s| *s = i).unwrap();
    }
    reader.join().unwrap();
}

#[derive(Default, Clone, Copy)]
struct Msg {
    producer: u64,
    counter: u64,
    check: u64,
}

fn multi_producer_run(batch: usize) {
    const PRODUCERS: u64 = 4;
    const PER: u64 = 100_000;
    let mut b = RingBuilder::<Msg>::new(1024);
    b.consumer(&[]);
    let (p, mut cs) = b.build_multi();
    let mut c = cs.pop().unwrap();
    let total = (PRODUCERS * PER) as usize;
    let reader = thread::spawn(move || {
        let mut next = [0u64; PRODUCERS as usize];
        let mut last_seq = -1i64;
        drain_n(&mut c, total, |m, seq, _| {
            assert_eq!(seq, last_seq + 1, "each seq exactly once, in order");
            last_seq = seq;
            assert_eq!(
                m.check,
                m.producer.wrapping_mul(31) ^ m.counter,
                "no torn slot"
            );
            assert_eq!(m.counter, next[m.producer as usize], "per-producer order");
            next[m.producer as usize] += 1;
        });
        assert!(next.iter().all(|&n| n == PER));
    });
    let handles: Vec<_> = (0..PRODUCERS)
        .map(|id| {
            let p = p.clone();
            thread::spawn(move || {
                let mut i = 0;
                while i < PER {
                    let n = batch.min((PER - i) as usize);
                    p.publish_batch(n, |k, s| {
                        let counter = i + k as u64;
                        *s = Msg {
                            producer: id,
                            counter,
                            check: id.wrapping_mul(31) ^ counter,
                        };
                    })
                    .unwrap();
                    i += n as u64;
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    reader.join().unwrap();
}

#[test]
fn multi_producer_integrity_single_claims() {
    multi_producer_run(1);
}

#[test]
fn multi_producer_integrity_batched_claims() {
    multi_producer_run(37);
}

#[test]
fn batch_claim_consume() {
    let mut b = RingBuilder::<u32>::new(16);
    b.consumer(&[]);
    let (mut p, mut cs) = b.build_single();
    let last = p.publish_batch(5, |i, s| *s = i as u32 * 10).unwrap();
    assert_eq!(last, 4);
    let mut seen = Vec::new();
    assert_eq!(cs[0].poll(|v, seq, eob| seen.push((*v, seq, eob))), 5);
    assert_eq!(
        seen,
        vec![
            (0, 0, false),
            (10, 1, false),
            (20, 2, false),
            (30, 3, false),
            (40, 4, true)
        ]
    );

    // staged writes are invisible until commit
    p.stage(|s| *s = 1).unwrap();
    p.stage(|s| *s = 2).unwrap();
    assert_eq!(p.staged(), 2);
    assert_eq!(cs[0].poll(|_, _, _| {}), 0);
    p.commit();
    assert_eq!(cs[0].poll(|_, _, _| {}), 2);

    // batch cap
    p.publish_batch(10, |i, s| *s = i as u32).unwrap();
    cs[0].set_max_batch(4);
    assert_eq!(cs[0].poll(|_, _, _| {}), 4);
    assert_eq!(cs[0].poll(|_, _, _| {}), 4);
    assert_eq!(cs[0].poll(|_, _, _| {}), 2);
}

#[test]
fn gating_blocks_and_try_publish_full() {
    let mut b = RingBuilder::<u32>::new(4);
    b.consumer(&[]);
    let (mut p, mut cs) = b.build_single();
    for i in 0..4 {
        p.try_publish(|s| *s = i).unwrap();
    }
    assert_eq!(p.try_publish(|s| *s = 99), Err(PublishError::Full));

    let done = Arc::new(AtomicBool::new(false));
    let d = done.clone();
    let writer = thread::spawn(move || {
        p.publish(|s| *s = 4).unwrap(); // must block until a slot frees
        d.store(true, Ordering::SeqCst);
        p
    });
    thread::sleep(Duration::from_millis(50));
    assert!(
        !done.load(Ordering::SeqCst),
        "producer must not lap the consumer"
    );
    let mut seen = Vec::new();
    cs[0].poll(|v, _, _| seen.push(*v));
    let _p = writer.join().unwrap();
    assert!(done.load(Ordering::SeqCst));
    cs[0].poll(|v, _, _| seen.push(*v));
    assert_eq!(seen, vec![0, 1, 2, 3, 4], "zero loss under Block");
}

#[test]
fn staged_work_is_committed_before_waiting() {
    // a single producer that staged a full ring must publish before waiting
    // for space, or it would wait on consumers that can't see its work
    let mut b = RingBuilder::<u32>::new(4);
    b.consumer(&[]);
    let (mut p, mut cs) = b.build_single();
    let mut c = cs.pop().unwrap();
    let reader = thread::spawn(move || {
        let mut seen = Vec::new();
        drain_n(&mut c, 10, |v, _, _| seen.push(*v));
        seen
    });
    for i in 0..10 {
        p.stage(|s| *s = i).unwrap();
    }
    p.commit();
    assert_eq!(reader.join().unwrap(), (0..10).collect::<Vec<_>>());
}

#[test]
fn try_publish_cas_never_leaks_claims() {
    let mut b = RingBuilder::<u32>::new(8);
    b.consumer(&[]);
    let (p, mut cs) = b.build_multi();
    for i in 0..8 {
        p.try_publish(|s| *s = i).unwrap();
    }
    assert_eq!(p.try_publish(|s| *s = 99), Err(PublishError::Full));
    assert_eq!(
        p.try_publish_batch(3, |_, s| *s = 99),
        Err(PublishError::Full)
    );
    let ctl = p.control();
    assert_eq!(
        ctl.published(),
        7,
        "a failed try leaves the cursor untouched"
    );
    let mut seen = Vec::new();
    cs[0].poll(|v, _, _| seen.push(*v));
    assert_eq!(seen, (0..8).collect::<Vec<_>>());
    p.try_publish(|s| *s = 8).unwrap();
    cs[0].poll(|v, _, _| seen.push(*v));
    assert_eq!(*seen.last().unwrap(), 8);
}

#[test]
fn try_and_block_producers_never_double_claim() {
    const PER: u32 = 20_000;
    let mut b = RingBuilder::<u64>::new(64);
    b.consumer(&[]);
    let (p, mut cs) = b.build_multi();
    let mut c = cs.pop().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let st = stop.clone();
    let reader = thread::spawn(move || {
        let mut seen = std::collections::HashSet::new();
        let mut last = -1i64;
        loop {
            // read `stop` first: once set, every accepted publish is visible
            let stopping = st.load(Ordering::SeqCst);
            let n = c.poll(|v, seq, _| {
                assert_eq!(seq, last + 1);
                last = seq;
                assert!(seen.insert(*v), "value delivered twice");
            });
            if n == 0 {
                if stopping {
                    break;
                }
                thread::yield_now();
            }
        }
        seen.len()
    });
    let workers: Vec<_> = (0..4u64)
        .map(|id| {
            let p = p.clone();
            let acc = accepted.clone();
            thread::spawn(move || {
                for i in 0..PER {
                    let v = (id << 32) | i as u64;
                    if id % 2 == 0 {
                        p.publish(|s| *s = v).unwrap();
                        acc.fetch_add(1, Ordering::SeqCst);
                    } else if p.try_publish(|s| *s = v).is_ok() {
                        acc.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
    stop.store(true, Ordering::SeqCst);
    let delivered = reader.join().unwrap();
    assert_eq!(delivered, accepted.load(Ordering::SeqCst));
    assert!(
        delivered >= 2 * PER as usize,
        "every Block publish delivered"
    );
}

#[test]
fn stalled_producer_gates_only_its_own_slot() {
    let mut b = RingBuilder::<u32>::new(16);
    b.consumer(&[]);
    let (p, mut cs) = b.build_multi();
    p.publish(|s| *s = 0).unwrap();
    p.publish(|s| *s = 1).unwrap();

    let claimed = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let (pa, cl, rl) = (p.clone(), claimed.clone(), release.clone());
    let stalled = thread::spawn(move || {
        pa.publish(|s| {
            cl.wait(); // seq 2 is claimed, not yet published
            rl.wait();
            *s = 2;
        })
        .unwrap();
    });
    claimed.wait();
    p.publish(|s| *s = 3).unwrap(); // later producer publishes past the gap
    p.publish(|s| *s = 4).unwrap();

    let mut seen = Vec::new();
    cs[0].poll(|v, _, _| seen.push(*v));
    assert_eq!(
        seen,
        vec![0, 1],
        "consumer stops exactly at the unpublished slot"
    );
    release.wait();
    stalled.join().unwrap();
    cs[0].poll(|v, _, _| seen.push(*v));
    assert_eq!(seen, vec![0, 1, 2, 3, 4]);
}

#[test]
fn barrier_dependency_orders_stages() {
    const N: i64 = 200_000;
    let mut b = RingBuilder::<i64>::new(256);
    let a = b.consumer(&[]);
    let bb = b.consumer(&[a]);
    let (mut p, cs) = b.build_single();
    let mut cs = cs.into_iter();
    let (mut ca, mut cb) = (cs.next().unwrap(), cs.next().unwrap());
    assert_eq!((a.index(), bb.index()), (0, 1));
    let a_seq = ca.sequence();
    let ta = thread::spawn(move || {
        drain_n(&mut ca, N as usize, |_, _, _| {
            std::hint::spin_loop(); // a slow upstream stage
        })
    });
    let tb = thread::spawn(move || {
        drain_n(&mut cb, N as usize, |v, seq, _| {
            assert_eq!(*v, seq);
            assert!(
                a_seq.get() >= seq,
                "stage B saw {seq} before stage A finished it"
            );
        })
    });
    for i in 0..N {
        p.publish(|s| *s = i).unwrap();
    }
    ta.join().unwrap();
    tb.join().unwrap();
}

#[test]
fn wait_strategies_all_deliver() {
    for wait in [
        WaitStrategy::BusySpin,
        WaitStrategy::Yield,
        WaitStrategy::backoff(),
        WaitStrategy::Blocking,
    ] {
        let mut b = RingBuilder::<u32>::new(16);
        b.consumer_with(&[], wait);
        let (mut p, mut cs) = b.build_single();
        let mut c = cs.pop().unwrap();
        let ctl = p.control();
        let reader = thread::spawn(move || {
            let mut seen = Vec::new();
            while c.wait_poll(|v, _, _| seen.push(*v)).is_ok() {}
            seen
        });
        for i in 0..20 {
            p.publish(|s| *s = i).unwrap();
            if i % 5 == 0 {
                thread::sleep(Duration::from_millis(3)); // let the waiter go idle
            }
        }
        while ctl.consumed() < 19 {
            thread::yield_now();
        }
        ctl.alert();
        assert_eq!(
            reader.join().unwrap(),
            (0..20).collect::<Vec<_>>(),
            "{wait:?}"
        );
    }
}

#[test]
fn multicast_all_consumers_see_everything_and_slowest_gates() {
    let mut b = RingBuilder::<u32>::new(8);
    for _ in 0..3 {
        b.consumer(&[]);
    }
    let (mut p, mut cs) = b.build_single();
    for i in 0..8 {
        p.publish(|s| *s = i).unwrap();
    }
    for c in cs.iter_mut().take(2) {
        let mut seen = Vec::new();
        c.poll(|v, _, _| seen.push(*v));
        assert_eq!(seen, (0..8).collect::<Vec<_>>());
    }
    assert_eq!(
        p.try_publish(|s| *s = 8),
        Err(PublishError::Full),
        "third consumer gates"
    );
    let mut seen = Vec::new();
    cs[2].poll(|v, _, _| seen.push(*v));
    assert_eq!(seen, (0..8).collect::<Vec<_>>());
    p.try_publish(|s| *s = 8).unwrap();
}

#[test]
fn shutdown_clean_via_dsl() {
    let journaled = Arc::new(AtomicI64::new(-1));
    let applied = Arc::new(Mutex::new(Vec::new()));
    let (j, a, j2) = (journaled.clone(), applied.clone(), journaled.clone());
    let (p, handle) = Disruptor::<u64>::new(64)
        .handle(vec![Box::new(move |_: &u64, seq: i64, _: bool| {
            j.store(seq, Ordering::SeqCst)
        })])
        .then(vec![Box::new(move |v: &u64, seq: i64, _: bool| {
            assert!(j2.load(Ordering::SeqCst) >= seq, "journal-before-apply");
            a.lock().unwrap().push(*v);
        })])
        .spawn_multi();
    let producers: Vec<_> = (0..2u64)
        .map(|id| {
            let p = p.clone();
            thread::spawn(move || {
                for i in 0..5_000u64 {
                    p.publish(|s| *s = id * 1_000_000 + i).unwrap();
                }
            })
        })
        .collect();
    for t in producers {
        t.join().unwrap();
    }
    handle.shutdown();
    assert_eq!(
        applied.lock().unwrap().len(),
        10_000,
        "in-flight drained before stop"
    );
    assert_eq!(journaled.load(Ordering::SeqCst), 9_999);
    assert!(p.control().is_alerted());
}

#[test]
fn multi_ring_processor_round_robins() {
    let mut rings = Vec::new();
    let mut consumers = Vec::new();
    for _ in 0..3 {
        let mut b = RingBuilder::<u32>::new(8);
        b.consumer(&[]);
        let (p, mut cs) = b.build_single();
        rings.push(p);
        consumers.push(cs.pop().unwrap());
    }
    for (r, p) in rings.iter_mut().enumerate() {
        for i in 0..3 {
            p.publish(|s| *s = r as u32 * 10 + i).unwrap();
        }
    }
    let mut mrp = MultiRingProcessor::new(consumers);
    let mut seen = Vec::new();
    assert_eq!(mrp.poll(|ring, v, _, _| seen.push((ring, *v))), 9);
    for r in 0..3 {
        let per: Vec<u32> = seen
            .iter()
            .filter(|(x, _)| *x == r)
            .map(|(_, v)| *v)
            .collect();
        assert_eq!(
            per,
            vec![r as u32 * 10, r as u32 * 10 + 1, r as u32 * 10 + 2]
        );
    }
}
