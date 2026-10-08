//! A5 — zero steady-state allocation (plan §5.3 `no_alloc.rs`).
//!
//! A counting global allocator (its own test binary — the allocator is
//! per-binary) wraps `System`. After a warmup that creates every book and
//! fills every pool, a million more commands through a P=2 pipeline with
//! binary command journals must allocate **nothing**, on any thread: no
//! producer, router, journal, engine, egress or I/O-thread allocation.
//!
//! The `unsafe` here is the `GlobalAlloc` contract, in a test — library
//! code stays `forbid(unsafe_code)`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static COUNTING: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        System.alloc(l)
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        System.alloc_zeroed(l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        System.realloc(p, l, n)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
}

#[global_allocator]
static A: Counting = Counting;

use orderer::*;
use orderer_core::*;

/// Non-crossing churn on 16 books: rests, replaces, cancels, IOCs that
/// cross — every hot path, bounded book sizes.
fn workload(n: usize, seed: u64) -> Vec<(Symbol, Command)> {
    let mut x = seed | 1;
    let mut next = || {
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    };
    (0..n)
        .map(|_| {
            let sym = (next() % 16) as Symbol;
            let id = next() % 512;
            let (side, px) = if next() % 2 == 0 {
                (Side::Bid, 900 + (next() % 100) as i64)
            } else {
                (Side::Ask, 1001 + (next() % 100) as i64)
            };
            let qty = next() % 50 + 1;
            let cmd = match next() % 10 {
                0..=3 => Command::new(id, side, px, qty, Tif::Gtc),
                4 => Command::new(
                    id,
                    side,
                    if side == Side::Bid { 1050 } else { 950 },
                    qty,
                    Tif::Ioc,
                ),
                5..=6 => Command::cancel(id),
                _ => Command::replace(id, px, qty),
            };
            (sym, cmd)
        })
        .collect()
}

#[test]
fn steady_state_allocates_nothing() {
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_secs(120));
        eprintln!("watchdog: no_alloc exceeded 120s");
        std::process::abort();
    });
    let cfg = BookConfig {
        price_min: 1,
        price_max: 2_000,
        max_orders: 1024,
        index: IndexKind::Ladder,
    };
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("no-alloc");
    let _ = std::fs::remove_dir_all(&dir);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(2)
        .journal(JournalConfig {
            dir: dir.clone(),
            format: JournalFormat::Binary,
            fsync: FsyncPolicy::every_n(1024),
            events: false,
            append: false,
        })
        .build()
        .unwrap();
    let warm = workload(300_000, 7);
    let measured = workload(1_000_000, 8);

    // warmup: every book exists, pools and channel waiter lists sized
    p.publish_batch(&warm).unwrap();
    p.drain().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));

    COUNTING.store(true, Ordering::SeqCst);
    for chunk in measured.chunks(64) {
        p.publish_batch(chunk).unwrap();
    }
    p.drain().unwrap();
    COUNTING.store(false, Ordering::SeqCst);
    let allocs = ALLOCS.load(Ordering::SeqCst);

    p.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(allocs, 0, "allocations in the steady state");
}
