//! The event-chain digest (`spec/REPLICATION.md` 13.16): conformance vectors
//! shared with the other languages, sensitivity, continuity across a snapshot
//! restore, and cost.
//!
//! `ORDERER_GEN_DIGEST_VECTORS=DIR` writes the vector files into DIR instead of
//! checking them (run from the spec repo's reference implementation).

use orderer::harness::parse_corpus;
use orderer::recover::restore;
use orderer::{EventDigest, PartitionMap, Pipeline};
use orderer_cluster::pipeline_sm::PipelineSm;
use orderer_cluster::replica::StateMachine;
use orderer_cluster::sha256::{hex, sha256};
use orderer_core::{BookConfig, Command, FifoCore, Side, Symbol, Tif};
use std::path::PathBuf;
use std::time::Instant;

const TAG: &[u8] = b"orderer-evdigest/1";

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn corpus(name: &str) -> (BookConfig, Vec<(Symbol, Command)>) {
    let path = root()
        .join("vectors/pipeline")
        .join(format!("{name}.cmd.jsonl"));
    let c = parse_corpus(
        &std::fs::read_to_string(&path).unwrap(),
        &path.to_string_lossy(),
    )
    .unwrap();
    (c.book, c.cmds)
}

/// Run `cmds` through a fresh pipeline and return (chains, applied commands).
fn chains(book: BookConfig, cmds: &[(Symbol, Command)], partitions: u32) -> Vec<u64> {
    let (digest, handle) = EventDigest::new(partitions, &[]);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(book)
        .partitions(partitions)
        .egress(digest)
        .build()
        .unwrap();
    p.publish_batch(cmds).unwrap();
    p.drain().unwrap();
    let v = handle.values();
    p.shutdown().unwrap();
    v
}

/// The replica-level digest: SHA-256 over tag, commands applied, partitions, chains.
fn combine(iseq: u64, chains: &[u64]) -> [u8; 32] {
    let mut b = TAG.to_vec();
    b.extend_from_slice(&iseq.to_le_bytes());
    b.extend_from_slice(&(chains.len() as u32).to_le_bytes());
    for c in chains {
        b.extend_from_slice(&c.to_le_bytes());
    }
    sha256(&b)
}

fn vector_text(iseq: u64, chains: &[u64]) -> String {
    format!(
        "orderer-evdigest/1\npartitions {}\ncommands {iseq}\nchains {}\ndigest {}\n",
        chains.len(),
        chains
            .iter()
            .map(|c| format!("{c:016x}"))
            .collect::<Vec<_>>()
            .join(" "),
        hex(&combine(iseq, chains)),
    )
}

#[test]
fn chains_match_the_shared_vectors() {
    let gen = std::env::var("ORDERER_GEN_DIGEST_VECTORS").ok();
    let mut checked = 0;
    for name in ["multisymbol", "fuzz_s11"] {
        let (book, cmds) = corpus(name);
        for p in [1u32, 2, 4] {
            let text = vector_text(cmds.len() as u64, &chains(book, &cmds, p));
            let file = format!("{name}.P{p}.txt");
            match &gen {
                Some(dir) => std::fs::write(PathBuf::from(dir).join(&file), &text).unwrap(),
                None => {
                    let want = std::fs::read_to_string(
                        root().join("vectors/replication-digest").join(&file),
                    )
                    .unwrap_or_else(|e| panic!("{file}: {e}"));
                    assert_eq!(text, want, "{file}");
                    checked += 1;
                }
            }
        }
    }
    if gen.is_none() {
        assert_eq!(checked, 6);
    }
}

#[test]
fn chains_are_deterministic_and_sensitive_to_every_field() {
    let base: Vec<(Symbol, Command)> = (1..=40u64)
        .map(|i| {
            let side = if i % 2 == 0 { Side::Bid } else { Side::Ask };
            (
                (i % 5) as u32,
                Command::new(i, side, 100 + (i % 3) as i64, 1 + i % 4, Tif::Gtc),
            )
        })
        .collect();
    let book = BookConfig::default();
    let reference = chains(book, &base, 2);
    assert_eq!(reference, chains(book, &base, 2), "not deterministic");
    type Edit = fn(&mut (Symbol, Command));
    let tweak = |f: Edit| {
        let mut v = base.clone();
        f(&mut v[0]);
        chains(book, &v, 2)
    };
    fn qty(c: &mut (Symbol, Command)) {
        if let Command::New { qty, .. } = &mut c.1 {
            *qty += 1;
        }
    }
    fn price(c: &mut (Symbol, Command)) {
        if let Command::New { price, .. } = &mut c.1 {
            *price += 1;
        }
    }
    fn id(c: &mut (Symbol, Command)) {
        if let Command::New { order_id, .. } = &mut c.1 {
            *order_id += 1000;
        }
    }
    fn sym(c: &mut (Symbol, Command)) {
        c.0 += 7;
    }
    for (what, f) in [
        ("qty", qty as Edit),
        ("price", price),
        ("order id", id),
        ("symbol", sym),
    ] {
        assert_ne!(
            reference,
            tweak(f),
            "a different {what} did not change the chain"
        );
    }
    // Dropping a command changes it too.
    assert_ne!(reference, chains(book, &base[..39], 2));
}

#[test]
fn a_resting_difference_is_invisible_to_the_chain_until_it_trades() {
    // The limit of an event-stream digest, stated as a test: two replicas whose
    // books differ only in a resting order's price emit the same events until
    // something trades against it. The full-state (deep) digest sees it at once.
    let book = BookConfig::default();
    let run = |price: i64| {
        let mut sm = PipelineSm::<FifoCore>::new(
            book,
            PartitionMap::hash(1).unwrap(),
            move |init, digest| {
                let mut b = Pipeline::<FifoCore>::builder().partitions(1).egress(digest);
                if let Some(i) = init {
                    b = b.initial(i);
                }
                b.build().unwrap()
            },
        );
        let mut data = Vec::new();
        orderer_cluster::codec::encode_batch(
            &[(1, Command::new(1, Side::Ask, price, 5, Tif::Gtc))],
            &mut data,
        );
        sm.apply(1, &orderer_consensus::Entry { term: 1, data });
        (sm.digest(1).unwrap(), sm.deep_digest(1).unwrap())
    };
    let (chain_a, deep_a) = run(100);
    let (chain_b, deep_b) = run(101);
    assert_eq!(
        chain_a, chain_b,
        "the chain saw a price that has not traded"
    );
    assert_ne!(
        deep_a, deep_b,
        "the deep digest missed a different resting price"
    );
}

#[test]
fn a_restored_pipeline_continues_the_same_chain() {
    let (book, cmds) = corpus("fuzz_s11");
    let p = 2u32;
    let whole = chains(book, &cmds, p);

    // First half, then snapshot and chains.
    let (d1, h1) = EventDigest::new(p, &[]);
    let mut first = Pipeline::<FifoCore>::builder()
        .book_config(book)
        .partitions(p)
        .egress(d1)
        .build()
        .unwrap();
    first.publish_batch(&cmds[..1500]).unwrap();
    first.drain().unwrap();
    let snap = first.snapshot().unwrap();
    let mid = h1.values();
    first.shutdown().unwrap();

    // A new pipeline from the snapshot, chains carried over, runs the rest.
    let map = PartitionMap::hash(p).unwrap();
    let (_, cores) = restore::<FifoCore>(book, &map, Some(&snap)).unwrap();
    let (d2, h2) = EventDigest::new(p, &mid);
    let mut second = Pipeline::<FifoCore>::builder()
        .book_config(book)
        .partition_map(map)
        .egress(d2)
        .initial(orderer::Initial {
            cores,
            next_iseq: snap.iseq + 1,
        })
        .build()
        .unwrap();
    second.publish_batch(&cmds[1500..]).unwrap();
    second.drain().unwrap();
    assert_eq!(h2.values(), whole, "the chain broke across a snapshot");
    second.shutdown().unwrap();
}

#[test]
fn the_digest_costs_a_barrier_not_a_snapshot() {
    let book = BookConfig::default();
    let mut sm =
        PipelineSm::<FifoCore>::new(book, PartitionMap::hash(2).unwrap(), move |init, digest| {
            let mut b = Pipeline::<FifoCore>::builder().partitions(2).egress(digest);
            if let Some(i) = init {
                b = b.initial(i);
            }
            b.build().unwrap()
        });
    // 300k resting orders over 8 symbols: bids and asks that never cross.
    let cmds: Vec<(Symbol, Command)> = (1..=300_000u64)
        .map(|i| {
            let (side, price) = if i % 2 == 0 {
                (Side::Bid, 1000 - (i % 500) as i64)
            } else {
                (Side::Ask, 2000 + (i % 500) as i64)
            };
            ((i % 8) as u32, Command::new(i, side, price, 1, Tif::Gtc))
        })
        .collect();
    let mut payload = Vec::new();
    orderer_cluster::codec::encode_batch(&cmds, &mut payload);
    sm.apply(
        1,
        &orderer_consensus::Entry {
            term: 1,
            data: payload,
        },
    );
    let t = Instant::now();
    let cheap = sm.digest(1).unwrap();
    let cheap_t = t.elapsed();
    let t = Instant::now();
    let deep = sm.deep_digest(1).unwrap();
    let deep_t = t.elapsed();
    println!("digest {cheap_t:?}, deep digest {deep_t:?} (300k resting orders)");
    assert_ne!(cheap, deep);
    assert!(
        cheap_t < deep_t / 10,
        "digest {cheap_t:?} is not much cheaper than deep {deep_t:?}"
    );
    assert!(cheap_t.as_millis() < 100, "digest took {cheap_t:?}");
}
