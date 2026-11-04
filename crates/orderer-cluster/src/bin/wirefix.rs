//! wirefix — write wire-format fixtures for ports of the replication runtime.
//!
//!   wirefix OUT_DIR
//!
//! Writes, from this implementation's own codec, WAL and snapshot store:
//!   msgs.txt        `hex | message` for every protocol message kind
//!   persists.txt    `hex | persist` (the WAL frame body of one write)
//!   batches.txt     `hex | session client seq | commands` (log entry payloads)
//!   wal-*.bin       WAL files, intact and damaged, with wal-*.expect giving the
//!                   state recovery must reach (or `corrupt` / `badheader`)
//!   snap-*.snap     snapshot files, with snap-*.expect
//! A port reads each file and must decode, re-encode and recover identically.

use orderer::journal::crc32c;
use orderer_cluster::codec::{encode_batch, encode_msg, encode_persist, encode_session_batch};
use orderer_cluster::snapstore::SnapStore;
use orderer_cluster::wal::{Wal, WalError};
use orderer_consensus::trace::{fmt_disk, fmt_msg};
use orderer_consensus::{Entry, Msg, Persist};
use orderer_core::{Command, OType, Side, Tif};
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

fn hex(b: &[u8]) -> String {
    b.iter().fold(String::new(), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    })
}

fn e(term: u32, bytes: &[u8]) -> Entry {
    Entry {
        term,
        data: bytes.to_vec(),
    }
}

fn ent_text(es: &[Entry]) -> String {
    if es.is_empty() {
        return "-".into();
    }
    es.iter()
        .map(|x| {
            format!(
                "{}:{}",
                x.term,
                if x.data.is_empty() {
                    "-".into()
                } else {
                    hex(&x.data)
                }
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn persist_text(p: &Persist) -> String {
    format!(
        "{} {} {} {} {}",
        p.term,
        p.voted_for.map_or("-".into(), |v| v.to_string()),
        p.from,
        p.compact.map_or("-".into(), |(i, t)| format!("{i}:{t}")),
        ent_text(&p.entries)
    )
}

fn main() {
    let out = std::env::args().nth(1).expect("usage: wirefix OUT_DIR");
    let out = Path::new(&out);
    fs::create_dir_all(out).unwrap();

    // ---- messages ----
    let msgs = vec![
        Msg::Vote {
            term: 9,
            last_index: 1 << 40,
            last_term: 3,
            pre: true,
        },
        Msg::Vote {
            term: 1,
            last_index: 0,
            last_term: 0,
            pre: false,
        },
        Msg::VoteReply {
            term: 9,
            granted: true,
            pre: false,
        },
        Msg::VoteReply {
            term: 4_000_000_000,
            granted: false,
            pre: true,
        },
        Msg::Append {
            term: 4,
            prev_index: 7,
            prev_term: 3,
            entries: vec![],
            commit: 5,
        },
        Msg::Append {
            term: 4,
            prev_index: 7,
            prev_term: 3,
            entries: vec![e(3, &[]), e(4, &[1, 2, 3]), e(4, &[0xff; 40])],
            commit: 6,
        },
        Msg::AppendReply {
            term: 4,
            success: true,
            index: 99,
        },
        Msg::AppendReply {
            term: 4,
            success: false,
            index: 0,
        },
    ];
    let mut s = String::new();
    for m in &msgs {
        let mut b = Vec::new();
        encode_msg(m, &mut b);
        let _ = writeln!(s, "{} | {}", hex(&b), fmt_msg(m));
    }
    fs::write(out.join("msgs.txt"), s).unwrap();

    // ---- persists ----
    let persists = vec![
        Persist {
            token: 0,
            term: 5,
            voted_for: Some(2),
            from: 11,
            entries: vec![e(4, &[9]), e(5, &[])],
            compact: Some((10, 4)),
        },
        Persist {
            token: 0,
            term: 1,
            voted_for: None,
            from: 1,
            entries: vec![],
            compact: None,
        },
        Persist {
            token: 0,
            term: 7,
            voted_for: Some(0),
            from: 4,
            entries: vec![e(7, b"hello")],
            compact: None,
        },
    ];
    let mut s = String::new();
    for p in &persists {
        let mut b = Vec::new();
        encode_persist(p, &mut b);
        let _ = writeln!(s, "{} | {}", hex(&b), persist_text(p));
    }
    fs::write(out.join("persists.txt"), s).unwrap();

    // ---- batches: `hex | client seq | sym:kind:side:otype:tif:id:price:qty ...` ----
    let cmds = vec![
        (7u32, Command::new(1, Side::Ask, 100, 10, Tif::Gtc)),
        (9, Command::new(2, Side::Bid, 101, 4, Tif::Ioc)),
        (7, Command::Cancel { order_id: 1 }),
        (
            3,
            Command::Replace {
                order_id: 2,
                price: 99,
                qty: 5,
            },
        ),
        (
            3,
            Command::New {
                order_id: 8,
                side: Side::Bid,
                otype: OType::Market,
                price: 0,
                qty: 3,
                tif: Tif::Gtc,
            },
        ),
        (4, Command::new(11, Side::Bid, -5, 1, Tif::PostOnly)),
        (4, Command::new(12, Side::Ask, 7, 2, Tif::Fok)),
    ];
    let desc = |c: &(u32, Command)| match c.1 {
        Command::New {
            order_id,
            side,
            otype,
            price,
            qty,
            tif,
        } => format!(
            "{}:new:{}:{}:{}:{order_id}:{price}:{qty}",
            c.0,
            if side == Side::Bid { "bid" } else { "ask" },
            if otype == OType::Market {
                "market"
            } else {
                "limit"
            },
            match tif {
                Tif::Gtc => "gtc",
                Tif::Ioc => "ioc",
                Tif::Fok => "fok",
                Tif::PostOnly => "post_only",
            }
        ),
        Command::Cancel { order_id } => format!("{}:cancel:-:-:-:{order_id}:0:0", c.0),
        Command::Replace {
            order_id,
            price,
            qty,
        } => format!("{}:replace:-:-:-:{order_id}:{price}:{qty}", c.0),
    };
    let text: Vec<String> = cmds.iter().map(desc).collect();
    let mut s = String::new();
    let mut b = Vec::new();
    encode_batch(&cmds, &mut b);
    let _ = writeln!(s, "{} | - 0 | {}", hex(&b), text.join(" "));
    let mut b = Vec::new();
    encode_session_batch(42, 9, &cmds[..3], &mut b);
    let _ = writeln!(s, "{} | 42 9 | {}", hex(&b), text[..3].join(" "));
    let _ = writeln!(s, "{} | - 0 | ", hex(&[]));
    fs::write(out.join("batches.txt"), s).unwrap();

    // ---- WAL ----
    let work = std::env::temp_dir().join(format!("wirefix-{}", std::process::id()));
    let _ = fs::remove_dir_all(&work);
    let rec = |p: &Persist| p.clone();
    let writes = vec![
        Persist {
            token: 0,
            term: 1,
            voted_for: Some(1),
            from: 1,
            entries: vec![e(1, &[1]), e(1, &[2]), e(1, &[3])],
            compact: None,
        },
        Persist {
            token: 0,
            term: 2,
            voted_for: Some(2),
            from: 3,
            entries: vec![e(2, &[9, 9])],
            compact: None,
        },
        Persist {
            token: 0,
            term: 2,
            voted_for: Some(2),
            from: 4,
            entries: vec![e(2, &[]), e(2, &[4])],
            compact: None,
        },
        Persist {
            token: 0,
            term: 3,
            voted_for: None,
            from: 6,
            entries: vec![],
            compact: Some((3, 2)),
        },
    ];
    let dir = work.join("good");
    let (mut wal, _, _) = Wal::open(&dir).unwrap();
    for p in &writes {
        wal.append(&rec(p));
    }
    wal.sync().unwrap();
    drop(wal);
    let good = fs::read(dir.join("raft.wal")).unwrap();

    let expect_for = |bytes: &[u8], tag: &str| -> String {
        let d = work.join(format!("probe-{tag}"));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        fs::write(d.join("raft.wal"), bytes).unwrap();
        match Wal::open(&d) {
            Ok((_, disk, rep)) => format!(
                "ok {} frames={} torn={}",
                fmt_disk(&disk),
                rep.frames,
                rep.torn_bytes
            ),
            Err(WalError::Corrupt { offset }) => format!("corrupt {offset}"),
            Err(WalError::BadHeader) => "badheader".into(),
            Err(WalError::Io(e)) => panic!("io {e}"),
        }
    };
    let mut variants: Vec<(String, Vec<u8>)> = vec![("good".into(), good.clone())];
    // frame boundaries
    let mut bounds = vec![8usize];
    let mut off = 8;
    while off < good.len() {
        let len = u32::from_le_bytes(good[off..off + 4].try_into().unwrap()) as usize;
        off += 8 + len;
        bounds.push(off);
    }
    for (i, w) in bounds.windows(2).enumerate() {
        variants.push((format!("torn-mid{i}"), good[..(w[0] + w[1]) / 2].to_vec()));
        variants.push((format!("torn-hdr{i}"), good[..w[0] + 5].to_vec()));
    }
    for z in [1usize, 8, 17, 300, 5000] {
        let mut v = good.clone();
        v.extend(std::iter::repeat(0).take(z));
        variants.push((format!("zeros{z}"), v));
    }
    let mut mid = good.clone();
    mid[bounds[1] + 12] ^= 0xff;
    variants.push(("corrupt-mid".into(), mid));
    let mut last = good.clone();
    let n = last.len();
    last[n - 1] ^= 1;
    variants.push(("flip-last".into(), last));
    variants.push(("badheader".into(), b"not a wal at all".to_vec()));
    for (name, bytes) in &variants {
        fs::write(out.join(format!("wal-{name}.bin")), bytes).unwrap();
        fs::write(
            out.join(format!("wal-{name}.expect")),
            expect_for(bytes, name) + "\n",
        )
        .unwrap();
    }

    // ---- snapshots ----
    let store = SnapStore::open(work.join("snaps")).unwrap();
    let data: Vec<u8> = (0..300u32).map(|i| (i * 7) as u8).collect();
    store.put(42, 5, &data).unwrap();
    store.put(7, 2, b"").unwrap();
    for (idx, d) in [(42u64, &data[..]), (7, &b""[..])] {
        let name = format!("snap-{idx:020}.snap");
        fs::copy(work.join("snaps").join(&name), out.join(&name)).unwrap();
        let term = if idx == 42 { 5 } else { 2 };
        fs::write(
            out.join(format!("snap-{idx}.expect")),
            format!("{idx} {term} {} {:08x} {}\n", d.len(), crc32c(d), hex(d)),
        )
        .unwrap();
    }
    let _ = fs::remove_dir_all(&work);
    println!("wrote fixtures to {}", out.display());
}
