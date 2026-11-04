//! Conformance traces: a text format that records every input a [`Node`]
//! received and every effect and state it produced, so another
//! implementation of the protocol can replay the inputs and must reproduce
//! the outputs exactly (`vectors/replication/` in the spec repo).
//!
//! One trace is a sequence of *stanzas*. A stanza starts with a `node` line
//! (a fresh node built from a disk image) and continues with `in` lines
//! (inputs) and `out`/`state` lines (what the node produced after the inputs
//! so far). Where a node crashed and restarted, the trace starts a new
//! stanza. Everything is deterministic, including the node's election-timeout
//! jitter, which comes from the xorshift generator seeded in the `node` line.
//!
//! ```text
//! node id=1 peers=0,1,2 election=10 heartbeat=2 batch=64 prevote=1 seed=42 disk=3:0:0:0:1:-,2:6162
//! in tick
//! in step 0 vote 4 7 3 1
//! out send 0 votereply 0 1 1
//! state role=Follower term=3 leader=- commit=0 applied=0 base=0 last=2
//! ```
//!
//! `disk=term:voted(-|id):base:base_term:entries` with entries `term:hex`
//! (hex `-` for empty data) separated by commas, or `-` for none.

use crate::core::*;
use std::fmt::Write as _;

pub fn hex(d: &[u8]) -> String {
    if d.is_empty() {
        return "-".into();
    }
    d.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn unhex(s: &str) -> Result<Vec<u8>, String> {
    if s == "-" {
        return Ok(Vec::new());
    }
    if s.len() % 2 != 0 {
        return Err(format!("odd hex {s}"));
    }
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

fn entry(e: &Entry) -> String {
    format!("{}:{}", e.term, hex(&e.data))
}

fn entries(es: &[Entry]) -> String {
    if es.is_empty() {
        "-".into()
    } else {
        es.iter().map(entry).collect::<Vec<_>>().join(",")
    }
}

fn parse_entries(s: &str) -> Result<Vec<Entry>, String> {
    if s == "-" {
        return Ok(Vec::new());
    }
    s.split(',')
        .map(|p| {
            let (t, d) = p.split_once(':').ok_or("entry")?;
            Ok(Entry {
                term: t.parse().map_err(|_| "entry term")?,
                data: unhex(d)?,
            })
        })
        .collect()
}

fn b(v: bool) -> u8 {
    u8::from(v)
}

pub fn fmt_msg(m: &Msg) -> String {
    match m {
        Msg::Vote {
            term,
            last_index,
            last_term,
            pre,
        } => format!("vote {term} {last_index} {last_term} {}", b(*pre)),
        Msg::VoteReply { term, granted, pre } => {
            format!("votereply {term} {} {}", b(*granted), b(*pre))
        }
        Msg::Append {
            term,
            prev_index,
            prev_term,
            entries: es,
            commit,
        } => format!(
            "append {term} {prev_index} {prev_term} {commit} {}",
            entries(es)
        ),
        Msg::AppendReply {
            term,
            success,
            index,
        } => {
            format!("appendreply {term} {} {index}", b(*success))
        }
    }
}

pub fn parse_msg(w: &[&str]) -> Result<Msg, String> {
    let n = |i: usize| -> Result<u64, String> {
        w.get(i)
            .ok_or_else(|| "short message".to_string())?
            .parse::<u64>()
            .map_err(|e| e.to_string())
    };
    Ok(match *w.first().ok_or("empty message")? {
        "vote" => Msg::Vote {
            term: n(1)? as u32,
            last_index: n(2)?,
            last_term: n(3)? as u32,
            pre: n(4)? == 1,
        },
        "votereply" => Msg::VoteReply {
            term: n(1)? as u32,
            granted: n(2)? == 1,
            pre: n(3)? == 1,
        },
        "append" => Msg::Append {
            term: n(1)? as u32,
            prev_index: n(2)?,
            prev_term: n(3)? as u32,
            commit: n(4)?,
            entries: parse_entries(w.get(5).ok_or("append entries")?)?,
        },
        "appendreply" => Msg::AppendReply {
            term: n(1)? as u32,
            success: n(2)? == 1,
            index: n(3)?,
        },
        other => return Err(format!("unknown message {other}")),
    })
}

pub fn fmt_effect(e: &Effect) -> String {
    match e {
        Effect::Send { to, msg } => format!("send {to} {}", fmt_msg(msg)),
        Effect::Persist(p) => format!(
            "persist {} {} {} {} {} {}",
            p.token,
            p.term,
            p.voted_for.map_or("-".into(), |v| v.to_string()),
            p.from,
            p.compact.map_or("-".into(), |(i, t)| format!("{i}:{t}")),
            entries(&p.entries)
        ),
        Effect::Apply { index, entry: e } => format!("apply {index} {}", entry(e)),
        Effect::NeedSnapshot { to, index, term } => format!("needsnap {to} {index} {term}"),
        Effect::Restore { index, term } => format!("restore {index} {term}"),
    }
}

pub fn fmt_state(n: &Node) -> String {
    format!(
        "state role={:?} term={} leader={} commit={} applied={} base={} last={}",
        n.role(),
        n.term(),
        n.leader().map_or("-".into(), |l| l.to_string()),
        n.commit_index(),
        n.applied_index(),
        n.base(),
        n.last_index()
    )
}

pub fn fmt_disk(d: &Disk) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        d.term,
        d.voted_for.map_or("-".into(), |v| v.to_string()),
        d.base,
        d.base_term,
        entries(&d.log)
    )
}

pub fn fmt_node_line(id: NodeId, peers: &[NodeId], cfg: &Config, seed: u64, disk: &Disk) -> String {
    let mut p = peers.to_vec();
    p.sort_unstable();
    format!(
        "node id={id} peers={} election={} heartbeat={} batch={} prevote={} seed={seed} disk={}",
        p.iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(","),
        cfg.election_ticks,
        cfg.heartbeat_ticks,
        cfg.max_batch,
        b(cfg.pre_vote),
        fmt_disk(disk)
    )
}

fn field<'a>(line: &'a str, key: &str) -> Result<&'a str, String> {
    line.split_whitespace()
        .find_map(|kv| kv.strip_prefix(&format!("{key}=")))
        .ok_or_else(|| format!("missing {key}"))
}

fn parse_node_line(line: &str) -> Result<Node, String> {
    let num = |k: &str| {
        field(line, k)?
            .parse::<u64>()
            .map_err(|e| format!("{k}: {e}"))
    };
    let peers: Vec<NodeId> = field(line, "peers")?
        .split(',')
        .map(|p| p.parse().map_err(|_| "peer".to_string()))
        .collect::<Result<_, _>>()?;
    let cfg = Config {
        election_ticks: num("election")? as u32,
        heartbeat_ticks: num("heartbeat")? as u32,
        max_batch: num("batch")? as usize,
        pre_vote: num("prevote")? == 1,
        mutation: Mutation::None,
    };
    let d: Vec<&str> = field(line, "disk")?.splitn(5, ':').collect();
    if d.len() != 5 {
        return Err("disk".into());
    }
    let disk = Disk {
        term: d[0].parse().map_err(|_| "disk term")?,
        voted_for: if d[1] == "-" {
            None
        } else {
            Some(d[1].parse().map_err(|_| "disk vote")?)
        },
        base: d[2].parse().map_err(|_| "disk base")?,
        base_term: d[3].parse().map_err(|_| "disk base_term")?,
        log: parse_entries(d[4])?,
    };
    Ok(Node::restart(
        num("id")? as NodeId,
        peers,
        cfg,
        num("seed")?,
        disk,
    ))
}

/// Replay a trace against this implementation. `Err` names the first line
/// where the node's effects or state differ from the recorded ones.
pub fn replay(text: &str) -> Result<usize, String> {
    let mut node: Option<Node> = None;
    let mut expected: Vec<String> = Vec::new();
    let mut checked = 0usize;
    let mut pending_inputs = 0usize;
    for (ln, line) in text.lines().enumerate().map(|(i, l)| (i + 1, l)) {
        let w: Vec<&str> = line.split_whitespace().collect();
        let err = |m: String| format!("line {ln}: {m}");
        match w.first().copied() {
            None | Some("#") => {}
            Some("node") => {
                if pending_inputs > 0 || !expected.is_empty() {
                    return Err(err("stanza ended with unchecked inputs".into()));
                }
                node = Some(parse_node_line(line).map_err(&err)?);
            }
            Some("in") => {
                let n = node
                    .as_mut()
                    .ok_or_else(|| err("input before node".into()))?;
                pending_inputs += 1;
                match w.get(1).copied() {
                    Some("tick") => n.tick(),
                    Some("step") => {
                        let from: NodeId = w[2].parse().map_err(|_| err("from".into()))?;
                        n.step(from, parse_msg(&w[3..]).map_err(&err)?);
                    }
                    Some("propose") => {
                        let data = unhex(w.get(2).ok_or_else(|| err("propose data".into()))?)
                            .map_err(&err)?;
                        let _ = n.propose(data);
                    }
                    Some("replicate") => n.replicate(),
                    Some("persisted") => {
                        n.persisted(w[2].parse().map_err(|_| err("token".into()))?)
                    }
                    Some("compact") => n.compact(w[2].parse().map_err(|_| err("index".into()))?),
                    Some("install") => n.install_snapshot(
                        w[2].parse().map_err(|_| err("from".into()))?,
                        w[3].parse().map_err(|_| err("term".into()))?,
                        w[4].parse().map_err(|_| err("index".into()))?,
                        w[5].parse().map_err(|_| err("last_term".into()))?,
                    ),
                    other => return Err(err(format!("unknown input {other:?}"))),
                }
            }
            Some("out") | Some("state") => expected.push(line.to_string()),
            Some(other) => return Err(err(format!("unknown line kind {other}"))),
        }
        // A `state` line closes a drain point: compare everything since the
        // last one.
        if w.first() == Some(&"state") {
            let n = node
                .as_mut()
                .ok_or_else(|| err("state before node".into()))?;
            let mut got: Vec<String> = n
                .take_effects()
                .iter()
                .map(|e| format!("out {}", fmt_effect(e)))
                .collect();
            got.push(fmt_state(n));
            if got != expected {
                let i = got
                    .iter()
                    .zip(&expected)
                    .position(|(a, b)| a != b)
                    .unwrap_or(got.len().min(expected.len()));
                return Err(err(format!(
                    "output differs at item {i}:\n  want: {}\n  got:  {}",
                    expected.get(i).map_or("<end>", String::as_str),
                    got.get(i).map_or("<end>", String::as_str)
                )));
            }
            checked += 1;
            expected.clear();
            pending_inputs = 0;
        }
    }
    Ok(checked)
}
