//! Directed protocol scenarios shared by tests.
#![allow(dead_code)]

use orderer_consensus::{Config, Disk, Effect, Entry, Index, Msg, Mutation, Node, Role};

pub fn entry(term: u32, tag: &str) -> Entry {
    Entry {
        term,
        data: tag.as_bytes().to_vec(),
    }
}

/// Complete every pending write on `n`, returning the effects that released.
pub fn flush(n: &mut Node) -> Vec<Effect> {
    let mut all = Vec::new();
    loop {
        let fx = n.take_effects();
        let tokens: Vec<u64> = fx
            .iter()
            .filter_map(|e| {
                if let Effect::Persist(p) = e {
                    Some(p.token)
                } else {
                    None
                }
            })
            .collect();
        all.extend(fx);
        match tokens.last() {
            Some(&t) => n.persisted(t),
            None => return all,
        }
    }
}

/// The figure-8 hazard, driven by hand on a five-node cluster.
///
/// Node 0 holds `[t1, t2]` (the term-2 entry reached only a minority) and
/// wins term 4. Two followers then acknowledge up to index 2: a majority
/// holds the term-2 entry, but it is **not** committed, because a node
/// with a term-3 entry at index 2 can still win and overwrite it.
///
/// Returns node 0's commit index after those acknowledgements. A correct
/// protocol returns 0. Afterwards it checks that the leader's no-op, once
/// acknowledged, commits everything.
pub fn figure8(m: Mutation) -> Index {
    let cfg = Config {
        mutation: m,
        ..Config::default()
    };
    let disk = Disk {
        term: 3,
        voted_for: None,
        log: vec![entry(1, "a"), entry(2, "b")],
    };
    let mut n = Node::restart(0, vec![0, 1, 2, 3, 4], cfg, 1, disk);

    // Election timeout -> pre-vote round.
    let mut pre = false;
    for _ in 0..40 {
        n.tick();
        pre |= flush(&mut n).iter().any(|e| {
            matches!(
                e,
                Effect::Send {
                    msg: Msg::Vote { pre: true, .. },
                    ..
                }
            )
        });
        if pre {
            break;
        }
    }
    assert!(pre, "node never started a pre-vote");
    for from in [1, 2] {
        n.step(
            from,
            Msg::VoteReply {
                term: 4,
                granted: true,
                pre: true,
            },
        );
    }
    assert_eq!(n.role(), Role::Candidate);
    flush(&mut n);
    for from in [1, 2] {
        n.step(
            from,
            Msg::VoteReply {
                term: 4,
                granted: true,
                pre: false,
            },
        );
    }
    assert_eq!(n.role(), Role::Leader);
    assert_eq!(n.term(), 4);
    flush(&mut n);
    assert_eq!(
        n.log().len(),
        3,
        "leader appends its term-4 no-op at index 3"
    );

    for from in [1, 2] {
        n.step(
            from,
            Msg::AppendReply {
                term: 4,
                success: true,
                index: 2,
            },
        );
    }
    flush(&mut n);
    let after_old = n.commit_index();
    if after_old != 0 {
        return after_old;
    }

    // Acknowledging the term-4 no-op commits it and everything before it.
    for from in [1, 2] {
        n.step(
            from,
            Msg::AppendReply {
                term: 4,
                success: true,
                index: 3,
            },
        );
    }
    flush(&mut n);
    assert_eq!(
        n.commit_index(),
        3,
        "no-op ack must commit the whole prefix"
    );
    0
}
