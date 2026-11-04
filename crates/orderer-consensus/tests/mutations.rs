//! The simulator is only worth trusting if it fails on a broken protocol.
//! Each test enables one deliberate bug (feature `mutations`) and requires
//! the simulator to flag it within a bounded seed range.

use orderer_consensus::sim::{run, SimConfig};
use orderer_consensus::Mutation;

fn caught(m: Mutation, seeds: u64) -> Option<String> {
    let mut cfg = SimConfig::default();
    cfg.node_cfg.mutation = m;
    (1..=seeds).find_map(|s| run(s, &cfg).err())
}

#[test]
fn baseline_is_clean() {
    assert_eq!(caught(Mutation::None, 40), None);
}

#[test]
fn commit_on_one_ack_is_caught() {
    let e = caught(Mutation::CommitOnOneAck, 300).expect("simulator missed CommitOnOneAck");
    eprintln!("caught: {e}");
}

#[test]
fn skipping_the_up_to_date_check_is_caught() {
    let e = caught(Mutation::SkipUpToDateCheck, 600).expect("simulator missed SkipUpToDateCheck");
    eprintln!("caught: {e}");
}

#[test]
fn double_voting_is_caught() {
    let e = caught(Mutation::IgnoreVotedFor, 600).expect("simulator missed IgnoreVotedFor");
    eprintln!("caught: {e}");
}

// Not covered: the "commit only current-term entries by counting" rule (Raft
// figure 8). Dropping it was not found in 3000 five-node seeds, because the
// leader's no-op makes the unsafe interleaving rare. Recorded in spec/REPLICATION.md §10.
