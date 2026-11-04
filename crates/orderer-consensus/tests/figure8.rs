use orderer_consensus::Mutation;

mod common;

/// Raft paper figure 8: an entry from an earlier term that is on a majority
/// is still not committed until a current-term entry on top of it is.
#[test]
fn old_term_entry_on_a_majority_is_not_committed() {
    assert_eq!(common::figure8(Mutation::None), 0);
}
