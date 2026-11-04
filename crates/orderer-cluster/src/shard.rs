//! Scale-out: several independent replicated groups, each owning a disjoint
//! set of symbols (`spec/REPLICATION.md` §9, decision D1).
//!
//! One replicated log is bounded by what its leader can replicate. Sharding
//! multiplies that bound: every group has its own leader, log, followers and
//! fsync stream, so groups share nothing and fail independently. What it
//! costs is the single global ingress order: the commands of one symbol are
//! totally ordered (they all go to one group, which is matcher's whole
//! contract), but there is no order between symbols in different groups. That
//! is the same "no ordering across partitions" rule as `ROUTING.md`.

use crate::client::{Client, ClientError};
use crate::codec::encode_batch;
use orderer_consensus::Index;
use orderer_core::{Command, Symbol};

/// Which group owns a symbol. The mix is deliberately different from the
/// pipeline's own symbol-to-partition hash, so the symbols of one group still
/// spread evenly over that group's partitions.
#[derive(Clone, Copy, Debug)]
pub struct ShardMap {
    groups: u32,
}

impl ShardMap {
    pub fn new(groups: u32) -> ShardMap {
        assert!(groups >= 1, "at least one group");
        ShardMap { groups }
    }

    pub fn groups(&self) -> u32 {
        self.groups
    }

    pub fn group_of(&self, sym: Symbol) -> u32 {
        // splitmix64 finalizer
        let mut z = u64::from(sym).wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        (z % u64::from(self.groups)) as u32
    }

    /// Split a batch into one sub-batch per group. Each symbol's commands
    /// keep their relative order.
    pub fn split(&self, cmds: &[(Symbol, Command)]) -> Vec<Vec<(Symbol, Command)>> {
        let mut out = vec![Vec::new(); self.groups as usize];
        for c in cmds {
            out[self.group_of(c.0) as usize].push(*c);
        }
        out
    }
}

/// A client for a sharded cluster: one leader-following [`Client`] per group.
pub struct ShardedClient {
    map: ShardMap,
    groups: Vec<Client>,
}

impl ShardedClient {
    pub fn new(groups: Vec<Client>) -> ShardedClient {
        ShardedClient {
            map: ShardMap::new(groups.len() as u32),
            groups,
        }
    }

    pub fn map(&self) -> ShardMap {
        self.map
    }

    /// Submit a batch, split by group. Returns each group's outcome (`None`
    /// for a group the batch had no commands for). A failure in one group
    /// does not undo another's commit: the caller sees exactly which
    /// sub-batches are committed.
    pub fn propose(
        &mut self,
        cmds: &[(Symbol, Command)],
    ) -> Vec<Option<Result<Index, ClientError>>> {
        let parts = self.map.split(cmds);
        let mut payload = Vec::new();
        parts
            .iter()
            .zip(self.groups.iter_mut())
            .map(|(part, client)| {
                if part.is_empty() {
                    return None;
                }
                payload.clear();
                encode_batch(part, &mut payload);
                Some(client.propose(&payload))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orderer_core::{Side, Tif};

    #[test]
    fn every_symbol_has_one_stable_group_and_groups_are_balanced() {
        let m = ShardMap::new(4);
        let mut counts = [0usize; 4];
        for s in 0..40_000u32 {
            let g = m.group_of(s);
            assert_eq!(g, m.group_of(s));
            counts[g as usize] += 1;
        }
        for c in counts {
            assert!((9_000..=11_000).contains(&c), "unbalanced: {counts:?}");
        }
    }

    #[test]
    fn a_groups_symbols_still_spread_over_its_partitions() {
        // If the group hash correlated with the pipeline's partition hash, a
        // group's symbols would pile onto one partition.
        let m = ShardMap::new(2);
        let mut grid = [[0usize; 2]; 2];
        for s in 0..20_000u32 {
            grid[m.group_of(s) as usize][orderer::hash_partition(s, 2) as usize] += 1;
        }
        for row in grid {
            for c in row {
                assert!((4_000..=6_000).contains(&c), "skewed: {grid:?}");
            }
        }
    }

    #[test]
    fn split_keeps_each_symbols_order_and_loses_nothing() {
        let m = ShardMap::new(3);
        let cmds: Vec<_> = (0..300u64)
            .map(|i| {
                (
                    (i % 17) as u32,
                    Command::new(i + 1, Side::Bid, 100, 1, Tif::Gtc),
                )
            })
            .collect();
        let parts = m.split(&cmds);
        assert_eq!(parts.iter().map(Vec::len).sum::<usize>(), cmds.len());
        for (g, part) in parts.iter().enumerate() {
            assert!(part.iter().all(|c| m.group_of(c.0) as usize == g));
            // order within the group is the original relative order
            let ids: Vec<u64> = part
                .iter()
                .map(|c| match c.1 {
                    Command::New { order_id, .. } => order_id,
                    _ => unreachable!(),
                })
                .collect();
            assert!(ids.windows(2).all(|w| w[0] < w[1]));
        }
    }
}
