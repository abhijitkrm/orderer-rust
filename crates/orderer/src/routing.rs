//! Symbol → partition routing (spec/ROUTING.md).

use std::collections::HashMap;
use std::fmt;

use orderer_core::jsonflat::{get_str, get_u64};
use orderer_core::Symbol;

/// Largest partition count the spec allows.
pub const MAX_PARTITIONS: u32 = 1024;

/// Symbols below this route through a dense table (one load); others
/// through a hash map. Instrument ids are usually small and dense.
const DENSE: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingError(pub String);

impl fmt::Display for RoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RoutingError {}

/// spec/ROUTING.md §2 — multiply-shift Fibonacci hashing.
#[inline]
pub fn hash_partition(symbol: Symbol, partitions: u32) -> u32 {
    let h = (symbol as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (((h >> 32) * partitions as u64) >> 32) as u32
}

/// A fixed symbol → partition map: table overrides, hash for the rest.
#[derive(Clone)]
pub struct PartitionMap {
    partitions: u32,
    /// Partition of every symbol below `DENSE` (hash or override).
    dense: Box<[u32]>,
    sparse: HashMap<Symbol, u32>,
}

impl fmt::Debug for PartitionMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionMap")
            .field("partitions", &self.partitions)
            .field("overrides", &self.overrides())
            .finish()
    }
}

impl PartitionMap {
    /// Pure hash routing over `partitions` partitions.
    pub fn hash(partitions: u32) -> Result<PartitionMap, RoutingError> {
        if partitions == 0 || partitions > MAX_PARTITIONS {
            return Err(RoutingError(format!(
                "partitions must be 1..={MAX_PARTITIONS}, got {partitions}"
            )));
        }
        let mut dense = vec![0u32; DENSE].into_boxed_slice();
        for (s, slot) in dense.iter_mut().enumerate() {
            *slot = hash_partition(s as Symbol, partitions);
        }
        Ok(PartitionMap {
            partitions,
            dense,
            sparse: HashMap::new(),
        })
    }

    /// Hash routing plus explicit `(symbol, partition)` overrides.
    pub fn with_table(
        partitions: u32,
        table: &[(Symbol, u32)],
    ) -> Result<PartitionMap, RoutingError> {
        let mut m = PartitionMap::hash(partitions)?;
        let mut seen = std::collections::HashSet::new();
        for &(sym, p) in table {
            if p >= partitions {
                return Err(RoutingError(format!(
                    "symbol {sym}: partition {p} out of range for {partitions} partitions"
                )));
            }
            if !seen.insert(sym) {
                return Err(RoutingError(format!("symbol {sym} listed twice")));
            }
            if (sym as usize) < DENSE {
                m.dense[sym as usize] = p;
            }
            m.sparse.insert(sym, p);
        }
        Ok(m)
    }

    /// Parse an `orderer-partition-map/1` file (spec/ROUTING.md §3) for a
    /// pipeline of `partitions` partitions.
    pub fn parse_table(text: &str, partitions: u32) -> Result<PartitionMap, RoutingError> {
        let mut lines = text.lines().filter(|l| !l.trim().is_empty());
        let hdr = lines
            .next()
            .ok_or_else(|| RoutingError("empty partition map".into()))?;
        if get_str(hdr, "format") != Some("orderer-partition-map/1") {
            return Err(RoutingError("not an orderer-partition-map/1 header".into()));
        }
        match get_u64(hdr, "partitions") {
            Some(p) if p == partitions as u64 => {}
            Some(p) => {
                return Err(RoutingError(format!(
                    "partition map is for {p} partitions, pipeline has {partitions}"
                )))
            }
            None => return Err(RoutingError("partition map header lacks partitions".into())),
        }
        let mut table = Vec::new();
        for l in lines {
            let sym = get_u64(l, "symbol")
                .filter(|&s| s <= u32::MAX as u64)
                .ok_or_else(|| RoutingError(format!("bad symbol in: {l}")))?;
            let p = get_u64(l, "partition")
                .filter(|&p| p <= u32::MAX as u64)
                .ok_or_else(|| RoutingError(format!("bad partition in: {l}")))?;
            table.push((sym as Symbol, p as u32));
        }
        PartitionMap::with_table(partitions, &table)
    }

    #[inline(always)]
    pub fn partitions(&self) -> u32 {
        self.partitions
    }

    #[inline(always)]
    pub fn partition(&self, symbol: Symbol) -> u32 {
        if (symbol as usize) < DENSE {
            return self.dense[symbol as usize];
        }
        if self.sparse.is_empty() {
            return hash_partition(symbol, self.partitions);
        }
        match self.sparse.get(&symbol) {
            Some(&p) => p,
            None => hash_partition(symbol, self.partitions),
        }
    }

    /// Explicit overrides, sorted by symbol.
    pub fn overrides(&self) -> Vec<(Symbol, u32)> {
        let mut v: Vec<_> = self.sparse.iter().map(|(&s, &p)| (s, p)).collect();
        v.sort_unstable();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_values() {
        // spec/ROUTING.md §2
        let want = [0, 2, 0, 3, 1, 0, 2, 1];
        for (s, &p) in want.iter().enumerate() {
            assert_eq!(hash_partition(s as Symbol, 4), p, "symbol {s}");
        }
        assert_eq!(hash_partition(u32::MAX, 4), 3);
        let m = PartitionMap::hash(4).unwrap();
        let counts = (0..64).fold([0; 4], |mut c, s| {
            c[m.partition(s) as usize] += 1;
            c
        });
        assert_eq!(counts, [17, 15, 16, 16]);
        assert!((0..100_000).all(|s| PartitionMap::hash(1).unwrap().partition(s) == 0));
    }

    #[test]
    fn table_overrides_and_errors() {
        let text = "{\"format\":\"orderer-partition-map/1\",\"partitions\":4}\n{\"symbol\":10,\"partition\":3}\n{\"symbol\":5000000,\"partition\":1}\n";
        let m = PartitionMap::parse_table(text, 4).unwrap();
        assert_eq!(m.partition(10), 3);
        assert_eq!(m.partition(5_000_000), 1);
        assert_eq!(m.partition(11), hash_partition(11, 4));
        assert_eq!(m.partition(5_000_001), hash_partition(5_000_001, 4));
        assert!(PartitionMap::parse_table(text, 3).is_err(), "P mismatch");
        let dup = "{\"format\":\"orderer-partition-map/1\",\"partitions\":2}\n{\"symbol\":1,\"partition\":0}\n{\"symbol\":1,\"partition\":1}\n";
        assert!(PartitionMap::parse_table(dup, 2).is_err());
        let range = "{\"format\":\"orderer-partition-map/1\",\"partitions\":2}\n{\"symbol\":1,\"partition\":2}\n";
        assert!(PartitionMap::parse_table(range, 2).is_err());
        assert!(PartitionMap::hash(0).is_err());
        assert!(PartitionMap::hash(MAX_PARTITIONS + 1).is_err());
    }
}
