//! The deployment's reference data, read from a file by every node and tool.
//!
//! Format: one setting per line, `#` starts a comment, blank lines are ignored.
//!
//! ```text
//! ledger_shards   = 2        # ledger shards (accounts are sharded over them)
//! matching_groups = 2        # matching groups (symbols are sharded over them)
//! partitions      = 4        # pipeline partitions inside each matching group
//! fee_ppm         = 500      # default fee, parts per million of what a side receives
//! # symbol = ID base=ASSET quote=ASSET lot=BASE_ATOMS_PER_QTY [maker_ppm=N taker_ppm=N]
//! symbol = 10 base=1 quote=2 lot=1000
//! symbol = 11 base=3 quote=2 lot=10 maker_ppm=100 taker_ppm=300
//! ```
//!
//! Everything is an integer; assets are numeric ids (a name table belongs to the
//! gateway's front end, not to the ledger). A symbol belongs to the matching group
//! `orderer_cluster::shard::ShardMap::new(matching_groups).group_of(id)`. Stream
//! ids of group `g` are `g * partitions + partition`.
//!
//! [`ExchangeConfig::checksum`] is a SHA-256 over a canonical rendering (settings in
//! a fixed order, symbols sorted by id), so spelling, comments and ordering do not
//! matter but any change of meaning does. The gateway and exchange ports refuse a
//! peer whose checksum differs, and the replica transport key is derived from it, so
//! replicas built from different configurations cannot form a group (`secure.rs`).

use orderer_cluster::sha256::sha256;
use orderer_cluster::shard::ShardMap;
use orderer_ledger::{Config, Symbol};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SymbolCfg {
    pub id: u32,
    pub base: u32,
    pub quote: u32,
    pub lot: u128,
    /// `(maker_ppm, taker_ppm)`, overriding the default fee.
    pub fees: Option<(u32, u32)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExchangeConfig {
    pub ledger_shards: u16,
    pub matching_groups: u32,
    pub partitions: u32,
    pub fee_ppm: u32,
    /// Sorted by id.
    pub symbols: Vec<SymbolCfg>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ConfigError {}

fn err<T>(line: usize, msg: impl fmt::Display) -> Result<T, ConfigError> {
    Err(ConfigError(format!("line {line}: {msg}")))
}

fn num<T: std::str::FromStr>(line: usize, what: &str, v: &str) -> Result<T, ConfigError> {
    v.trim()
        .parse()
        .or_else(|_| err(line, format!("{what}: `{v}` is not a valid number here")))
}

/// Ledger shard ids sit in the high 16 bits of an order id; bit 15 is reserved for
/// orders the matching groups create themselves (credit slices).
pub const MAX_LEDGER_SHARDS: u16 = 0x8000;

impl ExchangeConfig {
    pub fn parse(text: &str) -> Result<ExchangeConfig, ConfigError> {
        let (mut shards, mut groups, mut parts, mut fee) = (None, None, None, None);
        let mut symbols: BTreeMap<u32, SymbolCfg> = BTreeMap::new();
        for (i, raw) in text.lines().enumerate() {
            let line = i + 1;
            let l = raw.split('#').next().unwrap_or("").trim();
            if l.is_empty() {
                continue;
            }
            let Some((key, value)) = l.split_once('=') else {
                return err(line, format!("expected `key = value`, found `{l}`"));
            };
            let (key, value) = (key.trim(), value.trim());
            let once = |slot: &mut Option<u64>, v: u64| -> Result<(), ConfigError> {
                if slot.replace(v).is_some() {
                    return err(line, format!("`{key}` is set twice"));
                }
                Ok(())
            };
            match key {
                "ledger_shards" => once(&mut shards, num(line, key, value)?)?,
                "matching_groups" => once(&mut groups, num(line, key, value)?)?,
                "partitions" => once(&mut parts, num(line, key, value)?)?,
                "fee_ppm" => once(&mut fee, num(line, key, value)?)?,
                "symbol" => {
                    let mut it = value.split_whitespace();
                    let id: u32 = num(line, "symbol id", it.next().unwrap_or(""))?;
                    let (mut base, mut quote, mut lot) = (None, None, None);
                    let (mut maker, mut taker) = (None, None);
                    for kv in it {
                        let Some((k, v)) = kv.split_once('=') else {
                            return err(
                                line,
                                format!("symbol {id}: expected name=value, found `{kv}`"),
                            );
                        };
                        let dup = |set: bool| -> Result<(), ConfigError> {
                            if set {
                                return err(line, format!("symbol {id}: `{k}` is set twice"));
                            }
                            Ok(())
                        };
                        match k {
                            "base" => {
                                dup(base.is_some())?;
                                base = Some(num::<u32>(line, "base", v)?)
                            }
                            "quote" => {
                                dup(quote.is_some())?;
                                quote = Some(num::<u32>(line, "quote", v)?)
                            }
                            "lot" => {
                                dup(lot.is_some())?;
                                lot = Some(num::<u128>(line, "lot", v)?)
                            }
                            "maker_ppm" => {
                                dup(maker.is_some())?;
                                maker = Some(num::<u32>(line, "maker_ppm", v)?)
                            }
                            "taker_ppm" => {
                                dup(taker.is_some())?;
                                taker = Some(num::<u32>(line, "taker_ppm", v)?)
                            }
                            other => {
                                return err(line, format!("symbol {id}: unknown field `{other}`"))
                            }
                        }
                    }
                    let (Some(base), Some(quote), Some(lot)) = (base, quote, lot) else {
                        return err(line, format!("symbol {id} needs base=, quote= and lot="));
                    };
                    let fees = match (maker, taker) {
                        (None, None) => None,
                        (Some(m), Some(t)) => Some((m, t)),
                        _ => {
                            return err(
                                line,
                                format!(
                                    "symbol {id}: give both maker_ppm and taker_ppm or neither"
                                ),
                            )
                        }
                    };
                    if symbols
                        .insert(
                            id,
                            SymbolCfg {
                                id,
                                base,
                                quote,
                                lot,
                                fees,
                            },
                        )
                        .is_some()
                    {
                        return err(line, format!("symbol {id} is defined twice"));
                    }
                }
                other => return err(line, format!("unknown setting `{other}`")),
            }
        }
        let need = |v: Option<u64>, name: &str| {
            v.ok_or_else(|| ConfigError(format!("missing setting `{name}`")))
        };
        let c = ExchangeConfig {
            ledger_shards: u16::try_from(need(shards, "ledger_shards")?)
                .map_err(|_| ConfigError("ledger_shards is too large".into()))?,
            matching_groups: u32::try_from(need(groups, "matching_groups")?)
                .map_err(|_| ConfigError("matching_groups is too large".into()))?,
            partitions: u32::try_from(need(parts, "partitions")?)
                .map_err(|_| ConfigError("partitions is too large".into()))?,
            fee_ppm: u32::try_from(need(fee, "fee_ppm")?)
                .map_err(|_| ConfigError("fee_ppm is too large".into()))?,
            symbols: symbols.into_values().collect(),
        };
        c.validate()?;
        Ok(c)
    }

    pub fn load(path: &str) -> Result<ExchangeConfig, ConfigError> {
        let text =
            std::fs::read_to_string(path).map_err(|e| ConfigError(format!("{path}: {e}")))?;
        Self::parse(&text).map_err(|e| ConfigError(format!("{path}: {e}")))
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let bad = |m: String| Err(ConfigError(m));
        if self.ledger_shards == 0 || self.ledger_shards >= MAX_LEDGER_SHARDS {
            return bad(format!(
                "ledger_shards must be 1..{} (got {})",
                MAX_LEDGER_SHARDS - 1,
                self.ledger_shards
            ));
        }
        if self.matching_groups == 0 || self.matching_groups > 4096 {
            return bad(format!(
                "matching_groups must be 1..4096 (got {})",
                self.matching_groups
            ));
        }
        if self.partitions == 0 || self.partitions > 1024 {
            return bad(format!(
                "partitions must be 1..1024 (got {})",
                self.partitions
            ));
        }
        if self.fee_ppm > 1_000_000 {
            return bad(format!(
                "fee_ppm {} is above 1,000,000 (100%)",
                self.fee_ppm
            ));
        }
        if self.symbols.is_empty() {
            return bad("at least one symbol is required".into());
        }
        for s in &self.symbols {
            if s.base == s.quote {
                return bad(format!(
                    "symbol {}: base and quote are both asset {}",
                    s.id, s.base
                ));
            }
            if s.lot == 0 {
                return bad(format!("symbol {}: lot must be at least 1", s.id));
            }
            if let Some((m, t)) = s.fees {
                if m > 1_000_000 || t > 1_000_000 {
                    return bad(format!("symbol {}: a fee above 1,000,000 ppm", s.id));
                }
            }
        }
        let map = ShardMap::new(self.matching_groups);
        for g in 0..self.matching_groups {
            if !self.symbols.iter().any(|s| map.group_of(s.id) == g) {
                return bad(format!(
                    "matching group {g} owns no symbol (symbols are assigned by hash; add symbols or lower matching_groups)"
                ));
            }
        }
        Ok(())
    }

    /// The reference data of ledger shard `shard`.
    pub fn ledger_config(&self, shard: u16) -> Config {
        let mut symbols = BTreeMap::new();
        let mut fees = BTreeMap::new();
        for s in &self.symbols {
            symbols.insert(
                s.id,
                Symbol {
                    base: s.base,
                    quote: s.quote,
                    lot: s.lot,
                },
            );
            if let Some(f) = s.fees {
                fees.insert(s.id, f);
            }
        }
        Config {
            shard,
            fee_ppm: self.fee_ppm,
            symbols,
            fees,
        }
    }

    pub fn shard_map(&self) -> ShardMap {
        ShardMap::new(self.matching_groups)
    }

    /// The matching group that owns `symbol`.
    pub fn group_of(&self, symbol: u32) -> u32 {
        self.shard_map().group_of(symbol)
    }

    /// First stream id of group `group`.
    pub fn source_base(&self, group: u32) -> u32 {
        group * self.partitions
    }

    /// Every stream: `(stream id, group)`, group by group.
    pub fn sources(&self) -> Vec<(u32, usize)> {
        (0..self.matching_groups)
            .flat_map(|g| (0..self.partitions).map(move |p| (g * self.partitions + p, g as usize)))
            .collect()
    }

    pub fn canonical(&self) -> String {
        let mut s = format!(
            "orderer-exchange-config/1\nledger_shards={}\nmatching_groups={}\npartitions={}\nfee_ppm={}\n",
            self.ledger_shards, self.matching_groups, self.partitions, self.fee_ppm
        );
        for y in &self.symbols {
            s.push_str(&format!(
                "symbol={} base={} quote={} lot={}",
                y.id, y.base, y.quote, y.lot
            ));
            if let Some((m, t)) = y.fees {
                s.push_str(&format!(" maker_ppm={m} taker_ppm={t}"));
            }
            s.push('\n');
        }
        s
    }

    pub fn checksum(&self) -> [u8; 32] {
        sha256(self.canonical().as_bytes())
    }

    /// The configuration the tests and the old `demo_config` use: two symbols, one
    /// group, one ledger shard.
    pub fn demo() -> ExchangeConfig {
        ExchangeConfig::parse(
            "ledger_shards = 1\nmatching_groups = 1\npartitions = 4\nfee_ppm = 500\n\
             symbol = 10 base=1 quote=2 lot=1000\nsymbol = 11 base=3 quote=2 lot=10\n",
        )
        .expect("the demo config is valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = "\
# a comment
ledger_shards = 2
matching_groups = 2
partitions = 4
fee_ppm = 500
symbol = 10 base=1 quote=2 lot=1000
symbol = 11 base=3 quote=2 lot=10 maker_ppm=100 taker_ppm=300  # trailing comment
symbol = 12 base=4 quote=2 lot=1
symbol = 13 base=5 quote=2 lot=1
symbol = 14 base=6 quote=2 lot=1
symbol = 15 base=7 quote=2 lot=1
";

    #[test]
    fn parses_and_derives() {
        let c = ExchangeConfig::parse(GOOD).unwrap();
        assert_eq!(
            (c.ledger_shards, c.matching_groups, c.partitions),
            (2, 2, 4)
        );
        assert_eq!(c.symbols.len(), 6);
        assert_eq!(c.symbols[1].fees, Some((100, 300)));
        let l = c.ledger_config(1);
        assert_eq!(l.shard, 1);
        assert_eq!(l.fee_ppm, 500);
        assert_eq!(l.symbols[&10].lot, 1000);
        assert_eq!(l.fees[&11], (100, 300));
        assert_eq!(c.sources().len(), 8);
        assert_eq!(c.sources()[5], (5, 1));
        assert_eq!(c.source_base(1), 4);
        for s in &c.symbols {
            assert!(c.group_of(s.id) < 2);
        }
    }

    #[test]
    fn the_checksum_ignores_spelling_and_tracks_meaning() {
        let a = ExchangeConfig::parse(GOOD).unwrap();
        // reordered, re-spaced, re-commented
        let shuffled: String = {
            let mut lines: Vec<&str> = GOOD.lines().collect();
            lines.reverse();
            lines.join("\n\n  ")
        };
        let b = ExchangeConfig::parse(&shuffled).unwrap();
        assert_eq!(a.checksum(), b.checksum());
        for edit in [
            ("lot=1000", "lot=1001"),
            ("fee_ppm = 500", "fee_ppm = 501"),
            ("maker_ppm=100", "maker_ppm=101"),
            ("partitions = 4", "partitions = 8"),
        ] {
            let c = ExchangeConfig::parse(&GOOD.replace(edit.0, edit.1)).unwrap();
            assert_ne!(a.checksum(), c.checksum(), "{edit:?}");
        }
    }

    #[test]
    fn mistakes_get_clear_errors() {
        let e = |t: &str| ExchangeConfig::parse(t).unwrap_err().0;
        let base = "ledger_shards = 1\nmatching_groups = 1\npartitions = 2\nfee_ppm = 0\nsymbol = 1 base=1 quote=2 lot=1\n";
        assert!(ExchangeConfig::parse(base).is_ok());
        assert!(e("").contains("missing setting `ledger_shards`"));
        assert!(e(&base.replace("fee_ppm = 0\n", "")).contains("missing setting `fee_ppm`"));
        assert!(e(&format!("{base}colour = red\n")).contains("line 6: unknown setting `colour`"));
        assert!(e(&format!("{base}fee_ppm = 1\n")).contains("`fee_ppm` is set twice"));
        assert!(e(&base.replace("= 2\nfee", "= two\nfee")).contains("`two` is not a valid number"));
        assert!(e(&format!("{base}symbol = 1 base=3 quote=4 lot=1\n")).contains("defined twice"));
        assert!(e(&base.replace("lot=1", "")).contains("needs base=, quote= and lot="));
        assert!(e(&base.replace("lot=1", "lot=0")).contains("lot must be at least 1"));
        assert!(e(&base.replace("quote=2", "quote=1")).contains("base and quote are both asset 1"));
        assert!(
            e(&base.replace("lot=1", "lot=1 maker_ppm=5")).contains("both maker_ppm and taker_ppm")
        );
        assert!(e(&base.replace("lot=1", "lot=1 colour=5")).contains("unknown field `colour`"));
        assert!(e(&base.replace("ledger_shards = 1", "ledger_shards = 0"))
            .contains("ledger_shards must be"));
        assert!(
            e(&base.replace("ledger_shards = 1", "ledger_shards = 32768"))
                .contains("ledger_shards must be")
        );
        assert!(e(&base.replace("fee_ppm = 0", "fee_ppm = 2000000")).contains("above 1,000,000"));
        assert!(e("just words").contains("expected `key = value`"));
        // a group that owns nothing is refused, not silently idle
        let many = base.replace("matching_groups = 1", "matching_groups = 64");
        assert!(e(&many).contains("owns no symbol"));
    }

    #[test]
    fn demo_matches_the_old_demo_config() {
        let d = ExchangeConfig::demo().ledger_config(3);
        let o = crate::demo_config(3);
        assert_eq!(d.shard, o.shard);
        assert_eq!(d.fee_ppm, o.fee_ppm);
        assert_eq!(d.symbols, o.symbols);
        assert_eq!(d.fees, o.fees);
    }
}
