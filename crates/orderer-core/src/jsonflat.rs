//! Minimal flat-JSON reader for vector/corpus lines — `{"k":num,"k":"str"}`
//! objects only (no nesting, no escapes needed for our grammar). Keeps the
//! bench binary and any non-serde consumers dependency-free.

use crate::types::*;

/// Value of `key` in a flat JSON object line, as a string slice.
pub fn get_str<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let pat = format!("\"{key}\":");
    let start = line.find(&pat)? + pat.len();
    let rest = &line[start..];
    if let Some(quoted) = rest.strip_prefix('"') {
        let end = quoted.find('"')?;
        Some(&quoted[..end])
    } else {
        let end = rest.find([',', '}']).unwrap_or(rest.len());
        Some(rest[..end].trim())
    }
}

pub fn get_i64(line: &str, key: &str) -> Option<i64> {
    get_str(line, key)?.parse().ok()
}

pub fn get_u64(line: &str, key: &str) -> Option<u64> {
    get_str(line, key)?.parse().ok()
}

/// Parse one canonical command line into a `Command`.
pub fn parse_command(line: &str) -> Option<Command> {
    match get_str(line, "cmd")? {
        "new" => {
            let side = match get_str(line, "side")? {
                "bid" => Side::Bid,
                "ask" => Side::Ask,
                _ => return None,
            };
            let otype = match get_str(line, "otype")? {
                "limit" => OType::Limit,
                "market" => OType::Market,
                _ => return None,
            };
            let tif = match get_str(line, "tif")? {
                "gtc" => Tif::Gtc,
                "ioc" => Tif::Ioc,
                "fok" => Tif::Fok,
                "post_only" => Tif::PostOnly,
                _ => return None,
            };
            Some(Command::New {
                order_id: get_u64(line, "order_id")?,
                side,
                otype,
                price: get_i64(line, "price")?,
                qty: get_u64(line, "qty")?,
                tif,
            })
        }
        "cancel" => Some(Command::Cancel {
            order_id: get_u64(line, "order_id")?,
        }),
        "replace" => Some(Command::Replace {
            order_id: get_u64(line, "order_id")?,
            price: get_i64(line, "price")?,
            qty: get_u64(line, "qty")?,
        }),
        _ => None,
    }
}

/// Corpus/vector header fields (defaults where optional).
pub fn parse_header(line: &str) -> (i64, i64, usize, IndexKind) {
    let pmin = get_i64(line, "pmin").unwrap_or(0);
    let pmax = get_i64(line, "pmax").unwrap_or(1_000_000);
    let max_orders = get_u64(line, "max_orders").unwrap_or(65_536) as usize;
    let index = match get_str(line, "index") {
        Some("tree") => IndexKind::Tree,
        _ => IndexKind::Ladder,
    };
    (pmin, pmax, max_orders, index)
}
