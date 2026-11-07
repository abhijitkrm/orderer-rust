//! Helpers the binaries share: configuration and key loading, address lists.

use crate::config::ExchangeConfig;
use crate::secure::Auth;
use std::net::SocketAddr;

/// The configuration a process was started with and the credentials derived from
/// it: the pre-shared key from `ORDERER_KEY` (if set) bound to the config checksum.
pub struct Setup {
    pub cfg: ExchangeConfig,
    pub auth: Auth,
}

impl Setup {
    pub fn load(path: &str) -> Result<Setup, String> {
        let cfg = ExchangeConfig::load(path).map_err(|e| e.to_string())?;
        let auth = Auth::from_env(cfg.checksum());
        Ok(Setup { cfg, auth })
    }
}

/// `A,B,C`.
pub fn parse_addrs(s: &str) -> Result<Vec<SocketAddr>, String> {
    s.split(',')
        .map(|a| a.trim().parse().map_err(|_| format!("bad address `{a}`")))
        .collect()
}

/// `A,B,C;D,E,F`: one list per ledger shard or per matching group.
pub fn parse_groups(s: &str) -> Result<Vec<Vec<SocketAddr>>, String> {
    s.split(';').map(parse_addrs).collect()
}

/// `on`, `off` or `relaxed[:MS]` -> (fsync, relaxed interval).
pub fn parse_fsync(m: &str) -> Result<(bool, Option<std::time::Duration>), String> {
    match m {
        "on" => Ok((true, None)),
        "off" => Ok((false, None)),
        _ => match m.strip_prefix("relaxed") {
            Some(rest) => {
                let ms = match rest {
                    "" => 5,
                    r => r
                        .strip_prefix(':')
                        .and_then(|x| x.parse::<u64>().ok())
                        .ok_or("--fsync relaxed:MS")?,
                };
                Ok((true, Some(std::time::Duration::from_millis(ms))))
            }
            None => Err("--fsync wants on, off or relaxed[:MS]".into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lists_and_fsync() {
        assert_eq!(parse_addrs("127.0.0.1:1,127.0.0.1:2").unwrap().len(), 2);
        assert!(parse_addrs("nope").is_err());
        assert_eq!(
            parse_groups("127.0.0.1:1;127.0.0.1:2,127.0.0.1:3").unwrap()[1].len(),
            2
        );
        assert_eq!(parse_fsync("on").unwrap(), (true, None));
        assert_eq!(parse_fsync("off").unwrap(), (false, None));
        assert_eq!(
            parse_fsync("relaxed:7").unwrap(),
            (true, Some(std::time::Duration::from_millis(7)))
        );
        assert!(parse_fsync("sometimes").is_err());
        assert!(parse_fsync("relaxed:x").is_err());
    }
}
