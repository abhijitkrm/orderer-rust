//! Several matching groups behind one ledger shard: each group applies only the
//! symbols it owns, a ledger shard's outbox is trimmed only as far as the slowest
//! group, and one settler reads every stream once.

use orderer::{PartitionMap, Pipeline};
use orderer_cluster::{Replica, ReplicaConfig};
use orderer_consensus::Config as RaftConfig;
use orderer_core::{BookConfig, FifoCore};
use orderer_exchange::relay::{ForwarderRelay, SettlerRelay};
use orderer_exchange::*;
use orderer_ledger::{Cmd, Event, Side};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn temp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("orderer-mg-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn free() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn rcfg(dir: &std::path::Path, name: &str) -> ReplicaConfig {
    let mut c = ReplicaConfig::new(0, vec![(0, free())], dir.join(name));
    c.tick = Duration::from_millis(5);
    c.consensus = RaftConfig {
        election_ticks: 10,
        heartbeat_ticks: 2,
        max_batch: 256,
        ..RaftConfig::default()
    };
    c.fsync = false;
    c.propose_timeout = Duration::from_secs(5);
    c
}

const CONFIG: &str = "\
ledger_shards = 1
matching_groups = 2
partitions = 2
fee_ppm = 0
symbol = 10 base=1 quote=2 lot=1000
symbol = 11 base=3 quote=2 lot=10
symbol = 12 base=4 quote=2 lot=1
symbol = 13 base=5 quote=2 lot=1
symbol = 14 base=6 quote=2 lot=1
symbol = 15 base=7 quote=2 lot=1
";

struct Tier {
    _r: Replica,
    srv: ExchangeServer,
}

fn tier(dir: &std::path::Path, cfg: &ExchangeConfig, g: u32) -> Tier {
    let (sm, view) = MatchingTierSm::<FifoCore>::new(
        BookConfig::default(),
        PartitionMap::hash(cfg.partitions).unwrap(),
        TierConfig {
            source_base: cfg.source_base(g),
            retain: 1 << 16,
        },
        move |init, plugs| {
            let mut b = Pipeline::<FifoCore>::builder()
                .partitions(2)
                .egress(plugs.digest)
                .egress(plugs.archive);
            if let Some(i) = init {
                b = b.initial(i);
            }
            b.build().unwrap()
        },
    );
    let sm = sm.with_group(g, cfg.matching_groups);
    let r = Replica::start(rcfg(dir, &format!("tier{g}")), sm).unwrap();
    let srv = ExchangeServer::start("127.0.0.1:0".parse().unwrap(), r.handle(), view).unwrap();
    Tier { _r: r, srv }
}

fn tc(addr: SocketAddr) -> TierClient {
    let mut t = TierClient::new(vec![addr]);
    t.budget = Duration::from_secs(10);
    t
}

#[test]
fn groups_apply_only_their_symbols_and_the_ledger_trims_to_the_slowest() {
    let dir = temp("two");
    let cfg = ExchangeConfig::parse(CONFIG).unwrap();
    let tiers = [tier(&dir, &cfg, 0), tier(&dir, &cfg, 1)];
    let (sm, view) = LedgerNodeSm::new(cfg.ledger_config(0));
    let lr = Replica::start(rcfg(&dir, "ledger"), sm).unwrap();
    let gw = GatewayServer::start("127.0.0.1:0".parse().unwrap(), lr.handle(), view).unwrap();
    let mut ledger = GatewayClient::new(vec![gw.addr]);
    ledger.budget = Duration::from_secs(10);
    let mut clients = vec![tc(tiers[0].srv.addr), tc(tiers[1].srv.addr)];

    // fund two accounts in every asset and trade every symbol once
    let mut seq = 0;
    let mut cmd = |l: &mut GatewayClient, c: &Cmd| {
        seq += 1;
        l.cmd(1, seq, c).unwrap()
    };
    for acct in [1u64, 2] {
        for asset in 1..=7u32 {
            cmd(
                &mut ledger,
                &Cmd::Deposit {
                    account: acct,
                    asset,
                    amount: 1_000_000,
                    external_id: acct * 100 + asset as u64,
                },
            );
        }
    }
    let mut orders = Vec::new();
    for s in &cfg.symbols {
        for (acct, side) in [(2u64, Side::Sell), (1, Side::Buy)] {
            let r = cmd(
                &mut ledger,
                &Cmd::Reserve {
                    account: acct,
                    symbol: s.id,
                    side,
                    price: 100,
                    qty: 1,
                },
            );
            let Reply::Reserved { order, .. } = r else {
                panic!("{r:?}")
            };
            orders.push(order);
        }
    }
    let owned = |g: u32| {
        cfg.symbols
            .iter()
            .filter(|s| cfg.group_of(s.id) == g)
            .count()
    };
    assert!(owned(0) >= 1 && owned(1) >= 1, "both groups own symbols");

    // Group 1 is unreachable for a moment: group 0 gets everything, the ledger
    // must keep what group 1 has not accepted.
    let mut fwd = ForwarderRelay::new(0, 2);
    assert!(
        fwd.step(&mut ledger, &mut clients).is_ok(),
        "first step with both groups"
    );
    // everything is already in (one batch); queue more orders, then cut group 1 off
    let more = cmd(
        &mut ledger,
        &Cmd::Reserve {
            account: 1,
            symbol: cfg.symbols[0].id,
            side: Side::Buy,
            price: 50,
            qty: 1,
        },
    );
    let Reply::Reserved { order: late, .. } = more else {
        panic!()
    };
    let dead = free();
    let mut cut = vec![tc(tiers[0].srv.addr), {
        let mut t = TierClient::new(vec![dead]);
        t.budget = Duration::from_millis(300);
        t
    }];
    assert!(
        fwd.step(&mut ledger, &mut cut).is_err(),
        "group 1 cannot be reached"
    );
    // group 1 was at the end; now give it a late message while it is unreachable:
    // group 0 accepts it, the ledger cannot trim it
    let st = ledger.state(true, &[]).unwrap();
    let mut live0 = tc(tiers[0].srv.addr);
    let c0 = live0.cursor(0).unwrap().unwrap();
    assert_eq!(c0, st.outbox_next, "group 0 has accepted everything queued");
    let mut live1 = tc(tiers[1].srv.addr);
    let c1 = live1.cursor(0).unwrap().unwrap();
    assert_eq!(c1, st.outbox_next - 1, "group 1 is one message behind");
    assert!(
        st.outbox_base <= c1,
        "the ledger trimmed to {} past group 1's cursor {c1}",
        st.outbox_base
    );
    // group 1 returns: everything converges and the ledger may trim
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        fwd.step(&mut ledger, &mut clients).unwrap();
        let st = ledger.state(true, &[]).unwrap();
        if st.outbox_base == st.outbox_next {
            break;
        }
        assert!(Instant::now() < end, "never converged: {st:?}");
    }

    // one settler, every stream once
    let mut set = SettlerRelay::new(vec![0], cfg.sources());
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        set.step(std::slice::from_mut(&mut ledger), &mut clients)
            .unwrap();
        let open = orders
            .iter()
            .filter(|o| ledger.order(true, **o).unwrap().is_some())
            .count();
        if open == 0 {
            break;
        }
        assert!(Instant::now() < end, "{open} orders never settled");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        ledger.order(true, late).unwrap().is_some(),
        "the late bid rests"
    );

    // each trade is in the stream of the group that owns its symbol, and only there
    let mut trades = [0usize; 2];
    for g in 0..2usize {
        for p in 0..cfg.partitions {
            let source = cfg.source_base(g as u32) + p;
            let (evs, _) = clients[g].read(source, 1, 1000).unwrap().unwrap();
            trades[g] += evs
                .iter()
                .filter(|(_, e)| matches!(e, Event::Trade { .. }))
                .count();
        }
    }
    assert_eq!(trades[0], owned(0), "group 0 traded exactly its symbols");
    assert_eq!(trades[1], owned(1), "group 1 traded exactly its symbols");
    // both groups' cursors count the whole outbox
    let st = ledger.state(true, &[]).unwrap();
    for c in clients.iter_mut() {
        assert_eq!(c.cursor(0).unwrap().unwrap(), st.outbox_next);
    }
    // the audit covers both groups: clean, caught up
    let mut audit = orderer_exchange::audit::Audit::new(
        vec![GatewayClient::new(vec![gw.addr])],
        vec![tc(tiers[0].srv.addr), tc(tiers[1].srv.addr)],
        cfg.sources(),
        {
            let c = cfg.clone();
            move |s| c.ledger_config(s)
        },
    );
    let rep = audit.round().unwrap();
    assert!(rep.violations.is_empty(), "{:?}", rep.violations);
    assert!(rep.caught_up, "{}", rep.summary);
    assert_eq!(rep.events as usize, owned(0) + owned(1));
    drop((gw, lr));
    for t in tiers {
        t.srv.shutdown();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_group_far_ahead_does_not_let_the_ledger_trim_past_a_slow_one() {
    let dir = temp("ahead");
    let cfg = ExchangeConfig::parse(CONFIG).unwrap();
    let tiers = [tier(&dir, &cfg, 0), tier(&dir, &cfg, 1)];
    let (sm, view) = LedgerNodeSm::new(cfg.ledger_config(0));
    let lr = Replica::start(rcfg(&dir, "ledger"), sm).unwrap();
    let gw = GatewayServer::start("127.0.0.1:0".parse().unwrap(), lr.handle(), view).unwrap();
    let mut ledger = GatewayClient::new(vec![gw.addr]);
    ledger.budget = Duration::from_secs(10);
    let mut clients = vec![tc(tiers[0].srv.addr), tc(tiers[1].srv.addr)];
    let mut seq = 0;
    for asset in [1u32, 2] {
        seq += 1;
        ledger
            .cmd(
                1,
                seq,
                &Cmd::Deposit {
                    account: 1,
                    asset,
                    amount: 1_000_000,
                    external_id: asset as u64,
                },
            )
            .unwrap();
    }
    for _ in 0..10 {
        seq += 1;
        let r = ledger
            .cmd(
                1,
                seq,
                &Cmd::Reserve {
                    account: 1,
                    symbol: 10,
                    side: Side::Buy,
                    price: 10,
                    qty: 1,
                },
            )
            .unwrap();
        assert!(matches!(r, Reply::Reserved { .. }));
    }
    // group 0 receives the whole outbox (as if its forwarder had run alone), group 1 nothing
    let (base, next, first, msgs) = ledger.outbox(true, 0, 1000).unwrap();
    assert_eq!((base, first), (0, 0));
    assert!(next >= 10);
    let ack = clients[0].forward(0, first, &msgs).unwrap().unwrap();
    assert_eq!(ack.cursor, next);
    assert_eq!(clients[1].cursor(0).unwrap().unwrap(), 0);
    // a fresh relay with a small window serves the slow group first
    let mut fwd = ForwarderRelay::new(0, 2);
    fwd.batch = 3;
    fwd.step(&mut ledger, &mut clients).unwrap();
    let st = ledger.state(true, &[]).unwrap();
    assert_eq!(clients[1].cursor(0).unwrap().unwrap(), 3);
    assert!(
        st.outbox_base <= 3,
        "the ledger trimmed to {} though group 1 has only accepted 3",
        st.outbox_base
    );
    // and it converges
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        fwd.step(&mut ledger, &mut clients).unwrap();
        let st = ledger.state(true, &[]).unwrap();
        if st.outbox_base == st.outbox_next {
            break;
        }
        assert!(Instant::now() < end);
    }
    drop((gw, lr));
    for t in tiers {
        t.srv.shutdown();
    }
    let _ = std::fs::remove_dir_all(&dir);
}
