//! The ledger node and its gateway: session exactly-once, snapshots, and the
//! relays and audit driven step by step against single-replica groups.

use orderer::{PartitionMap, Pipeline};
use orderer_cluster::{Replica, ReplicaConfig, StateMachine};
use orderer_consensus::{Config as RaftConfig, Entry};
use orderer_core::{BookConfig, FifoCore};
use orderer_exchange::audit::Audit;
use orderer_exchange::node::TAG_SESSION;
use orderer_exchange::relay::{ForwarderRelay, SettlerRelay, TickRelay};
use orderer_exchange::*;
use orderer_ledger::{encode_cmd, Cmd, Side};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

fn temp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("orderer-gw-{name}-{}", std::process::id()));
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

fn entry(data: Vec<u8>) -> Entry {
    Entry { term: 1, data }
}

fn session(client: u64, seq: u64, c: &Cmd) -> Vec<u8> {
    let mut v = vec![TAG_SESSION];
    v.extend_from_slice(&client.to_le_bytes());
    v.extend_from_slice(&seq.to_le_bytes());
    v.extend_from_slice(&encode_cmd(c));
    v
}

fn reserve() -> Cmd {
    Cmd::Reserve {
        account: 1,
        symbol: 10,
        side: Side::Buy,
        price: 100,
        qty: 1,
    }
}

fn deposit(id: u64) -> Cmd {
    Cmd::Deposit {
        account: 1,
        asset: 2,
        amount: 1_000,
        external_id: id,
    }
}

#[test]
fn a_repeated_session_command_is_applied_once_and_answers_the_same() {
    let (mut sm, view) = LedgerNodeSm::new(demo_config(0));
    sm.apply(1, &entry(session(7, 1, &deposit(1))));
    sm.apply(2, &entry(session(7, 2, &reserve())));
    let first = Reply::decode(&view.reply(2).unwrap()).unwrap();
    assert!(matches!(first, Reply::Reserved { .. }));
    // the same (client, seq) lands in the log again, later, after other work
    sm.apply(3, &entry(session(8, 1, &deposit(2))));
    sm.apply(4, &entry(session(7, 2, &reserve())));
    assert_eq!(
        Reply::decode(&view.reply(4).unwrap()).unwrap(),
        first,
        "same order id back"
    );
    assert_eq!(view.with(|l| l.open_orders()), 1, "one reservation");
    assert_eq!(view.with(|l| l.balance(1, 2).held), 100);
    // an older sequence number is stale, not applied
    sm.apply(5, &entry(session(7, 1, &deposit(3))));
    assert_eq!(
        Reply::decode(&view.reply(5).unwrap()).unwrap(),
        Reply::Stale
    );
    assert_eq!(view.with(|l| l.external_net(2).0), 2_000);
    // a bare command (a relay's) and an empty entry (a barrier)
    sm.apply(6, &entry(vec![]));
    assert!(view.reply(6).is_none());
    sm.apply(7, &entry(encode_cmd(&Cmd::Clock { now_ns: 5 })));
    assert_eq!(
        Reply::decode(&view.reply(7).unwrap()).unwrap(),
        Reply::Other("Ticked".into())
    );
    // garbage is counted, identically everywhere, and answered
    sm.apply(8, &entry(vec![0xEE, 1, 2]));
    sm.apply(9, &entry(vec![TAG_SESSION, 1]));
    assert_eq!(
        Reply::decode(&view.reply(8).unwrap()).unwrap(),
        Reply::Undecodable
    );
    assert_eq!(view.with(|l| l.anomalies()), 2);
}

#[test]
fn snapshot_carries_the_sessions_and_restores_to_the_same_digest() {
    let (mut a, va) = LedgerNodeSm::new(demo_config(0));
    a.apply(1, &entry(session(7, 1, &deposit(1))));
    a.apply(2, &entry(session(7, 2, &reserve())));
    a.apply(3, &entry(session(9, 1, &deposit(2))));
    let snap = a.snapshot(3);
    let (mut b, vb) = LedgerNodeSm::new(demo_config(0));
    b.restore(3, &snap).unwrap();
    assert_eq!(a.digest(3), b.digest(3));
    // the restored replica still refuses the repeat and gives the stored reply
    let before = va.reply(2).unwrap();
    b.apply(4, &entry(session(7, 2, &reserve())));
    assert_eq!(vb.reply(4).unwrap(), before);
    assert_eq!(vb.with(|l| l.open_orders()), 1);
    // corrupt snapshots are refused
    let (mut c, _) = LedgerNodeSm::new(demo_config(0));
    assert!(c.restore(3, &snap[..snap.len() / 2]).is_err());
    assert!(c.restore(3, b"nope").is_err());
    assert!(
        LedgerNodeSm::new(demo_config(1))
            .0
            .restore(3, &snap)
            .is_err(),
        "another shard"
    );
}

#[test]
fn the_session_table_is_bounded_and_evicts_the_oldest() {
    let (mut sm, view) = LedgerNodeSm::new(demo_config(0));
    let n = orderer_exchange::node::MAX_SESSIONS as u64 + 10;
    for c in 1..=n {
        sm.apply(c, &entry(session(c, 1, &Cmd::Clock { now_ns: c })));
    }
    assert!(
        sm.snapshot(n).len() < (n as usize) * 60,
        "table stays at the cap"
    );
    // client 1 was evicted: its repeat would be applied again (documented), client n is kept
    sm.apply(n + 1, &entry(session(n, 1, &Cmd::Clock { now_ns: 1 })));
    assert_eq!(
        view.with(|l| l.clock()),
        n,
        "the repeat of a kept client did nothing"
    );
}

fn raft() -> RaftConfig {
    RaftConfig {
        election_ticks: 10,
        heartbeat_ticks: 2,
        max_batch: 256,
        ..RaftConfig::default()
    }
}

fn cfg1(dir: &std::path::Path, name: &str) -> ReplicaConfig {
    let mut c = ReplicaConfig::new(0, vec![(0, free())], dir.join(name));
    c.tick = Duration::from_millis(5);
    c.consensus = raft();
    c.fsync = false;
    c.propose_timeout = Duration::from_secs(5);
    c
}

#[test]
fn deposits_orders_trades_and_an_audit_through_the_real_gateway_and_relays() {
    let dir = temp("e2e");
    // one matching replica, two ledger shards of one replica each
    let (sm, tview) = MatchingTierSm::<FifoCore>::new(
        BookConfig::default(),
        PartitionMap::hash(2).unwrap(),
        TierConfig {
            source_base: 100,
            retain: 1 << 16,
        },
        |init, plugs| {
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
    let tier = Replica::start(cfg1(&dir, "tier"), sm).unwrap();
    let tier_srv =
        ExchangeServer::start("127.0.0.1:0".parse().unwrap(), tier.handle(), tview).unwrap();
    let mut gws = Vec::new();
    let mut keep = Vec::new();
    for s in 0..2u16 {
        let (sm, view) = LedgerNodeSm::new(demo_config(s));
        let r = Replica::start(cfg1(&dir, &format!("l{s}")), sm).unwrap();
        let g = GatewayServer::start("127.0.0.1:0".parse().unwrap(), r.handle(), view).unwrap();
        gws.push(g.addr);
        keep.push((r, g));
    }
    let mut ledger: Vec<GatewayClient> = gws.iter().map(|a| GatewayClient::new(vec![*a])).collect();
    let mut tc = TierClient::new(vec![tier_srv.addr]);
    tc.budget = Duration::from_secs(10);
    // accounts 2 and 3 live on different shards (a % 2)
    let ok = |r: std::io::Result<Reply>| r.unwrap();
    for (acct, shard) in [(2u64, 0usize), (3, 1)] {
        for (asset, amt) in [(1u32, 100_000u128), (2, 100_000)] {
            let r = ok(ledger[shard].cmd(
                1,
                asset as u64 + acct * 10,
                &Cmd::Deposit {
                    account: acct,
                    asset,
                    amount: amt,
                    external_id: asset as u64,
                },
            ))
            .clone();
            assert_eq!(r, Reply::Other("Deposited".into()));
        }
    }
    // account 3 (shard 1) sells 2 lots at 100; account 2 (shard 0) buys 2 lots at 101
    let sell = ok(ledger[1].cmd(
        2,
        1,
        &Cmd::Reserve {
            account: 3,
            symbol: 10,
            side: Side::Sell,
            price: 100,
            qty: 2,
        },
    ));
    let buy = ok(ledger[0].cmd(
        3,
        1,
        &Cmd::Reserve {
            account: 2,
            symbol: 10,
            side: Side::Buy,
            price: 101,
            qty: 2,
        },
    ));
    let (Reply::Reserved { order: so, .. }, Reply::Reserved { order: bo, .. }) = (sell, buy) else {
        panic!()
    };
    // a retry of the same reserve returns the same order and holds nothing more
    let again = ok(ledger[0].cmd(
        3,
        1,
        &Cmd::Reserve {
            account: 2,
            symbol: 10,
            side: Side::Buy,
            price: 101,
            qty: 2,
        },
    ));
    assert_eq!(
        again,
        Reply::Reserved {
            order: bo,
            outbox: 0
        }
    );
    assert_eq!(ledger[0].balance(true, 2, 2).unwrap(), (100_000 - 202, 202));
    // an overdraft is refused, with a reason
    let r = ok(ledger[0].cmd(
        3,
        2,
        &Cmd::Reserve {
            account: 2,
            symbol: 10,
            side: Side::Buy,
            price: 100,
            qty: 10_000,
        },
    ));
    assert_eq!(r, Reply::Rejected("Insufficient".into()));

    let mut fwd: Vec<ForwarderRelay> = (0..2).map(ForwarderRelay::new).collect();
    let mut set: Vec<SettlerRelay> = (0..2)
        .map(|s| SettlerRelay::new(s, vec![100, 101]))
        .collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        for s in 0..2 {
            fwd[s].step(&mut ledger[s], &mut tc).unwrap();
            set[s].step(&mut ledger[s], &mut tc).unwrap();
        }
        let both_closed = ledger[0].order(true, bo).unwrap().is_none()
            && ledger[1].order(true, so).unwrap().is_none();
        if both_closed {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "orders never settled");
        std::thread::sleep(Duration::from_millis(10));
    }
    // the buyer's order reached matching first and rests at 101, so the trade is at
    // 101: the buyer paid its whole hold of 202 and got 2000 base less a 1-atom fee;
    // the seller got 202 quote (the fee floors to 0)
    assert_eq!(ledger[0].balance(true, 2, 2).unwrap(), (100_000 - 202, 0));
    assert_eq!(ledger[0].balance(true, 2, 1).unwrap(), (100_000 + 1_999, 0));
    assert_eq!(ledger[1].balance(true, 3, 2).unwrap(), (100_000 + 202, 0));
    assert_eq!(ledger[1].balance(true, 3, 1).unwrap(), (100_000 - 2_000, 0));

    // the tick driver: the clock moves, and a sweep of nothing is harmless
    let t = TickRelay {
        max_age_ns: 1,
        retry_ns: 1,
        max: 10,
    };
    t.clock(&mut ledger[0], 12_345).unwrap();
    assert_eq!(t.sweep(&mut ledger[0]).unwrap(), 0);
    assert_eq!(ledger[0].state(true, &[100]).unwrap().clock, 12_345);

    // the audit, from outside: clean and caught up, conservation checked
    let mut audit = Audit::new(
        gws.iter().map(|a| GatewayClient::new(vec![*a])).collect(),
        TierClient::new(vec![tier_srv.addr]),
        vec![100, 101],
        demo_config,
    );
    let rep = audit.round().unwrap();
    assert!(rep.violations.is_empty(), "{:?}", rep.violations);
    assert!(rep.caught_up, "{}", rep.summary);
    assert_eq!(rep.events, 1, "one trade");
    assert!(audit.verify_orders(&[(bo, 2), (so, 2)]).unwrap().is_empty());
    // a claim about an order nobody made is caught
    assert!(
        !audit.verify_orders(&[(bo, 2)]).unwrap().is_empty(),
        "the seller's order is unacknowledged"
    );
    let dep = [(1u32, 200_000u128), (2, 200_000)].into_iter().collect();
    assert!(audit
        .verify_money(&dep, &Default::default())
        .unwrap()
        .is_empty());
    let wrong = [(1u32, 200_001u128), (2, 200_000)].into_iter().collect();
    assert_eq!(
        audit
            .verify_money(&wrong, &Default::default())
            .unwrap()
            .len(),
        1
    );
    drop(keep);
    tier.crash();
    let _ = std::fs::remove_dir_all(&dir);
}
