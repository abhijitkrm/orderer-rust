//! Phase 2 (credit slices) as services: the credit group's request port and client,
//! the slice driver, the audit's slice identity, leader-stamped time and the thinned
//! forwarder, over real TCP against single-replica groups, driven step by step.

use orderer::{PartitionMap, Pipeline};
use orderer_cluster::{Replica, ReplicaConfig};
use orderer_consensus::Config as RaftConfig;
use orderer_core::{BookConfig, FifoCore};
use orderer_exchange::audit::{Audit, SliceAudit};
use orderer_exchange::relay::{ForwarderRelay, SettlerRelay, SliceDriver, Thinning};
use orderer_exchange::tier::credit::CreditConfig;
use orderer_exchange::*;
use orderer_ledger::policy::PolicyConfig;
use orderer_ledger::{Cmd, OutMsg, Reject, Side, Slice};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const BTC: u32 = 1;
const USD: u32 = 2;
const SYM: u32 = 10;

fn temp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("orderer-sp-{name}-{}", std::process::id()));
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

struct Tier {
    _replica: Replica,
    _srv: ExchangeServer,
    addr: SocketAddr,
    view: TierView,
}

fn tier(
    dir: &std::path::Path,
    name: &str,
    source_base: u32,
    credit: Option<CreditConfig>,
    group: Option<(u32, u32)>,
) -> Tier {
    let (mut sm, view) = MatchingTierSm::<FifoCore>::new(
        BookConfig::default(),
        PartitionMap::hash(2).unwrap(),
        TierConfig {
            source_base,
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
    if let Some((g, n)) = group {
        sm = sm.with_group(g, n);
    }
    if let Some(c) = credit {
        sm = sm.with_credit(c);
    }
    let replica = Replica::start(rcfg(dir, name), sm).unwrap();
    let srv = ExchangeServer::start(
        "127.0.0.1:0".parse().unwrap(),
        replica.handle(),
        view.clone(),
    )
    .unwrap();
    let addr = srv.addr;
    Tier {
        _replica: replica,
        _srv: srv,
        addr,
        view,
    }
}

struct Led {
    _replica: Replica,
    _srv: GatewayServer,
    addr: SocketAddr,
}

fn ledger(dir: &std::path::Path, shard: u16, stamp: bool) -> Led {
    let (sm, view) = LedgerNodeSm::new(demo_config(shard));
    let replica = Replica::start(rcfg(dir, &format!("l{shard}")), sm).unwrap();
    let srv = GatewayServer::start_stamped(
        "127.0.0.1:0".parse().unwrap(),
        replica.handle(),
        view,
        Auth::none(),
        stamp,
    )
    .unwrap();
    let addr = srv.addr;
    Led {
        _replica: replica,
        _srv: srv,
        addr,
    }
}

fn credit_cfg(epoch: u64) -> CreditConfig {
    CreditConfig {
        ledger: demo_config(0),
        group: 0,
        epoch,
        retain: 1 << 16,
    }
}

struct World {
    t: Tier,
    leds: Vec<Led>,
    gws: Vec<GatewayClient>,
    tc: TierClient,
    fwd: Vec<ForwarderRelay>,
    set: SettlerRelay,
    seq: u64,
}

fn world(name: &str, epoch: u64, stamp: bool) -> World {
    let dir = temp(name);
    let t = tier(&dir, "tier", 100, Some(credit_cfg(epoch)), None);
    let leds: Vec<Led> = (0..2).map(|s| ledger(&dir, s, stamp)).collect();
    let gws = leds
        .iter()
        .map(|l| GatewayClient::new(vec![l.addr]))
        .collect();
    let mut tc = TierClient::new(vec![t.addr]);
    tc.budget = Duration::from_secs(10);
    World {
        fwd: (0..2).map(|s| ForwarderRelay::new(s, 1)).collect(),
        set: SettlerRelay::new(vec![0, 1], vec![(100, 0), (101, 0), (102, 0)]),
        t,
        leds,
        gws,
        tc,
        seq: 0,
    }
}

impl World {
    fn cmd(&mut self, shard: usize, c: Cmd) -> Reply {
        self.seq += 1;
        self.gws[shard].cmd(77, self.seq, &c).unwrap()
    }
    fn fund(&mut self, account: u64, asset: u32, amount: u128) {
        let shard = (account % 2) as usize;
        let id = 1000 + account * 10 + asset as u64;
        let r = self.cmd(
            shard,
            Cmd::Deposit {
                account,
                asset,
                amount,
                external_id: id,
            },
        );
        assert_eq!(r, Reply::Other("Deposited".into()));
    }
    fn lend(&mut self, account: u64, asset: u32, amount: u128) {
        let shard = (account % 2) as usize;
        let r = self.cmd(
            shard,
            Cmd::Lend {
                group: 0,
                account,
                asset,
                amount,
            },
        );
        assert!(matches!(r, Reply::Other(_)), "{r:?}");
    }
    /// One round of every relay and a credit epoch tick.
    fn pump(&mut self) {
        for s in 0..2 {
            self.fwd[s]
                .step(&mut self.gws[s], std::slice::from_mut(&mut self.tc))
                .unwrap();
        }
        self.set
            .step(&mut self.gws, std::slice::from_mut(&mut self.tc))
            .unwrap();
        self.tc.tick().unwrap().unwrap();
    }
    fn until(&mut self, what: &str, mut f: impl FnMut(&mut World) -> bool) {
        let end = Instant::now() + Duration::from_secs(30);
        loop {
            self.pump();
            if f(self) {
                return;
            }
            assert!(Instant::now() < end, "timed out: {what}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    /// What shard `shard`'s ledger shows as lent to the group for `account`.
    fn lent(&mut self, shard: usize, account: u64, asset: u32) -> u128 {
        let snap = self.gws[shard].snapshot().unwrap();
        let mut l = orderer_ledger::Ledger::new(demo_config(shard as u16));
        l.restore(&snap).unwrap();
        l.lent(account, asset)
    }
    fn slice(&mut self) -> Slice {
        let st = self.tc.slice_state().unwrap().unwrap();
        let mut s = Slice::new(demo_config(0), 0);
        s.restore(&st.snapshot).unwrap();
        s
    }
    fn audit(&self) -> Audit {
        Audit::new(
            self.leds
                .iter()
                .map(|l| GatewayClient::new(vec![l.addr]))
                .collect(),
            vec![TierClient::new(vec![self.t.addr])],
            vec![(100, 0), (101, 0), (102, 0)],
            demo_config,
        )
        .with_slices(SliceAudit {
            group_idx: 0,
            source: 102,
            group: 0,
        })
    }
}

fn slice_client(w: &World) -> SliceClient {
    let mut t = TierClient::new(vec![w.t.addr]);
    t.budget = Duration::from_secs(10);
    SliceClient::new(t, SliceClient::fresh_id())
}

#[test]
fn orders_entered_at_the_group_trade_settle_and_the_audit_ties_slice_to_ledgers() {
    let mut w = world("e2e", 3, false);
    // account 3 (shard 1) holds base, account 2 (shard 0) holds quote
    w.fund(3, BTC, 100_000);
    w.fund(2, USD, 100_000);
    w.lend(3, BTC, 50_000);
    w.lend(2, USD, 50_000);
    w.until("the group has both slices", |w| {
        let s = w.slice();
        s.total(3, BTC) == 50_000 && s.total(2, USD) == 50_000
    });
    let mut sc = slice_client(&w);
    let sell = sc
        .enter(3, SYM, Side::Sell, 100, 2, false)
        .unwrap()
        .unwrap();
    let buy = sc.enter(2, SYM, Side::Buy, 101, 2, false).unwrap().unwrap();
    assert!(orderer_ledger::is_slice_order(sell) && orderer_ledger::is_slice_order(buy));
    // no credit: refused by the group, with a reason, and demand recorded
    assert_eq!(
        sc.enter(2, SYM, Side::Buy, 100, 1_000_000, false).unwrap(),
        Err(Reject::Insufficient)
    );
    assert_eq!(
        sc.enter(5, SYM, Side::Buy, 100, 1, false).unwrap(),
        Err(Reject::Insufficient)
    );
    let st = w.tc.slice_state().unwrap().unwrap();
    assert!(st.demand.contains(&(2, USD)) && st.demand.contains(&(5, USD)));
    // both orders crossed and filled inside the group; reports reach the ledgers
    w.until("fills reported to the ledgers", |w| {
        // the buyer's slice now holds base, the seller's quote: both as lent funds
        w.lent(0, 2, BTC) > 0 && w.lent(1, 3, USD) > 0
    });
    // the audit is clean, the system reaches rest and the slice identity is checked
    let mut audit = w.audit();
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        w.pump();
        let rep = audit.round().unwrap();
        assert!(rep.violations.is_empty(), "{:?}", rep.violations);
        if rep.at_rest {
            break;
        }
        assert!(Instant::now() < end, "never at rest: {}", rep.summary);
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn a_slice_request_repeated_through_retries_enters_once() {
    let mut w = world("retry", 1000, false);
    w.fund(2, USD, 10_000);
    w.lend(2, USD, 10_000);
    w.until("slice", |w| w.slice().total(2, USD) == 10_000);
    let req = |seq| orderer_exchange::wire::SliceReq {
        client: 0xAB,
        seq,
        body: orderer_exchange::wire::SliceBody::Enter {
            account: 2,
            symbol: SYM,
            side: Side::Buy,
            price: 100,
            qty: 1,
        },
    };
    let mut c = TierClient::new(vec![w.t.addr]);
    let first = c.slice_request(&req(1)).unwrap().unwrap();
    let again = c.slice_request(&req(1)).unwrap().unwrap();
    assert_eq!(first, again, "a retry gets the same answer");
    assert!(matches!(first, SliceAnswer::Entered(_)));
    assert_eq!(w.slice().open_orders(), 1, "and enters nothing more");
    assert_eq!(w.slice().available(2, USD), 10_000 - 100);
    // a later request moves the client on; the old one is stale, not applied
    let second = c.slice_request(&req(2)).unwrap().unwrap();
    assert!(matches!(second, SliceAnswer::Entered(_)) && second != first);
    assert_eq!(
        c.slice_request(&req(1)).unwrap().unwrap(),
        SliceAnswer::Stale(2)
    );
    assert_eq!(w.slice().open_orders(), 2);
    // another client's numbering is independent
    let mut other = req(1);
    other.client = 0xCD;
    assert!(matches!(
        c.slice_request(&other).unwrap().unwrap(),
        SliceAnswer::Entered(_)
    ));
    assert_eq!(w.slice().open_orders(), 3);
    // the SliceClient keeps stop-and-wait and resolves a pending request
    let mut sc = slice_client(&w);
    assert!(sc.enter(2, SYM, Side::Buy, 100, 1, false).unwrap().is_ok());
    assert_eq!(sc.resolve_pending().unwrap(), None);
}

#[test]
fn a_group_not_in_credit_mode_refuses_slice_requests() {
    let dir = temp("nocredit");
    let t = tier(&dir, "tier", 100, None, None);
    let mut c = TierClient::new(vec![t.addr]);
    c.budget = Duration::from_secs(5);
    let q = orderer_exchange::wire::SliceReq {
        client: 1,
        seq: 1,
        body: orderer_exchange::wire::SliceBody::Cancel {
            account: 1,
            order: 1,
        },
    };
    assert!(matches!(
        c.slice_request(&q).unwrap(),
        Err(Refusal::Error(_))
    ));
    assert!(matches!(c.slice_state().unwrap(), Err(Refusal::Error(_))));
}

#[test]
fn the_driver_lends_on_demand_and_takes_idle_credit_back() {
    let mut w = world("driver", 2, false);
    w.fund(2, USD, 1_000_000);
    w.fund(3, BTC, 1_000_000);
    let mut chunk = std::collections::BTreeMap::new();
    chunk.insert(USD, 200_000u128);
    chunk.insert(BTC, 300_000u128);
    let cfg = PolicyConfig {
        chunk,
        low_water_pct: 25,
        idle_ns: 1_000,
        max_actions: 16,
        ..Default::default()
    };
    let mut d = SliceDriver::new(cfg, 2, 0);
    let mut sc = slice_client(&w);
    // refused for lack of credit: that is the demand
    assert_eq!(
        sc.enter(2, SYM, Side::Buy, 100, 1, false).unwrap(),
        Err(Reject::Insufficient)
    );
    let mut now = 0u64;
    let step = |w: &mut World, d: &mut SliceDriver, now: u64| {
        d.step(&mut w.gws, &mut w.tc, &demo_config, now).unwrap()
    };
    let s = step(&mut w, &mut d, now);
    assert_eq!((s.lends, s.refused, s.ticked), (1, 0, true));
    // before the grant reaches the group a second look does not lend again
    now += 10;
    let s = step(&mut w, &mut d, now);
    assert_eq!(s.lends, 0, "grants in flight count as credit");
    w.until("the grant arrives", |w| w.slice().total(2, USD) == 200_000);
    assert_eq!(
        w.gws[0].balance(true, 2, USD).unwrap(),
        (800_000, 0),
        "the lent funds left `available`"
    );
    // the retry is now accepted and the demand is gone
    assert!(sc.enter(2, SYM, Side::Buy, 100, 1, false).unwrap().is_ok());
    assert!(w.tc.slice_state().unwrap().unwrap().demand.is_empty());
    // idle: the free credit is recalled after idle_ns, committed funds are not
    for _ in 0..4 {
        now += 5_000;
        step(&mut w, &mut d, now);
        w.pump();
    }
    w.until("idle credit returned", |w| {
        w.slice().total(2, USD) == 100 && w.gws[0].balance(true, 2, USD).unwrap().0 == 999_900
    });
    let mut audit = w.audit();
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        w.pump();
        let rep = audit.round().unwrap();
        assert!(rep.violations.is_empty(), "{:?}", rep.violations);
        if rep.at_rest {
            break;
        }
        assert!(Instant::now() < end, "never at rest: {}", rep.summary);
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn a_withdrawal_can_only_draw_on_available_funds_not_lent_ones() {
    let mut w = world("s4", 1000, false);
    w.fund(2, USD, 1_000);
    w.lend(2, USD, 800);
    let wd = |id, amount| Cmd::Withdraw {
        account: 2,
        asset: USD,
        amount,
        external_id: id,
    };
    // 1000 deposited, 800 lent: only 200 can be paid out
    assert_eq!(w.cmd(0, wd(1, 500)), Reply::Rejected("Insufficient".into()));
    assert_eq!(w.cmd(0, wd(2, 200)), Reply::Other("WithdrawHeld".into()));
    w.until("slice", |w| w.slice().total(2, USD) == 800);
    // committed funds are not recalled either: enter an order, then recall everything
    let mut sc = slice_client(&w);
    assert!(sc.enter(2, SYM, Side::Buy, 10, 30, false).unwrap().is_ok());
    let r = w.cmd(
        0,
        Cmd::Recall {
            group: 0,
            account: 2,
            asset: USD,
            amount: 800,
        },
    );
    assert!(matches!(r, Reply::Other(_)), "{r:?}");
    w.until("recall settled", |w| {
        w.gws[0].balance(true, 2, USD).unwrap().0 == 500
    });
    // 300 stay committed in the order; 500 came back (800 - 300) and remain lent... no:
    // available was 0 after the withdrawal hold and lend, so it is the 500 returned
    assert_eq!(w.slice().total(2, USD), 300);
    let mut audit = w.audit();
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        w.pump();
        let rep = audit.round().unwrap();
        assert!(rep.violations.is_empty(), "{:?}", rep.violations);
        if rep.at_rest {
            break;
        }
        assert!(Instant::now() < end, "never at rest: {}", rep.summary);
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn the_audit_catches_credit_the_ledger_never_lent() {
    // (a grant whose amount differs from what the ledger lent: the intake cursor is
    // consistent, so only the slice identity can see it)
    let mut w = world("forged", 1000, false);
    // no relays run in this test: the forged delivery is the only one
    w.fund(2, USD, 10_000);
    // the ledger lends 1,000; a forwarder bug delivers the grant as 5,000
    w.lend(2, USD, 1_000);
    let mut direct = TierClient::new(vec![w.t.addr]);
    direct
        .forward(
            0,
            0,
            &[OutMsg::Grant {
                group: 0,
                account: 2,
                asset: USD,
                amount: 5_000,
            }],
        )
        .unwrap()
        .unwrap();
    assert_eq!(w.slice().total(2, USD), 5_000);
    let mut audit = w.audit();
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        w.tc.tick().unwrap().unwrap();
        let rep = audit.round().unwrap();
        if let Some(v) = rep.violations.first() {
            assert!(v.contains("disagree"), "{v}");
            return;
        }
        assert!(Instant::now() < end, "never noticed: {}", rep.summary);
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn a_stamping_leader_puts_its_time_in_the_ledger_clock() {
    let mut w = world("stamp", 1000, true);
    assert_eq!(w.gws[0].state(true, &[100]).unwrap().clock, 0);
    w.fund(2, USD, 10);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let clock = w.gws[0].state(true, &[100]).unwrap().clock;
    assert!(
        clock > now - 60_000_000_000 && clock <= now + 60_000_000_000,
        "clock {clock} vs {now}"
    );
    // a command that is already stamped is left alone (no nesting), an old stamp
    // cannot lower the clock
    let r = w.cmd(
        0,
        Cmd::Deposit {
            account: 2,
            asset: USD,
            amount: 1,
            external_id: 99,
        }
        .at(5),
    );
    assert_eq!(r, Reply::Other("Deposited".into()));
    assert!(w.gws[0].state(true, &[100]).unwrap().clock >= clock);
}

#[test]
fn a_thinned_forwarder_sends_each_group_only_its_own_messages() {
    let dir = temp("thin");
    // two groups; pick two symbols the shard map puts in different groups
    let map = orderer_cluster::shard::ShardMap::new(2);
    let a = (10u32..).find(|s| map.group_of(*s) == 0).unwrap();
    let b = (10u32..).find(|s| map.group_of(*s) == 1).unwrap();
    let mk = |s: u16| {
        let mut c = demo_config(s);
        for sym in [a, b] {
            c.symbols.insert(
                sym,
                orderer_ledger::Symbol {
                    base: BTC,
                    quote: USD,
                    lot: 10,
                },
            );
        }
        c
    };
    let t0 = tier(&dir, "t0", 100, None, Some((0, 2)));
    let t1 = tier(&dir, "t1", 200, None, Some((1, 2)));
    let (sm, view) = LedgerNodeSm::new(mk(0));
    let r = Replica::start(rcfg(&dir, "l0"), sm).unwrap();
    let g = GatewayServer::start("127.0.0.1:0".parse().unwrap(), r.handle(), view).unwrap();
    let mut gw = GatewayClient::new(vec![g.addr]);
    let mut seq = 0;
    let mut cmd = |gw: &mut GatewayClient, c: Cmd| {
        seq += 1;
        gw.cmd(5, seq, &c).unwrap()
    };
    cmd(
        &mut gw,
        Cmd::Deposit {
            account: 1,
            asset: USD,
            amount: 1_000_000,
            external_id: 1,
        },
    );
    let mut ordered = Vec::new();
    for i in 0..30u64 {
        let symbol = if i % 3 == 0 { a } else { b };
        let reply = cmd(
            &mut gw,
            Cmd::Reserve {
                account: 1,
                symbol,
                side: Side::Buy,
                price: 10,
                qty: 1,
            },
        );
        let Reply::Reserved { order, .. } = reply else {
            panic!("{reply:?}")
        };
        ordered.push((symbol, order));
    }
    let mut tiers = vec![
        TierClient::new(vec![t0.addr]),
        TierClient::new(vec![t1.addr]),
    ];
    let mut f = ForwarderRelay::new(0, 2).thinned(Thinning {
        map,
        credit_groups: Vec::new(),
    });
    f.chaos = 2; // duplicates and overlaps of every kind, too
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        f.step(&mut gw, &mut tiers).unwrap();
        let (c0, c1) = (t0.view.cursor(0), t1.view.cursor(0));
        if c0 == 30 && c1 == 30 {
            break;
        }
        assert!(Instant::now() < end, "cursors {c0} {c1}");
    }
    // each group holds exactly its own orders, once
    let own = |g: u32| {
        ordered
            .iter()
            .filter(|(s, _)| map.group_of(*s) == g)
            .count()
    };
    for (view, g) in [(&t0.view, 0u32), (&t1.view, 1u32)] {
        let (_, live) = view.stats();
        assert_eq!(live, own(g), "group {g} has exactly its own orders");
    }
    assert_eq!(own(0) + own(1), 30);
}
