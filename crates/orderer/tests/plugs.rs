//! The two plug seams (plan §5.3 `plug_core.rs`, `plug_egress.rs`), and a
//! failing core stopping the pipeline instead of hanging it.

mod common;

use std::sync::{Arc, Mutex};

use common::*;
use orderer::*;
use orderer_core::*;

#[test]
fn noop_core_sees_every_command() {
    let cmds = fuzz_corpus(5, 5_000, 8);
    let (collect, events) = Collect::new(true);
    let mut p = Pipeline::<NoopCore>::builder()
        .partitions(3)
        .egress(collect)
        .build()
        .unwrap();
    p.publish_batch(&cmds).unwrap();
    p.drain().unwrap();
    p.shutdown().unwrap();
    let got: Vec<String> = events.take().iter().flat_map(|b| lines(b)).collect();
    assert_eq!(got.len(), cmds.len(), "one echo per command");
    assert_dense(&got);
    // each symbol's echoes follow its commands in order
    let mut per_sym: std::collections::BTreeMap<u32, Vec<Command>> = Default::default();
    for &(s, c) in &cmds {
        per_sym.entry(s).or_default().push(c);
    }
    for (sym, lines) in by_symbol(got.iter().map(String::as_str)) {
        let cmds = &per_sym[&(sym as u32)];
        for (l, c) in lines.iter().zip(cmds) {
            let id = orderer_core::jsonflat::get_u64(l, "order_id").unwrap();
            let want = match *c {
                Command::New { order_id, .. }
                | Command::Cancel { order_id }
                | Command::Replace { order_id, .. } => order_id,
            };
            assert_eq!(id, want);
        }
    }
}

/// (partition, events seen, batch ends, last iseq) per plug instance.
type Report = Arc<Mutex<Vec<(u32, u64, u64, u64)>>>;

/// A custom plug that checks it sees each partition's events in order.
struct OrderCheck {
    partition: u32,
    last_iseq: u64,
    seqs: std::collections::HashMap<u32, u64>,
    batches: u64,
    seen: u64,
    out: Report,
}

impl Egress for OrderCheck {
    fn on_event(&mut self, m: &EvtMsg) {
        assert!(
            m.iseq >= self.last_iseq,
            "iseq goes backwards within a partition"
        );
        self.last_iseq = m.iseq;
        let s = self.seqs.entry(m.symbol).or_insert(0);
        assert_eq!(m.seq, *s + 1, "per-book seq dense");
        *s = m.seq;
        self.seen += 1;
    }
    fn on_batch_end(&mut self) {
        self.batches += 1;
    }
    fn on_shutdown(&mut self) {
        self.out
            .lock()
            .unwrap()
            .push((self.partition, self.seen, self.batches, self.last_iseq));
    }
}

#[test]
fn custom_egress_sees_every_event_in_partition_order() {
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(9, 6_000, 12);
    let reference = reference_lines(cfg, &cmds);
    let out = Arc::new(Mutex::new(Vec::new()));
    let o = out.clone();
    let factory = move |ctx: &EgressCtx| -> Box<dyn Egress> {
        Box::new(OrderCheck {
            partition: ctx.partition,
            last_iseq: 0,
            seqs: Default::default(),
            batches: 0,
            seen: 0,
            out: o.clone(),
        })
    };
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(4)
        .stage_threads(2, 2)
        .egress(factory)
        .build()
        .unwrap();
    p.publish_batch(&cmds).unwrap();
    p.shutdown().unwrap();
    let out = out.lock().unwrap();
    assert_eq!(
        out.len(),
        4,
        "one instance per partition, each shut down once"
    );
    let total: u64 = out.iter().map(|r| r.1).sum();
    assert_eq!(
        total as usize,
        reference.len(),
        "every event delivered exactly once"
    );
    assert!(
        out.iter().all(|r| r.1 == 0 || r.2 > 0),
        "batch ends signalled"
    );
}

#[test]
fn callback_plug_is_cloned_per_partition() {
    let seen = Arc::new(Mutex::new(std::collections::BTreeSet::new()));
    let s = seen.clone();
    let mut p = Pipeline::<FifoCore>::builder()
        .partitions(3)
        .egress(Callback(move |part: u32, m: &EvtMsg| {
            s.lock().unwrap().insert((part, m.symbol));
        }))
        .build()
        .unwrap();
    for sym in 0..30 {
        p.publish(sym, Command::new(1, Side::Bid, 10, 1, Tif::Gtc))
            .unwrap();
    }
    p.shutdown().unwrap();
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 30);
    assert!(seen
        .iter()
        .all(|&(part, sym)| hash_partition(sym, 3) == part));
}

/// A core that panics on a poison order id.
struct PanicCore(FifoCore);

impl MatchingCore for PanicCore {
    fn new(cfg: BookConfig) -> Self {
        PanicCore(FifoCore::new(cfg))
    }
    fn apply<F: FnMut(Symbol, u64, &Event)>(&mut self, sym: Symbol, cmd: Command, emit: &mut F) {
        if let Command::Cancel { order_id: 666 } = cmd {
            panic!("poison command");
        }
        self.0.apply(sym, cmd, emit)
    }
    fn snapshot_blocks(&self, out: &mut Vec<(Symbol, String)>) {
        self.0.snapshot_blocks(out)
    }
    fn restore_book(&mut self, s: Symbol, q: u64, o: &[RestingOrder]) -> Result<(), RestoreError> {
        self.0.restore_book(s, q, o)
    }
}

#[test]
fn failing_core_fails_the_pipeline_instead_of_hanging() {
    let mut p = Pipeline::<PanicCore>::builder()
        .partitions(2)
        .build()
        .unwrap();
    p.publish(1, Command::new(1, Side::Bid, 10, 1, Tif::Gtc))
        .unwrap();
    p.publish(1, Command::cancel(666)).unwrap();
    match p.drain() {
        Err(Error::Failed(msg)) => assert!(msg.contains("engine"), "{msg}"),
        other => panic!("expected Failed, got {other:?}"),
    }
    assert!(matches!(p.shutdown(), Err(Error::Failed(_))));
}
