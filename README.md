# orderer-rust

[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

The reference implementation of [orderer](https://github.com/abhijitkrm/orderer):
an LMAX-Disruptor-style, multi-core order-matching engine around the
[matcher](https://github.com/abhijitkrm/matcher) order book. It implements
`orderer-spec/1.2`.

```
Handle::publish ─▶ ingress ─▶ router ─┬─▶ inbox[p] ─▶ engine[p] ─▶ outbox[p] ─▶ egress plugs
 (any threads)    (multi-     (iseq,  │              (journal +                (journal, acks,
                   producer)   route) └─▶ …            apply)                   metrics, yours)
```

- **Sequenced.** One ingress ring gives every command a total-order `iseq`.
- **Partitioned.** Each symbol lives on one partition; each partition is a
  single-writer matcher `Engine` on its own thread.
- **Journal-before-apply.** Durable acks wait for fsync. Journal I/O runs
  on dedicated threads with group commit, never on the hot path.
- **Byte-identical to matcher.** At one partition the event stream is
  matcher's exactly. At any P, each symbol's stream is. This is proven
  against the golden vectors and differential fuzzing with all five
  matcher ports.
- **No steady-state allocation**, and `unsafe` in exactly one ring module
  (plus an opt-in, one-call QoS shim).

## Install

```toml
[dependencies]
orderer = { git = "https://github.com/abhijitkrm/orderer-rust", tag = "v0.2.0" }
```

The crates are ready for crates.io at 0.2.0 but not yet published. The
publish order is `orderer-core` and `orderer-disruptor` (both pass
`cargo package`), then `orderer`, which depends on them.

## Quick start

```rust
use orderer::*;
use orderer_core::*;

let (collect, events) = Collect::new(true);
let mut p = Pipeline::<FifoCore>::builder()
    .partitions(2)
    .journal(JournalConfig::new(&dir, JournalFormat::Binary)) // durable, fsync every 1024
    .egress(collect) // or Acks, Metrics, Callback, your own Egress
    .build()?;

let h = p.handle(); // cloneable; publish from any thread
h.publish(7, Command::new(1, Side::Ask, 100, 10, Tif::Gtc))?;
h.publish(7, Command::new(2, Side::Bid, 100, 4, Tif::Gtc))?;
p.drain()?; // applied and delivered
p.snapshot()?.write(dir.join("books.snap"))?; // consistent cut, matcher-snap/1
p.shutdown()?;
```

The full version is `cargo run -p orderer --example quickstart`.

## Plug points

| Seam | Trait / type | Built-ins |
|---|---|---|
| Matching core | `orderer_core::MatchingCore` | `FifoCore` (matcher `Engine`), `NoopCore` (echo, for pipeline tests) |
| Egress | `orderer::Egress` + `EgressFactory` (one instance per partition) | `Collect`, `Callback`, `Acks` (durability-gated), `Metrics` (e2e latency) |
| Routing | `PartitionMap` | hash (spec/ROUTING.md), plus table overrides |
| Journals | `JournalConfig`, `FsyncPolicy`, `JournalPlacement` | JSONL or binary; inline (default) or a staged LMAX diamond |
| Waiting | `Waits` / `WaitStrategy` | BusySpin, Yield, Backoff, Blocking, per stage |
| Recovery | `orderer::recover` | snapshot + journals → cores at any P; `journal::repair_dir` for torn tails |
| Observability | `Pipeline::stats`, `PipelineStats::to_prometheus` | ring depths, counters, watermarks, fsync timings |
| Checkpoints | `Pipeline::checkpoint` | durable snapshot + journal segment rotation; old segments removed |

## Crates

| Crate | What it is |
|---|---|
| `orderer-core` | Matching semantics: matcher-rust, vendored, plus the `MatchingCore` seam. `#![forbid(unsafe_code)]` |
| `orderer-disruptor` | Generic LMAX Disruptor: `RingBuffer`, `Sequence`, single and multi producers, `Consumer` barriers, wait strategies, `EventProcessor`, a DSL. loom-checked |
| `orderer` | The pipeline, journals, recovery, egress plugs, and the five harness binaries |

Each crate is usable alone: just the book, just the ring, or the whole
engine.

## Harnesses (spec/HARNESS.md)

```bash
scripts/build-harness.sh            # → harness/bin/{orderrun,ordererfuzz,orderrecover,ordersnap,orderbench}
harness/bin/orderrun vectors/matcher/engine/001_multisymbol.cmd.jsonl --partitions 3 --journal-dir /tmp/j --binary
harness/bin/orderrecover --journal-dir /tmp/j --binary --partitions 2
harness/bin/orderbench bench/w6 --mode pipe --partitions 2
```

## Verify

```bash
scripts/test.sh                     # cargo test --workspace --release: golden + orderer vectors, integration, no-alloc
RUSTFLAGS="--cfg loom" cargo test -p orderer-disruptor --test loom --release
scripts/vendored.sh                 # vendored spec/ + vectors/ untouched
# cross-implementation proofs live in the spec repo: scripts/{diffuzz,exhaustive,e2e,snapdiff}.sh
```

## Performance

On an Apple M1 (4 performance + 4 efficiency cores):

| Configuration | Throughput |
|---|---|
| Pipeline alone (`NoopCore`) | 63–67M cmds/s |
| Matching, journals off, W6 | 14.3M (P=1) to 31.3M (P=3) |
| Durable journals (binary, `F_FULLFSYNC` group commit) | 11–12M at P=1, 18.5M at P=2, up to about 24M at best |

The spec's scaling gate (efficiency ≥ 0.9 at P=1, 2, 4 with durable
journals) is **not met on this machine**: about 0.85 / 0.66 / 0.3. The
limits are the drive's flush bandwidth and only 4 performance cores. See
the spec repo's `docs/RESULTS.md` for the full matrix and gap analysis,
and [`docs/DESIGN.md`](docs/DESIGN.md) for the architecture, the `unsafe`
protocol, the tuning guide, and the porting checklist.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
