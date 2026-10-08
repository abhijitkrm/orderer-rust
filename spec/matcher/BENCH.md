# BENCH — benchmark methodology v1

Goal: **comparable** latency/throughput numbers across implementations.
Same workloads, same measurement protocol, same report format. Relative
comparison on the dev machine (macOS arm64); absolute HFT targets are set from
measured data, not aspiration.

## 1. Corpora

Generated deterministically by `tools/vectorgen` (seeded xorshift64; seed
recorded in the file header). Not committed — regenerate on demand:

```bash
vectorgen --workload w1 --out bench/w1 [--seed 42]
# emits bench/w1.setup.cmd.jsonl  (book-building prefix, untimed)
#        bench/w1.run.cmd.jsonl   (measured commands)
```

| Workload | Setup | Run (measured) | Exercises |
|---|---|---|---|
| W1 build | — | 1M non-crossing GTC adds, both sides, ±5000 ticks around mid | insert path, pool, index |
| W2 sweep | 200k GTC dense ±200 ticks | 50k marketable limits + IOCs sweeping 1–10 levels | match loop, trade emit, level teardown |
| W3 churn | 100k GTC adds | 100k ops: ~80% cancel/replace on live ids, ~20% adds | lookup, unlink, re-queue |
| W4 mixed | 100k GTC adds | 1M ops ≈ exchange mix: 9% GTC, 3% IOC, 6% cancel, 82% replace | realistic blended path |
| W5 depth | books of 1k / 100k / 1M live orders | same W4 mix at each size | depth sensitivity curve |

(Counts are defaults; `--n` overrides. Keep corpora deterministic for a given
seed so runs are reproducible and comparable across languages.)

## 2. Measurement protocol

Identical in every implementation:

1. Parse corpora into in-memory command arrays **before** timing (I/O and
   parsing excluded).
2. Construct book sized to corpus (`max_orders` ≥ live-order high-water mark).
3. **Warmup**: run `setup` + first 10% of `run` on a throwaway book.
4. Fresh book: replay `setup` (untimed), then each `run` command:
   `t0=now(); book.apply(cmd, &NullSink); t1=now()`; store `t1-t0` ns.
5. Also record total wall time → throughput ops/s.

- Latency samples: store **all** per-op nanosecond deltas in a `Vec<u64>`;
  sort at end; report exact p50/p90/p99/p99.9/max + mean.
- Single-threaded. No logging/allocation in the timed loop. NullSink = counting
  no-op that the compiler cannot elide (sum seq into a volatile sink).
- Release builds only: Rust `lto="fat", codegen-units=1`; Go default `-O`
  (note `GOGC` variant); C++ `-O3 -flto -march=native` where supported.

## 3. Report format

Append one block per run to `docs/RESULTS.md`:

```
### <impl> @ <YYYY-MM-DD> <short-sha>
env: Apple M1 / macOS 14 / rustc 1.98.1 (lto=fat,cgu=1)
| workload | ops | ops/s | p50 | p90 | p99 | p99.9 | max |
```

## 4. Fairness rules

- Identical data structures and algorithms in all implementations (pool,
  intrusive levels, ladder index, open-addressed map).
- Pre-reserve capacities; no growth inside timed sections.
- Go reports two rows: default GC and `GOGC=off` (documented, not hidden).
- No claim of absolute HFT numbers on macOS: no core pinning, no kernel bypass.
  Linux deployment notes live in `docs/`; numbers here are comparative.
