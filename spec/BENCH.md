# BENCH — orderer benchmark methodology v1

Extends matcher's `spec/matcher/BENCH.md`. Core-mode numbers use matcher's
protocol unchanged. Pipeline mode adds multi-core measurement, and it gates
on **scaling efficiency**, not absolute throughput, so the gate means the
same thing in every language.

## 1. Corpora

`tools/ordergen` (seeded xorshift64, same generator as matcher's tools).
Corpora are not committed; regenerate them on demand:

```bash
ordergen --workload w4 --out bench/w4                 # w1–w5: byte-identical to matcher's vectorgen
ordergen --workload w6 --symbols 64 --out bench/w6    # multi-symbol engine corpus
```

| Workload | Shape | Use |
|---|---|---|
| W1–W5 | matcher `BENCH.md` §1, single book | core-mode parity with matcher; pipeline P=1 overhead |
| **W6** | `engine:true`, K symbols (default 64), per-symbol W4 mix | pipeline scaling gate |

**W6 definition.** All randomness comes from one xorshift64 stream seeded with
`seed | 1`. Each symbol `s ∈ [0, K)` keeps its own matcher-W4 generator state
(next order id from 1, live-id list). Order ids are per-symbol (matcher
`SPEC.md` §6).

- Setup: `setup-n` steps (default `1000 × K`, about 1k live orders per
  book). Each step picks `s = below(K)`, then emits one W1-style non-crossing
  GTC add on `s`.
- Run: `n` steps (default 2,000,000). Each picks `s = below(K)`, then emits
  one W4-mix step on `s` (82% replace, 6% cancel, 3% crossing IOC, 9% GTC
  add, with matcher vectorgen's exact draws). A step that needs a live id
  when `s` has none falls through as in vectorgen.
- Header: `{"format":"matcher-vector/1","name":"w6","engine":true,"pmin":490000,"pmax":510000,"max_orders":M,"index":"ladder","workload":"w6","seed":S,"symbols":K}`.
  `M` = the largest per-symbol count of adds across setup and run, plus 16.
  This is an upper bound on any book's live orders. The narrow price band
  (MID ± 10,000) keeps ladder books small enough for many symbols.

`ordergen`'s source is normative for exact draw order. Its output for a
given seed must never change once published. CI checks determinism, and
checks W1–W5 against matcher's vectorgen.

## 2. Modes

### 2.1 Core mode: the language's baseline

matcher `BENCH.md` §2, verbatim, on the implementation's embedded matcher
core:

- single-book corpora (W1–W5) go through `OrderBook`
- engine corpora (W6) go through one `Engine` on one thread

Core-mode numbers should match the same language's `matcher_bench` within
±10%, since it is the same code. Larger gaps are investigated.

Core mode also replays the run **untimed**: same fresh book or engine and
untimed setup, then the run commands back to back with one wall-clock
measurement and no per-op clock reads. Its ops/s is reported as `untimed=`
in the row's config column. On Apple M1 the two per-op clock reads of §2
cost about as much as a W4 command, so timed ops/s is roughly half of
untimed. Latency percentiles come from the timed pass; the throughput
baseline comes from the untimed one.

### 2.2 Pipeline mode

1. Parse corpora into memory **before** timing.
2. **Warmup.** Build a pipeline, run setup plus the first 10% of run,
   shut it down, discard it.
3. Build a fresh pipeline with the configuration under test. Publish setup
   and drain (untimed).
4. **Timed region.** Start N producer threads. Producer `k` publishes, in
   corpus order, exactly the run commands whose `symbol % N == k`. That
   preserves per-symbol order, so the workload is identical for every N.
   The clock starts before the first publish and stops when a drain
   completes after the last publish.
5. **Latency.** Each command is stamped at publish. Its sample is taken when
   the **first** event it caused reaches egress (every command emits at
   least one event), as `t_egress − t_publish` in ns from a monotonic clock.
   Store all samples and report the exact mean, p50, p90, p99, p99.9 and max
   (matcher `BENCH.md` §2).
6. `ops/s` = run commands ÷ timed wall seconds.

**Journal configuration is fixed for gated rows:** binary **command**
journals (`JOURNAL.md` §2.2), group-committed with an fsync at least every
1024 records per partition. The fsync must be the platform's real
durability primitive (`F_FULLFSYNC` on macOS, `fsync`/`fdatasync` on
Linux). Event journals are off in gated rows: they are derived data, which
recovery re-derives byte-identically (`JOURNAL.md` §5), and they add about
2.5× the bytes per command. Rows with event journals, `jsonl`, fsync off or
journals off may be reported for analysis and must say so. They never
count toward the gate.

## 3. `orderbench` CLI

```
orderbench <corpus-prefix> --mode core [--tag NAME]
orderbench <corpus-prefix> --mode pipe --partitions P [--producers N]
           [--journal binary|jsonl|off] [--journal-dir DIR] [--fsync N] [--tag NAME]
```

- Reads `<prefix>.setup.cmd.jsonl` and `<prefix>.run.cmd.jsonl`.
- Defaults: `--producers 1`, `--journal binary`, `--fsync 1024`, command
  journals only, `--journal-dir` a fresh temporary directory, `--tag` the
  prefix's basename.
- Prints one report row (§4) to stdout and the environment to stderr.
  Implementation-specific tuning flags are allowed, and must be listed in
  the row's config column.

## 4. Report format

Append one block per run set to `docs/RESULTS.md`:

```
### <impl> @ <YYYY-MM-DD> <short-sha>
env: <CPU (core topology)> / <OS> / <toolchain + flags> / journal <mode> fsync <N>
| workload | mode | P | prod | ops | ops/s | eff | mean | p50 | p90 | p99 | p99.9 | max | config |
```

Latencies are ns. Core rows report per-op latency (matcher protocol);
pipeline rows report end-to-end latency (§2.2 step 5). `eff` is blank for
core rows, which carry `untimed=<ops/s>` in the config column.

## 5. The scaling gate

```
eff(P) = pipe_ops_s(W6, P) / (P × core_untimed_ops_s(W6))
```

Both sides are plain wall-clock throughput. (orderer-spec/1 divided by the
*timed* core ops/s, which pays per-op clock reads the pipeline doesn't and
so overstated `eff` by about 1.5×. Corrected in 1.1; the gate only got
stricter.)

The gate is `eff(P) ≥ 0.9` for every `P ∈ {1, 2, 4}` (an implementation
may lower the top `P` to the machine's performance-core count, and must
say so), with the journal configuration of §2.2, on the same machine and
build as the core row.

The gate measures the pipeline, not the language. A slow language's
pipeline must still scale its own core near-linearly. Absolute throughput
targets (for example orderer-rust's 40M cmds/s stretch goal) are
per-implementation goals, recorded in that repo, not spec.

## 6. Fairness rules

matcher `BENCH.md` §4 applies. In addition:

- Report the machine's core topology. Performance/efficiency splits
  matter: a busy-spinning thread on an efficiency core is a different
  experiment.
- Report every tuning knob that differs from the implementation's defaults:
  ring sizes, wait strategies, batch sizes, thread placement.
- Never meet the gate with journaling off.
- Use corpora long enough to reach steady state. At least 10M run commands
  for W6 pipeline rows: journal buffers absorb the first several hundred
  milliseconds of I/O, so short runs overstate durable throughput.
