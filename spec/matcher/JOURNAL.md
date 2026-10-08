# JOURNAL — persistence, snapshot and replay contract

Determinism (SPEC §7) is what makes persistence simple here: a book is a pure
function of its command stream. Persistence is therefore *recording inputs
and outputs*, not serializing magic state.

## 1. Command journal (ingress)

Every command applied to a book/engine is first appended to the command
journal — one canonical command line per record, exactly the `.cmd.jsonl`
grammar in SCHEMA.md (with `"symbol"` when the writer is an engine tap):

```json
{"cmd":"new","symbol":10,"order_id":1,"side":"bid","otype":"limit","price":100,"qty":10,"tif":"gtc"}
```

Ordering rule: **journal before apply**. A crash after the append replays to
the identical state; a crash before it never happened. The journal tail is
the recovery log between snapshots.

## 2. Event journal (egress)

The sink seam is the journal point: wrap the downstream `Sink` so every
canonical event line is appended before (or while) it is delivered:

```json
{"seq":41,"ev":"trade","symbol":10,"maker":1,"taker":7,"price":90,"qty":3}
```

Because the stream is deterministic, a correct recovery produces a
byte-identical event journal — the strongest correctness check a matching
engine can offer: replay `cmd` journal ⇒ diff event journals ⇒ must be equal.

## 3. Snapshot format (`matcher-snap/1`)

JSONL, same compact conventions as SCHEMA.md. Captures *resting* state only —
there is no in-flight state: a command either fully applied or not at all.

```json
{"format":"matcher-snap/1","pmin":0,"pmax":1000000,"max_orders":65536,"index":"ladder"}
{"rec":"book","symbol":10,"seq":41}
{"rec":"order","order_id":1,"side":"bid","otype":"limit","tif":"gtc","price":100,"qty":6}
{"rec":"order","order_id":3,"side":"bid","otype":"limit","tif":"gtc","price":99,"qty":2}
{"rec":"order","order_id":7,"side":"ask","otype":"limit","tif":"post_only","price":105,"qty":3}
{"rec":"book","symbol":20,"seq":12}
...
```

All lines are flat objects (`"rec"` discriminates) so the existing flat-JSON
readers parse snapshots with no nesting support.

- Header: the engine's default `BookConfig` (per-book configs are a
  deployment concern — a book that needed a different config should be
  restored with it explicitly).
- `{"rec":"book",...}` opens a book block: `symbol`, and `seq` = the last
  emitted per-book sequence (0 if none). A single-book snapshot still uses a
  book block (symbol may be 0/absent → 0).
- `{"rec":"order",...}` — one per resting order, `qty` = leaves qty. Emitted
  in **book order**: bids best-price-first, asks best-price-first, FIFO
  within each level. Book order *is* priority order; restoring by insertion
  in this order reproduces exact FIFO position.
- A resting order's `tif` is preserved (informational; a snapshot order is
  always resting semantics — original TIF kept for audit/reconstruction).

Restore semantics: insert each order directly as resting, in file order —
no matching occurs (a correct book is never crossed: every `apply` leaves
bids < asks). The next command continues with `seq` resumed.

Determinism check: `snap → restore → snap` must be byte-identical, and
`restore → continue` must emit byte-identical events vs uninterrupted run.

## 4. Recovery procedure

```
book = snapshot(latest.snap) or empty
for cmd in journal-tail after snapshot point: book.apply(cmd, sink)
diff(sink output, original event journal tail)   // must be identical
```

## 5. Metrics seam

Optional wrapper sink — counts by event kind, exposes `trades`,
`fills_qty`, `rejects{reason}`, `resting` deltas. Metrics are *derived* —
never stored, never authoritative. Implementations ship a `MetricsSink`
decorator; keeping it outside the book keeps the hot path clean.

## 6. Backpressure contract

`apply`/`submit` is synchronous: the sink runs on the writer's thread. A slow
journal/sink stalls matching — production wraps sinks in a bounded egress
buffer and treats a full buffer as a liveness event (log + shed or block —
deployment choice, documented at the gateway).
