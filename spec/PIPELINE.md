# PIPELINE — orderer pipeline contract v1

This is the **contract of record** for the orderer pipeline. Every
implementation (`orderer-rust`, `orderer-cpp`, …) must satisfy it exactly.
Matching semantics are not restated here. They are matcher's
(`spec/matcher/SPEC.md`), applied unchanged by each partition's engine.
This document specifies what the pipeline adds: sequencing, partitioning,
journaling, durability, control and egress.

Companion documents: `ROUTING.md` (symbol → partition), `JOURNAL.md`
(on-disk formats, snapshots, recovery), `HARNESS.md` (CLI contract),
`BENCH.md` (benchmark protocol).

---

## 1. Model

```
producers ─▶ ingress ─▶ router ─▶ partition 0 … P-1 ─▶ egress consumers
              (total     (ROUTING.md)  each: cmd journal → engine → events
               order)
```

- A pipeline has `P ≥ 1` **partitions**. Each partition owns one matcher
  `Engine` (`spec/matcher/SPEC.md` §6) and is its sole writer.
- Every symbol belongs to exactly one partition for the pipeline's lifetime
  (`ROUTING.md`).
- **Producers** submit `(symbol, command)` pairs. Any number of producer
  threads may submit concurrently.
- **Egress consumers** receive the events each partition emits. Examples are
  event journals, acks, metrics and user callbacks.

Threading, queues and memory layout are implementation choices, recorded in
each implementation's `docs/DESIGN.md`. Only the observable behavior below
is contract.

## 2. Ingress sequence (`iseq`)

- The pipeline imposes one **total order** on all submitted commands: the
  *ingress order*.
- Each command is assigned `iseq` (u64), its 1-based position in the ingress
  order **counting commands only**. Control operations (§6) take part in
  the ingress order but do not consume an `iseq`. Within one pipeline run,
  `iseq` is dense: 1, 2, 3, …
- **Single producer:** ingress order = submission order. When a harness
  feeds a file, the k-th command line gets `iseq = k`, so every partition's
  journals are deterministic and comparable across implementations.
- **Multiple producers:** ingress order is *some* interleaving that preserves
  each producer's own submission order. Which interleaving is not
  specified.
- After recovery, `iseq` resumes from `max(recovered iseq) + 1`
  (`JOURNAL.md` §5). Commands that were sequenced but never journaled before
  a crash leave a gap. Gaps across a recovery are permitted, but there are
  none within a run.

## 3. Ordering guarantees

1. **Per symbol.** The events for symbol `s` are exactly the events a matcher
   `Engine` emits when it applies `s`'s commands in ingress order. They are
   byte-identical canonical lines, and `seq` is per book, dense from 1. This
   is the pipeline's whole semantic contract: it adds no semantics.
2. **Per partition.** Within partition `p`, commands are applied, and their
   events delivered, in ingress order restricted to `p`'s symbols. Every
   egress consumer of `p` sees every event of `p`, in that order.
3. **Across partitions.** There is **no ordering** between events of
   different partitions. Consumers that need a unified view merge by
   per-book `seq` or by `iseq`. This is the ITCH/iLink per-channel model
   (see `docs/SCALING.md`).
4. **One partition.** With `P = 1`, the event stream is identical to a
   single matcher `Engine` applying all commands in ingress order.

## 4. Journal-before-apply

Each partition has a **command journal** (`JOURNAL.md` §2).

- A command's record is **written** to its partition's command journal before
  the partition's engine applies it. No event caused by command `n` is
  observable before record `n` has been handed to the journal writer.
- **Durability** is separate. A record is *durable* once an fsync covering it
  has completed. Each partition tracks two watermarks:
  - `journaled_iseq`: the highest `iseq` written. This gates the engine.
  - `durable_iseq`: the highest `iseq` fsynced. This gates acks (§5).
- The fsync policy (every N records, every T ms, or never) is configuration.
  It changes latency and loss on power failure, never the content of any
  journal, snapshot or event stream.
- When journaling is disabled (`off`, for tests and analysis only), the
  pipeline still emits identical events. It just cannot recover.

## 5. Publish and acknowledgement contract

- A successful **publish** means the command was *sequenced*: it holds an
  `iseq` and will be applied unless the process dies first. It does **not**
  mean durable.
- An **ack** is the durable-acceptance signal for a command. It is delivered
  only once `durable_iseq ≥ iseq` for the command's partition, and it
  carries the command's `iseq`, `symbol` and the events it caused. An
  implementation that offers acks must never release one early. Clients
  correlate acks by `(symbol, order_id)` or by `iseq`.
- **Crash semantics.** After a crash and recovery (`JOURNAL.md` §5), each
  partition's state reflects exactly the prefix of its command subsequence
  that reached its journal. A command lost in a crash was never acked.
  Events computed but not yet delivered may be lost. Recovery re-derives
  them byte-identically.
- **Backpressure.** Every internal queue is bounded. When full, the ingress
  edge either **blocks** the producer until space frees, or **refuses**
  the command (`try_publish` → error: nothing sequenced, nothing consumed,
  `iseq` not advanced). Commands past the ingress edge are never dropped.

## 6. Control operations

Control operations travel in the ingress order, so they cut every partition
at the same logical point.

| Op | Effect | Completes when |
|---|---|---|
| `barrier` (drain) | none | every command earlier in ingress order has been applied, and all its events have been delivered to every egress consumer of every partition |
| `snapshot` | captures books at the cut | the snapshot (`JOURNAL.md` §4) reflects **exactly** the commands with `iseq ≤ N`, where `N` = number of commands earlier in ingress order |
| `shutdown` | stops the pipeline | as `barrier`, then all pipeline threads have exited; later publishes fail with a *closed* error; repeated shutdown is a no-op |

A snapshot never waits for, or blocks behind, commands later in the ingress
order. Partitions reach the cut independently.

## 7. Egress consumers

- Consumers are attached per pipeline and instantiated **per partition**.
  Each instance sees one partition's events (§3.2).
- Each delivered event carries `(symbol, seq, event, iseq)`, where `iseq`
  identifies the causing command. Implementations may add timing fields.
- A slow consumer applies backpressure to its partition. It never causes
  events to be skipped.
- The **event journal** (`JOURNAL.md` §3) is an egress consumer. It need not
  wait for durability.

## 8. Configuration that is observable

| Setting | Observable effect |
|---|---|
| `partitions` (P) | which partition journal a command lands in; harness stdout grouping (`HARNESS.md`) |
| partition map (`ROUTING.md`) | same |
| book config (`pmin`, `pmax`, `max_orders`, `index`) | matcher semantics (`spec/matcher/SPEC.md` §2), snapshot header |
| journal format (`jsonl` \| `binary` \| `off`) | journal file contents (`JOURNAL.md`) |

Ring sizes, wait strategies, thread placement, batch sizes and fsync policy
are **not** observable. Changing them must never change any output byte.
