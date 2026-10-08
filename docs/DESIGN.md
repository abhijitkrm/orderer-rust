# DESIGN — orderer-rust

How the reference implementation realises the orderer contract
(`spec/PIPELINE.md` and friends). Everything here is implementation
choice. Only what the spec pins down is observable, and changing anything
below must never change an output byte.

## 1. Shape

```
Handle::publish ─▶ ingress ─▶ router ─┬─▶ inbox[p] ─▶ engine[p] ─▶ outbox[p] ─▶ egress
 (any threads)    (multi-     (1 thr) │    (SPSC)    (journal +    (SPSC)     (grouped
                   producer)          └─▶ …           apply)                    threads)
                                                        │
                                              chunks ──▶ I/O thread[p]: write, group-commit fsync
```

| Thread | Count | Wait | Work |
|---|---|---|---|
| router | 1 | BusySpin (`Waits::low_latency`) | stamps `iseq`, routes by symbol, broadcasts controls, commits each inbox once per batch |
| engine | P | BusySpin | encodes the command record (inline journaling), applies to its `MatchingCore`, stages events into its outbox |
| egress | 1 by default (`stage_threads`) | Backoff | runs the egress plugs of every partition, marks drain epochs |
| I/O | 1 per journal file | blocks in syscalls | `write` + group-commit `fsync`, advances `flushed`/`durable` |
| journal | 0 (inline) or `stage_threads` | Backoff | only with `JournalPlacement::Stage` |

Crates:

- `orderer-core`: matcher-rust's book, verbatim (`docs/VENDORED.md`), plus
  the `MatchingCore` seam.
- `orderer-disruptor`: the generic machinery.
- `orderer`: the assembly, journals, recovery and harnesses.

## 2. LMAX → Rust mapping

| LMAX Disruptor | orderer-disruptor | Notes |
|---|---|---|
| `Sequence` | `Sequence` | `#[repr(align(128))] AtomicI64`. 128 B is Apple Silicon's line, and two x86 lines (adjacent-line prefetch) |
| `RingBuffer` | `RingBuffer<T>` | preallocated `T: Default` slots, power-of-two mask |
| `SingleProducerSequencer` | `SingleProducer` | claims are a local counter; `stage`/`commit` publishes a whole batch with one Release store |
| `MultiProducerSequencer` | `MultiProducer` | `fetch_add` claims; per-slot availability flags (lap numbers) so a stalled producer gates only its own slot. `try_publish` claims by CAS and never leaks a claim |
| `SequenceBarrier` | `Consumer` (cursor + dependency sequences) | dependencies are declared on `RingBuilder`; the producer gates on terminal consumers |
| `WaitStrategy` | `WaitStrategy::{BusySpin, Yield, Backoff, Blocking}` | per consumer; `Blocking` is condvar-based with a 1 ms bound |
| `BatchEventProcessor` | `EventProcessor`, `Consumer::poll`/`wait_poll` | `end_of_batch` flag; watermark published once per batch |
| (multi-ring) | `MultiRingProcessor` | one thread drains several rings |
| DSL `handleEventsWith().then()` | `Disruptor::new().handle().then().spawn_*()` | used by tests; the pipeline wires rings by hand |

## 3. `unsafe`: where and why

The workspace has exactly two `unsafe` sites:

1. **`orderer-disruptor/src/ring.rs`**, slot access. Slots are
   `UnsafeCell<T>` guarded by the sequence protocol, not locks:
   - **Write** only with an unpublished claim. A claim on `s` is granted only
     once every gating consumer has passed `s − size`.
   - **Read** only after observing `s` published (an Acquire of the cursor
     or availability flag that the writer set with Release after writing),
     and only until your own sequence passes `s` (a Release after reading).
   - Readers get `&T` only.

   The crate is `#![deny(unsafe_code)]` with `#[allow]` on this module.
   Verification: `tests/loom.rs` model-checks it, and fails if any Release
   is weakened to Relaxed. `tests/ring.rs` stresses it, with 100/100
   clean runs in debug and release.
2. **`orderer/src/affinity.rs`**: one FFI call,
   `pthread_set_qos_class_self_np`, behind the off-by-default `affinity`
   feature. `orderer` is otherwise `deny(unsafe_code)`, and `orderer-core`
   is `forbid(unsafe_code)`.

Tests may use `unsafe`: `tests/no_alloc.rs` implements `GlobalAlloc`.

## 4. Journaling

**Inline placement (default).** The engine thread encodes command `n`'s
record into its partition's chunk immediately before applying `n`.
Journal-before-apply (`spec/PIPELINE.md` §4) then holds in-thread. It costs
about 5–10 ns per command and saves a thread and a pipeline hop, which is
decisive with 4 performance cores (W6 at P=2: eff 0.97 inline vs 0.86
staged). `JournalPlacement::Stage` keeps LMAX's diamond: a journal consumer
ahead of the engine on the inbox. Both are tested end to end.

**No syscalls on pipeline threads.** Records are encoded into recycled 256
KB chunks, 64 per file. Full chunks, or partial ones after 50 µs idle or at
a barrier, snapshot or shutdown, go to that file's I/O thread over a
bounded channel. The I/O thread writes, then group-commits: it writes
everything already queued, then makes one `fsync` decision per
`FsyncPolicy`. Then it advances:

- `flushed[p]`: handed to the OS
- `durable[p]`: covered by a completed fsync (`F_FULLFSYNC` on macOS, via
  `File::sync_data`)

A slow disk costs chunks, never a stall on the hot path. The engine blocks
only when all 64 chunks are in flight, which is backpressure.

Why: a single `write()` measured up to 25 ms stalls under page-cache
pressure or a concurrent `F_FULLFSYNC`. With synchronous writes, durable
throughput was 2–5M commands/s; with this design, 15–24M.

**Acks.** `Acks` (an egress plug) queues events and releases them only when
`durable[p] ≥ iseq`. At shutdown, egress waits for the final group commit.

**Event journals** are an egress plug using the same chunk writer, without
fsync. They are derived data: recovery re-derives them byte-identically.

## 5. Controls and shutdown

Controls (`Barrier`, `Snapshot`, `Shutdown`) are published on the ingress
ring like commands. The router broadcasts them to every inbox, so they cut
every partition at the same ingress position:

- **drain**: completes when every partition's egress has passed the barrier
  epoch.
- **snapshot**: each engine deposits its book blocks under the op id. The
  caller merges by symbol into one `matcher-snap/1` body, with the cut
  (`iseq` of the last command before the control) in the `.meta` sidecar.
- **shutdown**: closes the pipeline, then waits out in-flight publishes
  using per-handle flags (SeqCst on both sides, the Dekker pattern), so a
  publish that returned `Ok` is always applied. It then publishes
  `Shutdown`, joins every thread, and finishes the journals.

A panicking thread (core, plug, invariant check) marks the pipeline failed
and alerts every ring. `drain`, `snapshot` and `shutdown` then return
`Error::Failed` instead of hanging.

## 6. Allocation discipline (A5)

In steady state, nothing allocates on any thread: producers, router,
engines, egress or I/O. Ring slots are `Copy` and preallocated. Books are
preallocated by matcher's design (pool, ladder, order map). Journal chunks
are recycled.

std's channels lazily allocate waiter state on a thread's first *blocking*
receive, so journal-owning threads call `writer::prewarm_thread()` at
startup. `tests/no_alloc.rs` counts every allocation across 1M commands
through a P=2 journaled pipeline: zero.

## 7. Tuning guide (this machine: M1, 4P+4E)

- **Partitions.** P = 2 gives the best efficiency. P = 3 gives the best
  absolute throughput. P = 4 oversubscribes the performance cores (router,
  producer and 4 engines all spin).
- **Waits.** Use `Waits::low_latency()` (router and engines spin) for
  throughput, and `Waits::relaxed()` (everything backs off) for many small
  pipelines (tests) or idle-mostly deployments.
- **Rings.** 16K/4K/8K (default) gave about 4× lower queueing latency than
  64K/16K/32K, at equal or better throughput. Below 4K/1K/2K, throughput
  drops.
- **Durability.** `FsyncPolicy::every_n(1024)` is the gated configuration.
  Group commit makes fsync frequency nearly irrelevant to throughput; the
  device's flush bandwidth is the ceiling (`docs/RESULTS.md` in the spec
  repo).
- **Event journals.** They cost about 2.5× the command journal's bytes.
  Enable them only if consumers need them on disk.
- **QoS (`affinity` feature).** Hot-only hints measured within noise.
  Demoting background threads hurt, so it is not done.

## 8. Porting checklist (orderer-cpp, -java, -go, -ts)

A port's threading is free, but its bytes are not. In order:

1. Vendor `spec/` + `vectors/` at a tagged `orderer-spec/N`. Port
   `matcher-<lang>` as the core. Run `vectors/regress/001_dense_map_churn`
   early. It catches the open-addressing order-map deletion bug fixed in
   matcher-rust 71a35f4 and matcher-cpp 1285f6c, so vendor a matcher at or
   after those commits.
2. **Routing**: `vectors/routing/hash.jsonl` must pass before anything else.
   Use unsigned 64-bit wrapping multiply and multiply-shift reduction; no
   `%`, no floats.
3. **Ring protocol**: one producer writes, then Release-publishes; consumers
   Acquire, read, and Release their watermark. Multi-producer publish needs
   availability flags (or an equivalent that doesn't convoy). Model-check
   or stress it as `tests/ring.rs` does.
4. **iseq**: stamped by the single router in ingress order, counting
   commands only. Controls carry the cut.
5. **Journals**: encode exactly `spec/JOURNAL.md` §2–3 (JSONL with `iseq`
   last, the 64-byte binary header with the book config, little-endian
   records). Keep syscalls off the engine path. Track `flushed` and
   `durable` per partition, and release acks only on `durable`.
6. **Harnesses**: `spec/HARNESS.md`. The listing is grouped by partition.
   `orderrun` is always tagged, `ordererfuzz` is tagged only for engine
   files. Exit code 2 for usage, input and corruption errors. Provide
   `scripts/build-harness.sh` (with `CHECKED=1`) and `scripts/test.sh`.
7. **Prove it**: run `vectors/manifest.json`, then the spec repo's
   `verify`, `diffuzz`, `exhaustive`, `e2e` and `snapdiff` scripts with the
   port checked out as a sibling. Then `bench.sh`, gated on the port's own
   untimed core baseline.
