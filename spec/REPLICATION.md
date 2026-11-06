# REPLICATION — replicated ingress log and failover (v0.2, implemented in orderer-rust)

**Status: implemented in `orderer-rust` only; not yet part of a tagged spec
and not yet ported.** §1-§12 are the design as proposed. The decisions in §11
are now resolved, and **§13 records what was built, the wire formats, what
was measured, and what is not covered.** Where §13 and an earlier section
differ, §13 is what the code does.

Extends `PIPELINE.md` and `JOURNAL.md`. Matching semantics stay matcher's
and are not restated.

---

## 1. Goal and non-goals

**Goal.** Survive the loss of a minority of nodes with no acknowledged
command lost, no command applied twice, and no divergence between nodes.
Failover completes without operator action.

**Why this fits.** The pipeline is already a deterministic state machine:
the same command sequence yields byte-identical events and snapshots in all
five ports. Replication therefore reduces to agreeing on **one totally
ordered command log**. Every node applies the same log and must reach the
same state.

**Non-goals (v1).**
- Dynamic membership. The node set is static configuration.
- Byzantine faults. Nodes crash, restart, lose messages and lose the tail
  of a file. They do not lie.
- Cross-datacenter tuning, leader leases for reads, and client sessions
  (step 2: wire protocol).
- More than one replicated group per pipeline (see §11, D1).

## 2. Model

```
producers ─▶ LEADER sequencer ─▶ replicated ingress log ─▶ commit
                                    │  append (term, iseq, cmd)
                         FOLLOWERS ◀┘  persist, then ack
commit ─▶ router ─▶ partitions ─▶ engines ─▶ egress     (on every node)
```

- A cluster has `N = 2f + 1` nodes (`N = 3` typical, tolerates `f = 1`).
  `N = 1` is valid and is the degenerate case: it behaves exactly like
  today's pipeline.
- Exactly one node is **leader** in a given **term**. Only the leader
  accepts producers and assigns `iseq`.
- The **ingress log** is the replicated object: a sequence of control and
  command entries in ingress order. This is the existing ingress order of
  `PIPELINE.md` §2, made durable and shared.
- Each node runs the unchanged pipeline *after* the log: router, partition
  engines, egress. Partition command journals stop being the source of
  truth in replicated mode (§6).

## 3. Log entries and terms

An entry is `(term, index, payload)`.

- `index` is the log position, 1-based and dense. For command entries
  `index` equals `iseq`. Control operations (`PIPELINE.md` §6) are entries
  too, so with controls present `index ≥ iseq`. **[DECIDE D5]** whether
  controls get their own `iseq`-free index or the log uses `index` only.
- `term` (u32) is the election epoch in which the leader created the entry.
- **Log matching.** If two logs hold an entry with the same `(index, term)`,
  the two logs are identical up to and including that index.
- **Binary record.** The 48-byte command record of `JOURNAL.md` §2.2
  version 2 has 4 reserved bytes at offset 44. Version 3 stores `term` there
  and extends the CRC-32C to bytes 0–39 plus 44–47. Control entries use
  `cmd = 0x10..0x1F` and the same record size.

## 4. Commit and apply

- An entry is **committed** once it is persisted (fsync completed) on a
  majority of nodes **in the leader's current term or earlier, with the
  Raft safety rule**: a leader commits entries from earlier terms only
  indirectly, by committing an entry of its own term after them.
- `commit_index` is monotone on every node and never moves backward.
- **Apply is gated by commit, not by local write.** This replaces the gate
  in `PIPELINE.md` §4 (`journaled_iseq` gates the engine) when `N > 1`:
  the engine applies entry `i` only when `commit_index ≥ i`. With `N = 1`,
  commit coincides with the local write, so §4 holds unchanged.
- Consequence: **no state is ever rolled back.** A node only applies
  committed entries, and committed entries are never lost, so there is no
  undo path to build or test. Uncommitted log tails may be truncated and
  rewritten; applied state may not.
- **Ack contract.** `durable_iseq` of `PIPELINE.md` §5 is redefined for
  `N > 1` as the highest `iseq` with `commit_index ≥ index`. An ack is
  therefore released only after a **quorum** has the command durable.
- **Cost.** Latency to ack is one network round trip plus a remote fsync,
  added to today's. This is inherent; the design batches (§9) to keep
  throughput.

## 5. Elections and fencing

- Raft-style: randomized election timeouts, `RequestVote` with the
  up-to-date check `(last_term, last_index)` lexicographic, one vote per
  term, persisted before replying.
- `term` and `voted_for` are persisted in a small metadata file with the
  same CRC and torn-write discipline as journals.
- **Fencing.** Every message carries the sender's term. A node rejects any
  message from a lower term. A deposed leader that learns of a higher term
  stops publishing immediately: later `publish` fails with a *not-leader*
  error carrying a leader hint. Nothing in flight is acknowledged after
  that, because acks require commit.
- A new leader appends a no-op entry in its term before accepting
  producers, which both commits earlier entries and establishes its
  authority.
- **Pre-vote** is required, so a partitioned node cannot inflate terms and
  depose a healthy leader on rejoin. **[DECIDE D4]**: whether pre-vote is
  v1 or v1.1.

## 6. Recovery, snapshots and catch-up

- On restart a node reads its metadata and ingress log, truncating any torn
  tail (`JOURNAL.md` §5.2), and rejoins as follower. It does not apply
  anything until it learns `commit_index`.
- **Checkpoints** (`JOURNAL.md` §6) keep their meaning: a durable snapshot
  at cut `N` allows deleting earlier segments. In replicated mode a
  checkpoint may only be taken at a cut ≤ `commit_index`.
- **Catch-up.** A follower whose needed prefix was already deleted receives
  the leader's latest snapshot, then the log suffix. Snapshot transfer is
  chunked, CRC-checked, and restartable.
- **State divergence detection.** At every checkpoint each node computes a
  SHA-256 over the canonical snapshot bytes (`matcher` `JOURNAL.md` §4,
  already byte-identical across ports) and sends `(cut, digest)` to the
  leader. A mismatch is a bug, not a recoverable fault. The mismatching node
  halts (fail-stop), the cluster keeps running if a quorum still agrees, and
  the event is surfaced loudly.

## 7. Egress and consumers

- Every node derives events deterministically. Only the **leader's** egress
  is live toward external consumers; followers run egress in
  discard-or-local mode.
- After a failover the new leader re-emits from its own applied state.
  Consumers de-duplicate on `(symbol, seq)`, which is dense per book and
  already part of the event contract (`PIPELINE.md` §7). This gives
  effectively-once delivery with no extra protocol.

## 8. Failure semantics (what a client can rely on)

| Situation | Guarantee |
|---|---|
| Leader crashes after the client got an ack | Command is committed on a quorum; the new leader applies it. Never lost. |
| Leader crashes before ack | Command may or may not be committed. The client does not know. Resolving this needs client-ID dedupe (step 2); the log carries what that needs. |
| Minority partition | Minority side cannot commit and refuses publishes; majority side continues. |
| Leader loses quorum | It stops acking and steps down after a timeout. |
| Follower loses disk tail | It truncates, catches up from leader. Cannot affect committed state. |
| Two nodes disagree on state | Impossible if deterministic; detected by §6 digests; offender halts. |

## 9. Performance expectations (to be measured, not assumed)

- A 48-byte entry at 30M entries/s is ~1.4 GB/s per follower link. A single
  replicated log therefore **bounds aggregate throughput by network and
  follower fsync bandwidth**, not by the cores. This is the design's main
  trade-off (D1).
- Mitigation: batch entries per message (target ≥ 4 KiB), pipeline
  `AppendEntries` without waiting for each ack, group-commit fsync on
  followers, and compress nothing (cost exceeds benefit at this size).
- Replication throughput targets must be measured on Linux with NVMe and a
  real NIC. The M1 numbers in `docs/RESULTS.md` are not valid for this
  (F_FULLFSYNC ~12 ms).

## 10. Verification plan (this is where the work is)

Writing consensus is the risky part, so correctness evidence comes first.

1. **Pure protocol core.** The protocol is a function: `(state, input) →
   (state', effects)`. Inputs: message received, timer fired, disk write
   completed, client proposal. Effects: send message, start disk write,
   set timer, apply entries. It performs no I/O and reads no clock.
2. **Deterministic simulation.** A single-threaded harness drives N cores
   with a seeded PRNG controlling: message drop / duplicate / reorder /
   delay, partitions, node crash and restart, torn and lost disk tails,
   slow or failing fsync, clock skew. It checks, after every step:
   - election safety: at most one leader per term;
   - log matching and leader completeness;
   - **committed prefix identical** on all nodes, never shrinking;
   - no entry applied twice, none applied before commit;
   - every acked command is in every later leader's log;
   - applied state digests equal at equal `commit_index`.
   Any failure prints the seed and replays exactly.
3. **Cross-port conformance (implemented: `vectors/replication/`).** The seeded input traces and expected effect
   traces become vectors in `vectors/replication/`. Every port's protocol
   core must produce byte-identical effects for the same input trace, the
   same technique already used for matching. This is how five ports stay
   consistent without five independent consensus bugs.
4. **Real-process failover test.** Extend `scripts/crash.sh`: kill -9 the
   leader under load, repeatedly, and check no acknowledged command is lost
   and all survivors' snapshots are identical.

## 11. Open decisions

- **D1 one log vs one log per partition.** *Recommendation: one log.* It
  preserves the total ingress order that barriers and snapshot cuts rely on
  (`PIPELINE.md` §6), and keeps recovery simple. Cost: replication bandwidth
  caps aggregate throughput (§9). Per-partition groups scale better but lose
  global cuts; revisit only if measurements show the cap matters.
- **D2 apply after commit** (recommended, §4) vs leader applies after local
  write and followers after commit. The latter lowers leader latency but
  needs a rollback path on leader change; rejected for v1.
- **D3 build vs reuse.** Implement the protocol core ourselves (needed for
  five ports and byte-level conformance vectors) vs wrap an existing
  library per language. *Recommendation: build, with the §10 tests as the
  justification for trusting it.*
- **D4 pre-vote in v1** (recommended) or later.
- **D5 control entries and `iseq`.** Separate index space vs shared (§3).
- **D6 static membership** for v1 (recommended).

## 12. Phasing

| Phase | Deliverable |
|---|---|
| R-A | This spec accepted; message wire format and vector format fixed |
| R-B | `orderer-rust`: pure protocol core + deterministic simulator, all §10.2 invariants green over a large seed sweep |
| R-C | Integrate: ingress log, commit-gated apply, `N = 1` regression-identical to today |
| R-D | TCP transport, snapshot transfer, real 3-node run |
| R-E | Failover under load (kill -9), digest checks, BENCH numbers on Linux |
| R-F | Ports: C++, Java, Go, TS against `vectors/replication/` |


---

## 13. As built (orderer-rust)

### 13.1 Decisions resolved

| | Decision |
|---|---|
| D1 | **One log per group**, and **several independent groups** for scale-out (§13.5). A group is a Raft group owning a set of symbols. Within a group the total ingress order holds; between groups there is none. |
| D2 | Apply after commit, as proposed. No state is ever rolled back. |
| D3 | Built, not wrapped. `orderer-consensus` is the protocol as a pure state machine; `orderer-cluster` is the runtime. |
| D4 | Pre-vote is in v1. |
| D5 | Controls are not yet log entries. A replicated cluster does not offer cluster-wide barrier/snapshot/shutdown controls; snapshots are per replica and driven by log index (§13.3). |
| D6 | Static membership. |

### 13.2 Protocol details beyond §1-§12

* **Acknowledgement is at commit, not apply.** A proposal is acknowledged
  once its entry is durable on a quorum. The state machine applies on its own
  thread (the *applier*); the consensus thread never calls it. A slow state
  machine therefore cannot starve ticks and heartbeats (a regression test
  makes one apply outlast the election timeout; it fails on the earlier
  inline-apply design). Producers are pushed back with `Busy` once the
  applier is `max_apply_backlog` entries behind.
* **Outcomes are explicit.** `Ok(index)` = committed. `NotLeader(hint)` and
  `Busy` = nothing was appended, retry. `Timeout` / `Unknown` = the entry may
  still commit; the client must not retry blindly (this is what the idempotent
  client IDs of the gateway step are for). `Superseded` = the index was
  committed with another leader's entry: lost.
* **Replication is pipelined.** The leader advances a follower's `next_index`
  optimistically after sending, so a burst of proposals is not resent; a lost
  message surfaces as a failed reply carrying a hint.
* **Divergence check.** At every `digest_every` log indexes each replica's
  applier computes a SHA-256 of its state (`StateMachine::digest`) and sends
  it to the others. A replica whose digest differs from a strict majority's
  halts itself (fail-stop). If no digest has a majority it halts too.
* **Log compaction and catch-up.** The applier snapshots at an applied index,
  stores the snapshot durably, and only then does the log drop the prefix it
  covers. A follower whose needed prefix is gone is sent the snapshot over the
  transport (chunked, length- and CRC-checked) and installs it
  (`Node::install_snapshot`).

### 13.3 On-disk and wire formats

All integers little-endian. CRC is CRC-32C (`JOURNAL.md` §2.2).

* **WAL** (`raft.wal`): 8-byte magic `ORDWAL\0\1`, then frames
  `len u32 | crc u32 | body`. One frame is one atomic consensus write: `term
  u32 | voted_for u8 (255 = none) | compact flag u8 [index u64, term u32] |
  from u64 | n u32 | n × (term u32, len u32, data)`. Replaying frames with the
  protocol core's own `Disk::apply` reproduces the state it handed to the
  disk. Recovery cuts a **torn tail** (incomplete or failing frame with
  nothing valid after it, including a **zero-filled tail**, which a SIGKILL
  during an append can leave) and **refuses to start** on damage followed by
  valid frames. A frame body shorter than 18 bytes is never valid, which is
  what makes zero fill detectable.
* **Snapshot** (`snap-<index>.snap`): `ORDS | index u64 | term u32 | len u64 |
  crc u32 | data`, written to a temp name, fsynced, renamed, directory fsynced.
* **Log entry payload**: empty (a leader's no-op) or `0x01 | n u32 | n × 32`
  bytes, each command being a journal record without `iseq`, checksum or
  padding (`symbol, cmd, side, otype, tif, order_id, price, qty`). One entry
  carries a whole batch, so consensus cost is per batch, not per command.
* **Transport frame**: `len u32 | crc u32 | kind u8 | body`. Kinds: message,
  snapshot begin / chunk / end, digest, hello. A frame over 64 MiB, with a bad
  checksum, a bad body, or from a node outside the cluster id closes the
  connection.
* **Client protocol**: the same envelope; requests `PROPOSE`, `STATUS`,
  `HASH`.

### 13.4 Verification that exists

* **Protocol core, simulated** (`orderer-consensus`): 3, 5 and 7 nodes, with
  and without pre-vote, message loss / duplication / reordering / delay,
  partitions, crashes with torn disk tails, slow fsync, compaction and
  snapshot catch-up. Checks election safety, log matching, leader
  completeness, one agreed committed sequence, no entry committed twice or
  never proposed, state-machine equality after healing. Tens of thousands of
  seeds clean (1,000 seeds: about 22,000 compactions and 3,000 snapshot
  installs). Five deliberate protocol bugs (commit on one ack, skip the vote
  up-to-date check, double vote, commit an old-term entry by counting) are
  caught, the last by a directed figure-8 test.
* **Real replicas over TCP and real files** (`orderer-cluster/tests`): 16
  cluster tests (election, ordering, redirect, leader crash with no
  acknowledged loss, repeated leader kills under concurrent load, isolated
  leader cannot commit, snapshot catch-up, whole-cluster restart, torn and
  corrupt WAL, digest divergence halts the odd node out, hostile connections,
  five nodes with two failures, no quorum no ack, slow state machine does not
  cost leadership), 6 client-protocol tests (connection reuse, frames split
  across the read timeout, malformed frames, leader following), 2 tests that
  run the real matching pipeline as the state machine and require its events
  and state to equal an unreplicated pipeline's through follower snapshot
  restore and leader failover, 2 sharded-group tests, and unit tests for the
  WAL (every possible torn prefix, zero tails, mid-file damage, rewrite), codec
  (truncations and random garbage never panic), snapshot store and SHA-256.
* **Real processes** (`orderer-rust/scripts/failover.sh`): three `orderer-node`
  processes with real fsync, a load generator, and `kill -9` of the current
  leader five or six times under load. Passes only if every acknowledged batch
  is present on every replica, in the same order, exactly once, and all
  replicas end at the same state digest. On the final code, six consecutive
  runs passed: 36 leader kills and 15,920 acknowledged batches (796,000
  commands). Early runs of
  this script found a real recovery bug (a zero-filled WAL tail misread as
  mid-file corruption; two of the first four runs failed on it).

### 13.5 Scale-out and what a single group can carry

One group is bounded by its leader: it writes every entry to its own WAL and
sends it to every follower. With 32-byte commands that is roughly `rate × 32 B`
of WAL and `rate × 32 B × (N-1)` of network egress at the leader. On a 10 GbE
NIC and N = 3 the leader's network alone caps one group near 19 M
commands/s (analytic; not measured, this machine has no NIC to saturate).
Independent groups (`orderer_cluster::shard`) multiply that: symbols are
assigned to groups by a hash chosen not to correlate with the pipeline's own
symbol-to-partition hash, `ShardedClient` splits a batch per group, and the
groups share nothing, so an outage in one does not stop another (tested).
The price is the global order: there is none between groups, only between the
commands of one symbol.

### 13.6 Measured (Apple M1, 8 cores, everything on one machine)

All replicas, producers and the OS share one 8-core laptop, one SSD and one
loopback interface, and a web browser was running. These are floors for what
dedicated hardware per replica would do, not ceilings, and the cross-group
scaling below is **not** demonstrated by them: the machine is the limit.

| Configuration | Result |
|---|---|
| 1 node, no-op state machine, no fsync | 46.9 M commands/s (1.5 GB/s of log) |
| 3 nodes, no-op, no fsync, 1000-command batches | 16.2 M commands/s (519 MB/s) |
| 3 nodes, no-op, **fsync on a quorum**, batch 100 / 1000 / 8000 | 1.3 / 8.3 / 12.3 M commands/s |
| 1 node, real matching pipeline (P = 2) | 31.6 M acked, 25.6 M applied |
| 3 nodes, real matching pipeline (P = 2) | 7.8 M acked, 6.6 M applied on every node |
| 1 / 2 / 3 groups, 3 nodes each, no-op | 16.2 / 16.5 / 18.3 M aggregate (box-limited) |
| ack latency, batch of 1, fsync on | about 10 ms (macOS full fsync) |

Two changes made most of the difference. CRC-32C was a byte-at-a-time loop at
388 MB/s and every byte is checksummed about seven times on its way through
WAL, sender, reader and follower WAL; a portable slicing-by-8 version reaches
1.7 GB/s and the optional `hwcrc` feature (runtime-detected CPU instruction)
5.3 GB/s. And applying inline starved heartbeats (§13.2).

**Against matcher.** matcher-rust applies about 100 M operations/s on one
core. The replicated path today carries 8-16 M commands/s per group (durable
and not) and the applier, a single thread decoding and publishing into the
pipeline, about 25 M. So one group is roughly a quarter to a sixth of one
matcher core, not equal to it. Reaching matcher's rate takes about five to
eight groups on dedicated hardware (or fewer with a wider applier and
smaller commands); the aggregate has been measured only on one machine, where
it does not scale.

### 13.7 Client safety, reads and transport authentication

* **Exactly-once submission (sessions).** A client with a session id sends
  each batch tagged `(client, seq)` (payload tag 2). Every replica wraps its
  state machine in `DedupSm`, which applies a batch only if `seq` is newer
  than the last applied for that client; the table is part of every snapshot
  and state digest, so replicas skip the same entries. `Client::propose_session`
  retries the *same* `(client, seq)` through lost connections and leader
  changes until it is definitely committed, so `Unknown` no longer reaches the
  caller. A session is one sequential stream: never send `seq + 1` before
  `seq` has an answer. In the process-level failover test this took unknown
  outcomes from 15 per run to 0, with every batch still applied exactly once.
* **Linearizable reads (log barrier).** `ReplicaHandle::barrier()` commits an
  empty entry and waits for the state machine to apply it. A leader that has
  been deposed cannot commit, so it cannot answer; a read after the barrier
  sees every write acknowledged before the call. It costs one log entry per
  read; a read-index optimisation (a heartbeat round instead of an entry) is
  future work.
* **Authenticated transport (not encrypted).** With a pre-shared cluster key
  (`ReplicaConfig::auth_key`, `ORDERER_KEY` for the binaries) replicas and
  clients run a challenge-response handshake (HMAC-SHA256) and every later
  frame carries a 128-bit MAC over direction, sequence number, kind and body.
  A peer without the key cannot join or be impersonated, and altered,
  replayed, dropped, reordered or reflected frames are rejected. Payloads are
  **not encrypted**; use a tunnel for secrecy. TLS itself would need a
  third-party dependency, which this workspace does not take.

### 13.8 Not covered

* Dynamic membership; confidentiality on the wire; a read-index fast path;
  key rotation (the key is static per process).
* Cluster-wide barrier / snapshot / shutdown controls, and any consistent
  snapshot across groups.
* A parallel applier. The applier is one thread; a snapshot also runs on it,
  so a large book pauses applying (not consensus) while it serialises.
* Throughput on real multi-machine hardware, with a real NIC and NVMe, and
  Linux fsync behaviour. Everything above is one laptop.
* The replication **runtime** (WAL, snapshots, transport, replica, client,
  sessions, authentication, scale-out client) exists in all five languages:
  orderer-rust, orderer-cpp (§13.10), orderer-go (§13.12), orderer-java
  (§13.13) and orderer-ts (§13.14).
* `unsafe` was added in one place, behind the `hwcrc` feature (runtime CPU
  detection, tested equal to the portable code): the third sanctioned site
  after `affinity` and `prefetch`.

### 13.9 Ports

The protocol core (`Node`: elections, replication, compaction, snapshot
install) is ported to **C++** (`orderer-cpp/include/orderer/consensus.hpp`),
**Go** (`orderer-go/consensus`), **Java** (`orderer-java` `Consensus`) and
**TypeScript** (`orderer-ts/src/consensus.ts`). Each replays all 28 traces in
`vectors/replication/` (28,000 recorded drain points: elections won,
pre-votes, replication, truncation, applies, compactions, snapshot requests,
installs, restores and crash-restart stanzas) and must reproduce every
recorded effect and state line byte for byte; each port's test also fails when
a vector line is altered. The ports have no simulator of their own: the
vectors carry what the Rust simulator exercised, and no more. A behaviour the
simulator never reached is not covered in any port.

Runtime ports are checked against `vectors/replication-wire/` (see its README):
protocol messages, WAL frame bodies, command batches, WAL files in every
damaged state with their expected recovery, and snapshots. A port must decode
and re-encode them identically, recover the same disk image from every WAL
(torn and zero-filled tails cut, mid-file damage refused), and write
byte-identical `good` WAL and snapshot files. orderer-cpp (`include/orderer/cluster/`),
orderer-go (`cluster/`), orderer-java (`...orderer.cluster`) and orderer-ts
(`src/cluster/`) implement the storage and wire layer against them.

### 13.10 The C++ runtime and mixed-language clusters

`orderer-cpp/include/orderer/cluster/` implements the whole runtime: CRC-32C,
SHA-256 / HMAC and the session MAC, the codec, WAL and snapshot store,
transport, replica (driver, WAL and applier threads), the client protocol with
sessions and `barrier()`, and the state machines `RecorderSm` and `DedupSm`.
It is checked three ways:

* **Fixtures** (`vectors/replication-wire/`): decodes and re-encodes every
  message, WAL frame body and command batch identically to Rust; recovers the
  same disk image from all 17 WAL states; and the WAL and snapshot files it
  writes are byte-identical to Rust's.
* **16 end-to-end tests** over real TCP and files (the list in §13.4, less the
  pipeline-specific ones), clean under ThreadSanitizer and AddressSanitizer.
* **Mixed clusters** (`orderer-cpp/scripts/interop.sh`): replicas of both
  languages in one cluster, driven by the Rust load tool, with `kill -9` of the
  leader several times. Passed for rust-rust-cpp, cpp-cpp-rust, cpp-cpp-cpp,
  a five-node cpp-rust-cpp-rust-cpp, and rust-cpp-cpp with the authenticated
  handshake on: every acknowledged batch survived on every replica in the same
  order exactly once, and replicas of both languages ended with the same state
  digest. That last point exercises, across languages, the log, snapshots
  digests, sessions and HMAC handshake.

The first mixed-cluster runs used the language-neutral `RecorderSm`. The C++
runtime now also runs **the matching pipeline as the replicated state machine**
(`cluster/pipeline_sm.hpp`, `orderer_node --sm pipeline`, the default), and the
same mixed clusters pass with it: replicas of both languages end with the same
state digest, which is a SHA-256 over `iseq | partitions | matcher-snap/1`, so
the Rust and C++ matching engines reach a byte-identical order book. With
`COMPACT=100 DOWN=5` the script forces lagging replicas to catch up by snapshot
and counts the installs by language pair: snapshots built from live pipeline
state crossed the boundary in both directions (in four configurations, for
example 4 of 4 installs across languages for rust-cpp-cpp and for cpp-rust-rust)
and every replica still ended in the same digest.

### 13.11 C++ throughput

`orderer-cpp`'s `clusterbench` takes the same flags and prints the same line as
orderer-rust's. Best of 5, interleaved, on the same busy 8-core M1 as §13.6
(all replicas and producers in one process), M commands/s acked / applied on
every node:

| Configuration | Rust | C++ |
|---|---|---|
| 3 nodes, no-op state machine, no fsync | 16.7 / 16.5 | 19.2 / 18.4 |
| 3 nodes, matching pipeline (P = 2), no fsync | 7.8 / 6.0 | 7.7 / 5.4 |
| 1 node, matching pipeline (P = 2), no fsync | 28.3 / 21.9 | 24.5 / 15.8 |
| 3 nodes, no-op, fsync on a quorum | 8.4-8.6 | 8.6-8.9 |

The replication runtime is not slower in C++. The single-node pipeline gap is
the C++ matcher and pipeline themselves: a profile puts about 43% of busy time
in the order-book code and only a few percent in decoding and publishing. Two
findings made the C++ runtime competitive. Building a sealed 48-byte journal
record per command to take 32 of its bytes cost 30% of the producers' time; a
direct 32-byte writer replaced it, with identical bytes (the fixtures prove
it). And log entries are immutable shared buffers (`consensus::Bytes`), so the
seven copies a 32 KB payload takes through a replica are reference-count
bumps. Measure on separate machines before relying on any of these numbers.

### 13.12 The Go runtime and three-language clusters

`orderer-go/cluster/` is the third runtime: codec, WAL, snapshot store,
authentication, transport, replica (driver, WAL and applier goroutines), client
protocol with sessions and `Barrier`, `RecorderSm`, `DedupSm` and the matching
pipeline as the replicated state machine (`PipelineSm`). CRC-32C, SHA-256 and
HMAC come from the Go standard library; the rest follows the C++ runtime
function for function, and `cmd/orderernode` takes the same command line as
`orderer-node` and `orderer_node`. It is checked the same three ways:

* **Fixtures** (`vectors/replication-wire/`): every message, WAL frame body and
  command batch decodes and re-encodes identically to Rust; all 17 WAL states
  recover the same disk image; the WAL and snapshot files written in Go are
  byte-identical to Rust's. This passed on the first run.
* **19 end-to-end tests** over real TCP and files: the 16 of §13.10, plus three
  that run the matching pipeline (identical state digests on every replica,
  catch-up by a snapshot of real book state, whole-cluster restart from disk).
  Clean under the race detector, three runs in a row.
* **Mixed clusters** (`orderer-go/scripts/interop.sh`, any mix of Go, Rust and
  C++ replicas), driven by the Rust load tool with `kill -9` of the leader
  several times. Passed: go-go-go, go-rust-go, rust-go-cpp, go-rust-cpp with
  the authenticated handshake, cpp-go-rust with the recorder state machine, and
  go-go-go with compaction. Every acknowledged batch survived on every replica
  in the same order, exactly once, and replicas of all languages ended with the
  same state digest. With `COMPACT=100 DOWN=5` on go-rust-cpp, three snapshot
  installs crossed language boundaries and the digests still agreed.

Go also has the scale-out client (`ShardMap`, `ShardedClient`; the group
assignment is checked against values computed independently from the splitmix64
definition, and two groups are tested end to end, including one group's outage
leaving the other serving) and `orderload`, the port of `orderer-load` with the
same modes and output. `interop.sh` drives clusters with `orderload` by default
(`LOADER=rust` for Rust's): the mixed clusters above pass with it, including
rust-go-cpp and go-rust-cpp with authentication and forced snapshot transfer (6
installs, all across languages), and `orderload verify` fails, with exit status
1, on a lost, duplicated, invented, reordered and divergent applied log.

Throughput, `orderer-go`'s `clusterbench` (same flags and output line as the
others) against orderer-rust's, interleaved on the §13.6 machine, which was
busy (load average about 10 on 8 cores) while these ran. Best of 5 unless
noted, M commands/s acked:

| Configuration | Rust | Go |
|---|---|---|
| 3 nodes, no-op state machine, no fsync | 17.1 | 17.2 |
| 1 node, no-op, no fsync | 50.3 | 46.9 |
| 3 groups of 3 nodes, no-op, no fsync | 22.9 | 22.6 |
| 3 nodes, no-op, fsync on a quorum, 4 producers, window 64 (best of 3) | 8.3 | 8.4 |
| 3 nodes, no-op, fsync on a quorum, 4 producers, window 256 (best of 3) | 10.5 | 9.9 |
| 3 nodes, no-op, fsync on a quorum, 2 producers, window 8 | 0.91 | 0.93 |
| 3 nodes, matching pipeline (P = 2), no fsync | 11.0 | 4.2 |
| 1 node, matching pipeline (P = 2), no fsync | 34.0 | 21.8 |
| 3 nodes, matching pipeline, fsync, 4 producers, window 256 (best of 3) | 9.4 | 4.9 |

The replication path costs the same in Go as in Rust wherever the state machine
is cheap. The last two rows are the Go matcher and pipeline, as the profile
shows: the busy time is in the order-book code (pool allocation and the price
ladder) and in goroutine hand-offs, and decoding a batch is under 4%. Two
settings mattered for a state machine whose three replicas share cores: running
the counting egress inline on the engine goroutines (3.0 to 4.0 M/s), and
keeping the default back-off waits, because busy-spinning three pipelines'
router and engine goroutines on 8 cores collapses to 0.6 M/s. The window-8
row is not a protocol limit: 16 batches in flight over an 18 ms durable ack is
0.9 M/s in every language; a larger window lets group commit coalesce the
fsyncs, which is what the window-64 and window-256 rows measure. Measure on separate machines
before relying on any of these numbers.

### 13.13 The Java runtime

`orderer-java`'s `io.github.abhijitkrm.orderer.cluster` package is the fourth
runtime, structured like the C++ and Go ones: `Wal`, `SnapStore`, `Transport`,
`Replica` (driver, WAL and applier threads), `Client` / `ClientServer`,
`RecorderSm`, `DedupSm`, `PipelineSm`, `ShardMap` / `ShardedClient`, and the
tools `ordernode`, `orderload` and `clusterbench`. CRC-32C, SHA-256 and HMAC
come from the JDK (the CRC is a hardware intrinsic). Checked the same three ways:

* **Fixtures:** all of `vectors/replication-wire/` decodes and re-encodes
  identically, all 17 WAL states recover the same image, and the WAL and
  snapshot files written in Java are byte-identical to Rust's.
* **26 end-to-end and scale-out tests**: 17 end-to-end tests (the 16
  scenarios of §13.12, with the five-node and no-quorum cases as separate
  tests), 3 that run the matching
  pipeline (identical digests, snapshot catch-up from real book state, whole
  restart), and 6 scale-out tests with the same golden group values as Go.
  The end-to-end and pipeline suites passed three runs in a row.
* **Mixed clusters** (`orderer-java/scripts/interop.sh`, any mix of Java, Rust,
  C++ and Go; the load tool is Java's `orderload` unless `LOADER=rust|go`),
  with `kill -9` of the leader: java-java-java, java-rust-java, rust-java-cpp,
  go-java-rust, java-go-cpp with the authenticated handshake, rust-java-cpp with
  the recorder state machine, and java-java-java with compaction all passed;
  Go's and Rust's load tools also drive Java nodes. Every acknowledged batch
  survived on every replica in the same order exactly once, and all replicas
  ended with the same state digest. With `COMPACT=100 DOWN=5`, a four-language
  cluster (java-rust-cpp-go) transferred snapshots across language boundaries
  and still ended in one digest.

One Java-specific limit. The price ladder holds one object per price tick, about
70 MB per book at the default 0..1,000,000 range, so three in-process replicas
of 64 symbols do not fit in memory; `clusterbench` takes `--pmax` (default
20000) for that. Cluster nodes must keep the default range, because the
snapshot header, and so the state digest, carries it.

Throughput, `orderer-java`'s `clusterbench` against orderer-rust's, interleaved
on the busy §13.6 machine (load average 7-13 while these ran, and another
benchmark was running on it at times, so Rust's own figures moved by 2x between
sessions). Best across two sessions, M commands/s acked:

| Configuration | Rust | Java |
|---|---|---|
| 3 nodes, no-op state machine, no fsync | 14.4 | 9.9 |
| 1 node, no-op, no fsync | 52.8 | 36.9 |
| 3 groups of 3 nodes, no-op, no fsync | 22.8 | 11.5 |
| 3 nodes, no-op, fsync on a quorum, 4 producers, window 64 | 8.6 | 7.2 |
| 3 nodes, no-op, fsync on a quorum, 4 producers, window 256 | 13.5 | 9.6 |
| 3 nodes, no-op, fsync on a quorum, 2 producers, window 8 | 0.89 | 0.87 |
| 3 nodes, matching pipeline (P = 2), no fsync | 9.5 | 5.5 |
| 1 node, matching pipeline (P = 2), no fsync | 33.7 | 18.7 |
| 3 nodes, matching pipeline, fsync, 4 producers, window 256 | 9.2 | 4.8 |

Java reaches about two thirds of Rust on the replication path and about half
with the pipeline. A profile puts the producers' per-command `Command` objects
and the pipeline's own allocation ahead of anything in the runtime; two fixes
mattered (the batch encoder zeroed each command's bytes twice, and messages were
encoded into a buffer that grew by doubling, copying a 32 KB payload several
times before framing; both are gone). Under CPU contention a JVM stop-the-world
collection can outlast the election timeout, and a 40 M-command run lost
leadership once: that is the protocol working, not a fault, but it makes long
single-process benchmarks of any language fragile on a loaded machine.

### 13.14 The TypeScript runtime

`orderer-ts/src/cluster/` is the fifth runtime, with the same pieces and tools
(`orderernode`, `orderload`, `clusterbench`). Node's `zlib.crc32` is the IEEE
polynomial, not Castagnoli, so CRC-32C is a slicing-by-8 table implementation;
SHA-256 and HMAC come from `crypto`. Because JavaScript has one thread per
isolate, the replica is one event-driven object rather than three threads:
the WAL does group commit through an off-loop `fs.fsync`, and applies run in
2 ms time slices on the same loop as the protocol. Two consequences follow.
`StateMachine.apply` may return `false` ("call me again with this entry"), which
`PipelineSm` uses with a non-blocking publish: the first version blocked the
loop for seconds while engines created books, and the cluster lost its leader.
And a single `apply` longer than the election timeout would stall the node, so
the slow-leader test uses a backlog of 20 ms applies instead of one 700 ms apply.

* **Fixtures:** the same as the other runtimes, including byte-identical WAL
  and snapshot files; 10 wire tests.
* **26 end-to-end and scale-out tests:** 20 end-to-end, three of them with
  the matching pipeline, and 6 scale-out, with the same golden group values.
* **Mixed clusters** (`orderer-ts/scripts/interop.sh`, any mix of TS, Rust, Go
  and C++), with `kill -9` of the leader: ts-ts-ts, ts-rust-go, go-ts-cpp,
  rust-cpp-ts, cpp-ts-go with the recorder state machine, ts-rust-cpp with the
  authenticated handshake and compaction, and a four-language ts-rust-go-cpp
  cluster with compaction and four kills (12 snapshot installs, all across
  languages). Every acknowledged batch survived on every replica in the same
  order exactly once, and every cluster ended in one state digest.

Limits specific to this port. matcher-ts uses JavaScript numbers, so order ids,
prices and quantities must stay below 2^53: TS's `orderload` uses ids below
that, and the Rust and Go load tools cannot drive a TS node (`interop.sh` picks
the TS loader when a TS replica is in the mix). matcher-ts's default book
(`priceMax` 1,000,000) costs about 90 MB per symbol per replica; it has to stay
the default for interop because the snapshot header carries it, and the TS-only
tests and benchmark use a 20,000-level ladder.

Throughput, `orderer-ts`'s `clusterbench` against orderer-rust's, interleaved,
best of 3-5, on the same loaded machine (load average 9-11, so Rust's figures
are about half their usual; the ratios matter more than the absolutes), M
commands/s acked:

| Configuration | Rust | TS |
|---|---|---|
| 3 nodes, no-op state machine, no fsync | 8.3 | 2.3 |
| 1 node, no-op, no fsync | 50.8 | 6.4 |
| 3 groups of 3 nodes, no-op, no fsync | 21.9 | 7.5 |
| 3 nodes, no-op, fsync on a quorum, 4 producers, window 64 | 8.3 | 3.4 |
| 3 nodes, no-op, fsync on a quorum, 4 producers, window 256 | 14.0 | 3.7 |
| 3 nodes, no-op, fsync on a quorum, 2 producers, window 8 | 0.89 | 1.29 |
| 3 nodes, matching pipeline (P = 2), no fsync | 8.0 | 0.89 |
| 1 node, matching pipeline (P = 2), no fsync | 34.4 | 3.6 |
| 3 nodes, matching pipeline, fsync, 4 producers, window 256 | 8.4 | 1.8 |

The TS runtime does on one thread what Rust spreads over several, so it trails
wherever the CPU is the limit. The window-8 row is bounded by the durable ack
latency, not by the CPU. The matching-pipeline rows add matcher-ts's own cost
(the per-language pipeline numbers in `docs/RESULTS.md`).
