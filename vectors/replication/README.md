# Replication conformance vectors

Per-node traces of the replication protocol core (`spec/REPLICATION.md`),
recorded from `orderer-rust`'s deterministic simulator (3 and 5 nodes, with
message loss, partitions, crashes, torn disks, compaction and snapshot
catch-up). An implementation's protocol core must replay every trace and
reproduce each recorded `out` and `state` line exactly.

`seedN-nodeK.trace` is node K of the simulator run with seed N (files for
seeds 31 and 32 are 5-node runs). Each file is cut after at most 1,000 drain
points. Regenerate with
`cargo run --release -p orderer-consensus --bin tracegen -- OUT_DIR`.

## Format

Line-oriented text. A `node` line starts a stanza (a fresh node built from a
disk image; a crash and restart starts a new one). `in` lines are inputs;
the `out` lines and the closing `state` line that follow are what the node
produced after the inputs since the previous `state` line.

```
node id=1 peers=0,1,2 election=10 heartbeat=2 batch=64 prevote=1 seed=42 disk=3:0:0:0:1:-,2:6162
in tick
in step 0 vote 4 7 3 1
out send 0 votereply 0 1 1
state role=Follower term=3 leader=- commit=0 applied=0 base=0 last=2
```

* `disk=term:voted(-|id):base:base_term:entries`; an entry is `term:hex` (hex `-`
  for empty data), entries joined by `,`, `-` for none.
* Inputs: `tick`, `step FROM MSG`, `propose HEX`, `replicate`, `persisted TOKEN`,
  `compact INDEX`, `install FROM TERM INDEX LAST_TERM`.
* Messages: `vote TERM LAST_INDEX LAST_TERM PRE`, `votereply TERM GRANTED PRE`,
  `append TERM PREV_INDEX PREV_TERM COMMIT ENTRIES`,
  `appendreply TERM SUCCESS INDEX`.
* Effects: `send TO MSG`, `persist TOKEN TERM VOTE FROM COMPACT ENTRIES`
  (`COMPACT` is `index:term` or `-`), `apply INDEX ENTRY`,
  `needsnap TO INDEX TERM`, `restore INDEX TERM`.
* `state role=ROLE term=T leader=L commit=C applied=A base=B last=X`.

The election timeout jitter comes from a xorshift64* generator seeded as
`splitmix64(seed ^ (id << 56))`, so a port needs the same generator
(`orderer-consensus/src/rng.rs`) for identical timings.

## Who replays these

orderer-rust (`orderer-consensus` `trace::replay`, and `tracegen` checks every
file it writes), orderer-cpp (`tests/replication_test.cpp`), orderer-go
(`consensus/consensus_test.go`), orderer-java (`ReplicationTest`) and
orderer-ts (`tests/replication.ts`). All five reproduce all 28 traces.
