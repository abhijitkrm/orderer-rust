# JOURNAL — orderer journals, snapshots and recovery v1.3

Extends matcher's `spec/matcher/JOURNAL.md`. Command and event lines,
snapshot format and determinism argument are matcher's. This document adds
what the pipeline needs:

- per-partition files
- an `iseq` watermark on command records
- a fixed-layout binary encoding
- a snapshot sidecar
- the recovery procedure
- (1.2) per-record checksums, crash repair, and segments rotated at
  checkpoints
- (1.3) repair cuts zero-filled tails and deletes a segment a crash left
  without a usable header

Vectors: `vectors/pipeline/`, `vectors/journal/`, `vectors/recovery/`.

---

## 1. Files

A journal directory holds, for each partition `p` in `0 … P-1` (decimal, no
padding):

| Format | Command journal | Event journal |
|---|---|---|
| `jsonl` | `cmd-{p}.journal` | `evt-{p}.journal` |
| `binary` | `cmd-{p}.bin` | `evt-{p}.bin` |

All `P` pairs exist even when a partition receives no commands. Such files
hold only their header.

**Segments (1.2).** A journal may be split into segments. The files above
are segment `0`. A checkpoint (§6) with cut `N` starts segment `N` in every
partition: `cmd-{p}.{N}.journal` / `cmd-{p}.{N}.bin`, and likewise for
`evt-`. Segment `N` holds exactly the records with `iseq > N` up to the
next segment's start. A partition's segments, in ascending start order,
form one logical journal: every reader in this document reads them that
way. Each segment file has its own header.

## 2. Command journal

One record per command routed to the partition, in ingress order
(`PIPELINE.md` §3.2), written before the command is applied
(`PIPELINE.md` §4).

### 2.1 JSONL

Line 1 is the header. It carries the writing pipeline's default book
config, with matcher's key names, so journals are self-describing:

```json
{"format":"orderer-journal/1","kind":"cmd","partition":0,"partitions":4,"pmin":0,"pmax":1000000,"max_orders":65536,"index":"ladder"}
```

Each later line is matcher's canonical engine command line
(`spec/matcher/SCHEMA.md`, `"symbol"` after `"cmd"`) with `"iseq":N`
appended as the **last** key:

```json
{"cmd":"new","symbol":10,"order_id":1,"side":"bid","otype":"limit","price":100,"qty":10,"tif":"gtc","iseq":1}
{"cmd":"cancel","symbol":10,"order_id":1,"iseq":4}
{"cmd":"replace","symbol":6,"order_id":3,"price":101,"qty":5,"iseq":7}
```

Matcher's flat readers look keys up by name. They read these lines
unchanged and ignore `iseq` and the header (matcher `JOURNAL.md` §4). A
journal tail can therefore be replayed by a matcher implementation.

Market orders keep matcher's canonical form: `"otype":"market"` with the
`price` and `tif` the command carried.

### 2.2 Binary

All integers are **little-endian**. Signed values are two's complement.
The file starts with a 64-byte header, followed by fixed-size records.

**Header (64 bytes, both kinds):**

| Offset | Size | Field | Value |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `ORDJ` (`4F 52 44 4A`) |
| 4 | 2 | version | `2` (1.2 writers); readers accept `1` |
| 6 | 1 | kind | `1` = cmd, `2` = evt |
| 7 | 1 | index | `0` = ladder, `1` = tree |
| 8 | 4 | partition | `p` |
| 12 | 4 | partitions | `P` |
| 16 | 4 | record_size | version 2: `48` (cmd) or `56` (evt); version 1: `40` / `48` |
| 20 | 4 | reserved | zeros |
| 24 | 8 | pmin | i64 |
| 32 | 8 | pmax | i64 |
| 40 | 8 | max_orders | u64 |
| 48 | 16 | reserved | zeros |

**Command record (40 bytes):**

| Offset | Size | Field | Encoding |
|---:|---:|---|---|
| 0 | 8 | iseq | u64 |
| 8 | 4 | symbol | u32 |
| 12 | 1 | cmd | `1` new · `2` cancel · `3` replace |
| 13 | 1 | side | `0` bid · `1` ask (new only, else `0`) |
| 14 | 1 | otype | `0` limit · `1` market (new only, else `0`) |
| 15 | 1 | tif | `0` gtc · `1` ioc · `2` fok · `3` post_only (new only, else `0`) |
| 16 | 8 | order_id | u64 |
| 24 | 8 | price | i64 (new, replace; cancel `0`) |
| 32 | 8 | qty | u64 (new, replace; cancel `0`) |
| 40 | 4 | crc | version 2 only: CRC-32C (Castagnoli) of bytes 0–39 |
| 44 | 4 | reserved | version 2 only: zeros |

**Checksum (version 2).** CRC-32C as in iSCSI/RFC 3720: reflected
polynomial `0x82F63B78`, initial value `0xFFFFFFFF`, final XOR
`0xFFFFFFFF`, stored little-endian. Check value: the nine ASCII bytes
`123456789` give `0xE3069283`. A record whose checksum does not match is
corrupt (§5). JSONL journals carry no checksum: they are the debugging
encoding.

## 3. Event journal

One record per event emitted by the partition's engine, in emission order.

### 3.1 JSONL

The header is as §2.1 with `"kind":"evt"`. Each later line is matcher's
canonical **symbol-tagged** event line, byte-for-byte
(`spec/matcher/SCHEMA.md`, `engine:true` form). It has no `iseq`, so event
journals are directly comparable with matcher engine output.

```json
{"format":"orderer-journal/1","kind":"evt","partition":0,"partitions":4,"pmin":0,"pmax":1000000,"max_orders":65536,"index":"ladder"}
{"seq":1,"ev":"accepted","symbol":10,"order_id":1,"leaves_qty":10}
```

### 3.2 Binary

The header is as §2.2 with kind `2` and record_size `56` (version 2) or
`48` (version 1).

The JSONL and binary headers carry the same fields. Every file of one
journal directory must agree on `partitions` and the book config.

**Event record (48 bytes):**

| Offset | Size | Field | Encoding |
|---:|---:|---|---|
| 0 | 8 | seq | u64, per-book sequence |
| 8 | 4 | symbol | u32 |
| 12 | 1 | ev | `1` accepted · `2` rejected · `3` trade · `4` closed · `5` replaced |
| 13 | 1 | reason | rejected: `1` invalid_qty · `2` invalid_price · `3` duplicate_order_id · `4` unknown_order_id · `5` post_only_would_cross · `6` fok_cannot_fill · `7` book_full; closed: `1` filled · `2` cancelled · `3` expired; otherwise `0` |
| 14 | 2 | reserved | `0` |
| 16 | 8 | a | u64 |
| 24 | 8 | b | u64 |
| 32 | 8 | c | i64 |
| 40 | 8 | d | u64 |
| 48 | 4 | crc | version 2 only: CRC-32C of bytes 0–47 |
| 52 | 4 | reserved | version 2 only: zeros |

| ev | a | b | c | d |
|---|---|---|---|---|
| accepted | order_id | 0 | 0 | leaves_qty |
| rejected | order_id | 0 | 0 | 0 |
| trade | maker | taker | price | qty |
| closed | order_id | 0 | 0 | 0 |
| replaced | order_id | 0 | price | qty |

Binary and JSONL encode the same information. Converting either to the
other is lossless, and `vectors/journal/` pins both encodings of the same
runs.

## 4. Snapshots

An orderer snapshot is **one** `matcher-snap/1` document (matcher
`JOURNAL.md` §3) plus a sidecar.

- **Body.** It is exactly the snapshot a single matcher `Engine` would write
  for the same books: one header line with the pipeline's default book
  config, then every book's block in **ascending symbol order** across all
  partitions. Implementations may capture partitions separately but must
  merge into this form. The body is therefore independent of `P` and
  byte-identical to matcher's snapshot of the same state.
- **Sidecar** at `<snapshot path>.meta`, one line plus newline:

  ```json
  {"format":"orderer-meta/1","iseq":120,"partitions":4}
  ```

  `iseq` is the cut: the snapshot reflects exactly the commands with
  `iseq ≤ 120` (`PIPELINE.md` §6). `partitions` records the writer's `P`,
  for information only.
- A snapshot taken before any command has `"iseq":0`.

**Restore.** Parse the body. Route each book to its partition under the
*restoring* pipeline's `P` and table (`ROUTING.md`), and restore it there
(matcher `JOURNAL.md` §3 restore semantics). Restoring under a different
`P` is valid.

## 5. Recovery

Inputs: an optional snapshot with its sidecar (cut `N`; no snapshot means
`N = 0` and empty books), and a journal directory.

1. Determine the book config. With a snapshot, use its header; the
   journals' headers must match it, or recovery fails. Without one, use the
   journals' headers.
2. Restore the snapshot (§4).
3. Read every partition's command journal (all its segments, §1). Records
   must have strictly increasing `iseq` within a partition.
4. Merge all records by `iseq` (they are disjoint across partitions). Drop
   records with `iseq ≤ N`.
5. Apply the remaining records in `iseq` order, through the pipeline or
   directly through engines routed per `ROUTING.md`.
6. Resume sequencing at `max(N, highest replayed iseq) + 1`.

After a crash, partitions may have lost different unsynced tails, so the
merged `iseq`s can have gaps. That is expected: only commands whose
durability was acknowledged (`PIPELINE.md` §5) are guaranteed to survive,
and each partition's surviving records are a prefix of what it was sent.

Determinism (matcher `SPEC.md` §7) guarantees the replayed events are
byte-identical to the original event journal's records for those commands.
`orderrecover` exists to prove it (`HARNESS.md`).

**Torn and corrupt journals are errors** in the default, *strict* mode.
Recovery must fail loudly rather than diverge silently. The following are
corruption, and harnesses exit 2:

- JSONL: a final line without a newline, or any record line that does not
  parse.
- Binary: a body length that is not a multiple of `record_size`, a bad magic
  or version, or a header whose partition or kind does not match the file
  name.
- Headers in one directory that disagree on `partitions` or the book config.
- Non-increasing `iseq` within a file.
- Binary version 2: a record whose CRC-32C does not match.

### 5.1 Repair (1.2)

A crash can tear only the end of a file: the last write may be partial, or
the file may have been extended without its data arriving. The second
happens without power loss: on macOS, killing a process during a large
write can leave the file extended by zero bytes where the data never
landed, a whole write buffer of them.
*Repair* mode handles exactly that, and nothing else. For the **last
segment** of each journal file family:

- JSONL: a final line without a newline is cut off.
- Binary: a partial final record is cut off. Then (1.3) every final
  record that is entirely zero bytes is cut off; no valid record is all
  zeros (its checksum would fail, and iseq 0 is never assigned). Then, in
  version 2, a final complete record whose checksum fails is cut off too
  (one record at most: the record the interrupted write was filling).
- (1.3) A segment that cannot hold a record is deleted, if its start is
  above 0. That is a JSONL segment with no newline at all, or a binary
  segment whose 64-byte header is invalid (empty, partial or zero-filled)
  and whose bytes after the header, if any, are all zero. A crash
  between creating segment `N` at a checkpoint (§6 step 2) and its header
  reaching the file leaves one. The previous segments hold everything, and repair continues with
  the segment before it as the last segment. A segment 0 like this is
  still corruption: its pipeline never journaled a command, so nothing
  in it was acknowledged.
- (1.3) If the last segment then holds no records, the segment before it
  is repaired too, and so on back to the first segment that holds one: a
  writer may create segment `N` while it is still writing the end of the
  previous segment (§6 step 2), so a crash can tear a segment that is no
  longer the last.

Repair truncates the file to its last valid record, in place, and reports
how many bytes it removed (a deleted segment: its whole size). Every
other defect is still corruption, exactly as in strict mode. A pipeline may only append (§6) to repaired or clean
files.

## 6. Checkpoints (1.2)

A checkpoint bounds recovery time and disk use.

1. Take a snapshot (§4) with cut `N`, through the rings, so every partition
   cuts at the same point of the ingress order.
2. At that same control message, every partition's journal writers open
   segment `N` (§1) and finish their current segments: written and
   synced, before any record goes to segment `N`. Segment `N` itself (its
   header) may appear while the previous segment is still being written.
3. Write the snapshot body and sidecar durably: write to temporary names,
   sync, rename, sync the directory.
4. Only then delete every segment whose start is below `N`, and older
   checkpoint snapshots.

A crash at any point leaves a recoverable directory: before step 3
completes, the previous checkpoint and every segment are still present;
after it, the new snapshot and segment `N` suffice. Recovery reads whatever
segments exist (§5 step 3) and drops records with `iseq ≤ N`, so stale
segments left behind by a crash between steps 3 and 4 are harmless.

Checkpoint snapshots live in the journal directory as
`checkpoint-{N}.snap` and `checkpoint-{N}.snap.meta`. Recovering a
directory without an explicit snapshot uses the highest-numbered complete
checkpoint (body and sidecar both present).
