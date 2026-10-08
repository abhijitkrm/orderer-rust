# JOURNAL — orderer journals, snapshots and recovery v1

Extends matcher's `spec/matcher/JOURNAL.md`. Command and event lines,
snapshot format and determinism argument are matcher's. This document adds
what the pipeline needs:

- per-partition files
- an `iseq` watermark on command records
- a fixed-layout binary encoding
- a snapshot sidecar
- the recovery procedure

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

## 2. Command journal

One record per command routed to the partition, in ingress order
(`PIPELINE.md` §3.2), written before the command is applied
(`PIPELINE.md` §4).

### 2.1 JSONL

Line 1 is the header:

```json
{"format":"orderer-journal/1","kind":"cmd","partition":0,"partitions":4}
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
The file starts with a 32-byte header, followed by fixed-size records.

**Header (32 bytes, both kinds):**

| Offset | Size | Field | Value |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `ORDJ` (`4F 52 44 4A`) |
| 4 | 2 | version | `1` |
| 6 | 1 | kind | `1` = cmd, `2` = evt |
| 7 | 1 | reserved | `0` |
| 8 | 4 | partition | `p` |
| 12 | 4 | partitions | `P` |
| 16 | 4 | record_size | `40` (cmd) or `48` (evt) |
| 20 | 12 | reserved | zeros |

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

## 3. Event journal

One record per event emitted by the partition's engine, in emission order.

### 3.1 JSONL

The header is as §2.1 with `"kind":"evt"`. Each later line is matcher's
canonical **symbol-tagged** event line, byte-for-byte
(`spec/matcher/SCHEMA.md`, `engine:true` form). It has no `iseq`, so event
journals are directly comparable with matcher engine output.

```json
{"format":"orderer-journal/1","kind":"evt","partition":0,"partitions":4}
{"seq":1,"ev":"accepted","symbol":10,"order_id":1,"leaves_qty":10}
```

### 3.2 Binary

The header is as §2.2 with kind `2` and record_size `48`.

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

1. Restore the snapshot (§4).
2. Read every partition's command journal. Records must have strictly
   increasing `iseq` within each file.
3. Merge all records by `iseq` (they are disjoint across partitions). Drop
   records with `iseq ≤ N`.
4. Apply the remaining records in `iseq` order, through the pipeline or
   directly through engines routed per `ROUTING.md`.
5. Resume sequencing at `max(N, highest replayed iseq) + 1`.

Determinism (matcher `SPEC.md` §7) guarantees the replayed events are
byte-identical to the original event journal's records for those commands.
`orderrecover` exists to prove it (`HARNESS.md`).

**Torn and corrupt journals are errors.** Recovery must fail loudly rather
than diverge silently. The following are corruption, and harnesses exit 2:

- JSONL: a final line without a newline, or any record line that does not
  parse.
- Binary: a body length that is not a multiple of `record_size`, a bad magic
  or version, or a header whose partition or kind does not match the file
  name.
- Non-increasing `iseq` within a file.
