# ROUTING — symbol → partition map v1

Every implementation must route every symbol to the **same partition** for
a given `P` and partition table. That makes per-partition journals
byte-comparable across languages. Vectors: `vectors/routing/`.

## 1. Inputs

- `symbol`: u32 (`spec/matcher/SPEC.md` §1).
- `P`: partition count, `1 ≤ P ≤ 1024`. Any other value is a configuration
  error.
- An optional **partition table**: explicit `symbol → partition` overrides
  (§3).

## 2. Hash routing (default)

For a symbol with no table entry:

```
h = (symbol × 0x9E3779B97F4A7C15) mod 2^64      # unsigned 64-bit, wrapping
partition = ((h >> 32) × P) >> 32                 # unsigned 64-bit, exact
```

- `symbol` is zero-extended to 64 bits before the multiply.
- `0x9E3779B97F4A7C15` is ⌊2^64 / φ⌋ (Fibonacci hashing). It spreads dense
  instrument ids (0, 1, 2, …) evenly.
- The reduction is multiply-shift, not `%`. `(h >> 32) < 2^32` and
  `P ≤ 2^10`, so the product fits in 64 bits. No implementation may use
  floating point.
- `P = 1` ⇒ every symbol → partition 0.

Language notes: Java has no unsigned 64-bit type. Use `long`
multiplication (identical bits) and `>>>` for every shift. TypeScript uses
`BigInt` with `BigInt.asUintN(64, …)`. Go and C++ use `uint64` and
`uint64_t` natively.

Reference values (`P = 4`): 0→0, 1→2, 2→0, 3→3, 4→1, 5→0, 6→2, 7→1;
4294967295→3. Symbols 0..63 at `P = 4` split 17/15/16/16. The full tables
are in `vectors/routing/`.

## 3. Partition table

A table overrides hash routing for the symbols it lists. On-disk format
(JSONL, compact conventions of `spec/matcher/SCHEMA.md`):

```json
{"format":"orderer-partition-map/1","partitions":4}
{"symbol":10,"partition":3}
{"symbol":20,"partition":3}
```

- The header's `partitions` must equal the pipeline's `P`.
- Each `partition` must satisfy `0 ≤ partition < P`.
- A symbol listed twice is an error.
- Symbols not listed fall back to §2.
- Errors are configuration errors: harnesses exit 2 (`HARNESS.md`).

## 4. Stability

The map is fixed for the pipeline's lifetime. Live symbol migration is out
of scope (matcher `docs/SCALING.md` §Rebalancing). Snapshots are
partition-independent (`JOURNAL.md` §4), so restoring with a different `P`
or table is how a deployment rebalances.
