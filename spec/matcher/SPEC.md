# SPEC — matcher semantics v1

This document is the **contract of record**. Every implementation
(`matcher-rust`, `matcher-go`, `matcher-cpp`, …) must satisfy it exactly.
Compliance is proven by the golden vectors in `vectors/` (format: `SCHEMA.md`):
for a given command stream, every implementation must emit the byte-identical
canonical event stream.

Design intent: the same shape real exchanges use — a single-writer limit order
book per symbol; commands in; deterministic, sequenced events out. No wall-clock
time, no randomness, no I/O inside the core.

---

## 1. Types

| Field | Type | Notes |
|---|---|---|
| `order_id` | u64 | caller-assigned; unique among **live** orders. A closed order's id MAY be reused. |
| `symbol` | u32 | `Engine` router key only; not part of single-book semantics. |
| `price` | i64 | opaque fixed-point ticks; only ordering matters. Scale is the application's choice. |
| `qty` | u64 | integer units (lots, shares, satoshis — application-defined). |
| `side` | enum | `bid` (buy), `ask` (sell). |
| `otype` | enum | `limit`, `market`. |
| `tif` | enum | `gtc`, `ioc`, `fok`, `post_only`. |
| `seq` | u64 | per-book event sequence, starts at 1, +1 per emitted event. |

`market` orders ignore `price` and `tif`: they are implicitly IOC and never rest.

## 2. Book configuration

Each `OrderBook` is constructed with:

| Param | Meaning |
|---|---|
| `price_min`, `price_max` | inclusive tick range for limit prices |
| `max_orders` | max live orders (`book_full` reject when exceeded) |
| index impl | `ladder` (bounded, O(1) best price) or `tree` (unbounded ordered map). Semantics identical; selection is a performance choice. Tree mode treats the range as unbounded — `invalid_price` then applies only to `price <= 0`. |

## 3. Commands

```text
new     { order_id, side, otype, price, qty, tif }
cancel  { order_id }
replace { order_id, price, qty }          # both fields are the NEW values
```

## 4. Events

Every emitted event carries the book `seq`. Within one command, events are
emitted in match order, then the terminal event.

| Event | Fields | Meaning |
|---|---|---|
| `accepted` | order_id, leaves_qty | order now rests in book (leaves_qty = remaining qty) |
| `rejected` | order_id, reason | command refused; no mutation |
| `trade` | maker, taker, price, qty | one execution; `price` is the resting level's price |
| `closed` | order_id, reason | order leaves the book / dies |
| `replaced` | order_id, price, qty | successful modify (price/qty = resulting values) |

### 4.1 Reject reasons

| Reason | Condition |
|---|---|
| `invalid_qty` | `qty == 0` (also for `replace` new qty) |
| `invalid_price` | limit `price` outside `[price_min, price_max]`; in tree mode: `price <= 0` |
| `duplicate_order_id` | `new` with an id already live |
| `unknown_order_id` | `cancel`/`replace` of a non-live id |
| `post_only_would_cross` | post-only order would execute immediately |
| `fok_cannot_fill` | fillable quantity within limit price < order qty |
| `book_full` | `new` ingested while live-order count == `max_orders`. Every `new` acquires a pool slot at ingest (resting or aggressive); the slot frees when the order closes. |

### 4.2 Close reasons

`filled` | `cancelled` | `expired` (IOC/market remainder).

### 4.3 Validation precedence (strict order)

For `new`:
1. `invalid_qty`
2. `invalid_price` (limit only; market ignores `price`)
3. `duplicate_order_id`
4. `book_full`
5. TIF checks: `post_only_would_cross`, then `fok_cannot_fill`

For `cancel`: `unknown_order_id`.
For `replace`: `unknown_order_id` → `invalid_qty` → `invalid_price`.

## 5. Matching rules (FIFO price-time)

1. An aggressive order matches the opposite side from **best price inward**.
   At the same price, orders execute in **arrival order** (level FIFO).
2. Trade `price` = resting (maker) order's price. Trade `qty` =
   `min(aggressor remaining, maker remaining)`. One `trade` event per fill.
   When a fill exhausts the maker, `trade` is emitted first, then
   `closed{filled}` — fill before consequence.
3. A maker reaching qty 0 is unlinked and emits `closed{filled}`.
4. When a price level empties it is removed before continuing to the next level.

**Post-match remainder handling for `new`:**

| Condition | Result |
|---|---|
| qty == 0 | `closed{filled}` |
| remainder, `gtc` | inserts at tail of its price level → `accepted{leaves_qty}` |
| remainder, `ioc` or `market` | `closed{expired}` |
| `market` with nothing fillable | zero trades + `closed{expired}` |

**FOK:** before matching, sum available quantity across opposite levels within
the limit price (read-only walk). If total < order qty → `rejected{fok_cannot_fill}`,
book untouched. Otherwise match normally (always fills completely).

**Post-Only:** if a limit bid's `price >= best_ask`, or a limit ask's
`price <= best_bid`, → `rejected{post_only_would_cross}`. Otherwise it rests.

**Cancel:** live order → unlink → `closed{cancelled}`. Non-live id →
`rejected{unknown_order_id}`.

**Replace:**

- `keep_priority` iff `new_price == old_price` AND `new_qty <= old_qty`.
  Then qty mutates in place (level total adjusts), `replaced{price, qty}` emitted.
- Otherwise: unlink, then re-enter the **aggressive limit GTC path** with the
  same `order_id`, new price, new qty. It may generate `trade`s; a resting
  remainder emits `replaced{price, qty=remaining}`; a fully filled remainder
  emits `closed{filled}` (no `replaced` event).
- Replace of a non-live id → `rejected{unknown_order_id}` (e.g. IOC orders never
  rest, so they can never be replaced).

**Self-matching:** allowed. The engine has no owner concept; self-trade
prevention belongs to the layer above.

## 6. Engine (multi-symbol router)

`Engine` maps `symbol → OrderBook` (each symbol its own config). `submit(symbol,
cmd)` routes to the book, creating it on first use with the engine's default
config if not pre-registered. Events passed to the sink are tagged with
`symbol`. Routing adds no semantics; all rules above are per-book:

- The order-id namespace is per-symbol: the same `order_id` may live
  simultaneously on different books and interacts only with its own book.
- `seq` remains per-book: events from different symbols interleave in the
  sink but carry each book's own dense sequence.
- A symbol's book is created lazily on first `submit`; its `seq` starts at 1.
- No cross-book atomicity: a command touches exactly one book. Features that
  need it (implied orders, cross-book STP) are out of scope (§9).

## 7. Determinism rules

- No wall-clock time, no RNG inside the core.
- `seq` is per-book, dense, monotone from 1.
- Same command stream ⇒ byte-identical canonical event stream (see SCHEMA.md).

## 8. Query API (read-only)

Implementations expose: `best_bid()`, `best_ask()`, `order(order_id)`,
`order_count()`, `level_count(side)`, `depth(side, n)` (price→qty pairs).
Queries mutate nothing and emit no events.

## 9. Extension points (reserved, not in v1)

- Additional matching algorithms (pro-rata, allocation) — the level `total_qty`
  accounting exists to support them.
- Iceberg/display qty, stop orders, implied orders.
- `owner_id` for self-trade prevention / fee attribution.
- Journaling/snapshots layered on the sink seam.
