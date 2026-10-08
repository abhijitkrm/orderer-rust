# SCHEMA — golden vector format v1

Golden vectors are pairs of JSONL files (one JSON object per line, UTF-8, `\n`
line endings, no trailing whitespace, no comments):

```
vectors/<area>/<NNN>_<name>.cmd.jsonl    # header + commands
vectors/<area>/<NNN>_<name>.evt.jsonl    # header + expected events
```

`vectors/manifest.json` lists all pairs with descriptions.

## Command file (.cmd.jsonl)

Line 1 — header:

```json
{"format":"matcher-vector/1","name":"fifo_same_price","pmin":0,"pmax":1000000,"max_orders":65536,"index":"ladder"}
```

- `pmin`, `pmax`: book price range (both required; implementations running in
  `tree` mode ignore them except `price<=0` checks per SPEC §4.1).
- `max_orders`: optional, default 65536.
- `index`: `"ladder"` | `"tree"` | `"both"`, default `"ladder"`. `"both"` runs
  the vector under both indexes — they must produce the identical event stream.
  Only use a single mode when semantics legitimately differ (out-of-range
  prices: rejected in ladder, valid in tree).
- `engine`: optional boolean, default false. When true the vector drives the
  multi-symbol `Engine` (SPEC §6): every command line carries `"symbol":N`
  (u32) and `pmin`/`pmax`/`max_orders`/`index` become the engine's default
  book config applied to each symbol it creates.

Subsequent lines — one command each. `cmd` field first, then fields in the
order shown:

```json
{"cmd":"new","order_id":1,"side":"bid","otype":"limit","price":100,"qty":10,"tif":"gtc"}
{"cmd":"cancel","order_id":1}
{"cmd":"replace","order_id":1,"price":101,"qty":5}
```

- `new`: `order_id` u64, `side` `"bid"|"ask"`, `otype` `"limit"|"market"`,
  `price` i64 (required for limit; any value/0 allowed for market — ignored),
  `qty` u64, `tif` `"gtc"|"ioc"|"fok"|"post_only"`.
- `cancel`: `order_id`.
- `replace`: `order_id`, `price`, `qty` — the new values.

In `engine:true` vectors, each command line additionally carries
`"symbol":N` (u32) immediately after `"cmd"`.

## Event file (.evt.jsonl)

Line 1 — header `{"format":"matcher-vector/1","name":"<same>"}`.
Then one event per line, canonical key order (byte-exact):

```json
{"seq":1,"ev":"accepted","order_id":1,"leaves_qty":10}
{"seq":2,"ev":"trade","maker":2,"taker":5,"price":100,"qty":4}
{"seq":3,"ev":"closed","order_id":5,"reason":"filled"}
{"seq":4,"ev":"rejected","order_id":9,"reason":"unknown_order_id"}
{"seq":5,"ev":"replaced","order_id":3,"price":101,"qty":5}
```

Canonical rules:
- Key order exactly: `seq`, `ev`, then event fields in the order above.
- In `engine:true` vectors, `"symbol":N` follows `ev` on every event line
  (the book's own per-book `seq` is unchanged — sequences are per-symbol).
- JSON compact: `{"k":v,"k":v}` — single `:` after keys, `,` between pairs, no spaces.
- Strings are the lowercase tokens from SPEC (reasons, ev names).
- Numbers serialized as base-10 integers.
- File ends with a single trailing newline.

## Runner contract

Each implementation provides a golden test that, per vector:
1. Parses the `.cmd` header → constructs a book (ladder AND tree modes).
2. Feeds each command line in order.
3. Serializes emitted events with the canonical writer.
4. Byte-compares each produced line to the `.evt` file line.

Any mismatch = spec violation. Implementation bugs are fixed in the
implementation; semantic ambiguities are resolved by amending SPEC.md FIRST,
then the vectors.
