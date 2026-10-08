# HARNESS — command-line contract v1

Every implementation ships these five tools with **identical arguments,
output bytes and exit codes**. That lets `scripts/` drive every language the
same way. Each tool mirrors a matcher harness, so the down-axis comparison
(orderer-X vs matcher-X) is a plain `cmp`.

| orderer tool | mirrors (matcher) | purpose |
|---|---|---|
| `orderrun` | `matcherrun` | run a command file through the pipeline, print events, optionally snapshot |
| `ordererfuzz` | `matcherfuzz` | same, for vectors and fuzz corpora (single-book or engine) |
| `orderrecover` | `matcherrecover` | restore a snapshot, replay a tail or journal directory, print events |
| `ordersnap` | `matchersnap` | run a command file, print the merged snapshot |
| `orderbench` | `matcher_bench` | benchmark (`BENCH.md`) |

Executable naming follows each language's convention (a binary, `node
dist/…js`, `java -cp … OrderRun`). The arguments and stdout below are the
contract.

## 1. Inputs

A **command file** is a matcher vector or corpus (`spec/matcher/SCHEMA.md`):

- Line 1 is a header. `pmin`, `pmax`, `max_orders` and `index` give the
  default book config, with matcher's defaults where absent (`0`,
  `1000000`, `65536`, `ladder`; `"both"` means `ladder`).
- `"engine":true` in the header selects **engine mode**: every command line
  carries `"symbol"`. Otherwise the file is **single-book**: every command
  goes to symbol `0`.
- Empty lines are skipped. Lines are submitted in file order by **one
  producer**, so the k-th command gets `iseq = k` (`PIPELINE.md` §2).

## 2. Common options

| Option | Default | Meaning |
|---|---|---|
| `--partitions P` | `1` | partition count (`ROUTING.md` §1) |
| `--partition-map FILE` | none | partition table (`ROUTING.md` §3) |
| `--journal-dir DIR` | none | write journals (`JOURNAL.md` §1) into `DIR`, creating it if needed and truncating existing journal files |
| `--binary` | off | journal format `binary` instead of `jsonl` (requires `--journal-dir`) |

Options may appear in any order after the positional arguments. Unknown
options are usage errors.

## 3. stdout: the event listing

Every tool that prints events prints canonical matcher event lines
(`spec/matcher/SCHEMA.md`), one per line, `\n`-terminated, **grouped by
partition**:

> all events of partition 0 in emission order, then all of partition 1, …,
> then partition P-1.

Grouping makes the output deterministic for any `P`. With `P = 1` it is the
plain emission order, byte-identical to the mirrored matcher tool.

**Tagging** (matching the matcher tool):

- `orderrun` and `orderrecover` always print **symbol-tagged** lines, as
  `matcherrun` and `matcherrecover` do.
- `ordererfuzz` prints tagged lines in engine mode and untagged lines for
  single-book files, as `matcherfuzz` does.

Nothing else is written to stdout. Diagnostics go to stderr.

## 4. Tools

### 4.1 `orderrun <cmd-file> [common options] [--snap PATH]`

Runs the file through a pipeline, drains, and prints the event listing
(§3). With `--snap PATH` it then takes a snapshot (`JOURNAL.md` §4) and
writes the body to `PATH` and the sidecar to `PATH.meta`. With `P = 1` the
body equals `matcherrun --snap`'s file.

### 4.2 `ordererfuzz <cmd-file> [common options]`

The same as `orderrun` without `--snap`, with §3 tagging. Builds with
internal assertions enabled (Rust debug, C++ sanitizer builds, …) check
book invariants after every command, as `matcherfuzz` does.

### 4.3 `orderrecover`

Two forms:

```
orderrecover <snapshot> <tail-file> [--partitions P] [--partition-map FILE]
orderrecover --journal-dir DIR [--snap PATH] [--binary] [--partitions P] [--partition-map FILE]
```

- **Tail form** (mirrors `matcherrecover`). Restore `<snapshot>`, then
  submit each line of `<tail-file>` that does not contain `"format"`, in
  order. Lines are matcher command lines, and `"symbol"` defaults to 0. An
  `"iseq"` key, if present, is ignored. A malformed line prints
  `<file>:<line>: malformed journal line: …` to stderr and exits 2. Prints
  the listing (§3) for the replayed commands only.
- **Journal form**. Recover per `JOURNAL.md` §5 from `DIR`'s journals
  (`--binary` selects `.bin` files), after the optional snapshot `PATH`
  (cut from `PATH.meta`). The journal's own `partitions` need not equal
  `--partitions`. Prints the listing for the replayed commands only.
  Corruption (`JOURNAL.md` §5) exits 2.

### 4.4 `ordersnap <cmd-file> [common options]`

Runs the file, drains, and prints the snapshot body (`JOURNAL.md` §4) to
stdout. The output equals `matchersnap`'s for any `P`.

### 4.5 `orderbench`

See `BENCH.md` §3.

## 5. Exit codes

| Code | Meaning |
|---|---|
| 0 | success |
| 2 | usage error, malformed input, invalid configuration (`ROUTING.md` §3), corrupt journal (`JOURNAL.md` §5) |
| other nonzero | internal failure (panic, invariant violation) |
