# Replication wire fixtures

Written by `orderer-rust`'s `wirefix` binary (`cargo run --release -p
orderer-cluster --bin wirefix -- OUT_DIR`) from its own codec, WAL and snapshot
store. A port of the replication **runtime** must read all of them and
reproduce the same values, bytes and recovery results
(`spec/REPLICATION.md` 13.3).

| File | Contents |
|---|---|
| `msgs.txt` | `hex \| message` (message in the trace format of `../replication/README.md`) for every protocol message kind |
| `persists.txt` | `hex \| term vote from compact entries`: one WAL frame body (a consensus write) |
| `batches.txt` | `hex \| client seq \| cmds`: log entry payloads; `client` is `-` for a plain batch; each command is `symbol:kind:side:otype:tif:order_id:price:qty` |
| `wal-NAME.bin` | a WAL file: `good`, torn variants (`torn-mid*`, `torn-hdr*`), zero-filled tails (`zeros*`), `corrupt-mid`, `flip-last`, `badheader` |
| `wal-NAME.expect` | what opening that WAL must give: `ok <disk> frames=N torn=M`, `corrupt <offset>`, or `badheader`; `<disk>` is `term:voted:base:base_term:entries` |
| `snap-N.snap`, `snap-N.expect` | a snapshot file, and `index term len crc32c hex(data)` |

A port must also write the same bytes: the `good` WAL (four writes, the last a
compaction) and both snapshots must come out identical to the files here.
