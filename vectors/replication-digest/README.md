# Replication digest vectors

Conformance vectors for the event-chain digest (`spec/REPLICATION.md` 13.16),
written by `orderer-rust` (`ORDERER_GEN_DIGEST_VECTORS=DIR cargo test -p
orderer-cluster --test event_digest chains_match`).

Each file `NAME.Pk.txt` is the result of running the whole of
`vectors/pipeline/NAME.cmd.jsonl` (`multisymbol`, `fuzz_s11`) through a fresh
pipeline with `k` partitions (default hash routing, default book config) and
reading the chains after a drain:

```
orderer-evdigest/1
partitions k
commands N                     number of commands applied (the pipeline's iseq)
chains h0 h1 ...               each partition's chain, 16 lowercase hex digits
digest <64 hex>                SHA-256(tag | N u64 | k u32 | chains as u64), all little-endian
```

A port must reproduce every line exactly. The chain is defined in 13.16.
