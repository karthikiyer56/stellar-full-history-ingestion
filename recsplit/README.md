# RecSplit

Minimal Perfect Hash Function (MPHF) for O(1) transaction hash to ledger sequence lookups.

## What It Does

Enables sub-microsecond txHash → ledgerSeq lookups with ~2 bits/key overhead. Uses mmap for index access and optional fuse filter for false-positive detection.

## Sub-directories

| Directory | Description |
|-----------|-------------|
| `builder/` | Constructs RecSplit indexes from transaction data. Reads from RocksDB or raw files. |
| `query/` | Two-step lookup: RecSplit → ledgerSeq, then RocksDB → LedgerCloseMeta |
