# flush-store

Flushes all MemTables to SST files for an existing txHash→ledgerSeq RocksDB store. This cleans up WAL files, enabling fast read-only opens for benchmarking.

**No compaction is performed** - only flush (MemTable → SST).

## Why Use This

After ingestion, WAL files may contain unflushed data. Opening a store with large WAL files triggers WAL replay, which can take 10+ minutes. Running `flush-store` converts all data to SST files, making subsequent opens near-instant.

## Usage

```bash
# Build
go build ./rocksdb/tx-hash-ledger-sequence-rework/flush-store/...

# Dry run - see current state without making changes
./flush-store --store /path/to/store --dry-run

# Flush with defaults
./flush-store --store /path/to/store

# Flush with more parallelism
./flush-store --store /path/to/store --max-background-jobs 32

# With logging
./flush-store --store /path/to/store --log-file flush.log --verbose
```

Run `./flush-store --help` for all available options.
