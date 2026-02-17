# AGENTS.md - standalone-utils

> Collection of CLI tools for RocksDB TxHash store operations.

## Overview

Six standalone utilities for operating on TxHash RocksDB stores. Each is a single-file `main.go` binary.

## Tools Index

| Tool | Purpose | Build Command |
|------|---------|---------------|
| `rocksdb-txhashstore-ingestion-tool` | Ingest LFS/GCS → RocksDB | `go build ./standalone-utils/rocksdb-txhashstore-ingestion-tool` |
| `rocksdb-txhashstore-query-tool` | Query benchmark with CSV output | `go build ./standalone-utils/rocksdb-txhashstore-query-tool` |
| `rocksdb-txhashstore-build-recsplit-tool` | Build RecSplit indexes | `go build ./standalone-utils/rocksdb-txhashstore-build-recsplit-tool` |
| `rocksdb-txhashstore-compaction-tool` | Compact all 16 CFs | `go build ./standalone-utils/rocksdb-txhashstore-compaction-tool` |
| `rocksdb-txhashstore-iteration-tool` | Parallel iteration + CF stats | `go build ./standalone-utils/rocksdb-txhashstore-iteration-tool` |
| `simple-captive-core-runner` | CaptiveStellarCore benchmark | `go build ./standalone-utils/simple-captive-core-runner` |

## Common Patterns

### All Tools Share

- Dual logging: `--log-file` and `--error-file` flags
- RocksDB path: `--rocksdb-path` or `--output-dir`
- Progress reporting with timing metrics

### CGO Required

All tools require RocksDB CGO environment:
```bash
# Verify before building
make check-rocksdb-env
```

## Tool Details

### rocksdb-txhashstore-ingestion-tool

Ingests ledgers from LFS or GCS into RocksDB TxHash store.

```bash
./rocksdb-txhashstore-ingestion-tool \
  --lfs-store /path/to/lfs \
  --output-dir /path/to/output \
  --start-ledger 1000000 \
  --end-ledger 2000000 \
  --log-file ingestion.log \
  --error-file ingestion.err
```

Modes:
- LFS mode (default): Read from local filesystem store
- GCS mode: Read from Google Cloud Storage
- Flush-only mode: Skip compaction after ingestion

### rocksdb-txhashstore-query-tool

Benchmarks query performance against TxHash store.

```bash
./rocksdb-txhashstore-query-tool \
  --rocksdb-path /path/to/store \
  --query-file txhashes.txt \
  --query-output results.csv \
  --log-file query.log
```

Output: CSV with hash, ledger_seq, latency_ns

### rocksdb-txhashstore-build-recsplit-tool

Builds RecSplit perfect-hash indexes from existing store.

```bash
./rocksdb-txhashstore-build-recsplit-tool \
  --rocksdb-path /path/to/store \
  --output-dir /path/to/indexes \
  --multi-index-enabled \
  --log-file recsplit.log
```

Modes:
- Single index: One index for entire store
- Multi-index: Per-CF indexes (16 total)

### rocksdb-txhashstore-compaction-tool

Compacts all 16 Column Families.

```bash
./rocksdb-txhashstore-compaction-tool \
  --rocksdb-path /path/to/store \
  --log-file compact.log
```

Reports per-CF compaction time and final size.

### rocksdb-txhashstore-iteration-tool

Iterates store in parallel, reports CF statistics.

```bash
./rocksdb-txhashstore-iteration-tool \
  --rocksdb-path /path/to/store \
  --parallel-readers 16 \
  --log-file iteration.log
```

Output: Per-CF key count, size, iteration time.

### simple-captive-core-runner

Benchmarks CaptiveStellarCore PrepareRange and GetLedger.

```bash
./simple-captive-core-runner \
  --start-ledger 1000000 \
  --end-ledger 1001000 \
  --log-file captive.log
```

## Build All Tools

```bash
# Build all standalone utils
for dir in standalone-utils/*/; do
  go build -o "bin/$(basename $dir)" "./$dir"
done

# Remember to delete after verification (per project convention)
rm -rf bin/
```

## Error Handling

All tools follow the same pattern:
- Config errors → ABORT with usage
- Store errors → ABORT with error message
- Progress errors → LOG and continue where possible

## Logging Format

All tools use consistent format:
```
[2006-01-02T15:04:05Z] [INFO] message
[2006-01-02T15:04:05Z] [ERROR] error message
```

Progress logs include:
- Elapsed time
- Items processed / total
- Rate (items/sec)
- ETA
