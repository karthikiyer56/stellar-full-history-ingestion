---
name: codebase-overview
description: Package structures, binary entry points, architecture patterns, and CLI flag conventions for stellar-full-history-ingestion
---

# Codebase Overview

## Package Structures

### `ingestion-workflow/` — Main Orchestrated Pipeline
```
main.go
internal/workflow/
  interfaces/       store and backend interfaces
  config/           configuration structs and validation
  orchestrator/     main workflow orchestrator (backfill + streaming)
  transition/       range transition logic (10M boundary handling)
  stores/
    rocksdb/base.go RocksDB base store
    meta/           meta store (RocksDB-backed key-value)
    lcm/            ledger close meta store
    txhash/         transaction hash store
  logging/          logger implementation
  metrics/          metrics collection
  types/            shared type definitions
  backend/          Stellar backend (captive core / horizon)
  testutil/         fixtures and mock backend
```

### `txhash-ingestion-workflow/` — TxHash Ingestion Pipeline
```
main.go
config.go
workflow.go
ingest.go
parallel_ingest.go
meta_store.go
query_handler.go
pkg/
  interfaces/       store interfaces
  store/            RocksDB store operations
  recsplit/         RecSplit index builder
  verify/           index verification
  compact/          RocksDB compaction
  cf/               column family management (16 CFs)
  logging/          logger
  memory/           RSS monitoring
  stats/            throughput, percentiles, ETA calculation
  types/            shared types
```

### `standalone-utils/` — 6 CLI Utility Tools
```
ingestion/           standalone ingestion tool
query/               query by ledger sequence or txhash
build-recsplit/      RecSplit index builder
compaction/          RocksDB compaction tool
iteration/           ledger iteration tool
captive-core-runner/ captive core runner
```

### `helpers/` — Shared Utilities
```
helpers.go    formatting, encoding, math, filesystem utilities
lfs/          LFS path helpers and ledger iterator
```

## Entry Points (Binaries)

| Location | Binary | Purpose |
|----------|--------|---------|
| `ingestion-workflow/main.go` | `ingestion-workflow` | Main orchestrated pipeline |
| `txhash-ingestion-workflow/main.go` | `txhash-workflow` | TxHash ingestion pipeline |
| `rocksdb/ingestion-v2/main.go` | `rocksdb-ingestion` | RocksDB direct ingestion |
| `local-fs/ingestion/` | `lfs-ingestion` | Local FS ingestion |
| `standalone-utils/*/main.go` | Various | 6 standalone utility tools |

## Architecture Patterns

### Phase-Oriented Workflow
Each range goes through ordered phases:
```
Ingest → RecSplit Build → Verify → Compact → Complete
```

### Column Family Partitioning
TxHash RocksDB uses **16 column families** partitioned by first hex char of txhash:
- CFs: `"0"`, `"1"`, ..., `"9"`, `"a"`, `"b"`, ..., `"f"` + `"default"`
- Enables parallel ingestion and per-CF compaction

### Crash Recovery
- Checkpoint every 1000 ledgers (backfill), every 1 ledger (streaming)
- Resume always from `last_committed_ledger + 1`

## Common CLI Flags

```
--lfs-store       Path to LFS ledger store
--start-ledger    First ledger to process
--end-ledger      Last ledger to process
--output-dir      Base output directory
--log-file        Path to log file (INFO/DEBUG)
--error-file      Path to error file (WARN/ERROR)
--dry-run         Validate config and exit
--block-cache-mb  RocksDB block cache size in MB
```
