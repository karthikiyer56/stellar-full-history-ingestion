# AGENTS.md - ingestion-workflow

> Orchestrated ingestion pipeline for Stellar ledger data.

## Overview

This is the main ingestion workflow that coordinates ledger data processing through multiple phases. It uses an `internal/` package structure to enforce module boundaries.

**Entry Point**: `main.go`
**Build**: `make build-workflow` or `go build -o bin/ingestion-workflow ./ingestion-workflow`

## Package Structure

```
ingestion-workflow/
├── main.go                          # Entry point
└── internal/workflow/
    ├── interfaces/interfaces.go     # Core contracts (READ FIRST)
    ├── config/config.go             # Runtime configuration
    ├── orchestrator/                # Control flow
    │   ├── range.go                 # Range-based processing
    │   └── backfill.go              # Backfill coordination
    ├── transition/                  # Phase implementations
    │   ├── coordinator.go           # Phase sequencing
    │   ├── lfs.go                   # LFS writer phase
    │   └── recsplit.go              # RecSplit index build
    ├── stores/                      # Storage implementations
    │   ├── rocksdb/base.go          # Shared RocksDB wiring
    │   ├── meta/meta.go             # Checkpoint/meta store
    │   ├── lcm/lcm.go               # LCM store
    │   └── txhash/                  # TxHash store + CF routing
    ├── logging/logger.go            # Dual-output logger
    ├── metrics/metrics.go           # Timing/percentile helpers
    ├── types/types.go               # Shared data types
    ├── backend/gcs.go               # GCS adapter
    └── testutil/                    # Test helpers
        ├── fixtures.go              # Test data
        └── mock_backend.go          # MockLedgerBackend
```

## Key Interfaces

Located in `internal/workflow/interfaces/interfaces.go`:

| Interface | Purpose |
|-----------|---------|
| `MetaStore` | Checkpoint persistence, phase tracking |
| `LedgerStore` | Generic ledger storage operations |
| `TxHashStore` | Transaction hash → ledger sequence mapping |
| `LFSWriter` | Local filesystem chunk writer |
| `RecSplitBuilder` | Perfect-hash index construction |
| `RangeOrchestrator` | Ledger range processing coordination |

## Workflow Phases

```
1. Ingest    → Fetch ledgers from GCS/backend
2. LFS Write → Write to local filesystem chunks
3. RecSplit  → Build perfect-hash indexes
4. Verify    → Validate index correctness
5. Compact   → Run RocksDB compaction
```

Phase transitions are managed by `transition/coordinator.go`.

## Store Layer

### Meta Store (`stores/meta/`)
- Checkpoint persistence
- Phase state tracking
- Config validation on resume

### TxHash Store (`stores/txhash/`)
- 16 Column Family partitioning (hex-based)
- CF routing via `cf/cf.go`
- Batch write support with timing metrics

### LCM Store (`stores/lcm/`)
- RocksDB-backed ledger storage
- Compaction utilities

## Testing

### Run Tests
```bash
make test-workflow        # All workflow tests
make test-stores          # Store tests only
```

### Test Utilities
| File | Purpose |
|------|---------|
| `testutil/mock_backend.go` | `MockLedgerBackend` for unit tests |
| `testutil/fixtures.go` | Shared test data |
| `logging/logger.go` | `NewTestLogger()` for test output |

### Test Categories
| Category | RocksDB Required | Files |
|----------|------------------|-------|
| Pure unit | No | `config_test.go`, `logger_test.go` |
| Store tests | Yes | `meta_test.go`, `lcm_test.go`, `txhash_test.go` |
| Orchestrator | No (uses mocks) | `range_test.go`, `backfill_test.go` |

## Logging Convention

All components use `DualLogger`:
- `--log-file`: INFO/DEBUG output
- `--error-file`: WARN/ERROR output

```go
logger := logging.NewDualLogger(logFile, errorFile, "component-name")
logger.Info("Processing ledger %d", seq)
logger.Error("Failed: %v", err)
```

## Metrics Pattern

Use `metrics/metrics.go` helpers:
```go
timer := metrics.NewTimer()
// ... operation ...
duration := timer.Elapsed()
metrics.RecordLatency("operation_name", duration)
// Reports p50, p90, p95, p99
```

## Configuration

Config loaded from TOML via `config/config.go`:

| Field | Required | Description |
|-------|----------|-------------|
| `lfs_store` | Yes | Path to LFS data directory |
| `output_dir` | Yes | Base output directory |
| `start_ledger` | Yes | First ledger to process |
| `end_ledger` | Yes | Last ledger to process |
| `checkpoint_interval` | No | Default: 1000 |

## Error Handling

| Error Type | Action |
|------------|--------|
| Config validation | ABORT with clear message |
| Store open failure | ABORT - RocksDB issue |
| Phase transition error | ABORT - data integrity |
| Backend fetch error | RETRY with backoff, then ABORT |

## Reading Order (Onboarding)

1. `interfaces/interfaces.go` - Understand contracts
2. `orchestrator/range.go` - Main control flow
3. `transition/coordinator.go` - Phase sequencing
4. `stores/rocksdb/base.go` - RocksDB patterns
5. `stores/meta/meta.go` - Checkpoint mechanics
