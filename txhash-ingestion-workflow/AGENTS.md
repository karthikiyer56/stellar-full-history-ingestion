# AGENTS.md - txhash-ingestion-workflow

> TxHash-focused ingestion pipeline with public `pkg/` API.

## Overview

Unified pipeline for ingesting transaction hash → ledger sequence mappings. Uses `pkg/` pattern for reusable components.

**Entry Point**: `main.go`
**Build**: `go build -o bin/txhash-ingestion-workflow ./txhash-ingestion-workflow`

## Package Structure

```
txhash-ingestion-workflow/
├── main.go              # Entry point, signal handling
├── config.go            # Configuration parsing
├── workflow.go          # Main workflow orchestration
├── ingest.go            # Sequential ingestion
├── parallel_ingest.go   # Parallel ingestion mode
├── meta_store.go        # Checkpoint store
├── query_handler.go     # SIGHUP query support
└── pkg/                 # Reusable components
    ├── interfaces/      # Core contracts
    ├── store/store.go   # TxHashStore (RocksDB + 16 CFs)
    ├── recsplit/        # RecSplit index builder
    ├── verify/          # Index verification
    ├── compact/         # RocksDB compaction
    ├── cf/cf.go         # Column family routing
    ├── logging/         # Dual-output logger
    ├── stats/           # Throughput/percentile stats
    ├── memory/          # RSS monitoring
    └── types/           # Shared data types
```

## Workflow Phases

```
Phase 1: INGEST    → Read LFS, write to RocksDB (16 CFs)
Phase 2: COMPACT   → Flush and compact all CFs
Phase 3: RECSPLIT  → Build perfect-hash indexes per CF
Phase 4: VERIFY    → Validate all indexes
```

## Key Components

### Store (`pkg/store/`)
- RocksDB-backed with 16 Column Families
- CF selection via `pkg/cf/cf.go` (first hex char of hash)
- Batch writes with timing metrics

### RecSplit (`pkg/recsplit/`)
- Perfect-hash index builder
- Supports single and multi-index modes
- Memory-aware construction

### Verification (`pkg/verify/`)
- Post-build index validation
- Reports mismatches without aborting

### Compaction (`pkg/compact/`)
- Per-CF flush and compaction
- Stats reporting

## Runtime Modes

| Mode | Flag | Description |
|------|------|-------------|
| Full workflow | (default) | All phases |
| Ingest only | `--ingest-only` | Stop after RocksDB write |
| RecSplit only | `--recsplit-only` | Build indexes from existing store |
| Query mode | SIGHUP | Handle queries via signal |

## Configuration

| Flag | Required | Description |
|------|----------|-------------|
| `--lfs-store` | Yes | Path to LFS ledger store |
| `--output-dir` | Yes | Base output directory |
| `--start-ledger` | Yes | First ledger |
| `--end-ledger` | Yes | Last ledger |
| `--log-file` | Yes | INFO/DEBUG log path |
| `--error-file` | Yes | WARN/ERROR log path |
| `--parallel-workers` | No | Default: 16 |
| `--batch-size` | No | Default: 1000 |
| `--multi-index-enabled` | No | Build per-CF indexes |

## Memory Monitoring

`pkg/memory/memory.go` provides RSS checks:
```go
if memory.GetRSSMB() > threshold {
    // Trigger flush or pause
}
```

Used during RecSplit building to avoid OOM.

## Stats & Metrics

`pkg/stats/stats.go` provides:
- Throughput calculation
- Latency percentiles (p50, p90, p95, p99)
- Progress reporting with ETA

## Signal Handling

```go
// SIGHUP triggers query handler
// SIGINT/SIGTERM triggers graceful shutdown
```

Query handler allows live lookups against the store while workflow runs.

## Testing

```bash
# No dedicated test target - use direct go test
CGO_ENABLED=1 go test -v ./txhash-ingestion-workflow/...
```

Tests may require RocksDB environment (see root AGENTS.md).

## Error Handling

| Phase | Error Action |
|-------|--------------|
| Ingest | ABORT on store errors |
| Compact | LOG and continue |
| RecSplit | ABORT on build failure |
| Verify | LOG mismatches, continue |

## CF Partitioning

Same pattern as ingestion-workflow:
- 16 CFs: `"0"` through `"f"`
- Plus `"default"` CF
- Routing: `cf.GetCF(hash)` returns CF name based on first hex char
