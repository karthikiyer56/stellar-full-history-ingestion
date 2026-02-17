# AGENTS.md - stellar-full-history-ingestion

**Module**: `github.com/karthikiyer56/stellar-full-history-ingestion` | **Go**: 1.24.3

## Critical Rules

### NEVER Do

| Rule | Reason |
|------|--------|
| `DisableWAL(true)` | WAL required for crash recovery; checkpoint system depends on it |
| `as any`, `@ts-ignore` | N/A (Go project) |
| Import `github.com/stellar/go` | ARCHIVED - use `github.com/stellar/go-stellar-sdk` |
| Duplicate helper functions | Check `helpers/` first; single source of truth |
| Inline formatting/utility code | Add to `helpers/` package instead of app code |
| Leave binaries in repo | Delete after `go build` verification |
| Empty catch blocks | Always handle errors explicitly |

## Metrics Requirements (MANDATORY)

**Every I/O-bound or compute-heavy operation MUST be instrumented.** No exceptions.

### Operations to Instrument

| Category | Operations |
|----------|------------|
| **GCS/Remote** | Bucket list, object get, object put, metadata fetch |
| **Filesystem** | File read, file write, directory scan, file delete |
| **RocksDB** | Put, Get, Delete, Iterate, Compact, Flush, Checkpoint |
| **MDBX** | Put, Get, Delete, Cursor ops, Sync |
| **Network** | HTTP requests, gRPC calls, retries |
| **Processing** | Ledger parse, transaction decode, hash compute, verification |
| **Build/Index** | RecSplit build, index write, compaction |

### Required Metrics Per Operation

| Metric | Format | Example |
|--------|--------|---------|
| **Count** | Total operations | `ledgers_processed: 1,234,567` |
| **Bytes** | Total data volume | `bytes_written: 12.34 GB` |
| **Duration** | Wall-clock time | `elapsed: 1h23m45s` |
| **Throughput** | Rate over time | `rate: 1.23K ledgers/s, 45.6 MB/s` |
| **Latency percentiles** | p50, p90, p95, p99 | `p50=1.2ms p90=5.4ms p95=12ms p99=45ms` |

### Implementation Pattern

```go
// Track operation metrics
type OpMetrics struct {
    Count       int64
    Bytes       int64
    StartTime   time.Time
    Latencies   []time.Duration  // For percentile calculation
}

// Log at completion
func (m *OpMetrics) Log(logger Logger, opName string) {
    elapsed := time.Since(m.StartTime)
    logger.Info("%s complete: count=%s, bytes=%s, elapsed=%s, rate=%s",
        opName,
        helpers.FormatNumber(m.Count),
        helpers.FormatBytes(m.Bytes),
        helpers.FormatDuration(elapsed),
        helpers.FormatRate(m.Count, elapsed))
    
    // Log percentiles if latencies tracked
    if len(m.Latencies) > 0 {
        p50, p90, p95, p99 := calculatePercentiles(m.Latencies)
        logger.Info("%s latency: p50=%v p90=%v p95=%v p99=%v",
            opName, p50, p90, p95, p99)
    }
}
```

### Progress Logging

For long-running operations, log progress **every 1 minute** and **always include elapsed time from process start**:

```go
// Log progress every 1 minute - ALWAYS include elapsed from start
if time.Since(lastLog) >= 1*time.Minute {
    elapsed := time.Since(processStart)  // Always from process start, not last log
    logger.Info("progress: %s/%s (%s), elapsed=%s, rate=%s, eta=%s",
        helpers.FormatNumber(count),
        helpers.FormatNumber(total),
        helpers.FormatPercent(float64(count)/float64(total), 1),
        helpers.FormatDuration(elapsed),  // REQUIRED: elapsed from start
        helpers.FormatRate(count-lastCount, time.Since(lastLog)),
        estimateETA(count, total, elapsed))
    lastLog = time.Now()
    lastCount = count
}
```

### Final Summary Format

Every tool MUST print a final summary:

```
================================================================================
OPERATION COMPLETE: [operation-name]
================================================================================
  Ledgers processed:  1,234,567
  Transactions:       45,678,901
  Bytes written:      123.45 GB
  Total duration:     1h23m45s
  Average rate:       1.23K ledgers/s (28.5 MB/s)
  
  Latency (ledger processing):
    p50=1.2ms  p90=5.4ms  p95=12.3ms  p99=45.6ms
  
  Latency (RocksDB write):
    p50=0.5ms  p90=2.1ms  p95=4.5ms   p99=15.2ms
================================================================================
```

## Build Requirements

### CGO Dependencies

This project requires CGO for RocksDB and MDBX bindings:

```bash
# macOS (Homebrew)
brew install rocksdb snappy lz4 zstd

# Linux (Debian/Ubuntu)
sudo apt install -y librocksdb-dev libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev libbz2-dev
```

### Key Makefile Targets

| Target | Purpose |
|--------|---------|
| `make check-rocksdb-env` | Verify RocksDB installation and CGO flags |
| `make build-workflow` | Build ingestion-workflow binary |
| `make test-workflow` | Run workflow tests with CGO |
| `make test-stores` | Run store-specific tests |
| `make build-mdbx` | Build MDBX-backed binary |
| `make test-mdbx` | Verify MDBX linkage |
| `make generate-proto` | Regenerate protobuf files |

## Shared Utilities - MUST REUSE

### `helpers/helpers.go`

| Function | Use For |
|----------|---------|
| `FormatBytes(int64)` | Format bytes as "1.23 GB" |
| `FormatDuration(time.Duration)` | Format duration as "1h2m3s" |
| `FormatNumber(int64)` | Format with commas "1,234,567" |
| `FormatRate(int64, time.Duration)` | Format rate as "1.23K/s" |
| `FormatPercent(float64, int)` | Format as "12.34%" |
| `Uint32ToBytes` / `BytesToUint32` | Big-endian encoding |
| `Uint64ToBytes` / `BytesToUint64` | Big-endian encoding |
| `EnsureDir(string)` | Create directory if not exists |
| `FileExists(string)` | Check if file/directory exists |
| `Min` / `Max` | For int64, uint32 |

### `helpers/lfs/`

| Component | Use For |
|-----------|---------|
| `LedgerIterator` | Iterate over ledgers efficiently |
| `DiscoverLedgerRange(dataDir)` | Find available ledger range |
| `ValidateLfsStore(dataDir)` | Validate LFS store path |
| `LedgerToChunkID(seq)` | Convert ledger to chunk ID |
| `ChunkFirstLedger` / `ChunkLastLedger` | Chunk boundary helpers |

## Entry Points (Binaries)

| Location | Binary | Purpose |
|----------|--------|---------|
| `ingestion-workflow/main.go` | ingestion-workflow | Main orchestrated pipeline |
| `txhash-ingestion-workflow/main.go` | txhash-workflow | TxHash ingestion pipeline |
| `rocksdb/ingestion-v2/main.go` | rocksdb-ingestion | RocksDB direct ingestion |
| `local-fs/ingestion/` | lfs-ingestion | Local FS ingestion |
| `standalone-utils/*/main.go` | Various | 6 utility tools |

## Architecture Patterns

- **Phase-Oriented Workflow**: Ingest → RecSplit → Verify → Compact
- **Column Family Partitioning**: 16 CFs based on first hex char ("0"-"f" + "default")
- **Crash Recovery**: Checkpoint every 1000 items, resume from LastCommitted + 1

## Testing

### IMPORTANT: CGO Environment Required

**NEVER run `go test` directly.** This project has CGO dependencies (RocksDB, MDBX) that require specific environment variables.

**ALWAYS use Makefile targets** or check the Makefile for required env vars first:

```bash
# CORRECT: Use Makefile targets
make test-workflow    # Tests ingestion-workflow with RocksDB
make test-stores      # Tests RocksDB stores only
make test-mdbx        # Tests MDBX linkage

# WRONG: Direct go test will fail with linker errors
go test ./ingestion-workflow/...  # ❌ Missing CGO flags
```

**If you must run `go test` directly**, first run `make check-rocksdb-env` and extract the CGO flags:

```bash
# macOS example (from Makefile):
CGO_ENABLED=1 \
CGO_CFLAGS="-I$(brew --prefix rocksdb)/include" \
CGO_LDFLAGS="-L$(brew --prefix rocksdb)/lib -L$(brew --prefix snappy)/lib -L$(brew --prefix lz4)/lib -L$(brew --prefix zstd)/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd" \
DYLD_LIBRARY_PATH=$(brew --prefix rocksdb)/lib \
go test -v ./ingestion-workflow/...

# Linux example:
CGO_ENABLED=1 \
CGO_CFLAGS="-I${ROCKSDB_HOME:-/usr/local}/include" \
CGO_LDFLAGS="-L${ROCKSDB_HOME:-/usr/local}/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd" \
LD_LIBRARY_PATH=${ROCKSDB_HOME:-/usr/local}/lib \
go test -v ./ingestion-workflow/...
```

### Test Conventions

- Test files: `*_test.go` alongside implementation
- Fixtures: `testutil/fixtures.go`
- Mocks: `testutil/mock_backend.go`
- CGO required: Tests need RocksDB env vars set

## Logging Format

All tools must use this format:
```
[2006-01-02T15:04:05Z] [LEVEL] message
```

Logger interface:
```go
type Logger interface {
    Info(format string, args ...interface{})
    Error(format string, args ...interface{})
    Debug(format string, args ...interface{})
    Warn(format string, args ...interface{})
    Separator()
    Sync()
}
```

## Subdirectory Documentation

| Directory | Has AGENTS.md | Notes |
|-----------|---------------|-------|
| `ingestion-workflow/` | Yes | Orchestrator, stores, transitions |
| `txhash-ingestion-workflow/` | Yes | TxHash pipeline with pkg/ |
| `standalone-utils/` | Yes | 6 CLI tools index |

## Quick Reference

### Common Flag Patterns

```bash
--lfs-store       # Path to LFS ledger store
--start-ledger    # First ledger to process
--end-ledger      # Last ledger to process
--output-dir      # Base output directory
--log-file        # Path to log file
--error-file      # Path to error file
--dry-run         # Validate config and exit
--block-cache-mb  # RocksDB block cache size
```

### Error Handling

| Type | Action |
|------|--------|
| Parse/data errors | ABORT - data integrity issue |
| Storage errors | ABORT - RocksDB/disk failure |
| Config errors | ABORT - invalid configuration |
| Verification mismatch | LOG and CONTINUE |
| Missing optional files | LOG and CONTINUE |