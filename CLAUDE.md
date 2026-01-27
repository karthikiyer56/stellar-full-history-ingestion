# Project: stellar-full-history-ingestion

This project contains tools for ingesting and indexing Stellar blockchain ledger data.

---

## Code Architecture Principles

### No Wrappers, No Indirection
- **Delete duplicate code** rather than creating wrapper/re-export files
- Consumers should import directly from the canonical source package
- If code exists in `pkg/`, delete local copies and update imports
- Prefer simplicity over backward compatibility shims

### Single Source of Truth
- Each type, interface, or utility should exist in exactly ONE place
- When refactoring: delete the old location, update all consumers
- Never maintain two copies of the same code

---

## Code Reuse Requirements

**CRITICAL**: Before writing new utility functions, ALWAYS check existing helpers first.

### `helpers/helpers.go` - General Utilities

| Function | Use For |
|----------|---------|
| `FormatBytes(int64)` | Format bytes as "1.23 GB" |
| `FormatBytesWithPrecision(int64, int)` | Format bytes with custom precision |
| `FormatDuration(time.Duration)` | Format duration as "1h2m3s" |
| `FormatNumber(int64)` | Format with commas "1,234,567" |
| `FormatRate(int64, time.Duration)` | Format rate as "1.23K/s" |
| `FormatPercent(float64, int)` | Format as "12.34%" |
| `Uint32ToBytes(uint32)` | Encode uint32 as 4-byte big-endian |
| `BytesToUint32([]byte)` | Decode 4-byte big-endian to uint32 |
| `Uint64ToBytes(uint64)` | Encode uint64 as 8-byte big-endian |
| `BytesToUint64([]byte)` | Decode 8-byte big-endian to uint64 |
| `HexStringToBytes(string)` | Convert hex string to bytes |
| `BytesToHexString([]byte)` | Convert bytes to hex string |
| `EnsureDir(string)` | Create directory if not exists |
| `FileExists(string)` | Check if file/directory exists |
| `IsDir(string)` | Check if path is directory |
| `GetDirSize(string)` | Get total size of directory |
| `GetFileCount(string)` | Count files in directory |
| `Min(int64, int64)` / `Max(int64, int64)` | Min/max for int64 |
| `MinUint32` / `MaxUint32` | Min/max for uint32 |
| `CalculateCompressionRatio(int64, int64)` | Calculate compression % |
| `CalculateOverhead(int64, int64)` | Calculate overhead % |

### `helpers/lfs/` - LFS Ledger Store Utilities

| Component | Use For |
|-----------|---------|
| `LedgerIterator` | Iterate over ledgers efficiently |
| `LedgerTiming` | Granular timing per ledger read |
| `DiscoverLedgerRange(dataDir)` | Find available ledger range in LFS store |
| `ValidateLfsStore(dataDir)` | Validate LFS store path exists and is valid |
| `LedgerToChunkID(ledgerSeq)` | Convert ledger sequence to chunk ID |
| `ChunkFirstLedger(chunkID)` | Get first ledger in a chunk |
| `ChunkLastLedger(chunkID)` | Get last ledger in a chunk |
| `ChunkExists(dataDir, chunkID)` | Check if chunk files exist |

---

## Logging Requirements

All tools MUST implement dual logging:

1. **Log File** (`--log-file` flag)
   - INFO and DEBUG level messages
   - Progress updates, statistics, summaries

2. **Error File** (`--error-file` flag)
   - ERROR and WARN level messages
   - Failures, mismatches, exceptions

### Log Format
```
[2006-01-02T15:04:05Z] [LEVEL] message
```

### Logger Interface Pattern
```go
type Logger interface {
    Info(format string, args ...interface{})
    Error(format string, args ...interface{})
    Debug(format string, args ...interface{})
    Warn(format string, args ...interface{})
    Separator()  // Log visual separator line
    Sync()       // Flush buffers to disk
}
```

---

## Metrics Requirements

Every significant operation MUST have comprehensive metrics:

### Timing Metrics
- Total duration for each phase/operation
- Per-item latency (e.g., per ledger, per batch)
- Breakdown of sub-operations where applicable

### Percentile Calculations
For latency measurements, always calculate and log:
- p50 (median)
- p90
- p95
- p99

### Throughput Metrics
- Items per second (ledgers/s, transactions/s, keys/s)
- Bytes per second where applicable
- Use `helpers.FormatRate()` for display

### Memory Metrics
- Log RSS (Resident Set Size) periodically
- Log before/after memory for major operations
- Warn when approaching memory thresholds

### Before/After Comparisons
For operations like compaction, show:
- File counts before/after
- Size before/after
- Reduction percentages

---

## Error Handling Policy

### Fatal Errors (ABORT immediately)
- Ledger/data parse errors (data integrity issue)
- Storage errors (RocksDB, disk I/O failures)
- Configuration validation failures
- Meta store corruption

### Non-Fatal Errors (LOG and CONTINUE)
- Verification mismatches (log details, continue to next)
- Individual query failures during SIGHUP handling
- Missing optional files

### Error Logging
- Always log to error file with full context
- Include: timestamp, operation, key/ID if applicable, error message
- For batch operations, summarize error counts at end

---

## Crash Recovery Requirements

All long-running tools MUST implement crash recovery:

### Checkpoint Frequency
- Every 1000 items/ledgers (configurable if needed)
- Balance between recovery granularity and I/O overhead

### Meta Store Pattern
- Use separate RocksDB instance for checkpoint data
- Store: phase, last_committed_item, counts, config
- Enable WAL (Write-Ahead Log) for durability

### Atomic Checkpointing
```
1. Complete batch write to main store
2. Update in-memory state
3. Write checkpoint to meta store (atomic)
```

### Resume Logic
```
1. Check if meta store exists
2. Validate config matches (abort if mismatch)
3. Load last checkpoint
4. Resume from last_committed + 1
```

### Count Accuracy Through Crashes
- Counts are checkpointed WITH progress, not computed from store
- On resume, restore counts from checkpoint
- Duplicates in main store are harmless (compaction dedupes)
- Verify counts after compaction by iterating store

---

## Documentation Requirements

### Code Comments
- Explain WHY, not just WHAT
- Document crash recovery behavior for each phase
- Explain count accuracy guarantees
- Note any non-obvious design decisions

### README Must Include
1. **Overview** - What the tool does, pipeline stages
2. **Prerequisites** - Dependencies, system requirements
3. **Installation** - Build instructions
4. **Usage** - All command-line flags with examples
5. **Output Structure** - Directory layout, file formats
6. **Crash Recovery** - All scenarios, how counts stay accurate
7. **Memory Requirements** - Expected usage, tuning options
8. **Performance Expectations** - Throughput, timing estimates
9. **Error Handling** - What's fatal vs non-fatal
10. **Troubleshooting** - Common issues and solutions

---

## Command-Line Flag Conventions

### Required Flags
| Pattern | Use For |
|---------|---------|
| `--lfs-store` | Path to LFS ledger store |
| `--start-ledger` | First ledger to process |
| `--end-ledger` | Last ledger to process |
| `--output-dir` | Base output directory |
| `--log-file` | Path to log file |
| `--error-file` | Path to error file |

### Optional Flags
| Pattern | Use For |
|---------|---------|
| `--dry-run` | Validate config and exit |
| `--parallel-X` | Enable parallel mode for operation X |
| `--block-cache-mb` | RocksDB block cache size |

---

## RocksDB Patterns

### Column Family Partitioning
When partitioning by key prefix (e.g., first hex char of hash):
- Use 16 column families: "0", "1", ..., "9", "a", "b", ..., "f"
- Plus "default" CF (required by RocksDB)

### Write-Optimized Settings (During Ingestion)
- Disable auto-compaction (`SetDisableAutoCompactions(true)`)
- Use manual full compaction after ingestion completes
- Configure MemTable sizes based on available RAM and number of CFs
- Calculate total memory: `WriteBufferSize × MaxWriteBufferNumber × NumCFs`

### Read-Optimized Settings (After Compaction)
- Enable bloom filters for point lookups
- Configure block cache based on available RAM

### Compaction
- Disable auto-compaction during bulk ingestion
- Run manual full compaction after ingestion completes
- Verify counts after compaction by iterating store

---

## Testing Checklist

Before considering a tool complete:
- [ ] Build compiles without errors
- [ ] All flags validated with helpful error messages
- [ ] Crash recovery works (kill and resume)
- [ ] Counts are accurate after crash/resume
- [ ] Metrics logged for every phase
- [ ] Error file captures all failures
- [ ] README documents all behavior
- [ ] Memory usage is reasonable

---

## Build Hygiene

After verifying a build compiles successfully:
- **DELETE the compiled executable** - do not leave binaries in the repo
- Only source code should be committed
- Run `rm <binary-name>` after `go build` verification
