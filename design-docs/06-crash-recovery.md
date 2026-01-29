# Crash Recovery

> **Purpose**: Comprehensive crash recovery scenarios and checkpoint mechanisms  
> **Related**: [Meta Store Design](./02-meta-store-design.md), [Backfill Workflow](./03-backfill-workflow.md), [Streaming Workflow](./04-streaming-workflow.md)

---

## Overview

The system implements robust crash recovery through per-range checkpointing in the meta store. All progress is tracked atomically, enabling resume from the exact point of failure with no data loss.

**Key Principles**:
- Checkpoint data is NEVER deleted
- Counts are checkpointed WITH progress (not computed from store)
- Duplicates in main store are harmless (compaction deduplicates)
- Recovery is automatic on restart

---

## Checkpoint Mechanism

### Checkpoint Frequency

| Mode | Frequency | Rationale |
|------|-----------|-----------|
| Backfill | Every 1000 ledgers | Balance throughput vs recovery granularity |
| Streaming | Every 1 ledger | Minimize data loss, low latency requirement |

### Atomic Checkpoint

All checkpoint updates use RocksDB WriteBatch for atomicity:

```go
func checkpoint(metaStore *RocksDB, rangeID uint32, ledgerSeq uint32, 
                ledgerCount uint64, txhashCounts map[string]uint64) error {
    batch := metaStore.NewWriteBatch()
    defer batch.Close()
    
    // Ledger progress
    batch.Put(
        fmt.Sprintf("range:%d:ledger:last_committed_ledger", rangeID),
        uint32ToBytes(ledgerSeq),
    )
    batch.Put(
        fmt.Sprintf("range:%d:ledger:count", rangeID),
        uint64ToBytes(ledgerCount),
    )
    
    // TxHash progress
    batch.Put(
        fmt.Sprintf("range:%d:txhash:last_committed_ledger", rangeID),
        uint32ToBytes(ledgerSeq),
    )
    batch.Put(
        fmt.Sprintf("range:%d:txhash:cf_counts", rangeID),
        json.Marshal(txhashCounts),
    )
    
    // Atomic commit
    return batch.Commit()
}
```

**Guarantee**: Either ALL keys update or NONE update (no partial state).

---

## Recovery Scenarios

### Scenario 1: Crash During Backfill Ingestion

**Situation**: Backfill running, crash at ledger 7,500,000 (mid-range 0)

**State at Crash**:
```
range:0:state = "INGESTING"
range:0:ledger:last_committed_ledger = 7499001  # Last checkpoint
range:0:ledger:count = 7499000
range:0:txhash:last_committed_ledger = 7499001
range:0:txhash:cf_counts = {"0": 468000, "1": 469500, ..., "f": 467800}
```

**Recovery Steps**:
1. Operator runs same command: `./stellar-rpc --backfill --start-ledger 2 --end-ledger 30000001`
2. Code loads meta store
3. Finds `range:0:state = "INGESTING"` (incomplete)
4. Reads `range:0:ledger:last_committed_ledger = 7499001`
5. **Resumes from ledger 7499002**
6. Continues until range completes

**Duplicate Handling**:
- Ledgers 7499002-7500000 may already be in RocksDB
- Re-ingestion writes same key→value pairs (idempotent)
- Compaction removes duplicates
- Counts remain accurate (restored from checkpoint, not recomputed)

**Post-Recovery**:
```
range:0:state = "COMPLETE"
range:0:ledger:phase = "IMMUTABLE"
range:0:txhash:phase = "COMPLETE"
```

---

### Scenario 2: Crash After Batch Write, Before Checkpoint

**Situation**: Batch of 1000 ledgers written to RocksDB, crash before checkpoint

**State at Crash**:
```
# Meta store (not updated):
range:0:ledger:last_committed_ledger = 5001

# RocksDB (updated):
# Ledgers 5002-6001 are in RocksDB
```

**Recovery**:
1. Load meta store: `last_committed_ledger = 5001`
2. Resume from ledger 5002
3. Re-ingest ledgers 5002-6001 (duplicates)
4. Continue from 6002

**Why This Works**:
- RocksDB writes are idempotent (same key→value)
- Checkpoint is the source of truth for progress
- Duplicates are harmless and removed during compaction

---

### Scenario 3: Crash During Compaction

**Situation**: Transition in progress, compaction running, crash occurs

**State at Crash**:
```
range:3:state = "TRANSITIONING"
range:3:ledger:phase = "WRITING_LFS"  # May be partially complete
range:3:txhash:phase = "COMPACTING"   # Interrupted
```

**Recovery**:
1. Restart streaming mode: `./stellar-rpc`
2. Code finds `range:3:state = "TRANSITIONING"`
3. **Restart transition goroutine** for range 3
4. Ledger sub-flow: Resume LFS writing (skip completed chunks)
5. TxHash sub-flow: **Restart compaction from beginning** (compaction is not resumable)

**Key Insight**: Compaction must restart from scratch, but this is acceptable because:
- Compaction is relatively fast (~5 min)
- Transitioning stores remain available for queries
- No data loss

---

### Scenario 4: Crash During Streaming

**Situation**: Streaming mode, crash at ledger 65,000,500

**State at Crash**:
```
range:6:state = "INGESTING"
range:6:ledger:last_committed_ledger = 65000500
range:6:txhash:last_committed_ledger = 65000500
```

**Recovery**:
1. Restart: `./stellar-rpc`
2. Gap detection: All prior ranges COMPLETE ✓
3. Find `range:6:state = "INGESTING"`
4. Resume from ledger 65000501
5. Continue streaming

**Batch Size = 1**: In streaming mode, checkpoint happens after every ledger, so recovery loses at most 1 ledger of progress.

---

### Scenario 5: Crash During Transition (Both Sub-Flows)

**Situation**: Transition in progress, both sub-flows running, crash occurs

**State at Crash**:
```
range:3:state = "TRANSITIONING"
range:3:ledger:phase = "WRITING_LFS"
range:3:txhash:phase = "BUILDING_RECSPLIT"

range:4:state = "INGESTING"
range:4:ledger:last_committed_ledger = 40500000
```

**Recovery**:
1. Restart streaming mode
2. Find `range:3:state = "TRANSITIONING"`
3. Find `range:4:state = "INGESTING"`
4. **Resume both**:
   - Spawn transition goroutine for range 3 (resume from current phases)
   - Resume ingestion for range 4 from ledger 40500001

**Parallel Recovery**:
- Transition continues in background
- Ingestion continues in foreground
- No blocking, no data loss

---

## Why Counts Are Always Accurate

### The Problem

If counts were computed by iterating the store after crash:
- Duplicates would inflate counts
- Expensive to recompute (millions of keys)
- Race conditions during concurrent writes

### The Solution

**Counts are checkpointed WITH progress**:

```go
// During ingestion:
ledgerCount++
txhashCounts[cf]++

// Every checkpoint:
checkpoint(metaStore, rangeID, ledgerSeq, ledgerCount, txhashCounts)
```

**On recovery**:
```go
// Restore counts from checkpoint
ledgerCount = metaStore.Get("range:N:ledger:count")
txhashCounts = metaStore.Get("range:N:txhash:cf_counts")

// Resume ingestion with restored counts
// Duplicates in RocksDB don't affect counts
```

**After compaction**:
- Duplicates removed from RocksDB
- Counts remain accurate (unchanged)
- Verification: Iterate store and compare to checkpointed counts

---

## Gap Detection

Streaming mode enforces "no gaps allowed" invariant.

### Gap Detection Algorithm

```go
func detectGaps(metaStore *RocksDB) error {
    ranges := loadAllRanges(metaStore)
    
    for _, r := range ranges {
        if r.State != "COMPLETE" {
            return fmt.Errorf(
                "Gap detected: Range %d (ledgers %d-%d) is %s, last_committed=%d",
                r.ID, r.StartLedger, r.EndLedger, r.State, r.LastCommitted,
            )
        }
    }
    
    return nil
}
```

### Example: Gap Detected

**Meta Store State**:
```
range:0:state = "COMPLETE"
range:1:state = "INGESTING"  # INCOMPLETE!
range:1:ledger:last_committed_ledger = 15000000
range:2:state = "COMPLETE"
```

**Streaming Startup**:
```
[ERROR] Cannot start streaming mode - incomplete ranges detected:
  Range 1 (ledgers 10000002-20000001): state=INGESTING, last_committed=15000000

Action required: Re-run backfill with --start-ledger 2 --end-ledger 30000001
```

**Resolution**: Operator must complete backfill for range 1 before streaming can start.

---

## Checkpoint Data Retention

**Policy**: Checkpoint data is NEVER deleted

**Rationale**:
- Enables crash recovery at any point
- Provides audit trail of ingestion history
- Allows verification of range completeness
- Minimal storage cost (KB per range)

**Cleanup**: Only delete checkpoint data when decommissioning the entire service.

---

## Recovery Time Estimates

| Scenario | Recovery Time | Notes |
|----------|---------------|-------|
| Backfill crash (mid-range) | Instant | Resume from checkpoint |
| Streaming crash | Instant | Resume from last ledger |
| Transition crash (ledger) | ~30-60 min | Resume LFS writing |
| Transition crash (txhash compaction) | ~5 min | Restart compaction |
| Transition crash (txhash recsplit) | ~15-20 min | Resume RecSplit build |

**Key Insight**: Recovery is fast because checkpoint data provides exact resume point.

---

## Testing Crash Recovery

### Manual Testing

```bash
# Start backfill
./stellar-rpc --backfill --start-ledger 2 --end-ledger 30000001

# Wait for progress (e.g., 5M ledgers)
# Kill process
kill -9 <pid>

# Restart with same command
./stellar-rpc --backfill --start-ledger 2 --end-ledger 30000001

# Verify: Resumes from checkpoint, no data loss
```

### Automated Testing

```go
func TestCrashRecovery(t *testing.T) {
    // Start ingestion
    service := startBackfill(2, 30000001)
    
    // Wait for checkpoint
    waitForLedger(service, 5000000)
    
    // Simulate crash
    service.Kill()
    
    // Restart
    service = startBackfill(2, 30000001)
    
    // Verify: Resumes from 5000001
    assert.Equal(t, 5000001, service.NextLedger())
}
```

---

## Related Documentation

- [Meta Store Design](./02-meta-store-design.md#scenario-2-crash-during-backfill-and-recovery) - Scenario 2: Crash Recovery
- [Meta Store Design](./02-meta-store-design.md#scenario-6-crash-during-transition) - Scenario 6: Transition Crash
- [Backfill Workflow](./03-backfill-workflow.md#checkpoint-mechanism) - Backfill checkpointing
- [Streaming Workflow](./04-streaming-workflow.md#graceful-shutdown) - Streaming shutdown and recovery
