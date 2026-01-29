# Transition Workflow

> **Purpose**: Detailed specification of the Active→Immutable transition process  
> **Related**: [Meta Store Design](./02-meta-store-design.md), [Streaming Workflow](./04-streaming-workflow.md)

---

## Overview

The transition workflow converts Active Stores (RocksDB) to Immutable Stores (LFS + RecSplit) when a 10M ledger range completes. This process runs in the background while ingestion continues to the next range.

**Key Characteristics**:
- Triggered automatically in streaming mode only (not backfill)
- Runs in parallel: Ledger sub-flow + TxHash sub-flow
- Non-blocking: Ingestion continues to new Active Stores
- Query-safe: Transitioning stores remain available for queries

---

## Trigger Conditions

Transition triggers when streaming mode ingests the **last ledger of a range**.

**Trigger Points**: 10,000,001, 20,000,001, 30,000,001, 40,000,001, ...

**NOT triggered in backfill mode** - backfill creates immutable stores directly.

---

## Transition Process

```mermaid
flowchart TD
    Trigger([Ingest Last Ledger of Range]) --> Checkpoint[Checkpoint to Meta Store]
    Checkpoint --> CreateNew[Create NEW Active Stores for Next Range]
    CreateNew --> UpdateMeta[Update Meta: range N → TRANSITIONING]
    UpdateMeta --> SpawnGoroutine[Spawn Transition Goroutine]
    
    SpawnGoroutine --> Parallel{Parallel Execution}
    
    Parallel --> LedgerFlow[Ledger Sub-Flow]
    Parallel --> TxHashFlow[TxHash Sub-Flow]
    
    LedgerFlow --> ReadRocks[Read from Active RocksDB]
    ReadRocks --> WriteLFS[Write LFS Chunks]
    WriteLFS --> VerifyLFS[Verify LFS]
    VerifyLFS --> LedgerDone[Ledger: IMMUTABLE]
    
    TxHashFlow --> Compact[Compact RocksDB 16 CFs]
    Compact --> BuildRec[Build RecSplit Indexes]
    BuildRec --> VerifyRec[Verify RecSplit]
    VerifyRec --> TxHashDone[TxHash: COMPLETE]
    
    LedgerDone --> BothDone{Both Complete?}
    TxHashDone --> BothDone
    
    BothDone -->|Yes| DeleteActive[Delete Active RocksDB]
    DeleteActive --> UpdateComplete[Update Meta: range N → COMPLETE]
    UpdateComplete --> Done([Transition Complete])
    
    BothDone -->|No| Wait[Wait for Other]
    Wait --> BothDone
```

---

## Parallel Sub-Workflows

### Ledger Sub-Flow

| Phase | Description | Duration | Meta Store Key |
|-------|-------------|----------|----------------|
| INGESTING | Writing to Active RocksDB | Ongoing | `range:N:ledger:phase = "INGESTING"` |
| WRITING_LFS | Reading RocksDB, writing 1000 LFS chunks | ~30-60 min | `range:N:ledger:phase = "WRITING_LFS"` |
| IMMUTABLE | LFS complete, RocksDB deleted | - | `range:N:ledger:phase = "IMMUTABLE"` |

**Process**:
1. Read all ledgers from Active RocksDB (sequential scan)
2. Group into 10K ledger chunks
3. Compress each chunk with zstd
4. Write to LFS format: `immutable/ledgers/range-N/chunks/XXXX/YYYYYY.data`
5. Verify: Read back and decompress each chunk
6. Mark phase as IMMUTABLE

### TxHash Sub-Flow

| Phase | Description | Duration | Meta Store Key |
|-------|-------------|----------|----------------|
| INGESTING | Writing to Active RocksDB | Ongoing | `range:N:txhash:phase = "INGESTING"` |
| COMPACTING | Full compaction of 16 CFs (parallel) | ~5 min | `range:N:txhash:phase = "COMPACTING"` |
| BUILDING_RECSPLIT | Build 16 RecSplit indexes | ~15-20 min | `range:N:txhash:phase = "BUILDING_RECSPLIT"` |
| VERIFYING_RECSPLIT | Verify all keys in indexes | ~5 min | `range:N:txhash:phase = "VERIFYING_RECSPLIT"` |
| COMPLETE | RecSplit ready, RocksDB deleted | - | `range:N:txhash:phase = "COMPLETE"` |

**Process**:
1. **Compact**: Run full compaction on all 16 CFs in parallel
2. **Build**: For each CF, build RecSplit minimal perfect hash index
3. **Verify**: For each CF, verify all keys can be looked up in RecSplit
4. **Write**: Save indexes to `immutable/txhash/range-N/index/cf-{0..f}.idx`
5. Mark phase as COMPLETE

---

## Multiple Active Stores During Transition

During transition, up to 4 RocksDB instances may be open:

```
Current Active Stores (Range N+1):
├── Ledger RocksDB: /data/active/ledger/rocksdb
└── TxHash RocksDB: /data/active/txhash/rocksdb
    ↓ Receiving new ledgers

Transitioning Stores (Range N):
├── Ledger RocksDB: /data/transitioning/ledger/rocksdb
└── TxHash RocksDB: /data/transitioning/txhash/rocksdb
    ↓ Being converted (read-only for queries)
    ↓ Transition goroutine running
```

**Query Routing**: Queries for range N are routed to transitioning stores until transition completes.

[See Query Routing](./07-query-routing.md#during-transition) for details.

---

## Transition Failure Handling

**If transition fails** (e.g., disk full, corruption):
- Transitioning stores remain ALIVE
- Queries continue to work (served from transitioning RocksDB)
- Transition goroutine retries with exponential backoff
- Ingestion continues to new Active Stores (no data loss)

**Recovery**:
- Automatic retry until success
- Operator can investigate logs during retries
- No manual intervention required unless persistent failure

---

## Example: Transition at Ledger 70,000,001

**Before (ledger 70,000,000)**:
```
range:6:state = "INGESTING"
range:6:ledger:phase = "INGESTING"
range:6:ledger:last_committed_ledger = 70000000
range:6:txhash:phase = "INGESTING"
range:6:txhash:last_committed_ledger = 70000000
```

**Trigger (ledger 70,000,001 ingested)**:
```
# Detect: shouldTriggerTransition(70000001) == true
# This is the LAST ledger of range 6

# Ingest ledger 70,000,001 to range 6's Active Stores
range:6:ledger:last_committed_ledger = 70000001
range:6:txhash:last_committed_ledger = 70000001

# Create NEW Active Stores for range 7
range:7:state = "INGESTING"
range:7:start_ledger = 70000002
range:7:end_ledger = 80000001

# Mark range 6 as transitioning
range:6:state = "TRANSITIONING"
range:6:ledger:phase = "WRITING_LFS"
range:6:txhash:phase = "COMPACTING"
```

**Next Ledger (70,000,002)**:
```
# Ingestion continues to range 7's Active Stores
range:7:ledger:last_committed_ledger = 70000002
range:7:txhash:last_committed_ledger = 70000002
```

**During Transition (t=15 min)**:
```
range:6:state = "TRANSITIONING"
range:6:ledger:phase = "WRITING_LFS"  # Still writing chunks
range:6:txhash:phase = "BUILDING_RECSPLIT"  # Compaction done, building indexes
```

**Transition Complete (t=45 min)**:
```
range:6:state = "COMPLETE"
range:6:completed_at = "2026-01-29T15:30:00Z"
range:6:ledger:phase = "IMMUTABLE"
range:6:ledger:immutable_path = "/data/stellar-rpc/immutable/ledgers/range-6"
range:6:txhash:phase = "COMPLETE"
range:6:txhash:recsplit_path = "/data/stellar-rpc/immutable/txhash/range-6"

# Active RocksDB for range 6 deleted
# Queries now routed to immutable stores
```

---

## Performance Expectations

**Ledger Sub-Flow**:
- Read throughput: ~10,000 ledgers/sec from RocksDB
- Write throughput: ~5,000 ledgers/sec to LFS (zstd compression)
- Total time: ~30-60 minutes for 10M ledgers

**TxHash Sub-Flow**:
- Compaction: ~5 minutes (16 CFs parallel)
- RecSplit build: ~15-20 minutes (16 indexes)
- Verification: ~5 minutes
- Total time: ~25-30 minutes

**Overlap**: Both sub-flows run in parallel, so total transition time is ~30-60 minutes (dominated by ledger sub-flow).

---

## Memory Requirements

**During Transition**:
- Current Active Stores: ~16GB (2 RocksDB instances)
- Transitioning Stores: ~16GB (2 RocksDB instances)
- Transition goroutine: ~2GB (buffers)
- **Total**: ~34GB

**After Transition**:
- Current Active Stores: ~16GB
- Immutable Stores: Minimal (memory-mapped files)
- **Total**: ~16GB

---

## Related Documentation

- [Meta Store Design](./02-meta-store-design.md#scenario-4-streaming-mode-10m-boundary-transition) - Scenario 4: Boundary Transition
- [Streaming Workflow](./04-streaming-workflow.md#10m-boundary-detection) - Boundary detection logic
- [Crash Recovery](./06-crash-recovery.md#scenario-6-crash-during-transition) - Transition crash recovery
- [Query Routing](./07-query-routing.md) - How queries are routed during transition
