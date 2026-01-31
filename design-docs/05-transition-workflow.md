# Transition Workflow

> **Purpose**: Detailed specification of the Active→Immutable transition process  
> **Related**: [Meta Store Design](./02-meta-store-design.md), [Streaming Workflow](./04-streaming-workflow.md)

---

## Overview

The transition workflow converts Active Stores (RocksDB) to Immutable Stores (LFS + RecSplit) when a 10M ledger range completes ingestion.

**Key Characteristics**:
- Triggered at 10M range boundary completion in both modes
- Same sub-workflow steps: RocksDB → LFS (ledgers), RocksDB → RecSplit (txhash)
- Runs in parallel: Ledger sub-flow + TxHash sub-flow
- **Streaming mode**: Non-blocking background goroutine, new Active Stores created, queries served during transition
- **Backfill mode**: Sequential/blocking per range, no new Active Stores, no queries served

---

## Trigger Conditions

Transition triggers when **the last ledger of a range** is ingested.

**Trigger Points**: 10,000,001, 20,000,001, 30,000,001, 40,000,001, ...

**Same trigger in both modes**: Whether in backfill or streaming, reaching the last ledger of a range initiates the transition sub-workflows.

See [Transition in Backfill vs Streaming Mode](#transition-in-backfill-vs-streaming-mode) for how the context differs.

---

## Transition in Backfill vs Streaming Mode

The transition sub-workflows (Ledger → LFS, TxHash → RecSplit) are **identical** in both modes.
The difference is in the surrounding context:

> **CRITICAL: RocksDB Deletion After Transition**
>
> **In BOTH modes (backfill AND streaming), once the transition sub-workflows complete and immutable files are created (LFS chunks + RecSplit indexes), the corresponding RocksDB stores for that 10-million ledger range are DELETED.**
>
> This is a **permanent, irreversible operation**. The immutable files become the sole source of truth for that range.

| Aspect | Backfill Mode | Streaming Mode |
|--------|---------------|----------------|
| **Trigger** | After range ingestion completes (last ledger written) | At 10M boundary (last ledger of range) |
| **New Active Stores** | NO - next range orchestrator creates fresh stores | YES - created immediately for continued ingestion |
| **Execution Model** | Sequential, blocking (range completes before next starts) | Background goroutine (ingestion continues to new stores) |
| **Query Serving** | NO - backfill mode doesn't serve queries | YES - transitioning stores remain queryable |
| **Parallelism** | Multiple ranges can transition in parallel (if 2+ orchestrators) | One transition at a time (single ingestion thread) |
| **After Completion** | Orchestrator picks up next PENDING range | Ingestion continues seamlessly to Range N+1 |
| **RocksDB Fate** | **DELETED after immutable files verified** | **DELETED after immutable files verified** |

### Why This Matters

**In Streaming Mode**, the system must:
1. Create new Active Stores BEFORE starting transition
2. Route new ledgers to the new stores immediately
3. Keep transitioning stores available for queries
4. Run transition as a background goroutine
5. **Delete transitioning RocksDB stores after immutable files are verified**

**In Backfill Mode**, none of this is needed because:
1. Each range orchestrator handles one range at a time
2. No queries are served during backfill
3. The next range doesn't start until transition completes (for that orchestrator)
4. Multiple orchestrators can work in parallel on different ranges
5. **Delete range RocksDB stores after immutable files are verified**

### The Transition Result (Same in Both Modes)

**Before Transition:**
```
Range N data lives in:
├── Ledger RocksDB: /data/.../ledger/rocksdb/  (mutable)
└── TxHash RocksDB: /data/.../txhash/rocksdb/  (mutable)
```

**After Transition:**
```
Range N data lives in:
├── LFS Chunks: /data/immutable/ledgers/chunks/000N/  (read-only)
└── RecSplit: /data/immutable/txhash/000N/    (read-only)

RocksDB stores: **DELETED** ← This is the key outcome!
```

### Flowchart: Backfill vs Streaming Context

```mermaid
flowchart TB
    subgraph Backfill["Backfill Mode"]
        B1[Ingest Range N] --> B2[Last Ledger Reached]
        B2 --> B3[Transition Sub-Workflows]
        B3 --> B4[Verify Immutable Files]
        B4 --> B5[DELETE RocksDB Stores]
        B5 --> B6[Range N → COMPLETE]
        B6 --> B7[Pick Next PENDING Range]
    end
    
    subgraph Streaming["Streaming Mode"]
        S1[Ingest Ledger] --> S2{Last Ledger of Range?}
        S2 -->|No| S1
        S2 -->|Yes| S3[Create NEW Active Stores for Range N+1]
        S3 --> S4[Spawn Transition Goroutine for Range N]
        S4 --> S5[Continue Ingesting to Range N+1]
        S4 --> S6[Transition Sub-Workflows - Background]
        S6 --> S7[Verify Immutable Files]
        S7 --> S8[DELETE RocksDB Stores]
        S8 --> S9[Range N → COMPLETE]
    end
```

**Key Insight**: The sub-workflow steps (Ledger Sub-Flow, TxHash Sub-Flow documented below) are IDENTICAL. Only the orchestration context differs. **The end result - RocksDB deletion and immutable file creation - is the same in both modes.**

---

## QueryRouter Integration

The transition workflow must update the QueryRouter registry to reflect store changes. The QueryRouter is dependency-injected into the transition orchestrator.

### Dependency Injection

```go
type TransitionOrchestrator struct {
    rangeID     uint32
    metaStore   *MetaStore
    queryRouter *QueryRouter  // Injected - used to update registry
    // ... other fields
}

func NewTransitionOrchestrator(rangeID uint32, meta *MetaStore, qr *QueryRouter) *TransitionOrchestrator {
    return &TransitionOrchestrator{
        rangeID:     rangeID,
        metaStore:   meta,
        queryRouter: qr,
    }
}
```

### Transition Sequence with QueryRouter Updates

> **ORDERING NOTE**: RocksDB deletion (step 6) happens BEFORE marking the range COMPLETE (step 7). The rationale: the COMPLETE state should only be set after all cleanup is finished—this ensures crash recovery never sees a "COMPLETE" range with orphaned RocksDB files.

```
1. Range N reaches last ledger (e.g., 60,000,001)

2. [STREAMING MODE ONLY] Create new Active Stores for Range N+1
   → queryRouter.AddActiveStore(N+1, newLedgerDB, newTxHashDB)

3. Mark Range N as TRANSITIONING in meta store
   → queryRouter.PromoteToTransitioning(N)
   → Note: Range N stores are now "transitioning" in registry

4. Spawn transition goroutine for Range N
   → Ledger sub-flow: RocksDB → LFS chunks
   → TxHash sub-flow: RocksDB → RecSplit indexes

5. Both sub-flows complete, immutable files verified
   → queryRouter.AddImmutableStores(N, lfsStore, recsplitStore)

6. Delete transitioning RocksDB stores
   → queryRouter.RemoveTransitioningStores(N)
   → RocksDB files physically deleted from disk

7. Update meta store: range:N:state = "COMPLETE"
   → This is the FINAL step, marking transition fully done
```

### Locking Implications

| Step | QueryRouter Method | Lock Type | Queries Blocked? |
|------|-------------------|-----------|------------------|
| 2 | AddActiveStore | Write | Yes (brief) |
| 3 | PromoteToTransitioning | Write | Yes (brief) |
| 5 | AddImmutableStores | Write | Yes (brief) |
| 6 | RemoveTransitioningStores | Write | Yes (brief) |

**Key Insight**: Write locks are held only for pointer swaps and map updates (microseconds). Queries experience negligible blocking. Meta store update (step 7) does not require QueryRouter lock.

### Sequence Diagram

```mermaid
sequenceDiagram
    participant Ingestion
    participant Transition
    participant QueryRouter
    participant MetaStore

    Note over Ingestion: Ledger 60,000,001 processed
    
    Ingestion->>QueryRouter: AddActiveStore(7, newLedgerDB, newTxHashDB)
    Ingestion->>QueryRouter: PromoteToTransitioning(6)
    Ingestion->>MetaStore: range:6:state = "TRANSITIONING"
    Ingestion->>Transition: Spawn goroutine for Range 6
    
    Note over Ingestion: Continues ingesting to Range 7
    
    Transition->>Transition: Build LFS chunks
    Transition->>Transition: Build RecSplit indexes
    Transition->>Transition: Verify immutable files
    
    Transition->>QueryRouter: AddImmutableStores(6, lfs, recsplit)
    Transition->>QueryRouter: RemoveTransitioningStores(6)
    Note over Transition: RocksDB stores deleted
    Transition->>MetaStore: range:6:state = "COMPLETE"
```

See [Query Routing - QueryRouter Architecture](./07-query-routing.md#queryrouter-architecture) for the full struct and method definitions.

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
4. Write to LFS format: `immutable/ledgers/chunks/XXXX/YYYYYY.data`
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
4. **Write**: Save indexes to `immutable/txhash/000N/index/cf-{0..f}.idx`
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

## Crash Recovery During Transition

If the **service crashes** (kill -9, power failure, OOM) during transition, the workflow resumes automatically on restart.

### Why This Works: Meta Store vs QueryRouter

| Component | Type | Survives Crash? | Purpose |
|-----------|------|-----------------|---------|
| **Meta Store** (RocksDB) | Persistent | ✅ Yes | Source of truth for range states and phases |
| **QueryRouter** (struct) | In-memory | ❌ No | Runtime routing cache—pointers to open stores |

**On crash:**
1. QueryRouter is lost (in-memory pointers gone)
2. Meta store survives with durable state (`range:6:state = "TRANSITIONING"`, `range:6:ledger:phase = "WRITING_LFS"`, etc.)

**On restart:**
1. System reads meta store to discover all range states
2. Rebuilds QueryRouter by opening appropriate stores based on meta store state
3. Resumes transition from the last checkpointed phase

### Restart Behavior by Crash Point

| Crash During | Meta Store State | Restart Action |
|--------------|------------------|----------------|
| Step 3 (mark TRANSITIONING) | `range:N:state = "TRANSITIONING"` | Resume transition sub-workflows from beginning |
| Step 4 (building LFS/RecSplit) | Phase shows `WRITING_LFS` or `BUILDING_RECSPLIT` | Resume from checkpointed progress within phase |
| Step 5 (adding immutable stores) | Immutable files exist, phase incomplete | Verify files, complete phase, continue to step 6 |
| Step 6 (delete RocksDB) | Immutable files verified | Delete RocksDB (may already be deleted), continue to step 7 |
| After step 6, before step 7 | RocksDB deleted, state still `TRANSITIONING` | Just mark `COMPLETE` (safe: immutable files exist) |

### Key Invariant

The COMPLETE state is written **last** (step 7), after RocksDB deletion (step 6). This ensures:
- If crash happens after step 6 but before step 7, restart sees `TRANSITIONING` and checks for immutable files—they exist, so it marks COMPLETE
- A range marked COMPLETE **always** has valid immutable files and **never** has orphaned RocksDB files

See [Crash Recovery - Scenario 5: Crash During Transition](./06-crash-recovery.md#scenario-5-crash-during-transition) for detailed walkthrough.

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
range:6:ledger:immutable_path = "/data/stellar-rpc/immutable/ledgers/chunks/0006"
range:6:txhash:phase = "COMPLETE"
range:6:txhash:recsplit_path = "/data/stellar-rpc/immutable/txhash/0006"

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

**Disk Storage**: For detailed disk storage calculations per 10M range (Ledger LFS: ~1.5 TB, TxHash RecSplit: ~15 GB), see [Storage Size Reference](./01-architecture-overview.md#storage-size-reference-per-10m-ledger-range).

---

## Related Documentation

- [Meta Store Design](./02-meta-store-design.md#scenario-4-streaming-mode-10m-boundary-transition) - Scenario 4: Boundary Transition
- [Streaming Workflow](./04-streaming-workflow.md#10m-boundary-detection) - Boundary detection logic
- [Crash Recovery](./06-crash-recovery.md#scenario-6-crash-during-transition) - Transition crash recovery
- [Query Routing](./07-query-routing.md) - How queries are routed during transition
