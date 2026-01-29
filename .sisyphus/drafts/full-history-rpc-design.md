# Draft: Stellar Full History RPC Service Design

## Requirements Summary (from User)

### Core Concepts

| Term | Definition |
|------|------------|
| **Ledger Range** | Strictly 10 million ledgers (e.g., 10,000,001 to 20,000,000) |
| **Operating Mode** | Streaming (default) or Backfill - mutually exclusive, one process for ingestion |
| **LedgerBackend** | Interface for reading ledgers - BufferedStorageBackend (GCS) or CaptiveStellarCore |
| **Active Store** | RocksDB store for recent/live data - supports reads AND writes |
| **Immutable Store** | Custom LFS format on disk - read-only after creation, 10K ledgers per chunk |
| **Meta Store** | Separate RocksDB for tracking state, progress, active ranges, etc. |

### Hardware Constraints

- **CPU**: 32 cores
- **RAM**: 128 GB
- **CaptiveStellarCore**: ~8GB RAM per instance
- **Ledger size**: ~150KB compressed, ~1MB uncompressed
- **Batch size**: 1,000 ledgers (memory before commit)

### Operating Modes

1. **Backfill Mode**:
   - Processes bounded range (e.g., 2 to 60,000,000)
   - Can use BufferedStorageBackend (GCS) OR CaptiveStellarCore
   - No query capability while running
   - Multiple orchestrators for different 10M ranges in parallel

2. **Streaming Mode**:
   - Processes unbounded range (real-time ledgers)
   - MUST use CaptiveStellarCore (live data not in GCS)
   - Query capability enabled
   - Single process only

---

## Codebase Analysis

### What Exists (PoC Code)

| Component | Location | What It Does | What's Missing |
|-----------|----------|--------------|----------------|
| **LFS Ledger Store** | `local-fs/ingestion/` | Chunk-based storage (10K ledgers), index files, zstd compression | Complete |
| **RocksDB Ingestion** | `rocksdb/ingestion-v2/` | Ingest from GCS to RocksDB, batch processing, LedgerBackend usage | Need to adapt for Active Store |
| **TxHash Workflow** | `txhash-ingestion-workflow/` | Full pipeline: Ingest → Compact → RecSplit → Verify | Reads from existing LFS, doesn't write to immutable ledger stores |
| **Requirements Doc** | `random-readmes/requitements-for-full-history-rpc-service.md` | Complete requirements specification | N/A - reference doc |

### Key Patterns to Reuse

1. **Phase-based workflow** (`txhash-ingestion-workflow/workflow.go`):
   - Phase state machine with crash recovery
   - Meta store checkpointing every N items
   - Parallel execution where safe

2. **LFS chunk format** (`local-fs/ingestion/`):
   - 10K ledgers per chunk
   - `.data` + `.index` file pairs
   - Efficient point and range lookups

3. **LedgerBackend abstraction** (`rocksdb/ingestion-v2/`):
   - BufferedStorageBackend for GCS
   - Ready for CaptiveStellarCore integration

### What's Missing (Must Build)

| Component | Priority | Complexity | Notes |
|-----------|----------|------------|-------|
| **Active→Immutable Transition** | HIGH | HIGH | The core 4-5 hour process |
| **Dual Active Store Management** | HIGH | HIGH | When active→immutable is happening, new store needed |
| **Query Routing Layer** | HIGH | MEDIUM | Route to correct store based on ledger seq |
| **Backfill Orchestrator** | HIGH | MEDIUM | Manage multiple 10M range workers |
| **Gap Detection** | HIGH | LOW | Validate all ranges complete before streaming |
| **Streaming Ingestion** | MEDIUM | MEDIUM | CaptiveStellarCore integration |
| **TxHash Index (Immutable)** | MEDIUM | HIGH | RecSplit indexes for historical txHash→ledgerSeq |

---

## Decisions Made (User Confirmed)

### 1. Backfill Architecture ✅

**Decision**: Single Process, Multiple Goroutine Orchestrators

- Shared meta store
- Parallel 10M range workers (goroutines)
- LedgerBackend is configurable via TOML: `BufferedStorageBackend` OR `CaptiveStellarCore`
  - Not always GCS/S3 access available
  - Both implement same interface: `PrepareRange()` + `GetLedger()`

**CRITICAL: Backfill is Start-to-End Process**
- `--backfill --start-ledger 2 --end-ledger 60000000`
- Process runs parallel orchestrators
- When ALL ranges complete ingestion → **GRACEFUL EXIT**
- **NO automatic transition to streaming mode**
- Streaming mode is started separately (without `--backfill` flag)

### 2. Data Source for Backfill ✅

**Decision**: Configurable via TOML

```toml
[backfill]
ledger_backend = "buffered_storage"  # or "captive_core"

[backfill.buffered_storage]
gcs_bucket = "stellar-ledgers"
# ... GCS config

[backfill.captive_core]
binary_path = "/usr/bin/stellar-core"
config_path = "/etc/stellar-core.cfg"
# ... captive core config
```

### 3. Active Store Structure ✅

**Decision**: Separate RocksDB instances per data type

| Store Type | Key → Value | Notes |
|------------|-------------|-------|
| **Ledger Store** | LedgerSeq → Compressed LCM | Single CF |
| **TxHash Store** | TxHash → LedgerSeq | 16 CFs (sharded by first hex char) |
| (Future) **Events Store** | EventKey → Data | Will have own Active→Immutable flow |

**Rationale**: Extensibility. Each data type has:
- Its own RocksDB instance
- Its own notion of Active→Immutable transition
- Independent lifecycle management

### 4. HTTP Server During Backfill ✅

**Decision**: HTTP server runs, but limited endpoints

- **Available**: `getHealth`, `getStatus`
  - Returns backfill status
  - Shows progress of each 10M range
- **Unavailable**: All query endpoints (`getTransaction`, `getLedger`, etc.)

### 5. Active→Immutable Trigger ✅

**Decision**: Automatic at 10M boundary (STREAMING MODE ONLY)

**CRITICAL**: This concept ONLY applies in streaming mode, not backfill.

- Streaming from ledger 60,000,001
- Active range: 60,000,001 to 69,999,999
- When ledger 70,000,000 arrives → **TRIGGER TRANSITION**
- New active store created for 70,000,000+ range
- Previous active store enters transition phase

### 6. Transition Workflow ✅

**Decision**: Mini-orchestrator pattern with parallel sub-workflows

When transition triggers:
1. **Create new Active Store** for incoming range
2. **Spawn Transition Orchestrator** for previous range (goroutine)
3. Transition Orchestrator runs these **IN PARALLEL**:
   - **Ledger Sub-workflow**: Write Active→Immutable LFS chunks
   - **TxHash Sub-workflow**: Compact + Build RecSplit + Verify

**CRITICAL: Previous Active Store Lifecycle**
- Previous Active Store stays ALIVE for queries until:
  - Immutable LFS chunks created AND verified
  - RecSplit index built AND verified
- Only then: DELETE previous Active Store
- Signal: "TRANSITION SUCCESSFUL"

```
     ┌─────────────────────────────────────────────────────┐
     │        STREAMING MODE - 10M Boundary Trigger        │
     └─────────────────────────────────────────────────────┘
                              │
                              ▼
     ┌────────────────────────────────────────────────────┐
     │   1. Create NEW Active Stores for next 10M range   │
     │      (Ledger Store + TxHash Store)                 │
     └────────────────────────────────────────────────────┘
                              │
                              ▼
     ┌────────────────────────────────────────────────────┐
     │   2. Spawn Transition Orchestrator (goroutine)     │
     │      for PREVIOUS 10M range                        │
     └────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
     ┌────────────────────┐         ┌────────────────────┐
     │  LEDGER SUB-FLOW   │         │  TXHASH SUB-FLOW   │
     │  (parallel)        │         │  (parallel)        │
     ├────────────────────┤         ├────────────────────┤
     │ Read from Active   │         │ Compact RocksDB    │
     │ Write LFS chunks   │         │ Build RecSplit     │
     │ Verify chunks      │         │ Verify RecSplit    │
     └────────────────────┘         └────────────────────┘
              │                               │
              └───────────────┬───────────────┘
                              ▼
     ┌────────────────────────────────────────────────────┐
     │  3. BOTH complete? → DELETE previous Active Store  │
     │     Signal: TRANSITION_SUCCESSFUL                  │
     └────────────────────────────────────────────────────┘
```

### 7. Query Routing During Transition ✅

During transition, queries route to:
- **New incoming ledgers**: New Active Store
- **Previous range (in transition)**: Previous Active Store (still alive)
- **Older immutable ranges**: Immutable LFS + RecSplit indexes

### 8. Streaming Mode Startup ✅

**Decision**: Scan Meta Store + Validate All Ranges

When service starts in streaming mode (no `--backfill` flag):
1. Scan meta store for all 10M range states
2. Verify ALL ranges have reached terminal successful state:
   - Immutable LFS ledger store created ✓
   - Immutable RecSplit txhash index created + verified ✓
3. **IF validation fails**: Log failed ranges + reasons → EXIT
4. **IF validation passes**: Determine last processed ledger → Start CaptiveStellarCore from `last_ledger + 1`

**CRITICAL: Gap Detection**
- If backfill was incomplete and operator starts streaming mode → CODE DETECTS GAP → ABORT
- No silent gaps allowed

### 9. Parallel Orchestrators in Backfill ✅

**Decision**: Limited parallelism (~2 concurrent), configurable

**Current Reality** (as of ~2026):
- ~62 million ledgers exist (genesis Dec 2015 to now)
- Only 6 complete 10M ranges possible
- Even 128GB RAM with 1000 ledger batch size risks OOM with too many parallel

**Recommended**: ~2 parallel 10M range ingestions
- Complete 60M ledgers in ~3 turns
- Each turn processes 2 ranges in parallel

**Note on Ledger Density**:
- Ledgers 1-30M: ~10 txs/ledger (sparse)
- Ledgers 30M+: ~325 txs/ledger (dense)
- Future projection: ~300 txs/ledger at 10x faster rate

### 10. Crash Recovery per Range ✅

**Decision**: Per-range checkpointing (checkpoint info is never deleted)

- Each 10M range has its own checkpoint in meta store
- Checkpoint info is IDENTICAL for backfill vs streaming mode
- It's just "crash recovery metadata" - mode-agnostic
- Checkpoint info persists forever (never deleted)

### 11. Transition Failure Handling ✅

**Decision**: Keep using Active Stores, retry in goroutine (no background process)

During transition:
- **Previously Active Stores**: Continue serving queries (still alive)
- **Currently Active Stores**: Receiving new ledger ingestion
- Transition runs in separate goroutine (NOT a background process)
- Retry logic is within that goroutine until success

### 12. Backfill Process Lifecycle ✅

**CRITICAL BEHAVIOR**:

```bash
# Backfill invocation
./stellar-rpc --backfill --start-ledger 2 --end-ledger 60000000

# Validation rules:
# --start-ledger: Must be 2 OR (X % 10,000,000 == 1)
# --end-ledger: Must be (X % 10,000,000 == 0)
```

**Process Flow**:
1. Validate ledger range parameters
2. Initialize meta store (or load existing for crash recovery)
3. Spawn parallel orchestrators (e.g., 2 at a time)
4. Each orchestrator ingests its 10M range → creates immutable stores
5. When range completes, start next pending range
6. **ALL ranges complete** → Process exits gracefully
7. **Any failure** → Log error → Process aborts

**NO automatic transition to streaming mode. Operator must restart with streaming mode.**

### 13. Streaming Mode Process Lifecycle ✅

```bash
# Streaming invocation (no --backfill, no ledger range)
./stellar-rpc
```

**Startup Flow**:
1. Load meta store
2. Validate all backfilled ranges reached terminal state
3. **If gaps/incomplete ranges** → Log details → EXIT
4. Find last successfully processed ledger
5. Initialize CaptiveStellarCore from `last_ledger + 1`
6. Begin streaming ingestion (batch size = 1, update meta after each ledger)

**Graceful Shutdown**:
1. Finish processing current ledger
2. Update meta store with last successful ledger
3. Exit cleanly

**Crash Recovery**:
1. On restart, read last successful ledger from meta store
2. Resume from `last_ledger + 1`

---

## Current Numbers + Future Projections

### Historical Context

| Metric | Value | Notes |
|--------|-------|-------|
| Genesis | December 2015 | Ledger #2 is first |
| Current Ledgers | ~62 million | As of late 2025/early 2026 |
| 10M Ranges | ~6 complete | Ranges 1-6 (2 to 60,000,000) |
| Historical Ledger Close Time | 5-6 seconds | |
| Ledgers per Year | ~5.5 million | At 5-6 sec close time |
| Time Span per 10M Range | ~1 year 11 months | Historical |

### Transaction Density

| Era | Transactions/Ledger | Notes |
|-----|---------------------|-------|
| Early Years (ledgers 1-30M) | ~10 tx/ledger | Sparse |
| Recent Years (30M+) | ~325 tx/ledger | Dense |
| Future Target | ~300 tx/ledger | At higher throughput |

### Future Projections

| Metric | Current | Future |
|--------|---------|--------|
| Ledger Close Time | 5-6 seconds | 0.6 seconds (10x faster) |
| Time Span per 10M Range | ~23 months | ~2 months |
| Target Throughput | ~50 tx/sec | ~500 tx/sec |
| Expected tx/ledger | ~325 | ~300 |

**Implication**: In future, 10M range transitions will happen every ~2 months instead of every ~2 years.

---

## Research Findings (from Codebase)

### LFS Chunk Format (from local-fs/ingestion)

```
chunks/XXXX/YYYYYY.data   # Concatenated zstd-compressed LCM
chunks/XXXX/YYYYYY.index  # 8-byte header + offsets
```

- Chunk ID = (ledger_seq - 2) / 10000
- Efficient pread-based lookups
- ~150KB avg compressed per ledger

### RocksDB Ingestion Pattern (from rocksdb/ingestion-v2)

```go
// GCS source setup
dataStore := datastore.NewDataStore(ctx, datastoreConfig)
backend := ledgerbackend.NewBufferedStorageBackend(backendConfig, dataStore, dataStoreSchema)
backend.PrepareRange(ctx, ledgerbackend.BoundedRange(start, end))

// Batch processing
for ledgerSeq := start; ledgerSeq <= end; ledgerSeq++ {
    ledger := backend.GetLedger(ctx, ledgerSeq)
    // compress, write to RocksDB
}
```

### Workflow State Machine (from txhash-ingestion-workflow)

```
INGESTING → COMPACTING → BUILDING_RECSPLIT → VERIFYING_RECSPLIT → COMPLETE
```

With crash recovery at each phase checkpoint.

---

## Preliminary Architecture (Subject to User Feedback)

### High-Level Components

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           STELLAR FULL HISTORY RPC                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐        │
│  │   BACKFILL      │     │    STREAMING    │     │   QUERY         │        │
│  │   ORCHESTRATOR  │     │    INGESTION    │     │   ROUTER        │        │
│  │                 │     │                 │     │                 │        │
│  │ (Multiple 10M   │     │ (CaptiveStellar │     │ (Routes to      │        │
│  │  range workers) │     │  Core, live)    │     │  correct store) │        │
│  └────────┬────────┘     └────────┬────────┘     └────────┬────────┘        │
│           │                       │                       │                  │
│           ▼                       ▼                       ▼                  │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                          META STORE (RocksDB)                          │ │
│  │  - Range states (pending, active, immutable)                           │ │
│  │  - Current ledger, phase, counts                                       │ │
│  │  - Active store pointers                                               │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                          ACTIVE STORE (RocksDB)                        │ │
│  │  - LedgerSeq → Compressed LCM                                          │ │
│  │  - TxHash → LedgerSeq                                                  │ │
│  │  - Supports reads AND writes                                           │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                       IMMUTABLE STORES (LFS)                           │ │
│  │  - Per-range directories                                               │ │
│  │  - Ledger chunks (10K per chunk)                                       │ │
│  │  - RecSplit indexes for txHash                                         │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Proposed Meta Store Key Structure

The meta store is a single RocksDB instance that tracks all state for both backfill and streaming modes.

### Key Hierarchy

```
# Global State
global:mode                           → "backfill" | "streaming"
global:last_processed_ledger          → uint32 (for streaming mode)
global:backfill_start_ledger          → uint32
global:backfill_end_ledger            → uint32

# Per-Range State (10M ranges)
range:{range_id}:state                → RangeState enum
range:{range_id}:start_ledger         → uint32
range:{range_id}:end_ledger           → uint32
range:{range_id}:created_at           → timestamp
range:{range_id}:completed_at         → timestamp (if complete)

# Per-Range Ledger Store State
range:{range_id}:ledger:phase         → Phase enum
range:{range_id}:ledger:last_committed_ledger → uint32
range:{range_id}:ledger:count         → uint64 (total ledgers ingested)
range:{range_id}:ledger:immutable_path → string (path to LFS after transition)

# Per-Range TxHash Store State (mirrors txhash-ingestion-workflow pattern)
range:{range_id}:txhash:phase         → Phase enum
range:{range_id}:txhash:last_committed_ledger → uint32
range:{range_id}:txhash:cf_counts     → JSON map {"0": count, "1": count, ..., "f": count}
range:{range_id}:txhash:rocksdb_path  → string (Active Store path)
range:{range_id}:txhash:recsplit_path → string (Immutable RecSplit path after transition)

# Transition State (only during active→immutable transition)
range:{range_id}:transition:started_at     → timestamp
range:{range_id}:transition:ledger_status  → "pending" | "in_progress" | "complete"
range:{range_id}:transition:txhash_status  → "pending" | "in_progress" | "complete"
```

### Range State Enum

```go
type RangeState string

const (
    RangeStatePending      RangeState = "PENDING"      // Not started
    RangeStateIngesting    RangeState = "INGESTING"    // Active ingestion
    RangeStateTransitioning RangeState = "TRANSITIONING" // Active→Immutable in progress
    RangeStateComplete     RangeState = "COMPLETE"     // Fully immutable
    RangeStateFailed       RangeState = "FAILED"       // Error state
)
```

### Ledger Phase Enum

```go
type LedgerPhase string

const (
    LedgerPhaseIngesting   LedgerPhase = "INGESTING"
    LedgerPhaseWritingLFS  LedgerPhase = "WRITING_LFS"  // During transition
    LedgerPhaseImmutable   LedgerPhase = "IMMUTABLE"
)
```

### TxHash Phase Enum (reuses existing pattern)

```go
type TxHashPhase string

const (
    TxHashPhaseIngesting      TxHashPhase = "INGESTING"
    TxHashPhaseCompacting     TxHashPhase = "COMPACTING"
    TxHashPhaseBuildingRecSplit TxHashPhase = "BUILDING_RECSPLIT"
    TxHashPhaseVerifyingRecSplit TxHashPhase = "VERIFYING_RECSPLIT"
    TxHashPhaseComplete       TxHashPhase = "COMPLETE"
)
```

### Range ID Calculation

```go
// Range ID = floor((ledgerSeq - 2) / 10,000,000)
// Range 0: ledgers 2 - 10,000,001
// Range 1: ledgers 10,000,002 - 20,000,001
// Range 2: ledgers 20,000,002 - 30,000,001
// ...

func ledgerToRangeID(ledgerSeq uint32) uint32 {
    return (ledgerSeq - 2) / 10_000_000
}

func rangeFirstLedger(rangeID uint32) uint32 {
    if rangeID == 0 {
        return 2
    }
    return (rangeID * 10_000_000) + 2
}

func rangeLastLedger(rangeID uint32) uint32 {
    return ((rangeID + 1) * 10_000_000) + 1
}
```

---

## Proposed Directory Structure

```
/data/stellar-rpc/                        # Base data directory (configurable)
├── meta/                                 # Meta store (single RocksDB)
│   └── rocksdb/                          # RocksDB files
│
├── active/                               # Currently active stores
│   ├── ledger/                           # Active Ledger Store (RocksDB)
│   │   └── rocksdb/
│   └── txhash/                           # Active TxHash Store (RocksDB, 16 CFs)
│       └── rocksdb/
│
├── transitioning/                        # Previous range being transitioned (temporary)
│   ├── ledger/                           # Previous Ledger Store (still serving queries)
│   │   └── rocksdb/
│   └── txhash/                           # Previous TxHash Store (still serving queries)
│       └── rocksdb/
│
└── immutable/                            # Completed immutable stores
    ├── ledgers/                          # LFS ledger stores (adheres to existing format)
    │   ├── range-0/                      # Range 0: ledgers 2 - 10,000,001
    │   │   └── chunks/                   # LFS chunks (10K ledgers each)
    │   │       ├── 0000/
    │   │       │   ├── 000000.data
    │   │       │   ├── 000000.index
    │   │       │   ├── 000001.data
    │   │       │   └── ...
    │   │       └── 0001/
    │   │           └── ...
    │   ├── range-1/
    │   │   └── chunks/
    │   └── ...
    │
    └── txhash/                           # RecSplit indexes
        ├── range-0/
        │   └── index/
        │       ├── cf-0.idx
        │       ├── cf-1.idx
        │       └── ...cf-f.idx
        ├── range-1/
        │   └── index/
        └── ...
```

### LFS Chunk Path Convention (MUST ADHERE)

The LFS format from `local-fs/ingestion/lfs-ledger-ingestion.go` MUST be followed:

```go
// Directory: chunks/XXXX/YYYYYY.data and chunks/XXXX/YYYYYY.index
// Where: XXXX = chunkID / 1000, YYYYYY = chunkID

func getChunkDir(dataDir string, chunkID uint32) string {
    parentDir := chunkID / 1000
    return filepath.Join(dataDir, "chunks", fmt.Sprintf("%04d", parentDir))
}

func getDataPath(dataDir string, chunkID uint32) string {
    return filepath.Join(getChunkDir(dataDir, chunkID), fmt.Sprintf("%06d.data", chunkID))
}
```

---

## Meta Store Key Walkthrough: Scenarios

This section shows exactly how meta store keys are created, updated, and used for crash recovery and gap detection across different scenarios.

### Scenario 1: Fresh Backfill (Ledgers 2 to 30,000,001)

**Command**: `./stellar-rpc --backfill --start-ledger 2 --end-ledger 30000001`

**Initial State (t=0)**:
```
global:mode                          = "backfill"
global:backfill_start_ledger         = 2
global:backfill_end_ledger           = 30000001

range:0:state                        = "PENDING"
range:0:start_ledger                 = 2
range:0:end_ledger                   = 10000001

range:1:state                        = "PENDING"
range:1:start_ledger                 = 10000002
range:1:end_ledger                   = 20000001

range:2:state                        = "PENDING"
range:2:start_ledger                 = 20000002
range:2:end_ledger                   = 30000001
```

**During Ingestion of Range 0 (t=1)**:
```
range:0:state                        = "INGESTING"
range:0:ledger:phase                 = "INGESTING"
range:0:ledger:last_committed_ledger = 5000      # After first 5000 ledgers
range:0:ledger:count                 = 5000

range:0:txhash:phase                 = "INGESTING"
range:0:txhash:last_committed_ledger = 5000
range:0:txhash:cf_counts             = {"0": 312, "1": 298, ..., "f": 305}
```

**Checkpoint Update (every 1000 ledgers in backfill)**:
```
# After processing ledger 6000:
range:0:ledger:last_committed_ledger = 6000
range:0:ledger:count                 = 6000
range:0:txhash:last_committed_ledger = 6000
range:0:txhash:cf_counts             = {"0": 375, "1": 362, ..., "f": 368}
```

**Range 0 Ingestion Complete, Starting Transition (t=2)**:
```
range:0:state                        = "TRANSITIONING"
range:0:ledger:phase                 = "WRITING_LFS"
range:0:txhash:phase                 = "COMPACTING"
```

**Range 0 Fully Complete (t=3)**:
```
range:0:state                        = "COMPLETE"
range:0:completed_at                 = "2026-01-28T12:00:00Z"
range:0:ledger:phase                 = "IMMUTABLE"
range:0:ledger:immutable_path        = "/data/stellar-rpc/immutable/ledgers/range-0"
range:0:txhash:phase                 = "COMPLETE"
range:0:txhash:recsplit_path         = "/data/stellar-rpc/immutable/txhash/range-0"
```

### Scenario 2: Crash During Backfill and Recovery

**Situation**: Backfill running, crash occurs at ledger 7,500,000 (mid-range 0)

**State at Crash**:
```
range:0:state                        = "INGESTING"
range:0:ledger:last_committed_ledger = 7499000   # Last checkpoint
range:0:txhash:last_committed_ledger = 7499000
```

**Recovery Process**:
1. Operator runs: `./stellar-rpc --backfill --start-ledger 2 --end-ledger 30000001`
2. Code reads meta store, finds `range:0:state = "INGESTING"`
3. Code reads `range:0:ledger:last_committed_ledger = 7499000`
4. **Resume from ledger 7499001**
5. Continue until range completes

**Post-Recovery, Range 0 Complete**:
```
range:0:state                        = "COMPLETE"
range:0:ledger:phase                 = "IMMUTABLE"
range:0:txhash:phase                 = "COMPLETE"
```

### Scenario 3: Backfill Complete, Start Streaming Mode

**Pre-condition**: All ranges complete
```
range:0:state = "COMPLETE"
range:1:state = "COMPLETE"
range:2:state = "COMPLETE"
```

**Command**: `./stellar-rpc` (no --backfill flag)

**Startup Validation**:
1. Code scans all `range:*:state` keys
2. Verifies ALL are "COMPLETE"
3. Finds highest ledger: `range:2:end_ledger = 30000001`
4. Starts CaptiveStellarCore from ledger 30000002

**Switch to Streaming Mode**:
```
global:mode                          = "streaming"
global:last_processed_ledger         = 30000001

# New active range created:
range:3:state                        = "INGESTING"
range:3:start_ledger                 = 30000002
range:3:end_ledger                   = 40000001  # Expected end
```

### Scenario 4: Streaming Mode - 10M Boundary Transition

**Situation**: Streaming at ledger 39,999,999, about to hit 40,000,000

**Before Boundary (ledger 39,999,999)**:
```
global:last_processed_ledger         = 39999999
range:3:state                        = "INGESTING"
range:3:ledger:last_committed_ledger = 39999999
range:3:txhash:last_committed_ledger = 39999999
```

**Boundary Hit (ledger 40,000,000)**:
```
# Range 3 moves to transition:
range:3:state                        = "TRANSITIONING"
range:3:ledger:phase                 = "WRITING_LFS"
range:3:txhash:phase                 = "COMPACTING"

# New Range 4 starts:
range:4:state                        = "INGESTING"
range:4:start_ledger                 = 40000002
range:4:ledger:phase                 = "INGESTING"
range:4:txhash:phase                 = "INGESTING"

# Global pointer moves to new range:
global:last_processed_ledger         = 40000000
```

**Transition Complete for Range 3**:
```
range:3:state                        = "COMPLETE"
range:3:ledger:phase                 = "IMMUTABLE"
range:3:ledger:immutable_path        = "/data/stellar-rpc/immutable/ledgers/range-3"
range:3:txhash:phase                 = "COMPLETE"
range:3:txhash:recsplit_path         = "/data/stellar-rpc/immutable/txhash/range-3"
```

### Scenario 5: Gap Detection (Incomplete Backfill)

**Situation**: Backfill ran for ranges 0-2, but range 1 failed

**Meta Store State**:
```
range:0:state = "COMPLETE"
range:1:state = "INGESTING"    # INCOMPLETE!
range:1:ledger:last_committed_ledger = 15000000
range:2:state = "COMPLETE"
```

**Streaming Mode Startup Attempt**:
1. Code scans all `range:*:state` keys
2. Finds `range:1:state = "INGESTING"` (not COMPLETE)
3. **ABORT with error**:
   ```
   ERROR: Cannot start streaming mode - incomplete ranges detected:
     Range 1 (ledgers 10000002-20000001): state=INGESTING, last_committed=15000000
   
   Action required: Re-run backfill with --start-ledger 2 --end-ledger 30000001
   ```

### Scenario 6: Crash During Transition (Streaming Mode)

**Situation**: Streaming mode, transition in progress for range 3, crash occurs

**State at Crash**:
```
range:3:state                        = "TRANSITIONING"
range:3:ledger:phase                 = "WRITING_LFS"    # Incomplete
range:3:txhash:phase                 = "BUILDING_RECSPLIT"

range:4:state                        = "INGESTING"
range:4:ledger:last_committed_ledger = 40500000
```

**Recovery Process**:
1. Operator runs: `./stellar-rpc` (streaming mode)
2. Code finds `range:3:state = "TRANSITIONING"` (not COMPLETE)
3. Code finds `range:4:state = "INGESTING"` (active ingestion was happening)
4. **Resume both**:
   - Restart transition goroutine for range 3
   - Resume ingestion for range 4 from ledger 40500001

**Key Insight**: The "transitioning" RocksDB stores for range 3 are still on disk. The code reopens them and resumes the transition workflow (compact → recsplit → verify).

### Summary: Key Usage Patterns

| Key Pattern | Purpose | When Updated |
|-------------|---------|--------------|
| `global:mode` | Backfill vs Streaming | Process startup |
| `global:last_processed_ledger` | Streaming progress | After each ledger (streaming) |
| `range:{id}:state` | Overall range lifecycle | State transitions |
| `range:{id}:ledger:last_committed_ledger` | Crash recovery point | Every checkpoint (1000 ledgers backfill, 1 ledger streaming) |
| `range:{id}:ledger:phase` | Ledger sub-workflow state | Phase transitions |
| `range:{id}:txhash:cf_counts` | Accurate counts for RecSplit | Every checkpoint |
| `range:{id}:txhash:phase` | TxHash sub-workflow state | Phase transitions |
| `range:{id}:*:immutable_path` | Location of completed immutable stores | Transition complete |

---

## FAQs (Capturing Key Nuances)

### Q: Is checkpoint info different for backfill vs streaming?
**A: No.** Checkpoint information is mode-agnostic. It's just "crash recovery metadata" that tracks phase and progress. The same keys are used whether the range was created during backfill or streaming.

### Q: When is checkpoint info deleted?
**A: Never.** Checkpoint info for each 10M range persists forever in the meta store. This provides a complete audit trail of all processed ranges.

### Q: What happens if backfill crashes mid-range?
**A: Restart with same arguments.** The operator runs `--backfill --start-ledger X --end-ledger Y` again. The crash recovery component reads the meta store, finds the last checkpoint, and resumes from there.

### Q: Can the same process transition from backfill to streaming?
**A: No.** Backfill is a start-to-end process that exits when complete. Streaming is started separately without the `--backfill` flag. There is NO automatic pivot.

### Q: What if operator starts streaming mode before backfill completes?
**A: Code detects gap and aborts.** On streaming startup, the code validates ALL ranges in meta store have reached terminal successful state. If any range is incomplete, it logs details and exits.

### Q: How many Active Stores exist at once?
**A: During transition: 4 RocksDB instances.**
- 2 "Previously Active" stores (ledger + txhash) - serving queries during transition
- 2 "Currently Active" stores (ledger + txhash) - receiving new ingestion

### Q: Is there a background process for transition?
**A: No.** Transition runs in a goroutine, not a separate process. It's a mini-orchestrator within the main streaming process.

### Q: What's the batch size difference between backfill and streaming?
**A: Streaming batch size is 1.** After each ledger in streaming mode, the meta store is updated. This ensures minimal data loss on crash (<1 ledger).

### Q: How does query routing work during transition?
**A:** 
- Ledgers in new range → New Active Store
- Ledgers in previous range (transitioning) → Previous Active Store (still alive)
- Ledgers in older ranges → Immutable LFS + RecSplit

---

## Next Steps

1. ✅ All requirements clarified
2. ✅ Architecture decisions finalized
3. **Generate comprehensive design documents**:
   - Main design doc with complete architecture
   - Mermaid diagrams for all workflows
   - Glossary of terms
   - Ready for implementation
