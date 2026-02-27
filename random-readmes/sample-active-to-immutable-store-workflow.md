# Transaction Hash Index Architecture

## Design Document

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Architecture Overview](#2-architecture-overview)
3. [Data Structures](#3-data-structures)
4. [Runtime Operations](#4-runtime-operations)
5. [Startup and Recovery](#5-startup-and-recovery)
6. [Sizing and Performance](#6-sizing-and-performance)
7. [Operations](#7-operations)

**Appendices**

- [Appendix A: RocksDB Configuration](#appendix-a-rocksdb-configuration)
- [Appendix B: Detailed Code](#appendix-b-detailed-code)
- [Appendix C: Fallback Architecture — Hot Hash Table + StreamSplit](#appendix-c-fallback-architecture--hot-hash-table--streamsplit)

---

## 1. Introduction

### 1.1 Problem

Index all Stellar transaction hashes to support:

- **1,000 TPS** sustained write rate
- **50,000 RPS** query rate
- **16 GB RAM** constraint
- **10 ms P99** latency target
- **Full history** retention

### 1.2 Solution Summary

| Component | Role |
|-----------|------|
| RocksDB hot tier | Fast writes, ~500k ledgers retention |
| Sorted file + StreamSplit | Current period queryable data |
| StreamSplit archives | Compact cold storage per archive period |

### 1.3 Terminology

This document uses **ledger counts** rather than wall-clock time:

| Term | Definition |
|------|------------|
| **Ledger close time** | ~5 seconds (Stellar network average) |
| **Flush interval** | 500,000 ledgers (~29 days) |
| **Archive period** | 6,000,000 ledgers (~347 days, = 12 flush intervals) |
| **Current index** | Data from the current archive period (not yet archived) |
| **Period-end** | The 6M ledger boundary when the current index is archived |

### 1.4 Prerequisites

This document assumes familiarity with:

- **StreamSplit Specification** — MPHF construction, sorted input requirements, query semantics, integrity checking

### 1.5 Key Design Decisions

**Why RocksDB for hot tier:**
- Battle-tested library, minimal custom code
- Sorted iteration enables efficient flush (no custom sorting needed)
- Good write performance with WAL

**Why StreamSplit for cold storage:**
- Compact representation (see StreamSplit spec for sizing)
- O(1) query with single disk read
- Immutable after creation (simple operations)
- Built-in integrity checking (CRC32 per block, xxHash64 footer)

**Why separate hot tier and current index:**
- Hot tier stays small (~100 GB) for good cache efficiency
- Flush every 500k ledgers limits re-indexing scope if rebuild from Stellar is needed
- Period-end archival is instant (just rename files)

### 1.6 API Contract

**Primary Query Interface:**

```go
// Lookup returns the ledger sequence number for a transaction hash.
// Returns (ledgerSeq, true) if found, or (0, false) if not found.
//
// Thread Safety: Safe for concurrent use from multiple goroutines.
// Latency: P50 < 500µs, P99 < 2ms for transactions within 10 archive periods.
func Lookup(txHash [32]byte) (ledgerSeq uint32, found bool)
```

**Ingestion:** Handled internally by `ingestLoop()` which fetches ledgers from Stellar and calls `processBatch()`. See §4 for the goroutine model.

**Data Source:** The system ingests ledgers from a Stellar history archive or Captive Core instance. The `fetchLedgerFromStellar()` function abstracts this; implementation depends on deployment environment.

---

## 2. Architecture Overview

### 2.1 System Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         Query Path                               │
│  lookup() → queryHotTier() → indexRegistry.Query()              │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│   Hot Tier       │ │  Current Index   │ │    Archives      │
│   (RocksDB)      │ │  (StreamSplit)   │ │  (StreamSplit)   │
│                  │ │                  │ │                  │
│ cf_data_0 ←───┐  │ │ index_*_*.ssp   │ │ 50000000.ssp    │
│ cf_data_1 ←─┐ │  │ │ transactions_*  │ │ 56300000.ssp    │
│ cf_meta     │ │  │ │   _*.sorted     │ │ ...              │
│   (active)──┘ │  │ └──────────────────┘ │ history.ssp     │
│               │  │          ▲           └──────────────────┘
│    Flush ─────┘  │          │                   ▲
└──────────────────┘          │                   │
                              │ Period-End          │
                              └───────────────────┘
```

### 2.2 Storage Layout

```
/data/
├── hot/                            # Single RocksDB with 3 column families
│   ├── OPTIONS-*
│   ├── MANIFEST-*
│   ├── *.sst                       # SST files (shared by all CFs)
│   └── *.log                       # WAL (shared by all CFs)
├── current_index/
│   ├── transactions_6500000_a3f2b1c9.sorted  # Sorted file (named by maxLedger + random suffix)
│   └── index_6500000_a3f2b1c9.ssp            # StreamSplit index (matching name)
├── archives/
│   ├── history.ssp                 # Pre-built archive (see note below)
│   ├── 50000000.ssp                # Archives named by base ledger
│   ├── 56000000.ssp                # (must be multiple of 6M)
│   └── ...
└── temp/                           # Working directory for flush
```

**Directory constants (used throughout code examples):**

```go
const (
    DataDir         = "/data"
    HotDir          = "/data/hot"
    TempDir         = "/data/temp"
    CurrentIndexDir = "/data/current_index"
    ArchivesDir     = "/data/archives"
)
```

**Filesystem Requirement:** The `temp/`, `current_index/`, and `archives/` directories MUST be on the same filesystem. The flush process uses `rename()` to atomically move files from `temp/` to their final locations.

**history.ssp:** An optional pre-built StreamSplit index covering ledger history before this system was deployed. Created offline using historical data from Stellar archives. If present, it must be registered in `index_registry` at first startup. This allows the system to answer queries for old transactions without re-indexing all of history.

### 2.3 Data Flow Summary

| Stage | Data | Retention |
|-------|------|-----------|
| Hot tier | Recent transactions | ~500k ledgers |
| Current index | Current period transactions | Until 6M ledger boundary |
| Archives | Historical periods | Forever |

**Write flow:** Ledger → `processBatch()` → RocksDB hot tier → `runFlushPhase()` → current index → `runArchivePhase()` → archives

**Query flow:** `lookup()` → `queryHotTier()` → `indexRegistry.Query()` (current index, then archives newest-first)

---

## 3. Data Structures

### 3.1 RocksDB Column Families

The hot tier uses a single RocksDB database with three column families:

| Column Family | Purpose |
|---------------|---------|
| `cf_data_0` | Data column family (active or inactive) |
| `cf_data_1` | Data column family (the other one) |
| `cf_meta` | Stores operational state and index registry |

The two data column families alternate roles. Only one is active at a time; the other is either empty or being drained during flush.

```go
var (
    db      *rocksdb.DB
    cfData  [2]*rocksdb.ColumnFamily  // cf_data_0, cf_data_1
    cfMeta  *rocksdb.ColumnFamily     // cf_meta
)

const (
    CFNameData0 = "cf_data_0"
    CFNameData1 = "cf_data_1"
    CFNameMeta  = "cf_meta"
)
```

See Appendix A for RocksDB configuration.

### 3.2 Data Column Family Schema

| Field | Size | Description |
|-------|------|-------------|
| Key | 32 bytes | Transaction hash |
| Value | 4 bytes | Ledger sequence number (little-endian) |

**Sizing (per flush interval = 500k ledgers ≈ 29 days):**

| Metric | Value |
|--------|-------|
| Transactions per flush interval | ~2.63 B (at 1,000 TPS design target) |
| Key + value size | 36 bytes |
| Raw data | ~95 GB |
| With compression (LZ4) | ~70-80 GB |
| With LSM overhead | ~100 GB |

### 3.3 Meta Column Family Schema (cf_meta)

| Key | Value | Description |
|-----|-------|-------------|
| `"active"` | 1 byte | Active data CF index: `0x00` or `0x01` |
| `"current_ledger"` | 8 bytes | Current ledger sequence (little-endian uint64) |
| `"flush_task"` | protobuf bytes (optional) | Pending flush task state (see §4.4) |
| `"archive_task"` | protobuf bytes (optional) | Pending archive task state (see §4.5) |
| `"index_registry"` | protobuf bytes | Index file registry |

**Index Registry Schema:**

```protobuf
message IndexRegistry {
    optional CurrentIndex current_index = 1;
    repeated Archive archives = 2;  // Ordered newest-first by base_ledger
}

message CurrentIndex {
    string index_path = 1;
    string sorted_path = 2;
    uint32 base_ledger = 3;
    uint32 max_ledger = 4;
}

message Archive {
    string index_path = 1;
    uint32 base_ledger = 2;
}
```

The entire registry is read and written as a single atomic blob. This simplifies consistency — no partial states possible.

### 3.4 Current Index Files

**transactions_LEDGER_SUFFIX.sorted** — Sorted file containing all transactions for the current index (excluding hot tier). Each flush creates a new file with a unique name (`maxLedger` + random 8-char hex suffix).

| Field | Size |
|-------|------|
| Transaction hash | 32 bytes |
| Ledger sequence | 4 bytes (absolute) |
| **Entry size** | **36 bytes** |

Entries are sorted by transaction hash (lexicographic order).

**File format with checksum footer:**

```
┌─────────────────────────────────────┐
│ Entry 0 (36 bytes)                  │
│   [32 bytes: transaction hash]      │
│   [4 bytes: ledger sequence]        │
├─────────────────────────────────────┤
│ Entry 1 ... Entry N-1               │
├─────────────────────────────────────┤
│ Footer (48 bytes)                   │
│   [8 bytes: magic 0x5354584E494458] │
│   [4 bytes: version]                │
│   [4 bytes: entry count]            │
│   [32 bytes: SHA256 of entries]     │
└─────────────────────────────────────┘
```

**index_LEDGER_SUFFIX.ssp** — StreamSplit index built from the sorted file for O(1) queries. Uses the same naming suffix as the corresponding sorted file.

| Parameter | Value |
|-----------|-------|
| Payload size | 4 bytes (ledger offset from base_ledger) |
| Fingerprint size | 2 bytes |

*Payload stores `ledgerSeq - baseLedger` as uint32. Max value is ~6M (one archive period), which fits in 24 bits, but 4 bytes simplifies implementation.*

### 3.5 Archive Files

Archives are named deterministically by base ledger (e.g., `50000000.ssp`). Each archive is a standalone StreamSplit index covering one archive period (6M ledgers) of transactions.

---

## 4. Runtime Operations

This section describes all concurrent operations. Understanding concurrency requires seeing:
1. What locks exist and what they protect
2. How each goroutine acquires/releases locks
3. The invariants that ensure correctness

### 4.1 Concurrency Model

**Locks:**

```go
var (
    dbLock        sync.RWMutex   // Protects cfData handles during drop/recreate
    indexRegistry IndexRegistry  // Has its own internal RWMutex
)
```

| Lock | Protects | Held By |
|------|----------|---------|
| `dbLock` | `cfData[0]`, `cfData[1]` handles | `queryHotTier` (RLock), `processBatch` (RLock), `switchActiveCF` (Lock), `dropAndRecreateCF` (Lock) |
| `indexRegistry.mu` | `indexes` slice | `indexRegistry.Query` (RLock), `indexRegistry.Reload` (Lock) |

**Timing constants:**

```go
const (
    FlushIntervalLedgers   = 500_000    // ~29 days at 5 sec/ledger
    ArchivePeriodLedgers   = 6_000_000  // ~347 days (12 flush intervals)
)
```

**Goroutines:**

| Goroutine | Runs | Functions Called |
|-----------|------|------------------|
| Ingest (1) | Forever | `ingestLoop()` → `processBatch()`, `switchActiveCF()` |
| Task Worker (1) | Forever | `taskWorker()` → `runFlushPhase()`, `runArchivePhase()` |
| Query (N) | Per request | `lookup()` → `queryHotTier()`, `indexRegistry.Query()` |

**Invariants:**
1. Only one goroutine calls `processBatch` and `switchActiveCF` (the ingest loop)
2. Only one goroutine calls `runFlushPhase` and `runArchivePhase` (the task worker)
3. `queryHotTier` holds `dbLock.RLock` for its entire duration
4. `indexRegistry.Query` holds its own `RLock` separately
5. CF switch happens synchronously in ingest loop while holding `dbLock.Lock`
6. At most one `flush_task` and one `archive_task` exist at any time (enforced by checks in `switchActiveCF` and CLEANUP phase)

### 4.2 Query Path

Queries check the hot tier first (most recent data), then the cold tier (current index + archives).

```go
func lookup(txHash [32]byte) (uint32, bool) {
    // Check hot tier (RocksDB column families)
    if ledgerSeq, found := queryHotTier(txHash); found {
        return ledgerSeq, true
    }
    
    // Check cold tier (StreamSplit indexes: current index + archives)
    if result, found := indexRegistry.Query(txHash[:]); found {
        ledgerSeq := decodeLedgerSeqFromPayload(result.Payload, result.Metadata)
        return ledgerSeq, true
    }
    
    return 0, false
}

// queryHotTier checks both data column families for the transaction.
// Returns immediately on first match. No need to know which CF is active —
// a transaction exists in at most one CF.
func queryHotTier(txHash [32]byte) (uint32, bool) {
    dbLock.RLock()
    defer dbLock.RUnlock()
    
    for _, cf := range cfData {
        result, err := db.GetCF(readOpts, cf, txHash[:])
        if err == nil && result != nil {
            ledgerSeq := decodeLedgerSeq(result)
            result.Free()
            return ledgerSeq, true
        }
    }
    return 0, false
}
```

**IndexRegistry** manages all StreamSplit indexes (current index + archives):

```go
type IndexRegistry struct {
    mu      sync.RWMutex
    indexes []*StreamSplitIndex  // Ordered newest-first
}

func (r *IndexRegistry) Query(hash []byte) (Result, bool) {
    r.mu.RLock()
    defer r.mu.RUnlock()
    for _, idx := range r.indexes {
        if result, found := idx.Lookup(hash); found {
            return result, true
        }
    }
    return Result{}, false
}
```

`Reload()` reads the registry from cf_meta, diffs against current handles, opens new indexes, closes removed ones, and atomically swaps the slice under lock. See Appendix B for implementation.

### 4.3 Write Path

The ingest loop fetches ledgers from Stellar and writes transactions to the active CF:

```go
func ingestLoop(startLedger int64) {
    for ledgerSeq := startLedger; ; ledgerSeq++ {
        ledger := fetchLedgerFromStellar(ledgerSeq)
        processBatch(ledger)
        
        // Check flush trigger
        if ledgerSeq % FlushIntervalLedgers == 0 {
            isPeriodEnd := (ledgerSeq % ArchivePeriodLedgers == 0)
            switchActiveCF(isPeriodEnd)
            taskTrigger <- struct{}{}  // Signal task worker
        }
    }
}

func processBatch(ledger Ledger) {
    // Read active CF index from cf_meta (single source of truth)
    activeIdx := readActiveIndex()
    
    dbLock.RLock()
    defer dbLock.RUnlock()
    
    batch := rocksdb.NewWriteBatch()
    for _, tx := range ledger.Transactions {
        batch.PutCF(cfData[activeIdx], tx.Hash[:], encodeLedgerSeq(ledger.Sequence))
    }
    
    // Update current_ledger atomically with transaction data
    batch.PutCF(cfMeta, []byte("current_ledger"), encodeUint64(ledger.Sequence))
    
    db.Write(writeOpts, batch)
    batch.Destroy()
}
```

**Write throughput:** At 1,000 TPS with 36-byte entries, this is ~35 KB/s sustained write rate — well within RocksDB capabilities.

**Timing assumption:** Flush must complete before the next flush trigger (500k ledgers). At ~5 seconds/ledger, this is ~29 days. Worst-case flush is ~15 minutes. If flush cannot keep up (detected by `switchActiveCF()` finding an existing `flush_task`), the system halts — this indicates a serious problem requiring investigation.

### 4.4 Flush Path (Task-Based)

Flush uses a persisted task with explicit phases. Each phase transition is saved to cf_meta, enabling trivial crash recovery — just resume from the current phase.

**Task State Machine:**

```
ITERATE → MERGE → BUILD → RENAME → COMMIT → CLEANUP → (done)
                                              ↓
                                        [if period-end]
                                              ↓
                                     Archive Task Created
```

| Phase | Description |
|-------|-------------|
| ITERATE | Dump frozen CF to sorted temp file (RocksDB iteration is already sorted) |
| MERGE | Merge temp file with existing sorted file, deduplicating by txHash |
| BUILD | Build StreamSplit index from merged sorted file |
| RENAME | Atomically move temp files to `current_index/` directory |
| COMMIT | Update `index_registry` in cf_meta to point to new files |
| CLEANUP | Delete old files and drop the frozen CF |

**Flush Task Schema:**

```protobuf
message FlushTask {
    oneof phase {
        FlushIterate iterate = 1;
        FlushMerge merge = 2;
        FlushBuild build = 3;
        FlushRename rename = 4;
        FlushCommit commit = 5;
        FlushCleanup cleanup = 6;
    }
}
// Each phase message carries cf_idx, trigger_archive, plus accumulated 
// paths from previous phases. See Appendix B for full schema.
```

**Task creation (synchronous, in ingest loop):**

```go
// switchActiveCF atomically switches the active CF and creates a flush task.
// Called from ingestLoop when flush trigger is reached.
// INVARIANT: No flush_task may exist when this is called.
func switchActiveCF(isPeriodEnd bool) {
    dbLock.Lock()
    defer dbLock.Unlock()
    
    // Verify no task in progress (invariant violation = bug)
    if readFlushTask() != nil {
        alertAndHalt("BUG: switchActiveCF called while flush_task exists")
    }
    
    oldIdx := readActiveIndex()
    newIdx := 1 - oldIdx
    
    // Create flush task starting at ITERATE phase
    task := &FlushTask{
        Phase: &FlushTask_Iterate{
            Iterate: &FlushIterate{
                CfIdx:          int32(oldIdx),
                TriggerArchive: isPeriodEnd,
            },
        },
    }
    
    batch := rocksdb.NewWriteBatch()
    batch.PutCF(cfMeta, []byte("active"), []byte{byte(newIdx)})
    batch.PutCF(cfMeta, []byte("flush_task"), proto.Marshal(task))
    db.Write(writeOptsSync, batch)
    batch.Destroy()
    
    log.Info("CF_SWITCH", "old", oldIdx, "new", newIdx, "trigger_archive", isPeriodEnd)
}
```

**Task execution (asynchronous, in task worker):**

```go
func taskWorker() {
    for range taskTrigger {
        for task := readFlushTask(); task != nil; task = readFlushTask() {
            runFlushPhase(task)
        }
        for task := readArchiveTask(); task != nil; task = readArchiveTask() {
            runArchivePhase(task)
        }
    }
}
```

**Phase execution pattern** (ITERATE shown as example):

```go
func runFlushPhase(task *FlushTask) {
    switch p := task.Phase.(type) {
    case *FlushTask_Iterate:
        phase := p.Iterate
        
        // Do work
        tempSorted := "/data/temp/cf_dump.sorted"
        iterateCFWithChecksum(cfData[phase.CfIdx], tempSorted)
        
        // Persist next phase, then return (loop will call us again)
        saveFlushTask(&FlushTask{Phase: &FlushTask_Merge{...}})
        
    case *FlushTask_Commit:
        // COMMIT is special: updates registry atomically with phase transition
        batch := rocksdb.NewWriteBatch()
        batch.PutCF(cfMeta, []byte("index_registry"), proto.Marshal(newRegistry))
        batch.PutCF(cfMeta, []byte("flush_task"), proto.Marshal(cleanupTask))
        db.Write(writeOptsSync, batch)
        
        indexRegistry.Reload()  // Make new files queryable
        
    // ... other phases follow same pattern
    }
}
```

All phases follow the pattern: (1) do idempotent work, (2) persist next phase, (3) return. The COMMIT phase is notable because it atomically updates the registry and phase in one write batch. See Appendix B for complete implementation.

Each phase is idempotent — see §4.8 for the patterns used.

### 4.5 Period-End Path (Task-Based)

At the 6M ledger boundary, an archive task moves the current index to archives.

**Task State Machine:**

```
COPY → COMMIT → CLEANUP → (done)
```

| Phase | Description |
|-------|-------------|
| COPY | Copy current index `.ssp` file to `archives/` with deterministic name |
| COMMIT | Update `index_registry`: add new archive, clear `current_index` |
| CLEANUP | Delete old current index files (`.ssp` and `.sorted`) |

Archive phases follow the same pattern as flush phases. See Appendix B for implementation.

### 4.6 Column Family Cleanup

```go
func dropAndRecreateCF(idx int) {
    dbLock.Lock()
    defer dbLock.Unlock()
    
    db.DropColumnFamily(cfData[idx])
    cfData[idx] = db.CreateColumnFamily(cfDataOpts, cfNames[idx])
}
```

**Idempotency note:** `dropAndRecreateCF` is called only when `cf_cleared == false`. After it succeeds, `cf_cleared` is set to `true` and persisted. If crash occurs between drop and create, startup's `GetOrCreateColumnFamily` creates the missing CF, and CLEANUP re-runs with `cf_cleared == false`, calling `dropAndRecreateCF` again (which is safe on an empty CF).

**Concurrency during flush:**
- Ingestion continues to new CF (no blocking)
- Queries continue checking both CFs (`queryHotTier` holds `dbLock.RLock`)
- Old CF is frozen — iteration is safe without locks (no concurrent writes, drop happens later)
- Task state persisted to cf_meta after each phase
- Brief `indexRegistry.mu.Lock` during `Reload()` (~microseconds)
- Brief `dbLock.Lock` during `dropAndRecreateCF()` (~2ms)

**Synchronization guarantee:** `switchActiveCF()` acquires `Lock`, which blocks until all in-flight `processBatch()` and `queryHotTier()` calls (holding `RLock`) complete. After the switch, all subsequent writes go to the new CF. The old CF is "frozen".

### 4.7 Lock Acquisition Summary

This table shows which locks each operation acquires:

| Operation | `dbLock` | `indexRegistry.mu` | Duration | Notes |
|-----------|----------|-------------------|----------|-------|
| `queryHotTier()` | RLock | — | ~5-100 µs | Checks both CFs |
| `indexRegistry.Query()` | — | RLock | ~100-150 µs | Iterates indexes |
| `processBatch()` | RLock | — | ~1-5 ms | Writes to active CF |
| `switchActiveCF()` | **Lock** | — | ~2 ms | Blocks until RLocks release |
| `runFlushPhase()` | — | — | ~minutes | No lock for slow I/O |
| `runArchivePhase()` | — | — | ~seconds | No lock for slow I/O |
| `indexRegistry.Reload()` | — | **Lock** | ~1 ms | Called by COMMIT phases |
| `dropAndRecreateCF()` | **Lock** | — | ~2 ms | Called by CLEANUP phase |

**Key observations:**
1. Queries and writes use RLock — they run concurrently
2. CF switch uses Lock — brief pause (~2ms) while in-flight RLocks complete
3. Task phases do slow I/O without any lock — no blocking
4. Lock acquired only twice per flush, briefly: during Reload (~1ms) and drop/recreate (~2ms)

### 4.8 Correctness Invariants

The system maintains these invariants:

| Invariant | What It Ensures |
|-----------|-----------------|
| **Query Totality** | `lookup()` always terminates (no deadlocks, no use-after-free) |
| **Query Completeness** | Every committed transaction is findable |
| **Data Integrity** | Returned results match what was written; corruption is detected |
| **File Consistency** | Files on disk match `index_registry`; no orphans |
| **Task Exclusivity** | At most one flush task and one archive task exist at any time |
| **Phase Idempotency** | Every task phase produces the same result if re-run |

**Key correctness mechanisms:**

- **Locking:** `dbLock` protects CF handles; `indexRegistry.mu` protects index handles. RLock for queries, Lock for modifications.
- **Ordering:** Files created before registry update; registry updated before old files deleted; `Reload()` completes before `dropAndRecreateCF()`.
- **Checksums:** RocksDB internal checksums, sorted file SHA256 footer, StreamSplit CRC32/xxHash64.
- **Both CFs checked:** `queryHotTier()` checks both CFs, ensuring no data loss during flush.

**Phase idempotency patterns:**

| Pattern | Phases | Mechanism |
|---------|--------|-----------|
| Overwrite fixed path | ITERATE, MERGE, BUILD | Write to `/data/temp/*`, deterministic output |
| Persist before action | RENAME | Save final paths before rename, use `exists()` guards |
| Capture in previous phase | RENAME→COMMIT, COPY→COMMIT | Store old paths in phase message before registry changes |
| Check before modify | ARCHIVE_COMMIT | Check if archive already in list before prepending |
| Skip if done | COPY, CLEANUP | `if !exists()` guard, `cf_cleared` flag |

---

## 5. Startup and Recovery

### 5.1 Startup Sequence

| Step | Action |
|------|--------|
| 1 | Open RocksDB, get or create all column families |
| 2 | Initialize `cf_meta` if first startup (set `active=0`, `current_ledger=0`) |
| 3 | Read `current_ledger` and check for pending tasks |
| 4 | Load index registry via `indexRegistry.Reload()` |
| 5 | Start ingest goroutine from `current_ledger + 1` |
| 6 | Resume any pending flush/archive tasks to completion |
| 7 | Start task worker for normal operation |

**Key points:**
- `GetOrCreateColumnFamily` handles crash between CF drop and create
- Pending tasks are resumed idempotently before normal operation begins
- Queries work immediately because `lookup()` checks both CFs

See Appendix B for full implementation.

### 5.2 Recovery

**Recovery is trivial:** On startup, if a task exists in cf_meta, just call `runFlushPhase()` or `runArchivePhase()`. Phase idempotency (see §4.8) ensures re-running any phase produces the correct result.

| Task State at Startup | Action |
|----------------------|--------|
| No tasks | Normal startup |
| Any `FlushTask` phase | Re-run that phase via `runFlushPhase()` |
| Any `ArchiveTask` phase | Re-run that phase via `runArchivePhase()` |

**Why no orphan cleanup is needed:** Each CLEANUP phase message contains the exact paths to delete (`old_index_path`, `old_sorted_path`, `temp_sorted`). If crash occurs before CLEANUP completes, re-running the phase deletes those files.

---

## 6. Sizing and Performance

### 6.1 Scale Parameters

| Metric | Value |
|--------|-------|
| Ledger period (archive cycle) | 6 M ledgers |
| Transactions per ledger | ~5,000 (at 1,000 TPS × 5 sec/ledger) |
| Transactions per 6M ledgers | ~30 B |
| Bytes per transaction (hot) | 36 bytes |
| Current index sorted file | ~1.1 TB |
| Current index | ~200 GB |
| Archive per 6M ledgers | ~200 GB |

### 6.2 Flush Configuration

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `FlushIntervalLedgers` | 500,000 | Limits re-indexing scope on corruption |
| `ArchivePeriodLedgers` | 6,000,000 | Creates deterministic archive boundaries |

**Constraint:** `ArchivePeriodLedgers` must be an exact multiple of `FlushIntervalLedgers`.

### 6.3 Hardware Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| RAM | 16 GB | 32 GB |
| NVMe | 4 TB | 8 TB |
| CPU | 4 cores | 8 cores |

**Memory breakdown (userspace):**

| Component | Derivation | Size |
|-----------|------------|------|
| RocksDB memtables | 64 MB × 3 buffers × 2 CFs | ~400 MB |
| RocksDB block cache | Configured (Appendix A) | 512 MB |
| StreamSplit RAM indexes | 16 bytes × 2^(P-B) per index; ~134 MB per 30B-key index × 11 indexes | ~1.5 GB |
| Flush working set | Merge buffers, StreamSplit build (~50 MB per spec) | ~50 MB peak |
| **Total userspace** | | **~2.5 GB** |

*StreamSplit RAM index calculation: P = log₂(N/3.6), B = 10 for small payloads. At 30B keys: P=33, R=23, NumBlocks=8.4M, RAM=134 MB per index. See StreamSplit spec §5.3.*

**Memory (OS page cache):** RocksDB SST files and StreamSplit blocks are read via mmap or pread. The 16 GB minimum allows ~13 GB for page cache, improving query latency for hot data.

**Storage Budget (10 archive periods / 60M ledgers):**

| Component | Size |
|-----------|------|
| Hot tier | ~100 GB |
| Current index | ~1.34 TB |
| Archives (10 periods) | ~2 TB |
| **Total** | **~3.5 TB** |

### 6.4 Performance

**Latency:**

| Operation | P50 | P99 |
|-----------|-----|-----|
| Hot tier lookup | ~5 µs | ~100 µs |
| Current index lookup | ~150 µs | ~250 µs |
| Archive lookup (per period) | ~100 µs | ~150 µs |
| Full lookup (10 periods) | ~1 ms | ~2 ms |

**Throughput:**

| Operation | Rate |
|-----------|------|
| Writes | 1,000 TPS sustained |
| Queries | 50,000 RPS |
| Flush | ~15 min worst case (end of archive period) |

---

## 7. Operations

### 7.1 Monitoring

**Metrics to expose (Prometheus format):**

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `txindex_query_duration_seconds` | Histogram | `tier` | Query latency by tier (hot/current/archive) |
| `txindex_queries_total` | Counter | `result` | Query count by result (found/not_found) |
| `txindex_writes_total` | Counter | — | Transactions indexed |
| `txindex_current_ledger` | Gauge | — | Most recent ledger indexed |
| `txindex_flush_duration_seconds` | Histogram | `phase` | Flush phase duration |
| `txindex_hot_tier_bytes` | Gauge | — | Hot tier size |
| `txindex_disk_free_bytes` | Gauge | — | Available disk space |

**Alert thresholds:**

| Metric | Alert Threshold | Severity | Description |
|--------|-----------------|----------|-------------|
| Query P99 | > 8 ms | Warning | Approaching SLA |
| Query P99 | > 10 ms | Critical | SLA breach |
| Hot tier size | > 130 GB | Warning | Flush overdue |
| Compaction pending bytes | > 10 GB | Warning | RocksDB falling behind |
| Disk free space | < 2 TB | Warning | May impact flush |
| Disk free space | < 500 GB | Critical | Cannot complete flush |
| Flush duration | > 20 min | Warning | Flush taking too long |

### 7.2 Health Checks

| Endpoint | Checks | Failure Action |
|----------|--------|----------------|
| `GET /health` | Process is running | Restart container |
| `GET /ready` | RocksDB accessible, registry files exist, disk > 500 GB free | Remove from load balancer |

### 7.3 Graceful Shutdown

On SIGTERM:
1. Stop accepting new queries
2. Wait for in-flight queries to complete
3. Let current task phase complete (do not interrupt mid-phase)
4. Close RocksDB and index handles
5. Exit

### 7.4 Error Handling

| Category | Strategy | Recovery |
|----------|----------|----------|
| Network errors (Stellar) | Retry with exponential backoff (max 5 min) | Automatic |
| Invalid transaction data | Log error, skip entry, continue | Manual review |
| Disk full | Halt ingestion, alert | Add disk or delete old archives |
| RocksDB corruption | Halt, alert | Rebuild from checkpoint or Stellar |
| Index file corruption | Halt, alert | Rebuild from Stellar |
| Checksum mismatch | Halt, alert | Rebuild from Stellar |

### 7.5 Backup

| Component | Method |
|-----------|--------|
| Hot tier | RocksDB checkpoint API (consistent snapshot without blocking writes) |
| Current index | Copy files listed in `index_registry` |
| Archives | Copy directly (immutable) |

---

## Appendix A: RocksDB Configuration

```go
dbOpts := rocksdb.NewOptions()
dbOpts.SetCreateIfMissing(true)
dbOpts.SetCreateMissingColumnFamilies(true)

// Optimize for write-heavy workload
dbOpts.SetWriteBufferSize(64 * 1024 * 1024)      // 64 MB memtable
dbOpts.SetMaxWriteBufferNumber(3)
dbOpts.SetTargetFileSizeBase(64 * 1024 * 1024)   // 64 MB SST files
dbOpts.SetMaxBytesForLevelBase(256 * 1024 * 1024)
dbOpts.SetCompression(rocksdb.LZ4Compression)

// Enable bloom filters for fast negative lookups
blockOpts := rocksdb.NewBlockBasedTableOptions()
blockOpts.SetFilterPolicy(rocksdb.NewBloomFilterPolicy(10))
blockOpts.SetBlockCache(rocksdb.NewLRUCache(512 * 1024 * 1024))  // 512 MB
dbOpts.SetBlockBasedTableFactory(blockOpts)
```

---

## Appendix B: Detailed Code

This appendix contains complete implementations referenced from the main document.

### B.1 Flush Task Schema

```protobuf
message FlushTask {
    oneof phase {
        FlushIterate iterate = 1;
        FlushMerge merge = 2;
        FlushBuild build = 3;
        FlushRename rename = 4;
        FlushCommit commit = 5;
        FlushCleanup cleanup = 6;
    }
}

message FlushIterate {
    int32 cf_idx = 1;
    bool trigger_archive = 2;
}

message FlushMerge {
    int32 cf_idx = 1;
    bool trigger_archive = 2;
    string temp_sorted = 3;
}

message FlushBuild {
    int32 cf_idx = 1;
    bool trigger_archive = 2;
    string temp_sorted = 3;
    string merged_path = 4;
}

message FlushRename {
    int32 cf_idx = 1;
    bool trigger_archive = 2;
    string temp_sorted = 3;
    string merged_path = 4;
    string index_path = 5;
    uint32 base_ledger = 6;
    uint32 max_ledger = 7;
    string final_sorted = 8;
    string final_index = 9;
}

message FlushCommit {
    int32 cf_idx = 1;
    bool trigger_archive = 2;
    string temp_sorted = 3;
    string final_sorted = 4;
    string final_index = 5;
    uint32 base_ledger = 6;
    uint32 max_ledger = 7;
    string old_index_path = 8;
    string old_sorted_path = 9;
}

message FlushCleanup {
    int32 cf_idx = 1;
    bool trigger_archive = 2;
    string temp_sorted = 3;
    string old_index_path = 4;
    string old_sorted_path = 5;
    bool cf_cleared = 6;
}
```

### B.2 Archive Task Schema

```protobuf
message ArchiveTask {
    oneof phase {
        ArchiveCopy copy = 1;
        ArchiveCommit commit = 2;
        ArchiveCleanup cleanup = 3;
    }
}

message ArchiveCopy {
    // No fields — reads CurrentIndex from registry
}

message ArchiveCommit {
    string archive_path = 1;
    string old_index_path = 2;
    string old_sorted_path = 3;
    uint32 base_ledger = 4;
}

message ArchiveCleanup {
    string archive_path = 1;
    string old_index_path = 2;
    string old_sorted_path = 3;
}
```

### B.3 IndexRegistry.Reload

```go
func (r *IndexRegistry) Reload() {
    state := readIndexRegistry()
    paths := state.AllIndexPaths()
    
    r.mu.RLock()
    existingByPath := make(map[string]*StreamSplitIndex)
    for _, idx := range r.indexes {
        existingByPath[idx.Path] = idx
    }
    r.mu.RUnlock()
    
    newIndexes := make([]*StreamSplitIndex, 0, len(paths))
    for _, path := range paths {
        if existing, ok := existingByPath[path]; ok {
            newIndexes = append(newIndexes, existing)
            delete(existingByPath, path)
        } else {
            idx, _ := openStreamSplitIndex(path)
            newIndexes = append(newIndexes, idx)
        }
    }
    
    r.mu.Lock()
    r.indexes = newIndexes
    r.mu.Unlock()
    
    for _, old := range existingByPath {
        old.Close()
    }
}
```

### B.4 runFlushPhase (Complete)

```go
func runFlushPhase(task *FlushTask) {
    switch p := task.Phase.(type) {
    
    case *FlushTask_Iterate:
        phase := p.Iterate
        tempSorted := "/data/temp/cf_dump.sorted"
        iterateCFWithChecksum(cfData[phase.CfIdx], tempSorted)
        saveFlushTask(&FlushTask{Phase: &FlushTask_Merge{Merge: &FlushMerge{
            CfIdx: phase.CfIdx, TriggerArchive: phase.TriggerArchive,
            TempSorted: tempSorted,
        }}})
        
    case *FlushTask_Merge:
        phase := p.Merge
        state := readIndexRegistry()
        oldSortedPath := ""
        if state.CurrentIndex != nil {
            oldSortedPath = state.CurrentIndex.SortedPath
        }
        mergedPath := "/data/temp/merged.sorted"
        mergeWithDedup(oldSortedPath, phase.TempSorted, mergedPath)
        saveFlushTask(&FlushTask{Phase: &FlushTask_Build{Build: &FlushBuild{
            CfIdx: phase.CfIdx, TriggerArchive: phase.TriggerArchive,
            TempSorted: phase.TempSorted, MergedPath: mergedPath,
        }}})
        
    case *FlushTask_Build:
        phase := p.Build
        indexPath := "/data/temp/index.ssp"
        baseLedger, maxLedger := getLedgerRange(phase.MergedPath)
        buildStreamSplit(phase.MergedPath, indexPath, baseLedger, maxLedger)
        saveFlushTask(&FlushTask{Phase: &FlushTask_Rename{Rename: &FlushRename{
            CfIdx: phase.CfIdx, TriggerArchive: phase.TriggerArchive,
            TempSorted: phase.TempSorted, MergedPath: phase.MergedPath,
            IndexPath: indexPath, BaseLedger: baseLedger, MaxLedger: maxLedger,
        }}})
        
    case *FlushTask_Rename:
        phase := p.Rename
        finalSorted, finalIndex := phase.FinalSorted, phase.FinalIndex
        if finalSorted == "" {
            suffix := randomHexSuffix(4)
            finalSorted = fmt.Sprintf("/data/current_index/transactions_%d_%s.sorted", phase.MaxLedger, suffix)
            finalIndex = fmt.Sprintf("/data/current_index/index_%d_%s.ssp", phase.MaxLedger, suffix)
            phase.FinalSorted, phase.FinalIndex = finalSorted, finalIndex
            saveFlushTask(&FlushTask{Phase: &FlushTask_Rename{Rename: phase}})
        }
        if exists(phase.MergedPath) { rename(phase.MergedPath, finalSorted) }
        if exists(phase.IndexPath) { rename(phase.IndexPath, finalIndex) }
        fsync("/data/current_index/")
        
        state := readIndexRegistry()
        oldIndexPath, oldSortedPath := "", ""
        if state.CurrentIndex != nil {
            oldIndexPath, oldSortedPath = state.CurrentIndex.IndexPath, state.CurrentIndex.SortedPath
        }
        saveFlushTask(&FlushTask{Phase: &FlushTask_Commit{Commit: &FlushCommit{
            CfIdx: phase.CfIdx, TriggerArchive: phase.TriggerArchive,
            TempSorted: phase.TempSorted, FinalSorted: finalSorted, FinalIndex: finalIndex,
            BaseLedger: phase.BaseLedger, MaxLedger: phase.MaxLedger,
            OldIndexPath: oldIndexPath, OldSortedPath: oldSortedPath,
        }}})
        
    case *FlushTask_Commit:
        phase := p.Commit
        state := readIndexRegistry()
        state.CurrentIndex = &CurrentIndex{
            IndexPath: phase.FinalIndex, SortedPath: phase.FinalSorted,
            BaseLedger: phase.BaseLedger, MaxLedger: phase.MaxLedger,
        }
        nextTask := &FlushTask{Phase: &FlushTask_Cleanup{Cleanup: &FlushCleanup{
            CfIdx: phase.CfIdx, TriggerArchive: phase.TriggerArchive,
            TempSorted: phase.TempSorted, OldIndexPath: phase.OldIndexPath,
            OldSortedPath: phase.OldSortedPath,
        }}}
        batch := rocksdb.NewWriteBatch()
        batch.PutCF(cfMeta, []byte("index_registry"), proto.Marshal(state))
        batch.PutCF(cfMeta, []byte("flush_task"), proto.Marshal(nextTask))
        db.Write(writeOptsSync, batch)
        indexRegistry.Reload()
        
    case *FlushTask_Cleanup:
        phase := p.Cleanup
        deleteIfExists(phase.OldIndexPath)
        deleteIfExists(phase.OldSortedPath)
        deleteIfExists(phase.TempSorted)
        if !phase.CfCleared {
            dropAndRecreateCF(int(phase.CfIdx))
            phase.CfCleared = true
            saveFlushTask(&FlushTask{Phase: &FlushTask_Cleanup{Cleanup: phase}})
        }
        batch := rocksdb.NewWriteBatch()
        if phase.TriggerArchive {
            batch.PutCF(cfMeta, []byte("archive_task"), proto.Marshal(&ArchiveTask{
                Phase: &ArchiveTask_Copy{Copy: &ArchiveCopy{}},
            }))
        }
        batch.DeleteCF(cfMeta, []byte("flush_task"))
        db.Write(writeOptsSync, batch)
    }
}
```

### B.5 runArchivePhase (Complete)

```go
func runArchivePhase(task *ArchiveTask) {
    switch p := task.Phase.(type) {
    
    case *ArchiveTask_Copy:
        state := readIndexRegistry()
        if state.CurrentIndex == nil {
            db.DeleteCF(writeOptsSync, cfMeta, []byte("archive_task"))
            return
        }
        archivePath := fmt.Sprintf("/data/archives/%d.ssp", state.CurrentIndex.BaseLedger)
        if !exists(archivePath) {
            copyFile(state.CurrentIndex.IndexPath, archivePath)
            fsync("/data/archives/")
        }
        saveArchiveTask(&ArchiveTask{Phase: &ArchiveTask_Commit{Commit: &ArchiveCommit{
            ArchivePath: archivePath, OldIndexPath: state.CurrentIndex.IndexPath,
            OldSortedPath: state.CurrentIndex.SortedPath, BaseLedger: state.CurrentIndex.BaseLedger,
        }}})
        
    case *ArchiveTask_Commit:
        phase := p.Commit
        state := readIndexRegistry()
        alreadyExists := false
        for _, a := range state.Archives {
            if a.IndexPath == phase.ArchivePath { alreadyExists = true; break }
        }
        if !alreadyExists {
            state.Archives = append([]*Archive{{IndexPath: phase.ArchivePath, BaseLedger: phase.BaseLedger}}, state.Archives...)
        }
        state.CurrentIndex = nil
        nextTask := &ArchiveTask{Phase: &ArchiveTask_Cleanup{Cleanup: &ArchiveCleanup{
            ArchivePath: phase.ArchivePath, OldIndexPath: phase.OldIndexPath, OldSortedPath: phase.OldSortedPath,
        }}}
        batch := rocksdb.NewWriteBatch()
        batch.PutCF(cfMeta, []byte("index_registry"), proto.Marshal(state))
        batch.PutCF(cfMeta, []byte("archive_task"), proto.Marshal(nextTask))
        db.Write(writeOptsSync, batch)
        indexRegistry.Reload()
        
    case *ArchiveTask_Cleanup:
        phase := p.Cleanup
        deleteIfExists(phase.OldIndexPath)
        deleteIfExists(phase.OldSortedPath)
        db.DeleteCF(writeOptsSync, cfMeta, []byte("archive_task"))
    }
}
```

### B.6 Startup

```go
func startup() {
    db = openRocksDB("/data/hot", cfNames)
    
    cfData[0] = db.GetOrCreateColumnFamily(CFNameData0)
    cfData[1] = db.GetOrCreateColumnFamily(CFNameData1)
    cfMeta = db.GetOrCreateColumnFamily(CFNameMeta)
    
    if !exists(cfMeta, "active") {
        db.PutCF(writeOptsSync, cfMeta, []byte("active"), []byte{0})
        db.PutCF(writeOptsSync, cfMeta, []byte("current_ledger"), make([]byte, 8))
    }
    
    currentLedger := readCurrentLedger()
    indexRegistry.Reload()
    
    go ingestLoop(currentLedger + 1)
    
    go func() {
        for task := readFlushTask(); task != nil; task = readFlushTask() {
            runFlushPhase(task)
        }
        for task := readArchiveTask(); task != nil; task = readArchiveTask() {
            runArchivePhase(task)
        }
        taskWorker()
    }()
}
```

---

## Appendix C: Fallback Architecture — Hot Hash Table + StreamSplit

If RocksDB's hot tier (cf_data_0/cf_data_1) proves problematic, an alternative uses the persistent hash table (see **Hash Table Specification**) for hot transaction storage while keeping RocksDB for metadata.

### C.1 Prerequisites

This appendix assumes familiarity with:

1. **Hash Table Specification** — Defines the persistent linear-probing hash table with WAL, mmap-based storage, and crash recovery
2. **StreamSplit Specification** — Defines the MPHF construction algorithm and query semantics

### C.2 Overview

Replace RocksDB's data column families with:
- **Two persistent hash tables** (per Hash Table Specification) for hot transaction writes, alternating like dual CFs
- **Write-ahead log** for durability (built into each hash table)
- Same StreamSplit cold tier

**Keep RocksDB for metadata:**
- cf_meta still stores `active`, `current_ledger`, `flush_task`, `archive_task`, `index_registry`
- Task-based recovery model unchanged

**Key difference from RocksDB hot tier:**
- No background compaction (simpler operational model, no write stalls)

### C.3 Directory Structure

```
/data/
├── hot/
│   ├── ht_0.ht                                   # Memory-mapped hash table 0
│   ├── ht_0.wal                                  # WAL for hash table 0
│   ├── ht_1.ht                                   # Memory-mapped hash table 1
│   └── ht_1.wal                                  # WAL for hash table 1
├── meta/
│   └── rocks.db/                                 # RocksDB for cf_meta only
├── current_index/
│   ├── transactions_6500000_a3f2b1c9.sorted      # Named by maxLedger + random suffix
│   └── index_6500000_a3f2b1c9.ssp                # Matching StreamSplit index
└── archives/
    └── *.ssp                                     # Named by baseLedger
```

### C.4 Flush Frequency Analysis: 500k vs 3M Ledgers

| Aspect | 500k Ledgers | 3M Ledgers |
|--------|--------------|------------|
| **Hot tier size (per HT)** | ~95 GB | ~570 GB |
| **Hash table file (per HT)** | ~124 GB (95 GB × 1.31 overhead) | ~747 GB (570 GB × 1.31 overhead) |
| **Total hot tier (2 HTs)** | ~248 GB | ~1.5 TB |
| **Flushes per 6M ledgers** | 12 | 2 |
| **Flush time** | ~2-15 min | ~8-20 min |
| **Recovery scope** | Re-index 500k ledgers | Re-index 3M ledgers |

*Hash table overhead = (40-byte slots ÷ 36-byte entries) ÷ 0.85 load factor ≈ 1.31×*

*Note: Two hash tables are needed (like dual CFs) so one can be frozen for flush while the other accepts writes.*

**500k Ledgers (Frequent Flush):**
- ✓ Smaller per-HT footprint (~124 GB each)
- ✓ Smaller recovery scope on corruption
- ✗ More frequent flush disruptions

**3M Ledgers (Infrequent Flush):**
- ✓ Only 2 flushes per 6M ledgers
- ✓ Simpler operational model
- ✗ Requires ~1.5 TB total for dual hash tables
- ✗ Larger recovery scope (re-index 3M ledgers on corruption)

**Recommendation:** Use 500k ledgers (frequent flush) to keep hash table file size manageable.

### C.5 Flush Timing

| Step | Time | Notes |
|------|------|-------|
| Enumerate hot tier | ~30-45 sec | Sequential read of ~95 GB at 2-3 GB/s |
| Merge | ~3 min | Read 1.14 TB + 95 GB, write 1.23 TB |
| StreamSplit build | ~10 min | 31.5B keys × ~55M keys/sec (sorted) |
| **Total** | **~14 min** | End of 6M ledger period (worst case) |

*First flush (500k ledgers, 2.63B keys, no existing file): ~1 min total.*

### C.6 Trade-offs: Hash Table vs RocksDB Hot Tier

| Aspect | Hash Table Hot Tier | RocksDB Hot Tier |
|--------|---------------------|-------------------|
| Write batch latency | ~5.5 ms (at 1,000 TPS) | ~2 ms |
| Flush time (first, 500k ledgers) | ~1 min | ~11 min |
| Flush time (last, 6M ledgers) | ~14 min | ~28 min |
| Flush peak RAM | ~50 MB (build) | ~500 MB |
| Background CPU | None | 1-2 cores (compaction) |
| Implementation | Custom HT + merge | Library |
| Hot tier RAM (userspace) | < 500 MB (mmap metadata) | ~1 GB (cache + memtables) |

*Per hash table spec: batch latency scales with TPS — ~2 ms at 285 TPS, ~5.5 ms at 1,000 TPS, ~40 ms at 10,000 TPS.*

*Note: Both architectures use RocksDB for metadata (cf_meta). The comparison above is only for the hot transaction storage.*

### C.7 When to Consider Hash Table Hot Tier

- RocksDB hot tier compaction causes write stalls or latency spikes
- RocksDB hot tier memory usage (bloom filters, block cache, memtables) is problematic
- RocksDB requires excessive tuning for acceptable hot tier performance
- Want no background compaction CPU usage for hot tier
- Simpler operational model preferred (no LSM tree complexity for transaction storage)

