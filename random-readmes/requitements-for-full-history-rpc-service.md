# Stellar Full History Ingestion Service — Requirements & Context

This document defines the requirements for building a service that ingests Stellar blockchain ledger data and makes it queryable. It is intended as input for an AI agent (Claude Code) to design and implement the system.

**Important**: This document describes WHAT to build and the constraints. It does NOT prescribe HOW to build it. The implementing agent should study the referenced code and make design decisions.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Glossary](#2-glossary)
3. [System Constraints](#3-system-constraints)
4. [Data Characteristics](#4-data-characteristics)
5. [Operating Modes](#5-operating-modes)
6. [Storage Architecture](#6-storage-architecture)
7. [Key Workflows](#7-key-workflows)
8. [Codebase References](#8-codebase-references)
9. [Open Questions for Design](#9-open-questions-for-design)
10. [Success Criteria](#10-success-criteria)

---

## 1. Problem Statement

### 1.1 Goal

Build a service that:
1. Ingests Stellar ledger data (LedgerCloseMeta) from the network or cloud storage
2. Stores the data in queryable format
3. Supports both historical backfill and real-time streaming
4. Transitions data from active (recent) storage to immutable (historical) storage over time

### 1.2 Scale

- **Total ledger range**: ~10 million ledgers per major range
- **Full history**: Multiple 10M ranges to cover entire Stellar history
- **Continuous operation**: After backfill, transition to streaming mode to stay current

---

## 2. Glossary

### 2.1 Core Concepts

| Term | Definition |
|------|------------|
| **Ledger** | A block in Stellar's blockchain. Contains transactions, operations, and state changes. Identified by a sequence number starting at 2. |
| **LedgerCloseMeta (LCM)** | The complete metadata emitted when a ledger closes. This is the raw data unit being ingested. Variable size, typically ~1MB uncompressed, ~150KB compressed. |
| **Ledger Sequence** | Monotonically increasing uint32 identifier for each ledger. Stellar starts at ledger 2. |
| **Chunk** | A fixed-size container of consecutive ledgers. In this system, **1 chunk = 10,000 ledgers**. |
| **Ledger Range** | A span of ledgers defined by start and end sequence numbers. The primary operational unit is a **10 million ledger range**. |

### 2.2 Storage Concepts

| Term | Definition |
|------|------------|
| **Active Store** | Storage for recent ledger data. Uses RocksDB. Supports both reads and writes. Contains approximately the most recent 10M ledgers. |
| **Immutable Store** | Storage for historical ledger data. Uses a custom Local File System (LFS) format. Read-only after creation. Organized into 10k-ledger chunks. |
| **Meta Store** | Tracks the status of chunks and stores operational state. Used for crash recovery, progress tracking, and coordination. |

### 2.3 Data Source Concepts

| Term | Definition |
|------|------------|
| **LedgerBackend** | An interface from the Stellar Go SDK that provides access to ledger data. Has multiple implementations. |
| **CaptiveStellarCore** | A LedgerBackend implementation that spawns a stellar-core subprocess and streams LCM via named pipe. Requires ~8GB RAM per instance. Can run in catchup (bounded range) or live (unbounded) mode. |
| **BufferedStorageBackend** | A LedgerBackend implementation that reads pre-exported LCM from cloud object storage (GCS/S3). No stellar-core required. Configurable worker pool for concurrent fetches. |

### 2.4 Operational Concepts

| Term | Definition |
|------|------------|
| **Backfill Mode** | Ingesting historical ledgers to catch up to the present. Typically processes a bounded range. Can use either CaptiveStellarCore or BufferedStorageBackend. |
| **Streaming Mode** | Ingesting new ledgers as they close on the network in real-time. Must use CaptiveStellarCore (not BufferedStorageBackend). |
| **Orchestrator** | A process responsible for ingesting a specific ledger range. Multiple orchestrators can run in parallel for different ranges during backfill. |

---

## 3. System Constraints

### 3.1 Hardware

| Resource | Specification |
|----------|---------------|
| CPU | 32 cores |
| RAM | 128 GB |
| Storage | NVMe SSD (size TBD based on retention) |

### 3.2 Memory Budget

| Component | Memory Requirement |
|-----------|-------------------|
| CaptiveStellarCore instance | ~8 GB each |
| RocksDB (active store) | Configurable, see `rocksdb/ingestion-v2/config.go` for MemTable sizing |
| Processing buffers | Proportional to batch size |

### 3.3 Processing Parameters

| Parameter | Value | Notes |
|-----------|-------|-------|
| Ledger batch size | 1,000 ledgers | For checkpoint/commit granularity |
| Chunk size | 10,000 ledgers | For immutable store organization |
| Operational range | 10,000,000 ledgers | Primary unit for backfill orchestration |

### 3.4 Data Sizes

| Metric | Value |
|--------|-------|
| Uncompressed LCM | ~1 MB per ledger (varies by transaction count) |
| Compressed LCM (zstd) | ~150 KB per ledger |
| Transactions per ledger | ~5,000 at 1,000 TPS |

---

## 4. Data Characteristics

### 4.1 LedgerCloseMeta Structure

The LCM is an XDR-encoded blob containing:
- Ledger header (sequence, close time, hash, etc.)
- Transaction set (all transactions in the ledger)
- Transaction results and metadata
- State changes (ledger entries created/modified/deleted)

### 4.2 Key Relationships

| Lookup Pattern | Description |
|----------------|-------------|
| Ledger sequence → LCM | Primary lookup: given a sequence number, retrieve the full LCM |
| Transaction hash → Ledger sequence | Secondary index: given a tx hash, find which ledger contains it |

### 4.3 Access Patterns

| Pattern | Storage Type | Characteristics |
|---------|--------------|-----------------|
| Recent ledgers | Active Store (RocksDB) | High write throughput, frequent reads, sequential keys |
| Historical ledgers | Immutable Store (LFS) | No writes after creation, random reads, chunk-based organization |

---

## 5. Operating Modes

### 5.1 Backfill Mode

**Purpose**: Ingest historical ledgers to build up the data store from scratch or fill gaps.

**Characteristics**:
- Processes a bounded range of ledgers (e.g., ledgers 50,000,000 to 60,000,000)
- Can use either CaptiveStellarCore or BufferedStorageBackend as data source
- Multiple orchestrators can run in parallel for different ranges
- Each orchestrator manages its own 10M ledger range
- Writes to active store (RocksDB)

**Data Source Options**:
1. **BufferedStorageBackend** (preferred for backfill)
   - Reads from GCS bucket `sdf-ledger-close-meta/v1/ledgers/pubnet`
   - Configurable parallelism (see `rocksdb/ingestion-v2/` for buffer/worker settings)
   - No stellar-core process required

2. **CaptiveStellarCore** (alternative)
   - Spawns stellar-core in catchup mode
   - Higher resource usage (~8GB per instance)
   - Required if GCS data not available

### 5.2 Streaming Mode

**Purpose**: Ingest new ledgers as they close on the network in real-time.

**Characteristics**:
- Single process only (not parallelizable)
- Must use CaptiveStellarCore in live/unbounded mode
- Cannot use BufferedStorageBackend (data not yet exported)
- Writes to active store (RocksDB)
- Should detect and fill any gaps before starting

**Gap Detection**:
Before transitioning from backfill to streaming, the system must:
1. Identify the highest ledger in the active store
2. Query the network for the current ledger
3. Fill any gap between them
4. Only then start streaming

---

## 6. Storage Architecture

### 6.1 Active Store (RocksDB)

**Purpose**: Store recent ledger data for fast reads and writes.

**Characteristics**:
- Uses RocksDB with write-optimized configuration during ingestion
- Supports both point lookups and range scans
- Contains approximately the most recent 10M ledgers
- Data eventually transitions to immutable store

**Key-Value Schema**:
- Key: 4-byte ledger sequence (big-endian uint32)
- Value: zstd-compressed LedgerCloseMeta

**Reference Implementation**: See `rocksdb/ingestion-v2/` for:
- RocksDB configuration patterns (`config.go`)
- Write optimization settings (`stores.go`)
- Batch processing (`processing.go`)

### 6.2 Immutable Store (Custom LFS)

**Purpose**: Store historical ledger data in a compact, read-optimized format.

**Characteristics**:
- File-based storage organized into chunks
- Each chunk contains exactly 10,000 ledgers
- Two files per chunk: `.data` (compressed ledgers) and `.index` (byte offsets)
- Read-only after creation
- No RocksDB overhead

**Directory Layout**:
```
<data_dir>/
└── chunks/
    ├── 0000/
    │   ├── 000000.data
    │   ├── 000000.index
    │   ├── 000001.data
    │   ├── 000001.index
    │   └── ...
    ├── 0001/
    │   ├── 001000.data
    │   ├── 001000.index
    │   └── ...
    └── ...
```

**Reference Implementation**: See `local-fs/ingestion/` for:
- Chunk format specification
- Index file format (header + offsets)
- Read/write operations
- Ledger-to-chunk mapping functions

**Reference Documentation**: See `random-readmes/how-to-create-custom-lfs-ledger-store.md` for:
- Detailed file format specification
- Index header structure
- Offset encoding (4-byte vs 8-byte)

### 6.3 Meta Store

**Purpose**: Track operational state for crash recovery and coordination.

**Responsibilities**:
- Track chunk status (pending, active, immutable)
- Store progress checkpoints (last committed ledger, counts)
- Enable crash recovery
- Support active→immutable transition tracking

**Reference Pattern**: See `txhash-ingestion-workflow/meta_store.go` for checkpoint patterns.

---

## 7. Key Workflows

### 7.1 Backfill Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                    BACKFILL ORCHESTRATION                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   Orchestrator A              Orchestrator B              ...    │
│   (Ledgers 0-10M)            (Ledgers 10M-20M)                  │
│        │                          │                              │
│        ▼                          ▼                              │
│   ┌─────────┐                ┌─────────┐                        │
│   │ Ledger  │                │ Ledger  │                        │
│   │ Backend │                │ Backend │                        │
│   └────┬────┘                └────┬────┘                        │
│        │                          │                              │
│        ▼                          ▼                              │
│   ┌─────────┐                ┌─────────┐                        │
│   │ Active  │                │ Active  │                        │
│   │ Store   │                │ Store   │                        │
│   │(RocksDB)│                │(RocksDB)│                        │
│   └─────────┘                └─────────┘                        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Process**:
1. Each orchestrator is assigned a 10M ledger range
2. Orchestrator initializes LedgerBackend (BufferedStorageBackend or CaptiveStellarCore)
3. Fetch ledgers in batches (1k ledgers per batch)
4. Compress and write to active store
5. Checkpoint progress to meta store
6. On completion, signal readiness for active→immutable transition

### 7.2 Active→Immutable Transition Workflow

**Purpose**: Move data from RocksDB (active) to LFS (immutable) format.

**Duration**: Approximately 4-5 hours for a 10M ledger range.

**Critical Requirement**: During transition, queries must continue to work. This requires maintaining dual active stores temporarily.

```
┌─────────────────────────────────────────────────────────────────┐
│                 ACTIVE → IMMUTABLE TRANSITION                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Phase 1: PREPARATION                                            │
│  ─────────────────────                                           │
│  • Freeze writes to current active store (Store A)               │
│  • Create new active store (Store B) for incoming writes         │
│  • Queries: Check Store A first, then Store B                    │
│                                                                  │
│  Phase 2: CONVERSION                                             │
│  ────────────────────                                            │
│  • Iterate Store A in chunks (10k ledgers)                       │
│  • Write each chunk to immutable LFS format                      │
│  • Update meta store: mark chunks as immutable                   │
│  • Queries: LFS for converted chunks, Store A for rest, Store B  │
│                                                                  │
│  Phase 3: CLEANUP                                                │
│  ───────────────                                                 │
│  • Verify all chunks converted successfully                      │
│  • Delete Store A                                                │
│  • Queries: LFS for historical, Store B for recent               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Reference Documentation**: See `random-readmes/sample-active-to-immutable-store-workflow.md` for:
- Detailed phase descriptions
- State machine for transition
- Lock handling patterns

**Reference Implementation**: See `txhash-ingestion-workflow/` for:
- PoC code that reads from existing LFS (note: needs adaptation to read from RocksDB)
- Phase state machine
- Crash recovery patterns

### 7.3 Streaming Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                    STREAMING WORKFLOW                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. GAP DETECTION                                                │
│     • Read highest ledger from active store                      │
│     • Query network for current ledger                           │
│     • If gap exists: fill via backfill before streaming          │
│                                                                  │
│  2. INITIALIZE                                                   │
│     • Start CaptiveStellarCore in unbounded/live mode            │
│     • ~8GB memory allocation                                     │
│                                                                  │
│  3. STREAM LOOP                                                  │
│     • Receive LCM from named pipe                                │
│     • Compress and write to active store                         │
│     • Checkpoint progress                                        │
│     • Repeat                                                     │
│                                                                  │
│  Note: Single process only. Not parallelizable.                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 7.4 Query Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                      QUERY ROUTING                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Query: GetLedger(sequence)                                      │
│                                                                  │
│  1. Determine storage location from meta store:                  │
│     • If sequence in active range → query active store           │
│     • If sequence in immutable range → query immutable store     │
│     • If sequence in transition → check both                     │
│                                                                  │
│  2. Active Store Query:                                          │
│     • Point lookup in RocksDB by key (sequence)                  │
│     • Decompress zstd                                            │
│     • Return LCM                                                 │
│                                                                  │
│  3. Immutable Store Query:                                       │
│     • Calculate chunk ID from sequence                           │
│     • Read index file for byte offset                            │
│     • Read compressed data from data file                        │
│     • Decompress zstd                                            │
│     • Return LCM                                                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 8. Codebase References

### 8.1 LedgerBackend Usage

**Directory**: `rocksdb/ingestion-v2/`

**Key Files**:
| File | Purpose |
|------|---------|
| `config.go` | RocksDB settings, TOML configuration structure |
| `config.toml` | Example configuration with detailed comments |
| `processing.go` | LedgerBackend usage, LCM compression, transaction extraction |
| `stores.go` | RocksDB store management, batch writing |
| `stats.go` | Progress tracking, metrics, timing |
| `main.go` | Entry point, batch processing loop |

**What to Learn**:
- How to initialize BufferedStorageBackend (see `main.go` GCS setup)
- Batch processing pattern (fetch → compress → write → checkpoint)
- RocksDB configuration for bulk ingestion (disable auto-compaction, etc.)

### 8.2 LFS Format & Implementation

**Directory**: `local-fs/ingestion/`

**Key Files**:
| File | Purpose |
|------|---------|
| `lfs-ledger-ingestion.go` | Complete LFS implementation: ingestion, querying, iteration |

**What to Learn**:
- Chunk format (data + index files)
- Ledger-to-chunk mapping functions
- Index file format (version, offset size, offsets)
- Read patterns (point lookup, range iteration)
- LedgerIterator pattern for efficient range reads

### 8.3 LFS Format Documentation

**File**: `random-readmes/how-to-create-custom-lfs-ledger-store.md`

**What to Learn**:
- Detailed file format specification
- Index header structure
- Sequence-to-location mapping formula
- Size limits and constraints

### 8.4 Active→Immutable Transition Pattern

**Directory**: `txhash-ingestion-workflow/`

**Key Files**:
| File | Purpose |
|------|---------|
| `README.md` | Comprehensive documentation of workflow phases |
| `workflow.go` | Phase orchestration |
| `meta_store.go` | Checkpoint persistence |
| `pkg/compact/compact.go` | Compaction patterns |

**What to Learn**:
- State machine for multi-phase workflows
- Crash recovery with checkpointing
- Parallel processing patterns
- Count verification after operations

**Note**: This PoC reads from an existing LFS store, not RocksDB. The design will need to adapt this to read from RocksDB active store.

### 8.5 Sample Workflow Documentation

**File**: `random-readmes/sample-active-to-immutable-store-workflow.md`

**What to Learn**:
- Detailed architecture for hot/cold tiering
- Phase-based task execution
- Idempotency patterns for crash recovery
- Lock handling during transitions

### 8.6 LedgerBackend Interface

**File**: `random-readmes/ledgerBackend-synopsis.md`

**What to Learn**:
- LedgerBackend interface definition
- CaptiveStellarCore vs BufferedStorageBackend differences
- Range types (Bounded, Unbounded, Single)

### 8.7 Project Standards

**File**: `CLAUDE.md`

**What to Learn**:
- Helper functions available (`helpers/helpers.go`, `helpers/lfs/`)
- Logging requirements (dual log + error files)
- Metrics requirements (timing, percentiles, throughput)
- Error handling policy (fatal vs non-fatal)
- Crash recovery patterns
- RocksDB configuration patterns

---

## 9. Open Questions for Design

The implementing agent should make decisions on these questions and document rationale:

### 9.1 Backfill Architecture

1. **Single vs Multiple Processes for Backfill**
   - Option A: Single process handling one 10M range at a time
   - Option B: Multiple processes, each handling different 10M ranges in parallel
   - Considerations: Memory budget (128GB total), CaptiveStellarCore memory (~8GB each), coordination complexity

2. **Shared vs Separate Meta Stores**
   - If running multiple backfill processes, should they share a meta store or each have their own?
   - Considerations: Coordination, crash recovery scope, deployment simplicity

### 9.2 Storage Decisions

3. **When to Trigger Active→Immutable Transition**
   - Based on ledger count threshold?
   - Based on time/age of data?
   - Manual trigger?
   - Considerations: Query patterns, storage costs, transition duration (4-5 hours)

4. **Active Store Sizing**
   - How many ledgers in active store before transition?
   - Considerations: RocksDB performance at scale, memory usage, transition frequency

### 9.3 Operational Decisions

5. **Gap Handling Strategy**
   - How to detect gaps in ingested data?
   - How to prioritize gap filling vs forward progress?

6. **Failure Recovery Granularity**
   - Checkpoint every N ledgers (currently 1k)?
   - Trade-offs between recovery time and I/O overhead

### 9.4 Query Routing

7. **Query Path During Transition**
   - How to efficiently route queries when data is partially in active, partially in immutable?
   - How to handle queries for data being actively converted?

---

## 10. Success Criteria

### 10.1 Functional Requirements

| Requirement | Description |
|-------------|-------------|
| **F1** | Ingest ledgers from BufferedStorageBackend (GCS) for backfill |
| **F2** | Ingest ledgers from CaptiveStellarCore for streaming |
| **F3** | Store compressed LCM in active store (RocksDB) |
| **F4** | Transition data from active to immutable store (LFS) |
| **F5** | Query any ledger by sequence number |
| **F6** | Resume from crash at any point in any workflow |

### 10.2 Performance Requirements

| Requirement | Target |
|-------------|--------|
| **P1** | Backfill ingestion rate: TBD ledgers/second |
| **P2** | Streaming latency: Keep up with ~5 second ledger close time |
| **P3** | Point query latency: < 10ms for active store, < 50ms for immutable |
| **P4** | Active→Immutable transition: Complete within 4-5 hours for 10M ledgers |

### 10.3 Reliability Requirements

| Requirement | Description |
|-------------|-------------|
| **R1** | Zero data loss on crash (checkpoint-based recovery) |
| **R2** | Queries continue working during active→immutable transition |
| **R3** | Accurate counts after crash recovery (counts checkpointed with progress) |

### 10.4 Operational Requirements

| Requirement | Description |
|-------------|-------------|
| **O1** | Dual logging: INFO/DEBUG to log file, ERROR/WARN to error file |
| **O2** | Progress metrics: ledgers/second, compression ratio, ETA |
| **O3** | Memory monitoring: RSS tracking, warnings at thresholds |

---

## Appendix A: Ledger-to-Chunk Mapping

```
chunk_size = 10,000
first_ledger = 2  (Stellar starts at ledger 2)

chunk_id = (ledger_sequence - first_ledger) / chunk_size
local_index = (ledger_sequence - first_ledger) % chunk_size
chunk_first_ledger = (chunk_id * chunk_size) + first_ledger
chunk_last_ledger = ((chunk_id + 1) * chunk_size) + first_ledger - 1
```

**Examples**:
| Ledger Sequence | Chunk ID | Local Index |
|-----------------|----------|-------------|
| 2 | 0 | 0 |
| 10,001 | 0 | 9,999 |
| 10,002 | 1 | 0 |
| 1,234,567 | 123 | 4,565 |

---

## Appendix B: Memory Budget Example

With 128GB RAM and conservative allocation:

| Component | Allocation | Notes |
|-----------|------------|-------|
| OS + overhead | 8 GB | |
| CaptiveStellarCore (if used) | 8 GB | One instance for streaming |
| RocksDB MemTables | 8 GB | Configurable per store |
| RocksDB Block Cache | 16 GB | For read performance |
| Processing buffers | 8 GB | Batch compression, etc. |
| Available for parallelism | 80 GB | ~10 additional CaptiveStellarCore if needed |

---

## Appendix C: File Format Quick Reference

### C.1 LFS Index File

```
┌─────────────────────────────────────┐
│ Header (8 bytes)                    │
│   version: u8 (= 1)                 │
│   offset_size: u8 (4 or 8)          │
│   reserved: [u8; 6]                 │
├─────────────────────────────────────┤
│ Offsets (little-endian)             │
│   offset[0]: u32 or u64             │
│   offset[1]: u32 or u64             │
│   ...                               │
│   offset[count]: u32 or u64         │
└─────────────────────────────────────┘

Record size for ledger i = offset[i+1] - offset[i]
```

### C.2 LFS Data File

```
┌─────────────────────────────────────┐
│ Record 0: zstd(ledger_0)            │
├─────────────────────────────────────┤
│ Record 1: zstd(ledger_1)            │
├─────────────────────────────────────┤
│ ...                                 │
└─────────────────────────────────────┘

No framing. Boundaries from index file.
```

---

*End of Requirements Document*