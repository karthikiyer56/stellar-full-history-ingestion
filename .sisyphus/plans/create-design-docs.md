# Work Plan: Create Stellar Full History RPC Design Documentation

## TL;DR

> **Quick Summary**: Create comprehensive design documentation for the Stellar Full History RPC Service with detailed sub-documents for each major component.
> 
> **Deliverables**:
> - `design-docs/README.md` - Main index and overview
> - `design-docs/01-architecture-overview.md` - High-level architecture
> - `design-docs/02-meta-store-design.md` - Meta store key structure with examples
> - `design-docs/03-backfill-workflow.md` - Backfill mode detailed workflow
> - `design-docs/04-streaming-workflow.md` - Streaming mode detailed workflow
> - `design-docs/05-transition-workflow.md` - Active→Immutable transition
> - `design-docs/06-crash-recovery.md` - Crash recovery scenarios
> - `design-docs/07-query-routing.md` - Query routing logic
> - `design-docs/08-directory-structure.md` - File system layout
> - `design-docs/09-configuration.md` - TOML configuration reference
> 
> **Estimated Effort**: Medium
> **Parallel Execution**: YES - 3 waves
> **Critical Path**: Task 1 (README) → Tasks 2-9 (parallel) → Task 10 (verification)

---

## Context

### Original Request
Create comprehensive design documentation for the Stellar Full History RPC Service that supports two operating modes (Backfill and Streaming), handles 10M ledger ranges, and transitions data from Active Stores (RocksDB) to Immutable Stores (LFS + RecSplit).

### Interview Summary
See `.sisyphus/drafts/full-history-rpc-design.md` for complete interview notes and decisions.

**Key Decisions**:
- Single process with goroutine orchestrators for backfill
- Separate RocksDB instances per data type (ledger, txhash)
- Automatic transition at 10M boundary (streaming mode only)
- Per-range checkpointing, never deleted
- LFS chunk format for immutable ledgers
- RecSplit indexes for immutable txhash

### Source Material Conflict Resolution Policy

**Authoritative Source Order** (highest to lowest priority):
1. **This plan's Canonical Definitions section** (see below) - ALWAYS takes precedence
2. `.sisyphus/drafts/full-history-rpc-design.md` - Interview decisions
3. `.sisyphus/plans/stellar-full-history-rpc-design.md` - Previous consolidated design

If any example or definition in source materials conflicts with the Canonical Definitions below, **use the Canonical Definitions**. Update examples to match.

### Known Conflicts to Resolve (CONCRETE FIXES)

When writing documents, apply these corrections to source material:

| Source Location | Incorrect Value | Correct Value (Canonical) |
|-----------------|-----------------|---------------------------|
| Draft line ~725-750 | Transition trigger at 40,000,000 | Trigger at 40,000,001 (last ledger of range 3) |
| Draft line ~277-278 | `X % 10,000,000 == 1` validation | Use `rangeFirstLedger(N)` formula: valid starts are 2, 10000002, 20000002... |
| Draft line ~278 | `X % 10,000,000 == 0` + 1 validation | Use `rangeLastLedger(N)` formula: valid ends are 10000001, 20000001, 30000001... |
| Any source | "getTransaction" (ambiguous) | "getTransactionByHash" |
| Any source | "getLedger" (ambiguous) | "getLedgerBySequence" |
| Any source | "GetLedger" / "GetTransaction" in Go code | "GetLedgerBySequence" / "GetTransactionByHash" |
| Draft line ~598-600 | Streaming batch size example | Streaming uses batch=1, backfill uses batch=1000 |
| stellar-full-history-rpc-design.md:541 | `GetLedger` function name | Change to `GetLedgerBySequence` |
| stellar-full-history-rpc-design.md:563 | `GetTransaction` function name | Change to `GetTransactionByHash` |
| Draft scenario 4 (~725-750) | Boundary at 39,999,999 → 40,000,000 | Boundary at 40,000,000 → 40,000,001 (trigger is last ledger of range) |

### Mandatory Ledger Number Audit (EXECUTOR INSTRUCTION)

Before finalizing any document that contains ledger sequence numbers:

1. **Search for all numeric examples**: Find all 6+ digit numbers in the document
2. **For each number**, verify it matches canonical boundaries:
   - Range first ledgers: 2, 10000002, 20000002, 30000002, ...
   - Range last ledgers: 10000001, 20000001, 30000001, ...
   - Transition triggers: 10000001, 20000001, 30000001, ... (same as last ledgers)
3. **Recalculate using formulas** if number doesn't match:
   - `rangeFirstLedger(N) = (N * 10,000,000) + 2`
   - `rangeLastLedger(N) = ((N + 1) * 10,000,000) + 1`
4. **Fix any mismatches** before committing the document

---

## Canonical Definitions (MANDATORY - Use Across ALL Documents)

### Ledger Range Math

**Constants**:
```go
const (
    FirstLedger = 2           // First ledger in Stellar blockchain
    RangeSize   = 10_000_000  // 10 million ledgers per range
)
```

**Range ID Calculation**:
```go
// Range ID = floor((ledgerSeq - FirstLedger) / RangeSize)
func ledgerToRangeID(ledgerSeq uint32) uint32 {
    return (ledgerSeq - FirstLedger) / RangeSize
}
```

**Range Boundaries** (CANONICAL):

| Range ID | First Ledger | Last Ledger | Notes |
|----------|--------------|-------------|-------|
| 0 | 2 | 10,000,001 | First range starts at genesis (ledger 2) |
| 1 | 10,000,002 | 20,000,001 | |
| 2 | 20,000,002 | 30,000,001 | |
| N | (N × 10,000,000) + 2 | ((N+1) × 10,000,000) + 1 | General formula |

```go
func rangeFirstLedger(rangeID uint32) uint32 {
    return (rangeID * RangeSize) + FirstLedger
}

func rangeLastLedger(rangeID uint32) uint32 {
    return ((rangeID + 1) * RangeSize) + FirstLedger - 1
}
```

### Transition Trigger (CANONICAL)

**Trigger Condition**: Transition is triggered when ingesting the **first ledger of a NEW range**.

```go
// Transition triggers when we're about to ingest the first ledger of range N+1
// This happens when the CURRENT ledger is the LAST ledger of range N
func shouldTriggerTransition(ledgerSeq uint32) bool {
    // Check if this ledger is the last ledger of its range
    rangeID := ledgerToRangeID(ledgerSeq)
    return ledgerSeq == rangeLastLedger(rangeID)
}
```

**Trigger Points** (ledger sequences that trigger transition):
- 10,000,001 (last ledger of range 0 → triggers transition, next ledger starts range 1)
- 20,000,001 (last ledger of range 1 → triggers transition)
- 30,000,001 (last ledger of range 2 → triggers transition)
- etc.

**Workflow**:
1. Ingest ledger 10,000,001 (last ledger of range 0)
2. Detect: `shouldTriggerTransition(10000001) == true`
3. Create NEW Active Stores for range 1
4. Spawn transition goroutine for range 0
5. Next ingested ledger (10,000,002) goes to range 1's Active Store

### CLI Arguments (CANONICAL)

**Backfill Command**:
```bash
./stellar-rpc --backfill --start-ledger <START> --end-ledger <END>
```

**Validation Rules**:
- `--start-ledger`: Must be `2` OR `rangeFirstLedger(N)` for some N
  - Valid: 2, 10000002, 20000002, 30000002, ...
- `--end-ledger`: Must be `rangeLastLedger(N)` for some N
  - Valid: 10000001, 20000001, 30000001, ...

**Examples** (CANONICAL):
```bash
# Ingest first 3 ranges (60M ledgers)
./stellar-rpc --backfill --start-ledger 2 --end-ledger 30000001

# Ingest ranges 3-5
./stellar-rpc --backfill --start-ledger 30000002 --end-ledger 60000001
```

### API Endpoint Names (CANONICAL)

Use these exact names across ALL documents:

| Endpoint | Full Signature | Short Reference |
|----------|----------------|-----------------|
| Get Transaction | `getTransactionByHash(txHash: bytes) → LedgerSeq` | "getTransactionByHash" |
| Get Ledger | `getLedgerBySequence(ledgerSeq: uint32) → LedgerCloseMeta` | "getLedgerBySequence" |
| Health Check | `getHealth() → HealthStatus` | "getHealth" |
| Status | `getStatus() → ServiceStatus` | "getStatus" |

**DO NOT USE**: "getTransaction", "getLedger" (too ambiguous)

### Canonical TOML Configuration Schema (AUTHORITATIVE)

This is the authoritative schema for Task 9's configuration reference. Use this table for types, required/optional, and defaults.

**Decision Criteria:**
- **Required**: Needed to start the service; no sensible default
- **Optional (with default)**: Service can start without explicit value
- **Conditional**: Required only in specific modes

| Table | Key | Type | Required | Default | Description |
|-------|-----|------|----------|---------|-------------|
| `[service]` | `data_dir` | string | **Required** | - | Base directory for all data stores |
| `[service]` | `http_port` | int | Optional | `8080` | HTTP server port |
| `[service]` | `mode` | string | Optional | `"streaming"` | Operating mode: "backfill" or "streaming" |
| `[meta_store]` | `path` | string | Optional | `{data_dir}/meta/rocksdb` | Meta store RocksDB path |
| `[active_stores]` | `ledger_path` | string | Optional | `{data_dir}/active/ledger/rocksdb` | Active ledger store path |
| `[active_stores]` | `txhash_path` | string | Optional | `{data_dir}/active/txhash/rocksdb` | Active txhash store path |
| `[immutable_stores]` | `ledgers_base` | string | Optional | `{data_dir}/immutable/ledgers` | Base path for immutable LFS chunks |
| `[immutable_stores]` | `txhash_base` | string | Optional | `{data_dir}/immutable/txhash` | Base path for immutable RecSplit indexes |
| `[rocksdb]` | `block_cache_mb` | int | Optional | `8192` | Block cache size in MB |
| `[rocksdb]` | `write_buffer_mb` | int | Optional | `512` | Write buffer size in MB per CF |
| `[rocksdb]` | `max_write_buffer_number` | int | Optional | `2` | Max write buffers per CF |
| `[backfill]` | `start_ledger` | uint32 | **Conditional** | - | Required in backfill mode. Must be rangeFirstLedger(N) |
| `[backfill]` | `end_ledger` | uint32 | **Conditional** | - | Required in backfill mode. Must be rangeLastLedger(N) |
| `[backfill]` | `ledger_backend` | string | **Conditional** | - | Required in backfill mode. "buffered_storage" or "captive_core" |
| `[backfill]` | `parallel_ranges` | int | Optional | `2` | Number of parallel 10M range orchestrators |
| `[backfill]` | `checkpoint_interval` | int | Optional | `1000` | Ledgers between checkpoints |
| `[backfill.buffered_storage]` | `bucket_path` | string | **Conditional** | - | Required if ledger_backend="buffered_storage" |
| `[backfill.buffered_storage]` | `buffer_size` | int | Optional | `10000` | Ledger buffer size |
| `[backfill.buffered_storage]` | `num_workers` | int | Optional | `200` | Download worker count |
| `[backfill.captive_core]` | `binary_path` | string | **Conditional** | - | Required if ledger_backend="captive_core" |
| `[backfill.captive_core]` | `config_path` | string | **Conditional** | - | Required if ledger_backend="captive_core" |
| `[streaming]` | `checkpoint_interval` | int | Optional | `1` | Ledgers between checkpoints (1 = every ledger) |
| `[streaming.captive_core]` | `binary_path` | string | **Conditional** | - | Required in streaming mode |
| `[streaming.captive_core]` | `config_path` | string | **Conditional** | - | Required in streaming mode |

### Multi-Disk Configuration (for 08-directory-structure.md)

Document these scenarios:
1. **Single disk**: All paths under one `data_dir` (default)
2. **Separate volumes**: Override individual paths to place stores on different mounts
   - Example: NVMe for `active_stores`, HDD for `immutable_stores`
3. **Path override pattern**: Any `*_path` or `*_base` key can be absolute path to different volume
4. **No striping**: Service does NOT implement internal striping; use volume manager if needed

### Required Section Headings (for Cross-Link Anchors)

Each document MUST include these headings to enable stable cross-references. Use exact markdown headings so anchors are predictable.

| Document | Required Headings (becomes anchor) |
|----------|-----------------------------------|
| README.md | `## Document Index`, `## Quick Reference`, `## Key Invariants` |
| 01-architecture-overview.md | `## System Components`, `## Data Flow`, `## Store Types`, `## Hardware Requirements`, `## HTTP Endpoints by Mode` |
| 02-meta-store-design.md | `## Key Hierarchy`, `## Enums`, `## Range ID Calculation`, `## Scenario Walkthroughs` |
| 03-backfill-workflow.md | `## Command Format`, `## Validation Rules`, `## Process Flow`, `## Checkpoint Mechanism`, `## Example` |
| 04-streaming-workflow.md | `## Startup Validation`, `## Ingestion Loop`, `## Boundary Detection`, `## Graceful Shutdown`, `## Example` |
| 05-transition-workflow.md | `## Trigger Conditions`, `## Parallel Sub-Workflows`, `## Ledger Sub-Flow`, `## TxHash Sub-Flow`, `## Example` |
| 06-crash-recovery.md | `## Checkpoint Mechanism`, `## Recovery Scenarios`, `## Count Accuracy`, `## Gap Detection` |
| 07-query-routing.md | `## getLedgerBySequence Routing`, `## getTransactionByHash Routing`, `## Decision Tree`, `## Examples` |
| 08-directory-structure.md | `## File Tree`, `## LFS Chunk Paths`, `## RecSplit Paths`, `## Multi-Disk Configuration` |
| 09-configuration.md | `## TOML Reference`, `## Validation Rules`, `## Example Configurations` |

**Cross-link format**: `[See Recovery Scenarios](./06-crash-recovery.md#recovery-scenarios)`

---

## Work Objectives

### Core Objective
Create a complete, well-organized design documentation set that enables implementation of the Stellar Full History RPC Service.

### Concrete Deliverables
- 10 markdown files in `design-docs/` directory
- Each document self-contained but cross-referenced
- Mermaid diagrams for visual explanation
- Code examples where appropriate
- Scenario walkthroughs with concrete examples

### Definition of Done
- [x] All 10 documents created in `design-docs/`
- [x] Documents render correctly (valid markdown, valid mermaid)
- [x] Cross-references between documents work
- [x] No placeholder text or TODOs in final documents

### Must Have
- Comprehensive meta store key structure with examples
- Crash recovery scenarios with step-by-step walkthroughs
- Transition workflow with parallel sub-flow details
- Query routing logic with examples

### Must NOT Have (Guardrails)
- Production implementation code (no full package-level implementations, no production-ready functions)
- Test infrastructure details (to be added later)
- Unresolved questions or placeholders
- Duplicate content across documents

### ALLOWED Code in Documents
The following are explicitly permitted and expected:
- **Go pseudocode** for illustrating logic (e.g., routing decisions, validation rules)
- **Small pure-math helpers** (e.g., `ledgerToRangeID()`, `rangeFirstLedger()`)
- **Struct/type definitions** for conceptual clarity
- **Example key-value pairs** with concrete values
- **Code blocks showing expected function signatures** (not full implementations)

**MANDATORY: All range/boundary calculations MUST use the canonical helpers from Canonical Definitions:**
- `ledgerToRangeID(ledgerSeq)` - Calculate range ID
- `rangeFirstLedger(rangeID)` - First ledger of range
- `rangeLastLedger(rangeID)` - Last ledger of range  
- `shouldTriggerTransition(ledgerSeq)` - Check if ledger triggers transition

**DO NOT introduce alternate formulas** (e.g., modulo-based shortcuts). All boundary logic must derive from these canonical functions.

---

## Document Responsibility Matrix (NO DUPLICATION)

To comply with the guardrail "Must NOT Have: Duplicate content across documents", use this matrix:

| Topic | AUTHORITATIVE Document | Other Documents |
|-------|----------------------|-----------------|
| **TOML Configuration (full reference)** | 09-configuration.md | Others: link to 09, show only minimal examples |
| **Directory Structure (full tree)** | 08-directory-structure.md | Others: link to 08, inline only paths relevant to context |
| **Meta Store Key Hierarchy (complete)** | 02-meta-store-design.md | Others: link to 02, show only keys relevant to workflow |
| **Range Boundary Math (formulas)** | 02-meta-store-design.md | Others: reference 02 or Canonical Definitions in this plan |
| **Query Routing Logic (full)** | 07-query-routing.md | Others: link to 07, brief mention only |
| **Crash Recovery Scenarios (all 5+)** | 06-crash-recovery.md | Others: link to 06, mention relevant scenario by name |
| **Hardware Requirements** | 01-architecture-overview.md | Others: link to 01, no duplication |

**Rules for Authors:**
1. If your task needs content from another doc's AUTHORITATIVE column, **link don't copy**
2. Include only the minimum context needed for comprehension
3. Use markdown links: `[See Configuration Reference](./09-configuration.md#backfill-settings)`
4. When a workflow doc mentions config, show a 2-3 key example max, then link for full reference

---

## Verification Strategy

### Design Documents - Objective Verification Methods

Each document will be verified using these **objective, reproducible methods**:

**1. Mermaid Diagram Validation:**
- Paste each mermaid code block into https://mermaid.live/
- Diagram must render without syntax errors
- If errors occur, fix syntax before committing

**2. Cross-Reference Verification:**
- All `[link text](./other-doc.md)` relative links must resolve
- Test by opening `design-docs/README.md` in GitHub web UI or VSCode markdown preview
- Each link must navigate to the correct section

**3. Placeholder Detection:**
- Search each document for: `TODO`, `TBD`, `FIXME`, `[placeholder]`, `[...]`, `...` on its own line
- Zero matches required (placeholders must be resolved)

**4. Required Content Verification:**
- For each task's acceptance criteria, grep/search for the required sections
- Example: Task 2 requires "Hardware requirements" → `grep -i "hardware requirements" 01-architecture-overview.md` must match

**5. Ledger Number Audit:**
- Search for 6+ digit numbers in each document
- Verify against canonical boundaries (see Mandatory Ledger Number Audit section)

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Start Immediately):
└── Task 1: Create README.md with overview and index

Wave 2 (After Wave 1):
├── Task 2: Create 01-architecture-overview.md
├── Task 3: Create 02-meta-store-design.md
├── Task 4: Create 03-backfill-workflow.md
├── Task 5: Create 04-streaming-workflow.md
├── Task 6: Create 05-transition-workflow.md
├── Task 7: Create 06-crash-recovery.md
├── Task 8: Create 07-query-routing.md
└── Task 9: Create 08-directory-structure.md and 09-configuration.md

Wave 3 (After Wave 2):
└── Task 10: Verification and cross-reference check
```

---

## TODOs

- [x] 1. Create design-docs/README.md

  **What to do**:
  - Create `design-docs/` directory
  - Create `README.md` with:
    - Overview of the service
    - Document index table
    - Quick reference (operating modes, key concepts)
    - Architecture diagram (ASCII)
    - Reading order recommendation
    - Existing code to reuse table
    - Key invariants list

  **Must NOT do**:
  - Include implementation details
  - Duplicate detailed content from sub-documents

  **Recommended Agent Profile**:
  - **Category**: `writing`
    - Reason: Documentation writing task
  - **Skills**: []
    - No special skills needed for documentation writing

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 1 (foundation)
  - **Blocks**: Tasks 2-9
  - **Blocked By**: None

  **References**:
  - `.sisyphus/drafts/full-history-rpc-design.md` - All content source
  - `.sisyphus/plans/stellar-full-history-rpc-design.md` - Previous full design

  **Existing Code to Reuse Table** (include in README):
  | Component | Location | What It Does | Reuse For |
  |-----------|----------|--------------|-----------|
  | LFS Ledger Store | `local-fs/ingestion/lfs-ledger-ingestion.go` | Chunk-based storage (10K ledgers), zstd compression | Immutable ledger stores |
  | RocksDB Ingestion | `rocksdb/ingestion-v2/` | LedgerBackend usage, batch processing | Active store ingestion patterns |
  | TxHash Workflow | `txhash-ingestion-workflow/workflow.go` | Phase state machine, RecSplit, crash recovery | Transition workflow patterns |
  | BufferedStorageBackend | `rocksdb/ingestion-v2/main.go` | GCS ledger source configuration | Backfill data source |

  **Key Invariants** (include in README, top 3-5):
  1. Range boundaries: First ledger = (N × 10M) + 2, Last ledger = ((N+1) × 10M) + 1
  2. Checkpoint info is NEVER deleted (persists forever in meta store)
  3. Transition only triggers in streaming mode, not backfill
  4. No gaps allowed: streaming mode validates all prior ranges are COMPLETE
  5. Batch size: 1000 ledgers (backfill), 1 ledger (streaming)

  **Acceptance Criteria**:
  - [ ] File `design-docs/README.md` exists
  - [ ] Contains document index with links to all 9 sub-documents
  - [ ] Contains architecture ASCII diagram
  - [ ] Contains quick reference tables
  - [ ] Contains "Existing Code to Reuse" table with code paths
  - [ ] Contains "Key Invariants" list (3-5 items)

  **Commit**: YES
  - Message: `docs: create design-docs README with index and overview`
  - Files: `design-docs/README.md`

---

- [x] 2. Create design-docs/01-architecture-overview.md

  **What to do**:
  - Create comprehensive architecture document with:
    - System components diagram (mermaid)
    - Data flow diagrams
    - Component responsibilities table
    - Store types (Active vs Immutable)
    - HTTP endpoints available per mode
    - Hardware requirements

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2 (with Tasks 3-9)
  - **Blocks**: Task 10
  - **Blocked By**: Task 1

  **References**:
  - `.sisyphus/drafts/full-history-rpc-design.md:392-430` - Architecture diagram
  - `random-readmes/requitements-for-full-history-rpc-service.md` - Requirements

  **Content to Include**:
  
  ```markdown
  # Architecture Overview
  
  ## System Components
  
  [Mermaid diagram showing HTTP Server, Query Router, Ingestion Engine, Meta Store, Active Stores, Immutable Stores]
  
  ## Component Responsibilities
  
  | Component | Responsibility |
  |-----------|----------------|
  | HTTP Server | Expose endpoints, handle requests |
  | Query Router | Route to correct store based on ledger/range |
  | Ingestion Engine | Process ledgers from LedgerBackend |
  | Meta Store | Track state, phases, checkpoints |
  | Active Stores | RocksDB for live data |
  | Immutable Stores | LFS + RecSplit for historical data |
  
  ## Data Flow
  
  [Diagram showing: LedgerBackend → Ingestion → Active Store → Transition → Immutable Store]
  
  ## Store Types
  
  ### Active Stores (RocksDB)
  - Ledger Store: LedgerSeq → Compressed LCM
  - TxHash Store: TxHash → LedgerSeq (16 CFs)
  
  ### Immutable Stores
  - LFS Chunks: 10K ledgers per chunk, zstd compressed
  - RecSplit: Minimal perfect hash, 16 index files
  
  ## Hardware Requirements
  - CPU: 32 cores
  - RAM: 128 GB
  - CaptiveStellarCore: ~8GB per instance
  - Disk: SSD recommended
  
  ## HTTP Endpoints by Mode
  
  | Mode | Available Endpoints |
  |------|---------------------|
  | Backfill | getHealth, getStatus (query endpoints unavailable) |
  | Streaming | getHealth, getStatus, getTransactionByHash, getLedgerBySequence |
  
  **Canonical Endpoint Names** (MUST USE):
  - getTransactionByHash (NOT "getTransaction")
  - getLedgerBySequence (NOT "getLedger")
  ```

  **Acceptance Criteria**:
  - [ ] File `design-docs/01-architecture-overview.md` exists
  - [ ] Contains mermaid system component diagram that renders in mermaid.live
  - [ ] Contains component responsibilities table with all 6 components listed
  - [ ] Contains data flow explanation with diagram
  - [ ] Contains "Hardware Requirements" section with CPU, RAM, Disk specs
  - [ ] Contains "HTTP Endpoints by Mode" section with canonical endpoint names
  - [ ] Uses canonical endpoint names (getTransactionByHash, getLedgerBySequence) throughout

  **Commit**: NO (group with Task 10)

---

- [x] 3. Create design-docs/02-meta-store-design.md

  **What to do**:
  - Create detailed meta store document with:
    - Complete key hierarchy
    - Enums (RangeState, LedgerPhase, TxHashPhase)
    - Range ID calculation formulas
    - **Scenario walkthroughs** showing how keys evolve:
      - Scenario 1: Fresh backfill
      - Scenario 2: Crash during backfill and recovery
      - Scenario 3: Backfill complete, start streaming
      - Scenario 4: Streaming mode - 10M boundary transition
      - Scenario 5: Gap detection
      - Scenario 6: Crash during transition
    - Key usage summary table

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocks**: Task 10
  - **Blocked By**: Task 1

  **References**:
  - `.sisyphus/drafts/full-history-rpc-design.md:434-536` - Key hierarchy, enums, range ID calculation
  - `.sisyphus/drafts/full-history-rpc-design.md:605-820` - **Scenario walkthroughs** (ALL 6 scenarios):
    - Scenario 1: Fresh Backfill (lines 609-668)
    - Scenario 2: Crash During Backfill and Recovery (lines 670-693)
    - Scenario 3: Backfill Complete, Start Streaming Mode (lines 695-721)
    - Scenario 4: Streaming Mode - 10M Boundary Transition (lines 723-759) **NOTE: Fix boundary example to use canonical 40,000,001**
    - Scenario 5: Gap Detection (lines 761-782)
    - Scenario 6: Crash During Transition (lines 784-806)

  **Content to Include** (CRITICAL - must have all scenarios):
  
  ```markdown
  # Meta Store Design
  
  ## Purpose
  The meta store is a single RocksDB instance tracking all state...
  
  ## Key Hierarchy
  
  ### Global Keys
  | Key | Type | Description |
  |-----|------|-------------|
  | global:mode | string | "backfill" or "streaming" |
  | global:last_processed_ledger | uint32 | Streaming progress |
  | global:backfill_start_ledger | uint32 | Backfill range start |
  | global:backfill_end_ledger | uint32 | Backfill range end |
  
  ### Per-Range Keys
  [Complete table with all keys]
  
  ## Enums
  
  ### RangeState
  | Value | Meaning |
  |-------|---------|
  | PENDING | Not started |
  | INGESTING | Active ingestion |
  | TRANSITIONING | Converting to immutable |
  | COMPLETE | Fully immutable |
  | FAILED | Error state |
  
  ## Range ID Calculation
  [Go code for ledgerToRangeID, rangeFirstLedger, rangeLastLedger]
  
  ## Scenario Walkthroughs
  
  ### Scenario 1: Fresh Backfill (Ledgers 2 to 30,000,001)
  
  **Command**: ./stellar-rpc --backfill --start-ledger 2 --end-ledger 30000001
  
  **t=0 Initial State**:
  [Show exact keys and values]
  
  **t=1 During Ingestion**:
  [Show keys updated]
  
  **t=2 Range Complete**:
  [Show final state]
  
  [Continue for all 6 scenarios from draft]
  ```

  **Acceptance Criteria**:
  - [ ] File `design-docs/02-meta-store-design.md` exists
  - [ ] Contains complete key hierarchy table
  - [ ] Contains all 6 scenario walkthroughs with example key values
  - [ ] Contains Range ID calculation formulas with Go code

  **Commit**: NO (group with Task 10)

---

- [x] 4. Create design-docs/03-backfill-workflow.md

  **What to do**:
  - Create detailed backfill workflow document with:
    - Command format and validation rules
    - Process lifecycle flowchart (mermaid)
    - Parallel orchestrator design
    - Checkpoint mechanism
    - Example: Processing ledgers 2 to 30,000,001 (ranges 0, 1, 2)
    - TOML configuration summary (2-3 key examples + link to 09-configuration.md per Document Responsibility Matrix)
    - Exit conditions (success vs failure)
    - **Use canonical range boundaries from Canonical Definitions section**

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocks**: Task 10
  - **Blocked By**: Task 1

  **References**:
  - `.sisyphus/drafts/full-history-rpc-design.md:268-290` - Backfill lifecycle
  - `.sisyphus/drafts/full-history-rpc-design.md:83-99` - Backfill architecture

  **Content to Include**:
  
  ```markdown
  # Backfill Workflow
  
  ## Overview
  Backfill mode ingests historical ledger ranges...
  
  ## Command Format
  
  ./stellar-rpc --backfill --start-ledger <N> --end-ledger <M>
  
  ### Validation Rules
  - --start-ledger: Must be `rangeFirstLedger(N)` for some N ≥ 0
    - Valid values: 2, 10000002, 20000002, 30000002, ...
    - Formula: `(N * 10,000,000) + 2`
  - --end-ledger: Must be `rangeLastLedger(N)` for some N ≥ 0
    - Valid values: 10000001, 20000001, 30000001, ...
    - Formula: `((N + 1) * 10,000,000) + 1`
  
  ## Process Flow
  
  [Mermaid flowchart: Start → Validate → Init Meta → Spawn Orchestrators → Process Ranges → Exit]
  
  ## Parallel Orchestrator Design
  
  - ~2 concurrent orchestrators recommended
  - Each processes one 10M range independently
  - Shared meta store, separate RocksDB instances
  
  [Mermaid diagram showing parallel orchestrators]
  
  ## Checkpoint Mechanism
  
  - Checkpoint every 1000 ledgers
  - Atomic update to meta store
  - Keys updated: last_committed_ledger, cf_counts
  
  ## Example: Processing Ledgers 2 to 30,000,001
  
  **Ranges Created**:
  - Range 0: 2 - 10,000,001
  - Range 1: 10,000,002 - 20,000,001
  - Range 2: 20,000,002 - 30,000,001
  
  **Execution (2 parallel)**:
  - Turn 1: Range 0 + Range 1
  - Turn 2: Range 2
  
  **Timeline**:
  [Show approximate timings]
  ```

  **Acceptance Criteria**:
  - [ ] File `design-docs/03-backfill-workflow.md` exists
  - [ ] Contains mermaid process flow diagram
  - [ ] Contains validation rules for command
  - [ ] Contains concrete example with 3 ranges

  **Commit**: NO (group with Task 10)

---

- [x] 5. Create design-docs/04-streaming-workflow.md

  **What to do**:
  - Create detailed streaming workflow document with:
    - Startup validation (gap detection)
    - Process lifecycle flowchart (mermaid)
    - Ingestion loop details
    - 10M boundary detection
    - Graceful shutdown sequence
    - Example: Starting from ledger 60,000,002

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocks**: Task 10
  - **Blocked By**: Task 1

  **References**:
  - `.sisyphus/drafts/full-history-rpc-design.md:292-315` - Streaming lifecycle
  - `.sisyphus/drafts/full-history-rpc-design.md:215-229` - Streaming startup

  **Content to Include**:
  
  ```markdown
  # Streaming Workflow
  
  ## Overview
  Streaming mode ingests real-time ledgers via CaptiveStellarCore...
  
  ## Startup Validation
  
  Before ingestion begins:
  1. Load meta store
  2. Scan all range:*:state keys
  3. Verify ALL are "COMPLETE"
  4. If any incomplete → LOG ERROR + EXIT
  5. Find last_processed_ledger
  6. Start CaptiveStellarCore from last_ledger + 1
  
  [Go code for validateNoGaps()]
  
  ## Ingestion Loop
  
  [Mermaid flowchart]
  
  - Batch size: 1 (checkpoint after every ledger)
  - Update meta store after each ledger
  - Check for 10M boundary after each ledger
  
  ## 10M Boundary Detection
  
  **MUST USE CANONICAL HELPERS** (from Canonical Definitions section):
  
  // Use the canonical shouldTriggerTransition from plan's Canonical Definitions
  func shouldTriggerTransition(ledgerSeq uint32) bool {
      rangeID := ledgerToRangeID(ledgerSeq)
      return ledgerSeq == rangeLastLedger(rangeID)
  }
  
  Trigger ledgers (last ledger of each range): 10000001, 20000001, 30000001, ...
  
  ## Graceful Shutdown
  
  1. Receive shutdown signal
  2. Finish processing current ledger
  3. Update meta store
  4. Close CaptiveStellarCore
  5. Exit cleanly
  
  ## Example: Starting from Ledger 60,000,002
  
  **Pre-conditions**:
  - Ranges 0-5 are COMPLETE (backfill finished)
  - global:last_processed_ledger = 60000001
  
  **Startup**:
  1. Validate: Ranges 0-5 all COMPLETE ✓
  2. Create Range 6 entry (INGESTING)
  3. Start CaptiveStellarCore from 60000002
  4. Begin ingestion loop
  ```

  **Acceptance Criteria**:
  - [ ] File `design-docs/04-streaming-workflow.md` exists
  - [ ] Contains startup validation logic with code
  - [ ] Contains mermaid ingestion loop diagram
  - [ ] Contains concrete example starting from ledger 60,000,002

  **Commit**: NO (group with Task 10)

---

- [x] 6. Create design-docs/05-transition-workflow.md

  **What to do**:
  - Create detailed transition workflow document with:
    - Trigger conditions
    - Parallel sub-workflows diagram (mermaid)
    - Ledger sub-flow: Active RocksDB → LFS chunks
    - TxHash sub-flow: Compact → RecSplit → Verify
    - Multiple Active Stores during transition (diagram)
    - Transition failure handling
    - Example: Transition at ledger 70,000,001 (last ledger of range 6)

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocks**: Task 10
  - **Blocked By**: Task 1

  **References**:
  - `.sisyphus/drafts/full-history-rpc-design.md:154-206` - Transition workflow
  - `.sisyphus/drafts/full-history-rpc-design.md:258-267` - Transition failure

  **Content to Include**:
  
  ```markdown
  # Active → Immutable Transition Workflow
  
  ## Overview
  When streaming mode hits a 10M ledger boundary...
  
  ## Trigger Conditions
  
  - ONLY in streaming mode (NOT backfill)
  - When ledger 10000001, 20000001, 30000001, etc. is ingested
    (last ledger of current range triggers transition)
  - Automatic, not manual
  
  ## Transition Process
  
  [Mermaid flowchart showing: Trigger → Create New Active → Spawn Transition → Parallel Ledger+TxHash → Delete Old]
  
  ## Parallel Sub-Workflows
  
  ### Ledger Sub-Flow
  
  | Phase | Description | Duration |
  |-------|-------------|----------|
  | WRITING_LFS | Read RocksDB, write 1000 chunks | ~30-60 min |
  | IMMUTABLE | Complete | - |
  
  ### TxHash Sub-Flow
  
  | Phase | Description | Duration |
  |-------|-------------|----------|
  | COMPACTING | Full compaction (16 CFs parallel) | ~5 min |
  | BUILDING_RECSPLIT | Build 16 indexes | ~15-20 min |
  | VERIFYING_RECSPLIT | Verify all keys | ~5 min |
  | COMPLETE | Ready | - |
  
  ## Multiple Active Stores During Transition
  
  [ASCII diagram showing 4 RocksDB instances: 2 current + 2 transitioning]
  
  ## Transition Failure Handling
  
  - Transitioning stores remain alive for queries
  - Goroutine retries until success
  - No data loss - ingestion continues to new stores
  
  ## Example: Transition at Ledger 70,000,001
  
  **Before (ledger 70,000,000)**:
  [Show meta store state - range 6 actively ingesting]
  
  **Trigger (ledger 70,000,001 ingested - last ledger of range 6)**:
  [Show what happens step by step]
  - Detect: shouldTriggerTransition(70000001) == true
  - Create NEW Active Stores for range 7
  - Spawn transition goroutine for range 6
  - Next ingested ledger (70,000,002) goes to range 7's Active Store
  
  **After (transition complete)**:
  [Show final state - range 6 COMPLETE, range 7 INGESTING]
  ```

  **Acceptance Criteria**:
  - [ ] File `design-docs/05-transition-workflow.md` exists
  - [ ] Contains mermaid parallel workflow diagram
  - [ ] Contains phase tables for both sub-flows
  - [ ] Contains concrete example at ledger 70,000,001 (last ledger of range 6, triggers transition)

  **Commit**: NO (group with Task 10)

---

- [x] 7. Create design-docs/06-crash-recovery.md

  **What to do**:
  - Create detailed crash recovery document with:
    - Checkpoint mechanism explanation
    - Recovery scenarios with step-by-step examples:
      - Crash during backfill ingestion
      - Crash after batch write, before checkpoint
      - Crash during compaction
      - Crash during transition
      - Crash during streaming
    - Why counts are always accurate
    - Gap detection logic

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocks**: Task 10
  - **Blocked By**: Task 1

  **References**:
  - `.sisyphus/drafts/full-history-rpc-design.md:605-820` - Scenario walkthroughs (same as Task 3)
  - `txhash-ingestion-workflow/README.md:679-920` - **Crash recovery section**: checkpoint patterns (line 679+), recovery scenarios (lines 697-785), count accuracy (lines 852+)

  **Content to Include**:
  
  ```markdown
  # Crash Recovery
  
  ## Overview
  The system implements robust crash recovery...
  
  ## Checkpoint Mechanism
  
  - Backfill: Every 1000 ledgers
  - Streaming: Every 1 ledger
  - Atomic via RocksDB WriteBatch
  
  [Go code for checkpoint function]
  
  ## Recovery Scenarios
  
  ### Scenario 1: Crash During Backfill Ingestion
  
  **Situation**: Crash at ledger 7,500,000 (mid-range 0)
  
  **State at crash**:
  range:0:state = "INGESTING"
  range:0:ledger:last_committed_ledger = 7499000
  
  **Recovery steps**:
  1. Operator runs same command
  2. Code loads meta store
  3. Finds range:0 incomplete
  4. Reads last_committed = 7499000
  5. Resumes from 7499001
  
  **Duplicate handling**:
  - Ledgers 7499001-7500000 may be in RocksDB already
  - Re-ingestion creates duplicates (same key→value)
  - Compaction deduplicates
  
  ### Scenario 2: Crash After Batch, Before Checkpoint
  [Detailed walkthrough]
  
  ### Scenario 3: Crash During Transition
  [Detailed walkthrough]
  
  ## Why Counts Are Always Accurate
  
  - cf_counts tracks CHECKPOINTED data, not all data in RocksDB
  - On resume, restore counts from checkpoint
  - Duplicates don't affect counts
  - Compaction removes duplicates, count stays accurate
  
  [Diagram showing count accuracy through crash]
  
  ## Gap Detection
  
  [Go code for gap detection on streaming startup]
  ```

  **Acceptance Criteria**:
  - [ ] File `design-docs/06-crash-recovery.md` exists
  - [ ] Contains at least 5 crash scenarios with detailed steps
  - [ ] Explains why counts stay accurate
  - [ ] Contains gap detection code

  **Commit**: NO (group with Task 10)

---

- [x] 8. Create design-docs/07-query-routing.md

  **What to do**:
  - Create query routing document with:
    - Router logic for getLedgerBySequence
    - Router logic for getTransactionByHash
    - Decision tree based on range state
    - Examples for each routing path
    - **Use canonical endpoint names from Canonical Definitions section**

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocks**: Task 10
  - **Blocked By**: Task 1

  **References**:
  - `.sisyphus/drafts/full-history-rpc-design.md:208-213` - Query routing during transition concept
  - `.sisyphus/plans/stellar-full-history-rpc-design.md:536-579` - **AUTHORITATIVE**: Full router logic with Go pseudocode and switch-on-range-state logic
  - `.sisyphus/plans/stellar-full-history-rpc-design.md:569-579` - Query routing example during transition

  **Error Semantics** (MUST DOCUMENT):
  - **COMPLETE / TRANSITIONING / INGESTING**: Route to appropriate store, return data
  - **PENDING**: Return error "range not yet ingested" (HTTP 404 with explanation)
  - **FAILED**: Return error "range ingestion failed" (HTTP 503 with explanation)
  - **Unknown range**: Return "ledger sequence out of range" (HTTP 404)
  
  The writer MUST use the pseudocode from `stellar-full-history-rpc-design.md:540-566` as the basis for the routing logic, updating endpoint names to canonical (`GetLedgerBySequence`, `GetTransactionByHash`).

  **Content to Include**:
  
  ```markdown
  # Query Routing
  
  ## Overview
  The Query Router directs requests to the correct data store...
  
  ## getLedgerBySequence Routing
  
  [Go pseudocode for GetLedgerBySequence function]
  
  ## getTransactionByHash Routing
  
  [Go pseudocode for GetTransactionByHash function]
  
  ## Decision Tree
  
  [Mermaid diagram: ledgerSeq → rangeID → check state → route to store]
  
  ## Examples
  
  ### Example 1: getLedgerBySequence(5000000) (Immutable Range)
  1. Calculate range: (5000000 - 2) / 10000000 = 0
  2. Check range:0:state = "COMPLETE"
  3. Route to: immutable/ledgers/range-0/
  4. Return LFS chunk lookup
  
  ### Example 2: getLedgerBySequence(65000000) (Active Range)
  [walkthrough]
  
  ### Example 3: getTransactionByHash During Transition
  [walkthrough showing transitioning store usage]
  ```

  **Acceptance Criteria**:
  - [ ] File `design-docs/07-query-routing.md` exists
  - [ ] Contains router pseudocode for getLedgerBySequence and getTransactionByHash
  - [ ] Contains at least 3 routing examples with concrete ledger numbers
  - [ ] Uses canonical endpoint names (getTransactionByHash, getLedgerBySequence)

  **Commit**: NO (group with Task 10)

---

- [x] 9. Create design-docs/08-directory-structure.md and 09-configuration.md

  **What to do**:
  - Create directory structure document with:
    - Complete file tree
    - Path conventions for LFS chunks
    - Path conventions for RecSplit indexes
    - Multi-disk considerations
  - Create configuration document with:
    - Complete TOML reference (all tables and keys)
    - Configuration validation rules
    - Example configurations for backfill and streaming modes

  **Recommended Agent Profile**:
  - **Category**: `writing`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 2
  - **Blocks**: Task 10
  - **Blocked By**: Task 1

  **References**:
  - `.sisyphus/drafts/full-history-rpc-design.md:540-601` - Directory structure with complete file tree
  - `.sisyphus/drafts/full-history-rpc-design.md:100-116` - Partial configuration (backfill backend selection)
  - `.sisyphus/plans/stellar-full-history-rpc-design.md` "## Configuration" section (~line 743+) - Example TOML config block
  - **THIS PLAN's "Canonical TOML Configuration Schema" section** - **AUTHORITATIVE for types, required/optional, and defaults**

  **Required TOML Tables (must document all)**:
  The configuration document MUST cover these top-level tables. Use the Canonical TOML Configuration Schema table in this plan for exact key definitions:
  - `[service]` - Service-level settings (port, mode, data directory)
  - `[meta_store]` - Meta store RocksDB settings
  - `[active_stores]` - Active store RocksDB settings (ledger + txhash)
  - `[immutable_stores]` - Immutable store paths and settings
  - `[rocksdb]` - Shared RocksDB tuning parameters
  - `[backfill]` - Backfill mode settings (ledger_backend choice, range, parallelism)
  - `[backfill.buffered_storage]` - GCS/S3 backend config
  - `[backfill.captive_core]` - CaptiveStellarCore backend config
  - `[streaming]` - Streaming mode settings (captive core config)

  **Multi-Disk Considerations** (for 08-directory-structure.md):
  Reference this plan's "Multi-Disk Configuration" section for scenarios to document.

  **Acceptance Criteria**:
  - [ ] File `design-docs/08-directory-structure.md` exists with complete file tree
  - [ ] File `design-docs/09-configuration.md` exists with TOML reference
  - [ ] 09-configuration.md documents ALL required TOML tables listed above
  - [ ] Each TOML key includes: type, required/optional, default value (if any), description
  - [ ] Contains at least 2 example configurations (backfill mode, streaming mode)
  - [ ] Contains LFS chunk path calculation code (pseudocode)
  - [ ] Directory structure matches canonical layout from draft:540-601

  **Commit**: NO (group with Task 10)

---

- [x] 10. Verification and Final Commit

  **What to do**:
  - Verify all 10 documents exist
  - Check mermaid diagrams render correctly (paste into mermaid.live)
  - Verify cross-references work (test links in markdown preview)
  - Run placeholder detection (search for TODO, TBD, FIXME, [...])
  - Run ledger number audit on all documents
  - Run final review
  - Commit all documents together

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: NO
  - **Parallel Group**: Wave 3 (final)
  - **Blocks**: None
  - **Blocked By**: Tasks 1-9

  **Verification Steps** (OBJECTIVE):
  1. **File existence**: `ls design-docs/*.md | wc -l` must equal 10
  2. **Mermaid validation**: For each mermaid block, paste into mermaid.live - must render without errors
  3. **Cross-reference check**: Open README.md in VSCode markdown preview, click each link - all must resolve
  4. **Placeholder detection**: `grep -r "TODO\|TBD\|FIXME\|\[placeholder\]\|\[\.\.\.\]" design-docs/` must return 0 matches
  5. **Ledger number audit**: Search each document for 6+ digit numbers, verify against canonical boundaries
     - Valid start ledgers: 2, 10000002, 20000002, ...
     - Valid end/trigger ledgers: 10000001, 20000001, 30000001, ...

  **Acceptance Criteria**:
  - [ ] All 10 files exist in design-docs/
  - [ ] All mermaid diagrams render in mermaid.live without syntax errors
  - [ ] No broken cross-references (all links resolve)
  - [ ] Zero placeholder text found (TODO, TBD, FIXME, etc.)
  - [ ] Ledger Number Audit completed on all 10 documents (no boundary violations)
  - [ ] Uses canonical endpoint names throughout (getTransactionByHash, getLedgerBySequence)
  - [ ] Git commit created

  **Commit**: YES
  - Message: `docs: add comprehensive design documentation for full history RPC service`
  - Files: `design-docs/*.md`

---

## Commit Strategy

| After Task | Message | Files |
|------------|---------|-------|
| 1 | `docs: create design-docs README with index and overview` | `design-docs/README.md` |
| 10 | `docs: add comprehensive design documentation for full history RPC service` | `design-docs/*.md` |

---

## Success Criteria

### Verification
- All 10 markdown files exist in `design-docs/`
- Documents are well-organized and cross-referenced
- Mermaid diagrams are valid syntax
- Examples are concrete with specific values

### Final Checklist
- [x] All deliverables present
- [x] No placeholder text
- [x] Cross-references work
- [x] Commit created
