---
name: architecture
description: Complete architecture reference for stellar-full-history-ingestion — package layout, store design, meta store keys, range/chunk math, query routing, crash recovery, and design doc conventions
---

# Architecture Reference

Authoritative reference for `design-docs-v2/`. When in doubt, these docs are truth:
- `01-architecture-overview.md` — two-pipeline overview
- `02-meta-store-design.md` — all meta keys and state enums
- `04-streaming-workflow.md` — streaming ingestion loop
- `06-streaming-transition-workflow.md` — ACTIVE → COMPLETE goroutine
- `07-crash-recovery.md` — all crash scenarios (S1–S5, B1–B7)
- `08-query-routing.md` — routing matrix by range state
- `09-directory-structure.md` — full on-disk file tree
- `11-checkpointing-and-transitions.md` — boundary math and formulas

---

## Two-Pipeline Architecture

| Dimension | Backfill | Streaming |
|-----------|----------|-----------|
| Backend | BSB (BufferedStorageBackend) | CaptiveStellarCore |
| Active store | None — writes direct to LFS + raw txhash flat files | Two RocksDB instances per range |
| Queries | Not served | `getLedgerBySequence`, `getTransactionByHash` |
| RecSplit trigger | All 1,000 chunks done for a range | 10M-ledger range boundary |
| Process lifecycle | Exits when all ranges complete | Long-running daemon |
| Flush interval | ~100 ledgers (write buffer cap) | Per-ledger checkpoint to meta store |

**BSB and CaptiveStellarCore are mutually exclusive** — never conflate them.

---

## Active Store Architecture (Streaming Only)

Two **separate RocksDB instances** per active range — NOT one instance with two CFs:

| Store | Path | Schema | Column Families |
|-------|------|--------|-----------------|
| Ledger store | `<active_stores_base_dir>/ledger-store-chunk-{chunkID:06d}/` | `key=uint32BE(ledgerSeq)`, `value=zstd(LCM)` | **None** — default CF only |
| TxHash store | `<active_stores_base_dir>/txhash-store-range-{rangeID:04d}/` | `key=txhash[32]`, `value=uint32BE(ledgerSeq)` | **16 CFs**, one per nibble `0`–`f`; route via `txhash[0] >> 4` |

At most one active range exists at a time. Both stores stay open for queries until the transition goroutine sets `COMPLETE` and deletes them.

---

## Directory Structure

```
{data_dir}/
├── meta/rocksdb/                              ← Meta store (WAL NEVER disabled)
├── <active_stores_base_dir>/
│   ├── ledger-store-chunk-{chunkID:06d}/      ← Streaming only; default CF; one per 10K-ledger chunk
│   └── txhash-store-range-{rangeID:04d}/       ← Streaming only; 16 CFs by nibble; one per 10M-ledger range
└── immutable/
    ├── ledgers/chunks/{XXXX}/{YYYYYY}.data    ← LFS chunk files (+ .index)
    └── txhash/{rangeID:04d}/
        ├── raw/{chunkID:06d}.bin              ← Backfill only; 36B/entry; deleted after RecSplit
        └── index/cf-{nibble}.idx              ← RecSplit index, 16 files per range
```

No `transitioning/` directory — all transition state lives in the meta store only.  
No `global:mode` key — mode determined by `--mode` startup flag.

---

## Range & Chunk Math

```go
const (
    FirstLedger    = 2
    RangeSize       = 10_000_000
    ChunkSize        = 10_000
    ChunksPerRange   = 1_000
)

ledgerToRangeID(seq)  = (seq - 2) / 10_000_000
rangeFirstLedger(N)   = (N * 10_000_000) + 2
rangeLastLedger(N)    = ((N+1) * 10_000_000) + 1

ledgerToChunkID(seq)  = (seq - 2) / 10_000
chunkFirstLedger(C)   = (C * 10_000) + 2
chunkLastLedger(C)    = ((C+1) * 10_000) + 1
chunkToRangeID(C)     = C / 1000
```

**Transition triggers** (streaming): `10,000,001 / 20,000,001 / 30,000,001 / …`  
Pattern: `((N+1) × 10,000,000) + 1` for range N.

### Range Boundaries (First 5)

| Range | First Ledger | Last Ledger | Chunk IDs |
|-------|-------------|------------|-----------|
| 0 | 2 | 10,000,001 | 0–999 |
| 1 | 10,000,002 | 20,000,001 | 1000–1999 |
| 2 | 20,000,002 | 30,000,001 | 2000–2999 |

### File Paths

```
LFS chunk:    immutable/ledgers/chunks/{chunkID/1000:04d}/{chunkID:06d}.data
Raw txhash:   immutable/txhash/{rangeID:04d}/raw/{chunkID:06d}.bin
RecSplit CF:  immutable/txhash/{rangeID:04d}/index/cf-{nibble}.idx
```

---

## Meta Store Key Hierarchy

Meta store: single RocksDB at `meta/rocksdb/`. WAL is **NEVER** disabled. Present in both modes.

### Per-Range State Keys

| Key | Value | Notes |
|-----|-------|-------|
| `range:{N:04d}:state` | `INGESTING` / `RECSPLIT_BUILDING` / `ACTIVE` / `TRANSITIONING` / `COMPLETE` | Primary state machine key |
| `range:{N:04d}:recsplit:state` | `BUILDING` / `COMPLETE` | Backfill only |
| `range:{N:04d}:recsplit:cf:{XX:02d}:done` | `"1"` | Per-CF done flag; backfill + streaming |

### Per-Chunk Flags

| Key | Set When | Mode |
|-----|----------|------|
| `range:{N:04d}:chunk:{C:06d}:lfs_done` | After LFS `.data`+`.index` fsynced | Backfill (at chunk end) + Streaming (background goroutine during ACTIVE, or Phase 1 during TRANSITIONING) |
| `range:{N:04d}:chunk:{C:06d}:txhash_done` | After `.bin` fsynced | **Backfill only** — never written for streaming ranges |

**Chunk skip rule**:
- Backfill: both `lfs_done="1"` AND `txhash_done="1"` → skip
- Streaming (transition resume): `lfs_done="1"` → skip (no `txhash_done` for streaming)

### Streaming Checkpoint

```
streaming:last_committed_ledger = uint32BE(ledgerSeq)
```
Written after every successful WriteBatch to both active stores. Resume: `last_committed_ledger + 1`.

### State Machines

**Backfill**: `INGESTING` → `RECSPLIT_BUILDING` → `COMPLETE`  
**Streaming**: `ACTIVE` → `TRANSITIONING` → `COMPLETE`  
**RecSplit sub-state**: `PENDING` → `BUILDING` → `COMPLETE`

---

## Background LFS Flush During ACTIVE (Streaming)

While a range is `ACTIVE`, a background goroutine flushes completed 10K-ledger chunks from the **ledger store** → LFS at each chunk boundary:

1. After ledger `chunkLastLedger(C)` committed → read chunk C from ledger store → write `.data`+`.index` → fsync → set `lfs_done="1"`
2. Range state stays `ACTIVE` — this is not a transition
3. TxHash store is **not** flushed during ACTIVE; RecSplit build only happens in TRANSITIONING Phase 2

**Effect on transition**: By the time `TRANSITIONING` is set, most `lfs_done` flags are already set. Phase 1 of the transition goroutine only flushes remaining chunks (typically just the last partial chunk).

---

## Streaming Transition Goroutine (Background)

Spawned at `rangeLastLedger(N)`. Runs concurrently with range N+1 ingestion.

```
Phase 1: Scan lfs_done flags for all 1000 chunks of range N
         Chunks with lfs_done="1" → skip (flushed during ACTIVE)
         Chunks without → read from ledger store → write LFS → fsync → set lfs_done="1"

Phase 2: Build RecSplit from txhash store (16 CFs by nibble)
         After each CF: set recsplit:cf:XX:done="1"

Phase 3: Spot-check verify LFS (100 random ledgers) + RecSplit (100 random txhashes)

Phase 4: Set range:N:state = "COMPLETE"

Phase 5: Delete ledger store + txhash store directories
```

---

## Query Routing Matrix

Queries served in streaming mode only. Routing by range state:

| Range State | `getLedgerBySequence` | `getTransactionByHash` |
|-------------|----------------------|----------------------|
| `ACTIVE` | `<active_stores_base_dir>/ledger-store-chunk-{chunkID:06d}/` (default CF) | `<active_stores_base_dir>/txhash-store-range-{rangeID:04d}/` (CF for nibble) |
| `TRANSITIONING` | `<active_stores_base_dir>/ledger-store-chunk-{chunkID:06d}/` (still open) | `<active_stores_base_dir>/txhash-store-range-{rangeID:04d}/` (still open) |
| `COMPLETE` | `immutable/ledgers/chunks/{XXXX}/{YYYYYY}.data` | `immutable/txhash/{N:04d}/index/cf-{nibble}.idx` |

`getTransactionByHash` probes ranges newest→oldest: ACTIVE first, TRANSITIONING, then COMPLETE (RecSplit).

**RecSplit false positives**: RecSplit always returns a candidate (no "not found"). Protocol:
1. Get `candidate_ledger = recsplit.Lookup(txHash)`
2. Fetch `LedgerCloseMeta` for `candidate_ledger`
3. Scan tx list — if no match → false positive → continue to next range

---

## Crash Recovery

### Core Invariants

1. Flags (`lfs_done`, `txhash_done`, `recsplit:cf:XX:done`) written **after** fsync — never before
2. Chunk flags are permanent — never deleted once set
3. `streaming:last_committed_ledger` written after WriteBatch succeeds
4. Active stores never deleted until verification passes and `COMPLETE` is set
5. Meta store WAL is **never** disabled — entire crash recovery model depends on it
6. Backfill BSB instances run in parallel → non-contiguous chunk completion is normal, not exceptional

### Streaming Scenarios

| Scenario | State at Crash | Recovery |
|----------|---------------|----------|
| S1: Mid-ingestion | `ACTIVE` | Resume from `last_committed_ledger + 1`; both active stores intact via WAL |
| S2: Phase 1 (LFS flush) of transition | `TRANSITIONING`, some `lfs_done` absent | Scan `lfs_done` flags; rewrite missing chunks from ledger store |
| S3: Phase 2 (RecSplit) of transition | `TRANSITIONING`, all `lfs_done` set, some CF flags absent | Skip Phase 1; rebuild missing CFs from txhash store |
| S4: After verify, before store delete | `TRANSITIONING`, all flags set | Re-verify; delete both stores; set `COMPLETE` |
| S5: After COMPLETE, stores still on disk | `COMPLETE` | Both stores are orphaned — delete on startup; route to immutable |

### Backfill Resume Rule

On restart: scan **all 1,000 chunk flag pairs** for each `INGESTING` range (no early exit — gaps are non-contiguous). Per-chunk decision:

| lfs_done | txhash_done | Action |
|----------|-------------|--------|
| `"1"` | `"1"` | Skip |
| `"1"` | absent | Re-fetch for txhash only; keep LFS file |
| absent | `"1"` | Re-fetch for LFS only; keep txhash file (rare) |
| absent | absent | Delete partial files; full rewrite |

---

## Package Structure

```
ingestion-workflow/
  internal/
    orchestrator/     main workflow (backfill + streaming)
    transition/       range transition logic
    stores/
      meta/           meta store (RocksDB)
      lcm/            ledger store (RocksDB, default CF)
      txhash/         txhash store (RocksDB, 16 CFs)
    backend/          BSB or CaptiveStellarCore
    config/           TOML config structs

helpers/
  helpers.go          formatting, encoding, math utilities
  lfs/                LFS path helpers, ledger iterator
```

### Common CLI Flags

```
--mode              backfill | streaming
--config            path to TOML config
--lfs-store         path to LFS ledger store
--start-ledger      first ledger to process
--end-ledger        last ledger to process
--block-cache-mb    RocksDB block cache size in MB
```

---

## Design Doc Conventions

- **NEVER ASCII art** — mermaid diagrams only (`flowchart` preferred; `sequenceDiagram` only for protocol handshakes)
- **`design-docs-v2/`** is authoritative — do not modify `design-docs/` (v1, read-only)
- Doc structure: Overview → Diagram → Design → Data Model → Error Handling
- Every line carries information; no filler prose
- File naming: `NN-short-name.md` (zero-padded number)
- **Mermaid diagrams: plain black/white ONLY** — never use `classDef` color blocks or `:::className` annotations; all diagrams must render with default Mermaid theming
- **MANDATORY: Verify Mermaid renders correctly** — after writing any Mermaid diagram, open a Markdown preview (e.g. VS Code preview, GitHub preview, or mermaid.live) and confirm the diagram renders without errors before considering the doc complete

### Active Store Naming Conventions

| Store | Path Pattern | Transition Cadence |
|-------|-------------|-------------------|
| Ledger store | `<active_stores_base_dir>/ledger-store-chunk-{chunkID:06d}/` | Every 10K ledgers (chunk boundary) |
| TxHash store | `<active_stores_base_dir>/txhash-store-range-{rangeID:04d}/` | Every 10M ledgers (range boundary) |

`<active_stores_base_dir>` is the operator-configured path for active stores (default: `{data_dir}/active`). `{chunkID:06d}` is the current chunk ID (6 digits). `{rangeID:04d}` is the range ID (4 digits). These are the naming conventions for ALL docs — do not invent alternative prefixes.