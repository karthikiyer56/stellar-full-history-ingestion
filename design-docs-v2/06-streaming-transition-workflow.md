# Streaming Transition Workflow

## Overview

The streaming transition workflow converts a completed range from active RocksDB storage to immutable storage (LFS chunks + RecSplit index). It is triggered when a 10M-ledger range boundary is crossed during streaming ingestion, runs in a background goroutine, and proceeds concurrently with ingestion of the next range.

This workflow is **distinct from the backfill transition** — its input is two fully-populated active RocksDB stores (ledger store + txhash store), and it must handle live queries to the range while conversion proceeds.

**Important**: By the time the transition goroutine is spawned, most LFS chunks will already have been flushed during the ACTIVE phase (background per-chunk flush). Phase 1 of the transition only needs to flush any remaining chunks not yet done.

---

## Trigger Condition

Triggered in the streaming ingestion loop when:
```go
ledgerSeq == rangeLastLedger(currentRange)
```
(e.g., ledger 10,000,001 for range 0, ledger 20,000,001 for range 1, etc.)

At trigger:
- Current range's last ledger is committed to active RocksDB with WAL
- `streaming:last_committed_ledger` is updated to boundary ledger
- `range:N:state` set to `TRANSITIONING`
- Background goroutine spawned for transition
- New active store created for range N+1
- `range:N+1:state` set to `ACTIVE`
- Ingestion of range N+1 starts immediately

---

## Workflow Diagram

```mermaid
flowchart TD
    START(["Background goroutine starts for range N"]) --> SET_TRANS["range:N:state = TRANSITIONING<br/>(already set before goroutine spawned)"]
    SET_TRANS --> LFS_PHASE["Phase 1: Flush remaining LFS chunks"]
    LFS_PHASE --> CHUNK_LOOP["For each chunk 0..999 in range N"]
    CHUNK_LOOP --> LFS_DONE{lfs_done = '1'?}
    LFS_DONE -->|yes| MORE["skip (already flushed during ACTIVE)"]
    LFS_DONE -->|no| READ_CHUNK["Read 10K ledgers from range N ledger store<br/>(sequential scan by uint32BE key<br/>across the appropriate chunk's RocksDB store)"]
    READ_CHUNK --> WRITE_LFS["Write LFS chunk file:<br/>{chunkID:06d}.data + .index<br/>zstd-compressed LCM records + offset table<br/>fsync"]
    WRITE_LFS --> SET_LFS["Set range:N:chunk:{chunkID:06d}:lfs_done = 1"]
    SET_LFS --> MORE{more chunks?}
    MORE -->|yes| CHUNK_LOOP
    MORE -->|no| TXHASH_PHASE["Phase 2: RecSplit Build from TxHash Store"]

    TXHASH_PHASE --> SET_RS_STATE["Set range:N:recsplit:state = BUILDING"]
    SET_RS_STATE --> SPAWN_CFS["Spawn 16 goroutines (one per CF: 0..f)<br/>All goroutines run concurrently"]
    SPAWN_CFS --> CF_DONE2{cf:XX:done = 1?<br/>per goroutine}
    CF_DONE2 -->|yes| NEXT_CF["Goroutine exits immediately<br/>(CF already built)"]
    CF_DONE2 -->|no| SCAN_CF["Scan txhash store CF X<br/>iterate all keys where first hex char of txhash == X<br/>build RecSplit MPH over (txhash, ledgerSeq) pairs<br/>write immutable/txhash/{N:04d}/index/cf-{X}.idx<br/>fsync"]
    SCAN_CF --> SET_CF["Set range:N:recsplit:cf:{X:02d}:done = 1"]
    SET_CF --> NEXT_CF
    NEXT_CF --> ALL_DONE{"All 16 goroutines<br/>complete?"}
    ALL_DONE -->|no| WAIT["Wait"]
    ALL_DONE -->|yes| VERIFY["Verify: spot-check random ledgers and txhashes<br/>against new immutable stores"]
    VERIFY --> SET_COMPLETE["range:N:recsplit:state = COMPLETE<br/>range:N:state = COMPLETE<br/>(meta store written before router swap)"]
    SET_COMPLETE --> ADD_IMM["router.AddImmutableStores(N, lfs, recsplit)<br/>queries for range N now route to LFS + RecSplit"]
    ADD_IMM --> DELETE_ACTIVE["router.RemoveTransitioningStores(N)<br/>close + delete active ledger store + txhash store for range N<br/>(safe: routing already swapped to immutable)"]
    DELETE_ACTIVE --> DONE(["Range N transition complete"])
```

**Query routing during transition**: While `range:N:state == "TRANSITIONING"`, both active RocksDB stores for range N remain open. All queries for range N ledgers/transactions are served from them. The immutable LFS and RecSplit files are not used for queries until `AddImmutableStores` completes and routes the QueryRouter to immutable stores. The active stores are only deleted **after** the router swap — there is no query gap.

**Critical ordering**: `SET_COMPLETE` (meta store) → `AddImmutableStores` (router swap) → `RemoveTransitioningStores` (delete). Deletion always happens last, after routing is already pointed at immutable stores.

---

## Phase 1: LFS Chunk Writes

Each chunk (10K ledgers) is read from the active ledger store and written to an LFS chunk file.

**Which stores are read**: By the time the transition goroutine spawns, the streaming ingestion loop has already moved to range N+1's first `ledger-store-chunk-{N*1000:06d}/`. Range N's ledgers are spread across up to 1000 chunk stores (`ledger-store-chunk-{rangeFirstChunk:06d}/` through `ledger-store-chunk-{rangeLastChunk:06d}/`). Most of these were already flushed to LFS during the ACTIVE phase (background per-chunk flush). The transition goroutine reads only from the chunk stores that haven't yet been flushed.

**Read method**: Sequential scan by `uint32BE(ledgerSeq)` key within each chunk's RocksDB store (default CF). The data is already zstd-compressed in RocksDB; it is written as-is into the LFS chunk format with an offset index for random access.

**Transitioning pointer**: The `transitioningLedgerStore` in the QueryRouter holds the **last chunk store of range N** (e.g. `ledger-store-chunk-005999/` for range 5). This is the store that was current at the range boundary and is still open for reads during transition. The transition goroutine may need to open earlier chunk stores (e.g. `ledger-store-chunk-005000/` through `ledger-store-chunk-005998/`) directly from disk if they still need to be flushed.

**LFS chunk file format**:
- `.data` file: contiguous compressed LCM records (variable-length)
- `.index` file: offset table, one `uint64` per ledger, enabling O(1) random access

**Flush/fsync**: Each chunk file pair is fsynced before setting `lfs_done`. Partial writes are safe — the `lfs_done` flag is the sole indicator of completion.

---

## Phase 2: RecSplit Build from Active Store

Unlike backfill (which reads raw flat files), the streaming transition reads directly from the active txhash store. All 16 CF index files are built **in parallel** — 16 goroutines run concurrently, one per CF (`0`–`f`). Each goroutine independently:

1. Checks `recsplit:cf:{X:02d}:done` — if already `"1"`, exits immediately (crash resume)
2. Iterates all keys in txhash store CF `X` (the CF whose name matches the first hex character of the txhash; `key[0] >> 4 == X` in raw byte terms)
3. Builds RecSplit minimal perfect hash over the matching `(txhash, ledgerSeq)` pairs
4. Writes `immutable/txhash/{rangeID:04d}/index/cf-{X}.idx`
5. fsyncs
6. Sets `range:N:recsplit:cf:{X:02d}:done = 1` in meta store

The orchestrator waits for all 16 goroutines to complete before proceeding to verification.

The active RocksDB store is read-only during RecSplit build (ingestion has moved to range N+1's store).

**Note**: The streaming transition does **not** produce raw txhash flat files. It builds RecSplit directly from RocksDB. This is the primary structural difference from the backfill transition.

---

## Verification Step

Before deleting the active store, the workflow performs a spot-check:

1. Sample 100 random ledger sequence numbers from range N
2. Read each from the new LFS chunk file → compare against RocksDB value
3. Sample 100 random txhashes from range N
4. Look up each in the RecSplit index → verify by fetching the ledger from LFS and confirming presence

If any mismatch is detected: ABORT; do not delete active store; log error; set range to error state.

---

## State Transitions in Meta Store

```
At transition trigger (ledger 10,000,001, range 0):
  range:0000:state                     →  "TRANSITIONING"
  streaming:last_committed_ledger      →  10,000,001
  range:0001:state                     →  "ACTIVE"

Phase 1 progresses (chunk writes, range 0 = global chunks 000000–000999):
  range:0000:chunk:000000:lfs_done     →  "1"
  range:0000:chunk:000001:lfs_done     →  "1"
  ...
  range:0000:chunk:000999:lfs_done     →  "1"   ← last chunk of range 0 (global ID 000999)

Phase 2 starts:
  range:0000:recsplit:state            →  "BUILDING"

Per-CF progress:
  range:0000:recsplit:cf:00:done       →  "1"
  ...
  range:0000:recsplit:cf:0f:done       →  "1"

Verification passes, router swap and active store deleted:
  range:0000:recsplit:state            →  "COMPLETE"
  range:0000:state                     →  "COMPLETE"
  # router.AddImmutableStores(0, ...) called next — queries now route to LFS + RecSplit
  # router.RemoveTransitioningStores(0) called last — active stores deleted from disk
```

For contrast, range 5 (global chunks 005000–005999):
```
At transition trigger (ledger 50,000,001, range 5):
  range:0005:state                     →  "TRANSITIONING"
  streaming:last_committed_ledger      →  50,000,001
  range:0006:state                     →  "ACTIVE"

Phase 1 chunk writes (global IDs 005000–005999):
  range:0005:chunk:005000:lfs_done     →  "1"   ← first chunk of range 5 (global ID 005000)
  ...
  range:0005:chunk:005999:lfs_done     →  "1"   ← last chunk of range 5 (global ID 005999)

Phase 2 RecSplit:
  range:0005:recsplit:state            →  "BUILDING"
  range:0005:recsplit:cf:00:done       →  "1"
  ...
  range:0005:recsplit:cf:0f:done       →  "1"
  range:0005:recsplit:state            →  "COMPLETE"
  range:0005:state                     →  "COMPLETE"
  # router.AddImmutableStores(5, ...) → router.RemoveTransitioningStores(5)
```

---

## Crash Recovery

If the streaming daemon crashes while the transition goroutine is running:

1. On restart: read all range states
2. If `range:N:state == "TRANSITIONING"`: resume transition
3. Scan `lfs_done` flags — skip completed chunks in Phase 1
4. Scan `recsplit:cf:XX:done` flags — skip completed CFs in Phase 2
5. Active store is still intact (WAL-backed); all reads for resume are from it

**Active store is never deleted until all flags are set and verification passes**. Crash recovery is always safe.

---

## Relationship to Streaming Ingestion

```mermaid
flowchart LR
    T0(["Ledger 10,000,001 committed"]) --> SPAWN["Spawn transition goroutine for range 0"]
    T0 --> NEWSTORE["Create active store for range 1"]
    NEWSTORE --> INGEST1["Continue ingesting range 1<br/>(ledgers 10,000,002+)"]
    SPAWN --> TRANS0["Transition range 0:<br/>LFS chunks + RecSplit<br/>(runs concurrently)"]
    INGEST1 -.->|"queries for range 0<br/>served from still-open<br/>range 0 active store"| TRANS0
    TRANS0 --> COMPLETE["range:0000:state = COMPLETE<br/>Active store deleted"]
```

---

## Error Handling

| Error | Action |
|-------|--------|
| RocksDB read failure during LFS write | ABORT transition; do not set `lfs_done`; log; daemon restarts and resumes |
| LFS file write/fsync failure | ABORT transition; do not set `lfs_done` |
| RecSplit build failure | ABORT transition; do not set `cf:XX:done` |
| Verification mismatch | ABORT; do NOT delete active store; log; operator intervention required |
| Active store delete failure | LOG and continue; store will be cleaned up on next run |

---

## getEvents Immutable Store — Placeholder

> **Status**: Not yet designed. This section reserves space for future work.

When `getEvents` support is added to the streaming transition workflow, it will require:

- **Phase 3: Events index build** — after RecSplit completes, before active stores deletion
- Input: events data from the separate active events RocksDB store (written during streaming ingestion)
- Output: `immutable/events/{rangeID:04d}/index/` — events index files
- Meta store tracking: `range:N:events_index:state` and per-partition done flags
- Verification step extends to include events: spot-check random events against new index
- No active store is deleted until Phase 1 (LFS), Phase 2 (RecSplit), and Phase 3 (events index) all complete

The workflow diagram above will gain a Phase 3 branch between `VERIFY` and `DELETE_ACTIVE`.

---

## Related Documents

- [04-streaming-workflow.md](./04-streaming-workflow.md) — trigger conditions and concurrency model
- [02-meta-store-design.md](./02-meta-store-design.md) — state keys written during transition
- [07-crash-recovery.md](./07-crash-recovery.md) — streaming transition crash scenarios
- [08-query-routing.md](./08-query-routing.md) — query routing during TRANSITIONING state
- [05-backfill-transition-workflow.md](./05-backfill-transition-workflow.md) — contrast: backfill transition uses raw flat files
