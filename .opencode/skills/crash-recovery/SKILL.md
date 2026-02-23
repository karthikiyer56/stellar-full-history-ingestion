---
name: crash-recovery
description: Checkpoint strategy, recovery procedures for all 6 crash scenarios, transition independence, and error handling rules
---

# Crash Recovery

## Checkpoint Strategy

| Mode | Frequency | Mechanism |
|------|-----------|-----------|
| Backfill | Every 1,000 ledgers | Atomic `RocksDB WriteBatch` |
| Streaming | Every 1 ledger | Atomic `RocksDB WriteBatch` |

All checkpoints are atomic — either all keys in the batch are written, or none are.

## The Golden Resume Rule

> **Always resume from `last_committed_ledger + 1`**

Additional rules:
- Counts are checkpointed WITH progress (not recomputed from store on restart)
- Duplicates are harmless — compaction deduplicates; same key always produces same value
- Each sub-workflow (ledger, txhash) tracks its own `last_committed_ledger` independently

## 6 Crash Scenarios

### 1. Crash During Ingestion (Before Checkpoint)
- **Recovery:** Resume from `last_committed_ledger + 1`; re-ingest lost ledgers
- Duplicate entries produced by re-ingestion are harmless

### 2. Crash After Batch But Before Checkpoint Write
- **Recovery:** Same as #1 — `last_committed_ledger` not updated; re-process from that point

### 3. Crash During LFS Writing
- **Recovery:** Resume from `lfs_last_chunk_written + 1` (persisted in meta store)
- Re-write incomplete chunks; already-written chunks are skipped

### 4. Crash During Compaction
- **Recovery:** Restart compaction from beginning — compaction is idempotent
- No partial state to clean up

### 5. Crash During RecSplit Build
- **Recovery:** Rebuild RecSplit index from scratch — cannot resume partial builds
- Delete partial index files before restarting

### 6. Crash During Streaming Mode
- **Recovery:** Lose at most 1 ledger (single-ledger checkpoint interval)
- Resume from last checkpoint

## Transition Sub-Flow Independence

The ledger sub-flow and txhash sub-flow recover **independently** from each other:
- Each has its own `last_committed_ledger` in meta store
- A crash in one sub-flow does NOT require restarting the other
- Transition keys (`range:{id}:transition:*`) track each sub-flow's status separately

## Gap Detection (Streaming Mode)

On startup in streaming mode:
1. Validate ALL prior ranges are in `COMPLETE` state
2. If any gap detected → **ABORT** with error
3. Data integrity is at risk if prior ranges are not complete

## Error Handling

| Error Type | Action |
|------------|--------|
| Parse / data errors | **ABORT** — data integrity issue |
| Storage errors (RocksDB/disk failure) | **ABORT** — storage failure |
| Config / flag errors | **ABORT** — invalid configuration |
| Verification mismatch | **LOG** and **CONTINUE** |
| Missing optional files | **LOG** and **CONTINUE** |
