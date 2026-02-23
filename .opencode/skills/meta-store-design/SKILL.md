---
name: meta-store-design
description: RocksDB-backed meta store key hierarchy, state enums, and sub-workflow tracking for the Stellar ingestion pipeline
---

# Meta Store Design

## Backend

The meta store is **RocksDB-backed** key-value store. All keys are strings; values are strings or encoded integers.

## Key Hierarchy

### Global Keys

| Key | Type | Description |
|-----|------|-------------|
| `global:mode` | string | Ingestion mode: `backfill` or `streaming` |
| `global:last_processed_ledger` | uint32 | Last fully processed ledger |
| `global:backfill_start_ledger` | uint32 | Backfill range start ledger |
| `global:backfill_end_ledger` | uint32 | Backfill range end ledger |

### Per-Range Keys

| Key | Type | Description |
|-----|------|-------------|
| `range:{id}:state` | RangeState | Current state of this range |
| `range:{id}:start_ledger` | uint32 | First ledger of this range |
| `range:{id}:end_ledger` | uint32 | Last ledger of this range |
| `range:{id}:created_at` | timestamp | When range was created |
| `range:{id}:completed_at` | timestamp | When range completed (set on COMPLETE) |

### Ledger Sub-Workflow Keys

| Key | Type | Description |
|-----|------|-------------|
| `range:{id}:ledger:phase` | LedgerPhase | Current phase of ledger processing |
| `range:{id}:ledger:last_committed_ledger` | uint32 | Last durably checkpointed ledger |
| `range:{id}:ledger:count` | int64 | Ledgers processed (checkpointed with progress) |
| `range:{id}:ledger:lfs_last_chunk_written` | int32 | Last chunk written to LFS; **-1 sentinel = none written yet** |
| `range:{id}:ledger:immutable_path` | string | Path to immutable LFS store (set when IMMUTABLE) |

### TxHash Sub-Workflow Keys

| Key | Type | Description |
|-----|------|-------------|
| `range:{id}:txhash:phase` | TxHashPhase | Current phase of txhash processing |
| `range:{id}:txhash:last_committed_ledger` | uint32 | Last durably checkpointed ledger |
| `range:{id}:txhash:cf_counts` | string | Per-CF counts: `"0:N,1:M,...,f:K"` |
| `range:{id}:txhash:rocksdb_path` | string | Path to active RocksDB store |
| `range:{id}:txhash:recsplit_path` | string | Path to built RecSplit index |

### Transition Keys (Temporary)

These keys exist only during a range transition. Cleaned up after transition completes.

| Key | Type | Description |
|-----|------|-------------|
| `range:{id}:transition:started_at` | timestamp | When transition began |
| `range:{id}:transition:ledger_status` | string | Ledger sub-flow transition status |
| `range:{id}:transition:txhash_status` | string | TxHash sub-flow transition status |

## State Machine Enums

### RangeState
```
PENDING → INGESTING → TRANSITIONING → COMPLETE
                              ↓
                           FAILED
```

### LedgerPhase
```
INGESTING → COMPACTING → WRITING_LFS → IMMUTABLE
```

### TxHashPhase
```
INGESTING → COMPACTING → BUILDING_RECSPLIT → VERIFYING_RECSPLIT → COMPLETE
```

## Notes

- `cf_counts` compact format: `"0:N,1:M,2:K,...,f:Z"` — one entry per column family (0-9, a-f)
- `lfs_last_chunk_written = -1` sentinel means LFS writing has not started
- Each sub-workflow (ledger, txhash) tracks its own `last_committed_ledger` independently
- Counts are checkpointed WITH progress — not recomputed from store on restart
