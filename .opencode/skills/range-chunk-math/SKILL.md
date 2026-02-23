---
name: range-chunk-math
description: Range and chunk boundary formulas, transition triggers, and path conventions for Stellar ledger ingestion
---

# Range & Chunk Math

## ⚡ TRANSITION TRIGGERS (Critical for Workflow Decisions)

A **range transition** fires when the current ledger sequence is the LAST ledger in a range.

```
shouldTriggerTransition(seq) = seq == rangeLastLedger(ledgerToRangeID(seq))
```

**Transition fires at these ledger sequences:** `10000001, 20000001, 30000001, ...`

When `seq` hits a transition trigger:
1. Current range is COMPLETE
2. Begin TRANSITIONING state (compact → build RecSplit → verify → write LFS)
3. Start new range

## Formulas

### Range Math
```
ledgerToRangeID(seq)   = (seq - 2) / 10_000_000
rangeFirstLedger(id)   = (id * 10_000_000) + 2
rangeLastLedger(id)    = ((id + 1) * 10_000_000) + 1
```

### Chunk Math
```
LedgerToChunkID(seq)   = (seq - 2) / 10_000
chunkFirstLedger(id)   = (id * 10_000) + 2
chunkLastLedger(id)    = ((id + 1) * 10_000) + 1
```

### Checkpoint Math
```
isCheckpoint(seq)      = (seq - 1) % 1000 == 0 && seq > 1
```

## Constants

| Constant | Value |
|----------|-------|
| `FirstLedger` | 2 |
| `RangeSize` | 10,000,000 |
| `ChunkSize` | 10,000 |
| `CheckpointInterval` | 1,000 |

## Relationships

```
1 range  = 10,000,000 ledgers = 1,000 chunks
1 chunk  = 10,000 ledgers
```

## Key Sequences

- **Checkpoint sequence:** 1001, 2001, 3001, ..., 10000001, 10001001, ...
- **Transition triggers:** 10000001, 20000001, 30000001, 40000001, ...

## Verification Examples

| Ledger | RangeID | ChunkID | Transition? | Checkpoint? |
|--------|---------|---------|-------------|-------------|
| 2 | 0 | 0 | No | No |
| 1001 | 0 | 0 | No | Yes |
| 10001 | 0 | 1 | No | No |
| 10000001 | 0 | 1000 | **YES** | Yes |
| 10000002 | 1 | 1000 | No | No |
| 20000001 | 1 | 2000 | **YES** | Yes |

## File Paths

### LFS Chunk Path
```
chunks/XXXX/YYYYYY.data

Where:
  XXXX   = chunkID / 1000  (zero-padded 4 digits)
  YYYYYY = chunkID         (zero-padded 6 digits)

Example: chunk 1234 → chunks/0001/001234.data
```

### RecSplit Index Path
```
immutable/txhash/XXXX/index/cf-{X}.idx

Where:
  XXXX = rangeID (zero-padded)
  X    = column family name (0-f, one file per CF)
```
