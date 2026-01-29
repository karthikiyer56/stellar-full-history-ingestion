# Checkpointing and Range Transitions

## Overview

This document provides the canonical reference for checkpoint timing, range boundary calculations, and transition triggers in the Stellar Full History RPC Service. It establishes the mathematical invariants that must be maintained across all scenarios and provides concrete examples to clarify corner cases.

**Purpose**: Serve as the single source of truth for:
- When checkpoints occur (ledger sequence numbers)
- How range boundaries are calculated
- When transitions from active to immutable stores trigger
- How crash recovery maintains data integrity

**Key Insight**: Stellar's genesis ledger is 2 (not 0 or 1). This offset requires careful handling in checkpoint and range calculations to ensure each checkpoint covers exactly 1000 ledgers and each range contains exactly 10,000,000 ledgers.

---

## Key Constants

```go
const (
    FirstLedger        = 2           // Stellar genesis ledger
    RangeSize          = 10_000_000  // Ledgers per immutable store range
    CheckpointInterval = 1000        // Ledgers between checkpoints
    ChunkSize          = 10_000      // Ledgers per LFS chunk
)
```

---

## Math Contract (Invariants)

These invariants MUST be maintained in ALL examples, scenarios, and implementations:

### INVARIANT 1: Checkpoint Sequence

**Rule**: Checkpoints occur at ledgers: 1001, 2001, 3001, ..., N×1000+1, ...

**Formula**: 
```go
func isCheckpoint(ledgerSeq uint32) bool {
    return (ledgerSeq - 1) % 1000 == 0 && ledgerSeq > 1
}
```

**Why the `-1`?**
- Without `-1`: First checkpoint at 1000 covers ledgers 2-1000 (only 999 ledgers) ❌
- With `-1`: First checkpoint at 1001 covers ledgers 2-1001 (exactly 1000 ledgers) ✅

**Examples**:
- Checkpoint 1: ledger 1001 (covers ledgers 2-1001, count=1000)
- Checkpoint 2: ledger 2001 (covers ledgers 1002-2001, count=1000)
- Checkpoint 5: ledger 5001 (covers ledgers 4002-5001, count=1000)
- Checkpoint 6: ledger 6001 (covers ledgers 5002-6001, count=1000)
- Checkpoint 7499: ledger 7499001 (covers ledgers 7498002-7499001, count=1000)
- Checkpoint 10000: ledger 10000001 (covers ledgers 9999002-10000001, count=1000)

### INVARIANT 2: Resume Rule

**Rule**: Always resume from `last_committed_ledger + 1`

**Examples**:
- If `last_committed_ledger = 5001`, resume from ledger 5002
- If `last_committed_ledger = 7499001`, resume from ledger 7499002
- If `last_committed_ledger = 10000001`, resume from ledger 10000002

**Why**: The `last_committed_ledger` has already been processed and checkpointed. Resuming from it would create a duplicate entry for that exact ledger.

### INVARIANT 3: Duplicate Range

**Rule**: After a crash, duplicates span `[last_committed + 1, crash_ledger]` (inclusive)

**Examples**:
- Crash at ledger 7500000, last_committed = 7499001 → duplicates are ledgers 7499002-7500000 (inclusive)
- Crash at ledger 6000, last_committed = 5001 → duplicates are ledgers 5002-6000 (inclusive)

**Why This Is Safe**:
- RocksDB writes are idempotent (same key→value pairs)
- Compaction automatically removes duplicates
- Counts are restored from checkpoints, not recomputed from store

### INVARIANT 4: Count vs Last Committed

**Rule**: `count ≠ last_committed_ledger` (they are different values)

**Definitions**:
- `count`: Number of ledgers processed (e.g., 5000)
- `last_committed_ledger`: Sequence number where checkpoint occurred (e.g., 5001)

**Example**:
```
# After processing 5000 ledgers starting from ledger 2:
count = 5000                         # Processed ledgers 2, 3, 4, ..., 5001
last_committed_ledger = 5001         # Checkpoint occurred at ledger 5001
```

**Why They Differ**: Starting from ledger 2 (not 0) creates an offset. After processing N ledgers, the last ledger sequence is `2 + N - 1 = N + 1`.

---

## Checkpoint Logic

### Formula

```go
func isCheckpoint(ledgerSeq uint32) bool {
    return (ledgerSeq - 1) % 1000 == 0 && ledgerSeq > 1
}
```

### Checkpoint Sequence

The complete checkpoint sequence is:
```
1001, 2001, 3001, 4001, 5001, 6001, 7001, 8001, 9001, 10001,
11001, 12001, ..., 1000001, 1001001, ..., 10000001, 10001001, ...
```

### Walk-Through: First 3 Checkpoints

**Checkpoint 1 (ledger 1001)**:
- Covers: ledgers 2 to 1001 (inclusive)
- Count: 1000 ledgers
- Checkpoint state: `last_committed_ledger = 1001, count = 1000`

**Checkpoint 2 (ledger 2001)**:
- Covers: ledgers 1002 to 2001 (inclusive)
- Count: 1000 ledgers
- Checkpoint state: `last_committed_ledger = 2001, count = 2000`

**Checkpoint 3 (ledger 3001)**:
- Covers: ledgers 2002 to 3001 (inclusive)
- Count: 1000 ledgers
- Checkpoint state: `last_committed_ledger = 3001, count = 3000`

---

## Range Transition Logic

### Formula

```go
func shouldTriggerTransition(ledgerSeq uint32) bool {
    rangeID := ledgerToRangeID(ledgerSeq)
    return ledgerSeq == rangeLastLedger(rangeID)
}
```

### Transition Triggers

Transitions occur at these ledger sequences:
```
10,000,001, 20,000,001, 30,000,001, 40,000,001, 50,000,001, ...
```

**Pattern**: `(N + 1) × 10,000,000 + 1` for range N

### What Happens at Transition

When ledger 10,000,001 is processed:
1. Ledger 10,000,001 is written to Range 0 active stores
2. Checkpoint occurs (10,000,001 is a checkpoint ledger)
3. `shouldTriggerTransition(10000001)` returns `true`
4. Transition workflow starts:
   - Range 0 active stores → immutable stores (LFS + RecSplit)
   - Range 1 active stores are created
5. Next ledger (10,000,002) goes to Range 1 active stores

---

## Immutable Store Range Table

| Range ID | Contains (INCLUSIVE) | Ledger Count |
|----------|----------------------|--------------|
| 0 | Ledgers 2 to 10,000,001 (inclusive) | 10,000,000 |
| 1 | Ledgers 10,000,002 to 20,000,001 (inclusive) | 10,000,000 |
| 2 | Ledgers 20,000,002 to 30,000,001 (inclusive) | 10,000,000 |
| 3 | Ledgers 30,000,002 to 40,000,001 (inclusive) | 10,000,000 |
| 4 | Ledgers 40,000,002 to 50,000,001 (inclusive) | 10,000,000 |
| 5 | Ledgers 50,000,002 to 60,000,001 (inclusive) | 10,000,000 |

**Key Points**:
- Each range contains exactly 10,000,000 ledgers (inclusive of both boundaries)
- Range boundaries are calculated using: `rangeFirstLedger(N) = (N × 10,000,000) + 2`
- Last ledger: `rangeLastLedger(N) = ((N + 1) × 10,000,000) + 1`
- Transition triggers when processing the last ledger of a range (e.g., 10,000,001)
- The last ledger of each range is INCLUDED in that range (not the next range)

**Boundary Clarification**:
- Ledger 10,000,001 belongs to Range 0 (it is the last ledger, inclusive)
- Ledger 10,000,002 belongs to Range 1 (it is the first ledger, inclusive)
- There are no gaps between ranges
- There are no overlaps between ranges

---

## LFS Chunk Alignment

### LFS Chunk Structure

Each LFS chunk contains exactly 10,000 ledgers:
- Chunk 0: ledgers 2 to 10,001 (inclusive)
- Chunk 1: ledgers 10,002 to 20,001 (inclusive)
- Chunk 999: ledgers 9,990,002 to 10,000,001 (inclusive)
- Chunk 1000: ledgers 10,000,002 to 10,010,001 (inclusive)

### Range to Chunk Mapping

**Range 0** (ledgers 2 to 10,000,001):
- Spans chunks 0 to 999 (inclusive)
- Total: 1000 chunks × 10,000 ledgers = 10,000,000 ledgers ✓

**Range 1** (ledgers 10,000,002 to 20,000,001):
- Spans chunks 1000 to 1999 (inclusive)
- Total: 1000 chunks × 10,000 ledgers = 10,000,000 ledgers ✓

### Chunk Calculation Formula

From `helpers/lfs/chunk.go`:
```go
func LedgerToChunkID(ledgerSeq uint32) uint32 {
    return (ledgerSeq - FirstLedgerSequence) / ChunkSize
}

// FirstLedgerSequence = 2
// ChunkSize = 10000
```

**Examples**:
- Ledger 2 → Chunk 0
- Ledger 10,001 → Chunk 0 (last ledger in chunk)
- Ledger 10,002 → Chunk 1 (first ledger in chunk)
- Ledger 10,000,001 → Chunk 999 (last ledger in Range 0)
- Ledger 10,000,002 → Chunk 1000 (first ledger in Range 1)

---

## Complete Walk-Through: Range 0

This section walks through the complete lifecycle of Range 0 from first ledger to transition.

### Phase 1: Ingestion Starts

**Ledger 2** (first ledger):
- Range ID: 0
- Checkpoint: No (not a checkpoint ledger)
- Action: Write to Range 0 active stores

**Ledger 1001** (first checkpoint):
- Range ID: 0
- Checkpoint: Yes
- Action: Write to Range 0 active stores, then checkpoint
- State: `last_committed_ledger = 1001, count = 1000`

### Phase 2: Mid-Range Processing

**Ledger 5001** (checkpoint 5):
- Range ID: 0
- Checkpoint: Yes
- Action: Write to Range 0 active stores, then checkpoint
- State: `last_committed_ledger = 5001, count = 5000`

**Ledger 5000000** (mid-range, not checkpoint):
- Range ID: 0
- Checkpoint: No
- Action: Write to Range 0 active stores (no checkpoint)

### Phase 3: Approaching Range Boundary

**Ledger 10,000,001** (last ledger in Range 0):
- Range ID: 0
- Checkpoint: Yes (it's a checkpoint ledger)
- Transition: Yes (it's the last ledger in the range)
- Action:
  1. Write ledger 10,000,001 to Range 0 active stores
  2. Checkpoint: `last_committed_ledger = 10000001, count = 10000000`
  3. Trigger transition workflow
  4. Convert Range 0 active stores → immutable stores
  5. Create Range 1 active stores

### Phase 4: Next Range Begins

**Ledger 10,000,002** (first ledger in Range 1):
- Range ID: 1
- Checkpoint: No
- Action: Write to Range 1 active stores

**Summary**:
- Range 0 processed ledgers 2 to 10,000,001 (inclusive) = 10,000,000 ledgers
- Range 0 had 10,000 checkpoints (at ledgers 1001, 2001, ..., 10000001)
- Transition triggered after ledger 10,000,001 was fully committed
- Range 1 starts immediately with ledger 10,000,002

---

## FAQ Section

### Q: What is the exact last ledger in Range 0 immutable store?

**A**: Ledger 10,000,001 (inclusive). This is the last ledger stored in Range 0.

**Explanation**: Range 0 contains ledgers 2 to 10,000,001 (inclusive). The boundary is inclusive, meaning ledger 10,000,001 is part of Range 0, not Range 1.

---

### Q: Which store handles getLedgerBySequence(10000001)?

**A**: Range 0 immutable store (after transition completes).

**Explanation**: Ledger 10,000,001 is the last ledger in Range 0 (inclusive). After the transition workflow completes, this ledger is stored in the Range 0 immutable LFS store, specifically in chunk 999.

---

### Q: Which store handles getLedgerBySequence(10000002)?

**A**: Range 1 store (either active or immutable depending on ingestion progress).

**Explanation**: Ledger 10,000,002 is the first ledger in Range 1 (inclusive). If ingestion is ongoing, it's in the Range 1 active RocksDB store. If Range 1 has completed and transitioned, it's in the Range 1 immutable LFS store.

---

### Q: What ledgers are in LFS chunk `chunks/0000/000999.index`?

**A**: Ledgers 9,990,002 to 10,000,001 (inclusive).

**Explanation**: 
- Chunk 999 is the last chunk in Range 0
- Each chunk contains exactly 10,000 ledgers
- Chunk 999 starts at: `(999 × 10000) + 2 = 9,990,002`
- Chunk 999 ends at: `((999 + 1) × 10000) + 1 = 10,000,001`
- This chunk contains the last 10,000 ledgers of Range 0, including the boundary ledger 10,000,001

---

### Q: When does the transition to immutable store happen?

**A**: After ledger 10,000,001 is fully processed and committed.

**Explanation**: The transition workflow is triggered by `shouldTriggerTransition(10000001)` returning `true`. This happens AFTER:
1. Ledger 10,000,001 is written to Range 0 active stores
2. Checkpoint is written to meta store
3. All data is safely persisted

Only then does the transition workflow begin converting Range 0 from active to immutable stores.

---

### Q: If I crash at ledger 7,500,000 with last_committed_ledger = 7499001, what ledgers get re-ingested?

**A**: Ledgers 7,499,002 to 7,500,000 (inclusive).

**Explanation**:
- Resume point: `last_committed_ledger + 1 = 7499001 + 1 = 7499002`
- Crash point: 7,500,000
- Duplicate range: [7499002, 7500000] (inclusive)
- These ledgers may already be in RocksDB from before the crash
- Re-ingestion is safe (idempotent writes)
- Compaction removes duplicates

---

### Q: Why is the checkpoint formula `(ledgerSeq - 1) % 1000 == 0` instead of `ledgerSeq % 1000 == 0`?

**A**: To ensure each checkpoint covers exactly 1000 ledgers, accounting for Stellar starting at ledger 2.

**Explanation**:
- Without `-1`: Checkpoints at 1000, 2000, 3000, ...
  - First checkpoint (1000) covers ledgers 2-1000 = 999 ledgers ❌
- With `-1`: Checkpoints at 1001, 2001, 3001, ...
  - First checkpoint (1001) covers ledgers 2-1001 = 1000 ledgers ✅

The `-1` adjustment compensates for the offset introduced by starting at ledger 2 instead of 0.

---

### Q: Are range boundaries inclusive or exclusive?

**A**: **Inclusive** on both ends.

**Explanation**:
- Range 0: ledgers 2 to 10,000,001 (inclusive) means both 2 AND 10,000,001 are in Range 0
- Range 1: ledgers 10,000,002 to 20,000,001 (inclusive) means both 10,000,002 AND 20,000,001 are in Range 1
- There are no gaps: ledger 10,000,001 is in Range 0, ledger 10,000,002 is in Range 1
- There are no overlaps: each ledger belongs to exactly one range

---

### Q: What happens if I query ledger 10,000,001 during the transition?

**A**: The query is routed to Range 0 active store (which still exists during transition).

**Explanation**: During the transition workflow:
1. Range 0 active stores remain accessible for queries
2. Transition workflow reads from active stores and writes to immutable stores
3. Only after transition completes are the active stores deleted
4. Query routing checks: if Range 0 is in "TRANSITIONING" state, route to active store
5. After transition completes, queries route to immutable store

---

## Code Reference

The following functions implement the checkpoint and transition logic described in this document.

### Range ID Calculation

From `design-docs/02-meta-store-design.md:143-145`:

```go
func ledgerToRangeID(ledgerSeq uint32) uint32 {
    return (ledgerSeq - FirstLedger) / RangeSize
}
```

**Examples**:
- `ledgerToRangeID(2)` → 0
- `ledgerToRangeID(10000001)` → 0
- `ledgerToRangeID(10000002)` → 1

---

### Range First Ledger

From `design-docs/02-meta-store-design.md:148-150`:

```go
func rangeFirstLedger(rangeID uint32) uint32 {
    return (rangeID * RangeSize) + FirstLedger
}
```

**Examples**:
- `rangeFirstLedger(0)` → 2
- `rangeFirstLedger(1)` → 10,000,002
- `rangeFirstLedger(2)` → 20,000,002

---

### Range Last Ledger

From `design-docs/02-meta-store-design.md:153-155`:

```go
func rangeLastLedger(rangeID uint32) uint32 {
    return ((rangeID + 1) * RangeSize) + FirstLedger - 1
}
```

**Examples**:
- `rangeLastLedger(0)` → 10,000,001
- `rangeLastLedger(1)` → 20,000,001
- `rangeLastLedger(2)` → 30,000,001

---

### Transition Trigger Check

From `design-docs/02-meta-store-design.md:158-161`:

```go
func shouldTriggerTransition(ledgerSeq uint32) bool {
    rangeID := ledgerToRangeID(ledgerSeq)
    return ledgerSeq == rangeLastLedger(rangeID)
}
```

**Examples**:
- `shouldTriggerTransition(10000001)` → true (last ledger of Range 0)
- `shouldTriggerTransition(10000002)` → false (first ledger of Range 1)
- `shouldTriggerTransition(20000001)` → true (last ledger of Range 1)

---

### LFS Chunk Calculation

From `helpers/lfs/chunk.go:46-47`:

```go
func LedgerToChunkID(ledgerSeq uint32) uint32 {
    return (ledgerSeq - FirstLedgerSequence) / ChunkSize
}

// Where:
// FirstLedgerSequence = 2
// ChunkSize = 10000
```

**Examples**:
- `LedgerToChunkID(2)` → 0
- `LedgerToChunkID(10001)` → 0 (last ledger in chunk 0)
- `LedgerToChunkID(10002)` → 1 (first ledger in chunk 1)
- `LedgerToChunkID(10000001)` → 999 (last chunk in Range 0)
- `LedgerToChunkID(10000002)` → 1000 (first chunk in Range 1)

**Chunk Boundaries**:
- Each chunk contains exactly 10,000 ledgers
- Chunk N contains ledgers: `(N × 10000) + 2` to `((N + 1) × 10000) + 1` (inclusive)
- Range 0 spans chunks 0-999 (1000 chunks × 10,000 ledgers = 10,000,000 ledgers)

---

## Related Documentation

- [02-meta-store-design.md](./02-meta-store-design.md) - Meta store implementation with range ID formulas
- [04-streaming-workflow.md](./04-streaming-workflow.md) - Streaming mode ingestion and transition detection
- [05-transition-workflow.md](./05-transition-workflow.md) - Detailed transition workflow from active to immutable stores
- [06-crash-recovery.md](./06-crash-recovery.md) - Crash recovery scenarios demonstrating checkpoint usage

---

## Summary

This document establishes the canonical reference for checkpoint and transition logic:

1. **Checkpoints** occur at ledgers 1001, 2001, 3001, ..., N×1000+1
2. **Resume** always from `last_committed_ledger + 1`
3. **Ranges** are inclusive on both boundaries (e.g., Range 0 = ledgers 2 to 10,000,001)
4. **Transitions** trigger at ledgers 10,000,001, 20,000,001, 30,000,001, ...
5. **Duplicates** after crash are safe (idempotent, removed by compaction)
6. **Count ≠ last_committed_ledger** (they measure different things)

All examples in other design documents should conform to these invariants.
