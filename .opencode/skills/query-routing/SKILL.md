---
name: query-routing
description: QueryRouter design — store lifecycle, query routing logic across active/transitioning/immutable stores, and RecSplit false positive handling
---

# Query Routing

## QueryRouter Struct

```go
type QueryRouter struct {
    mu            sync.RWMutex
    active        map[uint32]Store           // rangeID → active RocksDB store
    transitioning map[uint32]Store           // rangeID → transitioning RocksDB store
    immutable     map[uint32]RecSplitIndex   // rangeID → immutable RecSplit index
}
```

## Store Lifecycle

Stores progress through this lifecycle in order:

```
AddActiveStore(rangeID)
    ↓
PromoteToTransitioning(rangeID)     ← at 10M ledger boundary
    ↓
AddImmutableStores(rangeID, index)  ← after RecSplit build completes
    ↓
RemoveTransitioningStores(rangeID)  ← cleanup after immutable ready
```

## getLedgerBySequence Routing

```
1. rangeID = ledgerToRangeID(seq)
2. Check range state in meta store
3. Route:
   INGESTING     → active[rangeID]
   TRANSITIONING → transitioning[rangeID]
   COMPLETE      → immutable LFS path
```

## getTransactionByHash Routing

Search order: **newest → oldest** (active → transitioning → immutable)

```
1. Check active stores (iterate newest rangeID first)
2. Check transitioning stores
3. Check immutable RecSplit indexes
   → RecSplit lookup may return FALSE POSITIVES — MUST verify
   → Fetch LCM (Ledger Close Meta) for the candidate ledger
   → Parse LCM and confirm tx hash matches
   → If mismatch (false positive): continue searching next store
```

### RecSplit False Positive Types

All 3 types must be handled by fetching and parsing the LCM:

| Type | Description |
|------|-------------|
| NORMAL | Hash maps to a ledger that does not contain the transaction |
| COMPOUND | Hash is a substring/prefix match of a real hash in the index |
| PARTIAL | Index collision with an unrelated transaction |

**Recovery for all types:** fetch the LCM for the candidate ledger, parse it, check if the target txhash is actually present. If not found → false positive → continue to next store.

## Concurrency Model

- `sync.RWMutex` on the QueryRouter
- **Read lock (`RLock`)** for all queries — multiple concurrent reads allowed simultaneously
- **Write lock (`Lock`)** ONLY during store lifecycle transitions (at 10M ledger boundaries — very rare)
- Result: query reads are non-blocking in the steady state
