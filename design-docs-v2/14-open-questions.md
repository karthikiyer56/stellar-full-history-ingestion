# Open Questions

> **Status**: Active — tracks unresolved design decisions that will affect future implementation

---

## OQ-1: Streaming Transition Sub-flow Cadence and Meta Store Evolution

### Context

The current design defines two distinct sub-flow cadences during streaming:

- **Ledger store** rotates at **chunk boundaries** (every 10K ledgers) — 1,000 swaps per range via `SwapActiveLedgerStore`
- **TxHash store** rotates at **range boundaries** (every 10M ledgers) — 1 swap per range via `AddActiveStore`

The background LFS flush goroutine writes LFS chunk files at each chunk boundary during ACTIVE, setting `lfs_done` flags incrementally. By the time the range transitions to TRANSITIONING, most LFS chunks are already flushed.

### What's TBD

The exact transition flow and cadence for storing **events** (and potentially other future data types) is still undecided. The cadence may be **10K ledgers** (consistent with LFS chunk granularity), but the specific steps in the transition sub-flow — when to flush, when to build indexes, and how to coordinate with existing sub-flows — are not yet defined.

### What Will NOT Change

**Existing meta store keys and state machines are stable.** The current key hierarchy (documented in [02-meta-store-design.md](./02-meta-store-design.md)) will not change:

| Sub-workflow | Keys | Stable? |
|-------------|------|---------|
| Range state (`range:{N:04d}:state`) | 1 per range | ✅ Unchanged |
| Chunk LFS flags (`range:{N:04d}:chunk:{C:06d}:lfs_done`) | 1,000 per range | ✅ Unchanged |
| Chunk txhash flags (`range:{N:04d}:chunk:{C:06d}:txhash_done`) | 1,000 per range (backfill only) | ✅ Unchanged |
| RecSplit state (`range:{N:04d}:recsplit:state`) | 1 per range | ✅ Unchanged |
| RecSplit CF flags (`range:{N:04d}:recsplit:cf:{XX}:done`) | 16 per range | ✅ Unchanged |
| Streaming checkpoint (`streaming:last_committed_ledger`) | 1 global | ✅ Unchanged |

### What Will Likely Be Added

A **new sub-flow** for events (and potentially other future data types) with its own state tracking, following the existing pattern:

```
# New keys (additive — no modifications to existing keys):
range:{N:04d}:chunk:{C:06d}:events_done          ← per-chunk flag, analogous to lfs_done
range:{N:04d}:events_index:state                  ← PENDING / BUILDING / COMPLETE
range:{N:04d}:events_index:cf:{XX}:done           ← per-CF or per-partition done flag
```

The range state machine may extend with a new intermediate state:

```
# Backfill:
INGESTING → RECSPLIT_BUILDING → EVENTS_INDEX_BUILDING → COMPLETE

# Streaming:
ACTIVE → TRANSITIONING → COMPLETE  (unchanged — events index built within TRANSITIONING)
```

This is already anticipated in the placeholder sections throughout the design docs (see [02-meta-store-design.md — getEvents Placeholder](./02-meta-store-design.md#getevents-immutable-store--placeholder)).

### Design Principle

The meta store key hierarchy was designed to be **additive**. New sub-flows introduce new keys — they never modify or reinterpret existing keys. The chunk skip rule generalizes: a chunk is skippable on resume when **all** applicable flags are set (today: `lfs_done` AND `txhash_done`; future: AND `events_done`).

---

## OQ-2: Service Identity — `stellar-full-history-rpc` vs `stellar-rpc`

### Context

The full-history ingestion and query service could ship as either:

1. **A new binary**: `stellar-full-history-rpc` — a dedicated service separate from the existing `stellar-rpc`
2. **Integrated into the existing binary**: `stellar-rpc` — the same service that currently handles sliding-window RPC, extended with full-history capabilities

The existing `stellar-rpc` already has a `--backfill` flag (added in [PR #571](https://github.com/stellar/stellar-rpc/pull/571)), which synchronously fills a SQLite DB with the most recent `HISTORY_RETENTION_WINDOW` ledgers (~7 days) from a CDP datastore before starting live captive-core ingestion. This is a **warm-up mechanism for the existing sliding-window retention**, not a full-history solution.

### Trade-off Analysis

| Dimension | New binary (`stellar-full-history-rpc`) | Same binary (`stellar-rpc`) |
|-----------|----------------------------------------|------------------------------|
| **Config clarity** | Clean slate; all config is full-history specific | Must coexist with 80+ existing config fields; risk of confusion between `--backfill` (7-day SQLite fill) and full-history backfill (genesis→tip, RocksDB) |
| **Storage isolation** | RocksDB + LFS is the only store; simple lifecycle | Two DB engines (SQLite for sliding-window + RocksDB for full-history) in the same process; complicates `mustInitializeStorage()`, `ResetCache()`, and fee-window resets |
| **Startup semantics** | Can run as a long-lived ingestion daemon without blocking the RPC server | Existing `--backfill` is **synchronous and blocking** (~3h for 7 days). Full-history backfill (years of data) cannot block startup |
| **Retention semantics** | No conflict; full history means "everything" | `HistoryRetentionWindow` controls SQLite pruning; full-history effectively means `MAX_UINT32` — may break assumptions in fee windows and migrations |
| **Abstraction boundary** | Clean separation of concerns: sliding-window RPC ≠ archive node | Single binary for operators to deploy, but at the cost of mixing two fundamentally different retention models |
| **Existing exploration** | Mirrors the approach in this project (`stellar-full-history-ingestion`) | The stellar team is actively prototyping RocksDB integration inside `stellar-rpc` ([#583](https://github.com/stellar/stellar-rpc/issues/583), [#584](https://github.com/stellar/stellar-rpc/issues/584)), but also exploring a "fully embedded archive node" ([#531](https://github.com/stellar/stellar-rpc/issues/531)) and "file-based ledger storage" ([#586](https://github.com/stellar/stellar-rpc/issues/586)) — no consensus yet |

### The `--backfill` Collision Problem

PR #571's `--backfill` has specific semantics that would collide with a full-history backfill feature:

| Attribute | Existing `--backfill` (PR #571) | Full-history backfill (this design) |
|-----------|--------------------------------|-------------------------------------|
| **Scope** | Most recent `HISTORY_RETENTION_WINDOW` (~7 days / 120,960 ledgers) | Genesis → tip (all ~58M+ ledgers, growing) |
| **Storage** | SQLite | RocksDB + LFS chunks + RecSplit indexes |
| **Blocking?** | Yes — synchronous, ~3 hours | Cannot be blocking — runs for days/weeks |
| **Idempotent?** | Yes (gap check) | Yes (chunk flag scan) |
| **Source** | CDP datastore via BSB | CDP datastore via BSB (shared) or CaptiveStellarCore |
| **Retention** | Sliding window — old ledgers are pruned | Permanent — nothing is pruned |

### Config Layout If Integrated

If the decision is to integrate into `stellar-rpc`, all full-history config should live under a `[full_history]` TOML section to avoid collision with the existing top-level `BACKFILL` flag:

```toml
# ─── Existing stellar-rpc config (unchanged) ─────────────────────────
ENDPOINT = "localhost:8000"
STELLAR_CORE_BINARY_PATH = "/usr/bin/stellar-core"
CAPTIVE_CORE_CONFIG_PATH = "/etc/stellar/captive-core.cfg"
HISTORY_RETENTION_WINDOW = 120960          # ~7 days sliding window
SERVE_LEDGERS_FROM_DATASTORE = true
BACKFILL = true                            # existing: fill SQLite with 7 days

[datastore_config]
  type = "GCS"
  [datastore_config.params]
    destination_bucket_path = "gs://stellar-ledgers/mainnet"
  [datastore_config.schema]
    ledgers_per_file = 1
    files_per_partition = 64000

# ─── Full-history extension (new) ────────────────────────────────────
[full_history]
  enabled = true
  mode = "backfill"                        # "backfill" | "streaming"
  data_dir = "/data/stellar-full-history"

  [full_history.meta_store]
    path = "/ssd0/stellar-full-history/meta/rocksdb"

  [full_history.immutable_stores]
    ledgers_base = "/ssd1/stellar-full-history/immutable/ledgers"
    txhash_base  = "/ssd2/stellar-full-history/immutable/txhash"

  [full_history.backfill]
    start_ledger = 2
    end_ledger   = 60000001
    parallel_ranges = 2
    flush_interval  = 100

    [full_history.backfill.bsb]
      bucket_path = "gs://stellar-ledgers/mainnet"
      num_bsb_instances_per_range = 20
      buffer_size = 1000
      num_workers = 20

  [full_history.streaming]
    # start_ledger = auto

  [full_history.active_stores]
    base_path = "/ssd3/stellar-full-history/active"

  [full_history.rocksdb]
    block_cache_mb = 8192
    write_buffer_mb = 64
```

**Key principle**: Everything under `[full_history]` is self-contained. The existing `BACKFILL` flag, `HISTORY_RETENTION_WINDOW`, and SQLite-based sliding window continue to operate independently. The two systems share `[datastore_config]` for CDP access but nothing else.

### Recommendation Leaning

`stellar-full-history-rpc` as a **separate binary** is cleaner from both a config perspective and clean abstractions perspective. The storage backends (SQLite vs RocksDB+LFS), retention models (sliding window vs permanent), startup semantics (blocking 3h vs long-running daemon), and crash recovery strategies are fundamentally different. Forcing them into a single binary creates coupling with no operational benefit.

---

## OQ-3: Transaction Submission Support

### Context

The current design supports two query endpoints (`getTransactionByHash`, `getLedgerBySequence`) and two status endpoints (`getHealth`, `getStatus`). The question is whether the full-history service should also support **transaction submission** — i.e., accepting signed transactions from clients and forwarding them to the Stellar network.

### Precedent: Erigon v3 (Ethereum Archive Node)

Erigon v3, the most widely deployed Ethereum archive node implementation, **fully supports transaction submission** alongside full-history querying. It is not a read-only archive — the same process handles both historical queries and live transaction relay.

| Capability | Erigon v3 Support |
|-----------|-------------------|
| `eth_sendRawTransaction` | ✅ Fully implemented — transactions are forwarded to an integrated txpool, then gossiped to peers via a Sentry component |
| `eth_sendRawTransactionSync` | ✅ Implemented (EIP-7966) — blocks until the transaction is included and a receipt is available |
| `eth_sendTransaction` (node-side signing) | ❌ Intentionally not implemented — nodes should not manage private keys |
| Archive mode + TX submission | ✅ All node types (Minimal, Full, Archive) support the full RPC API including transaction submission |

**Architecture**: Erigon's txpool runs **inline by default** (same process). For high-throughput deployments, it can be extracted as a standalone service alongside a Sentry daemon (p2p gossip). The RPCDaemon forwards `eth_sendRawTransaction` calls to the txpool via gRPC.

### Implications for This Design

| Approach | Description | Pros | Cons |
|----------|-------------|------|------|
| **No TX submission** | Read-only full-history archive; clients submit transactions through a separate Horizon/RPC endpoint | Simpler; no p2p or network participation needed; pure storage + query service | Operators must run a second service for write operations; clients need two endpoints |
| **Proxy TX submission** | Accept `sendTransaction` RPCs and forward them to a configured upstream Stellar RPC / Horizon endpoint | Single endpoint for clients; no p2p complexity in this service | Adds a network hop; availability depends on upstream; not truly self-contained |
| **Native TX submission** | Integrate with CaptiveStellarCore or a Stellar Core instance to submit transactions directly to the network | Fully self-contained archive node (like Erigon); single deployment | Significant complexity; requires persistent network connectivity and a transaction pool; goes beyond the core mission of historical data serving |

### Decision Criteria

1. **Is the primary use case historical data retrieval or full-node functionality?** If historical queries are the 90% case, TX submission adds complexity for marginal benefit.
2. **Does the operator already run a Stellar RPC / Horizon instance?** If yes, proxy is trivial. If this is their only Stellar service, native submission becomes more valuable.
3. **Is CaptiveStellarCore already running for streaming mode?** If yes, it already has network connectivity — adding TX submission through it may be low incremental cost.

### Current Leaning

TBD. The Erigon precedent shows that archive nodes *can* support submission, but Erigon is a general-purpose execution client — not a purpose-built archival service. This project's scope is narrower (full-history storage and querying), which argues for deferring TX submission or supporting it only as a proxy.

---

## Related Documents

- [02-meta-store-design.md](./02-meta-store-design.md) — current key hierarchy and getEvents placeholder
- [10-configuration.md](./10-configuration.md) — current TOML reference
- [01-architecture-overview.md](./01-architecture-overview.md) — two-pipeline design and getEvents placeholder
- [05-backfill-transition-workflow.md](./05-backfill-transition-workflow.md) — backfill transition details
- [06-streaming-transition-workflow.md](./06-streaming-transition-workflow.md) — streaming transition details
