# Metrics & Sizing Reference

> **This is the single source of truth for all concrete numbers, storage estimates, memory budgets, hardware requirements, and structural constants.** All other documents link here instead of duplicating these figures.

---

## Structural Constants

| Constant | Value | Notes |
|----------|-------|-------|
| `FirstLedger` | 2 | Ledger sequence of the first ledger in Stellar history |
| `RangeSize` | 10,000,000 ledgers | One range = one RecSplit index |
| `ChunkSize` | 10,000 ledgers | One chunk = one LFS file pair + one raw txhash flat file |
| Chunks per range | 1,000 | = RangeSize ÷ ChunkSize |
| RecSplit column families | 16 | Sharded by first hex nibble of txhash (`0`–`f`) |
| Default BSB instances per orchestrator | 20 | `[backfill.bsb].num_instances` |
| Max parallel range orchestrators | 2 | `[backfill].parallel_ranges` |
| Max BSB instances in flight | 40 | 2 orchestrators × 20 instances |
| Ledgers per BSB instance (`num_instances=20`) | 500,000 | = RangeSize ÷ 20 |
| Ledgers per BSB instance (`num_instances=10`) | 1,000,000 | = RangeSize ÷ 10 |
| Chunks per BSB instance (`num_instances=20`) | 50 | = 500K ÷ 10K |
| Chunks per BSB instance (`num_instances=10`) | 100 | = 1M ÷ 10K |
| BSB internal prefetch window | 1,000 ledgers | `[backfill.bsb].buffer_size` |
| BSB internal download workers | 20 | `[backfill.bsb].num_workers` |
| Flush interval | ~100 ledgers | `[backfill].flush_interval`; max ledgers in RAM per chunk write |
| Spot-check sample size (streaming transition) | 100 ledgers + 100 txhashes | Random verification before active store deletion |
| Raw txhash entry size | 36 bytes | `txhash[32] \|\| ledgerSeq[4]` big-endian |
| Average compressed LCM size | ~2 KB | Per ledger, after zstd |
| Average raw ledger size | ~10 KB | Per ledger, uncompressed from BSB/GCS |
| Average transactions per ledger | ~10 | Used for txhash buffer sizing |
| Approximate transactions per range | 1B+ | 10M ledgers × ~10 tx/ledger |

---

## Range Boundaries (First 5)

| Range | First Ledger | Last Ledger |
|-------|-------------|------------|
| 0 | 2 | 10,000,001 |
| 1 | 10,000,002 | 20,000,001 |
| 2 | 20,000,002 | 30,000,001 |
| 3 | 30,000,002 | 40,000,001 |
| 4 | 40,000,002 | 50,000,001 |

Formulas: `rangeFirstLedger(N) = (N × 10,000,000) + 2`, `rangeLastLedger(N) = ((N+1) × 10,000,000) + 1`.  
See [11-checkpointing-and-transitions.md](./11-checkpointing-and-transitions.md) for full math.

---

## Storage Estimates (Per 10M-Ledger Range)

| Component | Path | Size | Notes |
|-----------|------|------|-------|
| LFS chunk files | `immutable/ledgers/chunks/` | ~1.5 TB | 1,000 `.data` + `.index` pairs, zstd-compressed LCMs |
| RecSplit index | `immutable/txhash/{N:04d}/index/` | ~15 GB | 16 CF files (`cf-0.idx`–`cf-f.idx`) |
| Raw txhash flat files | `immutable/txhash/{N:04d}/raw/` | ~120 GB | Temporary; deleted after all 16 CF indexes are built |
| Active ledger store | `active/rocksdb/{N:04d}-ledger-store/` | ~1.7 TB | Streaming only; default CF; deleted post-transition |
| Active txhash store | `active/rocksdb/{N:04d}-txhash-store/` | ~10–20 GB | Streaming only; 16 CFs by nibble; deleted post-transition |
| Meta store | `meta/rocksdb/` | ~100 MB | Shared across all ranges; grows slowly |

**Peak disk required during backfill** (2 ranges in flight simultaneously): ~2 × (~1.5 TB LFS + ~120 GB raw) + ~100 MB meta ≈ **~3.2 TB**.

**Peak disk required during streaming**: active ledger store (~1.7 TB) + active txhash store (~15 GB) + prior immutable ranges + meta ≈ **~1.7 TB active + immutable history**.

---

## Memory Budget — Backfill (BSB Mode)

Default settings: `parallel_ranges=2`, `num_instances=20`, `flush_interval=100`.

| Component | Per BSB instance | Per orchestrator (20 instances) | Total (2 orchestrators) |
|-----------|-----------------|--------------------------------|------------------------|
| BSB prefetch buffer | 1,000 ledgers × ~10 KB = ~10 MB | ~200 MB | ~400 MB |
| LFS write buffer | ≤100 ledgers × ~2 KB = ~200 KB | ~4 MB | ~8 MB |
| TxHash write buffer | ≤100 ledgers × ~10 tx × 36 B = ~36 KB | ~720 KB | ~1.4 MB |
| **Ingestion total** | — | **~205 MB** | **~410 MB** |

RocksDB block cache for meta store adds `block_cache_mb` (default **4 GB** for backfill, **8 GB** for streaming).  
Total backfill RAM: ~410 MB ingestion + 4 GB RocksDB cache ≈ **~4.5 GB**, well within the 128 GB system requirement.

**`flush_interval` RAM cap**: `flush_interval=100` keeps per-chunk write buffer under ~250 KB. Never set above 10,000 (chunk size) — that would accumulate an entire chunk in RAM before any flush.

---

## Memory Budget — Backfill (CaptiveStellarCore Mode)

| Component | Per orchestrator | Notes |
|-----------|-----------------|-------|
| CaptiveStellarCore process | ~8 GB RAM | One stellar-core process per orchestrator |
| LFS + TxHash write buffers | ~5 MB | Same flush discipline as BSB mode |
| **Total per orchestrator** | **~8 GB** | |
| **Total (2 orchestrators)** | **~16 GB** | `parallel_ranges=1` strongly recommended |

---

## Memory Budget — Streaming Mode

| Component | Size | Notes |
|-----------|------|-------|
| CaptiveStellarCore process | ~8 GB | One instance; single ledger/batch |
| Ledger store write buffer | 64 MB × 1 CF (default) × `max_write_buffer_number` | Default: 64 MB × 1 × 2 = 128 MB |
| TxHash store write buffer | 64 MB × 16 CFs × `max_write_buffer_number` | Default: 64 MB × 16 × 2 = 2,048 MB |
| RocksDB block cache | 8 GB (default `block_cache_mb`) | Shared meta + both active stores |
| **Approximate total** | **~18–19 GB** | Dominated by txhash store write buffers + core |

---

## Hardware Requirements

| Resource | Requirement | Notes |
|----------|-------------|-------|
| CPU | 32 cores | Parallelism: 40 BSB workers + RecSplit build threads |
| RAM | 128 GB | Headroom for OS, GCS client, RecSplit build working set |
| CaptiveStellarCore RAM | ~8 GB per instance | Streaming: 1 instance; backfill captive_core: 1 per orchestrator |
| Disk — active stores | SSD recommended | Low-latency write path for streaming RocksDB |
| Disk — immutable stores | HDD acceptable | Sequential writes during backfill; sequential reads at query time |
| Disk — meta store | SSD recommended | Random reads/writes for chunk flag tracking |
| Network | High bandwidth | GCS/S3 backfill fetches up to 40 BSB instances × 20 workers |

---

## Durations

| Operation | Duration | Notes |
|-----------|----------|-------|
| RecSplit build per range | ~4 hours | 1B+ transactions, 16 CF passes over 1,000 raw flat files |
| Chunk scan on resume | < 10 ms | 1,000 RocksDB `Get` calls — negligible |
| Progress log interval | 1 minute | Wall-clock elapsed from process start |

---

## `num_instances` Trade-off

| `num_instances` | BSB span | Chunks/instance | Parallelism | RAM overhead |
|----------------|----------|-----------------|-------------|-------------|
| `20` (default) | 500K ledgers | 50 | Higher | ~205 MB/orchestrator |
| `10` | 1M ledgers | 100 | Lower | ~105 MB/orchestrator |

Use `10` on memory-constrained machines; use `20` for maximum throughput.

---

## Related Documents

- [03-backfill-workflow.md](./03-backfill-workflow.md) — flush discipline, chunk write lifecycle
- [05-backfill-transition-workflow.md](./05-backfill-transition-workflow.md) — RecSplit build mechanics
- [09-directory-structure.md](./09-directory-structure.md) — on-disk paths for all stores
- [10-configuration.md](./10-configuration.md) — TOML knobs that drive these numbers
- [11-checkpointing-and-transitions.md](./11-checkpointing-and-transitions.md) — boundary math formulas
