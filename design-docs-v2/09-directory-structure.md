# Directory Structure

## Overview

The service organizes all data under a single configurable `data_dir`. Backfill mode writes directly to `immutable/` and never creates `active/` stores. Streaming mode uses `active/` for the live range and `immutable/` for completed ranges. Meta store is always present under `meta/`. No `transitioning/` directory exists — transition state is tracked entirely in the meta store.

---

## Full File Tree

```
{data_dir}/
├── meta/
│   └── rocksdb/                         ← Single meta store RocksDB instance
│       ├── MANIFEST-*
│       ├── *.sst
│       ├── *.log                        ← WAL (required — never disable)
│       └── OPTIONS-*
│
├── active/                              ← STREAMING MODE ONLY; absent during backfill
│   └── rocksdb/
│       ├── {rangeID:04d}-ledger-store/  ← Ledger store: default CF only; key=uint32BE(ledgerSeq), value=zstd(LCM)
│       │   ├── MANIFEST-*
│       │   ├── *.sst
│       │   ├── *.log
│       │   └── OPTIONS-*
│       └── {rangeID:04d}-txhash-store/  ← TxHash store: 16 CFs (one per nibble 0–f); key=txhash[32], value=uint32BE(ledgerSeq)
│           ├── MANIFEST-*
│           ├── *.sst
│           ├── *.log
│           └── OPTIONS-*
│
└── immutable/
    ├── ledgers/
    │   └── chunks/
    │       ├── 0000/                    ← chunkID/1000 directory (range 0: chunks 0–999)
    │       │   ├── 000000.data          ← chunk 0:  ledgers 2–10,001
    │       │   ├── 000000.index
    │       │   ├── 000001.data          ← chunk 1:  ledgers 10,002–20,001
    │       │   ├── 000001.index
    │       │   ├── ...
    │       │   ├── 000999.data          ← chunk 999: ledgers 9,990,002–10,000,001
    │       │   └── 000999.index
    │       ├── 0001/                    ← range 1: chunks 1000–1999
    │       │   ├── 001000.data
    │       │   ├── 001000.index
    │       │   └── ...
    │       └── {XXXX}/                  ← one directory per 1000 chunks
    │
    └── txhash/
        ├── 0000/                        ← range 0 txhash data
        │   ├── raw/
        │   │   ├── 000000.bin           ← chunk 0 raw txhash flat file (36B/entry)
        │   │   ├── 000001.bin
        │   │   ├── ...
        │   │   └── 000999.bin           ← chunk 999 raw txhash flat file
        │   └── index/
        │       ├── cf-0.idx             ← RecSplit CF 0 (txhashes starting with nibble 0)
        │       ├── cf-1.idx
        │       ├── ...
        │       ├── cf-e.idx
        │       └── cf-f.idx             ← RecSplit CF 15
        ├── 0001/
        │   ├── raw/
        │   │   └── ...
        │   └── index/
        │       └── ...
        └── {rangeID:04d}/
```

**Notes**:
- `active/` is created only when streaming mode starts its first range. Both stores are deleted after streaming transition completes.
- `immutable/txhash/{rangeID:04d}/raw/` files are created during backfill ingestion and deleted after all 16 RecSplit CFs for that range are built and verified.
- In streaming mode, raw txhash flat files are **not** created; RecSplit is built directly from the active txhash store (reading each of its 16 CFs by nibble).

---

## LFS Chunk Path Convention

```
immutable/ledgers/chunks/{XXXX}/{YYYYYY}.data
immutable/ledgers/chunks/{XXXX}/{YYYYYY}.index
```

Where:
- `XXXX` = `chunkID / 1000` (4-digit zero-padded) — groups 1000 chunks per directory
- `YYYYYY` = `chunkID` (6-digit zero-padded)

### Path Formulas

```go
func chunkDir(dataDir string, chunkID uint32) string {
    return filepath.Join(dataDir, "immutable", "ledgers", "chunks",
        fmt.Sprintf("%04d", chunkID/1000))
}

func chunkDataPath(dataDir string, chunkID uint32) string {
    return filepath.Join(chunkDir(dataDir, chunkID), fmt.Sprintf("%06d.data", chunkID))
}

func chunkIndexPath(dataDir string, chunkID uint32) string {
    return filepath.Join(chunkDir(dataDir, chunkID), fmt.Sprintf("%06d.index", chunkID))
}
```

### Chunk ID Examples

| Ledger Seq | Chunk ID | Dir | Data Path |
|-----------|----------|-----|-----------|
| 2 | 0 | `chunks/0000/` | `chunks/0000/000000.data` |
| 10,001 | 0 | `chunks/0000/` | `chunks/0000/000000.data` |
| 10,002 | 1 | `chunks/0000/` | `chunks/0000/000001.data` |
| 9,990,002 | 999 | `chunks/0000/` | `chunks/0000/000999.data` |
| 10,000,001 | 999 | `chunks/0000/` | `chunks/0000/000999.data` |
| 10,000,002 | 1000 | `chunks/0001/` | `chunks/0001/001000.data` |
| 20,000,002 | 2000 | `chunks/0002/` | `chunks/0002/002000.data` |
| 50,000,002 | 5000 | `chunks/0005/` | `chunks/0005/005000.data` |

---

## Raw TxHash Flat File Path Convention

```
immutable/txhash/{rangeID:04d}/raw/{chunkID:06d}.bin
```

**Format**: Fixed-width, no header. Each entry is 36 bytes:

```
[txhash: 32 bytes][ledgerSeq: 4 bytes big-endian uint32]
```

Files are append-only during ingestion (flushed every ~100 ledgers), fsynced at chunk completion, and deleted once all 16 RecSplit CFs for the range are built.

### Path Formulas

```go
func rawTxHashPath(dataDir string, rangeID, chunkID uint32) string {
    return filepath.Join(dataDir, "immutable", "txhash",
        fmt.Sprintf("%04d", rangeID), "raw",
        fmt.Sprintf("%06d.bin", chunkID))
}
```

### Raw TxHash File Examples

| Range ID | Chunk ID | Path |
|---------|----------|------|
| 0 | 0 | `immutable/txhash/0000/raw/000000.bin` |
| 0 | 999 | `immutable/txhash/0000/raw/000999.bin` |
| 1 | 1000 | `immutable/txhash/0001/raw/001000.bin` |

---

## RecSplit Index Path Convention

```
immutable/txhash/{rangeID:04d}/index/cf-{nibble}.idx
```

Where `nibble` is the first hex character of the txhash: `0`–`9`, `a`–`f` (16 files per range).

### Path Formulas

```go
func recSplitPath(dataDir string, rangeID uint32, nibble string) string {
    return filepath.Join(dataDir, "immutable", "txhash",
        fmt.Sprintf("%04d", rangeID), "index",
        fmt.Sprintf("cf-%s.idx", nibble))
}
```

### RecSplit Index Examples

| Range ID | Nibble | Path |
|---------|--------|------|
| 0 | 0 | `immutable/txhash/0000/index/cf-0.idx` |
| 0 | a | `immutable/txhash/0000/index/cf-a.idx` |
| 0 | f | `immutable/txhash/0000/index/cf-f.idx` |
| 1 | 0 | `immutable/txhash/0001/index/cf-0.idx` |
| 5 | f | `immutable/txhash/0005/index/cf-f.idx` |

---

## Active Store Path Convention (Streaming Mode Only)

```
active/rocksdb/{rangeID:04d}-ledger-store/   ← ledger store (default CF only)
active/rocksdb/{rangeID:04d}-txhash-store/   ← txhash store (16 CFs, one per nibble 0–f)
```

**Two separate RocksDB instances per active range:**
- **Ledger store** (`{rangeID:04d}-ledger-store/`): default CF only. `key = uint32BE(ledgerSeq)`, `value = zstd(LedgerCloseMeta)`. No column families.
- **TxHash store** (`{rangeID:04d}-txhash-store/`): 16 column families, one per first hex nibble of txhash (`0`–`f`). CF routing: `CF = txhash[0] >> 4`. `key = txhash[32]`, `value = uint32BE(ledgerSeq)`.

At most one active range exists at a time. During streaming transition, both stores are kept alive for queries until the transition completes and is verified, then both are deleted.

---

## Meta Store Path

```
meta/rocksdb/
```

Single RocksDB instance. WAL must never be disabled. Present in both backfill and streaming modes.

---

## Multi-Disk Configuration

Path overrides via TOML config allow each subtree to live on a different volume:

```
meta/rocksdb/          → [meta_store].path               (default: {data_dir}/meta/rocksdb)
active/rocksdb/        → [active_stores].base_path        (default: {data_dir}/active/rocksdb)
                         (both ledger-store and txhash-store live under this base)
immutable/ledgers/     → [immutable_stores].ledgers_base  (default: {data_dir}/immutable/ledgers)
immutable/txhash/      → [immutable_stores].txhash_base   (default: {data_dir}/immutable/txhash)
```

**Typical layout**:

| Volume | Store | Rationale |
|--------|-------|-----------|
| NVMe SSD | `meta/rocksdb/` + `active/rocksdb/` | Low-latency write path |
| HDD/object storage | `immutable/ledgers/` + `immutable/txhash/` | Large, read-mostly |

---

## Storage Estimates

See [12-metrics-and-sizing.md](./12-metrics-and-sizing.md#storage-estimates) for per-range storage estimates across all store types.

---

## Related Documents

- [01-architecture-overview.md](./01-architecture-overview.md) — store types and their roles
- [03-backfill-workflow.md](./03-backfill-workflow.md) — which files are created during backfill
- [04-streaming-workflow.md](./04-streaming-workflow.md) — active store lifecycle
- [10-configuration.md](./10-configuration.md) — TOML path overrides
- [11-checkpointing-and-transitions.md](./11-checkpointing-and-transitions.md) — chunk boundary math
- [12-metrics-and-sizing.md](./12-metrics-and-sizing.md) — storage estimates, memory budgets, hardware requirements
