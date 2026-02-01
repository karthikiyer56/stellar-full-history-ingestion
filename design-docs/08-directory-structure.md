# Directory Structure

## Overview

The Stellar Full History RPC Service organizes data across multiple storage types with a well-defined directory hierarchy. This document describes the complete file tree, path conventions, and multi-disk configuration options.

---

## File Tree

```
/data/stellar-rpc/                        # Base data directory (configurable via [service].data_dir)
├── meta/                                 # Meta store (single RocksDB instance)
│   └── rocksdb/                          # RocksDB files (SST, WAL, MANIFEST, etc.)
│
├── active/                               # Currently active stores (current range being ingested)
│   ├── ledger/                           # Active Ledger Store (RocksDB)
│   │   └── rocksdb/                      # LedgerSeq → Compressed LedgerCloseMeta
│   └── txhash/                           # Active TxHash Store (RocksDB, 16 column families)
│       └── rocksdb/                      # TxHash → LedgerSeq (partitioned by first hex char)
│
├── transitioning/                        # Previous range being transitioned (temporary)
│   ├── ledger/                           # Previous Ledger Store (still serving queries during transition)
│   │   └── rocksdb/
│   └── txhash/                           # Previous TxHash Store (still serving queries during transition)
│       └── rocksdb/
│
└── immutable/                            # Completed immutable stores (historical ranges)
    ├── ledgers/                          # Single LFS store for ALL ranges (no per-range dirs)
    │   └── chunks/                       # LFS chunks (10,000 ledgers per chunk)
    │       ├── 0000/                     # Range 0: chunks 0-999 (ledgers 2 - 10,000,001)
    │       │   ├── 000000.data           # Chunk 0 data (zstd compressed)
    │       │   ├── 000000.index          # Chunk 0 index (ledger offsets)
    │       │   ├── 000001.data           # Chunk 1 data
    │       │   ├── 000001.index          # Chunk 1 index
    │       │   └── ...                   # Chunks 2-999
    │       ├── 0001/                     # Range 1: chunks 1000-1999 (ledgers 10,000,002 - 20,000,001)
    │       │   ├── 001000.data
    │       │   ├── 001000.index
    │       │   └── ...
│       ├── 0002/                     # Range 2: chunks 2000-2999 (ledgers 20,000,002 - 30,000,001)
│       │   ├── 002000.data
│       │   ├── 002000.index
│       │   └── ...
│       ├── 0005/                     # Range 5: chunks 5000-5999 (ledgers 50,000,002 - 60,000,001)
│       │   ├── 005000.data           # First chunk (ledgers 50,000,002 - 50,010,001)
│       │   ├── 005000.index
│       │   ├── 005001.data           # Second chunk (ledgers 50,010,002 - 50,020,001)
│       │   ├── 005001.index
│       │   ├── ...
│       │   ├── 005998.data           # Second-to-last chunk (ledgers 59,980,002 - 59,990,001)
│       │   ├── 005998.index
│       │   ├── 005999.data           # Last chunk (ledgers 59,990,002 - 60,000,001)
│       │   └── 005999.index
│       └── ...                       # More parent directories as ranges are ingested
    │
    └── txhash/                           # RecSplit indexes (minimal perfect hash) - per range
        ├── 0000/                         # Range 0 txhash index
        │   └── index/
        │       ├── cf-0.idx              # RecSplit index for CF "0" (txhashes starting with 0)
        │       ├── cf-1.idx              # RecSplit index for CF "1"
        │       ├── cf-2.idx
        │       ├── cf-3.idx
        │       ├── cf-4.idx
        │       ├── cf-5.idx
        │       ├── cf-6.idx
        │       ├── cf-7.idx
        │       ├── cf-8.idx
        │       ├── cf-9.idx
        │       ├── cf-a.idx              # RecSplit index for CF "a"
        │       ├── cf-b.idx
        │       ├── cf-c.idx
        │       ├── cf-d.idx
        │       ├── cf-e.idx
        │       └── cf-f.idx              # RecSplit index for CF "f"
        ├── 0001/                         # Range 1 txhash index
        │   └── index/
        │       ├── cf-0.idx
        │       └── ...
├── 0002/                         # Range 2 txhash index
│   └── index/
├── 0005/                         # Range 5 txhash index
│   └── index/
│       ├── cf-0.idx              # First file
│       ├── cf-1.idx              # Second file
│       ├── ...
│       ├── cf-e.idx              # Second-to-last file
│       └── cf-f.idx              # Last file
└── ...
```

---

## LFS Chunk Paths

### Path Convention (MUST ADHERE)

The LFS format from `local-fs/ingestion/lfs-ledger-ingestion.go` defines the chunk path structure. This convention **MUST** be followed for compatibility with existing LFS tooling.

**Directory Structure**:
```
chunks/XXXX/YYYYYY.data
chunks/XXXX/YYYYYY.index
```

Where:
- `XXXX` = `chunkID / 1000` (4-digit zero-padded parent directory)
- `YYYYYY` = `chunkID` (6-digit zero-padded chunk ID)

### Path Calculation (Go Pseudocode)

```go
// Calculate parent directory for a chunk
func getChunkDir(dataDir string, chunkID uint32) string {
    parentDir := chunkID / 1000
    return filepath.Join(dataDir, "chunks", fmt.Sprintf("%04d", parentDir))
}

// Calculate data file path
func getDataPath(dataDir string, chunkID uint32) string {
    return filepath.Join(getChunkDir(dataDir, chunkID), fmt.Sprintf("%06d.data", chunkID))
}

// Calculate index file path
func getIndexPath(dataDir string, chunkID uint32) string {
    return filepath.Join(getChunkDir(dataDir, chunkID), fmt.Sprintf("%06d.index", chunkID))
}
```

### Chunk ID Calculation

```go
const (
    FirstLedger       = 2
    LedgersPerChunk   = 10_000
)

// Convert ledger sequence to chunk ID
func ledgerToChunkID(ledgerSeq uint32) uint32 {
    return (ledgerSeq - FirstLedger) / LedgersPerChunk
}

// Get first ledger in a chunk
func chunkFirstLedger(chunkID uint32) uint32 {
    return (chunkID * LedgersPerChunk) + FirstLedger
}

// Get last ledger in a chunk
func chunkLastLedger(chunkID uint32) uint32 {
    return ((chunkID + 1) * LedgersPerChunk) + FirstLedger - 1
}
```

### Example Paths

| Ledger Sequence | Chunk ID | Parent Dir | Data Path | Index Path |
|-----------------|----------|------------|-----------|------------|
| 2 | 0 | `chunks/0000/` | `chunks/0000/000000.data` | `chunks/0000/000000.index` |
| 10,002 | 1 | `chunks/0000/` | `chunks/0000/000001.data` | `chunks/0000/000001.index` |
| 1,000,002 | 100 | `chunks/0000/` | `chunks/0000/000100.data` | `chunks/0000/000100.index` |
| 10,000,002 | 1000 | `chunks/0001/` | `chunks/0001/001000.data` | `chunks/0001/001000.index` |
| 20,000,002 | 2000 | `chunks/0002/` | `chunks/0002/002000.data` | `chunks/0002/002000.index` |
| 50,000,002 | 5000 | `chunks/0005/` | `chunks/0005/005000.data` | `chunks/0005/005000.index` |
| 59,990,002 | 5999 | `chunks/0005/` | `chunks/0005/005999.data` | `chunks/0005/005999.index` |

---

## RecSplit Paths

### Path Convention

RecSplit indexes are organized by range (using 4-digit zero-padded directory names to match LFS convention) and column family:

```
immutable/txhash/XXXX/index/cf-{X}.idx
```

Where:
- `XXXX` = Range ID (4-digit zero-padded: 0000, 0001, 0002, ...)
- `{X}` = Column family name (0-9, a-f)

### Path Calculation (Go Pseudocode)

```go
// Get RecSplit index path for a specific range and CF
func getRecSplitPath(baseDir string, rangeID uint32, cfName string) string {
    return filepath.Join(
        baseDir,
        "immutable",
        "txhash",
        fmt.Sprintf("%04d", rangeID),
        "index",
        fmt.Sprintf("cf-%s.idx", cfName),
    )
}
```

### Example Paths

| Range ID | CF Name | Path |
|----------|---------|------|
| 0 | 0 | `immutable/txhash/0000/index/cf-0.idx` |
| 0 | a | `immutable/txhash/0000/index/cf-a.idx` |
| 1 | 5 | `immutable/txhash/0001/index/cf-5.idx` |
| 2 | f | `immutable/txhash/0002/index/cf-f.idx` |
| 5 | 0 | `immutable/txhash/0005/index/cf-0.idx` |
| 5 | f | `immutable/txhash/0005/index/cf-f.idx` |

---

## Multi-Disk Configuration

The service supports flexible multi-disk configurations to optimize performance and cost.

### Scenario 1: Single Disk (Default)

All data stored under one `data_dir`:

```toml
[service]
data_dir = "/data/stellar-rpc"
```

**Result**:
- Meta store: `/data/stellar-rpc/meta/rocksdb`
- Active stores: `/data/stellar-rpc/active/ledger/rocksdb`, `/data/stellar-rpc/active/txhash/rocksdb`
- Immutable stores: `/data/stellar-rpc/immutable/ledgers/`, `/data/stellar-rpc/immutable/txhash/`

### Scenario 2: Separate Volumes for Active and Immutable

Place active stores on fast NVMe, immutable stores on cheaper HDD:

```toml
[service]
data_dir = "/data/stellar-rpc"  # Default for meta store

[active_stores]
ledger_path = "/nvme/stellar-rpc/active/ledger/rocksdb"
txhash_path = "/nvme/stellar-rpc/active/txhash/rocksdb"

[immutable_stores]
ledgers_base = "/hdd/stellar-rpc/immutable/ledgers"
txhash_base = "/hdd/stellar-rpc/immutable/txhash"
```

**Result**:
- Meta store: `/data/stellar-rpc/meta/rocksdb` (default disk)
- Active stores: `/nvme/stellar-rpc/active/...` (fast NVMe)
- Immutable stores: `/hdd/stellar-rpc/immutable/...` (large HDD)

### Scenario 3: Dedicated Disk for Meta Store

Place meta store on separate disk for isolation:

```toml
[service]
data_dir = "/data/stellar-rpc"

[meta_store]
path = "/ssd1/stellar-rpc/meta/rocksdb"

[active_stores]
ledger_path = "/ssd2/stellar-rpc/active/ledger/rocksdb"
txhash_path = "/ssd2/stellar-rpc/active/txhash/rocksdb"

[immutable_stores]
ledgers_base = "/hdd/stellar-rpc/immutable/ledgers"
txhash_base = "/hdd/stellar-rpc/immutable/txhash"
```

**Result**:
- Meta store: `/ssd1/...` (isolated disk)
- Active stores: `/ssd2/...` (shared fast disk)
- Immutable stores: `/hdd/...` (large HDD)

### Scenario 4: Path Override Pattern

Any `*_path` or `*_base` configuration key can be overridden with an absolute path to a different volume. The service does NOT implement internal striping; use volume manager (LVM, ZFS, etc.) if striping is needed.

**Key Override Rules**:
- If a path is **relative**, it's resolved relative to `data_dir`
- If a path is **absolute**, it's used as-is (different volume)
- Defaults are always relative to `data_dir`

---

## Storage Requirements

> For detailed calculations and methodology, see [Storage Size Reference](./01-architecture-overview.md#storage-size-reference-per-10m-ledger-range).

### Per-Range Estimates

Based on real-world data from ledger ranges 30,000,002 - 60,000,001. Earlier ranges may have smaller sizes due to lower network activity.

| Store Type | Size per 10M Ledgers | Notes |
|------------|----------------------|-------|
| Active Ledger (RocksDB) | ~1.58 TB | 10M × 150KB avg LCM + 5% overhead |
| Active TxHash (RocksDB) | ~140 GB | 3.25B entries × 36 bytes + 25% overhead |
| Immutable Ledgers (LFS) | ~1.5 TB | zstd compressed chunks |
| Immutable TxHash (RecSplit) | ~15 GB | 16 minimal perfect hash indexes |

### Total Storage (Example: 100M Ledgers)

| Component | Size                |
|-----------|---------------------|
| 10 Immutable Ledger Ranges | 10 × 1.5 TB = 15 TB |
| 10 Immutable TxHash Ranges | 10 × 15 GB = 150 GB |
| 1 Active Ledger Store | ~1.58 TB            |
| 1 Active TxHash Store | ~140 GB             |
| Meta Store | ~100 MB             |
| **Total** | **~16.87 TB**       |

---

## Related Documents

- [Configuration Reference](./09-configuration.md) - TOML configuration for all paths
- [Architecture Overview](./01-architecture-overview.md#store-types) - Store types and responsibilities
- [Query Routing](./07-query-routing.md) - How queries locate data in this structure

---

## References

- Existing LFS implementation: `local-fs/ingestion/lfs-ledger-ingestion.go`
- Existing RecSplit implementation: `txhash-ingestion-workflow/workflow.go`
