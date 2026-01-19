# compact-store

Manual compaction utility for RocksDB stores with 16 column families.

## Overview

This tool triggers manual compaction on a RocksDB store containing `tx_hash -> ledger_seq`
mappings. It's designed to be run after bulk ingestion when automatic compaction is disabled.

Compaction:
- Merges SST files to reduce read amplification
- Removes duplicate/deleted entries
- Optimizes the store for read performance

## Usage

### Compact All Column Families

```bash
# Sequential compaction (one CF at a time)
compact-store --store /data/tx-hash-store

# Parallel compaction (all CFs simultaneously)
compact-store --store /data/tx-hash-store --parallel
```

### Compact Specific Column Families

```bash
# Compact only CFs 0, 1, and 2
compact-store --store /data/tx-hash-store --cf 0 --cf 1 --cf 2
```

### Log to File

```bash
compact-store --store /data/tx-hash-store --log-file /var/log/compaction.log
```

## Options

| Flag | Description | Default |
|------|-------------|---------|
| `--store` | Path to RocksDB store | (required) |
| `--cf` | Compact only this column family (can be repeated) | all CFs |
| `--parallel` | Compact column families in parallel | sequential |
| `--log-file` | Log to file instead of stdout | stdout |
| `--help` | Show help message | |
| `--version` | Show version information | |

## Column Families

The tool operates on stores with 16 column families:

```
0, 1, 2, 3, 4, 5, 6, 7, 8, 9, a, b, c, d, e, f
```

Each column family contains transaction hashes where the first hex character matches
the CF name.

## Compaction Modes

### Sequential (Default)

Compacts one column family at a time. Lower resource usage but takes longer.

```
Compacting CF[0]... done (2m 15s)
Compacting CF[1]... done (2m 08s)
...
Compacting CF[f]... done (2m 22s)
Total time: 35m 42s
```

### Parallel (`--parallel`)

Compacts all column families simultaneously. Uses more CPU and I/O but completes faster.

```
CF[0]: completed in 5m 30s
CF[1]: completed in 5m 15s
CF[2]: completed in 5m 45s
...
Total time: 6m 12s
```

**Note**: Parallel mode works best when:
- You have many CPU cores
- Storage can handle high IOPS (NVMe SSD)
- System has enough memory for all concurrent operations

## When to Run

Run compaction after:

1. **Bulk ingestion** with auto-compaction disabled
2. **Merging multiple stores** into one
3. **Before building recsplit index** for optimal read performance

## Resource Usage

| Mode | CPU | I/O | Memory | Duration |
|------|-----|-----|--------|----------|
| Sequential | Low | Moderate | Low | Longer |
| Parallel | High | High | High | Shorter |

## Example Workflow

```bash
# 1. Ingest data with auto-compaction disabled
tx-hash-lfs-to-rocksdb \
  --lfs-store /data/lfs \
  --output /data/tx-hash-store

# 2. Run manual compaction
compact-store --store /data/tx-hash-store --parallel

# 3. Build recsplit index (optional)
recsplit-builder --store /data/tx-hash-store --output /data/tx-hash.recsplit
```

## Notes

- Compaction is CPU and I/O intensive - plan accordingly
- The store remains readable during compaction
- Progress is logged for each column family
- Can be safely interrupted (Ctrl+C) - resume with remaining CFs

## Building

```bash
go build ./rocksdb/compact-store/
```
