# LFS Input to RocksDB Output Processor

This tool reads ledgers from an LFS (Local Filesystem) store, extracts transaction
hashes, and writes `tx_hash -> ledger_seq` mappings to a RocksDB store with 16
column families.

## Features

- **Multi-worker parallelism**: Each worker processes a contiguous ledger range
- **Single writer goroutine**: All RocksDB writes go through one goroutine for consistency
- **WAL enabled**: Crash recovery is supported across reboots
- **No automatic compaction**: Use `compact-store` utility when ready for queries

## Column Family Structure

The output store uses 16 column families, partitioned by the first hex character
of the transaction hash:

| CF Name | Hash Prefix | Example Hash |
|---------|-------------|--------------|
| 0 | 0x00-0x0F | `0a3b5c...` |
| 1 | 0x10-0x1F | `1f2e3d...` |
| ... | ... | ... |
| f | 0xF0-0xFF | `fab123...` |

This partitioning enables:
- Parallel processing during recsplit index building
- Efficient range scans within each partition
- Balanced distribution (each CF gets ~6.25% of entries)

## Usage

### Command Line

```bash
# Process all ledgers in an LFS store
tx-hash-lfs-to-rocksdb \
  --lfs-store /data/stellar/lfs \
  --output /data/stellar/tx-hash-store

# Process specific ledger range with 8 workers
tx-hash-lfs-to-rocksdb \
  --lfs-store /data/stellar/lfs \
  --output /data/stellar/tx-hash-store \
  --start-ledger 1000000 \
  --end-ledger 2000000 \
  --workers 8

# Log to file (logs go ONLY to file, not stdout)
tx-hash-lfs-to-rocksdb \
  --lfs-store /data/stellar/lfs \
  --output /data/stellar/tx-hash-store \
  --log-file /var/log/tx-hash-processor.log
```

### With Config File

```bash
tx-hash-lfs-to-rocksdb --config config.toml
```

Example `config.toml`:

```toml
output_path = "/data/stellar/tx-hash-store"
batch_size = 100000

[lfs]
lfs_store_path = "/data/stellar/lfs"
start_ledger = 0      # 0 = auto-detect
end_ledger = 0        # 0 = auto-detect
workers = 16

[rocksdb]
write_buffer_size_mb = 256
max_write_buffer_number = 4
block_cache_size_mb = 8192
bloom_filter_bits_per_key = 12
disable_wal = false
disable_auto_compactions = true
```

## Options

| Flag | Description | Default |
|------|-------------|---------|
| `--lfs-store` | Path to LFS ledger store | (required) |
| `--output` | Path for output RocksDB store | (required) |
| `--start-ledger` | First ledger to process | auto-detect |
| `--end-ledger` | Last ledger to process | auto-detect |
| `--workers` | Number of worker goroutines | NumCPU |
| `--batch-size` | Entries per write batch | 100000 |
| `--config` | Path to TOML config file | (none) |
| `--log-file` | Log to file instead of stdout | stdout |
| `--error-file` | Error log to file instead of stderr | stderr |
| `--show-memory` | Show memory configuration and exit | |
| `--help` | Show help message | |
| `--version` | Show version information | |

## Memory Configuration

Default memory budget (~24GB):

| Component | Size | Calculation |
|-----------|------|-------------|
| MemTables | 16 GB | 256 MB × 4 × 16 CFs |
| Block Cache | 8 GB | Shared across all CFs |
| **Total** | **~24 GB** | |

To adjust, modify the `[rocksdb]` section in your config file.

## RocksDB Settings

| Setting | Value | Rationale |
|---------|-------|-----------|
| WAL | Enabled | Crash recovery across reboots |
| Auto Compaction | Disabled | Manual compaction after ingestion |
| Bloom Filter | 12 bits/key | ~0.3% false positive rate |
| Compression | None | Values are 4 bytes, compression adds overhead |

## Post-Processing

After ingestion completes:

1. **Run compaction** to optimize the store for reads:
   ```bash
   compact-store --store /data/stellar/tx-hash-store
   ```

2. **Build recsplit index** (optional) for constant-time lookups:
   ```bash
   # Use the recsplit tool from stellar-core
   ```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Main Thread                               │
│  ┌─────────────┐                                                │
│  │ Coordinator │ → Splits work, monitors progress               │
│  └─────────────┘                                                │
└─────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Worker Goroutines                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ Worker 0 │  │ Worker 1 │  │ Worker 2 │  │ Worker N │        │
│  │ L1 - L100│  │L101-L200 │  │L201-L300 │  │ ...      │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
│       │             │             │             │                │
│       └─────────────┴─────────────┴─────────────┘                │
│                          │                                       │
│                          ▼ (batch channel)                       │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Writer Goroutine                              │
│  ┌────────────────────────────────────────┐                     │
│  │ Receives batches, writes to RocksDB    │                     │
│  │ Single writer ensures consistency      │                     │
│  └────────────────────────────────────────┘                     │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    RocksDB Store                                 │
│  ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐ ... ┌────┐                 │
│  │CF 0│ │CF 1│ │CF 2│ │CF 3│ │CF 4│     │CF f│                 │
│  └────┘ └────┘ └────┘ └────┘ └────┘     └────┘                 │
└─────────────────────────────────────────────────────────────────┘
```

## Performance Tips

1. **Use SSDs**: LFS random reads benefit greatly from SSDs
2. **Match workers to CPU cores**: More workers than cores adds overhead
3. **Larger batch size**: Reduces RocksDB write overhead, but uses more memory
4. **Monitor memory**: Watch for OOM if processing very large ranges
