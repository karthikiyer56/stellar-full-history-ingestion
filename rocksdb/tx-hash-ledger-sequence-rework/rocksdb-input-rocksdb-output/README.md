# RocksDB Input to RocksDB Output Merge Processor

This tool merges multiple existing RocksDB stores containing `tx_hash -> ledger_seq`
mappings into a single output store with 16 column families.

## Use Cases

- **Merging stores from different time periods**: Combine monthly or yearly stores
- **Converting single-CF stores to multi-CF format**: Upgrade legacy stores
- **Consolidating distributed stores**: Merge stores from multiple machines

## Features

- **Sequential processing**: Processes one input store at a time (safe, predictable)
- **Column family aware**: Reads from and writes to proper column families
- **WAL enabled**: Crash recovery is supported
- **No automatic compaction**: Use `compact-store` utility when ready

## Usage

### Command Line

```bash
# Merge two stores
tx-hash-merge-stores \
  --input /data/store1 \
  --input /data/store2 \
  --output /data/merged-store

# Merge three stores with custom batch size
tx-hash-merge-stores \
  --input /data/2023-store \
  --input /data/2024-q1-store \
  --input /data/2024-q2-store \
  --output /data/full-store \
  --batch-size 200000

# Log to file
tx-hash-merge-stores \
  --input /data/store1 \
  --input /data/store2 \
  --output /data/merged-store \
  --log-file /var/log/merge.log
```

### With Config File

```bash
tx-hash-merge-stores --config config.toml
```

Example `config.toml`:

```toml
output_path = "/data/merged-store"
batch_size = 100000

input_stores = [
  "/data/store1",
  "/data/store2",
  "/data/store3"
]

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
| `--input` | Path to input RocksDB store (can be repeated) | (required) |
| `--output` | Path for output RocksDB store | (required) |
| `--batch-size` | Entries per write batch | 100000 |
| `--config` | Path to TOML config file | (none) |
| `--log-file` | Log to file instead of stdout | stdout |
| `--error-file` | Error log to file instead of stderr | stderr |
| `--help` | Show help message | |
| `--version` | Show version information | |

## Processing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     Input Store 1                                │
│  ┌────┐ ┌────┐ ┌────┐ ... ┌────┐                               │
│  │CF 0│ │CF 1│ │CF 2│     │CF f│                               │
│  └─┬──┘ └─┬──┘ └─┬──┘     └─┬──┘                               │
│    │      │      │          │                                    │
│    └──────┴──────┴──────────┘                                    │
│              │                                                   │
│              ▼ (iterate & batch)                                 │
└─────────────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Merge Processor                              │
│  ┌────────────────────────────────────────┐                     │
│  │ - Reads from each input CF             │                     │
│  │ - Batches entries                      │                     │
│  │ - Routes to output CF by hash prefix   │                     │
│  └────────────────────────────────────────┘                     │
└─────────────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Output Store                                 │
│  ┌────┐ ┌────┐ ┌────┐ ... ┌────┐                               │
│  │CF 0│ │CF 1│ │CF 2│     │CF f│                               │
│  └────┘ └────┘ └────┘     └────┘                               │
└─────────────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Input Store 2                                │
│              (repeat for all input stores)                       │
└─────────────────────────────────────────────────────────────────┘
```

## Handling Duplicates

If the same transaction hash exists in multiple input stores:
- **Last write wins**: The value from the last input store is kept
- This is idempotent: re-running the merge produces the same result
- Normally, duplicate tx hashes should have the same ledger sequence

## Memory Configuration

Default memory budget (~24GB):

| Component | Size | Calculation |
|-----------|------|-------------|
| MemTables | 16 GB | 256 MB × 4 × 16 CFs |
| Block Cache | 8 GB | Shared across all CFs |
| **Total** | **~24 GB** | |

## Post-Processing

After merge completes:

1. **Run compaction** to optimize the store for reads:
   ```bash
   compact-store --store /data/merged-store
   ```

2. **Verify the store**:
   ```bash
   # Check estimated key counts
   rocksdb_ldb --db=/data/merged-store list_column_families
   ```

## Performance Tips

1. **Input store order**: Process larger stores first for better caching
2. **SSD storage**: Both input and output should be on SSDs
3. **Batch size**: Larger batches reduce write overhead but use more memory
4. **Memory**: Ensure enough RAM for memtables + block cache
