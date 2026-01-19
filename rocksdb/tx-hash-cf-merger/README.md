# TX Hash to Ledger Sequence - Column Family Merger

Merges multiple `tx_hash_to_ledger_seq` RocksDB stores into a single store with 16 column families, partitioned by the first hex character of the transaction hash.

**Also supports building the mapping from an LFS (Local Filesystem) ledger store.**

## Input Sources

This tool supports two input sources (mutually exclusive):

### Option 1: Existing RocksDB Stores (`input_stores`)

Merge multiple existing `tx_hash_to_ledger_seq` stores:

```toml
input_stores = [
    "/data/stellar/tx_hash_to_ledger_seq/2020",
    "/data/stellar/tx_hash_to_ledger_seq/2021",
    "/data/stellar/tx_hash_to_ledger_seq/2022",
]
```

### Option 2: LFS Ledger Store (`lfs_ledger_store_path`)

Build the mapping by reading ledgers from an LFS store and extracting transaction hashes:

```toml
lfs_ledger_store_path = "/data/stellar/ledger-store"
lfs_start_ledger = 0  # 0 = auto-detect
lfs_end_ledger = 0    # 0 = auto-detect
lfs_workers = 16      # default: NumCPU
```

This mode:
- Uses multiple parallel workers to read ledgers
- Extracts transactions using `LedgerTransactionReader`
- Maps each transaction hash to its ledger sequence
- Progress logged every 1% or 60 seconds

## Two-Phase Workflow

This tool supports a **two-phase workflow** for optimal performance:

### Phase 1: Bulk Load (This Tool)

Merge existing stores with settings optimized for **write throughput**:
- WAL disabled (faster writes, crash = restart from scratch)
- Auto-compaction disabled (one final compaction at end)
- High L0 triggers (no write stalls)
- Large MemTables (8 GB total for 64GB RAM)

### Phase 2: Active Use (Generated Config)

After merge completes, use the store for **real-time ingestion + queries**:
- WAL enabled (crash recovery)
- Auto-compaction enabled (balanced read/write)
- Normal L0 triggers (4/20/36)
- Large block cache (16 GB for read performance)

The tool automatically generates `active_store_config.toml` in the output directory.

## Memory Requirements (64GB RAM Target)

| Phase | MemTables | Block Cache | Total |
|-------|-----------|-------------|-------|
| Bulk Load | 8 GB | 512 MB | ~9 GB |
| Active Use | 4 GB | 16 GB | ~20 GB |

Additional memory for bloom filters is loaded on-demand per SST file (~18 GB for 15B entries if all loaded).

## Column Family Partitioning

Transaction hashes are 32-byte random values. The first hex character (high nibble of first byte) determines the column family:

| First Byte | CF Name | Description |
|------------|---------|-------------|
| 0x00-0x0F  | `0`     | Hashes starting with 0... |
| 0x10-0x1F  | `1`     | Hashes starting with 1... |
| ...        | ...     | ... |
| 0xF0-0xFF  | `f`     | Hashes starting with f... |

### Benefits

1. **Even Distribution**: Hash randomness ensures ~6.25% of entries per CF
2. **Parallel I/O**: Each CF can be read/written independently
3. **Separate Compaction**: CFs are compacted separately, reducing memory pressure
4. **Isolation**: Issues in one CF don't affect others

## Building

```bash
cd /path/to/stellar-full-history-ingestion/rocksdb/tx-hash-cf-merger
go build -o tx-hash-cf-merger .
```

Or use the provided script with library paths:

```bash
./run_merger.sh --config config.toml
```

## Usage

### 1. Configure

Edit `config.toml` to specify your input source:

**For existing RocksDB stores:**
```toml
input_stores = ["/data/stellar/tx_hash_to_ledger_seq/2020", ...]
output_path = "/data/stellar/tx_hash_to_ledger_seq_merged"
```

**For LFS ledger store:**
```toml
# Comment out input_stores
lfs_ledger_store_path = "/data/stellar/ledger-store"
output_path = "/data/stellar/tx_hash_to_ledger_seq_merged"
```

### 2. Dry Run (Optional)

Validate configuration and count entries without writing:

```bash
./tx-hash-cf-merger --config config.toml --dry-run
```

### 3. Execute Merge

**From RocksDB stores:**
```bash
./tx-hash-cf-merger --config config.toml
```

**From LFS store with logging:**
```bash
./tx-hash-cf-merger --config config.toml \
    --lfs-workers 16 \
    --log-file /var/log/merger.log \
    --error-file /var/log/merger.err
```

After completion:
- Final compaction is performed automatically
- `active_store_config.toml` is generated in the output directory
- Instructions for Phase 2 are printed

### 4. Use the Merged Store (Phase 2)

Open the store with settings from the generated `active_store_config.toml`:

```go
// Load the generated config
cfg := LoadActiveStoreConfig("path/to/active_store_config.toml")

// Open with WAL enabled, auto-compaction enabled
db := OpenStoreWithActiveSettings(cfg)
```

## Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `--config` | Path to TOML configuration file | Required |
| `--batch-size` | Entries per write batch | 100000 |
| `--lfs-workers` | Override number of LFS workers | NumCPU |
| `--log-file` | Path to info log file (in addition to stdout) | None |
| `--error-file` | Path to error log file (in addition to stderr) | None |
| `--dry-run` | Validate config without writing | false |

## Configuration Reference

See `config.toml` for detailed documentation of all settings.

### Bulk Load Settings (`[rocksdb]`)

```toml
[rocksdb]
# Per-CF MemTable settings
write_buffer_size_mb = 128     # 128 × 4 × 16 = 8 GB total
max_write_buffer_number = 4

# Disable compaction during bulk load
l0_compaction_trigger = 999
l0_slowdown_writes_trigger = 999
l0_stop_writes_trigger = 999

# SST file organization
target_file_size_mb = 256
max_bytes_for_level_base_mb = 2560

# Bloom filters for hash lookups
bloom_filter_bits_per_key = 10

# Minimal block cache during writes
block_cache_size_mb = 512

# WAL disabled for speed
disable_wal = true
```

### Active Use Settings (`[active_store]`)

```toml
[active_store]
# WAL enabled for crash recovery
enable_wal = true

# Normal compaction triggers
l0_compaction_trigger = 4
l0_slowdown_writes_trigger = 20
l0_stop_writes_trigger = 36

# Large block cache for reads
block_cache_size_mb = 16384     # 16 GB

# Smaller MemTables for active use
write_buffer_size_mb = 64       # 64 × 4 × 16 = 4 GB total
max_write_buffer_number = 4
```

## Expected Performance

For 15 billion transactions (~600 GB raw data, ~37 GB per CF):

| Metric | Estimate |
|--------|----------|
| Read throughput | 500K-1M entries/sec |
| Write throughput | 300K-500K entries/sec |
| Final compaction | 1-3 hours |
| Total merge time | 4-8 hours |
| Final disk size | ~700-800 GB (with bloom filters) |

## Output Store Structure

```
output_path/
├── 000001.sst              # SST files (shared across CFs)
├── ...
├── CURRENT
├── IDENTITY
├── LOCK
├── LOG
├── MANIFEST-000001
├── OPTIONS-000001
└── active_store_config.toml  # Generated config for Phase 2
```

The store contains 17 column families:
- `default` (empty, required by RocksDB)
- `0` through `f` (16 data column families)

## Querying the Output Store

To look up a transaction hash:

```go
// Determine column family from hash
cfIndex := txHash[0] >> 4  // 0-15
cfName := fmt.Sprintf("%x", cfIndex)  // "0"-"f"

// Query the appropriate column family
cfHandle := db.GetCFHandle(cfName)
value, err := db.GetCF(readOpts, cfHandle, txHash)

// Value is 4-byte uint32 ledger sequence (big-endian)
ledgerSeq := binary.BigEndian.Uint32(value)
```

## Troubleshooting

### Out of Memory During Bulk Load

Reduce MemTable memory:

```toml
[rocksdb]
write_buffer_size_mb = 64    # 64 × 4 × 16 = 4 GB
max_write_buffer_number = 2  # 128 × 2 × 16 = 4 GB
```

### Slow Write Performance

1. Increase batch size:
   ```bash
   ./tx-hash-cf-merger --config config.toml --batch-size 500000
   ```

2. Ensure WAL is disabled in config:
   ```toml
   [rocksdb]
   disable_wal = true
   ```

3. Increase MemTable size if RAM permits.

### Final Compaction Taking Too Long

Increase background jobs:

```toml
[rocksdb]
max_background_jobs = 16  # More parallelism
```

### Too Many Open Files

Increase system limits and config:

```bash
ulimit -n 50000
```

```toml
[rocksdb]
max_open_files = 50000
```

### Read Performance Issues in Active Mode

1. Increase block cache:
   ```toml
   [active_store]
   block_cache_size_mb = 24576  # 24 GB if RAM permits
   ```

2. Ensure bloom filters are enabled (default 10 bits/key).

## Duplicate Key Handling

If the same transaction hash exists in multiple input stores:
- **Last value wins** - the value from the last input store is kept
- This is typically fine since tx_hash → ledger_seq is immutable (same hash always maps to same ledger)

If you need different behavior, pre-filter input stores before merging.
