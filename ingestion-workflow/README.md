# Ingestion Workflow

A complete Stellar blockchain data ingestion pipeline that fetches ledger data from GCS, stores it in RocksDB during ingestion, and transitions it to immutable storage (LFS chunks + RecSplit indexes).

## Overview

The ingestion workflow implements a three-phase lifecycle for Stellar ledger data:

1. **INGESTING**: Fetch ledgers from GCS → Write to active RocksDB stores (LCM + TxHash)
2. **TRANSITIONING**: Compact RocksDB → Export to immutable formats (LFS chunks + RecSplit indexes)
3. **COMPLETE**: Query from immutable storage, optionally delete RocksDB stores

### Key Features

- **Two-level phase tracking**: Range-level state + independent ledger/txhash sub-phases
- **Parallel compaction**: Ledger and TxHash stores compacted simultaneously during transition
- **Chunk-level crash recovery**: Resume LFS writing from the last completed chunk
- **Per-range backends**: Each 10M ledger range gets its own GCS backend instance for isolation
- **Atomic checkpointing**: Checkpoint every N ledgers (default: 1000) with full state preservation
- **RocksDB deletion gate**: Automatically deletes active stores only after BOTH phases complete

## Prerequisites

See top-level README for RocksDB build instructions and environment setup.

## Building

From repository root:

```bash
go build -o ingestion-workflow ./ingestion-workflow/
```

The binary will be ~46 MB. Run it directly or install to PATH.

## Configuration

Create a TOML config file (example: `config.toml`):

```toml
[service]
data_dir = "/data/stellar"  # Base directory for all data
mode = "backfill"            # Ingestion mode

[backfill]
start_ledger = 2             # First ledger to ingest (genesis is ledger 2)
end_ledger = 10000001        # Last ledger to ingest (exclusive)
ledger_backend = "buffered_storage"
parallel_ranges = 2          # Number of concurrent 10M ranges
checkpoint_interval = 1000   # Checkpoint every N ledgers

[backfill.buffered_storage]
bucket_path = "gs://your-bucket/stellar-pubnet/ledgers"  # GCS bucket path
buffer_size = 100            # Number of ledgers to buffer
num_workers = 5              # Parallel download workers

[active_stores]
base_path = "active/rocksdb"  # Path for active RocksDB stores (relative to data_dir)

[immutable_stores]
ledgers_base = "immutable/ledgers"  # Path for LFS chunks (relative to data_dir)
txhash_base = "immutable/txhash"    # Path for RecSplit indexes (relative to data_dir)
compression_level = 3                # Zstd compression level (1-22)

[rocksdb]
block_cache_mb = 512         # Block cache size per store
write_buffer_mb = 128        # Write buffer size per MemTable
max_write_buffer_number = 3  # Max MemTables before flush

[transition]
preserve_rocksdb_after_transition = false  # Delete RocksDB stores after transition completes

[metrics]
log_interval = 10  # Log metrics every N seconds (0 = disabled)
```

### Network Selection

The tool automatically selects the Stellar network passphrase based on config:
- If `service.mode = "testnet"` → uses testnet passphrase
- If `bucket_path` contains "testnet" → uses testnet passphrase
- Otherwise → uses pubnet (mainnet) passphrase

## Usage

### Dry Run (Validate Configuration)

```bash
./ingestion-workflow --config config.toml --dry-run
```

Example output:
```
Config valid.
Ledger range: 2 - 10000001
Total ranges: 1000
Parallel ranges: 2
Ledgers per range: 10000
```

### Full Ingestion

```bash
./ingestion-workflow \
  --config config.toml \
  --log-file /data/stellar/logs/ingestion.log \
  --error-file /data/stellar/logs/errors.log \
  --verbose
```

**Flags**:
- `--config <path>` (required): Path to TOML config file
- `--dry-run`: Validate config and exit (no processing)
- `--log-file <path>`: Log file for INFO/DEBUG messages (default: stdout)
- `--error-file <path>`: Error file for ERROR/WARN messages (default: stderr)
- `--verbose`: Enable DEBUG-level logging

### Monitoring Progress

Logs show progress for each range:

```
[2026-02-03T12:00:00Z] [INFO] [BACKFILL] Starting backfill for 1000 ranges (parallel=2)
[2026-02-03T12:00:01Z] [INFO] [BACKFILL] Range 0: starting
[2026-02-03T12:00:02Z] [INFO] [RANGE-0000] Ingestion starting (ledgers 2 - 10000001)
[2026-02-03T12:05:30Z] [INFO] [RANGE-0000] Checkpoint saved at ledger 1000 (1.2K ledgers/s)
...
[2026-02-03T14:30:00Z] [INFO] [RANGE-0000] Ingestion complete (10M ledgers, 2h 29m)
[2026-02-03T14:30:01Z] [INFO] [TRANSITION] Range 0: ledger phase → COMPACTING
[2026-02-03T14:35:00Z] [INFO] [TRANSITION] Range 0: ledger compaction completed in 4m 59s
[2026-02-03T14:35:01Z] [INFO] [TRANSITION] Range 0: txhash phase → COMPACTING
...
```

## Directory Structure

After ingestion, the data directory will contain:

```
/data/stellar/
├── meta/                          # Meta store (range state, checkpoints)
│   └── *.sst                      # RocksDB SST files
├── active/
│   └── rocksdb/
│       ├── 0000-ledger-store/     # LCM RocksDB store for range 0
│       ├── 0000-txhash-store/     # TxHash RocksDB store for range 0
│       ├── 0001-ledger-store/     # Range 1 stores
│       └── 0001-txhash-store/
└── immutable/
    ├── ledgers/
    │   └── chunks/
    │       ├── 0000/              # Parent directory (every 10K chunks)
    │       │   ├── 000000.data    # Chunk 0: ledgers 2-10001 (zstd compressed)
    │       │   ├── 000000.index   # Chunk 0 index (binary offsets)
    │       │   ├── 000001.data
    │       │   └── 000001.index
    │       └── 0001/
    │           └── ...
    └── txhash/
        ├── 0000/                  # Range 0 indexes
        │   └── index/
        │       ├── cf-0.idx       # RecSplit index for CF 0
        │       ├── cf-1.idx
        │       └── ...
        └── 0001/                  # Range 1 indexes
            └── index/
                └── ...
```

### Storage Sizes (Approximate)

For 10M ledgers (one range):
- **Active RocksDB** (during ingestion): ~300-500 GB (LCM) + ~50-100 GB (TxHash)
- **Immutable LFS** (after transition): ~80-120 GB (zstd level 3)
- **Immutable RecSplit** (after transition): ~10-20 GB (indexes)

After transition with `preserve_rocksdb_after_transition = false`, only immutable storage remains.

## Crash Recovery

The workflow supports crash recovery at multiple granularities:

### Ledger-Level Recovery

If crashed during **INGESTING** phase:
- Reads `range:{id}:ledger:last_committed` from meta store
- Reads `range:{id}:txhash:last_committed` from meta store
- Resumes from `min(ledger_checkpoint, txhash_checkpoint) + 1`

### Chunk-Level Recovery

If crashed during **WRITING_LFS** phase:
- Reads `range:{id}:ledger:lfs_last_chunk_written` from meta store
- Resumes from chunk `lastWritten + 1`
- Example: If chunk 42 was written, resume from chunk 43

### Compaction Recovery

If crashed during **COMPACTING** phase:
- Restarts compaction (idempotent operation)
- No partial state to recover

### RecSplit Recovery

If crashed during **BUILDING_RECSPLIT** phase:
- Rebuilds all 16 RecSplit indexes from scratch
- Uses CF counts from meta store to verify completeness

## Performance

Typical throughput on modern hardware (32 cores, NVMe SSD, 10 Gbps network):

- **Ingestion**: 1,000-2,000 ledgers/second
- **LCM compaction**: ~5 minutes per 10M ledgers
- **TxHash compaction**: ~3 minutes per 10M ledgers (16 CFs in parallel)
- **LFS writing**: ~10-15 minutes per 10M ledgers
- **RecSplit building**: ~8-12 minutes per 10M ledgers (16 indexes in parallel)

Total time per 10M ledgers: **2-3 hours** (including ingestion + transition)

## Troubleshooting

### Build Errors

**Error**: `undefined: grocksdb`

**Solution**: Ensure RocksDB is built and CGO flags are set:
```bash
export CGO_ENABLED=1
export CGO_CFLAGS="-I$HOME/workspace/rocksdb-dev/include"
export CGO_LDFLAGS="-L$HOME/workspace/rocksdb-dev -lrocksdb ..."
```

### Runtime Errors

**Error**: `Failed to create backend: context deadline exceeded`

**Solution**: Check GCS bucket path and network connectivity. Increase `buffer_size` if timeouts persist.

**Error**: `Failed to open LCM store: IO error`

**Solution**: Check disk space. RocksDB requires ~300-500 GB per 10M ledgers during ingestion.

**Error**: `ledger compaction failed: Corruption`

**Solution**: Delete `active/rocksdb/{rangeID}-ledger-store/` and restart. Ingestion will resume from last checkpoint.

### Memory Issues

If the process uses too much memory:
- Reduce `rocksdb.block_cache_mb` (default: 512 MB per store)
- Reduce `rocksdb.write_buffer_mb` (default: 128 MB per MemTable)
- Reduce `backfill.parallel_ranges` (each range = ~1-2 GB RAM for buffers)

## Testing

Run unit tests:

```bash
go test ./internal/workflow/... -v
```

**Note**: Some tests require RocksDB libraries in `LD_LIBRARY_PATH`. If tests fail with `libmdbx.dylib not found`, verify environment setup.

## Architecture

See `design-docs/` in repository root for detailed design documentation:

- `01-architecture-overview.md` - System architecture
- `02-meta-store-design.md` - Two-level phase tracking
- `06-crash-recovery.md` - Recovery scenarios
- `08-directory-structure.md` - File layout

## License

See LICENSE file in repository root.
