# TX Hash Ledger Sequence RocksDB Tools

This package provides a shared library and tools for building RocksDB stores that map
`tx_hash -> ledger_seq` for the Stellar blockchain.

## Overview

The tools in this package create a RocksDB store with 16 column families, partitioned
by the first hex character of the transaction hash. This enables:

- **Parallel processing**: Each CF can be processed independently for recsplit building
- **Efficient lookups**: Bloom filters and partitioning reduce lookup time
- **Balanced distribution**: ~6.25% of entries in each column family

## Directory Structure

```
tx-hash-ledger-sequence-rework/
├── config.go                           # Shared configuration (TOML parsing, validation)
├── logger.go                           # Logger with file-only logging support
├── stats.go                            # Thread-safe statistics tracking
├── store.go                            # OutputStore (16 CFs) + InputStore for merging
├── custom-lfs-input-rocksdb-output/    # Tool: LFS store -> RocksDB
│   ├── main.go
│   ├── processor.go
│   └── README.md
└── rocksdb-input-rocksdb-output/       # Tool: Merge RocksDB stores
    ├── main.go
    ├── processor.go
    └── README.md
```

## Shared Library Components

### config.go

Configuration management with TOML support:

```go
// Load configuration from file
config, err := txhashrework.LoadConfig("config.toml")

// Or use defaults
config := txhashrework.DefaultConfig()

// Validate for specific input type
err := config.ValidateForLfsInput()      // LFS input
err := config.ValidateForRocksDBInput()  // RocksDB merge
```

Key configuration sections:
- `OutputPath`: Where to create the output RocksDB store
- `BatchSize`: Entries per write batch (default: 100,000)
- `LFS`: LFS-specific settings (path, ledger range, workers)
- `RocksDB`: RocksDB tuning (memtables, block cache, bloom filters)

### logger.go

Logger with separate info and error streams:

```go
logger, err := txhashrework.NewLogger(txhashrework.LoggerConfig{
    LogFilePath:   "/var/log/processor.log",  // Logs ONLY to file
    ErrorFilePath: "/var/log/processor.err",  // Errors ONLY to file
})
defer logger.Close()

logger.Info("Processing ledger %d", ledgerSeq)
logger.Error("Failed to process: %v", err)
```

**Important**: When a log file is specified, logs go ONLY to that file (not to stdout).

### stats.go

Thread-safe statistics tracking:

```go
stats := txhashrework.NewStats()

// Atomically update stats from multiple goroutines
stats.AddLedgersAndTransactions(1, txCount)
stats.AddWrittenEntries(batchSize)
stats.IncrementErrors()

// Get snapshot for progress reporting
snapshot := stats.GetSnapshot()

// Print final summary
stats.LogSummary(logger, totalTime)
```

### store.go

RocksDB store management:

```go
// Create output store with 16 column families
store, err := txhashrework.OpenOutputStore(config, logger)
defer store.Close()

// Write batch of entries
entriesByCF := map[string][]txhashrework.Entry{
    "0": {{Key: hash1, Value: ledgerSeq1}, ...},
    "1": {{Key: hash2, Value: ledgerSeq2}, ...},
    ...
}
err := store.WriteBatch(entriesByCF)

// Flush to disk
err := store.FlushAll()

// Manual compaction
totalTime := store.CompactAll()
```

## Column Family Structure

| CF Name | Hash Prefix Range | Description |
|---------|-------------------|-------------|
| 0 | 0x00 - 0x0F | Hashes starting with 0 |
| 1 | 0x10 - 0x1F | Hashes starting with 1 |
| 2 | 0x20 - 0x2F | Hashes starting with 2 |
| ... | ... | ... |
| f | 0xF0 - 0xFF | Hashes starting with f |

## RocksDB Settings

Default configuration optimized for bulk writes:

| Setting | Default | Rationale |
|---------|---------|-----------|
| Write Buffer | 256 MB | Large memtables reduce flush frequency |
| Max Write Buffers | 4 | Allow background flushes while writing |
| Block Cache | 8 GB | Shared cache for read performance |
| Bloom Filter | 12 bits/key | ~0.3% false positive rate |
| WAL | Enabled | Crash recovery across reboots |
| Auto Compaction | Disabled | Manual compaction after ingestion |
| Compression | None | Values are 4 bytes, not worth compressing |

## Memory Budget

Default memory usage (~24 GB):

```
MemTables:    256 MB × 4 buffers × 16 CFs = 16,384 MB
Block Cache:  8,192 MB (shared)
────────────────────────────────────────────────────
Total:        ~24,576 MB (~24 GB)
```

## Tools

### 1. LFS to RocksDB (`custom-lfs-input-rocksdb-output/`)

Reads ledgers from an LFS store and extracts transaction hashes.

```bash
tx-hash-lfs-to-rocksdb \
  --lfs-store /data/stellar/lfs \
  --output /data/stellar/tx-hash-store \
  --workers 16
```

See [custom-lfs-input-rocksdb-output/README.md](custom-lfs-input-rocksdb-output/README.md) for details.

### 2. Merge Stores (`rocksdb-input-rocksdb-output/`)

Merges multiple existing RocksDB stores into one.

```bash
tx-hash-merge-stores \
  --input /data/store1 \
  --input /data/store2 \
  --output /data/merged-store
```

See [rocksdb-input-rocksdb-output/README.md](rocksdb-input-rocksdb-output/README.md) for details.

## Related Tools

### compact-store

Manual compaction utility for optimizing stores after bulk writes:

```bash
compact-store --store /data/stellar/tx-hash-store
```

See [../../compact-store/README.md](../compact-store/README.md) for details.

### tx-hash-store-benchmark

Benchmark tool for measuring read performance:

```bash
tx-hash-store-benchmark \
  --store /data/stellar/tx-hash-store \
  --hashes /data/sample-hashes.txt \
  --count 10000
```

## Workflow

1. **Ingest**: Use one of the input tools to build the store
2. **Compact**: Run `compact-store` to optimize for reads
3. **Query**: Use the store directly or build a recsplit index

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│   LFS Store or   │     │   tx-hash-xxx    │     │  RocksDB Store   │
│   Existing DB    │ ──▶ │   processor      │ ──▶ │  (16 CFs)        │
└──────────────────┘     └──────────────────┘     └────────┬─────────┘
                                                           │
                                                           ▼
                         ┌──────────────────┐     ┌──────────────────┐
                         │  compact-store   │ ──▶ │  Compacted Store │
                         └──────────────────┘     └────────┬─────────┘
                                                           │
                                                           ▼
                         ┌──────────────────┐     ┌──────────────────┐
                         │  recsplit build  │ ──▶ │ Optimized Index  │
                         └──────────────────┘     └──────────────────┘
```

## Building

```bash
# Build all tools
go build ./rocksdb/tx-hash-ledger-sequence-rework/...

# Build specific tool
go build ./rocksdb/tx-hash-ledger-sequence-rework/custom-lfs-input-rocksdb-output/
go build ./rocksdb/tx-hash-ledger-sequence-rework/rocksdb-input-rocksdb-output/
```

## Dependencies

- `github.com/linxGnu/grocksdb`: RocksDB Go bindings
- `github.com/pelletier/go-toml/v2`: TOML configuration parsing
- `github.com/karthikiyer56/stellar-full-history-ingestion/helpers`: Shared utilities
- `github.com/karthikiyer56/stellar-full-history-ingestion/helpers/lfs`: LFS utilities
