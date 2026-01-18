# stellar-full-history-ingestion

Tools for ingesting and querying Stellar blockchain full history data using various storage backends.

## Architecture

This project provides multiple approaches for storing and querying Stellar's LedgerCloseMeta (LCM) data:

### Storage Backends

**RocksDB** (`rocksdb/`) - Primary storage backend using Facebook's embedded key-value store. Supports three database types:
- **DB1**: ledgerSeq → zstd-compressed LedgerCloseMeta
- **DB2**: txHash → zstd-compressed TxData (protobuf)
- **DB3**: txHash → ledgerSeq (for two-step lookups)

**Local FS** (`local-fs/`) - Alternative file-based storage using chunked files (10,000 ledgers per chunk) with zstd compression and index files for random access. Simpler than RocksDB but with different performance characteristics.

**RecSplit** (`recsplit/`) - Minimal Perfect Hash Function (MPHF) index for O(1) txHash → ledgerSeq lookups with ~2 bits/key overhead. Used in combination with RocksDB DB1 for memory-efficient transaction lookups.

### Query Strategies

1. **One-step lookup**: txHash → TxData directly from DB2
2. **Two-step lookup**: txHash → ledgerSeq (via RecSplit or DB3) → LCM (from DB1)

The two-step approach with RecSplit is more memory-efficient for large datasets but requires decompressing the full LCM to extract transaction data.

## Modules

| Directory | Description |
|-----------|-------------|
| `rocksdb/` | RocksDB ingestion (`ingestion/`, `ingestion-v2/`, `ingestion-v3/`) and query tools |
| `recsplit/` | MPHF index builder and query tools |
| `local-fs/` | Chunk-based file storage ingestion and query |
| `benchmarking/` | Performance testing for local-fs and recsplit+rocksdb lookups |
| `helpers/` | Shared utility code |
| `protos/` | Protobuf definitions (TxData message) |

## Prerequisites

### RocksDB Build (Linux)

Install dependencies:
```bash
sudo apt install -y libbz2-dev libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev
```

Clone and build RocksDB:
```bash
mkdir -p ~/workspace/
git clone https://github.com/facebook/rocksdb.git ~/workspace/rocksdb-dev
cd ~/workspace/rocksdb-dev
git checkout v10.4.2

LIB_MODE=shared DEBUG_LEVEL=0 EXTRA_CXXFLAGS="-Wno-error=unused-parameter" make shared_lib -j32
```

### Environment Variables

Add to `.zshrc` or `.bashrc`:
```bash
# RocksDB / CGO flags
export CGO_ENABLED=1
export CGO_CFLAGS="-I$HOME/workspace/rocksdb-dev/include"
export CGO_LDFLAGS="-lstdc++ -ldl -lz -lbz2 -lsnappy -llz4 -lzstd \
                    -lm -pthread \
                    -L$HOME/workspace/rocksdb-dev -lrocksdb"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$HOME/workspace/rocksdb-dev"
```

## Building

Each module has its own `go build` command. See the README in each directory for details.
