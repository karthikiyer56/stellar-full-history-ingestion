# stellar-full-history-ingestion

Tools for ingesting and querying Stellar blockchain full history data using various storage backends.

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

## Building ingestion-workflow

The ingestion-workflow uses RocksDB for storage. Use the Makefile targets to build and test:

### Prerequisites

**macOS (Homebrew):**
```bash
brew install rocksdb snappy lz4 zstd
```

**Linux (Debian/Ubuntu):**
```bash
sudo apt install -y librocksdb-dev libsnappy-dev liblz4-dev libzstd-dev
```

**Build from source** (if needed): See [RocksDB installation guide](https://github.com/facebook/rocksdb/blob/main/INSTALL.md)

### Makefile Targets

```bash
# Verify dependencies are installed
make check-rocksdb-env

# Build the ingestion-workflow binary
make build-workflow

# Run all workflow tests
make test-workflow

# Run store tests only
make test-stores
```

**Note**: The `build-workflow` target creates `bin/ingestion-workflow`. Delete the binary after verification per project convention.

## Building Other Modules

Each module has its own `go build` command. See the README in each directory for details.
