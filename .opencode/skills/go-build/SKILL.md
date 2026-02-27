---
name: go-build
description: CGO build setup, Makefile targets, and build rules for the stellar-full-history-ingestion Go project (RocksDB required)
---

# Go Build Reference

## Project Info

| | |
|--|--|
| Module | `github.com/karthikiyer56/stellar-full-history-ingestion` |
| Go version | 1.24.3 |
| CGO | **Required** (RocksDB bindings) |

## NEVER Rules

| Rule | Reason |
|------|--------|
| Import `github.com/stellar/go` | **ARCHIVED** — use `github.com/stellar/go-stellar-sdk` instead |
| Leave binaries in repo | Delete after `go build` verification |

## CGO Dependencies

### macOS (Homebrew)
```bash
brew install rocksdb snappy lz4 zstd
```

### Linux (Debian/Ubuntu)
```bash
sudo apt install -y librocksdb-dev libsnappy-dev liblz4-dev libzstd-dev zlib1g-dev libbz2-dev
```

For custom RocksDB builds, set `ROCKSDB_HOME` to point to the build directory.

## Makefile Targets

| Target | Purpose |
|--------|---------|
| `make check-rocksdb-env` | Verify RocksDB installation and CGO flags — run FIRST |
| `make build-workflow` | Build `bin/ingestion-workflow` |
| `make test-workflow` | Run workflow tests with CGO |
| `make test-stores` | Run RocksDB store tests only |
| `make build-mdbx` | Build MDBX-backed binary |
| `make test-mdbx` | Verify MDBX linkage |
| `make generate-proto` | Regenerate protobuf files |

**Always run `make check-rocksdb-env` before building** to verify CGO environment is correct.

## Build Workflow

```bash
# 1. Verify environment
make check-rocksdb-env

# 2. Build
make build-workflow  # produces bin/ingestion-workflow

# 3. Verify binary works, then DELETE IT
./bin/ingestion-workflow --help
rm bin/ingestion-workflow
```

## Common CLI Flag Patterns

All binaries in this project follow these flag conventions:

```
--lfs-store       Path to LFS ledger store
--start-ledger    First ledger to process
--end-ledger      Last ledger to process
--output-dir      Base output directory
--log-file        Path to log file (INFO/DEBUG)
--error-file      Path to error file (WARN/ERROR)
--dry-run         Validate config and exit
--block-cache-mb  RocksDB block cache size in MB
```
