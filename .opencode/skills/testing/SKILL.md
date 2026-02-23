---
name: testing
description: Testing rules for stellar-full-history-ingestion — NEVER run go test directly, always use Makefile targets with proper CGO environment
---

# Testing

## ⚠️ CRITICAL: NEVER Run `go test` Directly

This project has **CGO dependencies** (RocksDB) that require specific environment variables. Running `go test` directly will fail with linker errors.

❌ **WRONG:**
```bash
go test ./ingestion-workflow/...  # FAILS — missing CGO flags
```

✅ **CORRECT — Use Makefile targets:**
```bash
make test-workflow    # ingestion-workflow tests with RocksDB
make test-stores      # RocksDB store tests only
make test-mdbx        # MDBX linkage tests
```

## Makefile Test Targets

| Target | What It Tests |
|--------|---------------|
| `make test-workflow` | Full ingestion-workflow test suite with RocksDB |
| `make test-stores` | RocksDB store unit tests only |
| `make test-mdbx` | MDBX linkage verification |

## If You Must Run `go test` Directly

First run `make check-rocksdb-env` to verify your environment. Then use the full CGO flags:

### macOS (Homebrew)
```bash
CGO_ENABLED=1 \
CGO_CFLAGS="-I$(brew --prefix rocksdb)/include" \
CGO_LDFLAGS="-L$(brew --prefix rocksdb)/lib -L$(brew --prefix snappy)/lib -L$(brew --prefix lz4)/lib -L$(brew --prefix zstd)/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd" \
DYLD_LIBRARY_PATH=$(brew --prefix rocksdb)/lib \
go test -v ./ingestion-workflow/...
```

### Linux
```bash
CGO_ENABLED=1 \
CGO_CFLAGS="-I${ROCKSDB_HOME:-/usr/local}/include" \
CGO_LDFLAGS="-L${ROCKSDB_HOME:-/usr/local}/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd" \
LD_LIBRARY_PATH=${ROCKSDB_HOME:-/usr/local}/lib \
go test -v ./ingestion-workflow/...
```

## Test File Conventions

| File | Location | Purpose |
|------|----------|---------|
| `*_test.go` | alongside implementation | unit and integration tests |
| `testutil/fixtures.go` | `testutil/` subdirectory | test data and fixtures |
| `testutil/mock_backend.go` | `testutil/` subdirectory | mock Stellar backend |

## Test Categories

| Category | CGO Required | Description |
|----------|-------------|-------------|
| Pure unit tests | No | No RocksDB, pure logic (can use `go test`) |
| Store tests | **Yes** | RocksDB read/write (must use Makefile) |
| Orchestrator tests | No | Uses mocks from `testutil/` |
