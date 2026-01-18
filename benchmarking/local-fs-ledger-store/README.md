# Local FS Ledger Store Benchmark

Benchmarks ledger retrieval performance from chunk-based file storage.

## What It Does

`lfs_getLedger_benchmark.go` reads ledgers from a list of ledger sequences and measures timing for index lookup, data read, decompression (zstd), and unmarshal. Reports statistics including p50/p95/p99 percentiles.

## Building

```bash
go build -o lfs-ledger-benchmark lfs_getLedger_benchmark.go
```

## Usage

```bash
./lfs-ledger-benchmark --data-dir /data/ledgers --input-file ledgers.txt
```

Run with `--help` for all flags. Use `generate_random_ledgers.sh` to create test input files.
