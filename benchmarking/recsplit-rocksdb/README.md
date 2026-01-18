# RecSplit + RocksDB Query Benchmark

Benchmarks transaction hash lookups using the two-stage architecture.

## What It Does

`recsplit_rocksdb_query_benchmark.go` measures performance of:
1. **RecSplit lookup**: txHash → ledgerSeq (O(1) via MPHF)
2. **RocksDB fetch**: ledgerSeq → compressed LedgerCloseMeta
3. **Validation**: Decompress and scan to confirm transaction exists

Tracks normal lookups, false positives, and compound false positives. Uses parallel workers with shared RocksDB block cache.

## Building

```bash
go build -o recsplit-rocksdb-benchmark recsplit_rocksdb_query_benchmark.go
```

## Usage

```bash
./recsplit-rocksdb-benchmark \
    --input-file txhashes.txt \
    --recsplit-index-files 2014.idx,2015.idx,2016.idx \
    --ledger-seq-to-lcm-dbs 2014-rocksdb,2015-rocksdb \
    --error-file errors.log
```

Run with `--help` for all flags. Input file should contain one hex transaction hash per line.
