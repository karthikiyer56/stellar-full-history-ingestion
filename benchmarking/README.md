# Benchmarking

Performance testing tools for measuring lookup performance across storage backends.

## Sub-directories

| Directory | Description |
|-----------|-------------|
| `local-fs-ledger-store/` | Benchmarks ledger read performance from chunk-based file storage |
| `recsplit-rocksdb/` | Benchmarks transaction hash lookups using RecSplit + RocksDB |

## Scripts

- `append_shuffle_sortR.sh` - Appends random hex hashes to a file and shuffles the dataset
