# RocksDB Ingestion V2

Batch-based ingestion from GCS with TOML configuration file support and selective store enabling.

## What It Does

Fetches ledger data from GCS and writes to RocksDB. Uses a TOML config file for database paths and settings. You can selectively enable which of the three stores to populate (ledger_seq_to_lcm, tx_hash_to_tx_data, tx_hash_to_ledger_seq).

## Building

```bash
go build -o rocksdb-ingestion-v2 stellar_rocksdb_ingestion.go
```

## Usage

```bash
./rocksdb-ingestion-v2 --config config.toml --start-ledger 1 --end-ledger 60000000 --ledger-batch-size 5000 --enable-tx-hash-to-tx-data
```

Run with `--help` for all available flags.
