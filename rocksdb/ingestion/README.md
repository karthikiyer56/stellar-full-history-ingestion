# RocksDB Ingestion

Ingests Stellar ledger data from GCS and writes to up to three RocksDB databases.

## What It Does

- **DB1**: ledgerSeq → compressed LedgerCloseMeta
- **DB2**: txHash → compressed TxData
- **DB3**: txHash → ledgerSeq

Supports bulk load optimizations (disabled WAL, deferred compaction) and configurable RocksDB tuning parameters.

## Building

```bash
go build -o rocksdb-ingestion full_rocksdb_ingestion.go
```

## Usage

```bash
./rocksdb-ingestion --db1 /data/db1 --db2 /data/db2 --db3 /data/db3 --ledger-batch-size 10000
```

Run with `--help` for all flags including RocksDB tuning options.
