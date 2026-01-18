# RocksDB Ingestion V3

**Note:** Despite the name, this does NOT ingest from GCS. It reformats an existing RocksDB with different configuration.

## What It Does

Three modes for working with existing RocksDB databases:
- **Backfill**: High-throughput copy with compaction at the end
- **Streaming**: Low-memory copy with continuous compaction
- **Compact-only**: Just compact an existing database

## Building

```bash
go build -o rocksdb-ingestion-v3 rocksdb_ingestion_v3.go
```

## Usage

```bash
./rocksdb-ingestion-v3 --input /data/source --output /data/dest --backfill
```

Run with `--help` for all flags.
