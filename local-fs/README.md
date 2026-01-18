# Local FS

File-based ledger storage as an alternative to RocksDB.

## What It Does

Chunk-based storage that ingests Stellar ledgers from GCS, stores them with zstd compression, and supports fast random access via index files. Each chunk holds 10,000 ledgers.

## Sub-directories

| Directory | Description |
|-----------|-------------|
| `ingestion/` | Ingests ledgers from GCS, stores in chunks. Also supports querying stored ledgers. |
