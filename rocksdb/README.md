# RocksDB

Primary storage backend for Stellar full history ingestion using RocksDB.

## Overview

This module provides comprehensive tools for storing, querying, and managing Stellar ledger and transaction data using RocksDB as the underlying key-value store.

## Sub-directories

| Directory | Description |
|-----------|-------------|
| `ingestion/` | Full RocksDB ingestion from GCS with multiple database outputs |
| `ingestion-v2/` | Batch-based ingestion with TOML configuration and flexible store selection |
| `ingestion-v3/` | **Reformats existing RocksDB** with different configuration (not GCS ingestion) |
| `query/` | Transaction lookup tools (one-step and two-step strategies) |
| `compaction/` | Database compaction utility |
| `merge_db/` | Merges multiple RocksDB instances into one |
| `monitor_db/` | Database health monitoring and optimization recommendations |

## Data Storage Format

- **Key**: Transaction hashes (hex encoded) or ledger sequence numbers
- **Value**: Zstd-compressed protobuf TxData containing:
  - Ledger sequence
  - Timestamp
  - Transaction envelope
  - Transaction result
  - Transaction metadata

## Database Types

| Database | Key | Value | Purpose |
|----------|-----|-------|---------|
| DB1 | ledgerSeq | compressed LCM | Ledger lookup |
| DB2 | txHash | compressed TxData | Direct transaction lookup |
| DB3 | txHash | ledgerSeq | Hash to ledger mapping |
