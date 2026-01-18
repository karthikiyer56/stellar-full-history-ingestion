# RocksDB Query

Transaction lookup tools for querying RocksDB databases.

## Sub-directories

| Directory | Description |
|-----------|-------------|
| `one_step_lookup/` | Direct lookup: txHash → TxData from DB2 |
| `two_step_lookup/` | Two-step: txHash → ledgerSeq (DB3) → LedgerCloseMeta (DB1) |
