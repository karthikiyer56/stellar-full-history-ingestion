# Draft: Remove tx_hash_to_tx_data Store

## Requirements (confirmed)
- Remove ALL code related to `tx_hash_to_tx_data` store from `rocksdb/ingestion-v2/`
- Keep the `tx_data/` package (proto bindings) untouched
- Keep `tx_hash_to_ledger_seq` store fully functional
- Keep `ledger_seq_to_lcm` store fully functional
- Code must compile and work after removal

## Technical Decisions
- This is a DELETION task, not a refactor - remove code entirely, don't create abstractions
- After removal, the tool will support TWO stores instead of THREE
- The `tx_data/` package import in processing.go will be REMOVED (no longer needed)
- `ProcessTransactions` flag now only depends on `EnableTxHashToLedgerSeq`

## Scope Boundaries
- INCLUDE: All 7 files in `rocksdb/ingestion-v2/`
- EXCLUDE: `tx_data/` package (explicitly requested to keep)
- EXCLUDE: Any other directories

## Files to Modify (confirmed from analysis)

### 1. config.go
- Remove `TxHashToTxData TxHashToTxDataConfig` field (line 41)
- Remove `TxHashToTxDataConfig` struct definition (lines 271-297)
- Remove default settings for tx_hash_to_tx_data (lines 437-450)
- Remove `EnableTxHashToTxData` from RuntimeConfig (line 363)
- Remove `TxHashToTxData` from IngestionConfig (line 396)
- Remove `EnableTxHashToTxData` flag handling (lines 494, 500)
- Update `ProcessTransactions` logic (line 503) - only check TxHashToLedgerSeq
- Remove validation for tx_hash_to_tx_data (lines 549-554)
- Remove `getTxHashToTxDataSummary()` function (lines 714-738)
- Remove call to `getTxHashToTxDataSummary()` (lines 675-677)

### 2. config.toml
- Remove entire `[tx_hash_to_tx_data]` section (lines 193-286)
- Update header comment to mention "two stores" (lines 6-7)
- Update example configurations (lines 397-459)

### 3. stores.go
- Remove `TxDataDB` and `TxDataOpts` fields from Stores struct (lines 52-54)
- Remove opening tx_hash_to_tx_data store (lines 112-135)
- Remove closing tx_hash_to_tx_data store (lines 220-225)
- Remove `TxDataWriteTime` from WriteBatchTiming (lines 398-399)
- Remove write batch goroutine for tx_hash_to_tx_data (lines 464-498)
- Remove `TxDataCompactTime` from CompactTiming (lines 582-583)
- Remove compaction for tx_hash_to_tx_data (lines 611-621)
- Remove `txDataSize` from GetDBSizes (lines 650-652)

### 4. processing.go
- Remove `TxHashToTxData` map from TxCompressionResult (line 371)
- Remove `UncompressedSize`/`CompressedSize` from TxCompressionResult (lines 372-373) if only for TxData
- Update `CompressTransactionsInParallel` to remove `enableTxData` param (line 388)
- Remove TxData-specific compression logic (lines 419-493)
- Remove `TxDataMap` from BatchProcessingResult (line 559)
- Remove `TxDataUncompressed`/`TxDataCompressed`/`TxDataCount` from BatchProcessingResult
- Update `ProcessBatch` to not pass enableTxData (lines 610-626)
- Remove `tx_data` package import (line 30)

### 5. stats.go
- Remove `TxDataStats CompressionStats` from GlobalStats (lines 46-47)
- Remove `TxDataCompressionTime` from TimingStats (lines 80-81)
- Remove `TxDataWriteTime` from TimingStats (line 85)
- Remove `TxDataCompactTime` from TimingStats (lines 89-90)
- Remove `TxDataStats` from BatchStats (lines 123-125)
- Remove `TxDataCompressionTime`, `TxDataWriteTime` from BatchStats (lines 129-133)
- Remove adding TxDataStats in `AddBatchStats` (lines 166-169, 176-178)
- Remove all logging references to tx_hash_to_tx_data throughout

### 6. main.go
- Remove `enableTxHashToTxData` variable (line 53)
- Remove flag definition for `--enable-tx-hash-to-tx-data` (line 64)
- Update validation (line 81-83) - check only 2 stores
- Remove `EnableTxHashToTxData` from runtimeCfg (line 103)
- Remove `stores.TxDataDB` from function calls (lines 265, 374, 475)

### 7. README.md
- Update "three stores" to "two stores" (line 7)
- Remove `--enable-tx-hash-to-tx-data` from example command (line 18)

## Open Questions
- None - scope is clear

## Research Findings
- The `tx_data` package import is ONLY used for tx_hash_to_tx_data compression
- Once TxData compression is removed, no other code uses `tx_data` package
- The proto bindings in `tx_data/` will remain but be unused by ingestion-v2
