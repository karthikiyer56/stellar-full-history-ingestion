// stats.go
// =============================================================================
// Statistics Tracking and Logging for Stellar RocksDB Ingestion
// =============================================================================
//
// This module handles:
// - Tracking compression statistics (per-store)
// - Tracking timing statistics (per-operation)
// - Tracking RocksDB read timing (when reading from existing store)
// - Logging batch summaries
// - Logging periodic extended statistics (every 10 batches)
// - Logging final summary
//
// =============================================================================

package main

import (
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// Global Statistics Structures
// =============================================================================

// GlobalStats tracks cumulative statistics across all batches.
// These counters are updated atomically and represent totals since start.
type GlobalStats struct {
	mu sync.Mutex

	// Ledger counts
	TotalLedgersProcessed int64
	TotalLedgersSkipped   int64

	// Transaction counts
	TotalTransactionsProcessed int64

	// Per-store compression statistics
	LcmStats     CompressionStats // ledger_seq_to_lcm
	TxDataStats  CompressionStats // tx_hash_to_tx_data
	HashSeqStats HashSeqStats     // tx_hash_to_ledger_seq (no compression, fixed size)

	// Timing statistics
	Timing TimingStats

	// RocksDB read timing (when using rocksdb source)
	RocksDBReadTiming RocksDBReadTiming

	// Start time for overall progress tracking
	StartTime time.Time
}

// CompressionStats tracks compression metrics for a single store type.
type CompressionStats struct {
	UncompressedBytes int64 // Total bytes before compression
	CompressedBytes   int64 // Total bytes after compression
	EntryCount        int64 // Number of entries (ledgers or transactions)
}

// HashSeqStats tracks statistics for tx_hash_to_ledger_seq store.
// This store has fixed-size entries (36 bytes) and no compression.
type HashSeqStats struct {
	EntryCount    int64 // Number of txHash→ledgerSeq mappings
	ExpectedBytes int64 // EntryCount × 36 bytes (theoretical minimum)
	RawFileBytes  int64 // Bytes written to raw data file (if enabled)
}

// TimingStats tracks cumulative timing for various operations.
type TimingStats struct {
	// Source data fetching
	GetLedgerTime time.Duration

	// Compression time per store
	LcmCompressionTime    time.Duration
	TxDataCompressionTime time.Duration

	// RocksDB write time per store
	LcmWriteTime     time.Duration
	TxDataWriteTime  time.Duration
	HashSeqWriteTime time.Duration

	// Final compaction time per store
	LcmCompactTime     time.Duration
	TxDataCompactTime  time.Duration
	HashSeqCompactTime time.Duration

	// Raw file write time
	RawFileWriteTime time.Duration
}

// RocksDBReadTiming tracks timing for reading from RocksDB source.
// All times are cumulative across all workers.
type RocksDBReadTiming struct {
	ReadTime       time.Duration // Time spent in RocksDB Get()
	DecompressTime time.Duration // Time spent decompressing data
	UnmarshalTime  time.Duration // Time spent unmarshalling XDR
	TotalTime      time.Duration // Total time including overhead
}

// =============================================================================
// Batch Statistics Structures
// =============================================================================

// BatchStats tracks statistics for a single batch.
// These are reset at the start of each batch.
type BatchStats struct {
	BatchNum    uint32
	StartLedger uint32
	EndLedger   uint32
	StartTime   time.Time

	// Counts for this batch
	LedgerCount      int
	TransactionCount int

	// Per-store compression stats for this batch
	LcmStats     CompressionStats
	TxDataStats  CompressionStats
	HashSeqCount int64

	// Timing for this batch
	GetLedgerTime         time.Duration
	LcmCompressionTime    time.Duration
	TxDataCompressionTime time.Duration
	LcmWriteTime          time.Duration
	TxDataWriteTime       time.Duration
	HashSeqWriteTime      time.Duration
	RawFileWriteTime      time.Duration

	// RocksDB read timing for this batch (when using rocksdb source)
	RocksDBReadTiming RocksDBReadTiming
}

// =============================================================================
// Global Stats Methods
// =============================================================================

// NewGlobalStats creates a new GlobalStats instance.
func NewGlobalStats() *GlobalStats {
	return &GlobalStats{
		StartTime: time.Now(),
	}
}

// AddBatchStats adds batch statistics to global totals.
// This is called at the end of each batch.
func (g *GlobalStats) AddBatchStats(batch *BatchStats) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Update counts
	g.TotalLedgersProcessed += int64(batch.LedgerCount)
	g.TotalTransactionsProcessed += int64(batch.TransactionCount)

	// Update compression stats
	g.LcmStats.UncompressedBytes += batch.LcmStats.UncompressedBytes
	g.LcmStats.CompressedBytes += batch.LcmStats.CompressedBytes
	g.LcmStats.EntryCount += batch.LcmStats.EntryCount

	g.TxDataStats.UncompressedBytes += batch.TxDataStats.UncompressedBytes
	g.TxDataStats.CompressedBytes += batch.TxDataStats.CompressedBytes
	g.TxDataStats.EntryCount += batch.TxDataStats.EntryCount

	g.HashSeqStats.EntryCount += batch.HashSeqCount
	g.HashSeqStats.ExpectedBytes = g.HashSeqStats.EntryCount * 36

	// Update timing
	g.Timing.GetLedgerTime += batch.GetLedgerTime
	g.Timing.LcmCompressionTime += batch.LcmCompressionTime
	g.Timing.TxDataCompressionTime += batch.TxDataCompressionTime
	g.Timing.LcmWriteTime += batch.LcmWriteTime
	g.Timing.TxDataWriteTime += batch.TxDataWriteTime
	g.Timing.HashSeqWriteTime += batch.HashSeqWriteTime
	g.Timing.RawFileWriteTime += batch.RawFileWriteTime

	// Update RocksDB read timing
	g.RocksDBReadTiming.ReadTime += batch.RocksDBReadTiming.ReadTime
	g.RocksDBReadTiming.DecompressTime += batch.RocksDBReadTiming.DecompressTime
	g.RocksDBReadTiming.UnmarshalTime += batch.RocksDBReadTiming.UnmarshalTime
	g.RocksDBReadTiming.TotalTime += batch.RocksDBReadTiming.TotalTime
}

// AddSkippedLedgers increments the skipped ledger counter.
func (g *GlobalStats) AddSkippedLedgers(count int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.TotalLedgersSkipped += int64(count)
}

// =============================================================================
// Logging Functions
// =============================================================================

// LogBatchSummary logs a detailed summary for a single batch.
func LogBatchSummary(batch *BatchStats, config *IngestionConfig, totalBatches uint32) {
	totalBatchDuration := time.Since(batch.StartTime)

	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("                         BATCH #%d of %d COMPLETE", batch.BatchNum, totalBatches)
	log.Printf("================================================================================")
	log.Printf("")

	// Ledger range and counts
	log.Printf("BATCH OVERVIEW:")
	log.Printf("  Ledger Range:        %d - %d", batch.StartLedger, batch.EndLedger)
	log.Printf("  Ledgers Processed:   %d", batch.LedgerCount)
	log.Printf("  Transactions:        %s", helpers.FormatNumber(int64(batch.TransactionCount)))
	log.Printf("  Total Batch Time:    %s", helpers.FormatDuration(totalBatchDuration))
	log.Printf("")

	// Output paths (always show for each batch)
	log.Printf("OUTPUT PATHS:")
	if config.EnableLedgerSeqToLcm {
		log.Printf("  ledger_seq_to_lcm:   %s", config.LedgerSeqToLcm.OutputPath)
	}
	if config.EnableTxHashToTxData {
		log.Printf("  tx_hash_to_tx_data:  %s", config.TxHashToTxData.OutputPath)
	}
	if config.EnableTxHashToLedgerSeq {
		log.Printf("  tx_hash_to_ledger_seq: %s", config.TxHashToLedgerSeq.OutputPath)
		if config.TxHashToLedgerSeq.RawDataFilePath != "" {
			log.Printf("  raw_data_file:       %s", config.TxHashToLedgerSeq.RawDataFilePath)
		}
	}
	if config.UseRocksDBSource {
		log.Printf("  rocksdb_lcm_source:  %s", config.RocksDBLcmStorePath)
	}
	log.Printf("")

	// Timing breakdown
	log.Printf("TIMING BREAKDOWN:")
	log.Printf("")

	// Ledger fetching
	log.Printf("  Ledger Fetching:")
	log.Printf("    Total Time:        %s", helpers.FormatDuration(batch.GetLedgerTime))
	if batch.LedgerCount > 0 {
		avgPerLedger := batch.GetLedgerTime / time.Duration(batch.LedgerCount)
		log.Printf("    Avg per Ledger:    %s", helpers.FormatDuration(avgPerLedger))
	}

	// RocksDB read breakdown (if applicable)
	if batch.RocksDBReadTiming.TotalTime > 0 {
		log.Printf("")
		log.Printf("    RocksDB Read Breakdown:")
		log.Printf("      Read:            %s", helpers.FormatDuration(batch.RocksDBReadTiming.ReadTime))
		log.Printf("      Decompress:      %s", helpers.FormatDuration(batch.RocksDBReadTiming.DecompressTime))
		log.Printf("      Unmarshal:       %s", helpers.FormatDuration(batch.RocksDBReadTiming.UnmarshalTime))
	}
	log.Printf("")

	// Per-store stats
	if config.EnableLedgerSeqToLcm {
		log.Printf("  ledger_seq_to_lcm:")
		log.Printf("    Compression:       %s", helpers.FormatDuration(batch.LcmCompressionTime))
		log.Printf("    RocksDB Write:     %s", helpers.FormatDuration(batch.LcmWriteTime))
		if batch.LcmStats.UncompressedBytes > 0 {
			ratio := 100.0 * (1.0 - float64(batch.LcmStats.CompressedBytes)/float64(batch.LcmStats.UncompressedBytes))
			log.Printf("    Data: %s → %s (%.1f%% reduction)",
				helpers.FormatBytes(batch.LcmStats.UncompressedBytes),
				helpers.FormatBytes(batch.LcmStats.CompressedBytes),
				ratio)
		}
		log.Printf("")
	}

	if config.EnableTxHashToTxData {
		log.Printf("  tx_hash_to_tx_data:")
		log.Printf("    Compression:       %s", helpers.FormatDuration(batch.TxDataCompressionTime))
		log.Printf("    RocksDB Write:     %s", helpers.FormatDuration(batch.TxDataWriteTime))
		if batch.TransactionCount > 0 {
			avgCompress := batch.TxDataCompressionTime / time.Duration(batch.TransactionCount)
			avgWrite := batch.TxDataWriteTime / time.Duration(batch.TransactionCount)
			log.Printf("    Avg per TX:        compress=%s, write=%s",
				helpers.FormatDuration(avgCompress), helpers.FormatDuration(avgWrite))
		}
		if batch.TxDataStats.UncompressedBytes > 0 {
			ratio := 100.0 * (1.0 - float64(batch.TxDataStats.CompressedBytes)/float64(batch.TxDataStats.UncompressedBytes))
			log.Printf("    Data: %s → %s (%.1f%% reduction)",
				helpers.FormatBytes(batch.TxDataStats.UncompressedBytes),
				helpers.FormatBytes(batch.TxDataStats.CompressedBytes),
				ratio)
		}
		log.Printf("")
	}

	if config.EnableTxHashToLedgerSeq {
		log.Printf("  tx_hash_to_ledger_seq:")
		log.Printf("    RocksDB Write:     %s", helpers.FormatDuration(batch.HashSeqWriteTime))
		if batch.HashSeqCount > 0 {
			expectedBytes := batch.HashSeqCount * 36
			log.Printf("    Entries: %s (%s expected)",
				helpers.FormatNumber(batch.HashSeqCount),
				helpers.FormatBytes(expectedBytes))
		}
		if config.TxHashToLedgerSeq.RawDataFilePath != "" {
			log.Printf("    Raw File Write:    %s", helpers.FormatDuration(batch.RawFileWriteTime))
		}
		log.Printf("")
	}

	// Batch averages
	log.Printf("BATCH AVERAGES:")
	if batch.LedgerCount > 0 {
		avgTimePerLedger := totalBatchDuration / time.Duration(batch.LedgerCount)
		log.Printf("  Time per Ledger:     %s", helpers.FormatDuration(avgTimePerLedger))
	}
	if batch.TransactionCount > 0 {
		avgTimePerTx := totalBatchDuration / time.Duration(batch.TransactionCount)
		log.Printf("  Time per TX:         %s", helpers.FormatDuration(avgTimePerTx))
	}

	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("")
}

// LogExtendedStats logs detailed statistics every 10 batches.
// This includes RocksDB overhead analysis and cumulative statistics.
func LogExtendedStats(
	global *GlobalStats,
	config *IngestionConfig,
	currentLedger uint32,
	lcmDB, txDataDB, hashSeqDB *grocksdb.DB,
	numWorkers int,
) {
	global.mu.Lock()
	defer global.mu.Unlock()

	elapsed := time.Since(global.StartTime)

	log.Printf("")
	log.Printf("################################################################################")
	log.Printf("                    EXTENDED STATISTICS (Every 10 Batches)")
	log.Printf("################################################################################")
	log.Printf("")

	log.Printf("CUMULATIVE PROGRESS:")
	log.Printf("  Current Ledger:      %d", currentLedger)
	log.Printf("  Ledgers Processed:   %s", helpers.FormatNumber(global.TotalLedgersProcessed))
	if global.TotalLedgersSkipped > 0 {
		log.Printf("  Ledgers Skipped:     %s", helpers.FormatNumber(global.TotalLedgersSkipped))
	}
	log.Printf("  Transactions:        %s", helpers.FormatNumber(global.TotalTransactionsProcessed))
	log.Printf("  Elapsed Time:        %s", helpers.FormatDuration(elapsed))
	if elapsed.Seconds() > 0 {
		rate := float64(global.TotalLedgersProcessed) / elapsed.Seconds()
		log.Printf("  Processing Rate:     %.2f ledgers/sec", rate)
	}
	log.Printf("")

	// Output paths reminder
	log.Printf("OUTPUT PATHS:")
	if config.EnableLedgerSeqToLcm {
		log.Printf("  ledger_seq_to_lcm:   %s", config.LedgerSeqToLcm.OutputPath)
	}
	if config.EnableTxHashToTxData {
		log.Printf("  tx_hash_to_tx_data:  %s", config.TxHashToTxData.OutputPath)
	}
	if config.EnableTxHashToLedgerSeq {
		log.Printf("  tx_hash_to_ledger_seq: %s", config.TxHashToLedgerSeq.OutputPath)
		if config.TxHashToLedgerSeq.RawDataFilePath != "" {
			log.Printf("  raw_data_file:       %s", config.TxHashToLedgerSeq.RawDataFilePath)
		}
	}
	log.Printf("")

	// Compression and storage analysis per store
	log.Printf("--------------------------------------------------------------------------------")
	log.Printf("                      COMPRESSION & STORAGE ANALYSIS")
	log.Printf("--------------------------------------------------------------------------------")
	log.Printf("")

	// ledger_seq_to_lcm store
	if config.EnableLedgerSeqToLcm && global.LcmStats.UncompressedBytes > 0 {
		diskSize := getRocksDBSSTSize(lcmDB)
		ratio := 100.0 * (1.0 - float64(global.LcmStats.CompressedBytes)/float64(global.LcmStats.UncompressedBytes))

		log.Printf("ledger_seq_to_lcm:")
		log.Printf("  Entries:             %s ledgers", helpers.FormatNumber(global.LcmStats.EntryCount))
		log.Printf("  Raw Data Size:       %s", helpers.FormatBytes(global.LcmStats.UncompressedBytes))
		log.Printf("  Compressed Size:     %s (%.1f%% reduction)",
			helpers.FormatBytes(global.LcmStats.CompressedBytes), ratio)
		log.Printf("  RocksDB Disk Size:   %s", helpers.FormatBytes(diskSize))

		if global.LcmStats.CompressedBytes > 0 && diskSize > 0 {
			overhead := float64(diskSize-global.LcmStats.CompressedBytes) / float64(global.LcmStats.CompressedBytes) * 100
			log.Printf("  RocksDB Overhead:    %.2f%% over compressed data", overhead)
		}
		if global.LcmStats.EntryCount > 0 {
			avgRaw := float64(global.LcmStats.UncompressedBytes) / float64(global.LcmStats.EntryCount)
			avgCompressed := float64(global.LcmStats.CompressedBytes) / float64(global.LcmStats.EntryCount)
			log.Printf("  Avg per Ledger:      %.2f KB raw → %.2f KB compressed",
				avgRaw/1024, avgCompressed/1024)
		}
		log.Printf("")
		logRocksDBLevelStats(lcmDB, "ledger_seq_to_lcm")
		log.Printf("")
	}

	// tx_hash_to_tx_data store
	if config.EnableTxHashToTxData && global.TxDataStats.UncompressedBytes > 0 {
		diskSize := getRocksDBSSTSize(txDataDB)
		ratio := 100.0 * (1.0 - float64(global.TxDataStats.CompressedBytes)/float64(global.TxDataStats.UncompressedBytes))

		log.Printf("tx_hash_to_tx_data:")
		log.Printf("  Entries:             %s transactions", helpers.FormatNumber(global.TxDataStats.EntryCount))
		log.Printf("  Raw Data Size:       %s", helpers.FormatBytes(global.TxDataStats.UncompressedBytes))
		log.Printf("  Compressed Size:     %s (%.1f%% reduction)",
			helpers.FormatBytes(global.TxDataStats.CompressedBytes), ratio)
		log.Printf("  RocksDB Disk Size:   %s", helpers.FormatBytes(diskSize))

		if global.TxDataStats.CompressedBytes > 0 && diskSize > 0 {
			overhead := float64(diskSize-global.TxDataStats.CompressedBytes) / float64(global.TxDataStats.CompressedBytes) * 100
			log.Printf("  RocksDB Overhead:    %.2f%% over compressed data", overhead)
		}
		if global.TxDataStats.EntryCount > 0 {
			avgRaw := float64(global.TxDataStats.UncompressedBytes) / float64(global.TxDataStats.EntryCount)
			avgCompressed := float64(global.TxDataStats.CompressedBytes) / float64(global.TxDataStats.EntryCount)
			log.Printf("  Avg per TX:          %.2f bytes raw → %.2f bytes compressed",
				avgRaw, avgCompressed)
		}
		log.Printf("")
		logRocksDBLevelStats(txDataDB, "tx_hash_to_tx_data")
		log.Printf("")
	}

	// tx_hash_to_ledger_seq store
	if config.EnableTxHashToLedgerSeq && global.HashSeqStats.EntryCount > 0 {
		diskSize := getRocksDBSSTSize(hashSeqDB)
		expectedBytes := global.HashSeqStats.ExpectedBytes

		log.Printf("tx_hash_to_ledger_seq:")
		log.Printf("  Entries:             %s mappings", helpers.FormatNumber(global.HashSeqStats.EntryCount))
		log.Printf("  Expected Size:       %s (%d entries × 36 bytes)",
			helpers.FormatBytes(expectedBytes), global.HashSeqStats.EntryCount)
		log.Printf("  RocksDB Disk Size:   %s", helpers.FormatBytes(diskSize))

		if expectedBytes > 0 && diskSize > 0 {
			overhead := float64(diskSize-expectedBytes) / float64(expectedBytes) * 100
			log.Printf("  RocksDB Overhead:    %.2f%% over raw data", overhead)
		}

		if config.TxHashToLedgerSeq.RawDataFilePath != "" {
			rawFileExpected := global.HashSeqStats.EntryCount * 36
			log.Printf("  Raw File Size:       %s (expected)", helpers.FormatBytes(rawFileExpected))
		}
		log.Printf("")
		logRocksDBLevelStats(hashSeqDB, "tx_hash_to_ledger_seq")
		log.Printf("")
	}

	// Total storage summary
	log.Printf("--------------------------------------------------------------------------------")
	log.Printf("                           TOTAL STORAGE SUMMARY")
	log.Printf("--------------------------------------------------------------------------------")
	log.Printf("")

	var totalRaw, totalCompressed, totalDisk int64
	totalRaw = global.LcmStats.UncompressedBytes + global.TxDataStats.UncompressedBytes + global.HashSeqStats.ExpectedBytes
	totalCompressed = global.LcmStats.CompressedBytes + global.TxDataStats.CompressedBytes + global.HashSeqStats.ExpectedBytes

	if config.EnableLedgerSeqToLcm && lcmDB != nil {
		totalDisk += getRocksDBSSTSize(lcmDB)
	}
	if config.EnableTxHashToTxData && txDataDB != nil {
		totalDisk += getRocksDBSSTSize(txDataDB)
	}
	if config.EnableTxHashToLedgerSeq && hashSeqDB != nil {
		totalDisk += getRocksDBSSTSize(hashSeqDB)
	}

	log.Printf("  Total Raw Data:      %s", helpers.FormatBytes(totalRaw))
	log.Printf("  Total Compressed:    %s", helpers.FormatBytes(totalCompressed))
	log.Printf("  Total Disk Usage:    %s", helpers.FormatBytes(totalDisk))

	if totalCompressed > 0 {
		overallOverhead := float64(totalDisk-totalCompressed) / float64(totalCompressed) * 100
		log.Printf("  Overall Overhead:    %.2f%% over compressed", overallOverhead)
	}
	if totalRaw > 0 {
		effectiveCompression := 100.0 * (1.0 - float64(totalDisk)/float64(totalRaw))
		log.Printf("  Effective Savings:   %.2f%% (raw → disk)", effectiveCompression)
	}
	log.Printf("")

	// RocksDB read timing (if using rocksdb source)
	if config.UseRocksDBSource && global.RocksDBReadTiming.TotalTime > 0 {
		log.Printf("--------------------------------------------------------------------------------")
		log.Printf("                        ROCKSDB SOURCE READ TIMING")
		log.Printf("--------------------------------------------------------------------------------")
		log.Printf("")
		log.Printf("  (Wall-clock estimates, parallelized across %d workers)", numWorkers)
		log.Printf("")
		log.Printf("  Read:                %s", helpers.FormatDuration(global.RocksDBReadTiming.ReadTime/time.Duration(numWorkers)))
		log.Printf("  Decompress:          %s", helpers.FormatDuration(global.RocksDBReadTiming.DecompressTime/time.Duration(numWorkers)))
		log.Printf("  Unmarshal:           %s", helpers.FormatDuration(global.RocksDBReadTiming.UnmarshalTime/time.Duration(numWorkers)))
		log.Printf("")
		if global.TotalLedgersProcessed > 0 {
			avgRead := global.RocksDBReadTiming.ReadTime / time.Duration(global.TotalLedgersProcessed)
			avgDecompress := global.RocksDBReadTiming.DecompressTime / time.Duration(global.TotalLedgersProcessed)
			avgUnmarshal := global.RocksDBReadTiming.UnmarshalTime / time.Duration(global.TotalLedgersProcessed)
			log.Printf("  Avg per Ledger:      read=%s, decompress=%s, unmarshal=%s",
				helpers.FormatDuration(avgRead),
				helpers.FormatDuration(avgDecompress),
				helpers.FormatDuration(avgUnmarshal))
		}
		log.Printf("")
	}

	// Timing breakdown
	log.Printf("--------------------------------------------------------------------------------")
	log.Printf("                           CUMULATIVE TIMING")
	log.Printf("--------------------------------------------------------------------------------")
	log.Printf("")
	log.Printf("  Get Ledger:          %s (%.1f%%)",
		helpers.FormatDuration(global.Timing.GetLedgerTime),
		100*global.Timing.GetLedgerTime.Seconds()/elapsed.Seconds())

	totalCompression := global.Timing.LcmCompressionTime + global.Timing.TxDataCompressionTime
	log.Printf("  Compression:         %s (%.1f%%)",
		helpers.FormatDuration(totalCompression),
		100*totalCompression.Seconds()/elapsed.Seconds())

	totalWrite := global.Timing.LcmWriteTime + global.Timing.TxDataWriteTime + global.Timing.HashSeqWriteTime
	log.Printf("  RocksDB Write:       %s (%.1f%%)",
		helpers.FormatDuration(totalWrite),
		100*totalWrite.Seconds()/elapsed.Seconds())

	if config.TxHashToLedgerSeq.RawDataFilePath != "" {
		log.Printf("  Raw File Write:      %s (%.1f%%)",
			helpers.FormatDuration(global.Timing.RawFileWriteTime),
			100*global.Timing.RawFileWriteTime.Seconds()/elapsed.Seconds())
	}
	log.Printf("")

	log.Printf("################################################################################")
	log.Printf("")
}

// LogProgress logs progress at each percentage point.
func LogProgress(
	global *GlobalStats,
	totalLedgers int,
	currentLedger uint32,
	lastPercent *int,
) {
	global.mu.Lock()
	processedCount := global.TotalLedgersProcessed
	skippedCount := global.TotalLedgersSkipped
	txCount := global.TotalTransactionsProcessed
	startTime := global.StartTime
	global.mu.Unlock()

	currentPercent := int((processedCount * 100) / int64(totalLedgers))
	if currentPercent <= *lastPercent {
		return
	}

	elapsed := time.Since(startTime)
	ledgersPerSec := float64(processedCount) / elapsed.Seconds()
	remaining := int64(totalLedgers) - processedCount

	var eta time.Duration
	if ledgersPerSec > 0 {
		eta = time.Duration(float64(remaining)/ledgersPerSec) * time.Second
	}

	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("    PROGRESS: %d%% | Ledger %d | %s/%s processed",
		currentPercent, currentLedger,
		helpers.FormatNumber(processedCount),
		helpers.FormatNumber(int64(totalLedgers)))
	if skippedCount > 0 {
		log.Printf("    Skipped: %s ledgers", helpers.FormatNumber(skippedCount))
	}
	log.Printf("    Transactions: %s | Rate: %.2f ledgers/sec | ETA: %s",
		helpers.FormatNumber(txCount), ledgersPerSec, helpers.FormatDuration(eta))
	log.Printf("================================================================================")
	log.Printf("")

	*lastPercent = currentPercent
}

// LogFinalSummary logs the final summary at the end of ingestion.
func LogFinalSummary(
	global *GlobalStats,
	config *IngestionConfig,
	lcmDB, txDataDB, hashSeqDB *grocksdb.DB,
	numWorkers int,
) {
	global.mu.Lock()
	defer global.mu.Unlock()

	elapsed := time.Since(global.StartTime)

	log.Printf("")
	log.Printf("################################################################################")
	log.Printf("################################################################################")
	log.Printf("                           INGESTION COMPLETE")
	log.Printf("################################################################################")
	log.Printf("################################################################################")
	log.Printf("")

	// Summary counts
	log.Printf("FINAL COUNTS:")
	log.Printf("  Total Ledgers Processed:     %s", helpers.FormatNumber(global.TotalLedgersProcessed))
	if global.TotalLedgersSkipped > 0 {
		log.Printf("  Total Ledgers Skipped:       %s", helpers.FormatNumber(global.TotalLedgersSkipped))
	}
	log.Printf("  Total Transactions:          %s", helpers.FormatNumber(global.TotalTransactionsProcessed))
	log.Printf("  Total Time:                  %s", helpers.FormatDuration(elapsed))
	if elapsed.Seconds() > 0 {
		rate := float64(global.TotalLedgersProcessed) / elapsed.Seconds()
		log.Printf("  Average Rate:                %.2f ledgers/sec", rate)
	}
	log.Printf("")

	// Time range
	log.Printf("TIME RANGE:")
	log.Printf("  Start Time:                  %s", config.StartTime)
	log.Printf("  End Time:                    %s", config.EndTime)
	log.Printf("  Ledger Range:                %d - %d", config.StartLedger, config.EndLedger)
	log.Printf("")

	// Output paths
	log.Printf("OUTPUT PATHS:")
	if config.EnableLedgerSeqToLcm {
		log.Printf("  ledger_seq_to_lcm:           %s", config.LedgerSeqToLcm.OutputPath)
	}
	if config.EnableTxHashToTxData {
		log.Printf("  tx_hash_to_tx_data:          %s", config.TxHashToTxData.OutputPath)
	}
	if config.EnableTxHashToLedgerSeq {
		log.Printf("  tx_hash_to_ledger_seq:       %s", config.TxHashToLedgerSeq.OutputPath)
		if config.TxHashToLedgerSeq.RawDataFilePath != "" {
			log.Printf("  raw_data_file:               %s", config.TxHashToLedgerSeq.RawDataFilePath)
		}
	}
	log.Printf("")

	// Timing breakdown
	log.Printf("--------------------------------------------------------------------------------")
	log.Printf("                           TIMING BREAKDOWN")
	log.Printf("--------------------------------------------------------------------------------")
	log.Printf("")

	log.Printf("  Get Ledger:                  %s (%.1f%%)",
		helpers.FormatDuration(global.Timing.GetLedgerTime),
		100*global.Timing.GetLedgerTime.Seconds()/elapsed.Seconds())

	if config.UseRocksDBSource && global.RocksDBReadTiming.TotalTime > 0 {
		log.Printf("    RocksDB Read:              %s", helpers.FormatDuration(global.RocksDBReadTiming.ReadTime/time.Duration(numWorkers)))
		log.Printf("    Decompress:                %s", helpers.FormatDuration(global.RocksDBReadTiming.DecompressTime/time.Duration(numWorkers)))
		log.Printf("    Unmarshal:                 %s", helpers.FormatDuration(global.RocksDBReadTiming.UnmarshalTime/time.Duration(numWorkers)))
	}

	log.Printf("")
	log.Printf("  Compression:")
	if config.EnableLedgerSeqToLcm {
		log.Printf("    LCM:                       %s", helpers.FormatDuration(global.Timing.LcmCompressionTime))
	}
	if config.EnableTxHashToTxData {
		log.Printf("    TxData:                    %s", helpers.FormatDuration(global.Timing.TxDataCompressionTime))
	}

	log.Printf("")
	log.Printf("  RocksDB Write:")
	if config.EnableLedgerSeqToLcm {
		log.Printf("    LCM:                       %s", helpers.FormatDuration(global.Timing.LcmWriteTime))
	}
	if config.EnableTxHashToTxData {
		log.Printf("    TxData:                    %s", helpers.FormatDuration(global.Timing.TxDataWriteTime))
	}
	if config.EnableTxHashToLedgerSeq {
		log.Printf("    HashSeq:                   %s", helpers.FormatDuration(global.Timing.HashSeqWriteTime))
	}

	if config.TxHashToLedgerSeq.RawDataFilePath != "" {
		log.Printf("")
		log.Printf("  Raw File Write:              %s", helpers.FormatDuration(global.Timing.RawFileWriteTime))
	}

	log.Printf("")
	log.Printf("  Final Compaction:")
	if config.EnableLedgerSeqToLcm {
		log.Printf("    LCM:                       %s", helpers.FormatDuration(global.Timing.LcmCompactTime))
	}
	if config.EnableTxHashToTxData {
		log.Printf("    TxData:                    %s", helpers.FormatDuration(global.Timing.TxDataCompactTime))
	}
	if config.EnableTxHashToLedgerSeq {
		log.Printf("    HashSeq:                   %s", helpers.FormatDuration(global.Timing.HashSeqCompactTime))
	}
	log.Printf("")

	// Detailed compression and storage analysis
	log.Printf("--------------------------------------------------------------------------------")
	log.Printf("                    DETAILED COMPRESSION & STORAGE ANALYSIS")
	log.Printf("--------------------------------------------------------------------------------")
	log.Printf("")

	// ledger_seq_to_lcm
	if config.EnableLedgerSeqToLcm && global.LcmStats.UncompressedBytes > 0 {
		diskSize := getRocksDBSSTSize(lcmDB)
		ratio := 100.0 * (1.0 - float64(global.LcmStats.CompressedBytes)/float64(global.LcmStats.UncompressedBytes))

		log.Printf("ledger_seq_to_lcm:")
		log.Printf("  Entries:                     %s ledgers", helpers.FormatNumber(global.LcmStats.EntryCount))
		log.Printf("  Raw Data Size:               %s", helpers.FormatBytes(global.LcmStats.UncompressedBytes))
		log.Printf("  After zstd Compression:      %s (%.1f%% reduction)",
			helpers.FormatBytes(global.LcmStats.CompressedBytes), ratio)
		log.Printf("  Final Disk Usage:            %s", helpers.FormatBytes(diskSize))

		if global.LcmStats.CompressedBytes > 0 && diskSize > 0 {
			overhead := float64(diskSize-global.LcmStats.CompressedBytes) / float64(global.LcmStats.CompressedBytes) * 100
			log.Printf("  RocksDB Overhead:            %.2f%% over compressed data", overhead)
		}
		if global.LcmStats.UncompressedBytes > 0 && diskSize > 0 {
			totalSavings := 100.0 * (1.0 - float64(diskSize)/float64(global.LcmStats.UncompressedBytes))
			log.Printf("  Effective Space Savings:     %.2f%% (raw → disk)", totalSavings)
		}
		if global.LcmStats.EntryCount > 0 {
			avgRaw := float64(global.LcmStats.UncompressedBytes) / float64(global.LcmStats.EntryCount)
			avgCompressed := float64(global.LcmStats.CompressedBytes) / float64(global.LcmStats.EntryCount)
			avgDisk := float64(diskSize) / float64(global.LcmStats.EntryCount)
			log.Printf("  Avg per Ledger:              %.2f KB raw → %.2f KB compressed → %.2f KB disk",
				avgRaw/1024, avgCompressed/1024, avgDisk/1024)
		}
		log.Printf("")
		logRocksDBLevelStats(lcmDB, "ledger_seq_to_lcm")
		log.Printf("")
	}

	// tx_hash_to_tx_data
	if config.EnableTxHashToTxData && global.TxDataStats.UncompressedBytes > 0 {
		diskSize := getRocksDBSSTSize(txDataDB)
		ratio := 100.0 * (1.0 - float64(global.TxDataStats.CompressedBytes)/float64(global.TxDataStats.UncompressedBytes))

		log.Printf("tx_hash_to_tx_data:")
		log.Printf("  Entries:                     %s transactions", helpers.FormatNumber(global.TxDataStats.EntryCount))
		log.Printf("  Raw Data Size:               %s", helpers.FormatBytes(global.TxDataStats.UncompressedBytes))
		log.Printf("  After zstd Compression:      %s (%.1f%% reduction)",
			helpers.FormatBytes(global.TxDataStats.CompressedBytes), ratio)
		log.Printf("  Final Disk Usage:            %s", helpers.FormatBytes(diskSize))

		if global.TxDataStats.CompressedBytes > 0 && diskSize > 0 {
			overhead := float64(diskSize-global.TxDataStats.CompressedBytes) / float64(global.TxDataStats.CompressedBytes) * 100
			log.Printf("  RocksDB Overhead:            %.2f%% over compressed data", overhead)
		}
		if global.TxDataStats.UncompressedBytes > 0 && diskSize > 0 {
			totalSavings := 100.0 * (1.0 - float64(diskSize)/float64(global.TxDataStats.UncompressedBytes))
			log.Printf("  Effective Space Savings:     %.2f%% (raw → disk)", totalSavings)
		}
		if global.TxDataStats.EntryCount > 0 {
			avgRaw := float64(global.TxDataStats.UncompressedBytes) / float64(global.TxDataStats.EntryCount)
			avgCompressed := float64(global.TxDataStats.CompressedBytes) / float64(global.TxDataStats.EntryCount)
			avgDisk := float64(diskSize) / float64(global.TxDataStats.EntryCount)
			log.Printf("  Avg per TX:                  %.2f bytes raw → %.2f bytes compressed → %.2f bytes disk",
				avgRaw, avgCompressed, avgDisk)
		}
		log.Printf("")
		logRocksDBLevelStats(txDataDB, "tx_hash_to_tx_data")
		log.Printf("")
	}

	// tx_hash_to_ledger_seq
	if config.EnableTxHashToLedgerSeq && global.HashSeqStats.EntryCount > 0 {
		diskSize := getRocksDBSSTSize(hashSeqDB)
		expectedBytes := global.HashSeqStats.ExpectedBytes

		log.Printf("tx_hash_to_ledger_seq:")
		log.Printf("  Entries:                     %s mappings", helpers.FormatNumber(global.HashSeqStats.EntryCount))
		log.Printf("  Expected Raw Size:           %s (%d entries × 36 bytes)",
			helpers.FormatBytes(expectedBytes), global.HashSeqStats.EntryCount)
		log.Printf("  Final Disk Usage:            %s", helpers.FormatBytes(diskSize))

		if expectedBytes > 0 && diskSize > 0 {
			overhead := float64(diskSize-expectedBytes) / float64(expectedBytes) * 100
			log.Printf("  RocksDB Overhead:            %.2f%% over raw data", overhead)
		}

		if config.TxHashToLedgerSeq.RawDataFilePath != "" {
			rawFileExpected := global.HashSeqStats.EntryCount * 36
			log.Printf("  Raw File Size:               %s", helpers.FormatBytes(rawFileExpected))
		}
		log.Printf("")
		logRocksDBLevelStats(hashSeqDB, "tx_hash_to_ledger_seq")
		log.Printf("")
	}

	// Total storage summary
	log.Printf("--------------------------------------------------------------------------------")
	log.Printf("                           TOTAL STORAGE SUMMARY")
	log.Printf("--------------------------------------------------------------------------------")
	log.Printf("")

	var totalRaw, totalCompressed, totalDisk int64
	totalRaw = global.LcmStats.UncompressedBytes + global.TxDataStats.UncompressedBytes + global.HashSeqStats.ExpectedBytes
	totalCompressed = global.LcmStats.CompressedBytes + global.TxDataStats.CompressedBytes + global.HashSeqStats.ExpectedBytes

	if config.EnableLedgerSeqToLcm && lcmDB != nil {
		totalDisk += getRocksDBSSTSize(lcmDB)
	}
	if config.EnableTxHashToTxData && txDataDB != nil {
		totalDisk += getRocksDBSSTSize(txDataDB)
	}
	if config.EnableTxHashToLedgerSeq && hashSeqDB != nil {
		totalDisk += getRocksDBSSTSize(hashSeqDB)
	}

	log.Printf("  Total Raw Data:              %s", helpers.FormatBytes(totalRaw))
	log.Printf("  Total After Compression:     %s", helpers.FormatBytes(totalCompressed))
	log.Printf("  Total Disk Usage:            %s", helpers.FormatBytes(totalDisk))

	if totalCompressed > 0 && totalDisk > 0 {
		overallOverhead := float64(totalDisk-totalCompressed) / float64(totalCompressed) * 100
		log.Printf("  Overall RocksDB Overhead:    %.2f%% over compressed", overallOverhead)
	}
	if totalRaw > 0 && totalDisk > 0 {
		effectiveSavings := 100.0 * (1.0 - float64(totalDisk)/float64(totalRaw))
		log.Printf("  Effective Space Savings:     %.2f%% (raw → disk)", effectiveSavings)
	}

	log.Printf("")
	log.Printf("################################################################################")
	log.Printf("################################################################################")
	log.Printf("")
}

// =============================================================================
// RocksDB Statistics Helpers
// =============================================================================

// getRocksDBSSTSize returns the total SST file size for a RocksDB instance.
func getRocksDBSSTSize(db *grocksdb.DB) int64 {
	if db == nil {
		return 0
	}
	sizeStr := db.GetProperty("rocksdb.total-sst-files-size")
	size, _ := strconv.ParseInt(sizeStr, 10, 64)
	return size
}

// logRocksDBLevelStats logs the file distribution across levels.
func logRocksDBLevelStats(db *grocksdb.DB, name string) {
	if db == nil {
		return
	}

	log.Printf("  RocksDB Level Statistics for %s:", name)

	l0 := db.GetProperty("rocksdb.num-files-at-level0")
	l1 := db.GetProperty("rocksdb.num-files-at-level1")
	l2 := db.GetProperty("rocksdb.num-files-at-level2")
	l3 := db.GetProperty("rocksdb.num-files-at-level3")
	l4 := db.GetProperty("rocksdb.num-files-at-level4")
	l5 := db.GetProperty("rocksdb.num-files-at-level5")
	l6 := db.GetProperty("rocksdb.num-files-at-level6")

	log.Printf("    Files by Level: L0=%s L1=%s L2=%s L3=%s L4=%s L5=%s L6=%s",
		l0, l1, l2, l3, l4, l5, l6)

	estimatedKeys := db.GetProperty("rocksdb.estimate-num-keys")
	totalSSTSize := db.GetProperty("rocksdb.total-sst-files-size")
	curMemtable := db.GetProperty("rocksdb.cur-size-all-mem-tables")
	compactionPending := db.GetProperty("rocksdb.compaction-pending")
	numRunningCompactions := db.GetProperty("rocksdb.num-running-compactions")

	keysInt, _ := strconv.ParseInt(estimatedKeys, 10, 64)
	sstInt, _ := strconv.ParseInt(totalSSTSize, 10, 64)
	memInt, _ := strconv.ParseInt(curMemtable, 10, 64)

	log.Printf("    Estimated Keys: %s", helpers.FormatNumber(keysInt))
	log.Printf("    SST Files Size: %s", helpers.FormatBytes(sstInt))
	log.Printf("    Memtable Size:  %s", helpers.FormatBytes(memInt))
	log.Printf("    Compaction Pending: %s | Running: %s", compactionPending, numRunningCompactions)
}
