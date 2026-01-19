// main.go
// =============================================================================
// Transaction Hash to Ledger Sequence - Column Family Merger
// =============================================================================
//
// This tool merges transaction hash → ledger sequence mappings into a single
// RocksDB store with 16 column families, partitioned by the first hex character
// of the transaction hash (0-9, a-f).
//
// INPUT SOURCES:
// ==============
// 1. Multiple existing tx_hash_to_ledger_seq RocksDB stores (input_stores)
// 2. LFS (Local Filesystem) ledger store (lfs_ledger_store_path)
//
// USAGE:
// ======
// # From existing RocksDB stores:
// ./tx-hash-cf-merger --config /path/to/config.toml
//
// # From LFS with custom workers and logging:
// ./tx-hash-cf-merger --config /path/to/config.toml \
//     --lfs-workers 16 \
//     --log-file /var/log/merger.log \
//     --error-file /var/log/merger.err
//
// =============================================================================

package main

import (
	"flag"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
)

func main() {
	// =========================================================================
	// Parse Command-Line Arguments
	// =========================================================================
	var (
		configPath    string
		batchSize     int
		lfsWorkers    int
		logFilePath   string
		errorFilePath string
		dryRun        bool
	)

	flag.StringVar(&configPath, "config", "", "Path to TOML configuration file (required)")
	flag.IntVar(&batchSize, "batch-size", 0, "Override batch size from config")
	flag.IntVar(&lfsWorkers, "lfs-workers", 0, "Override number of LFS workers (default: NumCPU)")
	flag.StringVar(&logFilePath, "log-file", "", "Path to info log file (in addition to stdout)")
	flag.StringVar(&errorFilePath, "error-file", "", "Path to error log file (in addition to stderr)")
	flag.BoolVar(&dryRun, "dry-run", false, "Validate config and count entries without writing")

	flag.Parse()

	// =========================================================================
	// Validate Required Arguments
	// =========================================================================
	if configPath == "" {
		log.Fatal("ERROR: --config is required")
	}

	// =========================================================================
	// Setup Logger
	// =========================================================================
	logger, err := NewDualLogger(LoggerConfig{
		LogFilePath:   logFilePath,
		ErrorFilePath: errorFilePath,
	})
	if err != nil {
		log.Fatalf("ERROR: Failed to setup logger: %v", err)
	}
	defer logger.Close()

	// =========================================================================
	// Load Configuration
	// =========================================================================
	logger.Info("Loading configuration from: %s", configPath)

	config, err := LoadConfig(configPath)
	if err != nil {
		logger.Error("Failed to load configuration: %v", err)
		os.Exit(1)
	}

	// Override from command line
	if batchSize > 0 {
		config.BatchSize = batchSize
	}
	if lfsWorkers > 0 {
		config.LfsWorkers = lfsWorkers
	}

	// =========================================================================
	// Print Configuration Summary
	// =========================================================================
	logger.Info("")
	logger.Info("================================================================================")
	logger.Info("        TX_HASH_TO_LEDGER_SEQ COLUMN FAMILY MERGER")
	logger.Info("================================================================================")
	logger.Info("")

	// Mode
	modeStr := "MERGE"
	if dryRun {
		modeStr = "DRY RUN (no writes)"
	}
	logger.Info("MODE:                  %s", modeStr)

	// Input source
	if config.IsLfsMode {
		logger.Info("Input Source:          LFS (Local Filesystem)")
		logger.Info("  LFS Store:           %s", config.LfsLedgerStorePath)
		logger.Info("  Ledger Range:        %d - %d (%s ledgers)",
			config.LfsStartLedger, config.LfsEndLedger,
			helpers.FormatNumber(int64(config.LfsEndLedger-config.LfsStartLedger+1)))
		logger.Info("  Workers:             %d", config.LfsWorkers)
	} else {
		logger.Info("Input Source:          RocksDB Stores (%d)", len(config.InputStores))
		for i, store := range config.InputStores {
			logger.Info("  [%d] %s", i+1, store)
		}
		logger.Info("Workers:               %d", runtime.NumCPU())
	}

	logger.Info("Output Store:          %s", config.OutputPath)
	logger.Info("Batch Size:            %s entries", helpers.FormatNumber(int64(config.BatchSize)))
	logger.Info("")
	logger.Info("Column Families:       16 (partitioned by first hex char: 0-9, a-f)")
	logger.Info("")
	logger.Info("RocksDB Settings:")
	logger.Info("  Write Buffer:        %d MB x %d = %d MB per CF",
		config.RocksDB.WriteBufferSizeMB,
		config.RocksDB.MaxWriteBufferNumber,
		config.RocksDB.WriteBufferSizeMB*config.RocksDB.MaxWriteBufferNumber)
	logger.Info("  Total MemTable RAM:  %d MB (16 CFs)",
		config.RocksDB.WriteBufferSizeMB*config.RocksDB.MaxWriteBufferNumber*16)
	logger.Info("  Target File Size:    %d MB", config.RocksDB.TargetFileSizeMB)
	logger.Info("  Bloom Filter:        %d bits/key", config.RocksDB.BloomFilterBitsPerKey)
	logger.Info("")
	logger.Info("================================================================================")
	logger.Info("")

	// =========================================================================
	// Validate Input (for non-LFS mode)
	// =========================================================================
	if !config.IsLfsMode {
		logger.Info("Validating input stores...")
		for _, storePath := range config.InputStores {
			if _, err := os.Stat(storePath); os.IsNotExist(err) {
				logger.Error("Input store does not exist: %s", storePath)
				os.Exit(1)
			}
			logger.Info("  [OK] %s", storePath)
		}
		logger.Info("")
	}

	// =========================================================================
	// Execute Merge
	// =========================================================================
	startTime := time.Now()

	var stats *MergeStats
	if config.IsLfsMode {
		stats, err = ExecuteLfsMerge(config, logger, dryRun)
	} else {
		stats, err = ExecuteMerge(config, dryRun)
	}

	if err != nil {
		logger.Error("Merge failed: %v", err)
		os.Exit(1)
	}

	totalTime := time.Since(startTime)

	// =========================================================================
	// Print Final Summary
	// =========================================================================
	logger.Info("")
	logger.Info("================================================================================")
	logger.Info("                              MERGE COMPLETE")
	logger.Info("================================================================================")
	logger.Info("")
	logger.Info("STATISTICS:")
	logger.Info("  Total Entries Read:    %s", helpers.FormatNumber(stats.TotalEntriesRead))
	logger.Info("  Total Entries Written: %s", helpers.FormatNumber(stats.TotalEntriesWritten))
	if stats.DuplicateEntries > 0 {
		logger.Info("  Duplicate Entries:     %s", helpers.FormatNumber(stats.DuplicateEntries))
	}
	logger.Info("")
	logger.Info("COLUMN FAMILY DISTRIBUTION:")

	for i := 0; i < 16; i++ {
		cfName := ColumnFamilyName(i)
		count := stats.EntriesPerCF[cfName]
		pct := float64(0)
		if stats.TotalEntriesWritten > 0 {
			pct = float64(count) / float64(stats.TotalEntriesWritten) * 100
		}
		logger.Info("  CF[%s]: %s (%.2f%%)", cfName, helpers.FormatNumber(count), pct)
	}
	logger.Info("")
	logger.Info("TIMING:")
	logger.Info("  Total Time:            %s", helpers.FormatDuration(totalTime))
	logger.Info("  Processing Time:       %s", helpers.FormatDuration(stats.ReadTime))
	logger.Info("  Write Time:            %s", helpers.FormatDuration(stats.WriteTime))
	logger.Info("  Compaction Time:       %s", helpers.FormatDuration(stats.CompactionTime))
	logger.Info("")

	if stats.ReadTime.Seconds() > 0 && stats.WriteTime.Seconds() > 0 {
		logger.Info("THROUGHPUT:")
		logger.Info("  Read Rate:             %s entries/sec",
			helpers.FormatNumber(int64(float64(stats.TotalEntriesRead)/stats.ReadTime.Seconds())))
		logger.Info("  Write Rate:            %s entries/sec",
			helpers.FormatNumber(int64(float64(stats.TotalEntriesWritten)/stats.WriteTime.Seconds())))
		logger.Info("")
	}

	logger.Info("================================================================================")
	logger.Info("")
	logger.Info("Merge completed successfully!")
}
