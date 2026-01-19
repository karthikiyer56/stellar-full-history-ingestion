// tx-hash-lfs-to-rocksdb reads ledgers from an LFS store, extracts transaction
// hashes, and writes tx_hash -> ledger_seq mappings to a RocksDB store.
//
// The output store uses 16 column families partitioned by the first hex
// character of the transaction hash, enabling parallel processing and
// efficient lookups.
//
// Usage:
//
//	tx-hash-lfs-to-rocksdb \
//	  --lfs-store /path/to/lfs/ledgers \
//	  --output /path/to/output/rocksdb \
//	  --start-ledger 1 \
//	  --end-ledger 1000000 \
//	  --workers 16
//
// Or with a config file:
//
//	tx-hash-lfs-to-rocksdb --config config.toml
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	txhashrework "github.com/karthikiyer56/stellar-full-history-ingestion/rocksdb/tx-hash-ledger-sequence-rework"
)

func main() {
	// =========================================================================
	// Parse Command Line Flags
	// =========================================================================
	var (
		configFile   = flag.String("config", "", "Path to TOML config file (optional)")
		lfsStorePath = flag.String("lfs-store", "", "Path to LFS ledger store")
		outputPath   = flag.String("output", "", "Path for output RocksDB store")
		startLedger  = flag.Uint("start-ledger", 0, "First ledger to process (0=auto-detect)")
		endLedger    = flag.Uint("end-ledger", 0, "Last ledger to process (0=auto-detect)")
		workers      = flag.Int("workers", 0, "Number of worker goroutines (0=NumCPU)")
		batchSize    = flag.Int("batch-size", 100000, "Entries per write batch")
		logFile      = flag.String("log-file", "", "Path to log file (stdout if empty)")
		errorFile    = flag.String("error-file", "", "Path to error log file (stderr if empty)")
		showMemory   = flag.Bool("show-memory", false, "Show memory configuration and exit")
		dryRun       = flag.Bool("dry-run", false, "Show full configuration including auto-detected range and exit")
		showHelp     = flag.Bool("help", false, "Show help message")
		showVersion  = flag.Bool("version", false, "Show version information")
	)

	flag.Parse()

	if *showHelp {
		printUsage()
		os.Exit(0)
	}

	if *showVersion {
		fmt.Println("tx-hash-lfs-to-rocksdb v1.0.0")
		fmt.Println("Part of stellar-full-history-ingestion tools")
		os.Exit(0)
	}

	// =========================================================================
	// Load Configuration
	// =========================================================================
	var config *txhashrework.Config
	var err error

	if *configFile != "" {
		config, err = txhashrework.LoadConfig(*configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
			os.Exit(1)
		}
	} else {
		config = &txhashrework.Config{}
		*config = txhashrework.DefaultConfig()
	}

	// Override with command-line flags
	if *lfsStorePath != "" {
		config.LFS.LfsStorePath = *lfsStorePath
	}
	if *outputPath != "" {
		config.OutputPath = *outputPath
	}
	if *startLedger > 0 {
		config.LFS.StartLedger = uint32(*startLedger)
	}
	if *endLedger > 0 {
		config.LFS.EndLedger = uint32(*endLedger)
	}
	if *workers > 0 {
		config.LFS.Workers = *workers
	}
	if *batchSize > 0 {
		config.BatchSize = *batchSize
	}

	// Show memory configuration if requested
	if *showMemory {
		showMemoryConfig(config)
		os.Exit(0)
	}

	// Validate configuration
	if err := config.ValidateForLfsInput(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		os.Exit(1)
	}

	// Handle dry-run mode - show configuration and exit
	if *dryRun {
		printDryRunSummary(config)
		os.Exit(0)
	}

	// =========================================================================
	// Setup Logger
	// =========================================================================
	logger, err := txhashrework.NewLogger(txhashrework.LoggerConfig{
		LogFilePath:   *logFile,
		ErrorFilePath: *errorFile,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	// =========================================================================
	// Print Configuration
	// =========================================================================
	logger.Info("================================================================================")
	logger.Info("                    TX HASH LFS TO ROCKSDB PROCESSOR")
	logger.Info("================================================================================")
	logger.Info("")
	logger.Info("INPUT:")
	logger.Info("  LFS Store:       %s", config.LFS.LfsStorePath)
	logger.Info("  Ledger Range:    %d - %d (%s ledgers)",
		config.LFS.StartLedger, config.LFS.EndLedger,
		helpers.FormatNumber(int64(config.LFS.EndLedger-config.LFS.StartLedger+1)))
	logger.Info("")
	logger.Info("OUTPUT:")
	logger.Info("  RocksDB Store:   %s", config.OutputPath)
	logger.Info("  Column Families: 16 (partitioned by first hex char)")
	logger.Info("")
	logger.Info("PROCESSING:")
	logger.Info("  Workers:         %d", config.LFS.Workers)
	logger.Info("  Batch Size:      %s entries", helpers.FormatNumber(int64(config.BatchSize)))
	logger.Info("")

	memtables, blockCache, total := config.CalculateMemoryUsage()
	logger.Info("MEMORY:")
	logger.Info("  MemTables:       %d MB (%d MB x %d x 16 CFs)",
		memtables, config.RocksDB.WriteBufferSizeMB, config.RocksDB.MaxWriteBufferNumber)
	logger.Info("  Block Cache:     %d MB", blockCache)
	logger.Info("  Total:           ~%d MB", total)
	logger.Info("")
	logger.Info("ROCKSDB:")
	logger.Info("  WAL:             %s", map[bool]string{true: "DISABLED", false: "ENABLED"}[config.RocksDB.DisableWAL])
	logger.Info("  Compaction:      %s", map[bool]string{true: "DISABLED (manual)", false: "ENABLED (auto)"}[config.RocksDB.DisableAutoCompactions])
	logger.Info("  Bloom Filter:    %d bits/key", config.RocksDB.BloomFilterBitsPerKey)
	logger.Info("")
	logger.Info("================================================================================")
	logger.Info("")

	// =========================================================================
	// Setup Signal Handler
	// =========================================================================
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("")
		logger.Info("Received signal %v, shutting down gracefully...", sig)
		// The cleanup will happen via deferred Close() calls
		os.Exit(1)
	}()

	// =========================================================================
	// Open Output Store
	// =========================================================================
	logger.Info("Opening output store with column families...")
	outputStore, err := txhashrework.OpenOutputStore(config, logger)
	if err != nil {
		logger.Error("Failed to open output store: %v", err)
		os.Exit(1)
	}
	defer outputStore.Close()
	logger.Info("")

	// =========================================================================
	// Process LFS Input
	// =========================================================================
	startTime := time.Now()
	processor := NewLfsProcessor(config, outputStore, logger)
	stats, err := processor.Process()
	if err != nil {
		logger.Error("Processing failed: %v", err)
		os.Exit(1)
	}

	totalTime := time.Since(startTime)

	// =========================================================================
	// Final Summary
	// =========================================================================
	stats.LogSummary(logger, totalTime)

	logger.Info("")
	logger.Info("NEXT STEPS:")
	logger.Info("  1. Run 'compact-store --store %s' to trigger manual compaction", config.OutputPath)
	logger.Info("  2. After compaction, the store is ready for recsplit or queries")
	logger.Info("")
	logger.Info("================================================================================")
}

func printUsage() {
	fmt.Println("tx-hash-lfs-to-rocksdb - Build tx_hash -> ledger_seq RocksDB store from LFS")
	fmt.Println()
	fmt.Println("USAGE:")
	fmt.Println("  tx-hash-lfs-to-rocksdb [OPTIONS]")
	fmt.Println()
	fmt.Println("OPTIONS:")
	fmt.Println("  --lfs-store PATH     Path to LFS ledger store (required)")
	fmt.Println("  --output PATH        Path for output RocksDB store (required)")
	fmt.Println("  --start-ledger N     First ledger to process (0=auto-detect)")
	fmt.Println("  --end-ledger N       Last ledger to process (0=auto-detect)")
	fmt.Println("  --workers N          Number of worker goroutines (default: NumCPU)")
	fmt.Println("  --batch-size N       Entries per write batch (default: 100000)")
	fmt.Println("  --config PATH        Path to TOML config file (optional)")
	fmt.Println("  --log-file PATH      Log to file instead of stdout")
	fmt.Println("  --error-file PATH    Error log to file instead of stderr")
	fmt.Println("  --dry-run            Show configuration (including auto-detected range) and exit")
	fmt.Println("  --show-memory        Show memory configuration and exit")
	fmt.Println("  --help               Show this help message")
	fmt.Println("  --version            Show version information")
	fmt.Println()
	fmt.Println("EXAMPLES:")
	fmt.Println("  # Process all ledgers in an LFS store")
	fmt.Println("  tx-hash-lfs-to-rocksdb \\")
	fmt.Println("    --lfs-store /data/stellar/lfs \\")
	fmt.Println("    --output /data/stellar/tx-hash-store")
	fmt.Println()
	fmt.Println("  # Process specific ledger range with 8 workers")
	fmt.Println("  tx-hash-lfs-to-rocksdb \\")
	fmt.Println("    --lfs-store /data/stellar/lfs \\")
	fmt.Println("    --output /data/stellar/tx-hash-store \\")
	fmt.Println("    --start-ledger 1000000 \\")
	fmt.Println("    --end-ledger 2000000 \\")
	fmt.Println("    --workers 8")
	fmt.Println()
	fmt.Println("OUTPUT:")
	fmt.Println("  Creates a RocksDB store with 16 column families (0-9, a-f)")
	fmt.Println("  Each column family contains transactions whose hash starts with")
	fmt.Println("  that hex character. This enables parallel processing and efficient")
	fmt.Println("  lookups.")
	fmt.Println()
	fmt.Println("  After ingestion, run 'compact-store' to compact the store")
	fmt.Println("  before using it for queries or building a recsplit index.")
}

func showMemoryConfig(config *txhashrework.Config) {
	memtables, blockCache, total := config.CalculateMemoryUsage()

	fmt.Println("MEMORY CONFIGURATION:")
	fmt.Println()
	fmt.Printf("  Write Buffer (MemTable):   %d MB per CF\n", config.RocksDB.WriteBufferSizeMB)
	fmt.Printf("  Max Write Buffer Number:   %d per CF\n", config.RocksDB.MaxWriteBufferNumber)
	fmt.Printf("  Column Families:           16\n")
	fmt.Printf("  Total MemTable RAM:        %d MB\n", memtables)
	fmt.Println()
	fmt.Printf("  Block Cache:               %d MB (shared)\n", blockCache)
	fmt.Println()
	fmt.Printf("  TOTAL ESTIMATED:           ~%d MB (~%.1f GB)\n", total, float64(total)/1024)
	fmt.Println()
	fmt.Println("ROCKSDB SETTINGS:")
	fmt.Printf("  WAL:                       %s\n", map[bool]string{true: "DISABLED", false: "ENABLED"}[config.RocksDB.DisableWAL])
	fmt.Printf("  Auto Compaction:           %s\n", map[bool]string{true: "DISABLED", false: "ENABLED"}[config.RocksDB.DisableAutoCompactions])
	fmt.Printf("  Bloom Filter:              %d bits/key\n", config.RocksDB.BloomFilterBitsPerKey)
	fmt.Printf("  Target SST Size:           %d MB\n", config.RocksDB.TargetFileSizeMB)
}

func printDryRunSummary(config *txhashrework.Config) {
	memtables, blockCache, total := config.CalculateMemoryUsage()

	fmt.Println("================================================================================")
	fmt.Println("                    DRY RUN - Configuration Summary")
	fmt.Println("================================================================================")
	fmt.Println()

	fmt.Println("INPUT:")
	fmt.Printf("  LFS Store:           %s\n", config.LFS.LfsStorePath)
	fmt.Println()

	fmt.Println("CHUNK RANGE (auto-detected):")
	fmt.Printf("  Start Chunk:         %d\n", config.LFS.LedgerRange.StartChunk)
	fmt.Printf("  End Chunk:           %d\n", config.LFS.LedgerRange.EndChunk)
	fmt.Printf("  Total Chunks:        %s\n", helpers.FormatNumber(int64(config.LFS.LedgerRange.TotalChunks)))
	fmt.Println()

	fmt.Println("LEDGER RANGE:")
	fmt.Printf("  Start Ledger:        %d\n", config.LFS.StartLedger)
	fmt.Printf("  End Ledger:          %d\n", config.LFS.EndLedger)
	fmt.Printf("  Total Ledgers:       %s\n", helpers.FormatNumber(int64(config.LFS.EndLedger-config.LFS.StartLedger+1)))
	fmt.Println()

	fmt.Println("OUTPUT:")
	fmt.Printf("  RocksDB Store:       %s\n", config.OutputPath)
	fmt.Printf("  Column Families:     16 (partitioned by first hex char: 0-9, a-f)\n")
	fmt.Println()

	fmt.Println("PROCESSING:")
	fmt.Printf("  Workers:             %d\n", config.LFS.Workers)
	fmt.Printf("  Batch Size:          %s entries\n", helpers.FormatNumber(int64(config.BatchSize)))
	fmt.Println()

	fmt.Println("MEMORY BUDGET:")
	fmt.Printf("  MemTables:           %d MB (%d MB x %d buffers x 16 CFs)\n",
		memtables, config.RocksDB.WriteBufferSizeMB, config.RocksDB.MaxWriteBufferNumber)
	fmt.Printf("  Block Cache:         %d MB (shared)\n", blockCache)
	fmt.Printf("  Total:               ~%d MB (~%.1f GB)\n", total, float64(total)/1024)
	fmt.Println()

	fmt.Println("ROCKSDB SETTINGS:")
	fmt.Printf("  WAL:                 %s\n", map[bool]string{true: "DISABLED", false: "ENABLED"}[config.RocksDB.DisableWAL])
	fmt.Printf("  Auto Compaction:     %s\n", map[bool]string{true: "DISABLED (manual)", false: "ENABLED"}[config.RocksDB.DisableAutoCompactions])
	fmt.Printf("  Bloom Filter:        %d bits/key (~%.1f%% false positive rate)\n",
		config.RocksDB.BloomFilterBitsPerKey,
		100.0*bloomFalsePositiveRate(config.RocksDB.BloomFilterBitsPerKey))
	fmt.Printf("  Target SST Size:     %d MB\n", config.RocksDB.TargetFileSizeMB)
	fmt.Printf("  Max Background Jobs: %d\n", config.RocksDB.MaxBackgroundJobs)
	fmt.Printf("  Max Open Files:      %d\n", config.RocksDB.MaxOpenFiles)
	fmt.Println()

	fmt.Println("================================================================================")
	fmt.Println("To proceed with ingestion, run the same command without --dry-run")
	fmt.Println("================================================================================")
}

// bloomFalsePositiveRate estimates the false positive rate for a bloom filter.
// Formula: (1 - e^(-k*n/m))^k where k = bits_per_key * ln(2)
// Simplified approximation: 0.6185^bits_per_key
func bloomFalsePositiveRate(bitsPerKey int) float64 {
	if bitsPerKey <= 0 {
		return 1.0
	}
	// Approximate false positive rate
	rate := 1.0
	for i := 0; i < bitsPerKey; i++ {
		rate *= 0.6185
	}
	return rate
}
