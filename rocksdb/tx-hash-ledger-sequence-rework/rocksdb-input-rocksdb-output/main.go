// tx-hash-merge-stores merges multiple RocksDB stores containing tx_hash ->
// ledger_seq mappings into a single output store with 16 column families.
//
// This tool is useful for:
//   - Merging stores from different time periods
//   - Converting single-CF stores to multi-CF format
//   - Consolidating distributed stores
//
// Usage:
//
//	tx-hash-merge-stores \
//	  --input /path/to/store1 \
//	  --input /path/to/store2 \
//	  --output /path/to/merged-store
//
// Or with a config file:
//
//	tx-hash-merge-stores --config config.toml
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	txhashrework "github.com/karthikiyer56/stellar-full-history-ingestion/rocksdb/tx-hash-ledger-sequence-rework"
)

// stringSliceFlag allows multiple --input flags
type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	return strings.Join(*s, ", ")
}

func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	// =========================================================================
	// Parse Command Line Flags
	// =========================================================================
	var inputStores stringSliceFlag

	var (
		configFile  = flag.String("config", "", "Path to TOML config file (optional)")
		outputPath  = flag.String("output", "", "Path for output RocksDB store")
		batchSize   = flag.Int("batch-size", 100000, "Entries per write batch")
		logFile     = flag.String("log-file", "", "Path to log file (stdout if empty)")
		errorFile   = flag.String("error-file", "", "Path to error log file (stderr if empty)")
		showHelp    = flag.Bool("help", false, "Show help message")
		showVersion = flag.Bool("version", false, "Show version information")
	)

	flag.Var(&inputStores, "input", "Path to input RocksDB store (can be repeated)")
	flag.Parse()

	if *showHelp {
		printUsage()
		os.Exit(0)
	}

	if *showVersion {
		fmt.Println("tx-hash-merge-stores v1.0.0")
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
	if len(inputStores) > 0 {
		config.InputStores = inputStores
	}
	if *outputPath != "" {
		config.OutputPath = *outputPath
	}
	if *batchSize > 0 {
		config.BatchSize = *batchSize
	}

	// Validate configuration
	if err := config.ValidateForRocksDBInput(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		os.Exit(1)
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
	logger.Info("                    TX HASH ROCKSDB MERGE PROCESSOR")
	logger.Info("================================================================================")
	logger.Info("")
	logger.Info("INPUT STORES (%d):", len(config.InputStores))
	for i, store := range config.InputStores {
		logger.Info("  %d. %s", i+1, store)
	}
	logger.Info("")
	logger.Info("OUTPUT:")
	logger.Info("  RocksDB Store:   %s", config.OutputPath)
	logger.Info("  Column Families: 16 (partitioned by first hex char)")
	logger.Info("")
	logger.Info("PROCESSING:")
	logger.Info("  Batch Size:      %s entries", helpers.FormatNumber(int64(config.BatchSize)))
	logger.Info("")

	memtables, blockCache, total := config.CalculateMemoryUsage()
	logger.Info("MEMORY:")
	logger.Info("  MemTables:       %d MB", memtables)
	logger.Info("  Block Cache:     %d MB", blockCache)
	logger.Info("  Total:           ~%d MB", total)
	logger.Info("")
	logger.Info("ROCKSDB:")
	logger.Info("  WAL:             %s", map[bool]string{true: "DISABLED", false: "ENABLED"}[config.RocksDB.DisableWAL])
	logger.Info("  Compaction:      %s", map[bool]string{true: "DISABLED (manual)", false: "ENABLED (auto)"}[config.RocksDB.DisableAutoCompactions])
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
	// Process Input Stores
	// =========================================================================
	startTime := time.Now()
	processor := NewMergeProcessor(config, outputStore, logger)
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
	fmt.Println("tx-hash-merge-stores - Merge multiple tx_hash RocksDB stores")
	fmt.Println()
	fmt.Println("USAGE:")
	fmt.Println("  tx-hash-merge-stores [OPTIONS]")
	fmt.Println()
	fmt.Println("OPTIONS:")
	fmt.Println("  --input PATH         Path to input RocksDB store (can be repeated)")
	fmt.Println("  --output PATH        Path for output RocksDB store (required)")
	fmt.Println("  --batch-size N       Entries per write batch (default: 100000)")
	fmt.Println("  --config PATH        Path to TOML config file (optional)")
	fmt.Println("  --log-file PATH      Log to file instead of stdout")
	fmt.Println("  --error-file PATH    Error log to file instead of stderr")
	fmt.Println("  --help               Show this help message")
	fmt.Println("  --version            Show version information")
	fmt.Println()
	fmt.Println("EXAMPLES:")
	fmt.Println("  # Merge two stores")
	fmt.Println("  tx-hash-merge-stores \\")
	fmt.Println("    --input /data/store1 \\")
	fmt.Println("    --input /data/store2 \\")
	fmt.Println("    --output /data/merged-store")
	fmt.Println()
	fmt.Println("  # Merge with config file")
	fmt.Println("  tx-hash-merge-stores --config config.toml")
	fmt.Println()
	fmt.Println("CONFIG FILE FORMAT (TOML):")
	fmt.Println()
	fmt.Println("  output_path = \"/data/merged-store\"")
	fmt.Println("  batch_size = 100000")
	fmt.Println("  input_stores = [")
	fmt.Println("    \"/data/store1\",")
	fmt.Println("    \"/data/store2\"")
	fmt.Println("  ]")
	fmt.Println()
	fmt.Println("OUTPUT:")
	fmt.Println("  Creates a RocksDB store with 16 column families (0-9, a-f)")
	fmt.Println("  partitioned by the first hex character of the transaction hash.")
	fmt.Println()
	fmt.Println("  After merging, run 'compact-store' to optimize the store.")
}
