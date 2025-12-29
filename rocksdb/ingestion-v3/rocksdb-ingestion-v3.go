// rocksdb-ingestion-v3.go
// =============================================================================
// RocksDB Store Copy Tool
// =============================================================================
//
// This tool copies data from one RocksDB store to another with configurable
// settings optimized for either streaming (low memory) or backfill (high throughput).
//
// USAGE:
// ======
// Backfill mode (high throughput, compaction at end):
//   ./rocksdb-ingestion-v3 --input /path/to/source --output /path/to/dest --backfill
//
// Streaming mode (low memory, continuous compaction):
//   ./rocksdb-ingestion-v3 --input /path/to/source --output /path/to/dest --streaming
//
// =============================================================================

package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// Constants
// =============================================================================

const (
	MB = 1024 * 1024
	GB = 1024 * 1024 * 1024

	// Default read cache size for input store
	DefaultReadCacheSizeMB = 512

	// Default batch size
	DefaultBatchSize = 10000

	// Default target file size for compact-only mode
	DefaultTargetFileSizeMB = 4096
)

// =============================================================================
// Configuration Structures
// =============================================================================

// Config holds all configuration for the copy operation
type Config struct {
	InputPath  string
	OutputPath string

	// Mode
	IsBackfill    bool
	IsStreaming   bool
	IsCompactOnly bool

	// Compact-only settings
	CompactTargetFileSizeMB       int
	CompactMaxBytesForLevelBaseMB int
	CompactMaxBackgroundJobs      int

	// Batch size
	BatchSize int

	// RocksDB settings (determined by mode)
	WriteBufferSizeMB           int
	MaxWriteBufferNumber        int
	MinWriteBufferNumberToMerge int
	L0CompactionTrigger         int
	L0SlowdownWritesTrigger     int
	L0StopWritesTrigger         int
	TargetFileSizeMB            int
	MaxBytesForLevelBaseMB      int
	MaxBackgroundJobs           int
	DisableWAL                  bool
	LevelCompactionDynamicLevel bool
	BloomFilterBitsPerKey       int
}

// CopyStats tracks statistics during the copy operation
type CopyStats struct {
	TotalKeysEstimate  int64
	TotalKeysProcessed int64
	TotalBatches       int64
	CurrentBatch       int64

	TotalBytesRead   int64 // Raw key+value bytes
	TotalKeysBytes   int64 // Just key bytes
	TotalValuesBytes int64 // Just value bytes

	StartTime           time.Time
	LastReportTime      time.Time
	LastReportedPercent int

	BatchWriteTimes []time.Duration
}

// RocksDBConfig represents parsed RocksDB configuration from OPTIONS file
type RocksDBConfig struct {
	WriteBufferSize                  int64
	MaxWriteBufferNumber             int
	MinWriteBufferNumberToMerge      int
	Level0FileNumCompactionTrigger   int
	Level0SlowdownWritesTrigger      int
	Level0StopWritesTrigger          int
	TargetFileSizeBase               int64
	MaxBytesForLevelBase             int64
	MaxBackgroundJobs                int
	LevelCompactionDynamicLevelBytes bool
}

// =============================================================================
// Default Configurations
// =============================================================================

// GetBackfillDefaults returns default settings for backfill mode
func GetBackfillDefaults() Config {
	return Config{
		WriteBufferSizeMB:           1024,
		MaxWriteBufferNumber:        6,
		MinWriteBufferNumberToMerge: 4,
		L0CompactionTrigger:         999,
		L0SlowdownWritesTrigger:     999,
		L0StopWritesTrigger:         999,
		TargetFileSizeMB:            4096,
		MaxBytesForLevelBaseMB:      40960,
		MaxBackgroundJobs:           8,
		DisableWAL:                  true,
		LevelCompactionDynamicLevel: false,
		BloomFilterBitsPerKey:       10,
		BatchSize:                   DefaultBatchSize,
	}
}

// GetStreamingDefaults returns default settings for streaming mode
func GetStreamingDefaults() Config {
	return Config{
		WriteBufferSizeMB:           256,
		MaxWriteBufferNumber:        4,
		MinWriteBufferNumberToMerge: 1,
		L0CompactionTrigger:         4,
		L0SlowdownWritesTrigger:     20,
		L0StopWritesTrigger:         36,
		TargetFileSizeMB:            4096,
		MaxBytesForLevelBaseMB:      40960,
		MaxBackgroundJobs:           4,
		DisableWAL:                  false,
		LevelCompactionDynamicLevel: false,
		BloomFilterBitsPerKey:       10,
		BatchSize:                   DefaultBatchSize,
	}
}

// =============================================================================
// Main Entry Point
// =============================================================================

func main() {
	// Parse command line arguments
	config, err := parseArgs()
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}

	// Validate configuration
	if err := validateConfig(config); err != nil {
		log.Fatalf("ERROR: %v", err)
	}

	// Handle compact-only mode
	if config.IsCompactOnly {
		runCompactOnly(config)
		return
	}

	// Print configuration summary
	printConfigSummary(config)

	// Open input store (read-only)
	log.Printf("Opening input store (read-only): %s", config.InputPath)
	inputDB, inputOpts, inputRO, err := openInputStore(config.InputPath)
	if err != nil {
		log.Fatalf("ERROR: Failed to open input store: %v", err)
	}
	defer inputDB.Close()
	defer inputOpts.Destroy()
	defer inputRO.Destroy()
	log.Printf("✓ Input store opened successfully")

	// Get estimated key count from input
	estimatedKeys := getEstimatedKeyCount(inputDB)
	log.Printf("  Estimated keys in input store: %s", helpers.FormatNumber(estimatedKeys))

	// Get input store size
	inputSize := getRocksDBSSTSize(inputDB)
	log.Printf("  Input store SST size: %s", helpers.FormatBytes(inputSize))
	log.Printf("")

	// Open output store (create new)
	log.Printf("Creating output store: %s", config.OutputPath)
	outputDB, outputOpts, err := openOutputStore(config)
	if err != nil {
		log.Fatalf("ERROR: Failed to create output store: %v", err)
	}
	defer outputDB.Close()
	defer outputOpts.Destroy()
	log.Printf("✓ Output store created successfully")
	log.Printf("")

	// Verify output store configuration
	log.Printf("================================================================================")
	log.Printf("                    VERIFYING OUTPUT STORE CONFIGURATION")
	log.Printf("================================================================================")
	log.Printf("")
	verifyOutputStoreConfig(config)
	log.Printf("")

	// Initialize statistics
	stats := &CopyStats{
		TotalKeysEstimate:   estimatedKeys,
		StartTime:           time.Now(),
		LastReportTime:      time.Now(),
		LastReportedPercent: -1,
	}

	// Calculate total batches
	if estimatedKeys > 0 {
		stats.TotalBatches = (estimatedKeys + int64(config.BatchSize) - 1) / int64(config.BatchSize)
	}

	// Perform the copy
	log.Printf("================================================================================")
	log.Printf("                          STARTING DATA COPY")
	log.Printf("================================================================================")
	log.Printf("")
	log.Printf("  Batch Size:          %s keys", helpers.FormatNumber(int64(config.BatchSize)))
	log.Printf("  Estimated Batches:   %s", helpers.FormatNumber(stats.TotalBatches))
	log.Printf("  WAL Disabled:        %v", config.DisableWAL)
	log.Printf("")

	err = copyData(inputDB, inputRO, outputDB, config, stats)
	if err != nil {
		log.Fatalf("ERROR: Copy failed: %v", err)
	}

	// Show pre-compaction stats
	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("                       PRE-COMPACTION STATISTICS")
	log.Printf("================================================================================")
	log.Printf("")
	printStoreStats(outputDB, "Output Store (before compaction)")

	// Check for pending/running compactions
	printCompactionStatus(outputDB)

	// Perform final compaction
	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("                         PERFORMING FINAL COMPACTION")
	log.Printf("================================================================================")
	log.Printf("")

	compactionStart := time.Now()
	outputDB.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
	compactionDuration := time.Since(compactionStart)

	log.Printf("✓ Compaction completed in %s", helpers.FormatDuration(compactionDuration))
	log.Printf("")

	// Show post-compaction stats
	log.Printf("================================================================================")
	log.Printf("                       POST-COMPACTION STATISTICS")
	log.Printf("================================================================================")
	log.Printf("")
	printStoreStats(outputDB, "Output Store (after compaction)")

	// Print final summary
	printFinalSummary(stats, config, inputSize, outputDB, compactionDuration)

	log.Printf("")
	log.Printf("✅ Copy completed successfully!")
}

// =============================================================================
// Argument Parsing
// =============================================================================

func parseArgs() (*Config, error) {
	var (
		inputPath        string
		outputPath       string
		backfill         bool
		streaming        bool
		compactOnly      bool
		batchSize        int
		targetFileSizeMB int
		levelBaseMB      int
		backgroundJobs   int
	)

	flag.StringVar(&inputPath, "input", "", "Path to input RocksDB store (required for copy modes)")
	flag.StringVar(&outputPath, "output", "", "Path to output RocksDB store (required for copy modes, or path to compact for --compact-only)")
	flag.BoolVar(&backfill, "backfill", false, "Use backfill mode (high throughput, compaction at end)")
	flag.BoolVar(&streaming, "streaming", false, "Use streaming mode (low memory, continuous compaction)")
	flag.BoolVar(&compactOnly, "compact-only", false, "Only compact an existing store (use with --output to specify store path)")
	flag.IntVar(&batchSize, "batch-size", DefaultBatchSize, "Number of keys per batch (copy modes only)")
	flag.IntVar(&targetFileSizeMB, "target-file-size-mb", DefaultTargetFileSizeMB, "Target SST file size in MB (used in --compact-only mode)")
	flag.IntVar(&levelBaseMB, "max-bytes-for-level-base-mb", 40960, "Max bytes for L1 in MB (used in --compact-only mode)")
	flag.IntVar(&backgroundJobs, "max-background-jobs", 8, "Max background compaction jobs (used in --compact-only mode)")

	flag.Parse()

	// Handle compact-only mode
	if compactOnly {
		if outputPath == "" {
			return nil, fmt.Errorf("--output is required for --compact-only mode (path to store to compact)")
		}
		if backfill || streaming {
			return nil, fmt.Errorf("--compact-only cannot be combined with --backfill or --streaming")
		}
		if inputPath != "" {
			return nil, fmt.Errorf("--input is not used in --compact-only mode")
		}

		return &Config{
			OutputPath:                    outputPath,
			IsCompactOnly:                 true,
			CompactTargetFileSizeMB:       targetFileSizeMB,
			CompactMaxBytesForLevelBaseMB: levelBaseMB,
			CompactMaxBackgroundJobs:      backgroundJobs,
		}, nil
	}

	// Validate required args for copy modes
	if inputPath == "" {
		return nil, fmt.Errorf("--input is required")
	}
	if outputPath == "" {
		return nil, fmt.Errorf("--output is required")
	}

	// Validate mode selection
	if !backfill && !streaming {
		return nil, fmt.Errorf("either --backfill, --streaming, or --compact-only must be specified")
	}
	if backfill && streaming {
		return nil, fmt.Errorf("cannot specify both --backfill and --streaming")
	}

	// Get defaults based on mode
	var config Config
	if backfill {
		config = GetBackfillDefaults()
	} else {
		config = GetStreamingDefaults()
	}

	// Set paths and mode flags
	config.InputPath = inputPath
	config.OutputPath = outputPath
	config.IsBackfill = backfill
	config.IsStreaming = streaming

	// Override batch size if provided
	if batchSize > 0 {
		config.BatchSize = batchSize
	}

	return &config, nil
}

// =============================================================================
// Validation
// =============================================================================

func validateConfig(config *Config) error {
	// Compact-only mode validation
	if config.IsCompactOnly {
		// Check path exists
		if _, err := os.Stat(config.OutputPath); os.IsNotExist(err) {
			return fmt.Errorf("store path does not exist: %s", config.OutputPath)
		}

		// Check if it's a valid RocksDB store
		currentFile := filepath.Join(config.OutputPath, "CURRENT")
		if _, err := os.Stat(currentFile); os.IsNotExist(err) {
			return fmt.Errorf("path is not a valid RocksDB store (no CURRENT file): %s", config.OutputPath)
		}

		// Validate target file size
		if config.CompactTargetFileSizeMB <= 0 {
			return fmt.Errorf("target-file-size-mb must be positive")
		}

		return nil
	}

	// Copy mode validation
	// Check input path exists
	if _, err := os.Stat(config.InputPath); os.IsNotExist(err) {
		return fmt.Errorf("input path does not exist: %s", config.InputPath)
	}

	// Check if input is a valid RocksDB store (has CURRENT file)
	currentFile := filepath.Join(config.InputPath, "CURRENT")
	if _, err := os.Stat(currentFile); os.IsNotExist(err) {
		return fmt.Errorf("input path is not a valid RocksDB store (no CURRENT file): %s", config.InputPath)
	}

	// Check output path does NOT exist
	if _, err := os.Stat(config.OutputPath); err == nil {
		return fmt.Errorf("output path already exists: %s", config.OutputPath)
	}

	// Validate batch size
	if config.BatchSize <= 0 {
		return fmt.Errorf("batch-size must be positive")
	}

	return nil
}

// =============================================================================
// Compact-Only Mode
// =============================================================================

func runCompactOnly(config *Config) {
	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("                     ROCKSDB COMPACT-ONLY MODE")
	log.Printf("================================================================================")
	log.Printf("")
	log.Printf("STORE PATH:            %s", config.OutputPath)
	log.Printf("")
	log.Printf("COMPACTION SETTINGS:")
	log.Printf("  target_file_size_mb:          %d MB", config.CompactTargetFileSizeMB)
	log.Printf("  max_bytes_for_level_base_mb:  %d MB", config.CompactMaxBytesForLevelBaseMB)
	log.Printf("  max_background_jobs:          %d", config.CompactMaxBackgroundJobs)
	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("")

	// Open the store with new compaction settings
	log.Printf("Opening store with new compaction settings...")
	db, opts, err := openStoreForCompaction(config)
	if err != nil {
		log.Fatalf("ERROR: Failed to open store: %v", err)
	}
	defer db.Close()
	defer opts.Destroy()
	log.Printf("✓ Store opened successfully")
	log.Printf("")

	// Show pre-compaction stats
	log.Printf("================================================================================")
	log.Printf("                       PRE-COMPACTION STATISTICS")
	log.Printf("================================================================================")
	log.Printf("")
	printStoreStats(db, "Store (before compaction)")
	printCompactionStatus(db)

	// Count files before
	fileCountBefore := countSSTFiles(config.OutputPath)
	sizeBefore := getRocksDBSSTSize(db)

	// Perform compaction
	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("                         PERFORMING COMPACTION")
	log.Printf("================================================================================")
	log.Printf("")
	log.Printf("Compacting with target_file_size = %d MB...", config.CompactTargetFileSizeMB)
	log.Printf("")

	compactionStart := time.Now()
	db.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
	compactionDuration := time.Since(compactionStart)

	log.Printf("✓ Compaction completed in %s", helpers.FormatDuration(compactionDuration))
	log.Printf("")

	// Wait for any background operations to finish
	waitForBackgroundOperations(db)

	// Show post-compaction stats
	log.Printf("================================================================================")
	log.Printf("                       POST-COMPACTION STATISTICS")
	log.Printf("================================================================================")
	log.Printf("")
	printStoreStats(db, "Store (after compaction)")

	// Count files after
	fileCountAfter := countSSTFiles(config.OutputPath)
	sizeAfter := getRocksDBSSTSize(db)

	// Print summary
	log.Printf("")
	log.Printf("################################################################################")
	log.Printf("                              COMPACTION SUMMARY")
	log.Printf("################################################################################")
	log.Printf("")
	log.Printf("  Compaction time:         %s", helpers.FormatDuration(compactionDuration))
	log.Printf("")
	log.Printf("  SST files before:        %d", fileCountBefore)
	log.Printf("  SST files after:         %d", fileCountAfter)
	log.Printf("  Files reduced by:        %d (%.1f%%)",
		fileCountBefore-fileCountAfter,
		float64(fileCountBefore-fileCountAfter)/float64(fileCountBefore)*100)
	log.Printf("")
	log.Printf("  Store size before:       %s", helpers.FormatBytes(sizeBefore))
	log.Printf("  Store size after:        %s", helpers.FormatBytes(sizeAfter))
	log.Printf("  Size change:             %s (%.2f%%)",
		helpers.FormatBytes(sizeAfter-sizeBefore),
		float64(sizeAfter-sizeBefore)/float64(sizeBefore)*100)
	log.Printf("")
	log.Printf("  Target file size:        %d MB", config.CompactTargetFileSizeMB)
	if fileCountAfter > 0 {
		avgFileSize := sizeAfter / int64(fileCountAfter)
		log.Printf("  Average file size:       %s", helpers.FormatBytes(avgFileSize))
	}
	log.Printf("")
	log.Printf("################################################################################")
	log.Printf("")
	log.Printf("✅ Compaction completed successfully!")
}

func openStoreForCompaction(config *Config) (*grocksdb.DB, *grocksdb.Options, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	// Set the new compaction settings
	opts.SetTargetFileSizeBase(uint64(config.CompactTargetFileSizeMB * MB))
	opts.SetTargetFileSizeMultiplier(1)
	opts.SetMaxBytesForLevelBase(uint64(config.CompactMaxBytesForLevelBaseMB * MB))
	opts.SetMaxBytesForLevelMultiplier(10)
	opts.SetMaxBackgroundJobs(config.CompactMaxBackgroundJobs)
	opts.SetLevelCompactionDynamicLevelBytes(false)

	// Disable auto-compaction (we'll trigger manually)
	opts.SetDisableAutoCompactions(true)

	// Use level compaction
	opts.SetCompactionStyle(grocksdb.LevelCompactionStyle)

	// No compression (data is already compressed)
	opts.SetCompression(grocksdb.NoCompression)

	// File resources
	opts.SetMaxOpenFiles(10000)

	// Logging
	opts.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)

	db, err := grocksdb.OpenDb(opts, config.OutputPath)
	if err != nil {
		opts.Destroy()
		return nil, nil, fmt.Errorf("failed to open RocksDB: %w", err)
	}

	return db, opts, nil
}

func countSSTFiles(path string) int {
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0
	}

	count := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".sst") {
			count++
		}
	}
	return count
}

func waitForBackgroundOperations(db *grocksdb.DB) {
	// Poll until no background operations are running
	maxWait := 60 * time.Second
	pollInterval := 500 * time.Millisecond
	waited := time.Duration(0)

	for waited < maxWait {
		runningCompactions := db.GetProperty("rocksdb.num-running-compactions")
		runningFlushes := db.GetProperty("rocksdb.num-running-flushes")
		pendingCompaction := db.GetProperty("rocksdb.compaction-pending")

		if runningCompactions == "0" && runningFlushes == "0" && pendingCompaction == "0" {
			return
		}

		time.Sleep(pollInterval)
		waited += pollInterval
	}

	log.Printf("⚠️  Warning: Background operations still running after %s", helpers.FormatDuration(maxWait))
}

// =============================================================================
// Store Opening Functions
// =============================================================================

func openInputStore(path string) (*grocksdb.DB, *grocksdb.Options, *grocksdb.ReadOptions, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	// Block-based table options with read cache
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	cache := grocksdb.NewLRUCache(uint64(DefaultReadCacheSizeMB * MB))
	bbto.SetBlockCache(cache)
	opts.SetBlockBasedTableFactory(bbto)

	db, err := grocksdb.OpenDbForReadOnly(opts, path, false)
	if err != nil {
		opts.Destroy()
		return nil, nil, nil, fmt.Errorf("failed to open RocksDB for reading: %w", err)
	}

	ro := grocksdb.NewDefaultReadOptions()

	return db, opts, ro, nil
}

func openOutputStore(config *Config) (*grocksdb.DB, *grocksdb.Options, error) {
	// Create output directory
	if err := os.MkdirAll(config.OutputPath, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetErrorIfExists(true) // Fail if DB already exists

	// Write buffer settings
	opts.SetWriteBufferSize(uint64(config.WriteBufferSizeMB * MB))
	opts.SetMaxWriteBufferNumber(config.MaxWriteBufferNumber)
	opts.SetMinWriteBufferNumberToMerge(config.MinWriteBufferNumberToMerge)

	// L0 management
	opts.SetLevel0FileNumCompactionTrigger(config.L0CompactionTrigger)
	opts.SetLevel0SlowdownWritesTrigger(config.L0SlowdownWritesTrigger)
	opts.SetLevel0StopWritesTrigger(config.L0StopWritesTrigger)

	// Compaction settings
	if config.IsBackfill {
		opts.SetDisableAutoCompactions(true)
	} else {
		opts.SetDisableAutoCompactions(false)
	}

	opts.SetCompactionStyle(grocksdb.LevelCompactionStyle)
	opts.SetTargetFileSizeBase(uint64(config.TargetFileSizeMB * MB))
	opts.SetTargetFileSizeMultiplier(1)
	opts.SetMaxBytesForLevelBase(uint64(config.MaxBytesForLevelBaseMB * MB))
	opts.SetMaxBytesForLevelMultiplier(10)
	opts.SetLevelCompactionDynamicLevelBytes(config.LevelCompactionDynamicLevel)

	// Background jobs
	opts.SetMaxBackgroundJobs(config.MaxBackgroundJobs)

	// No compression (data is already compressed)
	opts.SetCompression(grocksdb.NoCompression)

	// File resources
	opts.SetMaxOpenFiles(10000)

	// Block-based table options with bloom filter
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	if config.BloomFilterBitsPerKey > 0 {
		bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(config.BloomFilterBitsPerKey)))
	}
	opts.SetBlockBasedTableFactory(bbto)

	// Logging
	opts.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)
	opts.SetMaxLogFileSize(20 * MB)
	opts.SetKeepLogFileNum(3)

	db, err := grocksdb.OpenDb(opts, config.OutputPath)
	if err != nil {
		opts.Destroy()
		return nil, nil, fmt.Errorf("failed to create RocksDB: %w", err)
	}

	return db, opts, nil
}

// =============================================================================
// Configuration Verification
// =============================================================================

func verifyOutputStoreConfig(config *Config) {
	optionsFilePath := findOptionsFile(config.OutputPath)
	if optionsFilePath == "" {
		log.Printf("⚠️  WARNING: Could not find OPTIONS file in output store")
		return
	}

	log.Printf("Reading configuration from: %s", optionsFilePath)
	log.Printf("")

	parsedConfig, err := parseOptionsFile(optionsFilePath)
	if err != nil {
		log.Printf("⚠️  WARNING: Failed to parse OPTIONS file: %v", err)
		return
	}

	// Display all parsed options
	log.Printf("OUTPUT STORE ROCKSDB CONFIGURATION:")
	log.Printf("--------------------------------------------------------------------------------")
	log.Printf("")

	// Compare and display each setting
	discrepancies := 0

	// write_buffer_size
	expectedWriteBuffer := int64(config.WriteBufferSizeMB * MB)
	match := parsedConfig.WriteBufferSize == expectedWriteBuffer
	status := "✓"
	if !match {
		status = "✗ MISMATCH"
		discrepancies++
	}
	log.Printf("  write_buffer_size:                    %s (expected: %s) %s",
		helpers.FormatBytes(parsedConfig.WriteBufferSize),
		helpers.FormatBytes(expectedWriteBuffer),
		status)

	// max_write_buffer_number
	match = parsedConfig.MaxWriteBufferNumber == config.MaxWriteBufferNumber
	status = "✓"
	if !match {
		status = "✗ MISMATCH"
		discrepancies++
	}
	log.Printf("  max_write_buffer_number:              %d (expected: %d) %s",
		parsedConfig.MaxWriteBufferNumber,
		config.MaxWriteBufferNumber,
		status)

	// min_write_buffer_number_to_merge
	match = parsedConfig.MinWriteBufferNumberToMerge == config.MinWriteBufferNumberToMerge
	status = "✓"
	if !match {
		status = "✗ MISMATCH"
		discrepancies++
	}
	log.Printf("  min_write_buffer_number_to_merge:     %d (expected: %d) %s",
		parsedConfig.MinWriteBufferNumberToMerge,
		config.MinWriteBufferNumberToMerge,
		status)

	// level0_file_num_compaction_trigger
	match = parsedConfig.Level0FileNumCompactionTrigger == config.L0CompactionTrigger
	status = "✓"
	if !match {
		status = "✗ MISMATCH"
		discrepancies++
	}
	log.Printf("  level0_file_num_compaction_trigger:   %d (expected: %d) %s",
		parsedConfig.Level0FileNumCompactionTrigger,
		config.L0CompactionTrigger,
		status)

	// level0_slowdown_writes_trigger
	match = parsedConfig.Level0SlowdownWritesTrigger == config.L0SlowdownWritesTrigger
	status = "✓"
	if !match {
		status = "✗ MISMATCH"
		discrepancies++
	}
	log.Printf("  level0_slowdown_writes_trigger:       %d (expected: %d) %s",
		parsedConfig.Level0SlowdownWritesTrigger,
		config.L0SlowdownWritesTrigger,
		status)

	// level0_stop_writes_trigger
	match = parsedConfig.Level0StopWritesTrigger == config.L0StopWritesTrigger
	status = "✓"
	if !match {
		status = "✗ MISMATCH"
		discrepancies++
	}
	log.Printf("  level0_stop_writes_trigger:           %d (expected: %d) %s",
		parsedConfig.Level0StopWritesTrigger,
		config.L0StopWritesTrigger,
		status)

	// target_file_size_base
	expectedTargetFileSize := int64(config.TargetFileSizeMB * MB)
	match = parsedConfig.TargetFileSizeBase == expectedTargetFileSize
	status = "✓"
	if !match {
		status = "✗ MISMATCH"
		discrepancies++
	}
	log.Printf("  target_file_size_base:                %s (expected: %s) %s",
		helpers.FormatBytes(parsedConfig.TargetFileSizeBase),
		helpers.FormatBytes(expectedTargetFileSize),
		status)

	// max_bytes_for_level_base
	expectedLevelBase := int64(config.MaxBytesForLevelBaseMB * MB)
	match = parsedConfig.MaxBytesForLevelBase == expectedLevelBase
	status = "✓"
	if !match {
		status = "✗ MISMATCH"
		discrepancies++
	}
	log.Printf("  max_bytes_for_level_base:             %s (expected: %s) %s",
		helpers.FormatBytes(parsedConfig.MaxBytesForLevelBase),
		helpers.FormatBytes(expectedLevelBase),
		status)

	// max_background_jobs
	match = parsedConfig.MaxBackgroundJobs == config.MaxBackgroundJobs
	status = "✓"
	if !match {
		status = "✗ MISMATCH"
		discrepancies++
	}
	log.Printf("  max_background_jobs:                  %d (expected: %d) %s",
		parsedConfig.MaxBackgroundJobs,
		config.MaxBackgroundJobs,
		status)

	// level_compaction_dynamic_level_bytes
	match = parsedConfig.LevelCompactionDynamicLevelBytes == config.LevelCompactionDynamicLevel
	status = "✓"
	if !match {
		status = "✗ MISMATCH"
		discrepancies++
	}
	log.Printf("  level_compaction_dynamic_level_bytes: %v (expected: %v) %s",
		parsedConfig.LevelCompactionDynamicLevelBytes,
		config.LevelCompactionDynamicLevel,
		status)

	log.Printf("")
	log.Printf("--------------------------------------------------------------------------------")
	if discrepancies > 0 {
		log.Printf("⚠️  WARNING: %d configuration discrepancies found!", discrepancies)
	} else {
		log.Printf("✓ All configurations match expected values")
	}
}

func findOptionsFile(dbPath string) string {
	// Find the latest OPTIONS file
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		return ""
	}

	var latestOptions string
	var latestNum int

	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "OPTIONS-") {
			// Extract number from OPTIONS-XXXXXX
			numStr := strings.TrimPrefix(name, "OPTIONS-")
			num, err := strconv.Atoi(numStr)
			if err == nil && num > latestNum {
				latestNum = num
				latestOptions = filepath.Join(dbPath, name)
			}
		}
	}

	return latestOptions
}

func parseOptionsFile(path string) (*RocksDBConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := &RocksDBConfig{}
	scanner := bufio.NewScanner(file)

	// Regular expressions for parsing
	reWriteBufferSize := regexp.MustCompile(`write_buffer_size=(\d+)`)
	reMaxWriteBufferNumber := regexp.MustCompile(`max_write_buffer_number=(\d+)`)
	reMinWriteBufferNumberToMerge := regexp.MustCompile(`min_write_buffer_number_to_merge=(\d+)`)
	reLevel0CompactionTrigger := regexp.MustCompile(`level0_file_num_compaction_trigger=(\d+)`)
	reLevel0SlowdownTrigger := regexp.MustCompile(`level0_slowdown_writes_trigger=(\d+)`)
	reLevel0StopTrigger := regexp.MustCompile(`level0_stop_writes_trigger=(\d+)`)
	reTargetFileSizeBase := regexp.MustCompile(`target_file_size_base=(\d+)`)
	reMaxBytesForLevelBase := regexp.MustCompile(`max_bytes_for_level_base=(\d+)`)
	reMaxBackgroundJobs := regexp.MustCompile(`max_background_jobs=(\d+)`)
	reDynamicLevelBytes := regexp.MustCompile(`level_compaction_dynamic_level_bytes=(true|false)`)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if matches := reWriteBufferSize.FindStringSubmatch(line); matches != nil {
			config.WriteBufferSize, _ = strconv.ParseInt(matches[1], 10, 64)
		}
		if matches := reMaxWriteBufferNumber.FindStringSubmatch(line); matches != nil {
			config.MaxWriteBufferNumber, _ = strconv.Atoi(matches[1])
		}
		if matches := reMinWriteBufferNumberToMerge.FindStringSubmatch(line); matches != nil {
			config.MinWriteBufferNumberToMerge, _ = strconv.Atoi(matches[1])
		}
		if matches := reLevel0CompactionTrigger.FindStringSubmatch(line); matches != nil {
			config.Level0FileNumCompactionTrigger, _ = strconv.Atoi(matches[1])
		}
		if matches := reLevel0SlowdownTrigger.FindStringSubmatch(line); matches != nil {
			config.Level0SlowdownWritesTrigger, _ = strconv.Atoi(matches[1])
		}
		if matches := reLevel0StopTrigger.FindStringSubmatch(line); matches != nil {
			config.Level0StopWritesTrigger, _ = strconv.Atoi(matches[1])
		}
		if matches := reTargetFileSizeBase.FindStringSubmatch(line); matches != nil {
			config.TargetFileSizeBase, _ = strconv.ParseInt(matches[1], 10, 64)
		}
		if matches := reMaxBytesForLevelBase.FindStringSubmatch(line); matches != nil {
			config.MaxBytesForLevelBase, _ = strconv.ParseInt(matches[1], 10, 64)
		}
		if matches := reMaxBackgroundJobs.FindStringSubmatch(line); matches != nil {
			config.MaxBackgroundJobs, _ = strconv.Atoi(matches[1])
		}
		if matches := reDynamicLevelBytes.FindStringSubmatch(line); matches != nil {
			config.LevelCompactionDynamicLevelBytes = matches[1] == "true"
		}
	}

	return config, scanner.Err()
}

// =============================================================================
// Data Copy Function
// =============================================================================

func copyData(inputDB *grocksdb.DB, inputRO *grocksdb.ReadOptions, outputDB *grocksdb.DB, config *Config, stats *CopyStats) error {
	// Create iterator for input store
	it := inputDB.NewIterator(inputRO)
	defer it.Close()

	// Create write options for output store
	wo := grocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(config.DisableWAL)
	defer wo.Destroy()

	// Start iteration
	it.SeekToFirst()

	for it.Valid() {
		// Create a new batch
		batch := grocksdb.NewWriteBatch()
		batchKeyCount := 0
		batchBytesRead := int64(0)

		batchStart := time.Now()

		// Fill the batch
		for it.Valid() && batchKeyCount < config.BatchSize {
			key := it.Key()
			value := it.Value()

			keyData := key.Data()
			valueData := value.Data()

			// Track sizes
			keySize := int64(len(keyData))
			valueSize := int64(len(valueData))

			stats.TotalKeysBytes += keySize
			stats.TotalValuesBytes += valueSize
			batchBytesRead += keySize + valueSize

			// Add to batch
			batch.Put(keyData, valueData)

			// Free slices
			key.Free()
			value.Free()

			batchKeyCount++
			it.Next()
		}

		// Write batch
		writeStart := time.Now()
		if err := outputDB.Write(wo, batch); err != nil {
			batch.Destroy()
			return fmt.Errorf("failed to write batch: %w", err)
		}
		writeTime := time.Since(writeStart)

		batch.Destroy()

		// Update stats
		stats.TotalKeysProcessed += int64(batchKeyCount)
		stats.TotalBytesRead += batchBytesRead
		stats.CurrentBatch++
		stats.BatchWriteTimes = append(stats.BatchWriteTimes, writeTime)

		// Log batch completion
		batchDuration := time.Since(batchStart)
		log.Printf("Batch %s/%s complete: %s keys in %s (write: %s, %.2f MB/s)",
			helpers.FormatNumber(stats.CurrentBatch),
			helpers.FormatNumber(stats.TotalBatches),
			helpers.FormatNumber(int64(batchKeyCount)),
			helpers.FormatDuration(batchDuration),
			helpers.FormatDuration(writeTime),
			float64(batchBytesRead)/float64(MB)/writeTime.Seconds())

		// Report progress at each percentage point
		reportProgress(stats, outputDB)
	}

	if err := it.Err(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	return nil
}

// =============================================================================
// Progress Reporting
// =============================================================================

func reportProgress(stats *CopyStats, outputDB *grocksdb.DB) {
	if stats.TotalKeysEstimate <= 0 {
		return
	}

	currentPercent := int((stats.TotalKeysProcessed * 100) / stats.TotalKeysEstimate)

	// Only report at each new percentage point
	if currentPercent <= stats.LastReportedPercent {
		return
	}

	elapsed := time.Since(stats.StartTime)
	keysPerSec := float64(stats.TotalKeysProcessed) / elapsed.Seconds()
	remaining := stats.TotalKeysEstimate - stats.TotalKeysProcessed

	var eta time.Duration
	if keysPerSec > 0 {
		eta = time.Duration(float64(remaining)/keysPerSec) * time.Second
	}

	// Get current output store size
	outputSize := getRocksDBSSTSize(outputDB)

	// Calculate overhead
	overheadPercent := float64(0)
	if stats.TotalBytesRead > 0 {
		overheadPercent = (float64(outputSize) - float64(stats.TotalBytesRead)) / float64(stats.TotalBytesRead) * 100
	}

	// Calculate average batch write time
	var avgWriteTime time.Duration
	if len(stats.BatchWriteTimes) > 0 {
		var totalWriteTime time.Duration
		for _, t := range stats.BatchWriteTimes {
			totalWriteTime += t
		}
		avgWriteTime = totalWriteTime / time.Duration(len(stats.BatchWriteTimes))
	}

	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("    PROGRESS: %d%% | Batch %s/%s",
		currentPercent,
		helpers.FormatNumber(stats.CurrentBatch),
		helpers.FormatNumber(stats.TotalBatches))
	log.Printf("================================================================================")
	log.Printf("")
	log.Printf("  Records ingested:    %s", helpers.FormatNumber(stats.TotalKeysProcessed))
	log.Printf("  Records remaining:   %s", helpers.FormatNumber(remaining))
	log.Printf("  Processing rate:     %.2f keys/sec", keysPerSec)
	log.Printf("  ETA:                 %s", helpers.FormatDuration(eta))
	log.Printf("  Elapsed:             %s", helpers.FormatDuration(elapsed))
	log.Printf("")
	log.Printf("  Raw data ingested:   %s", helpers.FormatBytes(stats.TotalBytesRead))
	log.Printf("    - Keys:            %s", helpers.FormatBytes(stats.TotalKeysBytes))
	log.Printf("    - Values:          %s", helpers.FormatBytes(stats.TotalValuesBytes))
	log.Printf("  Output store size:   %s", helpers.FormatBytes(outputSize))
	log.Printf("  RocksDB overhead:    %.2f%%", overheadPercent)
	log.Printf("")
	log.Printf("  Avg batch write:     %s", helpers.FormatDuration(avgWriteTime))
	log.Printf("================================================================================")
	log.Printf("")

	stats.LastReportedPercent = currentPercent
	stats.LastReportTime = time.Now()
}

// =============================================================================
// Statistics and Logging Functions
// =============================================================================

func printConfigSummary(config *Config) {
	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("                     ROCKSDB STORE COPY CONFIGURATION")
	log.Printf("================================================================================")
	log.Printf("")
	log.Printf("PATHS:")
	log.Printf("  Input:               %s", config.InputPath)
	log.Printf("  Output:              %s", config.OutputPath)
	log.Printf("")

	mode := "BACKFILL"
	if config.IsStreaming {
		mode = "STREAMING"
	}
	log.Printf("MODE: %s", mode)
	log.Printf("")

	log.Printf("ROCKSDB SETTINGS:")
	log.Printf("  write_buffer_size_mb:                 %d MB", config.WriteBufferSizeMB)
	log.Printf("  max_write_buffer_number:              %d", config.MaxWriteBufferNumber)
	log.Printf("  min_write_buffer_number_to_merge:     %d", config.MinWriteBufferNumberToMerge)
	log.Printf("  RAM budget (memtables):               %d MB", config.WriteBufferSizeMB*config.MaxWriteBufferNumber)
	log.Printf("  Expected flush file size:             %d MB", config.WriteBufferSizeMB*config.MinWriteBufferNumberToMerge)
	log.Printf("")
	log.Printf("  l0_compaction_trigger:                %d", config.L0CompactionTrigger)
	log.Printf("  l0_slowdown_writes_trigger:           %d", config.L0SlowdownWritesTrigger)
	log.Printf("  l0_stop_writes_trigger:               %d", config.L0StopWritesTrigger)
	log.Printf("")
	log.Printf("  target_file_size_mb:                  %d MB", config.TargetFileSizeMB)
	log.Printf("  max_bytes_for_level_base_mb:          %d MB", config.MaxBytesForLevelBaseMB)
	log.Printf("  max_background_jobs:                  %d", config.MaxBackgroundJobs)
	log.Printf("  disable_wal:                          %v", config.DisableWAL)
	log.Printf("  level_compaction_dynamic_level_bytes: %v", config.LevelCompactionDynamicLevel)
	log.Printf("  bloom_filter_bits_per_key:            %d", config.BloomFilterBitsPerKey)
	log.Printf("")
	log.Printf("BATCH SETTINGS:")
	log.Printf("  batch_size:                           %d keys", config.BatchSize)
	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("")
}

func printStoreStats(db *grocksdb.DB, name string) {
	log.Printf("%s:", name)
	log.Printf("")

	// SST file size
	sstSize := getRocksDBSSTSize(db)
	log.Printf("  Total SST files size:    %s", helpers.FormatBytes(sstSize))

	// Estimated keys
	estimatedKeys := getEstimatedKeyCount(db)
	log.Printf("  Estimated keys:          %s", helpers.FormatNumber(estimatedKeys))

	// Memtable size
	memtableSize := getMemtableSize(db)
	log.Printf("  Current memtable size:   %s", helpers.FormatBytes(memtableSize))

	// Level statistics
	log.Printf("")
	log.Printf("  Files by level:")
	for i := 0; i <= 6; i++ {
		prop := fmt.Sprintf("rocksdb.num-files-at-level%d", i)
		numFiles := db.GetProperty(prop)
		if numFiles != "" && numFiles != "0" {
			log.Printf("    L%d: %s files", i, numFiles)
		}
	}
	log.Printf("")
}

func printCompactionStatus(db *grocksdb.DB) {
	runningCompactions := db.GetProperty("rocksdb.num-running-compactions")
	pendingCompaction := db.GetProperty("rocksdb.compaction-pending")
	runningFlushes := db.GetProperty("rocksdb.num-running-flushes")

	log.Printf("COMPACTION STATUS:")
	log.Printf("  Running compactions: %s", runningCompactions)
	log.Printf("  Compaction pending:  %s", pendingCompaction)
	log.Printf("  Running flushes:     %s", runningFlushes)
	log.Printf("")
}

func printFinalSummary(stats *CopyStats, config *Config, inputSize int64, outputDB *grocksdb.DB, compactionDuration time.Duration) {
	elapsed := time.Since(stats.StartTime)
	outputSize := getRocksDBSSTSize(outputDB)

	log.Printf("")
	log.Printf("################################################################################")
	log.Printf("                              FINAL SUMMARY")
	log.Printf("################################################################################")
	log.Printf("")
	log.Printf("COPY STATISTICS:")
	log.Printf("  Total keys copied:       %s", helpers.FormatNumber(stats.TotalKeysProcessed))
	log.Printf("  Total batches:           %s", helpers.FormatNumber(stats.CurrentBatch))
	log.Printf("  Total time:              %s", helpers.FormatDuration(elapsed))
	log.Printf("  Compaction time:         %s", helpers.FormatDuration(compactionDuration))
	log.Printf("  Copy time (excl. compact): %s", helpers.FormatDuration(elapsed-compactionDuration))
	log.Printf("")

	if elapsed.Seconds() > 0 {
		keysPerSec := float64(stats.TotalKeysProcessed) / elapsed.Seconds()
		log.Printf("  Average rate:            %.2f keys/sec", keysPerSec)
	}
	log.Printf("")

	log.Printf("DATA SIZES:")
	log.Printf("  Input store size:        %s", helpers.FormatBytes(inputSize))
	log.Printf("  Raw data copied:         %s", helpers.FormatBytes(stats.TotalBytesRead))
	log.Printf("    - Keys:                %s", helpers.FormatBytes(stats.TotalKeysBytes))
	log.Printf("    - Values:              %s", helpers.FormatBytes(stats.TotalValuesBytes))
	log.Printf("  Output store size:       %s", helpers.FormatBytes(outputSize))
	log.Printf("")

	// Calculate overheads
	if stats.TotalBytesRead > 0 {
		overheadVsRaw := (float64(outputSize) - float64(stats.TotalBytesRead)) / float64(stats.TotalBytesRead) * 100
		log.Printf("  RocksDB overhead vs raw: %.2f%%", overheadVsRaw)
	}
	if inputSize > 0 {
		sizeChange := (float64(outputSize) - float64(inputSize)) / float64(inputSize) * 100
		log.Printf("  Size change vs input:    %.2f%%", sizeChange)
	}
	log.Printf("")

	// Average batch write time
	if len(stats.BatchWriteTimes) > 0 {
		var totalWriteTime time.Duration
		var minWriteTime, maxWriteTime time.Duration
		minWriteTime = stats.BatchWriteTimes[0]
		maxWriteTime = stats.BatchWriteTimes[0]

		for _, t := range stats.BatchWriteTimes {
			totalWriteTime += t
			if t < minWriteTime {
				minWriteTime = t
			}
			if t > maxWriteTime {
				maxWriteTime = t
			}
		}
		avgWriteTime := totalWriteTime / time.Duration(len(stats.BatchWriteTimes))

		log.Printf("BATCH WRITE TIMING:")
		log.Printf("  Average:                 %s", helpers.FormatDuration(avgWriteTime))
		log.Printf("  Min:                     %s", helpers.FormatDuration(minWriteTime))
		log.Printf("  Max:                     %s", helpers.FormatDuration(maxWriteTime))
		log.Printf("  Total write time:        %s", helpers.FormatDuration(totalWriteTime))
		log.Printf("")
	}

	log.Printf("OUTPUT PATHS:")
	log.Printf("  Output store:            %s", config.OutputPath)
	log.Printf("")
	log.Printf("################################################################################")
}

// =============================================================================
// Helper Functions
// =============================================================================

func getRocksDBSSTSize(db *grocksdb.DB) int64 {
	if db == nil {
		return 0
	}
	sizeStr := db.GetProperty("rocksdb.total-sst-files-size")
	size, _ := strconv.ParseInt(sizeStr, 10, 64)
	return size
}

func getEstimatedKeyCount(db *grocksdb.DB) int64 {
	if db == nil {
		return 0
	}
	countStr := db.GetProperty("rocksdb.estimate-num-keys")
	count, _ := strconv.ParseInt(countStr, 10, 64)
	return count
}

func getMemtableSize(db *grocksdb.DB) int64 {
	if db == nil {
		return 0
	}
	sizeStr := db.GetProperty("rocksdb.cur-size-all-mem-tables")
	size, _ := strconv.ParseInt(sizeStr, 10, 64)
	return size
}
