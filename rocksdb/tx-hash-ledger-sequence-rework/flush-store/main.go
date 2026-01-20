// =============================================================================
// Flush Store Utility
// =============================================================================
//
// This utility opens an existing txHash→ledgerSeq RocksDB store in read-write
// mode, flushes all MemTables to SST files (which cleans up WAL files), then
// closes the store. NO COMPACTION is performed.
//
// This is useful after ingestion completes to ensure all data is in SST files
// and WAL files are cleaned up, enabling fast read-only opens for benchmarking.
//
// =============================================================================
// USAGE
// =============================================================================
//
// Basic flush:
//   flush-store --store /path/to/rocksdb/store
//
// Dry run (show stats without flushing):
//   flush-store --store /path/to/rocksdb/store --dry-run
//
// With more background jobs for faster flush:
//   flush-store --store /path/to/rocksdb/store --max-background-jobs 32
//
// =============================================================================

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	txhashrework "github.com/karthikiyer56/stellar-full-history-ingestion/rocksdb/tx-hash-ledger-sequence-rework"
	"github.com/linxGnu/grocksdb"
)

const (
	MB                       = 1024 * 1024
	DefaultBlockCacheMB      = 64
	DefaultMaxBackgroundJobs = 16
	EntrySize                = 36 // 32-byte key + 4-byte value
)

// FileInfo holds information about a file
type FileInfo struct {
	Name string
	Size int64
}

func main() {
	var (
		storePath         string
		blockCacheMB      int
		maxBackgroundJobs int
		logFile           string
		dryRun            bool
		verbose           bool
		showHelp          bool
	)

	flag.StringVar(&storePath, "store", "", "Path to existing RocksDB store (required)")
	flag.IntVar(&blockCacheMB, "block-cache", DefaultBlockCacheMB, "Block cache size in MB")
	flag.IntVar(&maxBackgroundJobs, "max-background-jobs", DefaultMaxBackgroundJobs, "Max background jobs for RocksDB flush")
	flag.StringVar(&logFile, "log-file", "", "Output file for logs (default: stdout)")
	flag.BoolVar(&dryRun, "dry-run", false, "Show stats without flushing")
	flag.BoolVar(&verbose, "verbose", false, "Show detailed flush progress")
	flag.BoolVar(&showHelp, "help", false, "Show help message")

	flag.Parse()

	if showHelp {
		printUsage()
		os.Exit(0)
	}

	if storePath == "" {
		fmt.Fprintf(os.Stderr, "ERROR: --store is required\n\n")
		printUsage()
		os.Exit(1)
	}

	// Get absolute path
	absStorePath, err := filepath.Abs(storePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Failed to get absolute path: %v\n", err)
		os.Exit(1)
	}

	// Validate store exists
	if _, err := os.Stat(absStorePath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "ERROR: Store path does not exist: %s\n", absStorePath)
		os.Exit(1)
	}

	// Validate max background jobs
	if maxBackgroundJobs < 1 {
		maxBackgroundJobs = 1
	}
	if maxBackgroundJobs > 64 {
		maxBackgroundJobs = 64
	}

	// Setup logger
	logger, err := txhashrework.NewLogger(txhashrework.LoggerConfig{
		LogFilePath:   logFile,
		ErrorFilePath: "",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	// Run
	if dryRun {
		if err := runDryRun(logger, absStorePath); err != nil {
			logger.Error("Dry run failed: %v", err)
			os.Exit(1)
		}
	} else {
		if err := runFlush(logger, absStorePath, blockCacheMB, maxBackgroundJobs, verbose); err != nil {
			logger.Error("Flush failed: %v", err)
			os.Exit(1)
		}
	}
}

// =============================================================================
// Dry Run
// =============================================================================

func runDryRun(logger *txhashrework.Logger, storePath string) error {
	logger.Info("")
	logger.Info("================================================================================")
	logger.Info("                     FLUSH STORE UTILITY (DRY RUN)")
	logger.Info("================================================================================")
	logger.Info("")
	logger.Info("Store Path: %s", storePath)
	logger.Info("")

	// Scan directory
	logger.Info("Scanning store directory...")
	scanStart := time.Now()
	sstCount, walCount, walBytes, walFiles, err := countFiles(storePath)
	scanDuration := time.Since(scanStart)
	if err != nil {
		return fmt.Errorf("failed to scan directory: %w", err)
	}

	logger.Info("Scan complete in %s", helpers.FormatDuration(scanDuration))
	logger.Info("")

	// Estimate WAL entries
	estimatedEntries := walBytes / EntrySize

	logger.Info("CURRENT STATE:")
	logger.Info("  SST Files:         %s", helpers.FormatNumber(int64(sstCount)))
	logger.Info("  WAL Files:         %s", helpers.FormatNumber(int64(walCount)))
	logger.Info("  WAL Size:          %s", formatBytes(walBytes))
	logger.Info("  Estimated Entries: %s (in WAL)", helpers.FormatNumber(estimatedEntries))
	logger.Info("")

	if walCount > 0 && len(walFiles) > 0 {
		logger.Info("WAL FILE DETAILS (largest first):")
		// Sort by size descending
		sort.Slice(walFiles, func(i, j int) bool {
			return walFiles[i].Size > walFiles[j].Size
		})
		// Show top 10
		showCount := walCount
		if showCount > 10 {
			showCount = 10
		}
		for i := 0; i < showCount; i++ {
			logger.Info("  %-20s %s", walFiles[i].Name, formatBytes(walFiles[i].Size))
		}
		if walCount > 10 {
			logger.Info("  ... and %d more files", walCount-10)
		}
		logger.Info("")
	}

	logger.Info("DRY RUN: No changes made. Run without --dry-run to flush.")
	logger.Info("================================================================================")
	logger.Info("")

	return nil
}

// =============================================================================
// Flush
// =============================================================================

func runFlush(logger *txhashrework.Logger, storePath string, blockCacheMB, maxBackgroundJobs int, verbose bool) error {
	totalStart := time.Now()

	logger.Info("")
	logger.Info("================================================================================")
	logger.Info("                         FLUSH STORE UTILITY")
	logger.Info("================================================================================")
	logger.Info("")
	logger.Info("Store Path:          %s", storePath)
	logger.Info("Block Cache:         %d MB", blockCacheMB)
	logger.Info("Max Background Jobs: %d", maxBackgroundJobs)
	logger.Info("")

	// Count files before
	logger.Info("Counting files before flush...")
	countStart := time.Now()
	sstBefore, walBefore, walBytesBefore, _, err := countFiles(storePath)
	countDuration := time.Since(countStart)
	if err != nil {
		return fmt.Errorf("failed to count files: %w", err)
	}

	estimatedEntriesBefore := walBytesBefore / EntrySize

	logger.Info("Count complete in %s", helpers.FormatDuration(countDuration))
	logger.Info("")
	logger.Info("BEFORE FLUSH:")
	logger.Info("  SST Files:         %s", helpers.FormatNumber(int64(sstBefore)))
	logger.Info("  WAL Files:         %s", helpers.FormatNumber(int64(walBefore)))
	logger.Info("  WAL Size:          %s", formatBytes(walBytesBefore))
	logger.Info("  Estimated Entries: %s (in WAL)", helpers.FormatNumber(estimatedEntriesBefore))
	logger.Info("")

	// Open store
	logger.Info("Opening store (this may take a while if WAL replay is needed)...")
	openStart := time.Now()
	store, err := openStoreForFlush(storePath, blockCacheMB, maxBackgroundJobs, logger)
	openDuration := time.Since(openStart)
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}
	defer store.Close()

	logger.Info("Store opened in %s", helpers.FormatDuration(openDuration))
	logger.Info("")

	// Flush all CFs
	logger.Info("Flushing all 16 column families...")
	if verbose {
		logger.Info("  (RocksDB will parallelize internally using %d background jobs)", maxBackgroundJobs)
	}
	logger.Info("")

	flushStart := time.Now()
	err = flushAll(store, verbose, logger)
	flushDuration := time.Since(flushStart)

	if err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	logger.Info("Flush complete in %s", helpers.FormatDuration(flushDuration))
	logger.Info("")

	// Close store
	logger.Info("Closing store...")
	closeStart := time.Now()
	store.Close()
	closeDuration := time.Since(closeStart)
	logger.Info("Store closed in %s", helpers.FormatDuration(closeDuration))
	logger.Info("")

	// Count files after
	logger.Info("Counting files after flush...")
	sstAfter, walAfter, walBytesAfter, _, err := countFiles(storePath)
	if err != nil {
		logger.Info("Warning: Failed to count files after flush: %v", err)
	} else {
		logger.Info("")
		logger.Info("AFTER FLUSH:")
		logger.Info("  SST Files:   %s (was %s)", helpers.FormatNumber(int64(sstAfter)), helpers.FormatNumber(int64(sstBefore)))
		logger.Info("  WAL Files:   %s (was %s)", helpers.FormatNumber(int64(walAfter)), helpers.FormatNumber(int64(walBefore)))
		logger.Info("  WAL Size:    %s (was %s)", formatBytes(walBytesAfter), formatBytes(walBytesBefore))
	}

	totalDuration := time.Since(totalStart)

	// Print summary
	logger.Info("")
	logger.Info("================================================================================")
	logger.Info("                              SUMMARY")
	logger.Info("================================================================================")
	logger.Info("  Count Files:   %s", helpers.FormatDuration(countDuration))
	logger.Info("  Open Store:    %s", helpers.FormatDuration(openDuration))
	logger.Info("  Flush All:     %s", helpers.FormatDuration(flushDuration))
	logger.Info("  Close Store:   %s", helpers.FormatDuration(closeDuration))
	logger.Info("  ─────────────────────")
	logger.Info("  TOTAL:         %s", helpers.FormatDuration(totalDuration))
	logger.Info("================================================================================")
	logger.Info("")

	return nil
}

// =============================================================================
// Flush
// =============================================================================

func flushAll(store *FlushStore, verbose bool, logger *txhashrework.Logger) error {
	cfNames := txhashrework.ColumnFamilyNames // 16 CFs: 0-9, a-f

	// Collect all CF handles (skip index 0 which is "default")
	cfHandles := make([]*grocksdb.ColumnFamilyHandle, len(cfNames))
	for i := range cfNames {
		cfHandles[i] = store.CFHandles[i+1] // +1 because index 0 is "default"
	}

	// Create flush options
	flushOpts := grocksdb.NewDefaultFlushOptions()
	flushOpts.SetWait(true)
	defer flushOpts.Destroy()

	if verbose {
		logger.Info("  Calling FlushCFs for all %d column families...", len(cfNames))
	}

	// Flush all CFs in one call - RocksDB handles parallelization internally
	err := store.DB.FlushCFs(cfHandles, flushOpts)

	if verbose {
		if err != nil {
			logger.Error("  FlushCFs failed: %v", err)
		} else {
			logger.Info("  FlushCFs completed successfully")
		}
	}

	return err
}

// =============================================================================
// FlushStore - Minimal store for flushing
// =============================================================================

type FlushStore struct {
	DB         *grocksdb.DB
	Opts       *grocksdb.Options
	CFHandles  []*grocksdb.ColumnFamilyHandle
	CFOpts     []*grocksdb.Options
	BlockCache *grocksdb.Cache
}

func openStoreForFlush(path string, blockCacheMB, maxBackgroundJobs int, logger *txhashrework.Logger) (*FlushStore, error) {
	// Create minimal block cache
	var blockCache *grocksdb.Cache
	if blockCacheMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(blockCacheMB * MB))
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false) // Must exist
	opts.SetCreateIfMissingColumnFamilies(false)
	opts.SetErrorIfExists(false)
	opts.SetMaxOpenFiles(-1)
	opts.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)
	opts.SetMaxBackgroundJobs(maxBackgroundJobs) // Control flush parallelism

	// Prepare column family names (16 CFs + default)
	cfNames := []string{"default"}
	cfNames = append(cfNames, txhashrework.ColumnFamilyNames...)

	// Create minimal options for each CF
	cfOptsList := make([]*grocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts := grocksdb.NewDefaultOptions()
		if blockCache != nil {
			bbto := grocksdb.NewDefaultBlockBasedTableOptions()
			bbto.SetBlockCache(blockCache)
			cfOpts.SetBlockBasedTableFactory(bbto)
		}
		cfOptsList[i] = cfOpts
	}

	// Open database (this will replay WAL if needed)
	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(opts, path, cfNames, cfOptsList)
	if err != nil {
		opts.Destroy()
		for _, cfOpt := range cfOptsList {
			if cfOpt != nil {
				cfOpt.Destroy()
			}
		}
		if blockCache != nil {
			blockCache.Destroy()
		}
		return nil, fmt.Errorf("failed to open store: %w", err)
	}

	return &FlushStore{
		DB:         db,
		Opts:       opts,
		CFHandles:  cfHandles,
		CFOpts:     cfOptsList,
		BlockCache: blockCache,
	}, nil
}

func (s *FlushStore) Close() {
	for _, cfHandle := range s.CFHandles {
		if cfHandle != nil {
			cfHandle.Destroy()
		}
	}
	if s.DB != nil {
		s.DB.Close()
	}
	if s.Opts != nil {
		s.Opts.Destroy()
	}
	for _, cfOpt := range s.CFOpts {
		if cfOpt != nil {
			cfOpt.Destroy()
		}
	}
	if s.BlockCache != nil {
		s.BlockCache.Destroy()
	}
}

// =============================================================================
// File Counting Helpers
// =============================================================================

func countFiles(storePath string) (sstCount int, walCount int, walBytes int64, walFiles []FileInfo, err error) {
	entries, err := os.ReadDir(storePath)
	if err != nil {
		return 0, 0, 0, nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		if strings.HasSuffix(name, ".sst") {
			sstCount++
		} else if strings.HasSuffix(name, ".log") {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			walCount++
			walBytes += info.Size()
			walFiles = append(walFiles, FileInfo{Name: name, Size: info.Size()})
		}
	}

	return sstCount, walCount, walBytes, walFiles, nil
}

func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}

// =============================================================================
// Usage
// =============================================================================

func printUsage() {
	fmt.Println("flush-store - Flush MemTables to SST files (cleans up WAL)")
	fmt.Println()
	fmt.Println("This utility opens an existing txHash→ledgerSeq RocksDB store,")
	fmt.Println("flushes all MemTables to SST files, and closes. NO COMPACTION.")
	fmt.Println()
	fmt.Println("USAGE:")
	fmt.Println("  flush-store --store PATH [OPTIONS]")
	fmt.Println()
	fmt.Println("OPTIONS:")
	fmt.Println("  --store PATH              Path to existing RocksDB store (required)")
	fmt.Println("  --block-cache N           Block cache size in MB (default: 64)")
	fmt.Println("  --max-background-jobs N   Max RocksDB background jobs for flush (default: 16)")
	fmt.Println("  --log-file FILE           Output file for logs (default: stdout)")
	fmt.Println("  --dry-run                 Show stats without flushing")
	fmt.Println("  --verbose                 Show detailed flush progress")
	fmt.Println("  --help                    Show this help message")
	fmt.Println()
	fmt.Println("EXAMPLES:")
	fmt.Println("  # Dry run to see current state")
	fmt.Println("  flush-store --store /data/tx-hash-store --dry-run")
	fmt.Println()
	fmt.Println("  # Flush with default settings")
	fmt.Println("  flush-store --store /data/tx-hash-store")
	fmt.Println()
	fmt.Println("  # Flush with more parallelism")
	fmt.Println("  flush-store --store /data/tx-hash-store --max-background-jobs 32")
}
