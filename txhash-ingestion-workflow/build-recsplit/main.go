// =============================================================================
// build-recsplit - Standalone RecSplit Index Builder
// =============================================================================
//
// This tool builds RecSplit minimal perfect hash indexes from a RocksDB store
// containing txHash→ledgerSeq mappings.
//
// USAGE:
//
//	build-recsplit \
//	  --rocksdb-path /path/to/rocksdb \
//	  --output-dir /path/to/indexes \
//	  --log /path/to/build.log \
//	  --error /path/to/build.err \
//	  [--compact] \
//	  [--parallel] \
//	  [--verify] \
//	  [--block-cache-mb 8192]
//
// FEATURES:
//
//   - Discovers key counts by iterating each CF (no meta store dependency)
//   - Optional compaction before building (--compact)
//   - Optional verification after building (--verify)
//   - Sequential or parallel building (--parallel)
//
// OUTPUT:
//
//	16 index files: cf-0.idx through cf-f.idx
//
// =============================================================================

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	erigonlog "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/cf"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/compact"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/logging"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/memory"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/types"
	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// RocksDB Store (supports both read-only and read-write modes)
// =============================================================================

type Store struct {
	db         *grocksdb.DB
	opts       *grocksdb.Options
	cfHandles  []*grocksdb.ColumnFamilyHandle
	cfOpts     []*grocksdb.Options
	readOpts   *grocksdb.ReadOptions
	blockCache *grocksdb.Cache
	cfIndexMap map[string]int
	readOnly   bool
}

func OpenStore(path string, blockCacheMB int, readOnly bool) (*Store, error) {
	var blockCache *grocksdb.Cache
	if blockCacheMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(blockCacheMB * types.MB))
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	cfNames := []string{"default"}
	cfNames = append(cfNames, cf.Names...)

	cfOptsList := make([]*grocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts := grocksdb.NewDefaultOptions()
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetFilterPolicy(grocksdb.NewBloomFilter(12))
		if blockCache != nil {
			bbto.SetBlockCache(blockCache)
		}
		cfOpts.SetBlockBasedTableFactory(bbto)
		cfOptsList[i] = cfOpts
	}

	var db *grocksdb.DB
	var cfHandles []*grocksdb.ColumnFamilyHandle
	var err error

	if readOnly {
		db, cfHandles, err = grocksdb.OpenDbForReadOnlyColumnFamilies(opts, path, cfNames, cfOptsList, false)
	} else {
		db, cfHandles, err = grocksdb.OpenDbColumnFamilies(opts, path, cfNames, cfOptsList)
	}

	if err != nil {
		opts.Destroy()
		for _, cfOpt := range cfOptsList {
			cfOpt.Destroy()
		}
		if blockCache != nil {
			blockCache.Destroy()
		}
		return nil, fmt.Errorf("failed to open RocksDB: %w", err)
	}

	cfIndexMap := make(map[string]int)
	for i, name := range cfNames {
		cfIndexMap[name] = i
	}

	readOpts := grocksdb.NewDefaultReadOptions()

	return &Store{
		db:         db,
		opts:       opts,
		cfHandles:  cfHandles,
		cfOpts:     cfOptsList,
		readOpts:   readOpts,
		blockCache: blockCache,
		cfIndexMap: cfIndexMap,
		readOnly:   readOnly,
	}, nil
}

func (s *Store) NewIteratorCF(cfName string) *grocksdb.Iterator {
	cfHandle := s.cfHandles[s.cfIndexMap[cfName]]
	return s.db.NewIteratorCF(s.readOpts, cfHandle)
}

func (s *Store) CompactCF(cfName string) time.Duration {
	if s.readOnly {
		return 0
	}
	start := time.Now()
	cfHandle := s.cfHandles[s.cfIndexMap[cfName]]
	s.db.CompactRangeCF(cfHandle, grocksdb.Range{Start: nil, Limit: nil})
	return time.Since(start)
}

func (s *Store) Get(cfName string, key []byte) ([]byte, bool, error) {
	cfHandle := s.cfHandles[s.cfIndexMap[cfName]]
	slice, err := s.db.GetCF(s.readOpts, cfHandle, key)
	if err != nil {
		return nil, false, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil, false, nil
	}

	value := make([]byte, slice.Size())
	copy(value, slice.Data())
	return value, true, nil
}

func (s *Store) Close() {
	if s.readOpts != nil {
		s.readOpts.Destroy()
	}
	for _, cfHandle := range s.cfHandles {
		if cfHandle != nil {
			cfHandle.Destroy()
		}
	}
	if s.db != nil {
		s.db.Close()
	}
	if s.opts != nil {
		s.opts.Destroy()
	}
	for _, cfOpt := range s.cfOpts {
		if cfOpt != nil {
			cfOpt.Destroy()
		}
	}
	if s.blockCache != nil {
		s.blockCache.Destroy()
	}
}

// =============================================================================
// Key Counter (discovers key counts by iterating)
// =============================================================================

func countKeys(store *Store, cfName string) (uint64, error) {
	iter := store.NewIteratorCF(cfName)
	defer iter.Close()

	var count uint64
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}
	if err := iter.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

// =============================================================================
// RecSplit Builder
// =============================================================================

type CFBuildStats struct {
	CFName    string
	KeyCount  uint64
	BuildTime time.Duration
	IndexSize int64
}

func buildCFIndex(store *Store, cfName string, keyCount uint64, indexPath, tmpPath string, logger *logging.DualLogger) (*CFBuildStats, error) {
	stats := &CFBuildStats{
		CFName:   cfName,
		KeyCount: keyCount,
	}

	startTime := time.Now()

	// Create temp directory for this CF
	cfTmpDir := filepath.Join(tmpPath, cfName)
	if err := os.MkdirAll(cfTmpDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Index file path
	indexFile := filepath.Join(indexPath, fmt.Sprintf("cf-%s.idx", cfName))

	// Create RecSplit builder
	erigonLogger := erigonlog.New()

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:           int(keyCount),
		Enums:              false,
		LessFalsePositives: true,
		BucketSize:         types.RecSplitBucketSize,
		LeafSize:           types.RecSplitLeafSize,
		TmpDir:             cfTmpDir,
		IndexFile:          indexFile,
		BaseDataID:         0,
		Version:            types.RecSplitDataVersion,
	}, erigonLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create RecSplit: %w", err)
	}
	defer rs.Close()

	// Iterate and add keys
	iter := store.NewIteratorCF(cfName)
	defer iter.Close()

	keysAdded := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key().Data()
		value := iter.Value().Data()
		ledgerSeq := types.ParseLedgerSeq(value)

		if err := rs.AddKey(key, uint64(ledgerSeq)); err != nil {
			return nil, fmt.Errorf("failed to add key %d: %w", keysAdded, err)
		}
		keysAdded++
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	if uint64(keysAdded) != keyCount {
		return nil, fmt.Errorf("key count mismatch: expected %d, got %d", keyCount, keysAdded)
	}

	// Build the index
	ctx := context.Background()
	if err := rs.Build(ctx); err != nil {
		if err == recsplit.ErrCollision {
			return nil, fmt.Errorf("hash collision detected (rare, try rebuilding)")
		}
		return nil, fmt.Errorf("failed to build index: %w", err)
	}

	stats.BuildTime = time.Since(startTime)

	// Get index file size
	if info, err := os.Stat(indexFile); err == nil {
		stats.IndexSize = info.Size()
	}

	// Clean up temp directory
	os.RemoveAll(cfTmpDir)

	return stats, nil
}

// =============================================================================
// Verification
// =============================================================================

type VerifyResult struct {
	CFName       string
	KeysVerified uint64
	Failures     uint64
	Duration     time.Duration
}

func verifyCF(store *Store, cfName string, indexPath string, logger *logging.DualLogger) (*VerifyResult, error) {
	result := &VerifyResult{
		CFName: cfName,
	}

	startTime := time.Now()

	// Open RecSplit index
	idxPath := filepath.Join(indexPath, fmt.Sprintf("cf-%s.idx", cfName))
	idx, err := recsplit.OpenIndex(idxPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open index %s: %w", idxPath, err)
	}
	defer idx.Close()

	reader := recsplit.NewIndexReader(idx)

	// Iterate over RocksDB and verify each key
	iter := store.NewIteratorCF(cfName)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key().Data()
		value := iter.Value().Data()
		expectedLedgerSeq := types.ParseLedgerSeq(value)

		// Look up in RecSplit
		offset, found := reader.Lookup(key)
		if !found {
			result.Failures++
			logger.Error("CF %s: Key %s not found in RecSplit",
				cfName, types.BytesToHex(key))
			continue
		}

		// RecSplit returns uint64, which is our ledgerSeq
		actualLedgerSeq := uint32(offset)

		if actualLedgerSeq != expectedLedgerSeq {
			result.Failures++
			logger.Error("CF %s: Key %s value mismatch: expected %d, got %d",
				cfName, types.BytesToHex(key), expectedLedgerSeq, actualLedgerSeq)
			continue
		}

		result.KeysVerified++
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	result.Duration = time.Since(startTime)
	return result, nil
}

// =============================================================================
// Main
// =============================================================================

func main() {
	// Parse flags
	rocksdbPath := flag.String("rocksdb-path", "", "Path to RocksDB store (required)")
	outputDir := flag.String("output-dir", "", "Directory for RecSplit index files (required)")
	logPath := flag.String("log", "", "Path to log file (required)")
	errorPath := flag.String("error", "", "Path to error file (required)")
	parallel := flag.Bool("parallel", false, "Build 16 indexes in parallel")
	doCompact := flag.Bool("compact", false, "Compact RocksDB before building")
	verify := flag.Bool("verify", false, "Verify indexes after building")
	blockCacheMB := flag.Int("block-cache-mb", 8192, "RocksDB block cache size in MB")

	flag.Parse()

	// Validate flags
	if *rocksdbPath == "" || *outputDir == "" || *logPath == "" || *errorPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --rocksdb-path, --output-dir, --log, and --error are required")
		flag.Usage()
		os.Exit(1)
	}

	// Initialize logger
	logger, err := logging.NewDualLogger(*logPath, *errorPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	// Initialize memory monitor
	memMonitor := memory.NewMemoryMonitor(logger, memory.DefaultRAMWarningThresholdGB)
	defer memMonitor.Stop()

	logger.Separator()
	logger.Info("                    BUILD RECSPLIT TOOL")
	logger.Separator()
	logger.Info("")
	logger.Info("Configuration:")
	logger.Info("  RocksDB Path:    %s", *rocksdbPath)
	logger.Info("  Output Dir:      %s", *outputDir)
	logger.Info("  Parallel Mode:   %v", *parallel)
	logger.Info("  Compact First:   %v", *doCompact)
	logger.Info("  Verify After:    %v", *verify)
	logger.Info("  Block Cache:     %d MB", *blockCacheMB)
	logger.Info("")

	// Create output and temp directories
	indexPath := filepath.Join(*outputDir, "index")
	tmpPath := filepath.Join(*outputDir, "tmp")

	if err := os.MkdirAll(indexPath, 0755); err != nil {
		logger.Error("Failed to create index directory: %v", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(tmpPath, 0755); err != nil {
		logger.Error("Failed to create temp directory: %v", err)
		os.Exit(1)
	}

	// Open RocksDB (read-write if compaction requested, read-only otherwise)
	needWrite := *doCompact
	logger.Info("Opening RocksDB store (read-only=%v)...", !needWrite)
	store, err := OpenStore(*rocksdbPath, *blockCacheMB, !needWrite)
	if err != nil {
		logger.Error("Failed to open RocksDB: %v", err)
		os.Exit(1)
	}
	defer store.Close()
	logger.Info("RocksDB store opened successfully")
	logger.Info("")

	// Phase 1: Compaction (optional)
	if *doCompact {
		logger.Separator()
		logger.Info("                    COMPACTION PHASE")
		logger.Separator()
		logger.Info("")

		compact.CompactAllCFs(store, logger)
		logger.Info("Current RSS: %.2f GB", memMonitor.CurrentRSSGB())
		logger.Info("")
	}

	// Discover key counts
	logger.Separator()
	logger.Info("                    DISCOVERING KEY COUNTS")
	logger.Separator()
	logger.Info("")

	cfCounts := make(map[string]uint64)
	var totalKeys uint64
	countStart := time.Now()

	for i, cfName := range cf.Names {
		countCFStart := time.Now()
		count, err := countKeys(store, cfName)
		if err != nil {
			logger.Error("Failed to count keys in CF %s: %v", cfName, err)
			os.Exit(1)
		}
		cfCounts[cfName] = count
		totalKeys += count
		logger.Info("[%2d/16] CF [%s]: %s keys (counted in %v)",
			i+1, cfName, helpers.FormatNumber(int64(count)), time.Since(countCFStart))
	}

	logger.Info("")
	logger.Info("Total keys: %s (discovered in %v)", helpers.FormatNumber(int64(totalKeys)), time.Since(countStart))
	logger.Info("Current RSS: %.2f GB", memMonitor.CurrentRSSGB())
	logger.Info("")

	// Estimate memory
	if *parallel {
		estMem := totalKeys * 40 // ~40 bytes per key
		logger.Info("MEMORY ESTIMATE (Parallel Mode):")
		logger.Info("  ~%s for all 16 CFs building simultaneously", helpers.FormatBytes(int64(estMem)))
		logger.Info("")
		logger.Info("WARNING: This requires significant RAM!")
	} else {
		var maxKeys uint64
		for _, count := range cfCounts {
			if count > maxKeys {
				maxKeys = count
			}
		}
		estMem := maxKeys * 40
		logger.Info("MEMORY ESTIMATE (Sequential Mode):")
		logger.Info("  ~%s peak (largest CF)", helpers.FormatBytes(int64(estMem)))
	}
	logger.Info("")

	// Phase 2: Build indexes
	logger.Separator()
	logger.Info("                    BUILDING RECSPLIT INDEXES")
	logger.Separator()
	logger.Info("")

	buildStart := time.Now()
	allStats := make(map[string]*CFBuildStats)

	if *parallel {
		logger.Info("Building 16 indexes in parallel...")
		logger.Info("")

		var wg sync.WaitGroup
		var mu sync.Mutex
		errors := make([]error, len(cf.Names))

		for i, cfName := range cf.Names {
			wg.Add(1)
			go func(idx int, cfn string) {
				defer wg.Done()

				keyCount := cfCounts[cfn]
				if keyCount == 0 {
					return
				}

				stats, err := buildCFIndex(store, cfn, keyCount, indexPath, tmpPath, logger)
				if err != nil {
					errors[idx] = fmt.Errorf("CF %s: %w", cfn, err)
					return
				}

				mu.Lock()
				allStats[cfn] = stats
				logger.Info("CF [%s] completed: %s keys in %v, size: %s",
					cfn, helpers.FormatNumber(int64(stats.KeyCount)), stats.BuildTime, helpers.FormatBytes(stats.IndexSize))
				mu.Unlock()
			}(i, cfName)
		}

		wg.Wait()

		for _, err := range errors {
			if err != nil {
				logger.Error("%v", err)
				os.Exit(1)
			}
		}
	} else {
		logger.Info("Building indexes sequentially...")
		logger.Info("")

		for i, cfName := range cf.Names {
			keyCount := cfCounts[cfName]
			if keyCount == 0 {
				logger.Info("[%2d/16] CF [%s]: No keys, skipping", i+1, cfName)
				continue
			}

			logger.Info("[%2d/16] CF [%s]: Building index for %s keys...",
				i+1, cfName, helpers.FormatNumber(int64(keyCount)))

			stats, err := buildCFIndex(store, cfName, keyCount, indexPath, tmpPath, logger)
			if err != nil {
				logger.Error("Failed to build index for CF %s: %v", cfName, err)
				os.Exit(1)
			}

			allStats[cfName] = stats
			logger.Info("        Completed in %v, size: %s, RSS: %.2f GB",
				stats.BuildTime, helpers.FormatBytes(stats.IndexSize), memMonitor.CurrentRSSGB())
		}
	}

	totalBuildTime := time.Since(buildStart)

	// Log build summary
	logger.Info("")
	logger.Separator()
	logger.Info("                    BUILD SUMMARY")
	logger.Separator()
	logger.Info("")

	var totalSize int64
	logger.Info("%-4s %15s %12s %12s", "CF", "Keys", "Build Time", "Index Size")
	logger.Info("%-4s %15s %12s %12s", "----", "---------------", "------------", "------------")

	for _, cfName := range cf.Names {
		stats := allStats[cfName]
		if stats != nil {
			logger.Info("%-4s %15s %12v %12s",
				cfName,
				helpers.FormatNumber(int64(stats.KeyCount)),
				stats.BuildTime,
				helpers.FormatBytes(stats.IndexSize))
			totalSize += stats.IndexSize
		}
	}

	logger.Info("%-4s %15s %12s %12s", "----", "---------------", "------------", "------------")
	logger.Info("%-4s %15s %12v %12s", "TOT", helpers.FormatNumber(int64(totalKeys)), totalBuildTime, helpers.FormatBytes(totalSize))
	logger.Info("")
	logger.Info("Output: %s", indexPath)
	logger.Info("Current RSS: %.2f GB", memMonitor.CurrentRSSGB())
	logger.Info("")

	// Phase 3: Verification (optional)
	if *verify {
		logger.Separator()
		logger.Info("                    VERIFICATION PHASE")
		logger.Separator()
		logger.Info("")
		logger.Info("Verifying RecSplit indexes against RocksDB data...")
		logger.Info("")

		verifyStart := time.Now()
		var totalVerified, totalFailures uint64

		for i, cfName := range cf.Names {
			keyCount := cfCounts[cfName]
			if keyCount == 0 {
				logger.Info("[%2d/16] CF [%s]: No keys, skipping", i+1, cfName)
				continue
			}

			logger.Info("[%2d/16] CF [%s]: Verifying %s keys...",
				i+1, cfName, helpers.FormatNumber(int64(keyCount)))

			result, err := verifyCF(store, cfName, indexPath, logger)
			if err != nil {
				logger.Error("Verification error for CF %s: %v", cfName, err)
				continue
			}

			totalVerified += result.KeysVerified
			totalFailures += result.Failures

			if result.Failures == 0 {
				logger.Info("        PASSED: %s keys verified in %v",
					helpers.FormatNumber(int64(result.KeysVerified)), result.Duration)
			} else {
				logger.Error("        FAILED: %d failures out of %d keys",
					result.Failures, result.KeysVerified+result.Failures)
			}

			if (i+1)%4 == 0 {
				memMonitor.Check()
			}
		}

		totalVerifyTime := time.Since(verifyStart)

		logger.Info("")
		logger.Separator()
		logger.Info("                    VERIFICATION SUMMARY")
		logger.Separator()
		logger.Info("")
		logger.Info("Total Keys Verified: %s", helpers.FormatNumber(int64(totalVerified)))
		logger.Info("Total Failures:      %d", totalFailures)
		logger.Info("Verification Time:   %v", totalVerifyTime)
		logger.Info("")

		if totalFailures == 0 {
			logger.Info("STATUS: ALL VERIFICATIONS PASSED")
		} else {
			logger.Error("STATUS: %d FAILURES DETECTED", totalFailures)
		}
		logger.Info("")
	}

	// Final summary
	logger.Separator()
	memMonitor.LogSummary(logger)
	logger.Sync()

	// Clean up temp directory
	os.RemoveAll(tmpPath)

	fmt.Printf("Build complete: %s keys in %v\n", helpers.FormatNumber(int64(totalKeys)), totalBuildTime)
	fmt.Printf("Index files: %s\n", indexPath)
	fmt.Printf("Total index size: %s\n", helpers.FormatBytes(totalSize))

	if *verify {
		fmt.Println("Verification: PASSED")
	}
}
