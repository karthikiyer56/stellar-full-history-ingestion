// =============================================================================
// recsplit.go - RecSplit Index Building
// =============================================================================
//
// This file implements the RecSplit building phase that creates perfect hash
// function indexes from the RocksDB data.
//
// WHAT IS RECSPLIT:
//
//	RecSplit is a minimal perfect hash function (MPHF) library that maps
//	each key to a unique integer in [0, n) without collisions.
//
//	In our case, we store txHash → ledgerSeq mappings. The RecSplit index
//	allows O(1) lookup of ledgerSeq given a txHash, with very low memory
//	overhead (~2 bits per key).
//
// BUILDING OPTIONS:
//
//	1. Sequential (--parallel-recsplit=false):
//	   - Build one index per CF, one at a time
//	   - Memory: ~9 GB peak (for ~220M keys)
//	   - Slower but lower memory
//
//	2. Parallel (--parallel-recsplit=true):
//	   - Build all 16 CF indexes in parallel
//	   - Memory: ~144 GB peak (16 × 9 GB)
//	   - Faster but requires much more RAM
//
// INDEX FILE NAMING:
//
//	Sequential: Single index file at <recsplit-index>/txhash.idx
//	Parallel: 16 index files at <recsplit-index>/cf-0.idx through cf-f.idx
//
// CRASH RECOVERY:
//
//	If RecSplit building is interrupted:
//	  1. Delete all files in temp directory
//	  2. Delete all files in index directory
//	  3. Rebuild from scratch
//
// =============================================================================

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	erigonlog "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// RecSplitBucketSize is the bucket size for RecSplit construction.
	// Larger buckets = faster build but more memory.
	RecSplitBucketSize = 2000

	// RecSplitLeafSize is the leaf size for RecSplit construction.
	RecSplitLeafSize = 8

	// RecSplitDataVersion is the data format version stored in the index.
	RecSplitDataVersion = 1
)

// =============================================================================
// RecSplit Builder Stats
// =============================================================================

// RecSplitStats holds statistics for RecSplit building.
type RecSplitStats struct {
	// StartTime when building began
	StartTime time.Time

	// EndTime when building completed
	EndTime time.Time

	// PerCFStats holds per-CF statistics
	PerCFStats map[string]*RecSplitCFStats

	// TotalTime is the total build time
	TotalTime time.Duration

	// TotalKeys is the total number of keys indexed
	TotalKeys uint64

	// Parallel indicates if parallel mode was used
	Parallel bool
}

// RecSplitCFStats holds per-CF RecSplit statistics.
type RecSplitCFStats struct {
	CFName     string
	KeyCount   uint64
	BuildTime  time.Duration
	IndexSize  int64
	IndexPath  string
	KeyAddTime time.Duration
}

// NewRecSplitStats creates a new RecSplitStats.
func NewRecSplitStats(parallel bool) *RecSplitStats {
	return &RecSplitStats{
		PerCFStats: make(map[string]*RecSplitCFStats),
		Parallel:   parallel,
	}
}

// LogSummary logs a summary of RecSplit building.
func (rs *RecSplitStats) LogSummary(logger Logger) {
	logger.Separator()
	logger.Info("                    RECSPLIT BUILD SUMMARY")
	logger.Separator()
	logger.Info("")

	if rs.Parallel {
		logger.Info("Mode: Parallel (16 CFs simultaneously)")
	} else {
		logger.Info("Mode: Sequential (one CF at a time)")
	}
	logger.Info("")

	logger.Info("PER-CF STATISTICS:")
	logger.Info("%-4s %15s %12s %12s", "CF", "Keys", "Build Time", "Index Size")
	logger.Info("%-4s %15s %12s %12s", "----", "---------------", "------------", "------------")

	var totalKeys uint64
	var totalSize int64

	for _, cf := range ColumnFamilyNames {
		stats := rs.PerCFStats[cf]
		if stats != nil {
			logger.Info("%-4s %15s %12v %12s",
				cf,
				helpers.FormatNumber(int64(stats.KeyCount)),
				stats.BuildTime,
				helpers.FormatBytes(stats.IndexSize))
			totalKeys += stats.KeyCount
			totalSize += stats.IndexSize
		}
	}

	logger.Info("%-4s %15s %12s %12s", "----", "---------------", "------------", "------------")
	logger.Info("%-4s %15s %12v %12s", "TOT", helpers.FormatNumber(int64(totalKeys)), rs.TotalTime, helpers.FormatBytes(totalSize))
	logger.Info("")
}

// =============================================================================
// RecSplit Builder
// =============================================================================

// RecSplitBuilder handles building RecSplit indexes from RocksDB data.
type RecSplitBuilder struct {
	store     TxHashStore
	cfCounts  map[string]uint64
	indexPath string
	tmpPath   string
	parallel  bool
	logger    Logger
	memory    *MemoryMonitor
	stats     *RecSplitStats
}

// NewRecSplitBuilder creates a new RecSplitBuilder.
//
// PARAMETERS:
//   - store: RocksDB store to read data from
//   - cfCounts: Per-CF key counts (from meta store)
//   - indexPath: Directory for output index files
//   - tmpPath: Directory for temporary files during build
//   - parallel: Whether to build indexes in parallel
//   - logger: Logger instance
//   - memory: Memory monitor
func NewRecSplitBuilder(
	store TxHashStore,
	cfCounts map[string]uint64,
	indexPath string,
	tmpPath string,
	parallel bool,
	logger Logger,
	memory *MemoryMonitor,
) *RecSplitBuilder {
	return &RecSplitBuilder{
		store:     store,
		cfCounts:  cfCounts,
		indexPath: indexPath,
		tmpPath:   tmpPath,
		parallel:  parallel,
		logger:    logger,
		memory:    memory,
		stats:     NewRecSplitStats(parallel),
	}
}

// Run executes the RecSplit building phase.
func (b *RecSplitBuilder) Run() (*RecSplitStats, error) {
	b.stats.StartTime = time.Now()

	b.logger.Separator()
	b.logger.Info("                    RECSPLIT BUILDING PHASE")
	b.logger.Separator()
	b.logger.Info("")

	// Clean up any existing files (for crash recovery)
	if err := b.cleanupDirectories(); err != nil {
		return nil, err
	}

	// Log memory estimate
	LogRecSplitMemoryEstimate(b.logger, b.cfCounts, b.parallel)

	// Build indexes
	var err error
	if b.parallel {
		err = b.buildParallel()
	} else {
		err = b.buildSequential()
	}

	if err != nil {
		return nil, err
	}

	b.stats.EndTime = time.Now()
	b.stats.TotalTime = time.Since(b.stats.StartTime)

	// Calculate total keys
	for _, stats := range b.stats.PerCFStats {
		b.stats.TotalKeys += stats.KeyCount
	}

	// Log summary
	b.stats.LogSummary(b.logger)
	b.memory.LogSummary(b.logger)

	b.logger.Sync()

	return b.stats, nil
}

// cleanupDirectories removes existing temp and index files.
func (b *RecSplitBuilder) cleanupDirectories() error {
	b.logger.Info("Cleaning up existing files...")

	// Clean temp directory
	if err := os.RemoveAll(b.tmpPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to clean temp directory: %w", err)
	}
	if err := helpers.EnsureDir(b.tmpPath); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Clean index directory
	if err := os.RemoveAll(b.indexPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to clean index directory: %w", err)
	}
	if err := helpers.EnsureDir(b.indexPath); err != nil {
		return fmt.Errorf("failed to create index directory: %w", err)
	}

	b.logger.Info("Directories cleaned")
	b.logger.Info("")

	return nil
}

// buildSequential builds indexes one CF at a time.
func (b *RecSplitBuilder) buildSequential() error {
	b.logger.Info("Building indexes sequentially (one CF at a time)...")
	b.logger.Info("")

	for i, cfName := range ColumnFamilyNames {
		keyCount := b.cfCounts[cfName]
		if keyCount == 0 {
			b.logger.Info("[%2d/16] CF [%s]: No keys, skipping", i+1, cfName)
			continue
		}

		b.logger.Info("[%2d/16] CF [%s]: Building index for %s keys...",
			i+1, cfName, helpers.FormatNumber(int64(keyCount)))

		stats, err := b.buildCFIndex(cfName, keyCount)
		if err != nil {
			return fmt.Errorf("failed to build index for CF %s: %w", cfName, err)
		}

		b.stats.PerCFStats[cfName] = stats
		b.logger.Info("        Completed in %v, size: %s",
			stats.BuildTime, helpers.FormatBytes(stats.IndexSize))

		// Check memory after each CF
		b.memory.Check()
	}

	return nil
}

// buildParallel builds all 16 CF indexes in parallel.
func (b *RecSplitBuilder) buildParallel() error {
	b.logger.Info("Building indexes in parallel (16 CFs simultaneously)...")
	b.logger.Info("")
	b.logger.Info("WARNING: This requires ~144 GB of RAM!")
	b.logger.Info("")

	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := make([]error, len(ColumnFamilyNames))

	for i, cfName := range ColumnFamilyNames {
		wg.Add(1)
		go func(idx int, cf string) {
			defer wg.Done()

			keyCount := b.cfCounts[cf]
			if keyCount == 0 {
				return
			}

			stats, err := b.buildCFIndex(cf, keyCount)
			if err != nil {
				errors[idx] = fmt.Errorf("CF %s: %w", cf, err)
				return
			}

			mu.Lock()
			b.stats.PerCFStats[cf] = stats
			b.logger.Info("CF [%s] completed: %s keys in %v",
				cf, helpers.FormatNumber(int64(stats.KeyCount)), stats.BuildTime)
			mu.Unlock()
		}(i, cfName)
	}

	wg.Wait()

	// Check for errors
	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}

// buildCFIndex builds a RecSplit index for a single column family.
func (b *RecSplitBuilder) buildCFIndex(cfName string, keyCount uint64) (*RecSplitCFStats, error) {
	stats := &RecSplitCFStats{
		CFName:   cfName,
		KeyCount: keyCount,
	}

	startTime := time.Now()

	// Determine output path
	indexFileName := fmt.Sprintf("cf-%s.idx", cfName)
	indexPath := filepath.Join(b.indexPath, indexFileName)
	stats.IndexPath = indexPath

	// Create temp directory for this CF
	cfTmpDir := filepath.Join(b.tmpPath, cfName)
	if err := helpers.EnsureDir(cfTmpDir); err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Create RecSplit builder
	erigonLogger := erigonlog.New()

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:           int(keyCount),
		Enums:              false, // We store ledgerSeq values, not sequential offsets
		LessFalsePositives: true,
		BucketSize:         RecSplitBucketSize,
		LeafSize:           RecSplitLeafSize,
		TmpDir:             cfTmpDir,
		IndexFile:          indexPath,
		BaseDataID:         0,
		Version:            RecSplitDataVersion,
	}, erigonLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create RecSplit: %w", err)
	}
	defer rs.Close()

	// Iterate over CF and add keys
	keyAddStart := time.Now()
	iter := b.store.NewIteratorCF(cfName)
	defer iter.Close()

	keysAdded := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// Parse ledger sequence from value
		ledgerSeq := ParseLedgerSeq(value)

		// Add to RecSplit
		if err := rs.AddKey(key, uint64(ledgerSeq)); err != nil {
			return nil, fmt.Errorf("failed to add key %d: %w", keysAdded, err)
		}
		keysAdded++
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	stats.KeyAddTime = time.Since(keyAddStart)

	// Verify key count
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
	if info, err := os.Stat(indexPath); err == nil {
		stats.IndexSize = info.Size()
	}

	// Clean up temp directory for this CF
	os.RemoveAll(cfTmpDir)

	return stats, nil
}

// GetStats returns the RecSplit build statistics.
func (b *RecSplitBuilder) GetStats() *RecSplitStats {
	return b.stats
}

// =============================================================================
// Convenience Function
// =============================================================================

// RunRecSplitBuild is a convenience function to run RecSplit building.
func RunRecSplitBuild(
	store TxHashStore,
	meta MetaStore,
	config *Config,
	logger Logger,
	memory *MemoryMonitor,
) (*RecSplitStats, error) {
	// Get CF counts from meta store
	cfCounts, err := meta.GetCFCounts()
	if err != nil {
		return nil, fmt.Errorf("failed to get CF counts: %w", err)
	}

	builder := NewRecSplitBuilder(
		store,
		cfCounts,
		config.RecsplitIndexPath,
		config.RecsplitTmpPath,
		config.ParallelRecsplit,
		logger,
		memory,
	)

	return builder.Run()
}

// =============================================================================
// RecSplit Verification Helper
// =============================================================================

// VerifyRecSplitIndex opens and performs a basic verification of a RecSplit index.
//
// RETURNS:
//   - keyCount: Number of keys in the index
//   - error: If the index is corrupted or unreadable
func VerifyRecSplitIndex(indexPath string) (uint64, error) {
	idx, err := recsplit.OpenIndex(indexPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open index: %w", err)
	}
	defer idx.Close()

	// Get key count (stored in index metadata)
	keyCount := idx.KeyCount()

	return keyCount, nil
}
