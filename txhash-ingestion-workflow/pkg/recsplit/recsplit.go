// =============================================================================
// pkg/recsplit/recsplit.go - RecSplit Index Building
// =============================================================================
//
// This package implements the RecSplit building phase that creates perfect hash
// function indexes from the RocksDB data.
//
// TWO MODES:
//
//   1. Combined Index (default, multiIndex=false):
//      - Reads all 16 CFs sequentially
//      - Feeds all keys into ONE RecSplit builder
//      - Produces single file: txhash.idx
//      - Lower memory usage (~9 GB for entire build)
//
//   2. Multi-Index (multiIndex=true):
//      - Reads all 16 CFs in parallel
//      - Each CF has its own RecSplit builder
//      - Produces 16 files: cf-0.idx through cf-f.idx
//      - Higher memory usage (~144 GB peak)
//
// =============================================================================

package recsplit

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
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/cf"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/memory"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/types"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// CombinedIndexFileName is the name of the single combined index file
	CombinedIndexFileName = "txhash.idx"
)

// =============================================================================
// RecSplit Builder Stats
// =============================================================================

// Stats holds statistics for RecSplit building.
type Stats struct {
	// StartTime when building began
	StartTime time.Time

	// EndTime when building completed
	EndTime time.Time

	// PerCFStats holds per-CF statistics (key addition time for combined mode)
	PerCFStats map[string]*CFStats

	// TotalTime is the total build time
	TotalTime time.Duration

	// TotalKeys is the total number of keys indexed
	TotalKeys uint64

	// MultiIndex indicates if multi-index mode was used
	MultiIndex bool

	// IndexBuildTime is the time spent building the index (combined mode only)
	IndexBuildTime time.Duration

	// KeyAddTime is the total time spent adding keys (combined mode only)
	KeyAddTime time.Duration

	// IndexSize is the total size of index file(s)
	IndexSize int64

	// IndexPath is the path to the index file (combined mode) or directory (multi-index mode)
	IndexPath string
}

// CFStats holds per-CF RecSplit statistics.
type CFStats struct {
	CFName     string
	KeyCount   uint64
	BuildTime  time.Duration // Full build time (multi-index) or key-add time (combined)
	IndexSize  int64         // Only set in multi-index mode
	IndexPath  string        // Only set in multi-index mode
	KeyAddTime time.Duration // Time to add keys from this CF
}

// NewStats creates a new Stats.
func NewStats(multiIndex bool) *Stats {
	return &Stats{
		PerCFStats: make(map[string]*CFStats),
		MultiIndex: multiIndex,
	}
}

// LogSummary logs a summary of RecSplit building.
func (rs *Stats) LogSummary(logger interfaces.Logger) {
	logger.Separator()
	logger.Info("                    RECSPLIT BUILD SUMMARY")
	logger.Separator()
	logger.Info("")

	if rs.MultiIndex {
		logger.Info("Mode: Multi-Index (16 separate index files)")
	} else {
		logger.Info("Mode: Combined (single index file: %s)", CombinedIndexFileName)
	}
	logger.Info("")

	if rs.MultiIndex {
		// Multi-index mode: show per-CF build time and size
		logger.Info("PER-CF STATISTICS:")
		logger.Info("%-4s %15s %12s %12s", "CF", "Keys", "Build Time", "Index Size")
		logger.Info("%-4s %15s %12s %12s", "----", "---------------", "------------", "------------")

		var totalKeys uint64
		var totalSize int64

		for _, cfName := range cf.Names {
			stats := rs.PerCFStats[cfName]
			if stats != nil {
				logger.Info("%-4s %15s %12s %12s",
					cfName,
					helpers.FormatNumber(int64(stats.KeyCount)),
					helpers.FormatDuration(stats.BuildTime),
					helpers.FormatBytes(stats.IndexSize))
				totalKeys += stats.KeyCount
				totalSize += stats.IndexSize
			}
		}

		logger.Info("%-4s %15s %12s %12s", "----", "---------------", "------------", "------------")
		logger.Info("%-4s %15s %12s %12s", "TOT", helpers.FormatNumber(int64(totalKeys)), helpers.FormatDuration(rs.TotalTime), helpers.FormatBytes(totalSize))
	} else {
		// Combined mode: show per-CF key-add time, then overall build time
		logger.Info("KEY ADDITION BY CF:")
		logger.Info("%-4s %15s %12s", "CF", "Keys Added", "Add Time")
		logger.Info("%-4s %15s %12s", "----", "---------------", "------------")

		var totalKeys uint64

		for _, cfName := range cf.Names {
			stats := rs.PerCFStats[cfName]
			if stats != nil {
				logger.Info("%-4s %15s %12s",
					cfName,
					helpers.FormatNumber(int64(stats.KeyCount)),
					helpers.FormatDuration(stats.KeyAddTime))
				totalKeys += stats.KeyCount
			}
		}

		logger.Info("%-4s %15s %12s", "----", "---------------", "------------")
		logger.Info("%-4s %15s %12s", "TOT", helpers.FormatNumber(int64(totalKeys)), helpers.FormatDuration(rs.KeyAddTime))
		logger.Info("")
		logger.Info("INDEX BUILD:")
		logger.Info("  Build Time:    %s", helpers.FormatDuration(rs.IndexBuildTime))
		logger.Info("  Index Size:    %s", helpers.FormatBytes(rs.IndexSize))
		logger.Info("  Index Path:    %s", rs.IndexPath)
		logger.Info("")
		logger.Info("TOTAL TIME:      %s", helpers.FormatDuration(rs.TotalTime))
	}

	logger.Info("")
}

// =============================================================================
// RecSplit Builder
// =============================================================================

// Builder handles building RecSplit indexes from RocksDB data.
type Builder struct {
	store      interfaces.TxHashStore
	cfCounts   map[string]uint64
	indexPath  string
	tmpPath    string
	multiIndex bool
	logger     interfaces.Logger
	memory     interfaces.MemoryMonitor
	stats      *Stats
}

// NewBuilder creates a new RecSplit Builder.
//
// PARAMETERS:
//   - store: RocksDB store to read data from
//   - cfCounts: Per-CF key counts (from meta store)
//   - indexPath: Directory for output index files
//   - tmpPath: Directory for temporary files during build
//   - multiIndex: If true, build 16 separate indexes; if false, build one combined index
//   - logger: Logger instance
//   - mem: Memory monitor
func NewBuilder(
	store interfaces.TxHashStore,
	cfCounts map[string]uint64,
	indexPath string,
	tmpPath string,
	multiIndex bool,
	logger interfaces.Logger,
	mem interfaces.MemoryMonitor,
) *Builder {
	return &Builder{
		store:      store,
		cfCounts:   cfCounts,
		indexPath:  indexPath,
		tmpPath:    tmpPath,
		multiIndex: multiIndex,
		logger:     logger,
		memory:     mem,
		stats:      NewStats(multiIndex),
	}
}

// Run executes the RecSplit building phase.
func (b *Builder) Run() (*Stats, error) {
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
	memory.LogRecSplitMemoryEstimate(b.logger, b.cfCounts, b.multiIndex)

	// Build indexes based on mode
	var err error
	if b.multiIndex {
		err = b.buildMultiIndex()
	} else {
		err = b.buildCombinedIndex()
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
func (b *Builder) cleanupDirectories() error {
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

// =============================================================================
// Combined Index Mode (default)
// =============================================================================

// buildCombinedIndex builds a single combined index from all 16 CFs.
func (b *Builder) buildCombinedIndex() error {
	b.logger.Info("Building combined index (single file: %s)...", CombinedIndexFileName)
	b.logger.Info("")

	// Calculate total key count
	var totalKeys uint64
	for _, count := range b.cfCounts {
		totalKeys += count
	}

	if totalKeys == 0 {
		b.logger.Info("No keys to index")
		return nil
	}

	b.logger.Info("Total keys to index: %s", helpers.FormatNumber(int64(totalKeys)))
	b.logger.Info("")

	// Determine output path
	indexFilePath := filepath.Join(b.indexPath, CombinedIndexFileName)
	b.stats.IndexPath = indexFilePath

	// Create RecSplit builder with total key count
	erigonLogger := erigonlog.New()

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:           int(totalKeys),
		Enums:              false, // We store ledgerSeq values, not sequential offsets
		LessFalsePositives: types.RecSplitLessFalsePositivesEnabled,
		BucketSize:         types.RecSplitBucketSize,
		LeafSize:           types.RecSplitLeafSize,
		TmpDir:             b.tmpPath,
		IndexFile:          indexFilePath,
		BaseDataID:         0,
		Version:            types.RecSplitDataVersion,
	}, erigonLogger)
	if err != nil {
		return fmt.Errorf("failed to create RecSplit: %w", err)
	}
	defer rs.Close()

	// Add keys from all CFs sequentially
	b.logger.Info("Adding keys from 16 column families...")
	b.logger.Info("")

	keyAddStart := time.Now()
	var totalKeysAdded uint64

	for i, cfName := range cf.Names {
		keyCount := b.cfCounts[cfName]
		if keyCount == 0 {
			b.logger.Info("[%2d/16] CF [%s]: No keys, skipping", i+1, cfName)
			b.stats.PerCFStats[cfName] = &CFStats{
				CFName:   cfName,
				KeyCount: 0,
			}
			continue
		}

		cfStart := time.Now()

		// Iterate over CF and add keys using scan-optimized iterator
		iter := b.store.NewScanIteratorCF(cfName)
		keysAdded := uint64(0)

		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()

			// Parse ledger sequence from value
			ledgerSeq := types.ParseLedgerSeq(value)

			// Add to RecSplit
			if err := rs.AddKey(key, uint64(ledgerSeq)); err != nil {
				iter.Close()
				return fmt.Errorf("failed to add key from CF %s: %w", cfName, err)
			}
			keysAdded++
		}

		if err := iter.Error(); err != nil {
			iter.Close()
			return fmt.Errorf("iterator error for CF %s: %w", cfName, err)
		}
		iter.Close()

		// Verify key count matches expected
		if keysAdded != keyCount {
			return fmt.Errorf("CF %s: key count mismatch: expected %d, got %d", cfName, keyCount, keysAdded)
		}

		cfAddTime := time.Since(cfStart)
		totalKeysAdded += keysAdded

		b.stats.PerCFStats[cfName] = &CFStats{
			CFName:     cfName,
			KeyCount:   keysAdded,
			KeyAddTime: cfAddTime,
		}

		b.logger.Info("[%2d/16] CF [%s]: Added %s keys in %s",
			i+1, cfName, helpers.FormatNumber(int64(keysAdded)), helpers.FormatDuration(cfAddTime))

		// Check memory after each CF
		b.memory.Check()
	}

	b.stats.KeyAddTime = time.Since(keyAddStart)

	b.logger.Info("")
	b.logger.Info("Key addition complete: %s keys in %s",
		helpers.FormatNumber(int64(totalKeysAdded)), helpers.FormatDuration(b.stats.KeyAddTime))
	b.logger.Info("")

	// Verify total key count
	if totalKeysAdded != totalKeys {
		return fmt.Errorf("total key count mismatch: expected %d, got %d", totalKeys, totalKeysAdded)
	}

	// Build the index
	b.logger.Info("Building combined index...")
	buildStart := time.Now()

	ctx := context.Background()
	if err := rs.Build(ctx); err != nil {
		if err == recsplit.ErrCollision {
			return fmt.Errorf("hash collision detected (rare, try rebuilding)")
		}
		return fmt.Errorf("failed to build index: %w", err)
	}

	b.stats.IndexBuildTime = time.Since(buildStart)

	// Get index file size
	if info, err := os.Stat(indexFilePath); err == nil {
		b.stats.IndexSize = info.Size()
	}

	b.logger.Info("Index built in %s, size: %s",
		helpers.FormatDuration(b.stats.IndexBuildTime), helpers.FormatBytes(b.stats.IndexSize))
	b.logger.Info("")

	// Clean up temp directory
	if err := os.RemoveAll(b.tmpPath); err != nil {
		b.logger.Info("Warning: Failed to clean up temp directory: %v", err)
	}

	return nil
}

// =============================================================================
// Multi-Index Mode
// =============================================================================

// buildMultiIndex builds 16 separate CF indexes in parallel.
func (b *Builder) buildMultiIndex() error {
	b.logger.Info("Building indexes in parallel (16 CFs simultaneously)...")
	b.logger.Info("")
	b.logger.Info("WARNING: This requires ~144 GB of RAM!")
	b.logger.Info("")

	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := make([]error, len(cf.Names))

	for i, cfName := range cf.Names {
		wg.Add(1)
		go func(idx int, cfn string) {
			defer wg.Done()

			keyCount := b.cfCounts[cfn]
			if keyCount == 0 {
				return
			}

			stats, err := b.buildCFIndex(cfn, keyCount)
			if err != nil {
				errors[idx] = fmt.Errorf("CF %s: %w", cfn, err)
				return
			}

			mu.Lock()
			b.stats.PerCFStats[cfn] = stats
			b.logger.Info("CF [%s] completed: %s keys in %s",
				cfn, helpers.FormatNumber(int64(stats.KeyCount)), helpers.FormatDuration(stats.BuildTime))
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

	// Calculate total index size
	for _, stats := range b.stats.PerCFStats {
		b.stats.IndexSize += stats.IndexSize
	}

	// Clean up temp directory
	if err := os.RemoveAll(b.tmpPath); err != nil {
		b.logger.Info("Warning: Failed to clean up temp directory: %v", err)
	}

	return nil
}

// buildCFIndex builds a RecSplit index for a single column family.
func (b *Builder) buildCFIndex(cfName string, keyCount uint64) (*CFStats, error) {
	stats := &CFStats{
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
		LessFalsePositives: types.RecSplitLessFalsePositivesEnabled,
		BucketSize:         types.RecSplitBucketSize,
		LeafSize:           types.RecSplitLeafSize,
		TmpDir:             cfTmpDir,
		IndexFile:          indexPath,
		BaseDataID:         0,
		Version:            types.RecSplitDataVersion,
	}, erigonLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create RecSplit: %w", err)
	}
	defer rs.Close()

	// Iterate over CF and add keys using scan-optimized iterator
	keyAddStart := time.Now()
	iter := b.store.NewScanIteratorCF(cfName)
	defer iter.Close()

	keysAdded := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// Parse ledger sequence from value
		ledgerSeq := types.ParseLedgerSeq(value)

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

	// Clean up temp directory for this CF (best-effort, parent cleanup will catch stragglers)
	os.RemoveAll(cfTmpDir)

	return stats, nil
}

// GetStats returns the RecSplit build statistics.
func (b *Builder) GetStats() *Stats {
	return b.stats
}

// =============================================================================
// RecSplit Verification Helper
// =============================================================================

// VerifyIndex opens and performs a basic verification of a RecSplit index.
//
// RETURNS:
//   - keyCount: Number of keys in the index
//   - error: If the index is corrupted or unreadable
func VerifyIndex(indexPath string) (uint64, error) {
	idx, err := recsplit.OpenIndex(indexPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open index: %w", err)
	}
	defer idx.Close()

	// Get key count (stored in index metadata)
	keyCount := idx.KeyCount()

	return keyCount, nil
}

// OpenIndex opens a RecSplit index for reading.
func OpenIndex(indexPath string) (*recsplit.Index, error) {
	return recsplit.OpenIndex(indexPath)
}

// NewIndexReader creates a new IndexReader for the given index.
func NewIndexReader(idx *recsplit.Index) *recsplit.IndexReader {
	return recsplit.NewIndexReader(idx)
}
