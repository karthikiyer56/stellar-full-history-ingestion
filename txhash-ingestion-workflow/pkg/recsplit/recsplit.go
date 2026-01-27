// =============================================================================
// pkg/recsplit/recsplit.go - RecSplit Index Building
// =============================================================================
//
// This package implements the RecSplit building phase that creates perfect hash
// function indexes from the RocksDB data.
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
	// BucketSize is the bucket size for RecSplit construction.
	// Larger buckets = faster build but more memory.
	BucketSize = 2000

	// LeafSize is the leaf size for RecSplit construction.
	LeafSize = 8

	// DataVersion is the data format version stored in the index.
	DataVersion = 1
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

	// PerCFStats holds per-CF statistics
	PerCFStats map[string]*CFStats

	// TotalTime is the total build time
	TotalTime time.Duration

	// TotalKeys is the total number of keys indexed
	TotalKeys uint64

	// Parallel indicates if parallel mode was used
	Parallel bool
}

// CFStats holds per-CF RecSplit statistics.
type CFStats struct {
	CFName     string
	KeyCount   uint64
	BuildTime  time.Duration
	IndexSize  int64
	IndexPath  string
	KeyAddTime time.Duration
}

// NewStats creates a new Stats.
func NewStats(parallel bool) *Stats {
	return &Stats{
		PerCFStats: make(map[string]*CFStats),
		Parallel:   parallel,
	}
}

// LogSummary logs a summary of RecSplit building.
func (rs *Stats) LogSummary(logger interfaces.Logger) {
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

	for _, cfName := range cf.Names {
		stats := rs.PerCFStats[cfName]
		if stats != nil {
			logger.Info("%-4s %15s %12v %12s",
				cfName,
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

// Builder handles building RecSplit indexes from RocksDB data.
type Builder struct {
	store     interfaces.TxHashStore
	cfCounts  map[string]uint64
	indexPath string
	tmpPath   string
	parallel  bool
	logger    interfaces.Logger
	memory    interfaces.MemoryMonitor
	stats     *Stats
}

// NewBuilder creates a new RecSplit Builder.
//
// PARAMETERS:
//   - store: RocksDB store to read data from
//   - cfCounts: Per-CF key counts (from meta store)
//   - indexPath: Directory for output index files
//   - tmpPath: Directory for temporary files during build
//   - parallel: Whether to build indexes in parallel
//   - logger: Logger instance
//   - mem: Memory monitor
func NewBuilder(
	store interfaces.TxHashStore,
	cfCounts map[string]uint64,
	indexPath string,
	tmpPath string,
	parallel bool,
	logger interfaces.Logger,
	mem interfaces.MemoryMonitor,
) *Builder {
	return &Builder{
		store:     store,
		cfCounts:  cfCounts,
		indexPath: indexPath,
		tmpPath:   tmpPath,
		parallel:  parallel,
		logger:    logger,
		memory:    mem,
		stats:     NewStats(parallel),
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
	memory.LogRecSplitMemoryEstimate(b.logger, b.cfCounts, b.parallel)

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

// buildSequential builds indexes one CF at a time.
func (b *Builder) buildSequential() error {
	b.logger.Info("Building indexes sequentially (one CF at a time)...")
	b.logger.Info("")

	for i, cfName := range cf.Names {
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
func (b *Builder) buildParallel() error {
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
			b.logger.Info("CF [%s] completed: %s keys in %v",
				cfn, helpers.FormatNumber(int64(stats.KeyCount)), stats.BuildTime)
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
		LessFalsePositives: true,
		BucketSize:         BucketSize,
		LeafSize:           LeafSize,
		TmpDir:             cfTmpDir,
		IndexFile:          indexPath,
		BaseDataID:         0,
		Version:            DataVersion,
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

	// Clean up temp directory for this CF
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
