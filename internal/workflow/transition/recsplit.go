package transition

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	erigonlog "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/stores/txhash/cf"
)

// =============================================================================
// RecSplit Builder - Builds 16 index files in parallel
// =============================================================================

// RecSplit index configuration constants
const (
	RecSplitBucketSize                = 2000
	RecSplitLeafSize                  = 8
	RecSplitDataVersion               = 1
	RecSplitLessFalsePositivesEnabled = true
)

// recsplitBuilder implements RecSplitBuilder interface.
// Builds 16 RecSplit index files (cf-0.idx through cf-f.idx) in parallel.
type recsplitBuilder struct {
	dataDir string
	log     interfaces.Logger
}

// NewRecSplitBuilder creates a new RecSplit builder.
//
// Parameters:
//   - dataDir: Base data directory (e.g., /data/stellar)
//   - log: Logger instance
//
// Output files are written to:
//
//	{dataDir}/immutable/txhash/{rangeID:04d}/index/cf-{0-f}.idx
func NewRecSplitBuilder(dataDir string, log interfaces.Logger) *recsplitBuilder {
	return &recsplitBuilder{
		dataDir: dataDir,
		log:     log.WithScope("RECSPLIT"),
	}
}

// BuildAll builds RecSplit indexes for all 16 column families in parallel.
//
// Process:
//  1. Create output directory: immutable/txhash/{rangeID:04d}/index/
//  2. Spawn 16 goroutines, one per CF
//  3. Each goroutine:
//     - Creates RecSplit builder with exact count from cfCounts
//     - Iterates CF using NewScanIteratorCF
//     - Adds all keys to RecSplit builder
//     - Verifies count matches (DEFINITIVE CHECK)
//     - Writes index file: cf-{name}.idx
//  4. Returns per-CF build durations
//
// CRASH RECOVERY:
//   - Not resumable mid-build
//   - On restart, delete all .idx files and rebuild from scratch
//
// COUNT VERIFICATION:
//   - Compares iterated key count vs cfCounts (from meta store)
//   - Build FAILS if counts don't match (cannot build index with wrong count)
//   - This is the definitive count check (post-compaction verification is early warning only)
//
// Returns:
//   - map[string]time.Duration: Per-CF build durations (CF name → duration)
//   - error: First error encountered, if any
func (rb *recsplitBuilder) BuildAll(
	rangeID uint32,
	cfCounts map[string]uint64,
	store interfaces.TxHashStore,
) (map[string]time.Duration, error) {

	// Create output directory: immutable/txhash/{rangeID:04d}/index/
	indexDir := filepath.Join(rb.dataDir, "immutable", "txhash", fmt.Sprintf("%04d", rangeID), "index")
	if err := helpers.EnsureDir(indexDir); err != nil {
		return nil, fmt.Errorf("failed to create index directory: %w", err)
	}

	rb.log.Info("Building RecSplit indexes for range %d (16 CFs in parallel)", rangeID)
	rb.log.Info("Output directory: %s", indexDir)
	rb.log.Info("")

	// Build all 16 CFs in parallel
	results := make(map[string]time.Duration)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errChan := make(chan error, 16)

	for i, cfName := range cf.Names {
		wg.Add(1)
		go func(idx int, name string) {
			defer wg.Done()

			count := cfCounts[name]
			if count == 0 {
				rb.log.Warn("CF %s has 0 entries, skipping RecSplit build", name)
				mu.Lock()
				results[name] = 0
				mu.Unlock()
				return
			}

			start := time.Now()
			outputPath := filepath.Join(indexDir, fmt.Sprintf("cf-%s.idx", name))

			// Build RecSplit for this CF
			if err := rb.buildCF(name, count, store, outputPath); err != nil {
				errChan <- fmt.Errorf("CF %s build failed: %w", name, err)
				return
			}

			duration := time.Since(start)
			mu.Lock()
			results[name] = duration
			mu.Unlock()

			rb.log.Info("[%2d/16] CF [%s]: RecSplit built in %s (%s keys)",
				idx+1, name, helpers.FormatDuration(duration), helpers.FormatNumber(int64(count)))
		}(i, cfName)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	if len(errChan) > 0 {
		return nil, <-errChan
	}

	rb.log.Info("")
	rb.log.Info("Range %d: RecSplit build complete (16 CFs)", rangeID)
	return results, nil
}

// buildCF builds a RecSplit index for a single column family.
//
// Process:
//  1. Create RecSplit builder with exact key count
//  2. Iterate CF using NewScanIteratorCF (scan-optimized with readahead)
//  3. Add each key to RecSplit builder
//  4. Verify iterated count == expected count (DEFINITIVE CHECK)
//  5. Build index and write to file
//
// Parameters:
//   - cfName: Column family name ("0" through "f")
//   - count: Expected key count from meta store
//   - store: TxHashStore to read keys from
//   - outputPath: Full path to output .idx file
//
// Returns:
//   - error: If iteration fails, count mismatch, or build fails
func (rb *recsplitBuilder) buildCF(
	cfName string,
	count uint64,
	store interfaces.TxHashStore,
	outputPath string,
) error {

	// Create temporary directory for RecSplit build (Erigon requires this)
	tmpDir := filepath.Join(rb.dataDir, "immutable", "txhash", "tmp", cfName)
	if err := helpers.EnsureDir(tmpDir); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Create RecSplit builder with exact key count
	erigonLogger := erigonlog.New()

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:           int(count),
		Enums:              false, // We store ledgerSeq as offset, not enum
		LessFalsePositives: RecSplitLessFalsePositivesEnabled,
		BucketSize:         RecSplitBucketSize,
		LeafSize:           RecSplitLeafSize,
		TmpDir:             tmpDir,
		IndexFile:          outputPath,
		BaseDataID:         0,
		Version:            RecSplitDataVersion,
	}, erigonLogger)
	if err != nil {
		return fmt.Errorf("failed to create RecSplit builder: %w", err)
	}
	defer rs.Close()

	// Iterate CF and add keys.
	// Why NewScanIteratorCF? It's optimized for full sequential scans with:
	//   - 2MB readahead buffer (reduces I/O syscalls)
	//   - FillCache=false (don't pollute block cache during one-time full scan)
	// Alternative NewIteratorCF would use default 256KB readahead and fill cache.
	iter := store.NewScanIteratorCF(cfName)
	defer iter.Close()

	keysSeen := uint64(0)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		txHash := iter.Key()  // 32-byte hash
		value := iter.Value() // 4-byte ledgerSeq (big-endian)

		// Parse ledgerSeq from value (big-endian uint32)
		if len(value) != 4 {
			return fmt.Errorf("invalid value length: expected 4 bytes, got %d", len(value))
		}
		ledgerSeq := uint32(value[0])<<24 | uint32(value[1])<<16 | uint32(value[2])<<8 | uint32(value[3])

		// Add key to RecSplit (offset = ledgerSeq)
		if err := rs.AddKey(txHash, uint64(ledgerSeq)); err != nil {
			return fmt.Errorf("failed to add key %d: %w", keysSeen, err)
		}

		keysSeen++
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	// DEFINITIVE COUNT CHECK: Verify iterated count matches expected count
	// This is the authoritative verification - build FAILS if counts don't match
	if keysSeen != count {
		return fmt.Errorf("count mismatch: expected %d keys, iterated %d keys", count, keysSeen)
	}

	// Build the index
	ctx := context.Background()
	if err := rs.Build(ctx); err != nil {
		if err == recsplit.ErrCollision {
			return fmt.Errorf("hash collision detected (rare, try rebuilding)")
		}
		return fmt.Errorf("failed to build index: %w", err)
	}

	return nil
}

// Compile-time interface check
var _ interfaces.RecSplitBuilder = (*recsplitBuilder)(nil)
