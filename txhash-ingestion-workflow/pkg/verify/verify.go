// =============================================================================
// pkg/verify/verify.go - RecSplit Verification Phase
// =============================================================================
//
// This package implements the verification phase that validates RecSplit indexes
// against the RocksDB data.
//
// Two modes are supported:
//   - Single-Index Mode (multiIndex=false): Opens txhash.idx once, shares reader
//     across 16 parallel goroutines verifying each CF
//   - Multi-Index Mode (multiIndex=true): Each goroutine opens its own cf-X.idx
//
// =============================================================================

package verify

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/erigontech/erigon/db/recsplit"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/cf"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/types"
)

// =============================================================================
// Verification Stats
// =============================================================================

// Stats holds statistics from the verification phase.
type Stats struct {
	// StartTime when verification began
	StartTime time.Time

	// EndTime when verification completed
	EndTime time.Time

	// TotalTime is the total verification time
	TotalTime time.Duration

	// PerCFStats holds per-CF verification statistics
	PerCFStats map[string]*CFStats

	// TotalKeysVerified is the total number of keys verified
	TotalKeysVerified uint64

	// TotalFailures is the total number of verification failures
	TotalFailures uint64

	// SuccessRate is the percentage of successful verifications
	SuccessRate float64
}

// CFStats holds per-CF verification statistics.
type CFStats struct {
	CFName        string
	KeysVerified  uint64
	Failures      uint64
	VerifyTime    time.Duration
	KeysPerSecond float64
}

// NewStats creates a new Stats.
func NewStats() *Stats {
	return &Stats{
		PerCFStats: make(map[string]*CFStats),
	}
}

// LogSummary logs a summary of verification results.
func (vs *Stats) LogSummary(logger interfaces.Logger) {
	logger.Separator()
	logger.Info("                    VERIFICATION SUMMARY")
	logger.Separator()
	logger.Info("")

	logger.Info("PER-CF STATISTICS:")
	logger.Info("%-4s %15s %10s %12s %15s",
		"CF", "Keys Verified", "Failures", "Time", "Keys/sec")
	logger.Info("%-4s %15s %10s %12s %15s",
		"----", "---------------", "----------", "------------", "---------------")

	var totalKeys, totalFailures uint64

	for _, cfName := range cf.Names {
		stats := vs.PerCFStats[cfName]
		if stats != nil {
			logger.Info("%-4s %15s %10s %12s %15.0f",
				cfName,
				helpers.FormatNumber(int64(stats.KeysVerified)),
				helpers.FormatNumber(int64(stats.Failures)),
				helpers.FormatDuration(stats.VerifyTime),
				stats.KeysPerSecond)
			totalKeys += stats.KeysVerified
			totalFailures += stats.Failures
		}
	}

	logger.Info("%-4s %15s %10s %12s %15s",
		"----", "---------------", "----------", "------------", "---------------")
	logger.Info("%-4s %15s %10s %12s",
		"TOT",
		helpers.FormatNumber(int64(totalKeys)),
		helpers.FormatNumber(int64(totalFailures)),
		helpers.FormatDuration(vs.TotalTime))

	logger.Info("")

	if totalKeys > 0 {
		successRate := 100.0 * float64(totalKeys-totalFailures) / float64(totalKeys)
		logger.Info("SUCCESS RATE: %.6f%% (%s/%s)",
			successRate, helpers.FormatNumber(int64(totalKeys-totalFailures)), helpers.FormatNumber(int64(totalKeys)))

		if totalFailures == 0 {
			logger.Info("STATUS: ALL VERIFICATIONS PASSED")
		} else {
			logger.Info("STATUS: %s FAILURES DETECTED (check error log)", helpers.FormatNumber(int64(totalFailures)))
		}
	}

	logger.Info("")
}

// =============================================================================
// Verifier
// =============================================================================

// Verifier handles verification of RecSplit indexes against RocksDB data.
// Verification runs in parallel across all 16 column families.
type Verifier struct {
	store      interfaces.TxHashStore
	meta       interfaces.MetaStore
	indexPath  string
	multiIndex bool
	logger     interfaces.Logger
	memory     interfaces.MemoryMonitor
	stats      *Stats

	// resumeFromCF is deprecated and ignored. Kept for backward compatibility.
	// Parallel verification always verifies all CFs from the beginning.
	resumeFromCF string
}

// NewVerifier creates a new Verifier.
//
// PARAMETERS:
//   - store: RocksDB store to verify against
//   - meta: Meta store for progress tracking (not used for per-CF checkpoints)
//   - indexPath: Directory containing RecSplit index files
//   - resumeFromCF: DEPRECATED - ignored, kept for backward compatibility
//   - multiIndex: If true, expects 16 cf-X.idx files; if false, expects single txhash.idx
//   - logger: Logger instance
//   - mem: Memory monitor
func NewVerifier(
	store interfaces.TxHashStore,
	meta interfaces.MetaStore,
	indexPath string,
	resumeFromCF string,
	multiIndex bool,
	logger interfaces.Logger,
	mem interfaces.MemoryMonitor,
) *Verifier {
	return &Verifier{
		store:        store,
		meta:         meta,
		indexPath:    indexPath,
		resumeFromCF: resumeFromCF,
		multiIndex:   multiIndex,
		logger:       logger,
		memory:       mem,
		stats:        NewStats(),
	}
}

// Run executes the verification phase.
//
// All 16 column families are verified in parallel using goroutines.
// Verification is read-only and idempotent, so parallel execution is safe.
// On crash, verification simply restarts from the beginning (no per-CF checkpoint needed).
func (v *Verifier) Run() (*Stats, error) {
	v.stats.StartTime = time.Now()

	if v.multiIndex {
		return v.runMultiIndex()
	}
	return v.runSingleIndex()
}

// =============================================================================
// Single-Index Mode
// =============================================================================

// runSingleIndex verifies using a single combined txhash.idx file.
// Opens the index once and shares the reader across 16 parallel goroutines.
func (v *Verifier) runSingleIndex() (*Stats, error) {
	v.logger.Separator()
	v.logger.Info("                    VERIFICATION PHASE (SINGLE INDEX)")
	v.logger.Separator()
	v.logger.Info("")

	v.logger.Info("Opening combined index: txhash.idx")
	v.logger.Info("Verifying all 16 column families in parallel with shared reader...")
	v.logger.Info("")

	// Open combined index once
	indexPath := filepath.Join(v.indexPath, "txhash.idx")
	idx, err := recsplit.OpenIndex(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open index %s: %w", indexPath, err)
	}
	defer idx.Close()

	// Create shared reader - RecSplit readers are thread-safe
	reader := recsplit.NewIndexReader(idx)

	// Pre-allocate results slice to maintain CF order
	results := make([]*CFStats, len(cf.Names))
	errors := make([]error, len(cf.Names))

	// Verify all CFs in parallel with shared reader
	var wg sync.WaitGroup
	for i, cfName := range cf.Names {
		wg.Add(1)
		go func(idx int, name string) {
			defer wg.Done()

			stats, err := v.verifyCFWithReader(name, reader)
			if err != nil {
				errors[idx] = err
				v.logger.Error("Verification error for CF %s: %v", name, err)
				return
			}

			results[idx] = stats
			v.logger.Info("  CF [%s] verified: %s keys in %s (%.0f keys/sec), failures: %s",
				name,
				helpers.FormatNumber(int64(stats.KeysVerified)),
				helpers.FormatDuration(stats.VerifyTime),
				stats.KeysPerSecond,
				helpers.FormatNumber(int64(stats.Failures)))
		}(i, cfName)
	}
	wg.Wait()

	return v.collectResults(results, errors)
}

// verifyCFWithReader verifies a single column family using a shared reader.
func (v *Verifier) verifyCFWithReader(cfName string, reader *recsplit.IndexReader) (*CFStats, error) {
	stats := &CFStats{
		CFName: cfName,
	}

	startTime := time.Now()

	// Iterate over RocksDB and verify each key using scan-optimized iterator
	iter := v.store.NewScanIteratorCF(cfName)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		expectedValue := iter.Value()
		expectedLedgerSeq := types.ParseLedgerSeq(expectedValue)

		// Look up in RecSplit
		offset, found := reader.Lookup(key)
		if !found {
			stats.Failures++
			v.logger.Error("CF %s: Key %s not found in RecSplit",
				cfName, types.BytesToHex(key))
			continue
		}

		// RecSplit returns uint64, which is our ledgerSeq
		actualLedgerSeq := uint32(offset)

		if actualLedgerSeq != expectedLedgerSeq {
			stats.Failures++
			v.logger.Error("CF %s: Key %s value mismatch: expected %d, got %d",
				cfName, types.BytesToHex(key), expectedLedgerSeq, actualLedgerSeq)
			continue
		}

		stats.KeysVerified++
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	stats.VerifyTime = time.Since(startTime)
	if stats.VerifyTime.Seconds() > 0 {
		stats.KeysPerSecond = float64(stats.KeysVerified) / stats.VerifyTime.Seconds()
	}

	return stats, nil
}

// =============================================================================
// Multi-Index Mode
// =============================================================================

// runMultiIndex verifies using 16 separate cf-X.idx files.
// Each goroutine opens its own index file.
func (v *Verifier) runMultiIndex() (*Stats, error) {
	v.logger.Separator()
	v.logger.Info("                    VERIFICATION PHASE (MULTI INDEX)")
	v.logger.Separator()
	v.logger.Info("")

	v.logger.Info("Verifying all 16 column families in parallel (each with own cf-X.idx)...")
	v.logger.Info("")

	// Pre-allocate results slice to maintain CF order
	results := make([]*CFStats, len(cf.Names))
	errors := make([]error, len(cf.Names))

	// Verify all CFs in parallel
	var wg sync.WaitGroup
	for i, cfName := range cf.Names {
		wg.Add(1)
		go func(idx int, name string) {
			defer wg.Done()

			stats, err := v.verifyCF(name)
			if err != nil {
				errors[idx] = err
				v.logger.Error("Verification error for CF %s: %v", name, err)
				return
			}

			results[idx] = stats
			v.logger.Info("  CF [%s] verified: %s keys in %s (%.0f keys/sec), failures: %s",
				name,
				helpers.FormatNumber(int64(stats.KeysVerified)),
				helpers.FormatDuration(stats.VerifyTime),
				stats.KeysPerSecond,
				helpers.FormatNumber(int64(stats.Failures)))
		}(i, cfName)
	}
	wg.Wait()

	return v.collectResults(results, errors)
}

// verifyCF verifies a single column family using its own cf-X.idx file.
func (v *Verifier) verifyCF(cfName string) (*CFStats, error) {
	stats := &CFStats{
		CFName: cfName,
	}

	startTime := time.Now()

	// Open RecSplit index for this CF
	indexPath := filepath.Join(v.indexPath, fmt.Sprintf("cf-%s.idx", cfName))
	idx, err := recsplit.OpenIndex(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open index %s: %w", indexPath, err)
	}
	defer idx.Close()

	reader := recsplit.NewIndexReader(idx)

	// Iterate over RocksDB and verify each key using scan-optimized iterator
	iter := v.store.NewScanIteratorCF(cfName)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		expectedValue := iter.Value()
		expectedLedgerSeq := types.ParseLedgerSeq(expectedValue)

		// Look up in RecSplit
		offset, found := reader.Lookup(key)
		if !found {
			stats.Failures++
			v.logger.Error("CF %s: Key %s not found in RecSplit",
				cfName, types.BytesToHex(key))
			continue
		}

		// RecSplit returns uint64, which is our ledgerSeq
		actualLedgerSeq := uint32(offset)

		if actualLedgerSeq != expectedLedgerSeq {
			stats.Failures++
			v.logger.Error("CF %s: Key %s value mismatch: expected %d, got %d",
				cfName, types.BytesToHex(key), expectedLedgerSeq, actualLedgerSeq)
			continue
		}

		stats.KeysVerified++
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	stats.VerifyTime = time.Since(startTime)
	if stats.VerifyTime.Seconds() > 0 {
		stats.KeysPerSecond = float64(stats.KeysVerified) / stats.VerifyTime.Seconds()
	}

	return stats, nil
}

// =============================================================================
// Shared Helpers
// =============================================================================

// collectResults gathers results from parallel verification and updates stats.
func (v *Verifier) collectResults(results []*CFStats, errors []error) (*Stats, error) {
	// Collect results (maintain order)
	for i, cfName := range cf.Names {
		if results[i] != nil {
			v.stats.PerCFStats[cfName] = results[i]
		}
	}

	v.stats.EndTime = time.Now()
	v.stats.TotalTime = time.Since(v.stats.StartTime)

	// Calculate totals
	for _, stats := range v.stats.PerCFStats {
		v.stats.TotalKeysVerified += stats.KeysVerified
		v.stats.TotalFailures += stats.Failures
	}

	if v.stats.TotalKeysVerified > 0 {
		v.stats.SuccessRate = 100.0 * float64(v.stats.TotalKeysVerified-v.stats.TotalFailures) /
			float64(v.stats.TotalKeysVerified)
	}

	// Check memory at end
	v.memory.Check()

	// Log summary
	v.stats.LogSummary(v.logger)
	v.memory.LogSummary(v.logger)

	v.logger.Sync()

	return v.stats, nil
}

// GetStats returns the verification statistics.
func (v *Verifier) GetStats() *Stats {
	return v.stats
}

// =============================================================================
// Quick Verification (Sample-Based)
// =============================================================================

// QuickVerify performs a quick verification using random sampling.
//
// This is useful for a fast sanity check without verifying all keys.
//
// PARAMETERS:
//   - store: RocksDB store
//   - indexPath: RecSplit index directory
//   - sampleSize: Number of keys to sample per CF
//   - multiIndex: If true, expects 16 cf-X.idx files; if false, expects single txhash.idx
//   - logger: Logger instance
//
// RETURNS:
//   - failures: Number of failed verifications
//   - error: If an unrecoverable error occurred
func QuickVerify(store interfaces.TxHashStore, indexPath string, sampleSize int, multiIndex bool, logger interfaces.Logger) (int, error) {
	logger.Info("Running quick verification (sample size: %d per CF)...", sampleSize)

	if multiIndex {
		return quickVerifyMultiIndex(store, indexPath, sampleSize, logger)
	}
	return quickVerifySingleIndex(store, indexPath, sampleSize, logger)
}

// quickVerifySingleIndex performs quick verification against txhash.idx.
func quickVerifySingleIndex(store interfaces.TxHashStore, indexPath string, sampleSize int, logger interfaces.Logger) (int, error) {
	// Open combined index once
	idxPath := filepath.Join(indexPath, "txhash.idx")
	idx, err := recsplit.OpenIndex(idxPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open index: %w", err)
	}
	defer idx.Close()

	reader := recsplit.NewIndexReader(idx)
	totalFailures := 0

	for _, cfName := range cf.Names {
		// Sample keys from RocksDB
		iter := store.NewIteratorCF(cfName)

		sampled := 0
		failures := 0

		for iter.SeekToFirst(); iter.Valid() && sampled < sampleSize; iter.Next() {
			key := iter.Key()
			expectedValue := iter.Value()
			expectedLedgerSeq := types.ParseLedgerSeq(expectedValue)

			offset, found := reader.Lookup(key)
			if !found {
				failures++
			} else if uint32(offset) != expectedLedgerSeq {
				failures++
			}

			sampled++
		}

		iter.Close()

		if failures > 0 {
			logger.Error("CF %s: %d/%d samples failed", cfName, failures, sampled)
			totalFailures += failures
		} else {
			logger.Info("CF %s: %d/%d samples passed", cfName, sampled, sampled)
		}
	}

	if totalFailures == 0 {
		logger.Info("Quick verification PASSED")
	} else {
		logger.Error("Quick verification FAILED: %d total failures", totalFailures)
	}

	return totalFailures, nil
}

// quickVerifyMultiIndex performs quick verification against cf-X.idx files.
func quickVerifyMultiIndex(store interfaces.TxHashStore, indexPath string, sampleSize int, logger interfaces.Logger) (int, error) {
	totalFailures := 0

	for _, cfName := range cf.Names {
		// Open RecSplit index for this CF
		idxPath := filepath.Join(indexPath, fmt.Sprintf("cf-%s.idx", cfName))
		idx, err := recsplit.OpenIndex(idxPath)
		if err != nil {
			return 0, fmt.Errorf("failed to open index for CF %s: %w", cfName, err)
		}

		reader := recsplit.NewIndexReader(idx)

		// Sample keys from RocksDB
		iter := store.NewIteratorCF(cfName)

		sampled := 0
		failures := 0

		for iter.SeekToFirst(); iter.Valid() && sampled < sampleSize; iter.Next() {
			key := iter.Key()
			expectedValue := iter.Value()
			expectedLedgerSeq := types.ParseLedgerSeq(expectedValue)

			offset, found := reader.Lookup(key)
			if !found {
				failures++
			} else if uint32(offset) != expectedLedgerSeq {
				failures++
			}

			sampled++
		}

		iter.Close()
		idx.Close()

		if failures > 0 {
			logger.Error("CF %s: %d/%d samples failed", cfName, failures, sampled)
			totalFailures += failures
		} else {
			logger.Info("CF %s: %d/%d samples passed", cfName, sampled, sampled)
		}
	}

	if totalFailures == 0 {
		logger.Info("Quick verification PASSED")
	} else {
		logger.Error("Quick verification FAILED: %d total failures", totalFailures)
	}

	return totalFailures, nil
}
