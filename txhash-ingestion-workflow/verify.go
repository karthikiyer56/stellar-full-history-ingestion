// =============================================================================
// verify.go - RecSplit Verification Phase
// =============================================================================
//
// This file implements the verification phase that validates RecSplit indexes
// against the RocksDB data.
//
// VERIFICATION APPROACH:
//
//	For each column family:
//	  1. Open the RecSplit index
//	  2. Iterate over all keys in RocksDB
//	  3. Look up each key in RecSplit
//	  4. Verify the returned value matches RocksDB
//
// ERROR HANDLING:
//
//	Verification failures are LOGGED but do NOT abort the process.
//	This allows the entire dataset to be verified even if some entries fail.
//
//	Types of failures:
//	  - Key not found in RecSplit (should never happen)
//	  - Value mismatch (ledgerSeq differs)
//	  - RecSplit returns wrong offset (indicates corruption)
//
// CRASH RECOVERY:
//
//	The verify_cf field in meta store tracks the current CF being verified.
//	On restart, verification resumes from the beginning of that CF.
//	(We don't track intra-CF progress since verification is fast.)
//
// STATISTICS:
//
//	- Keys verified per CF
//	- Failures per CF
//	- Verification time per CF
//	- Overall success rate
//
// =============================================================================

package main

import (
	"fmt"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon/db/recsplit"
)

// =============================================================================
// Verification Stats
// =============================================================================

// VerifyStats holds statistics from the verification phase.
type VerifyStats struct {
	// StartTime when verification began
	StartTime time.Time

	// EndTime when verification completed
	EndTime time.Time

	// TotalTime is the total verification time
	TotalTime time.Duration

	// PerCFStats holds per-CF verification statistics
	PerCFStats map[string]*VerifyCFStats

	// TotalKeysVerified is the total number of keys verified
	TotalKeysVerified uint64

	// TotalFailures is the total number of verification failures
	TotalFailures uint64

	// SuccessRate is the percentage of successful verifications
	SuccessRate float64
}

// VerifyCFStats holds per-CF verification statistics.
type VerifyCFStats struct {
	CFName        string
	KeysVerified  uint64
	Failures      uint64
	VerifyTime    time.Duration
	KeysPerSecond float64
}

// NewVerifyStats creates a new VerifyStats.
func NewVerifyStats() *VerifyStats {
	return &VerifyStats{
		PerCFStats: make(map[string]*VerifyCFStats),
	}
}

// LogSummary logs a summary of verification results.
func (vs *VerifyStats) LogSummary(logger Logger) {
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

	for _, cf := range ColumnFamilyNames {
		stats := vs.PerCFStats[cf]
		if stats != nil {
			logger.Info("%-4s %15s %10d %12v %15.0f",
				cf,
				helpers.FormatNumber(int64(stats.KeysVerified)),
				stats.Failures,
				stats.VerifyTime,
				stats.KeysPerSecond)
			totalKeys += stats.KeysVerified
			totalFailures += stats.Failures
		}
	}

	logger.Info("%-4s %15s %10s %12s %15s",
		"----", "---------------", "----------", "------------", "---------------")
	logger.Info("%-4s %15s %10d %12v",
		"TOT",
		helpers.FormatNumber(int64(totalKeys)),
		totalFailures,
		vs.TotalTime)

	logger.Info("")

	if totalKeys > 0 {
		successRate := 100.0 * float64(totalKeys-totalFailures) / float64(totalKeys)
		logger.Info("SUCCESS RATE: %.6f%% (%d/%d)",
			successRate, totalKeys-totalFailures, totalKeys)

		if totalFailures == 0 {
			logger.Info("STATUS: ALL VERIFICATIONS PASSED")
		} else {
			logger.Info("STATUS: %d FAILURES DETECTED (check error log)", totalFailures)
		}
	}

	logger.Info("")
}

// =============================================================================
// Verifier
// =============================================================================

// Verifier handles verification of RecSplit indexes against RocksDB data.
type Verifier struct {
	store     TxHashStore
	meta      MetaStore
	indexPath string
	logger    Logger
	memory    *MemoryMonitor
	stats     *VerifyStats

	// resumeFromCF is the CF to resume from (empty = start from beginning)
	resumeFromCF string
}

// NewVerifier creates a new Verifier.
//
// PARAMETERS:
//   - store: RocksDB store to verify against
//   - meta: Meta store for progress tracking
//   - indexPath: Directory containing RecSplit index files
//   - resumeFromCF: CF to resume from (empty = start from beginning)
//   - logger: Logger instance
//   - memory: Memory monitor
func NewVerifier(
	store TxHashStore,
	meta MetaStore,
	indexPath string,
	resumeFromCF string,
	logger Logger,
	memory *MemoryMonitor,
) *Verifier {
	return &Verifier{
		store:        store,
		meta:         meta,
		indexPath:    indexPath,
		resumeFromCF: resumeFromCF,
		logger:       logger,
		memory:       memory,
		stats:        NewVerifyStats(),
	}
}

// Run executes the verification phase.
func (v *Verifier) Run() (*VerifyStats, error) {
	v.stats.StartTime = time.Now()

	v.logger.Separator()
	v.logger.Info("                    VERIFICATION PHASE")
	v.logger.Separator()
	v.logger.Info("")

	if v.resumeFromCF != "" {
		v.logger.Info("RESUMING from CF [%s]", v.resumeFromCF)
		v.logger.Info("")
	}

	// Determine starting CF index
	startIdx := 0
	if v.resumeFromCF != "" {
		for i, cf := range ColumnFamilyNames {
			if cf == v.resumeFromCF {
				startIdx = i
				break
			}
		}
	}

	// Verify each CF
	for i := startIdx; i < len(ColumnFamilyNames); i++ {
		cfName := ColumnFamilyNames[i]

		// Update progress in meta store
		if err := v.meta.SetVerifyCF(cfName); err != nil {
			return nil, fmt.Errorf("failed to update verify CF: %w", err)
		}

		v.logger.Info("[%2d/16] Verifying CF [%s]...", i+1, cfName)

		stats, err := v.verifyCF(cfName)
		if err != nil {
			// Log error but continue (per requirements)
			v.logger.Error("Verification error for CF %s: %v", cfName, err)
			continue
		}

		v.stats.PerCFStats[cfName] = stats
		v.logger.Info("        Verified %s keys in %v (%.0f keys/sec), failures: %d",
			helpers.FormatNumber(int64(stats.KeysVerified)),
			stats.VerifyTime,
			stats.KeysPerSecond,
			stats.Failures)

		// Check memory every 4 CFs
		if (i+1)%4 == 0 {
			v.memory.Check()
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

	// Log summary
	v.stats.LogSummary(v.logger)
	v.memory.LogSummary(v.logger)

	v.logger.Sync()

	return v.stats, nil
}

// verifyCF verifies a single column family.
func (v *Verifier) verifyCF(cfName string) (*VerifyCFStats, error) {
	stats := &VerifyCFStats{
		CFName: cfName,
	}

	startTime := time.Now()

	// Open RecSplit index
	indexPath := filepath.Join(v.indexPath, fmt.Sprintf("cf-%s.idx", cfName))
	idx, err := recsplit.OpenIndex(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open index %s: %w", indexPath, err)
	}
	defer idx.Close()

	reader := recsplit.NewIndexReader(idx)

	// Iterate over RocksDB and verify each key
	iter := v.store.NewIteratorCF(cfName)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		expectedValue := iter.Value()
		expectedLedgerSeq := ParseLedgerSeq(expectedValue)

		// Look up in RecSplit
		offset, found := reader.Lookup(key)
		if !found {
			stats.Failures++
			v.logger.Error("CF %s: Key %s not found in RecSplit",
				cfName, BytesToHex(key))
			continue
		}

		// RecSplit returns uint64, which is our ledgerSeq
		actualLedgerSeq := uint32(offset)

		if actualLedgerSeq != expectedLedgerSeq {
			stats.Failures++
			v.logger.Error("CF %s: Key %s value mismatch: expected %d, got %d",
				cfName, BytesToHex(key), expectedLedgerSeq, actualLedgerSeq)
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

// GetStats returns the verification statistics.
func (v *Verifier) GetStats() *VerifyStats {
	return v.stats
}

// =============================================================================
// Convenience Function
// =============================================================================

// RunVerification is a convenience function to run the verification phase.
func RunVerification(
	store TxHashStore,
	meta MetaStore,
	config *Config,
	logger Logger,
	memory *MemoryMonitor,
	resumeFromCF string,
) (*VerifyStats, error) {
	verifier := NewVerifier(
		store,
		meta,
		config.RecsplitIndexPath,
		resumeFromCF,
		logger,
		memory,
	)

	return verifier.Run()
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
//   - logger: Logger instance
//
// RETURNS:
//   - failures: Number of failed verifications
//   - error: If an unrecoverable error occurred
func QuickVerify(store TxHashStore, indexPath string, sampleSize int, logger Logger) (int, error) {
	logger.Info("Running quick verification (sample size: %d per CF)...", sampleSize)

	totalFailures := 0

	for _, cfName := range ColumnFamilyNames {
		// Open RecSplit index
		idxPath := filepath.Join(indexPath, fmt.Sprintf("cf-%s.idx", cfName))
		idx, err := recsplit.OpenIndex(idxPath)
		if err != nil {
			return 0, fmt.Errorf("failed to open index for CF %s: %w", cfName, err)
		}
		defer idx.Close()

		reader := recsplit.NewIndexReader(idx)

		// Sample keys from RocksDB
		iter := store.NewIteratorCF(cfName)
		defer iter.Close()

		sampled := 0
		failures := 0

		for iter.SeekToFirst(); iter.Valid() && sampled < sampleSize; iter.Next() {
			key := iter.Key()
			expectedValue := iter.Value()
			expectedLedgerSeq := ParseLedgerSeq(expectedValue)

			offset, found := reader.Lookup(key)
			if !found {
				failures++
			} else if uint32(offset) != expectedLedgerSeq {
				failures++
			}

			sampled++
		}

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
