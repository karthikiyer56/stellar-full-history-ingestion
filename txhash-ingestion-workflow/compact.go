// =============================================================================
// compact.go - RocksDB Compaction Phase
// =============================================================================
//
// This file implements the compaction phase that runs after ingestion completes.
//
// WHY COMPACTION IS NECESSARY:
//
//	During ingestion, auto-compaction is disabled to maximize write throughput.
//	This results in all data landing in L0 (level 0) as overlapping SST files.
//
//	Compaction:
//	  1. Merges L0 files into sorted, non-overlapping files
//	  2. Removes duplicate keys (from crash recovery re-ingestion)
//	  3. Organizes data into levels (typically L6 after full compaction)
//	  4. Optimizes read performance for RecSplit building
//
// COMPACTION STRATEGY:
//
//	We perform full compaction on all 16 column families sequentially.
//	This is simpler than parallel compaction and sufficient for our use case.
//
// CRASH RECOVERY:
//
//	If compaction is interrupted, we restart it from the beginning.
//	Compaction is idempotent - re-compacting already-compacted data is safe.
//
// STATISTICS COLLECTED:
//
//	Before compaction:
//	  - SST file counts per level per CF
//	  - Total data size per CF
//	  - Estimated key counts
//
//	After compaction:
//	  - Same metrics (to show reduction)
//	  - Compaction time per CF
//	  - Total compaction time
//
// =============================================================================

package main

import (
	"fmt"
	"time"
)

// =============================================================================
// CompactionStats - Statistics for Compaction Phase
// =============================================================================

// CompactionStats holds statistics from the compaction phase.
type CompactionStats struct {
	// StartTime when compaction began
	StartTime time.Time

	// EndTime when compaction completed
	EndTime time.Time

	// PerCFTime holds the compaction time for each CF
	PerCFTime map[string]time.Duration

	// TotalTime is the total compaction time
	TotalTime time.Duration

	// BeforeStats holds CF stats before compaction
	BeforeStats []CFStats

	// AfterStats holds CF stats after compaction
	AfterStats []CFStats
}

// NewCompactionStats creates a new CompactionStats.
func NewCompactionStats() *CompactionStats {
	return &CompactionStats{
		PerCFTime: make(map[string]time.Duration),
	}
}

// LogSummary logs a summary of compaction statistics.
func (cs *CompactionStats) LogSummary(logger Logger) {
	logger.Separator()
	logger.Info("                    COMPACTION SUMMARY")
	logger.Separator()
	logger.Info("")

	// Per-CF times
	logger.Info("COMPACTION TIME BY COLUMN FAMILY:")
	for _, cf := range ColumnFamilyNames {
		elapsed := cs.PerCFTime[cf]
		logger.Info("  CF %s: %v", cf, elapsed)
	}
	logger.Info("")
	logger.Info("Total Compaction Time: %v", cs.TotalTime)
	logger.Info("")
}

// LogBeforeAfterComparison logs a before/after comparison.
func (cs *CompactionStats) LogBeforeAfterComparison(logger Logger) {
	logger.Info("BEFORE/AFTER COMPARISON:")
	logger.Info("")

	// Build maps for easy lookup
	beforeMap := make(map[string]CFStats)
	afterMap := make(map[string]CFStats)
	for _, s := range cs.BeforeStats {
		beforeMap[s.Name] = s
	}
	for _, s := range cs.AfterStats {
		afterMap[s.Name] = s
	}

	// Summary header
	logger.Info("%-4s %12s %12s %12s %12s",
		"CF", "Files Before", "Files After", "Size Before", "Size After")
	logger.Info("%-4s %12s %12s %12s %12s",
		"----", "------------", "-----------", "-----------", "----------")

	var totalFilesBefore, totalFilesAfter, totalSizeBefore, totalSizeAfter int64

	for _, cf := range ColumnFamilyNames {
		before := beforeMap[cf]
		after := afterMap[cf]

		logger.Info("%-4s %12d %12d %12s %12s",
			cf,
			before.TotalFiles,
			after.TotalFiles,
			FormatBytes(before.TotalSize),
			FormatBytes(after.TotalSize))

		totalFilesBefore += before.TotalFiles
		totalFilesAfter += after.TotalFiles
		totalSizeBefore += before.TotalSize
		totalSizeAfter += after.TotalSize
	}

	logger.Info("%-4s %12s %12s %12s %12s",
		"----", "------------", "-----------", "-----------", "----------")
	logger.Info("%-4s %12d %12d %12s %12s",
		"TOT",
		totalFilesBefore,
		totalFilesAfter,
		FormatBytes(totalSizeBefore),
		FormatBytes(totalSizeAfter))

	// Calculate reduction percentages
	if totalFilesBefore > 0 {
		fileReduction := 100.0 * float64(totalFilesBefore-totalFilesAfter) / float64(totalFilesBefore)
		logger.Info("")
		logger.Info("File count reduction: %.1f%%", fileReduction)
	}

	if totalSizeBefore > 0 && totalSizeAfter != totalSizeBefore {
		sizeChange := 100.0 * float64(totalSizeAfter-totalSizeBefore) / float64(totalSizeBefore)
		if sizeChange > 0 {
			logger.Info("Size increase: %.1f%% (expected due to metadata)", sizeChange)
		} else {
			logger.Info("Size reduction: %.1f%%", -sizeChange)
		}
	}

	logger.Info("")
}

// =============================================================================
// Compactor - Main Compaction Logic
// =============================================================================

// Compactor handles the compaction phase.
type Compactor struct {
	store  TxHashStore
	logger Logger
	memory *MemoryMonitor
	stats  *CompactionStats
}

// NewCompactor creates a new Compactor.
func NewCompactor(store TxHashStore, logger Logger, memory *MemoryMonitor) *Compactor {
	return &Compactor{
		store:  store,
		logger: logger,
		memory: memory,
		stats:  NewCompactionStats(),
	}
}

// Run executes the compaction phase.
//
// Compacts all 16 column families and collects before/after statistics.
func (c *Compactor) Run() (*CompactionStats, error) {
	c.stats.StartTime = time.Now()

	c.logger.Separator()
	c.logger.Info("                         COMPACTION PHASE")
	c.logger.Separator()
	c.logger.Info("")

	// Collect before stats
	c.logger.Info("Collecting pre-compaction statistics...")
	c.stats.BeforeStats = c.store.GetAllCFStats()
	LogAllCFStats(c.store, c.logger, "PRE-COMPACTION STATISTICS")

	// Log level distribution before
	c.logger.Info("Level distribution BEFORE compaction:")
	LogCFLevelStats(c.store, c.logger)

	// Take memory snapshot
	beforeMem := TakeMemorySnapshot()
	beforeMem.Log(c.logger, "Pre-Compaction")

	// Compact each CF
	c.logger.Separator()
	c.logger.Info("                    COMPACTING COLUMN FAMILIES")
	c.logger.Separator()
	c.logger.Info("")

	totalStart := time.Now()

	for i, cfName := range ColumnFamilyNames {
		c.logger.Info("[%2d/16] Compacting CF [%s]...", i+1, cfName)

		cfStart := time.Now()
		c.store.CompactCF(cfName)
		cfElapsed := time.Since(cfStart)

		c.stats.PerCFTime[cfName] = cfElapsed
		c.logger.Info("        Completed in %v", cfElapsed)

		// Check memory every 4 CFs
		if (i+1)%4 == 0 {
			c.memory.Check()
		}
	}

	c.stats.TotalTime = time.Since(totalStart)
	c.stats.EndTime = time.Now()

	c.logger.Info("")
	c.logger.Info("All column families compacted in %v", c.stats.TotalTime)
	c.logger.Info("")

	// Collect after stats
	c.logger.Info("Collecting post-compaction statistics...")
	c.stats.AfterStats = c.store.GetAllCFStats()
	LogAllCFStats(c.store, c.logger, "POST-COMPACTION STATISTICS")

	// Log level distribution after
	c.logger.Info("Level distribution AFTER compaction:")
	LogCFLevelStats(c.store, c.logger)

	// Log comparison
	c.stats.LogBeforeAfterComparison(c.logger)

	// Take memory snapshot
	afterMem := TakeMemorySnapshot()
	afterMem.Log(c.logger, "Post-Compaction")

	// Log summary
	c.stats.LogSummary(c.logger)

	c.logger.Sync()

	return c.stats, nil
}

// GetStats returns the compaction statistics.
func (c *Compactor) GetStats() *CompactionStats {
	return c.stats
}

// =============================================================================
// Convenience Function
// =============================================================================

// RunCompaction is a convenience function to run the compaction phase.
func RunCompaction(store TxHashStore, logger Logger, memory *MemoryMonitor) (*CompactionStats, error) {
	compactor := NewCompactor(store, logger, memory)
	return compactor.Run()
}

// =============================================================================
// Compaction Progress Logging
// =============================================================================

// LogCompactionStart logs the start of compaction.
func LogCompactionStart(logger Logger) {
	logger.Separator()
	logger.Info("                         COMPACTION PHASE")
	logger.Separator()
	logger.Info("")
	logger.Info("Starting compaction of all 16 column families...")
	logger.Info("")
	logger.Info("WHAT COMPACTION DOES:")
	logger.Info("  1. Merges L0 files into sorted, non-overlapping files")
	logger.Info("  2. Removes duplicate keys (from crash recovery)")
	logger.Info("  3. Organizes data into levels (L6 after full compaction)")
	logger.Info("  4. Optimizes read performance for RecSplit building")
	logger.Info("")
}

// EstimateCompactionTime provides a rough estimate of compaction time.
//
// Based on empirical observations:
//   - ~10-30 seconds per GB of data
//   - ~5-10 minutes for a typical CF with ~20GB of data
//   - ~1-2 hours for all 16 CFs with ~320GB total
func EstimateCompactionTime(totalSizeBytes int64) time.Duration {
	// Estimate ~20 seconds per GB
	sizeGB := float64(totalSizeBytes) / float64(GB)
	estimatedSeconds := sizeGB * 20
	return time.Duration(estimatedSeconds) * time.Second
}

// =============================================================================
// Post-Compaction Count Verification
// =============================================================================

// CountVerificationResult holds the result of count verification for one CF.
type CountVerificationResult struct {
	CFName        string
	ExpectedCount uint64
	ActualCount   uint64
	Match         bool
	Duration      time.Duration
}

// CountVerificationStats holds overall count verification statistics.
type CountVerificationStats struct {
	StartTime  time.Time
	EndTime    time.Time
	TotalTime  time.Duration
	Results    []CountVerificationResult
	AllMatched bool
	Mismatches int
}

// VerifyCountsAfterCompaction iterates through each CF in RocksDB and verifies
// that the actual count matches the expected count from meta store.
//
// WHY THIS IS IMPORTANT:
//
//	After compaction, duplicates should be removed. The count in RocksDB should
//	exactly match cfCounts stored in meta store. If there's a mismatch, it
//	indicates a bug or data corruption that would cause RecSplit build to fail.
//
// WHAT THIS DOES:
//
//	For each of the 16 column families:
//	  1. Get expected count from meta store (cfCounts)
//	  2. Iterate through RocksDB CF and count actual entries
//	  3. Compare and log any mismatches
//
// RETURNS:
//   - stats: Verification statistics including any mismatches
//   - error: Only if there's an iteration error (not for count mismatches)
//
// NOTE: Count mismatches are logged as errors but do NOT abort the process.
// The RecSplit build phase will fail if counts don't match, which is the
// definitive check. This early verification helps catch issues sooner.
func VerifyCountsAfterCompaction(
	store TxHashStore,
	meta MetaStore,
	logger Logger,
) (*CountVerificationStats, error) {
	stats := &CountVerificationStats{
		StartTime:  time.Now(),
		Results:    make([]CountVerificationResult, 0, 16),
		AllMatched: true,
	}

	logger.Separator()
	logger.Info("                POST-COMPACTION COUNT VERIFICATION")
	logger.Separator()
	logger.Info("")
	logger.Info("Verifying RocksDB entry counts match checkpointed counts...")
	logger.Info("")

	// Get expected counts from meta store
	expectedCounts, err := meta.GetCFCounts()
	if err != nil {
		return nil, fmt.Errorf("failed to get CF counts from meta store: %w", err)
	}

	// Calculate total expected
	var totalExpected uint64
	for _, count := range expectedCounts {
		totalExpected += count
	}
	logger.Info("Expected total entries: %s", FormatCount(int64(totalExpected)))
	logger.Info("")

	// Verify each CF
	logger.Info("%-4s %15s %15s %8s %12s", "CF", "Expected", "Actual", "Match", "Time")
	logger.Info("%-4s %15s %15s %8s %12s", "----", "---------------", "---------------", "--------", "------------")

	var totalActual uint64

	for _, cfName := range ColumnFamilyNames {
		cfStart := time.Now()

		expectedCount := expectedCounts[cfName]

		// Iterate and count actual entries
		actualCount := uint64(0)
		iter := store.NewIteratorCF(cfName)
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			actualCount++
		}
		if err := iter.Error(); err != nil {
			iter.Close()
			return nil, fmt.Errorf("iterator error for CF %s: %w", cfName, err)
		}
		iter.Close()

		cfDuration := time.Since(cfStart)
		match := expectedCount == actualCount

		result := CountVerificationResult{
			CFName:        cfName,
			ExpectedCount: expectedCount,
			ActualCount:   actualCount,
			Match:         match,
			Duration:      cfDuration,
		}
		stats.Results = append(stats.Results, result)
		totalActual += actualCount

		matchStr := "OK"
		if !match {
			matchStr = "MISMATCH"
			stats.AllMatched = false
			stats.Mismatches++
		}

		logger.Info("%-4s %15s %15s %8s %12v",
			cfName,
			FormatCount(int64(expectedCount)),
			FormatCount(int64(actualCount)),
			matchStr,
			cfDuration)
	}

	logger.Info("%-4s %15s %15s %8s %12s", "----", "---------------", "---------------", "--------", "------------")

	totalMatch := totalExpected == totalActual
	totalMatchStr := "OK"
	if !totalMatch {
		totalMatchStr = "MISMATCH"
	}

	stats.EndTime = time.Now()
	stats.TotalTime = time.Since(stats.StartTime)

	logger.Info("%-4s %15s %15s %8s %12v",
		"TOT",
		FormatCount(int64(totalExpected)),
		FormatCount(int64(totalActual)),
		totalMatchStr,
		stats.TotalTime)
	logger.Info("")

	// Log summary
	if stats.AllMatched {
		logger.Info("Count verification PASSED: All %d CFs match expected counts", len(ColumnFamilyNames))
	} else {
		logger.Error("Count verification FAILED: %d CF(s) have mismatched counts", stats.Mismatches)
		logger.Error("")
		logger.Error("MISMATCH DETAILS:")
		for _, r := range stats.Results {
			if !r.Match {
				diff := int64(r.ActualCount) - int64(r.ExpectedCount)
				logger.Error("  CF %s: expected %d, got %d (diff: %+d)",
					r.CFName, r.ExpectedCount, r.ActualCount, diff)
			}
		}
		logger.Error("")
		logger.Error("This may indicate:")
		logger.Error("  1. Duplicate entries not properly deduplicated during compaction")
		logger.Error("  2. Data corruption in RocksDB")
		logger.Error("  3. A bug in the ingestion counting logic")
		logger.Error("")
		logger.Error("The RecSplit build phase will fail if counts don't match.")
	}
	logger.Info("")

	logger.Sync()

	return stats, nil
}
