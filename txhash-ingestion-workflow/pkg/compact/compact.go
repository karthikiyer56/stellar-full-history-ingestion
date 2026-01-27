// =============================================================================
// pkg/compact/compact.go - RocksDB Compaction Phase
// =============================================================================
//
// This package implements the compaction phase that runs after ingestion completes.
//
// =============================================================================

package compact

import (
	"fmt"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/cf"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/memory"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/store"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/types"
)

// =============================================================================
// Compactable - Minimal Interface for Compaction
// =============================================================================

// Compactable is a minimal interface for any store that supports compaction.
// This allows the compaction logic to be reused by different store implementations
// (e.g., the main workflow store and the standalone build-recsplit tool).
type Compactable interface {
	CompactCF(cfName string) time.Duration
}

// CompactAllCFs compacts all 16 column families.
// This is a lightweight helper for tools that don't need full compaction statistics.
//
// Parameters:
//   - store: Any store implementing the Compactable interface
//   - logger: Logger for progress output
//
// Returns the total time taken for all compactions.
func CompactAllCFs(store Compactable, logger interfaces.Logger) time.Duration {
	logger.Info("Compacting all column families...")
	logger.Info("")

	totalStart := time.Now()

	for i, cfName := range cf.Names {
		logger.Info("[%2d/16] Compacting CF [%s]...", i+1, cfName)
		cfStart := time.Now()
		store.CompactCF(cfName)
		logger.Info("        Completed in %v", time.Since(cfStart))
	}

	totalTime := time.Since(totalStart)
	logger.Info("")
	logger.Info("All column families compacted in %v", totalTime)

	return totalTime
}

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
	BeforeStats []types.CFStats

	// AfterStats holds CF stats after compaction
	AfterStats []types.CFStats
}

// NewCompactionStats creates a new CompactionStats.
func NewCompactionStats() *CompactionStats {
	return &CompactionStats{
		PerCFTime: make(map[string]time.Duration),
	}
}

// LogSummary logs a summary of compaction statistics.
func (cs *CompactionStats) LogSummary(logger interfaces.Logger) {
	logger.Separator()
	logger.Info("                    COMPACTION SUMMARY")
	logger.Separator()
	logger.Info("")

	// Per-CF times
	logger.Info("COMPACTION TIME BY COLUMN FAMILY:")
	for _, cfName := range cf.Names {
		elapsed := cs.PerCFTime[cfName]
		logger.Info("  CF %s: %v", cfName, elapsed)
	}
	logger.Info("")
	logger.Info("Total Compaction Time: %v", cs.TotalTime)
	logger.Info("")
}

// LogBeforeAfterComparison logs a before/after comparison.
func (cs *CompactionStats) LogBeforeAfterComparison(logger interfaces.Logger) {
	logger.Info("BEFORE/AFTER COMPARISON:")
	logger.Info("")

	// Build maps for easy lookup
	beforeMap := make(map[string]types.CFStats)
	afterMap := make(map[string]types.CFStats)
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

	for _, cfName := range cf.Names {
		before := beforeMap[cfName]
		after := afterMap[cfName]

		logger.Info("%-4s %12d %12d %12s %12s",
			cfName,
			before.TotalFiles,
			after.TotalFiles,
			helpers.FormatBytes(before.TotalSize),
			helpers.FormatBytes(after.TotalSize))

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
		helpers.FormatBytes(totalSizeBefore),
		helpers.FormatBytes(totalSizeAfter))

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
	store  interfaces.TxHashStore
	logger interfaces.Logger
	memory interfaces.MemoryMonitor
	stats  *CompactionStats
}

// NewCompactor creates a new Compactor.
func NewCompactor(s interfaces.TxHashStore, logger interfaces.Logger, mem interfaces.MemoryMonitor) *Compactor {
	return &Compactor{
		store:  s,
		logger: logger,
		memory: mem,
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
	store.LogAllCFStats(c.store, c.logger, "PRE-COMPACTION STATISTICS")

	// Log level distribution before
	c.logger.Info("Level distribution BEFORE compaction:")
	store.LogCFLevelStats(c.store, c.logger)

	// Take memory snapshot
	beforeMem := memory.TakeMemorySnapshot()
	beforeMem.Log(c.logger, "Pre-Compaction")

	// Compact each CF
	c.logger.Separator()
	c.logger.Info("                    COMPACTING COLUMN FAMILIES")
	c.logger.Separator()
	c.logger.Info("")

	totalStart := time.Now()

	for i, cfName := range cf.Names {
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
	store.LogAllCFStats(c.store, c.logger, "POST-COMPACTION STATISTICS")

	// Log level distribution after
	c.logger.Info("Level distribution AFTER compaction:")
	store.LogCFLevelStats(c.store, c.logger)

	// Log comparison
	c.stats.LogBeforeAfterComparison(c.logger)

	// Take memory snapshot
	afterMem := memory.TakeMemorySnapshot()
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
func RunCompaction(s interfaces.TxHashStore, logger interfaces.Logger, mem interfaces.MemoryMonitor) (*CompactionStats, error) {
	compactor := NewCompactor(s, logger, mem)
	return compactor.Run()
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
func VerifyCountsAfterCompaction(
	s interfaces.TxHashStore,
	meta interfaces.MetaStore,
	logger interfaces.Logger,
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
	logger.Info("Expected total entries: %s", helpers.FormatNumber(int64(totalExpected)))
	logger.Info("")

	// Verify each CF
	logger.Info("%-4s %15s %15s %8s %12s", "CF", "Expected", "Actual", "Match", "Time")
	logger.Info("%-4s %15s %15s %8s %12s", "----", "---------------", "---------------", "--------", "------------")

	var totalActual uint64

	for _, cfName := range cf.Names {
		cfStart := time.Now()

		expectedCount := expectedCounts[cfName]

		// Iterate and count actual entries
		actualCount := uint64(0)
		iter := s.NewIteratorCF(cfName)
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
			helpers.FormatNumber(int64(expectedCount)),
			helpers.FormatNumber(int64(actualCount)),
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
		helpers.FormatNumber(int64(totalExpected)),
		helpers.FormatNumber(int64(totalActual)),
		totalMatchStr,
		stats.TotalTime)
	logger.Info("")

	// Log summary
	if stats.AllMatched {
		logger.Info("Count verification PASSED: All %d CFs match expected counts", len(cf.Names))
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

// EstimateCompactionTime provides a rough estimate of compaction time.
//
// Based on empirical observations:
//   - ~10-30 seconds per GB of data
//   - ~5-10 minutes for a typical CF with ~20GB of data
//   - ~1-2 hours for all 16 CFs with ~320GB total
func EstimateCompactionTime(totalSizeBytes int64) time.Duration {
	// Estimate ~20 seconds per GB
	sizeGB := float64(totalSizeBytes) / float64(types.GB)
	estimatedSeconds := sizeGB * 20
	return time.Duration(estimatedSeconds) * time.Second
}
