// =============================================================================
// stats.go - Statistics, Percentiles, and Histograms
// =============================================================================
//
// This file provides statistical utilities for tracking and reporting metrics:
//   - Latency tracking with percentile calculations (p50, p90, p95, p99)
//   - Throughput calculations
//   - Histogram generation
//   - Batch-level statistics for ingestion
//
// DESIGN PHILOSOPHY:
//
//   Statistics are collected in-memory and reported at checkpoints.
//   This provides detailed visibility into performance without impacting
//   throughput with excessive disk I/O.
//
// MEMORY USAGE:
//
//   - LatencyStats: O(n) where n = number of samples (for accurate percentiles)
//   - For large sample counts, consider using a digest algorithm
//   - Current design assumes thousands of samples per batch, not millions
//
// PERCENTILE CALCULATION:
//
//   Uses the "nearest rank" method:
//   - Sort samples
//   - p50 = sample at position ceil(n * 0.50)
//   - p90 = sample at position ceil(n * 0.90)
//   - etc.
//
// =============================================================================

package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// LatencyStats - Track and Calculate Latency Percentiles
// =============================================================================

// LatencyStats collects latency samples and computes statistics.
//
// THREAD SAFETY:
//
//	LatencyStats is safe for concurrent use from multiple goroutines.
//	All operations are protected by a mutex.
//
// USAGE:
//
//	stats := NewLatencyStats()
//	stats.Add(42 * time.Microsecond)
//	stats.Add(38 * time.Microsecond)
//	...
//	summary := stats.Summary()
//
// RESETTING:
//
//	Call Reset() to clear all samples and start fresh.
//	Typically called after each batch or checkpoint.
type LatencyStats struct {
	mu      sync.Mutex
	samples []time.Duration
}

// NewLatencyStats creates a new LatencyStats collector.
func NewLatencyStats() *LatencyStats {
	return &LatencyStats{
		samples: make([]time.Duration, 0, 1024),
	}
}

// Add records a latency sample.
func (ls *LatencyStats) Add(d time.Duration) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.samples = append(ls.samples, d)
}

// Count returns the number of samples collected.
func (ls *LatencyStats) Count() int {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return len(ls.samples)
}

// Reset clears all collected samples.
func (ls *LatencyStats) Reset() {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.samples = ls.samples[:0]
}

// LatencySummary contains computed latency statistics.
type LatencySummary struct {
	Count  int           // Number of samples
	Min    time.Duration // Minimum latency
	Max    time.Duration // Maximum latency
	Avg    time.Duration // Average (mean) latency
	StdDev time.Duration // Standard deviation
	P50    time.Duration // 50th percentile (median)
	P90    time.Duration // 90th percentile
	P95    time.Duration // 95th percentile
	P99    time.Duration // 99th percentile
}

// Summary computes statistics from collected samples.
//
// Returns a LatencySummary with all computed values.
// If no samples have been collected, returns a zero-value summary.
func (ls *LatencyStats) Summary() LatencySummary {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	n := len(ls.samples)
	if n == 0 {
		return LatencySummary{}
	}

	// Make a copy for sorting (to avoid modifying original order)
	sorted := make([]time.Duration, n)
	copy(sorted, ls.samples)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	// Calculate sum and variance components
	var sum int64
	for _, d := range ls.samples {
		sum += int64(d)
	}
	avg := sum / int64(n)

	// Calculate standard deviation
	var variance float64
	for _, d := range ls.samples {
		diff := float64(int64(d) - avg)
		variance += diff * diff
	}
	variance /= float64(n)
	stdDev := time.Duration(math.Sqrt(variance))

	return LatencySummary{
		Count:  n,
		Min:    sorted[0],
		Max:    sorted[n-1],
		Avg:    time.Duration(avg),
		StdDev: stdDev,
		P50:    percentile(sorted, 0.50),
		P90:    percentile(sorted, 0.90),
		P95:    percentile(sorted, 0.95),
		P99:    percentile(sorted, 0.99),
	}
}

// percentile calculates the p-th percentile from a sorted slice.
// p should be between 0 and 1 (e.g., 0.95 for 95th percentile).
func percentile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}

	// Use nearest-rank method
	idx := int(math.Ceil(float64(n)*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

// String returns a formatted string of the latency summary.
//
// Format:
//
//	count=1000 min=10µs max=500µs avg=42µs±15µs p50=38µs p90=80µs p95=120µs p99=250µs
func (s LatencySummary) String() string {
	if s.Count == 0 {
		return "count=0 (no samples)"
	}
	return fmt.Sprintf("count=%d min=%v max=%v avg=%v±%v p50=%v p90=%v p95=%v p99=%v",
		s.Count, s.Min, s.Max, s.Avg, s.StdDev, s.P50, s.P90, s.P95, s.P99)
}

// =============================================================================
// BatchStats - Statistics for a Single Batch
// =============================================================================

// BatchStats tracks statistics for a single batch of ledgers.
//
// A batch is typically 1000 ledgers. Statistics include:
//   - Parse time (reading and deserializing ledgers)
//   - Write time (writing to RocksDB)
//   - Transaction counts per column family
type BatchStats struct {
	// BatchNumber is the sequential batch number (1-based)
	BatchNumber int

	// StartLedger is the first ledger in this batch
	StartLedger uint32

	// EndLedger is the last ledger in this batch
	EndLedger uint32

	// ParseTime is the total time spent parsing ledgers
	ParseTime time.Duration

	// WriteTime is the total time spent writing to RocksDB
	WriteTime time.Duration

	// LedgerCount is the number of ledgers processed in this batch
	LedgerCount int

	// TxCount is the total number of transactions in this batch
	TxCount int

	// TxCountByCF tracks transaction counts per column family
	TxCountByCF map[string]int

	// ParseErrors is the number of ledgers that failed to parse
	// (should be 0; if > 0, ingestion aborts)
	ParseErrors int
}

// NewBatchStats creates a new BatchStats for the given batch.
func NewBatchStats(batchNum int, startLedger, endLedger uint32) *BatchStats {
	return &BatchStats{
		BatchNumber: batchNum,
		StartLedger: startLedger,
		EndLedger:   endLedger,
		TxCountByCF: make(map[string]int),
	}
}

// TotalTime returns the total time for this batch (parse + write).
func (bs *BatchStats) TotalTime() time.Duration {
	return bs.ParseTime + bs.WriteTime
}

// LedgersPerSecond returns the processing rate.
func (bs *BatchStats) LedgersPerSecond() float64 {
	totalSec := bs.TotalTime().Seconds()
	if totalSec == 0 {
		return 0
	}
	return float64(bs.LedgerCount) / totalSec
}

// TxPerSecond returns the transaction processing rate.
func (bs *BatchStats) TxPerSecond() float64 {
	totalSec := bs.TotalTime().Seconds()
	if totalSec == 0 {
		return 0
	}
	return float64(bs.TxCount) / totalSec
}

// LogSummary logs a summary of the batch to the logger.
func (bs *BatchStats) LogSummary(logger Logger) {
	logger.Info("Batch %d: ledgers %d-%d | %d txs | parse=%v write=%v | %.1f ledgers/sec | %.1f tx/sec",
		bs.BatchNumber, bs.StartLedger, bs.EndLedger,
		bs.TxCount, bs.ParseTime, bs.WriteTime,
		bs.LedgersPerSecond(), bs.TxPerSecond())
}

// LogCFBreakdown logs the transaction count per column family.
func (bs *BatchStats) LogCFBreakdown(logger Logger) {
	var parts []string
	for _, cf := range ColumnFamilyNames {
		count := bs.TxCountByCF[cf]
		parts = append(parts, fmt.Sprintf("%s:%d", cf, count))
	}
	logger.Info("  CF breakdown: %s", strings.Join(parts, " "))
}

// =============================================================================
// AggregatedStats - Cumulative Statistics Across Batches
// =============================================================================

// AggregatedStats accumulates statistics across multiple batches.
//
// USAGE:
//
//	agg := NewAggregatedStats()
//	for batch := range batches {
//	    stats := processBatch(batch)
//	    agg.AddBatch(stats)
//	}
//	agg.LogSummary(logger)
type AggregatedStats struct {
	mu sync.Mutex

	// TotalBatches is the number of batches processed
	TotalBatches int

	// TotalLedgers is the total number of ledgers processed
	TotalLedgers int

	// TotalTx is the total number of transactions processed
	TotalTx int

	// TotalParseTime is the cumulative parse time
	TotalParseTime time.Duration

	// TotalWriteTime is the cumulative write time
	TotalWriteTime time.Duration

	// TxCountByCF tracks cumulative transaction counts per CF
	TxCountByCF map[string]uint64

	// StartTime is when processing started
	StartTime time.Time

	// LastBatchEnd is when the last batch completed
	LastBatchEnd time.Time
}

// NewAggregatedStats creates a new AggregatedStats.
func NewAggregatedStats() *AggregatedStats {
	txCounts := make(map[string]uint64)
	for _, cf := range ColumnFamilyNames {
		txCounts[cf] = 0
	}
	return &AggregatedStats{
		TxCountByCF: txCounts,
		StartTime:   time.Now(),
	}
}

// AddBatch adds a batch's statistics to the aggregate.
func (as *AggregatedStats) AddBatch(batch *BatchStats) {
	as.mu.Lock()
	defer as.mu.Unlock()

	as.TotalBatches++
	as.TotalLedgers += batch.LedgerCount
	as.TotalTx += batch.TxCount
	as.TotalParseTime += batch.ParseTime
	as.TotalWriteTime += batch.WriteTime
	as.LastBatchEnd = time.Now()

	for cf, count := range batch.TxCountByCF {
		as.TxCountByCF[cf] += uint64(count)
	}
}

// GetCFCounts returns a copy of the CF counts map.
// This is used to update the meta store.
func (as *AggregatedStats) GetCFCounts() map[string]uint64 {
	as.mu.Lock()
	defer as.mu.Unlock()

	counts := make(map[string]uint64)
	for cf, count := range as.TxCountByCF {
		counts[cf] = count
	}
	return counts
}

// ElapsedTime returns the total elapsed time since start.
func (as *AggregatedStats) ElapsedTime() time.Duration {
	as.mu.Lock()
	defer as.mu.Unlock()
	return time.Since(as.StartTime)
}

// OverallLedgersPerSecond returns the overall processing rate.
func (as *AggregatedStats) OverallLedgersPerSecond() float64 {
	as.mu.Lock()
	defer as.mu.Unlock()

	elapsed := as.LastBatchEnd.Sub(as.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(as.TotalLedgers) / elapsed
}

// OverallTxPerSecond returns the overall transaction rate.
func (as *AggregatedStats) OverallTxPerSecond() float64 {
	as.mu.Lock()
	defer as.mu.Unlock()

	elapsed := as.LastBatchEnd.Sub(as.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(as.TotalTx) / elapsed
}

// LogSummary logs an aggregated summary to the logger.
func (as *AggregatedStats) LogSummary(logger Logger) {
	as.mu.Lock()
	defer as.mu.Unlock()

	elapsed := time.Since(as.StartTime)
	logger.Separator()
	logger.Info("                    INGESTION SUMMARY")
	logger.Separator()
	logger.Info("")
	logger.Info("TOTALS:")
	logger.Info("  Batches:          %d", as.TotalBatches)
	logger.Info("  Ledgers:          %d", as.TotalLedgers)
	logger.Info("  Transactions:     %d", as.TotalTx)
	logger.Info("")
	logger.Info("TIME:")
	logger.Info("  Parse Time:       %v", as.TotalParseTime)
	logger.Info("  Write Time:       %v", as.TotalWriteTime)
	logger.Info("  Total Time:       %v", elapsed)
	logger.Info("")
	logger.Info("THROUGHPUT:")
	logger.Info("  Ledgers/sec:      %.1f", float64(as.TotalLedgers)/elapsed.Seconds())
	logger.Info("  Transactions/sec: %.1f", float64(as.TotalTx)/elapsed.Seconds())
	logger.Info("")
}

// LogCFSummary logs the per-CF transaction counts.
func (as *AggregatedStats) LogCFSummary(logger Logger) {
	as.mu.Lock()
	defer as.mu.Unlock()

	logger.Info("TRANSACTIONS BY COLUMN FAMILY:")
	for _, cf := range ColumnFamilyNames {
		count := as.TxCountByCF[cf]
		pct := float64(count) / float64(as.TotalTx) * 100
		logger.Info("  CF %s: %12d (%.2f%%)", cf, count, pct)
	}
	logger.Info("")
}

// =============================================================================
// QueryStats - Statistics for Query Operations
// =============================================================================

// QueryStats collects statistics for SIGHUP query operations.
//
// Tracks:
//   - Individual query latencies (for percentile calculation)
//   - Found vs not-found counts
//   - Parse error counts (invalid txHashes in query file)
type QueryStats struct {
	mu sync.Mutex

	// Latencies for found queries
	foundLatencies *LatencyStats

	// Latencies for not-found queries
	notFoundLatencies *LatencyStats

	// FoundCount is the number of txHashes found
	FoundCount int

	// NotFoundCount is the number of txHashes not found
	NotFoundCount int

	// ParseErrorCount is the number of unparseable lines in query file
	ParseErrorCount int

	// StartTime is when the query batch started
	StartTime time.Time

	// EndTime is when the query batch ended
	EndTime time.Time
}

// NewQueryStats creates a new QueryStats.
func NewQueryStats() *QueryStats {
	return &QueryStats{
		foundLatencies:    NewLatencyStats(),
		notFoundLatencies: NewLatencyStats(),
		StartTime:         time.Now(),
	}
}

// AddFound records a successful query.
func (qs *QueryStats) AddFound(latency time.Duration) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.FoundCount++
	qs.foundLatencies.Add(latency)
}

// AddNotFound records a query that didn't find the txHash.
func (qs *QueryStats) AddNotFound(latency time.Duration) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.NotFoundCount++
	qs.notFoundLatencies.Add(latency)
}

// AddParseError records a line that couldn't be parsed.
func (qs *QueryStats) AddParseError() {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.ParseErrorCount++
}

// Finish marks the query batch as complete.
func (qs *QueryStats) Finish() {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.EndTime = time.Now()
}

// TotalQueries returns the total number of queries executed.
func (qs *QueryStats) TotalQueries() int {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	return qs.FoundCount + qs.NotFoundCount
}

// Duration returns the total duration of the query batch.
func (qs *QueryStats) Duration() time.Duration {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	if qs.EndTime.IsZero() {
		return time.Since(qs.StartTime)
	}
	return qs.EndTime.Sub(qs.StartTime)
}

// LogSummary logs a summary of query statistics.
func (qs *QueryStats) LogSummary(logger Logger) {
	qs.mu.Lock()
	defer qs.mu.Unlock()

	total := qs.FoundCount + qs.NotFoundCount

	logger.Info("")
	logger.Info("QUERY SUMMARY:")
	logger.Info("  Total Queries:     %d", total)
	logger.Info("  Found:             %d (%.2f%%)", qs.FoundCount, float64(qs.FoundCount)/float64(total)*100)
	logger.Info("  Not Found:         %d (%.2f%%)", qs.NotFoundCount, float64(qs.NotFoundCount)/float64(total)*100)
	logger.Info("  Parse Errors:      %d", qs.ParseErrorCount)
	logger.Info("  Duration:          %v", qs.EndTime.Sub(qs.StartTime))
	logger.Info("")

	if qs.FoundCount > 0 {
		found := qs.foundLatencies.Summary()
		logger.Info("FOUND QUERIES LATENCY:")
		logger.Info("  %s", found.String())
	}

	if qs.NotFoundCount > 0 {
		notFound := qs.notFoundLatencies.Summary()
		logger.Info("NOT-FOUND QUERIES LATENCY:")
		logger.Info("  %s", notFound.String())
	}
}

// =============================================================================
// ProgressTracker - Track and Report Progress
// =============================================================================

// ProgressTracker tracks progress and provides ETA calculations.
type ProgressTracker struct {
	mu sync.Mutex

	// Total is the total number of items to process
	Total int

	// Completed is the number of items completed
	Completed int

	// StartTime is when tracking started
	StartTime time.Time

	// LastUpdate is when progress was last updated
	LastUpdate time.Time
}

// NewProgressTracker creates a new ProgressTracker.
func NewProgressTracker(total int) *ProgressTracker {
	return &ProgressTracker{
		Total:     total,
		StartTime: time.Now(),
	}
}

// Update sets the current progress.
func (pt *ProgressTracker) Update(completed int) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.Completed = completed
	pt.LastUpdate = time.Now()
}

// Increment adds to the completed count.
func (pt *ProgressTracker) Increment(n int) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.Completed += n
	pt.LastUpdate = time.Now()
}

// Percentage returns the completion percentage (0-100).
func (pt *ProgressTracker) Percentage() float64 {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	if pt.Total == 0 {
		return 0
	}
	return float64(pt.Completed) / float64(pt.Total) * 100
}

// ETA returns the estimated time to completion.
//
// Based on the current rate of progress.
// Returns 0 if not enough data to estimate.
func (pt *ProgressTracker) ETA() time.Duration {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if pt.Completed == 0 {
		return 0
	}

	elapsed := time.Since(pt.StartTime)
	rate := float64(pt.Completed) / elapsed.Seconds() // items per second

	remaining := pt.Total - pt.Completed
	if remaining <= 0 {
		return 0
	}

	etaSeconds := float64(remaining) / rate
	return time.Duration(etaSeconds * float64(time.Second))
}

// LogProgress logs the current progress with ETA.
func (pt *ProgressTracker) LogProgress(logger Logger, label string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pct := float64(pt.Completed) / float64(pt.Total) * 100
	elapsed := time.Since(pt.StartTime)

	var eta time.Duration
	if pt.Completed > 0 {
		rate := float64(pt.Completed) / elapsed.Seconds()
		remaining := pt.Total - pt.Completed
		if remaining > 0 {
			eta = time.Duration(float64(remaining) / rate * float64(time.Second))
		}
	}

	logger.Info("%s: %d/%d (%.1f%%) | elapsed=%v | ETA=%v",
		label, pt.Completed, pt.Total, pct, elapsed.Truncate(time.Second), eta.Truncate(time.Second))
}

// =============================================================================
// FormatDuration - Human-Readable Duration Formatting
// =============================================================================

// FormatDuration formats a duration as a human-readable string.
//
// Examples:
//
//	45s
//	2m 30s
//	1h 15m
//	2d 5h
func FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", m, s)
	}
	if d < 24*time.Hour {
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		return fmt.Sprintf("%dh %dm", h, m)
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	return fmt.Sprintf("%dd %dh", days, hours)
}

// FormatBytes formats a byte count as a human-readable string.
//
// Examples:
//
//	1.5 KB
//	256.0 MB
//	1.2 GB
func FormatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
		TB = 1024 * GB
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/float64(TB))
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// FormatCount formats a large number with comma separators.
//
// Example: 1234567 → "1,234,567"
func FormatCount(n int64) string {
	if n < 0 {
		return "-" + FormatCount(-n)
	}
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}

	// Build string in reverse
	s := fmt.Sprintf("%d", n)
	var result strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}
