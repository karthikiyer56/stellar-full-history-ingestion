// =============================================================================
// internal/workflow/metrics/metrics.go - Metrics Collection and Analysis
// =============================================================================
//
// This package provides metrics collection for tracking:
//   - Latency statistics with percentile calculations (p50, p90, p95, p99)
//   - Throughput metrics (items/sec, bytes/sec)
//   - Memory usage (RSS, heap allocation)
//   - Batch-level statistics
//
// All timing and percentile calculations follow deterministic patterns
// for reproducible metrics across runs.
//
// =============================================================================

package metrics

import (
	"math"
	"runtime"
	"sort"
	"sync"
	"time"
)

// =============================================================================
// LatencyStats - Track and Calculate Latency Percentiles
// =============================================================================

// LatencyStats collects latency samples and computes statistics.
// Thread-safe for concurrent use.
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
func (ls *LatencyStats) Summary() LatencySummary {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	n := len(ls.samples)
	if n == 0 {
		return LatencySummary{}
	}

	sorted := make([]time.Duration, n)
	copy(sorted, ls.samples)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	var sum int64
	for _, d := range ls.samples {
		sum += int64(d)
	}
	avg := sum / int64(n)

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

	idx := int(math.Ceil(float64(n)*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

// =============================================================================
// MemoryStats - Track RSS and Heap Metrics
// =============================================================================

// MemoryStats tracks memory usage via runtime metrics.
type MemoryStats struct {
	mu sync.Mutex

	samples []MemorySample
}

// MemorySample represents memory usage at a point in time.
type MemorySample struct {
	Timestamp time.Time
	HeapAlloc uint64 // Bytes allocated in heap
	HeapSys   uint64 // Total heap system memory
}

// NewMemoryStats creates a new MemoryStats.
func NewMemoryStats() *MemoryStats {
	return &MemoryStats{
		samples: make([]MemorySample, 0, 100),
	}
}

// Record captures current memory stats.
func (ms *MemoryStats) Record() {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	ms.samples = append(ms.samples, MemorySample{
		Timestamp: time.Now(),
		HeapAlloc: m.HeapAlloc,
		HeapSys:   m.HeapSys,
	})
}

// PeakHeapAlloc returns the maximum heap allocation observed.
func (ms *MemoryStats) PeakHeapAlloc() uint64 {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	var peak uint64
	for _, s := range ms.samples {
		if s.HeapAlloc > peak {
			peak = s.HeapAlloc
		}
	}
	return peak
}

// CurrentHeapAlloc returns the latest heap allocation.
func (ms *MemoryStats) CurrentHeapAlloc() uint64 {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if len(ms.samples) == 0 {
		return 0
	}
	return ms.samples[len(ms.samples)-1].HeapAlloc
}

// SampleCount returns number of samples collected.
func (ms *MemoryStats) SampleCount() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return len(ms.samples)
}

// =============================================================================
// ThroughputStats - Track Processing Rate
// =============================================================================

// ThroughputStats tracks items processed and calculates rate.
type ThroughputStats struct {
	mu sync.Mutex

	itemsProcessed uint64
	bytesProcessed uint64
	startTime      time.Time
	lastUpdate     time.Time
}

// NewThroughputStats creates a new ThroughputStats.
func NewThroughputStats() *ThroughputStats {
	return &ThroughputStats{
		startTime:  time.Now(),
		lastUpdate: time.Now(),
	}
}

// AddItems records N items processed.
func (ts *ThroughputStats) AddItems(count uint64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.itemsProcessed += count
	ts.lastUpdate = time.Now()
}

// AddBytes records N bytes processed.
func (ts *ThroughputStats) AddBytes(count uint64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.bytesProcessed += count
	ts.lastUpdate = time.Now()
}

// ItemsPerSecond returns current throughput in items/sec.
func (ts *ThroughputStats) ItemsPerSecond() float64 {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	elapsed := ts.lastUpdate.Sub(ts.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(ts.itemsProcessed) / elapsed
}

// BytesPerSecond returns current throughput in bytes/sec.
func (ts *ThroughputStats) BytesPerSecond() float64 {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	elapsed := ts.lastUpdate.Sub(ts.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(ts.bytesProcessed) / elapsed
}

// TotalItems returns total items processed.
func (ts *ThroughputStats) TotalItems() uint64 {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.itemsProcessed
}

// TotalBytes returns total bytes processed.
func (ts *ThroughputStats) TotalBytes() uint64 {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.bytesProcessed
}

// Elapsed returns time since start.
func (ts *ThroughputStats) Elapsed() time.Duration {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.lastUpdate.Sub(ts.startTime)
}

// =============================================================================
// BatchMetrics - Per-Batch Statistics
// =============================================================================

// BatchMetrics tracks metrics for a single batch operation.
type BatchMetrics struct {
	BatchNumber      uint32
	StartLedger      uint32
	EndLedger        uint32
	LedgerCount      uint32
	TransactionCount uint64
	ParseTime        time.Duration
	WriteTime        time.Duration
	TotalTime        time.Duration
	BytesWritten     uint64
}

// =============================================================================
// AggregateMetrics - Cumulative Metrics Across Multiple Batches
// =============================================================================

// AggregateMetrics accumulates metrics across multiple batches.
type AggregateMetrics struct {
	mu sync.Mutex

	totalBatches      uint32
	totalLedgers      uint32
	totalTransactions uint64
	totalBytesWritten uint64
	totalParseTime    time.Duration
	totalWriteTime    time.Duration
	startTime         time.Time
	lastUpdateTime    time.Time
	latencies         *LatencyStats
	memory            *MemoryStats
}

// NewAggregateMetrics creates a new AggregateMetrics.
func NewAggregateMetrics() *AggregateMetrics {
	return &AggregateMetrics{
		startTime:      time.Now(),
		lastUpdateTime: time.Now(),
		latencies:      NewLatencyStats(),
		memory:         NewMemoryStats(),
	}
}

// AddBatch adds a batch's metrics to the aggregate.
func (am *AggregateMetrics) AddBatch(batch *BatchMetrics) {
	am.mu.Lock()
	defer am.mu.Unlock()

	am.totalBatches++
	am.totalLedgers += batch.LedgerCount
	am.totalTransactions += batch.TransactionCount
	am.totalBytesWritten += batch.BytesWritten
	am.totalParseTime += batch.ParseTime
	am.totalWriteTime += batch.WriteTime
	am.lastUpdateTime = time.Now()

	if batch.TotalTime > 0 {
		am.latencies.Add(batch.TotalTime)
	}
	am.memory.Record()
}

// GetLatencySummary returns latency percentiles.
func (am *AggregateMetrics) GetLatencySummary() LatencySummary {
	return am.latencies.Summary()
}

// GetMemoryStats returns memory statistics.
func (am *AggregateMetrics) GetMemoryStats() *MemoryStats {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.memory
}

// GetTotalBatches returns number of batches processed.
func (am *AggregateMetrics) GetTotalBatches() uint32 {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.totalBatches
}

// GetTotalLedgers returns total ledgers processed.
func (am *AggregateMetrics) GetTotalLedgers() uint32 {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.totalLedgers
}

// GetTotalTransactions returns total transactions processed.
func (am *AggregateMetrics) GetTotalTransactions() uint64 {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.totalTransactions
}

// GetTotalBytesWritten returns total bytes written.
func (am *AggregateMetrics) GetTotalBytesWritten() uint64 {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.totalBytesWritten
}

// GetElapsedTime returns time since start.
func (am *AggregateMetrics) GetElapsedTime() time.Duration {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.lastUpdateTime.Sub(am.startTime)
}

// GetLedgersPerSecond returns throughput in ledgers/sec.
func (am *AggregateMetrics) GetLedgersPerSecond() float64 {
	am.mu.Lock()
	defer am.mu.Unlock()

	elapsed := am.lastUpdateTime.Sub(am.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(am.totalLedgers) / elapsed
}

// GetTransactionsPerSecond returns throughput in transactions/sec.
func (am *AggregateMetrics) GetTransactionsPerSecond() float64 {
	am.mu.Lock()
	defer am.mu.Unlock()

	elapsed := am.lastUpdateTime.Sub(am.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(am.totalTransactions) / elapsed
}

// GetBytesPerSecond returns throughput in bytes/sec.
func (am *AggregateMetrics) GetBytesPerSecond() float64 {
	am.mu.Lock()
	defer am.mu.Unlock()

	elapsed := am.lastUpdateTime.Sub(am.startTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(am.totalBytesWritten) / elapsed
}

// GetAverageBatchTime returns average time per batch.
func (am *AggregateMetrics) GetAverageBatchTime() time.Duration {
	am.mu.Lock()
	defer am.mu.Unlock()

	if am.totalBatches == 0 {
		return 0
	}
	totalTime := am.totalParseTime + am.totalWriteTime
	return totalTime / time.Duration(am.totalBatches)
}
