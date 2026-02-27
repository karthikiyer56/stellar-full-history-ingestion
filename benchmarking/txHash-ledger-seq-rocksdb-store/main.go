// =============================================================================
// TX Hash -> Ledger Sequence RocksDB Store Benchmark Tool
// =============================================================================
//
// This tool benchmarks read performance of a tx_hash -> ledger_seq RocksDB store
// with 16 column families. It reads transaction hashes from a file and looks them
// up in the store, measuring latency and throughput.
//
// Features:
// - Store statistics display before benchmark (SST files, WAL files, per-CF stats)
// - Progress logging every 1% with running statistics
// - Separate tracking for Found, NotFound, and Error cases
// - Detailed latency statistics (min, max, avg, stddev, percentiles)
// - Latency histogram in final summary
// - Separate log and error file support
//
// =============================================================================
// USAGE
// =============================================================================
//
// Basic benchmark (output to stdout):
//   ./tx-hash-store-benchmark --store /data/store --hashes hashes.txt
//
// With separate log and error files:
//   ./tx-hash-store-benchmark --store /data/store --hashes hashes.txt \
//       --log-file benchmark.log --error-file benchmark.err
//
// With custom block cache:
//   ./tx-hash-store-benchmark --store /data/store --hashes hashes.txt \
//       --block-cache 2048
//
// =============================================================================

package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// Constants
// =============================================================================

const (
	MB                    = 1024 * 1024
	DefaultBlockCache     = 512
	BloomFilterBitsPerKey = 12 // Must match ingestion setting for optimal performance
)

// ColumnFamilyNames contains the names of all 16 column families.
var ColumnFamilyNames = []string{
	"0", "1", "2", "3", "4", "5", "6", "7",
	"8", "9", "a", "b", "c", "d", "e", "f",
}

// =============================================================================
// Error Types
// =============================================================================

// BenchmarkErrorType categorizes different error scenarios
type BenchmarkErrorType string

const (
	ErrTypeRocksDBError  BenchmarkErrorType = "ROCKSDB_ERROR"
	ErrTypeInvalidHash   BenchmarkErrorType = "INVALID_HASH"
	ErrTypeHashLoadError BenchmarkErrorType = "HASH_LOAD_ERROR"
)

// BenchmarkError represents a structured error during benchmarking
type BenchmarkError struct {
	Type          BenchmarkErrorType
	TxHash        string
	Message       string
	UnderlyingErr error
	Timestamp     time.Time
}

func (e *BenchmarkError) Error() string {
	if e.UnderlyingErr != nil {
		return fmt.Sprintf("[%s] Hash=%s: %s: %v", e.Type, e.TxHash, e.Message, e.UnderlyingErr)
	}
	return fmt.Sprintf("[%s] Hash=%s: %s", e.Type, e.TxHash, e.Message)
}

func newBenchmarkError(errType BenchmarkErrorType, txHash []byte, message string, underlying error) *BenchmarkError {
	hashStr := ""
	if txHash != nil {
		hashStr = hex.EncodeToString(txHash)
	}
	return &BenchmarkError{
		Type:          errType,
		TxHash:        hashStr,
		Message:       message,
		UnderlyingErr: underlying,
		Timestamp:     time.Now(),
	}
}

// =============================================================================
// Aggregated Statistics
// =============================================================================

// AggregatedStats holds timing samples and counts for statistical analysis
type AggregatedStats struct {
	mu sync.Mutex

	// Found lookups (hash exists in store)
	FoundLatencies    []float64 // in microseconds
	TotalFoundLatency time.Duration
	FoundCount        int64
	MinFoundLatency   time.Duration
	MaxFoundLatency   time.Duration

	// NotFound lookups (hash doesn't exist - not an error)
	NotFoundLatencies    []float64 // in microseconds
	TotalNotFoundLatency time.Duration
	NotFoundCount        int64
	MinNotFoundLatency   time.Duration
	MaxNotFoundLatency   time.Duration

	// Errors (actual failures)
	ErrorCount int64
}

// NewAggregatedStats creates a new stats collector with pre-allocated slices
func NewAggregatedStats(expectedCount int) *AggregatedStats {
	return &AggregatedStats{
		FoundLatencies:     make([]float64, 0, expectedCount),
		NotFoundLatencies:  make([]float64, 0, expectedCount),
		MinFoundLatency:    time.Duration(math.MaxInt64),
		MaxFoundLatency:    0,
		MinNotFoundLatency: time.Duration(math.MaxInt64),
		MaxNotFoundLatency: 0,
	}
}

// AddFoundSample adds a found lookup result
func (s *AggregatedStats) AddFoundSample(latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.FoundLatencies = append(s.FoundLatencies, float64(latency.Microseconds()))
	s.TotalFoundLatency += latency
	s.FoundCount++

	if latency < s.MinFoundLatency {
		s.MinFoundLatency = latency
	}
	if latency > s.MaxFoundLatency {
		s.MaxFoundLatency = latency
	}
}

// AddNotFoundSample adds a not-found lookup result
func (s *AggregatedStats) AddNotFoundSample(latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.NotFoundLatencies = append(s.NotFoundLatencies, float64(latency.Microseconds()))
	s.TotalNotFoundLatency += latency
	s.NotFoundCount++

	if latency < s.MinNotFoundLatency {
		s.MinNotFoundLatency = latency
	}
	if latency > s.MaxNotFoundLatency {
		s.MaxNotFoundLatency = latency
	}
}

// IncrementError increments the error count
func (s *AggregatedStats) IncrementError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ErrorCount++
}

// GetSnapshot returns a thread-safe snapshot of current stats
func (s *AggregatedStats) GetSnapshot() (foundCount, notFoundCount, errorCount int64,
	avgFoundLatency, avgNotFoundLatency time.Duration,
	minFoundLatency, maxFoundLatency, minNotFoundLatency, maxNotFoundLatency time.Duration,
	foundSamples, notFoundSamples []float64) {

	s.mu.Lock()
	defer s.mu.Unlock()

	foundCount = s.FoundCount
	notFoundCount = s.NotFoundCount
	errorCount = s.ErrorCount

	if s.FoundCount > 0 {
		avgFoundLatency = s.TotalFoundLatency / time.Duration(s.FoundCount)
		minFoundLatency = s.MinFoundLatency
		maxFoundLatency = s.MaxFoundLatency
	}

	if s.NotFoundCount > 0 {
		avgNotFoundLatency = s.TotalNotFoundLatency / time.Duration(s.NotFoundCount)
		minNotFoundLatency = s.MinNotFoundLatency
		maxNotFoundLatency = s.MaxNotFoundLatency
	}

	// Copy slices for percentile calculations
	foundSamples = make([]float64, len(s.FoundLatencies))
	copy(foundSamples, s.FoundLatencies)

	notFoundSamples = make([]float64, len(s.NotFoundLatencies))
	copy(notFoundSamples, s.NotFoundLatencies)

	return
}

// =============================================================================
// Statistical Functions
// =============================================================================

// calculateMean calculates the mean of samples (in microseconds)
func calculateMean(samples []float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range samples {
		sum += v
	}
	return sum / float64(len(samples))
}

// calculateStdDev calculates the standard deviation (in microseconds)
func calculateStdDev(samples []float64, mean float64) float64 {
	if len(samples) < 2 {
		return 0
	}
	sumSquares := 0.0
	for _, v := range samples {
		diff := v - mean
		sumSquares += diff * diff
	}
	return math.Sqrt(sumSquares / float64(len(samples)-1))
}

// calculatePercentile calculates the given percentile (samples must be sorted)
func calculatePercentile(sortedSamples []float64, percentile float64) float64 {
	if len(sortedSamples) == 0 {
		return 0
	}
	index := (percentile / 100.0) * float64(len(sortedSamples)-1)
	lower := int(math.Floor(index))
	upper := int(math.Ceil(index))

	if lower == upper || upper >= len(sortedSamples) {
		return sortedSamples[lower]
	}

	// Linear interpolation
	weight := index - float64(lower)
	return sortedSamples[lower]*(1-weight) + sortedSamples[upper]*weight
}

// MetricStats holds computed statistics for a single metric
type MetricStats struct {
	Count  int64
	Mean   time.Duration
	StdDev time.Duration
	Min    time.Duration
	Max    time.Duration
	P50    time.Duration
	P75    time.Duration
	P90    time.Duration
	P95    time.Duration
	P99    time.Duration
}

// computeMetricStats computes all statistics for a slice of samples (in microseconds)
func computeMetricStats(samples []float64, min, max time.Duration) MetricStats {
	if len(samples) == 0 {
		return MetricStats{}
	}

	// Sort a copy for percentile calculations
	sorted := make([]float64, len(samples))
	copy(sorted, samples)
	sort.Float64s(sorted)

	mean := calculateMean(samples)
	stdDev := calculateStdDev(samples, mean)

	return MetricStats{
		Count:  int64(len(samples)),
		Mean:   time.Duration(mean) * time.Microsecond,
		StdDev: time.Duration(stdDev) * time.Microsecond,
		Min:    min,
		Max:    max,
		P50:    time.Duration(calculatePercentile(sorted, 50)) * time.Microsecond,
		P75:    time.Duration(calculatePercentile(sorted, 75)) * time.Microsecond,
		P90:    time.Duration(calculatePercentile(sorted, 90)) * time.Microsecond,
		P95:    time.Duration(calculatePercentile(sorted, 95)) * time.Microsecond,
		P99:    time.Duration(calculatePercentile(sorted, 99)) * time.Microsecond,
	}
}

// computePercentiles computes percentiles from samples for progress reporting
func computePercentiles(samples []float64) (p50, p90, p95, p99 time.Duration) {
	if len(samples) == 0 {
		return
	}

	sorted := make([]float64, len(samples))
	copy(sorted, samples)
	sort.Float64s(sorted)

	p50 = time.Duration(calculatePercentile(sorted, 50)) * time.Microsecond
	p90 = time.Duration(calculatePercentile(sorted, 90)) * time.Microsecond
	p95 = time.Duration(calculatePercentile(sorted, 95)) * time.Microsecond
	p99 = time.Duration(calculatePercentile(sorted, 99)) * time.Microsecond
	return
}

// =============================================================================
// Histogram
// =============================================================================

// HistogramBucket represents a latency histogram bucket
type HistogramBucket struct {
	Label string
	MinUs int64 // minimum microseconds (inclusive)
	MaxUs int64 // maximum microseconds (exclusive), -1 for infinity
	Count int64
}

// computeHistogram computes a latency histogram from samples
func computeHistogram(samples []float64) []HistogramBucket {
	buckets := []HistogramBucket{
		{Label: "0-10µs", MinUs: 0, MaxUs: 10},
		{Label: "10-25µs", MinUs: 10, MaxUs: 25},
		{Label: "25-50µs", MinUs: 25, MaxUs: 50},
		{Label: "50-100µs", MinUs: 50, MaxUs: 100},
		{Label: "100-250µs", MinUs: 100, MaxUs: 250},
		{Label: "250-500µs", MinUs: 250, MaxUs: 500},
		{Label: "500µs-1ms", MinUs: 500, MaxUs: 1000},
		{Label: "1ms+", MinUs: 1000, MaxUs: -1},
	}

	for _, sample := range samples {
		sampleUs := int64(sample)
		for i := range buckets {
			if buckets[i].MaxUs == -1 {
				// Last bucket catches everything >= MinUs
				if sampleUs >= buckets[i].MinUs {
					buckets[i].Count++
				}
			} else if sampleUs >= buckets[i].MinUs && sampleUs < buckets[i].MaxUs {
				buckets[i].Count++
				break
			}
		}
	}

	return buckets
}

// =============================================================================
// Logger
// =============================================================================

// BenchmarkLogger handles logging to file or stdout
type BenchmarkLogger struct {
	infoLogger   *log.Logger
	errorLogger  *log.Logger
	logFile      *os.File
	errorFile    *os.File
	logToFile    bool
	errorToFile  bool
	logCount     int
	syncInterval int // sync every N log calls
}

// NewBenchmarkLogger creates a new logger with optional file outputs
func NewBenchmarkLogger(logPath, errorPath string) (*BenchmarkLogger, error) {
	bl := &BenchmarkLogger{
		syncInterval: 10, // sync every 10 log calls
	}

	// Setup info logger
	if logPath != "" {
		f, err := os.Create(logPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create log file: %w", err)
		}
		bl.logFile = f
		bl.logToFile = true
		bl.infoLogger = log.New(f, "", log.LstdFlags)
	} else {
		bl.infoLogger = log.New(os.Stdout, "", log.LstdFlags)
	}

	// Setup error logger
	if errorPath != "" {
		f, err := os.Create(errorPath)
		if err != nil {
			if bl.logFile != nil {
				bl.logFile.Close()
			}
			return nil, fmt.Errorf("failed to create error file: %w", err)
		}
		bl.errorFile = f
		bl.errorToFile = true
		bl.errorLogger = log.New(f, "", log.LstdFlags)
	} else {
		bl.errorLogger = log.New(os.Stdout, "", log.LstdFlags)
	}

	return bl, nil
}

// Info logs an info message
func (bl *BenchmarkLogger) Info(format string, v ...interface{}) {
	bl.infoLogger.Printf(format, v...)
	if bl.logToFile && bl.logFile != nil {
		bl.logCount++
		if bl.logCount >= bl.syncInterval {
			bl.logFile.Sync()
			bl.logCount = 0
		}
	}
}

// Error logs an error message
func (bl *BenchmarkLogger) Error(format string, v ...interface{}) {
	bl.errorLogger.Printf(format, v...)
	if bl.errorToFile && bl.errorFile != nil {
		bl.errorFile.Sync() // Always sync errors immediately
	}
}

// Sync forces a sync of log files
func (bl *BenchmarkLogger) Sync() {
	if bl.logToFile && bl.logFile != nil {
		bl.logFile.Sync()
	}
	if bl.errorToFile && bl.errorFile != nil {
		bl.errorFile.Sync()
	}
}

// Close closes any open file handles
func (bl *BenchmarkLogger) Close() {
	if bl.logFile != nil {
		bl.logFile.Close()
	}
	if bl.errorFile != nil {
		bl.errorFile.Close()
	}
}

// =============================================================================
// RocksDB Store
// =============================================================================

// Store wraps the RocksDB store for benchmarking.
type Store struct {
	db         *grocksdb.DB
	opts       *grocksdb.Options
	cfHandles  []*grocksdb.ColumnFamilyHandle
	cfOpts     []*grocksdb.Options
	blockCache *grocksdb.Cache
	ro         *grocksdb.ReadOptions
	cfIndexMap map[string]int
}

func openStore(path string, blockCacheMB int) (*Store, error) {
	// Create block cache
	var blockCache *grocksdb.Cache
	if blockCacheMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(blockCacheMB * MB))
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)
	opts.SetMaxOpenFiles(-1) // Keep all files open for better performance

	// Prepare CF names
	cfNames := []string{"default"}
	cfNames = append(cfNames, ColumnFamilyNames...)

	// Create options for each CF with block cache and bloom filter
	cfOptsList := make([]*grocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts := grocksdb.NewDefaultOptions()

		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		if blockCache != nil {
			bbto.SetBlockCache(blockCache)
		}
		// Enable bloom filter for read performance (must match ingestion setting)
		bbto.SetFilterPolicy(grocksdb.NewBloomFilter(BloomFilterBitsPerKey))
		cfOpts.SetBlockBasedTableFactory(bbto)

		cfOptsList[i] = cfOpts
	}

	// Open as read-only
	db, cfHandles, err := grocksdb.OpenDbForReadOnlyColumnFamilies(opts, path, cfNames, cfOptsList, false)
	if err != nil {
		opts.Destroy()
		for _, cfOpt := range cfOptsList {
			if cfOpt != nil {
				cfOpt.Destroy()
			}
		}
		if blockCache != nil {
			blockCache.Destroy()
		}
		return nil, fmt.Errorf("failed to open store: %w", err)
	}

	// Build CF index map
	cfIndexMap := make(map[string]int)
	for i, name := range cfNames {
		cfIndexMap[name] = i
	}

	ro := grocksdb.NewDefaultReadOptions()

	return &Store{
		db:         db,
		opts:       opts,
		cfHandles:  cfHandles,
		cfOpts:     cfOptsList,
		blockCache: blockCache,
		ro:         ro,
		cfIndexMap: cfIndexMap,
	}, nil
}

// Close releases all resources
func (s *Store) Close() {
	if s.ro != nil {
		s.ro.Destroy()
	}
	for _, cfHandle := range s.cfHandles {
		if cfHandle != nil {
			cfHandle.Destroy()
		}
	}
	if s.db != nil {
		s.db.Close()
	}
	if s.opts != nil {
		s.opts.Destroy()
	}
	for _, cfOpt := range s.cfOpts {
		if cfOpt != nil {
			cfOpt.Destroy()
		}
	}
	if s.blockCache != nil {
		s.blockCache.Destroy()
	}
}

// Get looks up a transaction hash and returns (value, found, error)
func (s *Store) Get(txHash []byte) ([]byte, bool, error) {
	// Determine column family
	cfName := getCFName(txHash)
	idx := s.cfIndexMap[cfName]
	cfHandle := s.cfHandles[idx]

	slice, err := s.db.GetCF(s.ro, cfHandle, txHash)
	if err != nil {
		return nil, false, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil, false, nil
	}

	// Copy the data
	result := make([]byte, slice.Size())
	copy(result, slice.Data())
	return result, true, nil
}

func getCFName(txHash []byte) string {
	if len(txHash) < 1 {
		return "0"
	}
	idx := int(txHash[0] >> 4) // High nibble
	if idx < 0 || idx > 15 {
		return "0"
	}
	return ColumnFamilyNames[idx]
}

// =============================================================================
// Store Statistics
// =============================================================================

// CFLevelStats holds per-level statistics for a column family
type CFLevelStats struct {
	FileCount int64
	Size      int64
}

// CFStats holds statistics for a single column family
type CFStats struct {
	Name          string
	EstimatedKeys int64
	TotalSize     int64
	TotalFiles    int64
	LevelStats    [7]CFLevelStats // L0-L6
}

// getPropertyInt parses an integer property from the database
func getPropertyInt(db *grocksdb.DB, property string) int64 {
	value := db.GetProperty(property)
	if value == "" {
		return 0
	}
	var result int64
	fmt.Sscanf(value, "%d", &result)
	return result
}

// getPropertyIntCF parses an integer property for a column family
func getPropertyIntCF(db *grocksdb.DB, property string, cf *grocksdb.ColumnFamilyHandle) int64 {
	value := db.GetPropertyCF(property, cf)
	if value == "" {
		return 0
	}
	var result int64
	fmt.Sscanf(value, "%d", &result)
	return result
}

// countFilesInStore counts SST and WAL files in the store directory
func countFilesInStore(storePath string) (sstCount int, walCount int, walBytes int64, err error) {
	entries, err := os.ReadDir(storePath)
	if err != nil {
		return 0, 0, 0, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()

		if strings.HasSuffix(name, ".sst") {
			sstCount++
		} else if strings.HasSuffix(name, ".log") {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			walCount++
			walBytes += info.Size()
		}
	}

	return sstCount, walCount, walBytes, nil
}

// getCFStats retrieves statistics for a column family using GetColumnFamilyMetadataCF
func getCFStats(db *grocksdb.DB, cfHandle *grocksdb.ColumnFamilyHandle, cfName string) CFStats {
	stats := CFStats{
		Name: cfName,
	}

	// Get estimated keys
	stats.EstimatedKeys = getPropertyIntCF(db, "rocksdb.estimate-num-keys", cfHandle)

	// Get column family metadata for per-level breakdown
	cfMeta := db.GetColumnFamilyMetadataCF(cfHandle)
	if cfMeta != nil {
		stats.TotalSize = int64(cfMeta.Size())
		stats.TotalFiles = int64(cfMeta.FileCount())

		levelMetas := cfMeta.LevelMetas()
		for _, levelMeta := range levelMetas {
			level := levelMeta.Level()
			if level >= 0 && level < 7 {
				sstMetas := levelMeta.SstMetas()
				stats.LevelStats[level] = CFLevelStats{
					FileCount: int64(len(sstMetas)),
					Size:      int64(levelMeta.Size()),
				}
			}
		}
	}

	return stats
}

// printStoreStats prints comprehensive store statistics before the benchmark
func printStoreStats(store *Store, storePath string, logger *BenchmarkLogger) {
	logger.Info("================================================================================")
	logger.Info("                          STORE STATISTICS")
	logger.Info("================================================================================")
	logger.Info("")

	// Storage overview from global properties
	logger.Info("STORAGE OVERVIEW:")
	totalSSTSize := getPropertyInt(store.db, "rocksdb.total-sst-files-size")
	liveSSTSize := getPropertyInt(store.db, "rocksdb.live-sst-files-size")
	liveDataSize := getPropertyInt(store.db, "rocksdb.estimate-live-data-size")
	memtableSize := getPropertyInt(store.db, "rocksdb.cur-size-all-mem-tables")

	logger.Info("  Total SST Files Size:  %s", helpers.FormatBytes(totalSSTSize))
	if liveSSTSize > 0 {
		logger.Info("  Live SST Files Size:   %s", helpers.FormatBytes(liveSSTSize))
	}
	if liveDataSize > 0 {
		logger.Info("  Est. Live Data Size:   %s", helpers.FormatBytes(liveDataSize))
	}
	if memtableSize > 0 {
		logger.Info("  Memtable Size:         %s", helpers.FormatBytes(memtableSize))
	}
	logger.Info("")

	// File counts from filesystem
	logger.Info("FILE COUNTS (Filesystem):")
	sstCount, walCount, walBytes, err := countFilesInStore(storePath)
	if err != nil {
		logger.Info("  (Could not read directory: %v)", err)
	} else {
		logger.Info("  SST Files:             %s", helpers.FormatNumber(int64(sstCount)))
		if walCount > 0 {
			logger.Info("  WAL Files:             %s (%s)", helpers.FormatNumber(int64(walCount)), helpers.FormatBytes(walBytes))
		} else {
			logger.Info("  WAL Files:             0")
		}
	}
	logger.Info("")

	// Collect per-CF statistics
	cfStatsList := make([]CFStats, 0, len(ColumnFamilyNames))
	var totalEstKeys int64
	var totalCFSize int64
	var totalCFFiles int64
	var levelTotals [7]CFLevelStats

	for _, cfName := range ColumnFamilyNames {
		idx := store.cfIndexMap[cfName]
		cfHandle := store.cfHandles[idx]
		cfStats := getCFStats(store.db, cfHandle, cfName)
		cfStatsList = append(cfStatsList, cfStats)

		totalEstKeys += cfStats.EstimatedKeys
		totalCFSize += cfStats.TotalSize
		totalCFFiles += cfStats.TotalFiles

		for level := 0; level < 7; level++ {
			levelTotals[level].FileCount += cfStats.LevelStats[level].FileCount
			levelTotals[level].Size += cfStats.LevelStats[level].Size
		}
	}

	// Column family summary
	logger.Info("COLUMN FAMILY SUMMARY:")
	logger.Info("  Total CFs:             16")
	logger.Info("  Total Estimated Keys:  %s", helpers.FormatNumber(totalEstKeys))
	logger.Info("  Total CF Size:         %s", helpers.FormatBytes(totalCFSize))
	logger.Info("  Total CF Files:        %s", helpers.FormatNumber(totalCFFiles))
	logger.Info("")

	// Per-CF level breakdown table
	logger.Info("COLUMN FAMILY DETAILS (Per-CF Level Breakdown):")
	logger.Info("")

	// Table header
	logger.Info("  CF   Est. Keys       Size        L0    L1    L2    L3    L4    L5    L6")
	logger.Info("  ─────────────────────────────────────────────────────────────────────────")

	for _, cfStats := range cfStatsList {
		// Format level file counts
		l0 := formatLevelCount(cfStats.LevelStats[0].FileCount)
		l1 := formatLevelCount(cfStats.LevelStats[1].FileCount)
		l2 := formatLevelCount(cfStats.LevelStats[2].FileCount)
		l3 := formatLevelCount(cfStats.LevelStats[3].FileCount)
		l4 := formatLevelCount(cfStats.LevelStats[4].FileCount)
		l5 := formatLevelCount(cfStats.LevelStats[5].FileCount)
		l6 := formatLevelCount(cfStats.LevelStats[6].FileCount)

		logger.Info("  %-4s %13s %11s  %5s %5s %5s %5s %5s %5s %5s",
			cfStats.Name,
			helpers.FormatNumber(cfStats.EstimatedKeys),
			helpers.FormatBytes(cfStats.TotalSize),
			l0, l1, l2, l3, l4, l5, l6)
	}

	// Table footer with totals
	logger.Info("  ─────────────────────────────────────────────────────────────────────────")
	logger.Info("  %-4s %13s %11s  %5s %5s %5s %5s %5s %5s %5s",
		"TOT",
		helpers.FormatNumber(totalEstKeys),
		helpers.FormatBytes(totalCFSize),
		formatLevelCount(levelTotals[0].FileCount),
		formatLevelCount(levelTotals[1].FileCount),
		formatLevelCount(levelTotals[2].FileCount),
		formatLevelCount(levelTotals[3].FileCount),
		formatLevelCount(levelTotals[4].FileCount),
		formatLevelCount(levelTotals[5].FileCount),
		formatLevelCount(levelTotals[6].FileCount))

	logger.Info("")
	logger.Info("================================================================================")
	logger.Info("")
}

// formatLevelCount formats a level file count for the table
func formatLevelCount(count int64) string {
	if count == 0 {
		return "-"
	}
	return fmt.Sprintf("%d", count)
}

// =============================================================================
// Hash Loading
// =============================================================================

func loadHashes(path string) ([][]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var hashes [][]byte
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse hex hash (should be 64 chars = 32 bytes)
		if len(line) != 64 {
			continue // Skip invalid lines
		}

		hash, err := hex.DecodeString(line)
		if err != nil {
			continue // Skip invalid hex
		}

		hashes = append(hashes, hash)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return hashes, nil
}

// =============================================================================
// Progress Reporter
// =============================================================================

func progressReporter(
	logger *BenchmarkLogger,
	stats *AggregatedStats,
	totalLookups int,
	progressChan <-chan int,
	doneChan chan<- struct{},
	startTime time.Time,
) {
	processed := int64(0)
	lastReportedPercent := -1

	for range progressChan {
		processed++
		currentPercent := int((processed * 100) / int64(totalLookups))

		if currentPercent > lastReportedPercent {
			lastReportedPercent = currentPercent
			elapsed := time.Since(startTime)

			foundCount, notFoundCount, errorCount,
				avgFoundLatency, avgNotFoundLatency,
				minFoundLatency, maxFoundLatency,
				minNotFoundLatency, maxNotFoundLatency,
				foundSamples, notFoundSamples := stats.GetSnapshot()

			// Calculate ETA
			var eta time.Duration
			if processed > 0 {
				avgTimePerLookup := elapsed / time.Duration(processed)
				remaining := int64(totalLookups) - processed
				eta = avgTimePerLookup * time.Duration(remaining)
			}

			rate := float64(0)
			if elapsed.Seconds() > 0 {
				rate = float64(processed) / elapsed.Seconds()
			}

			total := foundCount + notFoundCount + errorCount
			foundPct := float64(0)
			notFoundPct := float64(0)
			errorPct := float64(0)
			if total > 0 {
				foundPct = float64(foundCount) / float64(total) * 100
				notFoundPct = float64(notFoundCount) / float64(total) * 100
				errorPct = float64(errorCount) / float64(total) * 100
			}

			logger.Info("")
			logger.Info("================================================================================")
			logger.Info("PROGRESS: %d%% | %s / %s lookups | Elapsed: %s | ETA: %s",
				currentPercent,
				helpers.FormatNumber(processed),
				helpers.FormatNumber(int64(totalLookups)),
				helpers.FormatDuration(elapsed),
				helpers.FormatDuration(eta))
			logger.Info("Rate: %.2f lookups/sec", rate)
			logger.Info("Found: %s (%.1f%%) | Not Found: %s (%.1f%%) | Errors: %s (%.1f%%)",
				helpers.FormatNumber(foundCount), foundPct,
				helpers.FormatNumber(notFoundCount), notFoundPct,
				helpers.FormatNumber(errorCount), errorPct)

			// Found latency stats
			if foundCount > 0 {
				p50, p90, p95, p99 := computePercentiles(foundSamples)
				logger.Info("Found Latency:    Min=%s | Avg=%s | Max=%s | p50=%s | p90=%s | p95=%s | p99=%s",
					helpers.FormatDuration(minFoundLatency),
					helpers.FormatDuration(avgFoundLatency),
					helpers.FormatDuration(maxFoundLatency),
					helpers.FormatDuration(p50),
					helpers.FormatDuration(p90),
					helpers.FormatDuration(p95),
					helpers.FormatDuration(p99))
			}

			// NotFound latency stats
			if notFoundCount > 0 {
				p50, p90, p95, p99 := computePercentiles(notFoundSamples)
				logger.Info("NotFound Latency: Min=%s | Avg=%s | Max=%s | p50=%s | p90=%s | p95=%s | p99=%s",
					helpers.FormatDuration(minNotFoundLatency),
					helpers.FormatDuration(avgNotFoundLatency),
					helpers.FormatDuration(maxNotFoundLatency),
					helpers.FormatDuration(p50),
					helpers.FormatDuration(p90),
					helpers.FormatDuration(p95),
					helpers.FormatDuration(p99))
			}

			logger.Info("================================================================================")
		}
	}

	close(doneChan)
}

// =============================================================================
// Error Collector
// =============================================================================

func errorCollector(
	logger *BenchmarkLogger,
	errorChan <-chan *BenchmarkError,
	doneChan chan<- struct{},
) {
	for err := range errorChan {
		logger.Error("[%s] %s Hash=%s Timestamp=%s",
			err.Type,
			err.Message,
			err.TxHash,
			err.Timestamp.Format(time.RFC3339))
		if err.UnderlyingErr != nil {
			logger.Error("    Underlying error: %v", err.UnderlyingErr)
		}
	}
	close(doneChan)
}

// =============================================================================
// Main Benchmark Function
// =============================================================================

func runBenchmark(storePath, hashesFile string, blockCacheMB int, logger *BenchmarkLogger) error {
	logger.Info("")
	logger.Info("################################################################################")
	logger.Info("          TX HASH -> LEDGER SEQUENCE ROCKSDB STORE BENCHMARK")
	logger.Info("################################################################################")
	logger.Info("")

	// Load hashes
	logger.Info("Loading hashes from: %s", hashesFile)
	logger.Sync()
	hashLoadStart := time.Now()
	hashes, err := loadHashes(hashesFile)
	if err != nil {
		return fmt.Errorf("failed to load hashes: %w", err)
	}
	hashLoadElapsed := time.Since(hashLoadStart)

	totalHashes := len(hashes)
	logger.Info("Loaded %s hashes in %s", helpers.FormatNumber(int64(totalHashes)), helpers.FormatDuration(hashLoadElapsed))
	logger.Info("")

	if totalHashes == 0 {
		return fmt.Errorf("input file contains no valid hashes")
	}

	// Print configuration
	logger.Info("CONFIGURATION:")
	logger.Info("  Store Path:      %s", storePath)
	logger.Info("  Hashes File:     %s", hashesFile)
	logger.Info("  Total Lookups:   %s", helpers.FormatNumber(int64(totalHashes)))
	logger.Info("  Block Cache:     %d MB", blockCacheMB)
	logger.Info("")

	// Open store in read-only mode
	logger.Info("Opening store (read-only)...")
	logger.Sync()
	storeOpenStart := time.Now()
	store, err := openStore(storePath, blockCacheMB)
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}
	defer store.Close()
	storeOpenElapsed := time.Since(storeOpenStart)
	logger.Info("Store opened successfully in %s", helpers.FormatDuration(storeOpenElapsed))
	logger.Info("")

	// Print store statistics before benchmark
	printStoreStats(store, storePath, logger)

	// Initialize stats
	stats := NewAggregatedStats(totalHashes)

	// Create channels
	progressChan := make(chan int, 1000)
	errorChan := make(chan *BenchmarkError, 100)
	progressDone := make(chan struct{})
	errorDone := make(chan struct{})

	// Start time
	startTime := time.Now()

	// Start progress reporter
	go progressReporter(logger, stats, totalHashes, progressChan, progressDone, startTime)

	// Start error collector
	go errorCollector(logger, errorChan, errorDone)

	// Run benchmark
	logger.Info("================================================================================")
	logger.Info("                          STARTING BENCHMARK")
	logger.Info("================================================================================")
	logger.Info("")

	for i := 0; i < totalHashes; i++ {
		hash := hashes[i]

		start := time.Now()
		_, found, err := store.Get(hash)
		elapsed := time.Since(start)

		if err != nil {
			stats.IncrementError()
			errorChan <- newBenchmarkError(ErrTypeRocksDBError, hash, "Lookup failed", err)
		} else if found {
			stats.AddFoundSample(elapsed)
		} else {
			stats.AddNotFoundSample(elapsed)
		}

		progressChan <- 1
	}

	// Close channels and wait for collectors
	close(progressChan)
	close(errorChan)
	<-progressDone
	<-errorDone

	// Calculate final statistics
	totalElapsed := time.Since(startTime)

	// Get final snapshot
	foundCount, notFoundCount, errorCount,
		_, _,
		minFoundLatency, maxFoundLatency,
		minNotFoundLatency, maxNotFoundLatency,
		foundSamples, notFoundSamples := stats.GetSnapshot()

	// Compute final statistics
	foundStats := computeMetricStats(foundSamples, minFoundLatency, maxFoundLatency)
	notFoundStats := computeMetricStats(notFoundSamples, minNotFoundLatency, maxNotFoundLatency)

	// Print final summary
	logger.Info("")
	logger.Info("################################################################################")
	logger.Info("################################################################################")
	logger.Info("                           BENCHMARK COMPLETE")
	logger.Info("################################################################################")
	logger.Info("################################################################################")
	logger.Info("")

	totalProcessed := foundCount + notFoundCount + errorCount
	foundPct := float64(0)
	notFoundPct := float64(0)
	errorPct := float64(0)
	if totalProcessed > 0 {
		foundPct = float64(foundCount) / float64(totalProcessed) * 100
		notFoundPct = float64(notFoundCount) / float64(totalProcessed) * 100
		errorPct = float64(errorCount) / float64(totalProcessed) * 100
	}

	logger.Info("SUMMARY:")
	logger.Info("  Total Time:      %s", helpers.FormatDuration(totalElapsed))
	logger.Info("  Total Lookups:   %s", helpers.FormatNumber(totalProcessed))
	logger.Info("  Found:           %s (%.2f%%)", helpers.FormatNumber(foundCount), foundPct)
	logger.Info("  Not Found:       %s (%.2f%%)", helpers.FormatNumber(notFoundCount), notFoundPct)
	logger.Info("  Errors:          %s (%.2f%%)", helpers.FormatNumber(errorCount), errorPct)
	logger.Info("  Throughput:      %.2f lookups/sec", float64(totalProcessed)/totalElapsed.Seconds())
	logger.Info("")

	// Found latency statistics
	if foundCount > 0 {
		logger.Info("================================================================================")
		logger.Info("                        FOUND LATENCY STATISTICS")
		logger.Info("================================================================================")
		logger.Info("  Count:     %s", helpers.FormatNumber(foundStats.Count))
		logger.Info("  Min:       %s", helpers.FormatDuration(foundStats.Min))
		logger.Info("  Max:       %s", helpers.FormatDuration(foundStats.Max))
		logger.Info("  Avg:       %s", helpers.FormatDuration(foundStats.Mean))
		logger.Info("  Std Dev:   %s", helpers.FormatDuration(foundStats.StdDev))
		logger.Info("")
		logger.Info("  Percentiles:")
		logger.Info("    p50:     %s", helpers.FormatDuration(foundStats.P50))
		logger.Info("    p75:     %s", helpers.FormatDuration(foundStats.P75))
		logger.Info("    p90:     %s", helpers.FormatDuration(foundStats.P90))
		logger.Info("    p95:     %s", helpers.FormatDuration(foundStats.P95))
		logger.Info("    p99:     %s", helpers.FormatDuration(foundStats.P99))
		logger.Info("")
	}

	// Not found latency statistics
	if notFoundCount > 0 {
		logger.Info("================================================================================")
		logger.Info("                      NOT FOUND LATENCY STATISTICS")
		logger.Info("================================================================================")
		logger.Info("  Count:     %s", helpers.FormatNumber(notFoundStats.Count))
		logger.Info("  Min:       %s", helpers.FormatDuration(notFoundStats.Min))
		logger.Info("  Max:       %s", helpers.FormatDuration(notFoundStats.Max))
		logger.Info("  Avg:       %s", helpers.FormatDuration(notFoundStats.Mean))
		logger.Info("  Std Dev:   %s", helpers.FormatDuration(notFoundStats.StdDev))
		logger.Info("")
		logger.Info("  Percentiles:")
		logger.Info("    p50:     %s", helpers.FormatDuration(notFoundStats.P50))
		logger.Info("    p75:     %s", helpers.FormatDuration(notFoundStats.P75))
		logger.Info("    p90:     %s", helpers.FormatDuration(notFoundStats.P90))
		logger.Info("    p95:     %s", helpers.FormatDuration(notFoundStats.P95))
		logger.Info("    p99:     %s", helpers.FormatDuration(notFoundStats.P99))
		logger.Info("")
	}

	// Combined histogram
	if foundCount > 0 || notFoundCount > 0 {
		logger.Info("================================================================================")
		logger.Info("                        COMBINED LATENCY HISTOGRAM")
		logger.Info("================================================================================")

		// Combine all samples
		allSamples := make([]float64, 0, len(foundSamples)+len(notFoundSamples))
		allSamples = append(allSamples, foundSamples...)
		allSamples = append(allSamples, notFoundSamples...)

		buckets := computeHistogram(allSamples)
		totalSamples := int64(len(allSamples))

		for _, bucket := range buckets {
			pct := float64(0)
			if totalSamples > 0 {
				pct = float64(bucket.Count) / float64(totalSamples) * 100
			}
			logger.Info("  %-12s %s (%.1f%%)",
				bucket.Label+":",
				helpers.FormatNumber(bucket.Count),
				pct)
		}
		logger.Info("")
	}

	logger.Info("################################################################################")
	logger.Info("")

	return nil
}

// =============================================================================
// Main Entry Point
// =============================================================================

func main() {
	var (
		storePath    string
		hashesFile   string
		blockCacheMB int
		logFile      string
		errorFile    string
		showHelp     bool
	)

	flag.StringVar(&storePath, "store", "", "Path to RocksDB store (required)")
	flag.StringVar(&hashesFile, "hashes", "", "Path to file with hex tx hashes (required)")
	flag.IntVar(&blockCacheMB, "block-cache", DefaultBlockCache, "Block cache size in MB")
	flag.StringVar(&logFile, "log-file", "", "Output file for logs (default: stdout)")
	flag.StringVar(&errorFile, "error-file", "", "Output file for errors (default: stdout)")
	flag.BoolVar(&showHelp, "help", false, "Show help message")

	flag.Parse()

	if showHelp {
		printUsage()
		os.Exit(0)
	}

	// Validate required arguments
	if storePath == "" {
		fmt.Fprintf(os.Stderr, "ERROR: --store is required\n\n")
		printUsage()
		os.Exit(1)
	}
	if hashesFile == "" {
		fmt.Fprintf(os.Stderr, "ERROR: --hashes is required\n\n")
		printUsage()
		os.Exit(1)
	}

	// Get absolute paths
	absStorePath, err := filepath.Abs(storePath)
	if err != nil {
		log.Fatalf("ERROR: Failed to get absolute path for store: %v", err)
	}

	absHashesFile, err := filepath.Abs(hashesFile)
	if err != nil {
		log.Fatalf("ERROR: Failed to get absolute path for hashes file: %v", err)
	}

	// Setup logger
	logger, err := NewBenchmarkLogger(logFile, errorFile)
	if err != nil {
		log.Fatalf("ERROR: Failed to setup logger: %v", err)
	}
	defer logger.Close()

	// Run benchmark
	if err := runBenchmark(absStorePath, absHashesFile, blockCacheMB, logger); err != nil {
		logger.Error("Benchmark failed: %v", err)
		os.Exit(1)
	}
}

// =============================================================================
// Usage
// =============================================================================

func printUsage() {
	fmt.Println("tx-hash-store-benchmark - Benchmark tx_hash RocksDB store read performance")
	fmt.Println()
	fmt.Println("USAGE:")
	fmt.Println("  tx-hash-store-benchmark --store PATH --hashes FILE [OPTIONS]")
	fmt.Println()
	fmt.Println("OPTIONS:")
	fmt.Println("  --store PATH          Path to RocksDB store (required)")
	fmt.Println("  --hashes FILE         Path to file with hex tx hashes (required)")
	fmt.Println("                        All hashes in the file will be looked up")
	fmt.Println("  --block-cache N       Block cache size in MB (default: 512)")
	fmt.Println("  --log-file FILE       Output file for logs (default: stdout)")
	fmt.Println("  --error-file FILE     Output file for errors (default: stdout)")
	fmt.Println("  --help                Show this help message")
	fmt.Println()
	fmt.Println("FEATURES:")
	fmt.Println("  - Displays store statistics before benchmark (SST files, WAL files, per-CF stats)")
	fmt.Println("  - Progress logging every 1% with running statistics")
	fmt.Println("  - Separate tracking for Found, NotFound, and Error cases")
	fmt.Println("  - Detailed latency statistics with percentiles and histogram")
	fmt.Println()
	fmt.Println("HASHES FILE FORMAT:")
	fmt.Println("  One 64-character hex transaction hash per line:")
	fmt.Println()
	fmt.Println("    0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b")
	fmt.Println("    1f2e3d4c5b6a7f8e9d0c1b2a3f4e5d6c7b8a9f0e1d2c3b4a5f6e7d8c9b0a1f2e")
	fmt.Println("    # Lines starting with # are ignored")
	fmt.Println()
	fmt.Println("EXAMPLES:")
	fmt.Println("  # Basic benchmark")
	fmt.Println("  tx-hash-store-benchmark \\")
	fmt.Println("    --store /data/tx-hash-store \\")
	fmt.Println("    --hashes /data/sample-hashes.txt")
	fmt.Println()
	fmt.Println("  # With log files and larger cache")
	fmt.Println("  tx-hash-store-benchmark \\")
	fmt.Println("    --store /data/tx-hash-store \\")
	fmt.Println("    --hashes /data/sample-hashes.txt \\")
	fmt.Println("    --block-cache 4096 \\")
	fmt.Println("    --log-file benchmark.log \\")
	fmt.Println("    --error-file benchmark.err")
}
