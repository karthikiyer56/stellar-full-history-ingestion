// =============================================================================
// query-benchmark - Standalone Query Performance Testing Tool
// =============================================================================
//
// This tool benchmarks query performance against a compacted RocksDB store
// containing txHash→ledgerSeq mappings.
//
// USAGE:
//
//	query-benchmark \
//	  --rocksdb-path /path/to/rocksdb \
//	  --query-file /path/to/queries.txt \
//	  --output /path/to/results.csv \
//	  --log /path/to/benchmark.log \
//	  --block-cache-mb 8192
//
// INPUT FORMAT:
//
//	Query file contains one txHash per line (64-character hex string).
//
// OUTPUT FORMAT:
//
//	CSV with columns: txHash,ledgerSeq,queryTimeUs
//	ledgerSeq = -1 if not found
//
// =============================================================================

package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// Constants
// =============================================================================

const (
	MB = 1024 * 1024
	GB = 1024 * 1024 * 1024

	// Column family names
	cfCount = 16
)

var ColumnFamilyNames = []string{
	"0", "1", "2", "3", "4", "5", "6", "7",
	"8", "9", "a", "b", "c", "d", "e", "f",
}

// =============================================================================
// Logging
// =============================================================================

type Logger struct {
	mu      sync.Mutex
	logFile *os.File
}

func NewLogger(path string) (*Logger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &Logger{logFile: f}, nil
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(l.logFile, "[%s] %s\n", timestamp, msg)
}

func (l *Logger) Separator() {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintln(l.logFile, "=========================================================================")
}

func (l *Logger) Sync() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logFile.Sync()
}

func (l *Logger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logFile.Sync()
	l.logFile.Close()
}

// =============================================================================
// Latency Statistics
// =============================================================================

type LatencyStats struct {
	mu      sync.Mutex
	samples []time.Duration
}

func NewLatencyStats() *LatencyStats {
	return &LatencyStats{
		samples: make([]time.Duration, 0, 10000),
	}
}

func (ls *LatencyStats) Add(d time.Duration) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.samples = append(ls.samples, d)
}

type LatencySummary struct {
	Count  int
	Min    time.Duration
	Max    time.Duration
	Avg    time.Duration
	StdDev time.Duration
	P50    time.Duration
	P90    time.Duration
	P95    time.Duration
	P99    time.Duration
}

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

func (s LatencySummary) String() string {
	if s.Count == 0 {
		return "count=0 (no samples)"
	}
	return fmt.Sprintf("count=%d min=%v max=%v avg=%v±%v p50=%v p90=%v p95=%v p99=%v",
		s.Count, s.Min, s.Max, s.Avg, s.StdDev, s.P50, s.P90, s.P95, s.P99)
}

// =============================================================================
// RocksDB Store (Read-Only)
// =============================================================================

type Store struct {
	db         *grocksdb.DB
	opts       *grocksdb.Options
	cfHandles  []*grocksdb.ColumnFamilyHandle
	cfOpts     []*grocksdb.Options
	readOpts   *grocksdb.ReadOptions
	blockCache *grocksdb.Cache
	cfIndexMap map[string]int
}

func OpenStore(path string, blockCacheMB int) (*Store, error) {
	// Create shared block cache
	var blockCache *grocksdb.Cache
	if blockCacheMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(blockCacheMB * MB))
	}

	// Create database options
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	// Prepare column family names
	cfNames := []string{"default"}
	cfNames = append(cfNames, ColumnFamilyNames...)

	// Create options for each CF
	cfOptsList := make([]*grocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts := grocksdb.NewDefaultOptions()

		// Block-based table options with bloom filter
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetFilterPolicy(grocksdb.NewBloomFilter(12))
		if blockCache != nil {
			bbto.SetBlockCache(blockCache)
		}
		cfOpts.SetBlockBasedTableFactory(bbto)

		cfOptsList[i] = cfOpts
	}

	// Open database
	db, cfHandles, err := grocksdb.OpenDbForReadOnlyColumnFamilies(opts, path, cfNames, cfOptsList, false)
	if err != nil {
		opts.Destroy()
		for _, cfOpt := range cfOptsList {
			cfOpt.Destroy()
		}
		if blockCache != nil {
			blockCache.Destroy()
		}
		return nil, fmt.Errorf("failed to open RocksDB: %w", err)
	}

	// Build CF index map
	cfIndexMap := make(map[string]int)
	for i, name := range cfNames {
		cfIndexMap[name] = i
	}

	// Read options
	readOpts := grocksdb.NewDefaultReadOptions()

	return &Store{
		db:         db,
		opts:       opts,
		cfHandles:  cfHandles,
		cfOpts:     cfOptsList,
		readOpts:   readOpts,
		blockCache: blockCache,
		cfIndexMap: cfIndexMap,
	}, nil
}

func (s *Store) Get(txHash []byte) (value []byte, found bool, err error) {
	cfName := GetColumnFamilyName(txHash)
	cfHandle := s.cfHandles[s.cfIndexMap[cfName]]

	slice, err := s.db.GetCF(s.readOpts, cfHandle, txHash)
	if err != nil {
		return nil, false, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil, false, nil
	}

	value = make([]byte, slice.Size())
	copy(value, slice.Data())
	return value, true, nil
}

func (s *Store) Close() {
	if s.readOpts != nil {
		s.readOpts.Destroy()
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

// =============================================================================
// Helper Functions
// =============================================================================

func GetColumnFamilyName(txHash []byte) string {
	if len(txHash) < 1 {
		return "0"
	}
	idx := int(txHash[0] >> 4)
	if idx < 0 || idx > 15 {
		return "0"
	}
	return ColumnFamilyNames[idx]
}

func ParseLedgerSeq(value []byte) uint32 {
	if len(value) != 4 {
		return 0
	}
	return uint32(value[0])<<24 | uint32(value[1])<<16 | uint32(value[2])<<8 | uint32(value[3])
}

func HexToBytes(hexStr string) []byte {
	if len(hexStr)%2 != 0 {
		return nil
	}
	bytes := make([]byte, len(hexStr)/2)
	for i := 0; i < len(hexStr); i += 2 {
		b, err := strconv.ParseUint(hexStr[i:i+2], 16, 8)
		if err != nil {
			return nil
		}
		bytes[i/2] = byte(b)
	}
	return bytes
}

// =============================================================================
// Main
// =============================================================================

func main() {
	// Parse flags
	rocksdbPath := flag.String("rocksdb-path", "", "Path to RocksDB store (required)")
	queryFile := flag.String("query-file", "", "Path to query file (required)")
	outputPath := flag.String("output", "query-results.csv", "Path to CSV output")
	logPath := flag.String("log", "query-benchmark.log", "Path to log file")
	blockCacheMB := flag.Int("block-cache-mb", 8192, "RocksDB block cache size in MB")

	flag.Parse()

	// Validate flags
	if *rocksdbPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --rocksdb-path is required")
		flag.Usage()
		os.Exit(1)
	}
	if *queryFile == "" {
		fmt.Fprintln(os.Stderr, "Error: --query-file is required")
		flag.Usage()
		os.Exit(1)
	}

	// Initialize logger
	logger, err := NewLogger(*logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating log file: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	logger.Separator()
	logger.Info("                    QUERY BENCHMARK TOOL")
	logger.Separator()
	logger.Info("")
	logger.Info("Configuration:")
	logger.Info("  RocksDB Path:    %s", *rocksdbPath)
	logger.Info("  Query File:      %s", *queryFile)
	logger.Info("  Output File:     %s", *outputPath)
	logger.Info("  Block Cache:     %d MB", *blockCacheMB)
	logger.Info("")

	// Open RocksDB
	logger.Info("Opening RocksDB store...")
	store, err := OpenStore(*rocksdbPath, *blockCacheMB)
	if err != nil {
		logger.Info("ERROR: Failed to open RocksDB: %v", err)
		fmt.Fprintf(os.Stderr, "Error opening RocksDB: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()
	logger.Info("RocksDB store opened successfully")
	logger.Info("")

	// Open query file
	qf, err := os.Open(*queryFile)
	if err != nil {
		logger.Info("ERROR: Failed to open query file: %v", err)
		fmt.Fprintf(os.Stderr, "Error opening query file: %v\n", err)
		os.Exit(1)
	}
	defer qf.Close()

	// Open output file
	output, err := os.OpenFile(*outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		logger.Info("ERROR: Failed to create output file: %v", err)
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer output.Close()

	// Write CSV header
	fmt.Fprintln(output, "txHash,ledgerSeq,queryTimeUs")

	// Initialize statistics
	foundStats := NewLatencyStats()
	notFoundStats := NewLatencyStats()
	var foundCount, notFoundCount, parseErrors int

	logger.Info("Running queries...")
	startTime := time.Now()

	// Process queries
	scanner := bufio.NewScanner(qf)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		// Parse txHash
		txHash := HexToBytes(line)
		if txHash == nil || len(txHash) != 32 {
			parseErrors++
			logger.Info("Line %d: invalid txHash: %s", lineNum, line)
			continue
		}

		// Query
		queryStart := time.Now()
		value, found, err := store.Get(txHash)
		queryTime := time.Since(queryStart)

		if err != nil {
			logger.Info("Line %d: query error: %v", lineNum, err)
			continue
		}

		if found {
			ledgerSeq := ParseLedgerSeq(value)
			foundStats.Add(queryTime)
			foundCount++
			fmt.Fprintf(output, "%s,%d,%d\n", line, ledgerSeq, queryTime.Microseconds())
		} else {
			notFoundStats.Add(queryTime)
			notFoundCount++
			fmt.Fprintf(output, "%s,-1,%d\n", line, queryTime.Microseconds())
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Info("ERROR: Scanner error: %v", err)
	}

	totalTime := time.Since(startTime)
	totalQueries := foundCount + notFoundCount

	// Log summary
	logger.Separator()
	logger.Info("                    BENCHMARK RESULTS")
	logger.Separator()
	logger.Info("")
	logger.Info("SUMMARY:")
	logger.Info("  Total Queries:     %s", helpers.FormatNumber(int64(totalQueries)))
	logger.Info("  Found:             %s (%.2f%%)", helpers.FormatNumber(int64(foundCount)),
		float64(foundCount)/float64(totalQueries)*100)
	logger.Info("  Not Found:         %s (%.2f%%)", helpers.FormatNumber(int64(notFoundCount)),
		float64(notFoundCount)/float64(totalQueries)*100)
	logger.Info("  Parse Errors:      %d", parseErrors)
	logger.Info("  Total Time:        %v", totalTime)
	logger.Info("  Queries/sec:       %.2f", float64(totalQueries)/totalTime.Seconds())
	logger.Info("")

	if foundCount > 0 {
		foundSummary := foundStats.Summary()
		logger.Info("FOUND QUERIES LATENCY:")
		logger.Info("  %s", foundSummary.String())
		logger.Info("")
	}

	if notFoundCount > 0 {
		notFoundSummary := notFoundStats.Summary()
		logger.Info("NOT-FOUND QUERIES LATENCY:")
		logger.Info("  %s", notFoundSummary.String())
		logger.Info("")
	}

	logger.Info("Results written to: %s", *outputPath)
	logger.Separator()
	logger.Sync()

	fmt.Printf("Benchmark complete: %d queries in %v (%.2f qps)\n",
		totalQueries, totalTime, float64(totalQueries)/totalTime.Seconds())
	fmt.Printf("Results: %s, Log: %s\n", *outputPath, *logPath)
}
