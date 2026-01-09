// =============================================================================
// File-Based Ledger Storage Benchmark Tool
// =============================================================================
//
// This tool benchmarks ledger read performance from the file-based chunk storage.
// It reads ledger sequence numbers from an input file and measures timing for
// each read operation, providing detailed statistics.
//
// Features:
// - Concurrent reads with configurable worker count (default: 16)
// - Granular timing breakdown (index lookup, data read, decompress, unmarshal)
// - Statistical analysis: average, standard deviation, percentiles (p50, p95, p99)
// - Progress logging every 1% with ETA
// - Separate log and error file support
//
// =============================================================================
// USAGE
// =============================================================================
//
// Basic benchmark (output to stdout):
//   ./lfs-ledger-benchmark --data-dir /data/ledgers --input-file ledgers.txt
//
// With separate log and error files:
//   ./lfs-ledger-benchmark --data-dir /data/ledgers --input-file ledgers.txt \
//       --log-file benchmark.log --error-file benchmark.err
//
// Custom worker count:
//   ./lfs-ledger-benchmark --data-dir /data/ledgers --input-file ledgers.txt \
//       --workers 32
//
// =============================================================================

package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// Constants (reused from lfs-ledger-ingestion.go)
// =============================================================================

const (
	ChunkSize           = 10000
	FirstLedgerSequence = 2
	IndexHeaderSize     = 8
	IndexVersion        = 1
	DefaultWorkerCount  = 16
)

// =============================================================================
// Error Types
// =============================================================================

// BenchmarkErrorType categorizes different error scenarios
type BenchmarkErrorType string

const (
	ErrTypeIndexFileNotFound  BenchmarkErrorType = "INDEX_FILE_NOT_FOUND"
	ErrTypeDataFileNotFound   BenchmarkErrorType = "DATA_FILE_NOT_FOUND"
	ErrTypeIndexReadFailed    BenchmarkErrorType = "INDEX_READ_FAILED"
	ErrTypeDataReadFailed     BenchmarkErrorType = "DATA_READ_FAILED"
	ErrTypeDecompressFailed   BenchmarkErrorType = "DECOMPRESS_FAILED"
	ErrTypeUnmarshalFailed    BenchmarkErrorType = "UNMARSHAL_FAILED"
	ErrTypeInvalidIndexFormat BenchmarkErrorType = "INVALID_INDEX_FORMAT"
	ErrTypeLedgerOutOfRange   BenchmarkErrorType = "LEDGER_OUT_OF_RANGE"
)

// BenchmarkError represents a structured error during benchmarking
type BenchmarkError struct {
	Type          BenchmarkErrorType
	LedgerSeq     uint32
	ChunkID       uint32
	Message       string
	UnderlyingErr error
	Timestamp     time.Time
}

func (e *BenchmarkError) Error() string {
	if e.UnderlyingErr != nil {
		return fmt.Sprintf("[%s] Ledger %d (Chunk %d): %s: %v",
			e.Type, e.LedgerSeq, e.ChunkID, e.Message, e.UnderlyingErr)
	}
	return fmt.Sprintf("[%s] Ledger %d (Chunk %d): %s",
		e.Type, e.LedgerSeq, e.ChunkID, e.Message)
}

func newBenchmarkError(errType BenchmarkErrorType, ledgerSeq, chunkID uint32, message string, underlying error) *BenchmarkError {
	return &BenchmarkError{
		Type:          errType,
		LedgerSeq:     ledgerSeq,
		ChunkID:       chunkID,
		Message:       message,
		UnderlyingErr: underlying,
		Timestamp:     time.Now(),
	}
}

// =============================================================================
// Timing Structures
// =============================================================================

// LedgerTiming holds granular timing for reading a single ledger
type LedgerTiming struct {
	IndexLookupTime time.Duration
	DataReadTime    time.Duration
	DecompressTime  time.Duration
	UnmarshalTime   time.Duration
	TotalTime       time.Duration
}

// TimingResult holds the result of a single ledger read
type TimingResult struct {
	LedgerSeq uint32
	Timing    LedgerTiming
	Success   bool
	Error     *BenchmarkError
}

// AggregatedStats holds all timing samples for statistical analysis
type AggregatedStats struct {
	mu sync.Mutex

	// Raw samples for percentile calculations
	IndexLookupSamples []float64
	DataReadSamples    []float64
	DecompressSamples  []float64
	UnmarshalSamples   []float64
	FetchSamples       []float64 // Index + Data
	TotalSamples       []float64

	// Running sums for averages
	TotalIndexLookup time.Duration
	TotalDataRead    time.Duration
	TotalDecompress  time.Duration
	TotalUnmarshal   time.Duration
	TotalFetch       time.Duration
	TotalTime        time.Duration

	// Counts
	SuccessCount int64
	ErrorCount   int64
}

// NewAggregatedStats creates a new stats collector with pre-allocated slices
func NewAggregatedStats(expectedCount int) *AggregatedStats {
	return &AggregatedStats{
		IndexLookupSamples: make([]float64, 0, expectedCount),
		DataReadSamples:    make([]float64, 0, expectedCount),
		DecompressSamples:  make([]float64, 0, expectedCount),
		UnmarshalSamples:   make([]float64, 0, expectedCount),
		FetchSamples:       make([]float64, 0, expectedCount),
		TotalSamples:       make([]float64, 0, expectedCount),
	}
}

// AddSample adds a timing result to the statistics
func (s *AggregatedStats) AddSample(timing LedgerTiming) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fetchTime := timing.IndexLookupTime + timing.DataReadTime

	// Add to samples (in microseconds for numerical stability)
	s.IndexLookupSamples = append(s.IndexLookupSamples, float64(timing.IndexLookupTime.Microseconds()))
	s.DataReadSamples = append(s.DataReadSamples, float64(timing.DataReadTime.Microseconds()))
	s.DecompressSamples = append(s.DecompressSamples, float64(timing.DecompressTime.Microseconds()))
	s.UnmarshalSamples = append(s.UnmarshalSamples, float64(timing.UnmarshalTime.Microseconds()))
	s.FetchSamples = append(s.FetchSamples, float64(fetchTime.Microseconds()))
	s.TotalSamples = append(s.TotalSamples, float64(timing.TotalTime.Microseconds()))

	// Update running totals
	s.TotalIndexLookup += timing.IndexLookupTime
	s.TotalDataRead += timing.DataReadTime
	s.TotalDecompress += timing.DecompressTime
	s.TotalUnmarshal += timing.UnmarshalTime
	s.TotalFetch += fetchTime
	s.TotalTime += timing.TotalTime

	s.SuccessCount++
}

// IncrementError increments the error count
func (s *AggregatedStats) IncrementError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ErrorCount++
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
	Mean   time.Duration
	StdDev time.Duration
	P50    time.Duration
	P95    time.Duration
	P99    time.Duration
}

// computeMetricStats computes all statistics for a slice of samples (in microseconds)
func computeMetricStats(samples []float64) MetricStats {
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
		Mean:   time.Duration(mean) * time.Microsecond,
		StdDev: time.Duration(stdDev) * time.Microsecond,
		P50:    time.Duration(calculatePercentile(sorted, 50)) * time.Microsecond,
		P95:    time.Duration(calculatePercentile(sorted, 95)) * time.Microsecond,
		P99:    time.Duration(calculatePercentile(sorted, 99)) * time.Microsecond,
	}
}

// =============================================================================
// Chunk ID / Ledger Sequence Calculations (reused)
// =============================================================================

func ledgerToChunkID(ledgerSeq uint32) uint32 {
	return (ledgerSeq - FirstLedgerSequence) / ChunkSize
}

func chunkFirstLedger(chunkID uint32) uint32 {
	return (chunkID * ChunkSize) + FirstLedgerSequence
}

func chunkLastLedger(chunkID uint32) uint32 {
	return ((chunkID + 1) * ChunkSize) + FirstLedgerSequence - 1
}

func ledgerToLocalIndex(ledgerSeq uint32) uint32 {
	return (ledgerSeq - FirstLedgerSequence) % ChunkSize
}

// =============================================================================
// File Path Functions (reused)
// =============================================================================

func getChunkDir(dataDir string, chunkID uint32) string {
	parentDir := chunkID / 1000
	return filepath.Join(dataDir, "chunks", fmt.Sprintf("%04d", parentDir))
}

func getDataPath(dataDir string, chunkID uint32) string {
	return filepath.Join(getChunkDir(dataDir, chunkID), fmt.Sprintf("%06d.data", chunkID))
}

func getIndexPath(dataDir string, chunkID uint32) string {
	return filepath.Join(getChunkDir(dataDir, chunkID), fmt.Sprintf("%06d.index", chunkID))
}

// =============================================================================
// Ledger Reading (adapted from original)
// =============================================================================

// ReadLedgerWithTiming reads a single ledger and returns granular timing breakdown
func ReadLedgerWithTiming(dataDir string, ledgerSeq uint32, decoder *zstd.Decoder) (xdr.LedgerCloseMeta, LedgerTiming, *BenchmarkError) {
	var lcm xdr.LedgerCloseMeta
	var timing LedgerTiming
	totalStart := time.Now()

	chunkID := ledgerToChunkID(ledgerSeq)
	localIndex := ledgerToLocalIndex(ledgerSeq)

	// === INDEX LOOKUP ===
	indexStart := time.Now()

	indexPath := getIndexPath(dataDir, chunkID)
	indexFile, err := os.Open(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return lcm, timing, newBenchmarkError(ErrTypeIndexFileNotFound, ledgerSeq, chunkID,
				fmt.Sprintf("Index file not found: %s", indexPath), err)
		}
		return lcm, timing, newBenchmarkError(ErrTypeIndexReadFailed, ledgerSeq, chunkID,
			fmt.Sprintf("Failed to open index file: %s", indexPath), err)
	}

	// Read header
	header := make([]byte, IndexHeaderSize)
	if _, err := indexFile.ReadAt(header, 0); err != nil {
		indexFile.Close()
		return lcm, timing, newBenchmarkError(ErrTypeIndexReadFailed, ledgerSeq, chunkID,
			"Failed to read index header", err)
	}

	version := header[0]
	offsetSize := header[1]

	if version != IndexVersion {
		indexFile.Close()
		return lcm, timing, newBenchmarkError(ErrTypeInvalidIndexFormat, ledgerSeq, chunkID,
			fmt.Sprintf("Unsupported index version: %d", version), nil)
	}

	if offsetSize != 4 && offsetSize != 8 {
		indexFile.Close()
		return lcm, timing, newBenchmarkError(ErrTypeInvalidIndexFormat, ledgerSeq, chunkID,
			fmt.Sprintf("Invalid offset size: %d", offsetSize), nil)
	}

	// Read two adjacent offsets
	entryPos := int64(IndexHeaderSize) + int64(localIndex)*int64(offsetSize)
	offsetBuf := make([]byte, offsetSize*2)
	if _, err := indexFile.ReadAt(offsetBuf, entryPos); err != nil {
		indexFile.Close()
		return lcm, timing, newBenchmarkError(ErrTypeIndexReadFailed, ledgerSeq, chunkID,
			fmt.Sprintf("Failed to read offsets at position %d", entryPos), err)
	}

	indexFile.Close()

	var startOffset, endOffset uint64
	if offsetSize == 4 {
		startOffset = uint64(binary.LittleEndian.Uint32(offsetBuf[0:4]))
		endOffset = uint64(binary.LittleEndian.Uint32(offsetBuf[4:8]))
	} else {
		startOffset = binary.LittleEndian.Uint64(offsetBuf[0:8])
		endOffset = binary.LittleEndian.Uint64(offsetBuf[8:16])
	}

	recordSize := endOffset - startOffset
	timing.IndexLookupTime = time.Since(indexStart)

	// === DATA READ ===
	dataReadStart := time.Now()

	dataPath := getDataPath(dataDir, chunkID)
	dataFile, err := os.Open(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return lcm, timing, newBenchmarkError(ErrTypeDataFileNotFound, ledgerSeq, chunkID,
				fmt.Sprintf("Data file not found: %s", dataPath), err)
		}
		return lcm, timing, newBenchmarkError(ErrTypeDataReadFailed, ledgerSeq, chunkID,
			fmt.Sprintf("Failed to open data file: %s", dataPath), err)
	}

	compressed := make([]byte, recordSize)
	if _, err := dataFile.ReadAt(compressed, int64(startOffset)); err != nil {
		dataFile.Close()
		return lcm, timing, newBenchmarkError(ErrTypeDataReadFailed, ledgerSeq, chunkID,
			fmt.Sprintf("Failed to read compressed data at offset %d", startOffset), err)
	}

	dataFile.Close()
	timing.DataReadTime = time.Since(dataReadStart)

	// === DECOMPRESS ===
	decompressStart := time.Now()

	uncompressed, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return lcm, timing, newBenchmarkError(ErrTypeDecompressFailed, ledgerSeq, chunkID,
			"Failed to decompress data", err)
	}
	timing.DecompressTime = time.Since(decompressStart)

	// === UNMARSHAL ===
	unmarshalStart := time.Now()

	if err := lcm.UnmarshalBinary(uncompressed); err != nil {
		return lcm, timing, newBenchmarkError(ErrTypeUnmarshalFailed, ledgerSeq, chunkID,
			"Failed to unmarshal LedgerCloseMeta", err)
	}
	timing.UnmarshalTime = time.Since(unmarshalStart)

	timing.TotalTime = time.Since(totalStart)

	return lcm, timing, nil
}

// =============================================================================
// Input File Parsing
// =============================================================================

func parseInputFile(inputPath string) ([]uint32, error) {
	file, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	var ledgers []uint32
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		val, err := strconv.ParseUint(line, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid ledger sequence at line %d: %q: %w", lineNum, line, err)
		}

		if val < FirstLedgerSequence {
			return nil, fmt.Errorf("ledger sequence at line %d must be >= %d, got %d",
				lineNum, FirstLedgerSequence, val)
		}

		ledgers = append(ledgers, uint32(val))
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading input file: %w", err)
	}

	return ledgers, nil
}

// =============================================================================
// Logger Setup
// =============================================================================

type BenchmarkLogger struct {
	infoLogger  *log.Logger
	errorLogger *log.Logger
	logFile     *os.File
	errorFile   *os.File
}

func NewBenchmarkLogger(logPath, errorPath string) (*BenchmarkLogger, error) {
	bl := &BenchmarkLogger{}

	// Setup info logger
	if logPath != "" {
		f, err := os.Create(logPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create log file: %w", err)
		}
		bl.logFile = f
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
		bl.errorLogger = log.New(f, "", log.LstdFlags)
	} else {
		bl.errorLogger = log.New(os.Stdout, "", log.LstdFlags)
	}

	return bl, nil
}

func (bl *BenchmarkLogger) Info(format string, v ...interface{}) {
	bl.infoLogger.Printf(format, v...)
}

func (bl *BenchmarkLogger) Error(format string, v ...interface{}) {
	bl.errorLogger.Printf(format, v...)
}

func (bl *BenchmarkLogger) Close() {
	if bl.logFile != nil {
		bl.logFile.Close()
	}
	if bl.errorFile != nil {
		bl.errorFile.Close()
	}
}

// =============================================================================
// Worker Function
// =============================================================================

func benchmarkWorker(
	workerID int,
	dataDir string,
	ledgers []uint32,
	stats *AggregatedStats,
	progressChan chan<- int,
	errorChan chan<- *BenchmarkError,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	// Each worker gets its own decoder
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		errorChan <- newBenchmarkError(ErrTypeDecompressFailed, 0, 0,
			fmt.Sprintf("Worker %d failed to create decoder", workerID), err)
		return
	}
	defer decoder.Close()

	for _, ledgerSeq := range ledgers {
		_, timing, benchErr := ReadLedgerWithTiming(dataDir, ledgerSeq, decoder)

		if benchErr != nil {
			stats.IncrementError()
			errorChan <- benchErr
		} else {
			stats.AddSample(timing)
		}

		progressChan <- 1
	}
}

// =============================================================================
// Progress Reporter
// =============================================================================

func progressReporter(
	logger *BenchmarkLogger,
	stats *AggregatedStats,
	totalLedgers int,
	progressChan <-chan int,
	doneChan chan<- struct{},
	startTime time.Time,
) {
	processed := int64(0)
	lastReportedPercent := -1

	for range progressChan {
		processed++
		currentPercent := int((processed * 100) / int64(totalLedgers))

		if currentPercent > lastReportedPercent {
			lastReportedPercent = currentPercent
			elapsed := time.Since(startTime)

			stats.mu.Lock()
			successCount := stats.SuccessCount
			errorCount := stats.ErrorCount

			// Calculate running averages
			var avgIndexLookup, avgDataRead, avgDecompress, avgUnmarshal, avgFetch, avgTotal time.Duration
			if successCount > 0 {
				avgIndexLookup = stats.TotalIndexLookup / time.Duration(successCount)
				avgDataRead = stats.TotalDataRead / time.Duration(successCount)
				avgDecompress = stats.TotalDecompress / time.Duration(successCount)
				avgUnmarshal = stats.TotalUnmarshal / time.Duration(successCount)
				avgFetch = stats.TotalFetch / time.Duration(successCount)
				avgTotal = stats.TotalTime / time.Duration(successCount)
			}
			stats.mu.Unlock()

			// Calculate ETA
			var eta time.Duration
			if processed > 0 {
				avgTimePerLedger := elapsed / time.Duration(processed)
				remaining := int64(totalLedgers) - processed
				eta = avgTimePerLedger * time.Duration(remaining)
			}

			rate := float64(0)
			if elapsed.Seconds() > 0 {
				rate = float64(processed) / elapsed.Seconds()
			}

			logger.Info("")
			logger.Info("================================================================================")
			logger.Info("    PROGRESS: %d%% | %s / %s ledgers | Elapsed: %s | ETA: %s",
				currentPercent,
				helpers.FormatNumber(processed),
				helpers.FormatNumber(int64(totalLedgers)),
				helpers.FormatDuration(elapsed),
				helpers.FormatDuration(eta))
			logger.Info("    Rate: %.2f ledgers/sec | Success: %s | Errors: %s",
				rate,
				helpers.FormatNumber(successCount),
				helpers.FormatNumber(errorCount))
			logger.Info("    Running Averages:")
			logger.Info("      Index Lookup: %s | Data Read: %s | Fetch Total: %s",
				helpers.FormatDuration(avgIndexLookup),
				helpers.FormatDuration(avgDataRead),
				helpers.FormatDuration(avgFetch))
			logger.Info("      Decompress: %s | Unmarshal: %s | Total: %s",
				helpers.FormatDuration(avgDecompress),
				helpers.FormatDuration(avgUnmarshal),
				helpers.FormatDuration(avgTotal))
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
		logger.Error("[%s] %s Ledger=%d Chunk=%d Timestamp=%s",
			err.Type,
			err.Message,
			err.LedgerSeq,
			err.ChunkID,
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

func runBenchmark(dataDir, inputFile string, workerCount int, logger *BenchmarkLogger) error {
	logger.Info("")
	logger.Info("################################################################################")
	logger.Info("                     FILE-BASED LEDGER STORAGE BENCHMARK")
	logger.Info("################################################################################")
	logger.Info("")

	// Parse input file
	logger.Info("Loading input file: %s", inputFile)
	ledgers, err := parseInputFile(inputFile)
	if err != nil {
		return fmt.Errorf("failed to parse input file: %w", err)
	}

	totalLedgers := len(ledgers)
	logger.Info("Loaded %s ledger sequences", helpers.FormatNumber(int64(totalLedgers)))
	logger.Info("")

	if totalLedgers == 0 {
		return fmt.Errorf("input file contains no valid ledger sequences")
	}

	// Print configuration
	logger.Info("CONFIGURATION:")
	logger.Info("  Data Directory:  %s", dataDir)
	logger.Info("  Input File:      %s", inputFile)
	logger.Info("  Total Ledgers:   %s", helpers.FormatNumber(int64(totalLedgers)))
	logger.Info("  Worker Count:    %d", workerCount)
	logger.Info("")

	// Initialize stats
	stats := NewAggregatedStats(totalLedgers)

	// Split ledgers among workers
	ledgersPerWorker := totalLedgers / workerCount
	remainder := totalLedgers % workerCount

	workerLedgers := make([][]uint32, workerCount)
	start := 0
	for i := 0; i < workerCount; i++ {
		count := ledgersPerWorker
		if i < remainder {
			count++
		}
		end := start + count
		if end > totalLedgers {
			end = totalLedgers
		}
		workerLedgers[i] = ledgers[start:end]
		start = end
	}

	logger.Info("WORKER DISTRIBUTION:")
	for i, wl := range workerLedgers {
		logger.Info("  Worker %2d: %s ledgers", i, helpers.FormatNumber(int64(len(wl))))
	}
	logger.Info("")

	// Create channels
	progressChan := make(chan int, 1000)
	errorChan := make(chan *BenchmarkError, 1000)
	progressDone := make(chan struct{})
	errorDone := make(chan struct{})

	// Start time
	startTime := time.Now()

	// Start progress reporter
	go progressReporter(logger, stats, totalLedgers, progressChan, progressDone, startTime)

	// Start error collector
	go errorCollector(logger, errorChan, errorDone)

	// Start workers
	var wg sync.WaitGroup
	logger.Info("================================================================================")
	logger.Info("                          STARTING BENCHMARK")
	logger.Info("================================================================================")
	logger.Info("")

	for i := 0; i < workerCount; i++ {
		if len(workerLedgers[i]) > 0 {
			wg.Add(1)
			go benchmarkWorker(i, dataDir, workerLedgers[i], stats, progressChan, errorChan, &wg)
		}
	}

	// Wait for all workers to complete
	wg.Wait()

	// Close channels and wait for collectors
	close(progressChan)
	close(errorChan)
	<-progressDone
	<-errorDone

	// Calculate final statistics
	elapsed := time.Since(startTime)

	logger.Info("")
	logger.Info("################################################################################")
	logger.Info("################################################################################")
	logger.Info("                           BENCHMARK COMPLETE")
	logger.Info("################################################################################")
	logger.Info("################################################################################")
	logger.Info("")

	// Lock stats for final calculations
	stats.mu.Lock()

	// Compute final statistics for each metric
	indexLookupStats := computeMetricStats(stats.IndexLookupSamples)
	dataReadStats := computeMetricStats(stats.DataReadSamples)
	decompressStats := computeMetricStats(stats.DecompressSamples)
	unmarshalStats := computeMetricStats(stats.UnmarshalSamples)
	fetchStats := computeMetricStats(stats.FetchSamples)
	totalStats := computeMetricStats(stats.TotalSamples)

	successCount := stats.SuccessCount
	errorCount := stats.ErrorCount

	stats.mu.Unlock()

	// Log final summary
	logger.Info("SUMMARY:")
	logger.Info("  Total Time:          %s", helpers.FormatDuration(elapsed))
	logger.Info("  Total Ledgers:       %s", helpers.FormatNumber(int64(totalLedgers)))
	logger.Info("  Successful Reads:    %s", helpers.FormatNumber(successCount))
	logger.Info("  Failed Reads:        %s", helpers.FormatNumber(errorCount))
	logger.Info("  Throughput:          %.2f ledgers/sec", float64(successCount)/elapsed.Seconds())
	logger.Info("")

	logger.Info("================================================================================")
	logger.Info("                           TIMING STATISTICS")
	logger.Info("================================================================================")
	logger.Info("")

	logger.Info("FETCH FROM STORAGE (Index Lookup + Data Read):")
	logger.Info("  Mean:       %s", helpers.FormatDuration(fetchStats.Mean))
	logger.Info("  Std Dev:    %s", helpers.FormatDuration(fetchStats.StdDev))
	logger.Info("  P50:        %s", helpers.FormatDuration(fetchStats.P50))
	logger.Info("  P95:        %s", helpers.FormatDuration(fetchStats.P95))
	logger.Info("  P99:        %s", helpers.FormatDuration(fetchStats.P99))
	logger.Info("")

	logger.Info("  Breakdown - Index Lookup:")
	logger.Info("    Mean:     %s | Std Dev: %s", helpers.FormatDuration(indexLookupStats.Mean), helpers.FormatDuration(indexLookupStats.StdDev))
	logger.Info("    P50:      %s | P95: %s | P99: %s",
		helpers.FormatDuration(indexLookupStats.P50), helpers.FormatDuration(indexLookupStats.P95), helpers.FormatDuration(indexLookupStats.P99))
	logger.Info("")

	logger.Info("  Breakdown - Data Read:")
	logger.Info("    Mean:     %s | Std Dev: %s", helpers.FormatDuration(dataReadStats.Mean), helpers.FormatDuration(dataReadStats.StdDev))
	logger.Info("    P50:      %s | P95: %s | P99: %s",
		helpers.FormatDuration(dataReadStats.P50), helpers.FormatDuration(dataReadStats.P95), helpers.FormatDuration(dataReadStats.P99))
	logger.Info("")

	logger.Info("POST-FETCH PROCESSING:")
	logger.Info("  Decompress:")
	logger.Info("    Mean:     %s | Std Dev: %s", helpers.FormatDuration(decompressStats.Mean), helpers.FormatDuration(decompressStats.StdDev))
	logger.Info("    P50:      %s | P95: %s | P99: %s",
		helpers.FormatDuration(decompressStats.P50), helpers.FormatDuration(decompressStats.P95), helpers.FormatDuration(decompressStats.P99))
	logger.Info("")

	logger.Info("  Unmarshal:")
	logger.Info("    Mean:     %s | Std Dev: %s", helpers.FormatDuration(unmarshalStats.Mean), helpers.FormatDuration(unmarshalStats.StdDev))
	logger.Info("    P50:      %s | P95: %s | P99: %s",
		helpers.FormatDuration(unmarshalStats.P50), helpers.FormatDuration(unmarshalStats.P95), helpers.FormatDuration(unmarshalStats.P99))
	logger.Info("")

	logger.Info("TOTAL (End-to-End per Ledger):")
	logger.Info("  Mean:       %s", helpers.FormatDuration(totalStats.Mean))
	logger.Info("  Std Dev:    %s", helpers.FormatDuration(totalStats.StdDev))
	logger.Info("  P50:        %s", helpers.FormatDuration(totalStats.P50))
	logger.Info("  P95:        %s", helpers.FormatDuration(totalStats.P95))
	logger.Info("  P99:        %s", helpers.FormatDuration(totalStats.P99))
	logger.Info("")

	logger.Info("================================================================================")
	logger.Info("                           COMPARISON SUMMARY")
	logger.Info("================================================================================")
	logger.Info("")
	logger.Info("  Fetch (storage):     %s  <-- Compare to RocksDB", helpers.FormatDuration(fetchStats.Mean))
	logger.Info("  Decompress:          %s", helpers.FormatDuration(decompressStats.Mean))
	logger.Info("  Unmarshal:           %s", helpers.FormatDuration(unmarshalStats.Mean))
	logger.Info("  ─────────────────────────")
	logger.Info("  TOTAL:               %s", helpers.FormatDuration(totalStats.Mean))
	logger.Info("")
	logger.Info("################################################################################")
	logger.Info("################################################################################")
	logger.Info("")

	return nil
}

// =============================================================================
// Main Entry Point
// =============================================================================

func main() {
	var (
		dataDir     string
		inputFile   string
		logFile     string
		errorFile   string
		workerCount int
	)

	flag.StringVar(&dataDir, "data-dir", "", "Root directory for ledger storage (required)")
	flag.StringVar(&inputFile, "input-file", "", "File containing ledger sequences, one per line (required)")
	flag.StringVar(&logFile, "log-file", "", "Output file for logs (default: stdout)")
	flag.StringVar(&errorFile, "error-file", "", "Output file for errors (default: stdout)")
	flag.IntVar(&workerCount, "workers", DefaultWorkerCount, "Number of concurrent workers")

	flag.Parse()

	// Validate required arguments
	if dataDir == "" {
		log.Fatal("ERROR: --data-dir is required")
	}
	if inputFile == "" {
		log.Fatal("ERROR: --input-file is required")
	}

	// Get absolute paths
	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		log.Fatalf("ERROR: Failed to get absolute path for data-dir: %v", err)
	}

	absInputFile, err := filepath.Abs(inputFile)
	if err != nil {
		log.Fatalf("ERROR: Failed to get absolute path for input-file: %v", err)
	}

	// Validate worker count
	if workerCount < 1 {
		log.Fatal("ERROR: --workers must be at least 1")
	}

	// Setup logger
	logger, err := NewBenchmarkLogger(logFile, errorFile)
	if err != nil {
		log.Fatalf("ERROR: Failed to setup logger: %v", err)
	}
	defer logger.Close()

	// Run benchmark
	if err := runBenchmark(absDataDir, absInputFile, workerCount, logger); err != nil {
		logger.Error("Benchmark failed: %v", err)
		os.Exit(1)
	}
}
