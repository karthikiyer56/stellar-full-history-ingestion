package main

// =============================================================================
// recsplit_builder_optimized.go
// =============================================================================
//
// PURPOSE:
//   Builds a RecSplit index from a raw binary data file that maps
//   txHash (32 bytes) -> ledgerSeq (4 bytes).
//
//   This is an OPTIMIZED version with the following improvements:
//   - Buffered I/O with large buffers (configurable, default 256MB)
//   - Memory-mapped file reading option for faster access
//   - Parallel verification using worker pools
//   - Percentage-based progress reporting (1% increments)
//   - Reduced memory stats collection frequency
//   - Pre-allocated buffers to reduce GC pressure
//   - Batch processing for better CPU cache utilization
//
// RAW DATA FILE FORMAT:
//   - No headers, no delimiters, no newlines
//   - Each record is exactly 36 bytes:
//     [32 bytes: txHash (big-endian)] [4 bytes: ledgerSeq (big-endian uint32)]
//   - Records are written contiguously
//   - File size / 36 = transaction count
//
// RECSPLIT OVERVIEW:
//   RecSplit creates a minimal perfect hash function (MPHF) that maps N keys
//   to N unique integers [0, N-1]. Additionally, it stores associated values
//   using Elias-Fano encoding.
//
//   The index file contains:
//   - GolombRice: The perfect hash function (~2 bits/key)
//   - EliasFano (ef): Bucket metadata (~1-2 bits/key)
//   - EliasFano (offsets): The stored values (ledgerSeq) - size varies based on value distribution
//   - Existence filter (optional): For detecting non-existent keys (~9 bits/key if enabled)
//
// USAGE:
//   ./recsplit_builder \
//     --raw-data-file /path/to/transactions.bin \
//     --output-file /path/to/output/txhash.idx \
//     [--bucket-size 2000] \
//     [--leaf-size 8] \
//     [--enable-false-positive-support] \
//     [--tmp-dir /path/to/tmp] \
//     [--verify-count 10000] \
//     [--read-buffer-mb 1024] \
//     [--verify-workers 64]
//
// BUCKET SIZE / LEAF SIZE GUIDELINES:
//   These values are generally fixed regardless of key count. Erigon uses:
//   - BucketSize: 2000 (controls keys per bucket, affects build time vs index size)
//   - LeafSize: 8 (max keys in a leaf, affects lookup speed)
//
//   The algorithm scales by increasing bucket COUNT, not bucket size.
//   For most use cases, the defaults work well for any scale from 100 to 2.5B keys.
//
// MEMORY REQUIREMENTS:
//   - ~40 bytes per key during construction (for 2.5B keys: ~100GB)
//   - For 1.5B keys: ~60GB
//   - For 1B keys: ~40GB
//   - This implementation assumes everything fits in memory (up to 256GB usable)
//
// VERIFY COUNT:
//   --verify-count controls how many keys to verify after building:
//   -  0 = skip verification entirely
//   - -1 = verify ALL keys (can take hours for billions of keys)
//   -  N = verify N randomly sampled keys (default: 10000)
//
// PERFORMANCE TIPS FOR LARGE DATASETS (2B+ keys):
//   1. Use a fast SSD or NVMe drive for both input and tmp-dir
//   2. Set --read-buffer-mb to 1024 or higher (you have 256GB RAM)
//   3. Set --verify-workers to match your CPU core count (64 for your machine)
//   4. Consider using a RAM disk for tmp-dir if possible
//   5. Verification automatically uses memory-mapped I/O for speed
//
// TMP DIR CLEANUP:
//   The --tmp-dir directory will be AUTOMATICALLY DELETED (recursively) when:
//   - The program completes successfully
//   - The program exits with an error
//   - The program crashes or panics
//   WARNING: If you specify an existing directory with files, those files WILL BE DELETED!
//
// =============================================================================

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"io"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	erigonlog "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// RecordSize is the size of each record in the raw data file
	RecordSize = 36 // 32 bytes txHash + 4 bytes ledgerSeq

	// TxHashSize is the size of a transaction hash
	TxHashSize = 32

	// DefaultVerifyCount is the default number of keys to verify
	DefaultVerifyCount = 10000

	// DefaultReadBufferMB is the default read buffer size in megabytes
	// Larger buffers significantly reduce syscall overhead
	DefaultReadBufferMB = 256

	// DefaultVerifyWorkers is the default number of parallel verification workers
	DefaultVerifyWorkers = 16

	// BatchSize is the number of records to process in a batch
	// This improves CPU cache utilization
	BatchSize = 100000

	// MemStatsInterval controls how often we collect memory stats (as percentage)
	// Collecting mem stats is expensive, so we do it sparingly
	MemStatsInterval = 10 // Every 10%
)

// =============================================================================
// Global logger
// =============================================================================

var logger *log.Logger

// =============================================================================
// Timing structures
// =============================================================================

// BuildTiming holds timing information for each build phase
type BuildTiming struct {
	FileOpen     time.Duration // Time to open and analyze file
	KeyCounting  time.Duration // Time to count keys (included in FileOpen)
	RecSplitInit time.Duration // Time to initialize RecSplit
	KeyReading   time.Duration // Time to read all keys and add to RecSplit
	IndexBuild   time.Duration // Time to build the perfect hash function
	Verification time.Duration // Time to verify the index
	TotalTime    time.Duration // Total wall-clock time
}

// printMemStats prints current memory statistics using logger
// This function calls runtime.ReadMemStats which causes a brief stop-the-world pause
// Use sparingly (every 10% progress) to minimize impact on performance
func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	logger.Printf("  Memory: Alloc=%s, Sys=%s, HeapInuse=%s, NumGC=%d",
		helpers.FormatBytes(int64(m.Alloc)),
		helpers.FormatBytes(int64(m.Sys)),
		helpers.FormatBytes(int64(m.HeapInuse)),
		m.NumGC)
}

// printSeparator prints a visual separator line
func printSeparator() {
	logger.Println("================================================================================")
}

// =============================================================================
// Tmp directory cleanup management
// =============================================================================

// TmpDirCleaner manages automatic cleanup of the temporary directory
type TmpDirCleaner struct {
	tmpDir  string
	cleaned bool
	mu      sync.Mutex
}

// NewTmpDirCleaner creates a new cleaner and sets up signal handlers and panic recovery
func NewTmpDirCleaner(tmpDir string) *TmpDirCleaner {
	cleaner := &TmpDirCleaner{
		tmpDir:  tmpDir,
		cleaned: false,
	}

	// Set up signal handler for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		sig := <-sigChan
		logger.Printf("Received signal %v, cleaning up tmp directory: %s", sig, tmpDir)
		cleaner.Cleanup()
		os.Exit(1)
	}()

	return cleaner
}

// Cleanup removes the temporary directory recursively
func (c *TmpDirCleaner) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cleaned {
		return
	}

	if c.tmpDir != "" {
		logger.Printf("Cleaning up tmp directory: %s", c.tmpDir)
		if err := os.RemoveAll(c.tmpDir); err != nil {
			logger.Printf("WARNING: Failed to remove tmp directory %s: %v", c.tmpDir, err)
		} else {
			logger.Printf("Successfully removed tmp directory: %s", c.tmpDir)
		}
	}
	c.cleaned = true
}

// =============================================================================
// Memory-mapped file reader (optional, for faster access)
// =============================================================================

// MmapReader provides memory-mapped file access for faster reading
type MmapReader struct {
	data     []byte // Memory-mapped data
	fileSize int64  // Size of the file
	file     *os.File
}

// NewMmapReader creates a new memory-mapped file reader
func NewMmapReader(path string) (*MmapReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	fileSize := info.Size()

	// Memory-map the file using unix package (works on Linux)
	data, err := unix.Mmap(int(file.Fd()), 0, int(fileSize),
		unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}

	// Advise the kernel about our access pattern
	// For verification, we do random lookups, so MADV_RANDOM is more appropriate
	// This tells the kernel to disable read-ahead since we won't be reading sequentially
	// Note: This is just a hint to the kernel - if it fails, we can continue without it
	if err := unix.Madvise(data, unix.MADV_RANDOM); err != nil {
		// Log the error but don't fail - madvise is an optimization hint, not critical
		logger.Printf("  Warning: madvise(MADV_RANDOM) failed: %v (continuing anyway)", err)
	}

	return &MmapReader{
		data:     data,
		fileSize: fileSize,
		file:     file,
	}, nil
}

// Close unmaps and closes the file
func (r *MmapReader) Close() error {
	if r.data != nil {
		unix.Munmap(r.data)
		r.data = nil
	}
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// GetRecord returns the record at the given index
func (r *MmapReader) GetRecord(index int) (txHash []byte, ledgerSeq uint32) {
	offset := index * RecordSize
	txHash = r.data[offset : offset+TxHashSize]
	ledgerSeq = binary.BigEndian.Uint32(r.data[offset+TxHashSize : offset+RecordSize])
	return
}

// =============================================================================
// Parallel verification
// =============================================================================

// parallelVerifyAll verifies all keys using multiple workers
func parallelVerifyAll(
	idx *recsplit.Index,
	mmapReader *MmapReader,
	keyCount int,
	numWorkers int,
	progressChan chan<- int, // Channel to report progress
) (verified, failed int64) {
	var wg sync.WaitGroup
	var verifiedCount, failedCount atomic.Int64

	// Create a channel for work distribution
	// Each item is a range of records to verify
	type workRange struct {
		start int
		end   int
	}
	workChan := make(chan workRange, numWorkers*2)

	// Start workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Each worker gets its own index reader
			reader := recsplit.NewIndexReader(idx)
			defer reader.Close()

			for work := range workChan {
				localVerified := int64(0)
				localFailed := int64(0)

				for i := work.start; i < work.end; i++ {
					txHash, expectedSeq := mmapReader.GetRecord(i)
					actualSeq, found := reader.Lookup(txHash)

					if !found {
						localFailed++
					} else if uint32(actualSeq) == expectedSeq {
						localVerified++
					} else {
						localFailed++
					}
				}

				verifiedCount.Add(localVerified)
				failedCount.Add(localFailed)

				// Report progress
				if progressChan != nil {
					progressChan <- work.end - work.start
				}
			}
		}()
	}

	// Distribute work in chunks
	chunkSize := 100000 // 100k records per chunk for good load balancing
	for start := 0; start < keyCount; start += chunkSize {
		end := start + chunkSize
		if end > keyCount {
			end = keyCount
		}
		workChan <- workRange{start: start, end: end}
	}
	close(workChan)

	wg.Wait()
	return verifiedCount.Load(), failedCount.Load()
}

// parallelVerifySample verifies a random sample of keys using multiple workers
func parallelVerifySample(
	idx *recsplit.Index,
	mmapReader *MmapReader,
	keyCount int,
	sampleSize int,
	numWorkers int,
) (verified, failed int64) {
	// Generate random indices
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	indices := rng.Perm(keyCount)[:sampleSize]

	var wg sync.WaitGroup
	var verifiedCount, failedCount atomic.Int64

	// Distribute indices among workers
	indicesPerWorker := (sampleSize + numWorkers - 1) / numWorkers

	for w := 0; w < numWorkers; w++ {
		start := w * indicesPerWorker
		end := start + indicesPerWorker
		if end > sampleSize {
			end = sampleSize
		}
		if start >= sampleSize {
			break
		}

		wg.Add(1)
		go func(workerIndices []int) {
			defer wg.Done()

			reader := recsplit.NewIndexReader(idx)
			defer reader.Close()

			localVerified := int64(0)
			localFailed := int64(0)

			for _, recordIdx := range workerIndices {
				txHash, expectedSeq := mmapReader.GetRecord(recordIdx)
				actualSeq, found := reader.Lookup(txHash)

				if !found {
					localFailed++
				} else if uint32(actualSeq) == expectedSeq {
					localVerified++
				} else {
					localFailed++
				}
			}

			verifiedCount.Add(localVerified)
			failedCount.Add(localFailed)
		}(indices[start:end])
	}

	wg.Wait()
	return verifiedCount.Load(), failedCount.Load()
}

// =============================================================================
// Main
// =============================================================================

func main() {
	// Initialize logger with timestamps
	logger = log.New(os.Stdout, "", log.LstdFlags)

	// -------------------------------------------------------------------------
	// Parse command-line flags
	// -------------------------------------------------------------------------
	var (
		rawDataFile                string
		outputFile                 string
		tmpDir                     string
		bucketSize                 int
		leafSize                   int
		enableFalsePositiveSupport bool
		verifyCount                int
		readBufferMB               int
		verifyWorkers              int
	)

	flag.StringVar(&rawDataFile, "raw-data-file", "", "Path to raw binary data file (required)")
	flag.StringVar(&outputFile, "output-file", "", "Full path to output index file (required)")
	flag.StringVar(&tmpDir, "tmp-dir", "", "Temp directory for RecSplit (required, will be auto-deleted on exit/crash)")
	flag.IntVar(&bucketSize, "bucket-size", 0, "RecSplit bucket size (0 = use default)")
	flag.IntVar(&leafSize, "leaf-size", 0, "RecSplit leaf size (0 = use default)")
	flag.BoolVar(&enableFalsePositiveSupport, "enable-false-positive-support", false, "Enable false-positive detection filter (~9 bits/key overhead), sets LessFalsePositives=true and DataVersion=1")
	flag.IntVar(&verifyCount, "verify-count", DefaultVerifyCount, "Number of keys to verify after build: 0=skip, -1=all, N=sample N keys")
	flag.IntVar(&readBufferMB, "read-buffer-mb", DefaultReadBufferMB, "Read buffer size in MB for key reading phase (larger = fewer syscalls)")
	flag.IntVar(&verifyWorkers, "verify-workers", DefaultVerifyWorkers, "Number of parallel workers for verification")

	flag.Parse()

	// Validate required flags
	if rawDataFile == "" || outputFile == "" || tmpDir == "" {
		logger.Println("Usage: recsplit_builder --raw-data-file <path> --output-file <path> --tmp-dir <path> [options]")
		logger.Println()
		flag.PrintDefaults()
		logger.Println()
		logger.Println("Examples:")
		logger.Println("  # Basic usage with defaults (256MB buffer)")
		logger.Println("  recsplit_builder --raw-data-file txns.bin --output-file ./index/txhash.idx --tmp-dir /tmp/recsplit")
		logger.Println()
		logger.Println("  # Maximum performance for large datasets (2B keys)")
		logger.Println("  recsplit_builder --raw-data-file txns.bin --output-file ./index/txhash.idx --tmp-dir /tmp/recsplit \\")
		logger.Println("    --read-buffer-mb 1024 --verify-workers 64 --verify-count -1")
		logger.Println()
		logger.Println("  # With false-positive protection (automatically sets LessFalsePositives=true, DataVersion=1)")
		logger.Println("  recsplit_builder --raw-data-file txns.bin --output-file ./index/txhash.idx --tmp-dir /tmp/recsplit \\")
		logger.Println("    --enable-false-positive-support")
		logger.Println()
		logger.Println("NOTE: The --tmp-dir will be DELETED (recursively) when the program exits or crashes!")
		os.Exit(1)
	}

	// When --enable-false-positive-support is set, automatically configure:
	// - LessFalsePositives = true
	// - DataVersion = 1
	lessFalsePositives := false
	dataVersion := 0
	if enableFalsePositiveSupport {
		lessFalsePositives = true
		dataVersion = 1
	}

	// Set verify workers to number of CPUs if not specified or too high
	if verifyWorkers <= 0 {
		verifyWorkers = runtime.NumCPU()
	}

	// Create tmp directory if needed
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		logger.Printf("ERROR: Failed to create temp directory %s: %v", tmpDir, err)
		os.Exit(1)
	}

	// Set up automatic cleanup of tmp directory on exit/crash
	cleaner := NewTmpDirCleaner(tmpDir)
	defer cleaner.Cleanup()

	// Also handle panics
	defer func() {
		if r := recover(); r != nil {
			logger.Printf("PANIC: %v", r)
			cleaner.Cleanup()
			panic(r) // Re-panic after cleanup
		}
	}()

	// Create output directory if needed
	outputDir := filepath.Dir(outputFile)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		logger.Printf("ERROR: Failed to create output directory %s: %v", outputDir, err)
		os.Exit(1)
	}

	// Set defaults for bucket/leaf size
	if bucketSize == 0 {
		bucketSize = recsplit.DefaultBucketSize
	}
	if leafSize == 0 {
		leafSize = int(recsplit.DefaultLeafSize)
	}

	// Calculate read buffer size in bytes
	readBufferSize := readBufferMB * 1024 * 1024
	// Ensure buffer size is a multiple of record size for clean reads
	readBufferSize = (readBufferSize / RecordSize) * RecordSize

	timing := BuildTiming{}
	totalStart := time.Now()

	// -------------------------------------------------------------------------
	// Print configuration
	// -------------------------------------------------------------------------
	printSeparator()
	logger.Println("RecSplit Index Builder (OPTIMIZED)")
	printSeparator()
	logger.Println()
	logger.Println("Configuration:")
	logger.Printf("  Raw data file:               %s", rawDataFile)
	logger.Printf("  Output file:                 %s", outputFile)
	logger.Printf("  Temp directory:              %s (will be auto-deleted)", tmpDir)
	logger.Printf("  Bucket size:                 %d", bucketSize)
	logger.Printf("  Leaf size:                   %d", leafSize)
	logger.Printf("  Enable false-positive support: %v", enableFalsePositiveSupport)
	if enableFalsePositiveSupport {
		logger.Printf("    -> LessFalsePositives:     %v (auto-set)", lessFalsePositives)
		logger.Printf("    -> DataVersion:            %d (auto-set)", dataVersion)
	}
	logger.Printf("  Verify count:                %d", verifyCount)
	logger.Println()
	logger.Println("Optimization settings:")
	logger.Printf("  Read buffer size:     %s (for key reading phase)", helpers.FormatBytes(int64(readBufferSize)))
	logger.Printf("  Verify workers:       %d (for parallel verification)", verifyWorkers)
	logger.Printf("  Verification uses:    memory-mapped I/O (mmap)")
	logger.Printf("  CPU cores available:  %d", runtime.NumCPU())
	logger.Printf("  GOMAXPROCS:           %d", runtime.GOMAXPROCS(0))
	logger.Println()
	printMemStats()
	logger.Println()

	// -------------------------------------------------------------------------
	// Step 1: Open file and calculate key count
	// -------------------------------------------------------------------------
	printSeparator()
	logger.Println("STEP 1: Analyzing raw data file")
	printSeparator()
	logger.Println()

	stepStart := time.Now()

	fileInfo, err := os.Stat(rawDataFile)
	if err != nil {
		logger.Printf("ERROR: Failed to stat raw data file %s: %v", rawDataFile, err)
		os.Exit(1)
	}

	fileSize := fileInfo.Size()
	if fileSize%RecordSize != 0 {
		logger.Printf("ERROR: File size (%s bytes) is not a multiple of record size (%d)", helpers.FormatNumber(fileSize), RecordSize)
		logger.Printf("       This suggests the file is corrupted or not in the expected format.")
		os.Exit(1)
	}

	keyCount := int(fileSize / RecordSize)
	timing.FileOpen = time.Since(stepStart)

	logger.Printf("  File:                 %s", rawDataFile)
	logger.Printf("  File size:            %s (%s bytes)", helpers.FormatBytes(fileSize), helpers.FormatNumber(fileSize))
	logger.Printf("  Record size:          %d bytes", RecordSize)
	logger.Printf("  Key count:            %s", helpers.FormatNumber(int64(keyCount)))
	logger.Printf("  Time:                 %s", helpers.FormatDuration(timing.FileOpen))
	logger.Println()

	if keyCount == 0 {
		logger.Println("ERROR: No records found in file")
		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// Step 2: Initialize RecSplit
	// -------------------------------------------------------------------------
	printSeparator()
	logger.Println("STEP 2: Initializing RecSplit")
	printSeparator()
	logger.Println()

	stepStart = time.Now()

	logger.Printf("  Index file:           %s", outputFile)

	// Remove existing index if present
	os.Remove(outputFile)

	// Create logger (using erigon's logger)
	erigonLogger := erigonlog.New()

	// Calculate bucket count for information
	bucketCount := (keyCount + bucketSize - 1) / bucketSize

	logger.Printf("  Bucket size:          %d", bucketSize)
	logger.Printf("  Leaf size:            %d", leafSize)
	logger.Printf("  Bucket count:         %s", helpers.FormatNumber(int64(bucketCount)))
	logger.Printf("  Less false positives: %v", lessFalsePositives)
	logger.Printf("  Data version:         %d", dataVersion)
	logger.Println()

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:           keyCount,
		Enums:              false, // We store arbitrary values (ledgerSeq), not sequential offsets
		LessFalsePositives: lessFalsePositives,
		BucketSize:         bucketSize,
		LeafSize:           uint16(leafSize),
		TmpDir:             tmpDir,
		IndexFile:          outputFile,
		BaseDataID:         0,
		Version:            uint8(dataVersion),
	}, erigonLogger)
	if err != nil {
		logger.Printf("ERROR: Failed to create RecSplit: %v", err)
		os.Exit(1)
	}
	defer rs.Close()

	timing.RecSplitInit = time.Since(stepStart)
	logger.Printf("  Initialization time:  %s", helpers.FormatDuration(timing.RecSplitInit))
	logger.Println()
	printMemStats()
	logger.Println()

	// -------------------------------------------------------------------------
	// Step 3: Read raw data file and add keys
	// -------------------------------------------------------------------------
	printSeparator()
	logger.Println("STEP 3: Reading keys and adding to RecSplit")
	printSeparator()
	logger.Println()

	stepStart = time.Now()

	file, err := os.Open(rawDataFile)
	if err != nil {
		logger.Printf("ERROR: Failed to open raw data file %s: %v", rawDataFile, err)
		os.Exit(1)
	}
	defer file.Close()

	// Use buffered reader with large buffer for reduced syscall overhead
	// The buffer size is configurable via --read-buffer-mb flag
	bufReader := bufio.NewReaderSize(file, readBufferSize)

	// Pre-allocate a large buffer for batch reading
	// This reduces allocations and improves cache locality
	batchBuffer := make([]byte, BatchSize*RecordSize)

	keysAdded := 0
	lastPercentReported := -1
	startTime := time.Now()

	logger.Printf("  Reading %s records with %s buffer...", helpers.FormatNumber(int64(keyCount)), helpers.FormatBytes(int64(readBufferSize)))
	logger.Println()

	// Process records in batches for better performance
	for keysAdded < keyCount {
		// Calculate how many records to read in this batch
		remaining := keyCount - keysAdded
		batchRecords := BatchSize
		if remaining < batchRecords {
			batchRecords = remaining
		}
		batchBytes := batchRecords * RecordSize

		// Read a batch of records
		n, err := io.ReadFull(bufReader, batchBuffer[:batchBytes])
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			logger.Printf("ERROR: Failed to read batch at record %d: %v", keysAdded, err)
			os.Exit(1)
		}

		actualRecords := n / RecordSize
		if actualRecords == 0 {
			break
		}

		// Process each record in the batch
		for i := 0; i < actualRecords; i++ {
			offset := i * RecordSize

			// Extract txHash and ledgerSeq from the buffer
			// Note: We pass a slice of the buffer directly to avoid copying
			txHash := batchBuffer[offset : offset+TxHashSize]
			ledgerSeq := binary.BigEndian.Uint32(batchBuffer[offset+TxHashSize : offset+RecordSize])

			// Add to RecSplit
			if err := rs.AddKey(txHash, uint64(ledgerSeq)); err != nil {
				logger.Printf("ERROR: Failed to add key %d: %v", keysAdded+i, err)
				os.Exit(1)
			}
		}

		keysAdded += actualRecords

		// Report progress at each 1% increment
		currentPercent := (keysAdded * 100) / keyCount
		if currentPercent > lastPercentReported {
			elapsed := time.Since(startTime)
			rate := float64(keysAdded) / elapsed.Seconds()
			eta := time.Duration(float64(keyCount-keysAdded)/rate) * time.Second

			logger.Printf("  [%3d%%] Added %s keys | Rate: %.0f keys/sec | ETA: %s",
				currentPercent, helpers.FormatNumber(int64(keysAdded)), rate, helpers.FormatDuration(eta))

			// Print memory stats every 10% to avoid overhead
			if currentPercent%MemStatsInterval == 0 && currentPercent > 0 {
				printMemStats()
			}

			lastPercentReported = currentPercent
		}
	}

	timing.KeyReading = time.Since(stepStart)

	logger.Println()
	logger.Printf("  Total keys added:     %s", helpers.FormatNumber(int64(keysAdded)))
	logger.Printf("  Reading time:         %s", helpers.FormatDuration(timing.KeyReading))
	logger.Printf("  Average rate:         %.0f keys/sec", float64(keysAdded)/timing.KeyReading.Seconds())
	logger.Println()
	printMemStats()
	logger.Println()

	if keysAdded != keyCount {
		logger.Printf("ERROR: Key count mismatch: expected %d, got %d", keyCount, keysAdded)
		os.Exit(1)
	}

	// Close the file as we're done reading for key addition
	file.Close()

	// -------------------------------------------------------------------------
	// Step 4: Build the index
	// -------------------------------------------------------------------------
	printSeparator()
	logger.Println("STEP 4: Building RecSplit index")
	printSeparator()
	logger.Println()

	stepStart = time.Now()
	logger.Println("  Building perfect hash function...")
	logger.Println("  (This is CPU-intensive and may take a while for large datasets)")
	logger.Println()

	// Force garbage collection before the memory-intensive build phase
	// This gives us maximum available heap for the build process
	runtime.GC()
	logger.Println("  Pre-build GC completed")
	printMemStats()
	logger.Println()

	ctx := context.Background()
	if err := rs.Build(ctx); err != nil {
		if err == recsplit.ErrCollision {
			logger.Println("ERROR: Hash collision detected during build")
			logger.Println("       This is rare. Try rebuilding - a different salt will be used.")
		} else {
			logger.Printf("ERROR: Failed to build RecSplit: %v", err)
		}
		os.Exit(1)
	}

	timing.IndexBuild = time.Since(stepStart)

	logger.Printf("  Build time:           %s", helpers.FormatDuration(timing.IndexBuild))
	logger.Println()
	printMemStats()
	logger.Println()

	// -------------------------------------------------------------------------
	// Step 5: Analyze index size and structure
	// -------------------------------------------------------------------------
	printSeparator()
	logger.Println("STEP 5: Index analysis")
	printSeparator()
	logger.Println()

	// Get file size
	idxInfo, err := os.Stat(outputFile)
	if err != nil {
		logger.Printf("ERROR: Failed to stat index file %s: %v", outputFile, err)
		os.Exit(1)
	}
	idxSize := idxInfo.Size()

	logger.Printf("  Index file:           %s", outputFile)
	logger.Printf("  Index size:           %s (%s bytes)", helpers.FormatBytes(idxSize), helpers.FormatNumber(idxSize))
	logger.Printf("  Total bits/key:       %.2f", float64(idxSize*8)/float64(keyCount))
	logger.Println()

	// Open index to get detailed sizes
	idx, err := recsplit.OpenIndex(outputFile)
	if err != nil {
		logger.Printf("ERROR: Failed to open index for analysis: %v", err)
		os.Exit(1)
	}

	// Get size breakdown from the index
	total, offsets, ef, golombRice, existence, layer1 := idx.Sizes()

	logger.Println("  Size breakdown:")
	logger.Printf("    GolombRice (hash):  %s (%.2f bits/key) - The perfect hash function",
		golombRice.String(), float64(golombRice)*8/float64(keyCount))
	logger.Printf("    EF (bucket meta):   %s (%.2f bits/key) - Bucket metadata",
		ef.String(), float64(ef)*8/float64(keyCount))
	logger.Printf("    Offsets (values):   %s (%.2f bits/key) - Elias-Fano encoded ledgerSeqs",
		offsets.String(), float64(offsets)*8/float64(keyCount))
	logger.Printf("    Existence filter:   %s (%.2f bits/key) - False-positive detection",
		existence.String(), float64(existence)*8/float64(keyCount))
	logger.Printf("    Layer1:             %s (%.2f bits/key)",
		layer1.String(), float64(layer1)*8/float64(keyCount))
	logger.Printf("    -----------------------------------------")
	logger.Printf("    Total (from Sizes): %s", total.String())
	logger.Printf("    Total (file):       %s", helpers.FormatBytes(idxSize))
	logger.Println()

	logger.Println("  Component analysis:")
	logger.Printf("    Hash function:      %.2f bits/key (GolombRice)", float64(golombRice)*8/float64(keyCount))
	logger.Printf("    Value storage:      %.2f bits/key (Offsets/EliasFano)", float64(offsets)*8/float64(keyCount))
	logger.Printf("    Metadata:           %.2f bits/key (EF + Existence + Layer1)",
		float64(ef+existence+layer1)*8/float64(keyCount))
	logger.Println()

	// -------------------------------------------------------------------------
	// Step 6: Verification (with parallel processing)
	// -------------------------------------------------------------------------
	if verifyCount != 0 {
		printSeparator()
		logger.Println("STEP 6: Verification (PARALLEL)")
		printSeparator()
		logger.Println()

		stepStart = time.Now()

		// Use memory-mapped file for verification (much faster for random access)
		mmapReader, err := NewMmapReader(rawDataFile)
		if err != nil {
			logger.Printf("ERROR: Failed to create mmap reader for verification: %v", err)
			logger.Println("       Falling back to sequential verification...")

			// Fallback to sequential verification (original code path)
			// This is slower but doesn't require mmap support
			verifySequential(idx, rawDataFile, keyCount, verifyCount)
		} else {
			defer mmapReader.Close()

			var verified, failed int64

			if verifyCount == -1 {
				// Verify all keys with parallel workers
				logger.Printf("  Verifying ALL %s keys with %d parallel workers...",
					helpers.FormatNumber(int64(keyCount)), verifyWorkers)
				logger.Println()

				// Create progress channel
				progressChan := make(chan int, verifyWorkers*2)
				done := make(chan bool)

				// Start progress reporter goroutine
				go func() {
					processed := 0
					lastPercentReported := -1
					startTime := time.Now()

					for count := range progressChan {
						processed += count
						currentPercent := (processed * 100) / keyCount

						if currentPercent > lastPercentReported {
							elapsed := time.Since(startTime)
							rate := float64(processed) / elapsed.Seconds()
							eta := time.Duration(float64(keyCount-processed)/rate) * time.Second

							logger.Printf("  [%3d%%] Verified %s keys | Rate: %.0f keys/sec | ETA: %s",
								currentPercent, helpers.FormatNumber(int64(processed)), rate, helpers.FormatDuration(eta))

							// Print memory stats every 10%
							if currentPercent%MemStatsInterval == 0 && currentPercent > 0 {
								printMemStats()
							}

							lastPercentReported = currentPercent
						}
					}
					done <- true
				}()

				verified, failed = parallelVerifyAll(idx, mmapReader, keyCount, verifyWorkers, progressChan)
				close(progressChan)
				<-done

			} else {
				// Verify random sample with parallel workers
				keysToVerify := verifyCount
				if keysToVerify > keyCount {
					keysToVerify = keyCount
				}

				logger.Printf("  Verifying %s randomly sampled keys with %d parallel workers...",
					helpers.FormatNumber(int64(keysToVerify)), verifyWorkers)
				logger.Println()

				verified, failed = parallelVerifySample(idx, mmapReader, keyCount, keysToVerify, verifyWorkers)
			}

			timing.Verification = time.Since(stepStart)

			logger.Println()
			logger.Printf("  Verified:             %s", helpers.FormatNumber(verified))
			logger.Printf("  Failed:               %s", helpers.FormatNumber(failed))
			if verified+failed > 0 {
				logger.Printf("  Success rate:         %.4f%%", float64(verified)*100/float64(verified+failed))
			}
			logger.Printf("  Verification time:    %s", helpers.FormatDuration(timing.Verification))
			logger.Printf("  Verification rate:    %.0f keys/sec", float64(verified+failed)/timing.Verification.Seconds())
			logger.Println()

			if failed > 0 {
				logger.Println("WARNING: Some verifications failed!")
				logger.Println("         The index may be corrupted or there's a bug.")
			}
		}
	} else {
		logger.Println()
		logger.Println("  Verification skipped (--verify-count 0)")
		logger.Println()
	}

	idx.Close()

	// -------------------------------------------------------------------------
	// Summary
	// -------------------------------------------------------------------------
	timing.TotalTime = time.Since(totalStart)

	printSeparator()
	logger.Println("BUILD COMPLETE")
	printSeparator()
	logger.Println()
	logger.Println("  Output:")
	logger.Printf("    Index file:         %s", outputFile)
	logger.Printf("    Index size:         %s (%s bytes)", helpers.FormatBytes(idxSize), helpers.FormatNumber(idxSize))
	logger.Println()
	logger.Println("  Statistics:")
	logger.Printf("    Key count:          %s", helpers.FormatNumber(int64(keyCount)))
	logger.Printf("    Total bits/key:     %.2f", float64(idxSize*8)/float64(keyCount))
	logger.Printf("    Hash function:      %.2f bits/key", float64(golombRice)*8/float64(keyCount))
	logger.Printf("    Value storage:      %.2f bits/key", float64(offsets)*8/float64(keyCount))
	logger.Println()

	// -------------------------------------------------------------------------
	// Size comparison and reduction metric
	// -------------------------------------------------------------------------
	logger.Println("  Size Comparison:")
	logger.Printf("    Original raw file:  %s (%s bytes)", helpers.FormatBytes(fileSize), helpers.FormatNumber(fileSize))
	logger.Printf("    Index file:         %s (%s bytes)", helpers.FormatBytes(idxSize), helpers.FormatNumber(idxSize))

	// Calculate reduction percentage
	if fileSize > 0 {
		reductionBytes := fileSize - idxSize
		reductionPercent := float64(reductionBytes) * 100.0 / float64(fileSize)
		if reductionBytes > 0 {
			logger.Printf("    Size reduction:     %s (%.2f%% smaller)", helpers.FormatBytes(reductionBytes), reductionPercent)
		} else {
			increaseBytes := -reductionBytes
			increasePercent := float64(increaseBytes) * 100.0 / float64(fileSize)
			logger.Printf("    Size increase:      %s (%.2f%% larger)", helpers.FormatBytes(increaseBytes), increasePercent)
		}
		logger.Printf("    Compression ratio:  %.2fx", float64(fileSize)/float64(idxSize))
	}
	logger.Println()

	logger.Println("  Timing:")
	logger.Printf("    File analysis:      %s", helpers.FormatDuration(timing.FileOpen))
	logger.Printf("    RecSplit init:      %s", helpers.FormatDuration(timing.RecSplitInit))
	logger.Printf("    Key reading:        %s", helpers.FormatDuration(timing.KeyReading))
	logger.Printf("    Index build:        %s", helpers.FormatDuration(timing.IndexBuild))
	if verifyCount != 0 {
		logger.Printf("    Verification:       %s", helpers.FormatDuration(timing.Verification))
	}
	logger.Printf("    -----------------------------------------")
	logger.Printf("    Total time:         %s", helpers.FormatDuration(timing.TotalTime))
	logger.Println()
	printMemStats()
	logger.Println()

	// Projections for scale
	printSeparator()
	logger.Println("PROJECTIONS")
	printSeparator()
	logger.Println()
	bitsPerKey := float64(idxSize*8) / float64(keyCount)
	logger.Println("  Based on current bits/key ratio:")
	logger.Println()
	logger.Printf("    1M keys:     %s", helpers.FormatBytes(int64(1_000_000*bitsPerKey/8)))
	logger.Printf("    10M keys:    %s", helpers.FormatBytes(int64(10_000_000*bitsPerKey/8)))
	logger.Printf("    100M keys:   %s", helpers.FormatBytes(int64(100_000_000*bitsPerKey/8)))
	logger.Printf("    1B keys:     %s", helpers.FormatBytes(int64(1_000_000_000*bitsPerKey/8)))
	logger.Printf("    1.5B keys:   %s", helpers.FormatBytes(int64(1_500_000_000*bitsPerKey/8)))
	logger.Printf("    2.5B keys:   %s", helpers.FormatBytes(int64(2_500_000_000*bitsPerKey/8)))
	logger.Println()
	logger.Println("  For 10 years of data (15B keys):")
	logger.Printf("    Estimated index size: %s", helpers.FormatBytes(int64(15_000_000_000*bitsPerKey/8)))
	logger.Println()
	printSeparator()

	// Note: tmp directory cleanup happens automatically via defer cleaner.Cleanup()
}

// =============================================================================
// Fallback sequential verification (used if mmap fails)
// =============================================================================

// verifySequential performs sequential verification without mmap
// This is used as a fallback when memory mapping is not available
func verifySequential(idx *recsplit.Index, rawDataFile string, keyCount, verifyCount int) {
	stepStart := time.Now()

	// Create index reader
	reader := recsplit.NewIndexReader(idx)
	defer reader.Close()

	// Open raw data file
	file, err := os.Open(rawDataFile)
	if err != nil {
		logger.Printf("ERROR: Failed to open raw data file %s for verification: %v", rawDataFile, err)
		return
	}
	defer file.Close()

	bufReader := bufio.NewReaderSize(file, 64*1024*1024) // 64MB buffer
	recordBuf := make([]byte, RecordSize)

	var keysToVerify int
	var verifyAll bool
	if verifyCount == -1 {
		keysToVerify = keyCount
		verifyAll = true
		logger.Printf("  Verifying ALL %s keys (sequential fallback)...", helpers.FormatNumber(int64(keyCount)))
	} else {
		keysToVerify = verifyCount
		if keysToVerify > keyCount {
			keysToVerify = keyCount
		}
		logger.Printf("  Verifying %s keys (sequential fallback)...", helpers.FormatNumber(int64(keysToVerify)))
	}
	logger.Println()

	verified := 0
	failed := 0
	lastPercentReported := -1

	if verifyAll {
		// Verify all keys sequentially
		for i := 0; i < keyCount; i++ {
			n, err := io.ReadFull(bufReader, recordBuf)
			if err != nil || n != RecordSize {
				logger.Printf("ERROR: Failed to read record %d for verification", i)
				break
			}

			txHash := recordBuf[:TxHashSize]
			expectedLedgerSeq := binary.BigEndian.Uint32(recordBuf[TxHashSize:])

			actualLedgerSeq, found := reader.Lookup(txHash)
			if !found {
				failed++
				if failed <= 5 {
					logger.Printf("  NOT FOUND at record %d: expected ledgerSeq=%d",
						i, expectedLedgerSeq)
				}
				continue
			}

			if uint32(actualLedgerSeq) == expectedLedgerSeq {
				verified++
			} else {
				failed++
				if failed <= 5 {
					logger.Printf("  MISMATCH at record %d: expected ledgerSeq=%d, got=%d",
						i, expectedLedgerSeq, actualLedgerSeq)
				}
			}

			// Report progress at each 1%
			currentPercent := ((i + 1) * 100) / keyCount
			if currentPercent > lastPercentReported {
				logger.Printf("  [%3d%%] Verified %s keys...", currentPercent, helpers.FormatNumber(int64(i+1)))
				lastPercentReported = currentPercent
			}
		}
	} else {
		// For sample verification without mmap, we need to load records first
		logger.Println("  Loading records for random sampling...")
		records := make([]struct {
			txHash    [TxHashSize]byte
			ledgerSeq uint32
		}, keyCount)

		for i := 0; i < keyCount; i++ {
			n, err := io.ReadFull(bufReader, recordBuf)
			if err != nil || n != RecordSize {
				logger.Printf("ERROR: Failed to read record %d", i)
				return
			}
			copy(records[i].txHash[:], recordBuf[:TxHashSize])
			records[i].ledgerSeq = binary.BigEndian.Uint32(recordBuf[TxHashSize:])
		}

		logger.Println("  Sampling and verifying...")

		// Random sampling
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		indices := rng.Perm(keyCount)[:keysToVerify]

		for j, i := range indices {
			actualLedgerSeq, found := reader.Lookup(records[i].txHash[:])
			if !found {
				failed++
				if failed <= 5 {
					logger.Printf("  NOT FOUND at record %d: expected ledgerSeq=%d",
						i, records[i].ledgerSeq)
				}
				continue
			}

			if uint32(actualLedgerSeq) == records[i].ledgerSeq {
				verified++
			} else {
				failed++
				if failed <= 5 {
					logger.Printf("  MISMATCH at record %d: expected ledgerSeq=%d, got=%d",
						i, records[i].ledgerSeq, actualLedgerSeq)
				}
			}

			// Progress for sample verification
			currentPercent := ((j + 1) * 100) / keysToVerify
			if currentPercent > lastPercentReported && currentPercent%10 == 0 {
				logger.Printf("  [%3d%%] Verified %s samples...", currentPercent, helpers.FormatNumber(int64(j+1)))
				lastPercentReported = currentPercent
			}
		}
	}

	verificationTime := time.Since(stepStart)

	logger.Println()
	logger.Printf("  Verified:             %s", helpers.FormatNumber(int64(verified)))
	logger.Printf("  Failed:               %s", helpers.FormatNumber(int64(failed)))
	if verified+failed > 0 {
		logger.Printf("  Success rate:         %.4f%%", float64(verified)*100/float64(verified+failed))
	}
	logger.Printf("  Verification time:    %s", helpers.FormatDuration(verificationTime))
	logger.Println()

	if failed > 0 {
		logger.Println("WARNING: Some verifications failed!")
		logger.Println("         The index may be corrupted or there's a bug.")
	}
}
