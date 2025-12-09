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
//   ./recsplit_builder_optimized \
//     --raw-data-file /path/to/transactions.bin \
//     --output-dir /path/to/output \
//     --output-filename txhash.idx \
//     [--bucket-size 2000] \
//     [--leaf-size 8] \
//     [--less-false-positives] \
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
// =============================================================================

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/log/v3"
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

// =============================================================================
// Helper functions
// =============================================================================

// formatDuration formats a duration in a human-readable way
func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%.2fµs", float64(d.Microseconds()))
	} else if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Milliseconds()))
	} else if d < time.Minute {
		return fmt.Sprintf("%.2fs", d.Seconds())
	} else {
		return fmt.Sprintf("%.2fm", d.Minutes())
	}
}

// formatBytes formats a byte count in a human-readable way
func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}

// formatNumber formats a number with thousands separators
func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1_000_000 {
		return fmt.Sprintf("%d,%03d", n/1000, n%1000)
	}
	if n < 1_000_000_000 {
		return fmt.Sprintf("%d,%03d,%03d", n/1_000_000, (n/1000)%1000, n%1000)
	}
	return fmt.Sprintf("%d,%03d,%03d,%03d", n/1_000_000_000, (n/1_000_000)%1000, (n/1000)%1000, n%1000)
}

// printMemStats prints current memory statistics
// This function calls runtime.ReadMemStats which causes a brief stop-the-world pause
// Use sparingly (every 10% progress) to minimize impact on performance
func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("  Memory: Alloc=%s, Sys=%s, HeapInuse=%s, NumGC=%d\n",
		formatBytes(int64(m.Alloc)),
		formatBytes(int64(m.Sys)),
		formatBytes(int64(m.HeapInuse)),
		m.NumGC)
}

// printSeparator prints a visual separator line
func printSeparator() {
	fmt.Println("================================================================================")
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
		fmt.Printf("  Warning: madvise(MADV_RANDOM) failed: %v (continuing anyway)\n", err)
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
	// -------------------------------------------------------------------------
	// Parse command-line flags
	// -------------------------------------------------------------------------
	var (
		rawDataFile        string
		outputDir          string
		outputFilename     string
		tmpDir             string
		bucketSize         int
		leafSize           int
		lessFalsePositives bool
		verifyCount        int
		dataVersion        int
		readBufferMB       int
		verifyWorkers      int
	)

	flag.StringVar(&rawDataFile, "raw-data-file", "", "Path to raw binary data file (required)")
	flag.StringVar(&outputDir, "output-dir", "", "Output directory for index file (required)")
	flag.StringVar(&outputFilename, "output-filename", "txhash.idx", "Name of the output index file")
	flag.StringVar(&tmpDir, "tmp-dir", "", "Temp directory for RecSplit (defaults to output-dir)")
	flag.IntVar(&bucketSize, "bucket-size", 0, "RecSplit bucket size (0 = use default)")
	flag.IntVar(&leafSize, "leaf-size", 0, "RecSplit leaf size (0 = use default)")
	flag.BoolVar(&lessFalsePositives, "less-false-positives", false, "Enable false-positive detection filter (~9 bits/key overhead)")
	flag.IntVar(&verifyCount, "verify-count", DefaultVerifyCount, "Number of keys to verify after build: 0=skip, -1=all, N=sample N keys")
	flag.IntVar(&dataVersion, "version", 0, "Data structure version: 0=legacy, 1=with fuse filter support")
	flag.IntVar(&readBufferMB, "read-buffer-mb", DefaultReadBufferMB, "Read buffer size in MB for key reading phase (larger = fewer syscalls)")
	flag.IntVar(&verifyWorkers, "verify-workers", DefaultVerifyWorkers, "Number of parallel workers for verification")

	flag.Parse()

	// Validate required flags
	if rawDataFile == "" || outputDir == "" {
		fmt.Println("Usage: recsplit_builder_optimized --raw-data-file <path> --output-dir <path> [options]")
		fmt.Println()
		flag.PrintDefaults()
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  # Basic usage with defaults (256MB buffer)")
		fmt.Println("  recsplit_builder_optimized --raw-data-file txns.bin --output-dir ./index")
		fmt.Println()
		fmt.Println("  # Maximum performance for large datasets (2B keys)")
		fmt.Println("  recsplit_builder_optimized --raw-data-file txns.bin --output-dir ./index \\")
		fmt.Println("    --read-buffer-mb 1024 --verify-workers 64 --verify-count -1")
		fmt.Println()
		fmt.Println("  # With false-positive protection")
		fmt.Println("  recsplit_builder_optimized --raw-data-file txns.bin --output-dir ./index \\")
		fmt.Println("    --less-false-positives --version 1")
		os.Exit(1)
	}

	// Set verify workers to number of CPUs if not specified or too high
	if verifyWorkers <= 0 {
		verifyWorkers = runtime.NumCPU()
	}

	// Default tmp directory to output directory
	if tmpDir == "" {
		tmpDir = outputDir
	}

	// Create directories if needed
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("ERROR: Failed to create output directory: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		fmt.Printf("ERROR: Failed to create temp directory: %v\n", err)
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
	fmt.Println("RecSplit Index Builder (OPTIMIZED)")
	printSeparator()
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Printf("  Raw data file:        %s\n", rawDataFile)
	fmt.Printf("  Output directory:     %s\n", outputDir)
	fmt.Printf("  Output filename:      %s\n", outputFilename)
	fmt.Printf("  Temp directory:       %s\n", tmpDir)
	fmt.Printf("  Bucket size:          %d\n", bucketSize)
	fmt.Printf("  Leaf size:            %d\n", leafSize)
	fmt.Printf("  Less false positives: %v\n", lessFalsePositives)
	fmt.Printf("  Data version:         %d\n", dataVersion)
	fmt.Printf("  Verify count:         %d\n", verifyCount)
	fmt.Println()
	fmt.Println("Optimization settings:")
	fmt.Printf("  Read buffer size:     %s (for key reading phase)\n", formatBytes(int64(readBufferSize)))
	fmt.Printf("  Verify workers:       %d (for parallel verification)\n", verifyWorkers)
	fmt.Printf("  Verification uses:    memory-mapped I/O (mmap)\n")
	fmt.Printf("  CPU cores available:  %d\n", runtime.NumCPU())
	fmt.Printf("  GOMAXPROCS:           %d\n", runtime.GOMAXPROCS(0))
	fmt.Println()
	printMemStats()
	fmt.Println()

	// -------------------------------------------------------------------------
	// Step 1: Open file and calculate key count
	// -------------------------------------------------------------------------
	printSeparator()
	fmt.Println("STEP 1: Analyzing raw data file")
	printSeparator()
	fmt.Println()

	stepStart := time.Now()

	fileInfo, err := os.Stat(rawDataFile)
	if err != nil {
		fmt.Printf("ERROR: Failed to stat raw data file: %v\n", err)
		os.Exit(1)
	}

	fileSize := fileInfo.Size()
	if fileSize%RecordSize != 0 {
		fmt.Printf("ERROR: File size (%d) is not a multiple of record size (%d)\n", fileSize, RecordSize)
		fmt.Printf("       This suggests the file is corrupted or not in the expected format.\n")
		os.Exit(1)
	}

	keyCount := int(fileSize / RecordSize)
	timing.FileOpen = time.Since(stepStart)

	fmt.Printf("  File size:            %s\n", formatBytes(fileSize))
	fmt.Printf("  Record size:          %d bytes\n", RecordSize)
	fmt.Printf("  Key count:            %s\n", formatNumber(int64(keyCount)))
	fmt.Printf("  Time:                 %s\n", formatDuration(timing.FileOpen))
	fmt.Println()

	if keyCount == 0 {
		fmt.Println("ERROR: No records found in file")
		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// Step 2: Initialize RecSplit
	// -------------------------------------------------------------------------
	printSeparator()
	fmt.Println("STEP 2: Initializing RecSplit")
	printSeparator()
	fmt.Println()

	stepStart = time.Now()

	idxPath := filepath.Join(outputDir, outputFilename)
	fmt.Printf("  Index file:           %s\n", idxPath)

	// Remove existing index if present
	os.Remove(idxPath)

	// Create logger (using erigon's logger)
	logger := log.New()

	// Calculate bucket count for information
	bucketCount := (keyCount + bucketSize - 1) / bucketSize

	fmt.Printf("  Bucket size:          %d\n", bucketSize)
	fmt.Printf("  Leaf size:            %d\n", leafSize)
	fmt.Printf("  Bucket count:         %s\n", formatNumber(int64(bucketCount)))
	fmt.Printf("  Less false positives: %v\n", lessFalsePositives)
	fmt.Printf("  Data version:         %d\n", dataVersion)
	fmt.Println()

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:           keyCount,
		Enums:              false, // We store arbitrary values (ledgerSeq), not sequential offsets
		LessFalsePositives: lessFalsePositives,
		BucketSize:         bucketSize,
		LeafSize:           uint16(leafSize),
		TmpDir:             tmpDir,
		IndexFile:          idxPath,
		BaseDataID:         0,
		Version:            uint8(dataVersion),
	}, logger)
	if err != nil {
		fmt.Printf("ERROR: Failed to create RecSplit: %v\n", err)
		os.Exit(1)
	}
	defer rs.Close()

	timing.RecSplitInit = time.Since(stepStart)
	fmt.Printf("  Initialization time:  %s\n", formatDuration(timing.RecSplitInit))
	fmt.Println()
	printMemStats()
	fmt.Println()

	// -------------------------------------------------------------------------
	// Step 3: Read raw data file and add keys
	// -------------------------------------------------------------------------
	printSeparator()
	fmt.Println("STEP 3: Reading keys and adding to RecSplit")
	printSeparator()
	fmt.Println()

	stepStart = time.Now()

	file, err := os.Open(rawDataFile)
	if err != nil {
		fmt.Printf("ERROR: Failed to open raw data file: %v\n", err)
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

	fmt.Printf("  Reading %s records with %s buffer...\n", formatNumber(int64(keyCount)), formatBytes(int64(readBufferSize)))
	fmt.Println()

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
			fmt.Printf("ERROR: Failed to read batch at record %d: %v\n", keysAdded, err)
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
				fmt.Printf("ERROR: Failed to add key %d: %v\n", keysAdded+i, err)
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

			fmt.Printf("  [%3d%%] Added %s keys | Rate: %.0f keys/sec | ETA: %s\n",
				currentPercent, formatNumber(int64(keysAdded)), rate, formatDuration(eta))

			// Print memory stats every 10% to avoid overhead
			if currentPercent%MemStatsInterval == 0 && currentPercent > 0 {
				printMemStats()
			}

			lastPercentReported = currentPercent
		}
	}

	timing.KeyReading = time.Since(stepStart)

	fmt.Println()
	fmt.Printf("  Total keys added:     %s\n", formatNumber(int64(keysAdded)))
	fmt.Printf("  Reading time:         %s\n", formatDuration(timing.KeyReading))
	fmt.Printf("  Average rate:         %.0f keys/sec\n", float64(keysAdded)/timing.KeyReading.Seconds())
	fmt.Println()
	printMemStats()
	fmt.Println()

	if keysAdded != keyCount {
		fmt.Printf("ERROR: Key count mismatch: expected %d, got %d\n", keyCount, keysAdded)
		os.Exit(1)
	}

	// Close the file as we're done reading for key addition
	file.Close()

	// -------------------------------------------------------------------------
	// Step 4: Build the index
	// -------------------------------------------------------------------------
	printSeparator()
	fmt.Println("STEP 4: Building RecSplit index")
	printSeparator()
	fmt.Println()

	stepStart = time.Now()
	fmt.Println("  Building perfect hash function...")
	fmt.Println("  (This is CPU-intensive and may take a while for large datasets)")
	fmt.Println()

	// Force garbage collection before the memory-intensive build phase
	// This gives us maximum available heap for the build process
	runtime.GC()
	fmt.Println("  Pre-build GC completed")
	printMemStats()
	fmt.Println()

	ctx := context.Background()
	if err := rs.Build(ctx); err != nil {
		if err == recsplit.ErrCollision {
			fmt.Println("ERROR: Hash collision detected during build")
			fmt.Println("       This is rare. Try rebuilding - a different salt will be used.")
		} else {
			fmt.Printf("ERROR: Failed to build RecSplit: %v\n", err)
		}
		os.Exit(1)
	}

	timing.IndexBuild = time.Since(stepStart)

	fmt.Printf("  Build time:           %s\n", formatDuration(timing.IndexBuild))
	fmt.Println()
	printMemStats()
	fmt.Println()

	// -------------------------------------------------------------------------
	// Step 5: Analyze index size and structure
	// -------------------------------------------------------------------------
	printSeparator()
	fmt.Println("STEP 5: Index analysis")
	printSeparator()
	fmt.Println()

	// Get file size
	idxInfo, err := os.Stat(idxPath)
	if err != nil {
		fmt.Printf("ERROR: Failed to stat index file: %v\n", err)
		os.Exit(1)
	}
	idxSize := idxInfo.Size()

	fmt.Printf("  Index file:           %s\n", idxPath)
	fmt.Printf("  Index size:           %s\n", formatBytes(idxSize))
	fmt.Printf("  Total bits/key:       %.2f\n", float64(idxSize*8)/float64(keyCount))
	fmt.Println()

	// Open index to get detailed sizes
	idx, err := recsplit.OpenIndex(idxPath)
	if err != nil {
		fmt.Printf("ERROR: Failed to open index for analysis: %v\n", err)
		os.Exit(1)
	}

	// Get size breakdown from the index
	total, offsets, ef, golombRice, existence, layer1 := idx.Sizes()

	fmt.Println("  Size breakdown:")
	fmt.Printf("    GolombRice (hash):  %s (%.2f bits/key) - The perfect hash function\n",
		golombRice.String(), float64(golombRice)*8/float64(keyCount))
	fmt.Printf("    EF (bucket meta):   %s (%.2f bits/key) - Bucket metadata\n",
		ef.String(), float64(ef)*8/float64(keyCount))
	fmt.Printf("    Offsets (values):   %s (%.2f bits/key) - Elias-Fano encoded ledgerSeqs\n",
		offsets.String(), float64(offsets)*8/float64(keyCount))
	fmt.Printf("    Existence filter:   %s (%.2f bits/key) - False-positive detection\n",
		existence.String(), float64(existence)*8/float64(keyCount))
	fmt.Printf("    Layer1:             %s (%.2f bits/key)\n",
		layer1.String(), float64(layer1)*8/float64(keyCount))
	fmt.Printf("    -----------------------------------------\n")
	fmt.Printf("    Total (from Sizes): %s\n", total.String())
	fmt.Printf("    Total (file):       %s\n", formatBytes(idxSize))
	fmt.Println()

	fmt.Println("  Component analysis:")
	fmt.Printf("    Hash function:      %.2f bits/key (GolombRice)\n", float64(golombRice)*8/float64(keyCount))
	fmt.Printf("    Value storage:      %.2f bits/key (Offsets/EliasFano)\n", float64(offsets)*8/float64(keyCount))
	fmt.Printf("    Metadata:           %.2f bits/key (EF + Existence + Layer1)\n",
		float64(ef+existence+layer1)*8/float64(keyCount))
	fmt.Println()

	// -------------------------------------------------------------------------
	// Step 6: Verification (with parallel processing)
	// -------------------------------------------------------------------------
	if verifyCount != 0 {
		printSeparator()
		fmt.Println("STEP 6: Verification (PARALLEL)")
		printSeparator()
		fmt.Println()

		stepStart = time.Now()

		// Use memory-mapped file for verification (much faster for random access)
		mmapReader, err := NewMmapReader(rawDataFile)
		if err != nil {
			fmt.Printf("ERROR: Failed to create mmap reader for verification: %v\n", err)
			fmt.Println("       Falling back to sequential verification...")

			// Fallback to sequential verification (original code path)
			// This is slower but doesn't require mmap support
			verifySequential(idx, rawDataFile, keyCount, verifyCount)
		} else {
			defer mmapReader.Close()

			var verified, failed int64

			if verifyCount == -1 {
				// Verify all keys with parallel workers
				fmt.Printf("  Verifying ALL %s keys with %d parallel workers...\n",
					formatNumber(int64(keyCount)), verifyWorkers)
				fmt.Println()

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

							fmt.Printf("  [%3d%%] Verified %s keys | Rate: %.0f keys/sec | ETA: %s\n",
								currentPercent, formatNumber(int64(processed)), rate, formatDuration(eta))

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

				fmt.Printf("  Verifying %s randomly sampled keys with %d parallel workers...\n",
					formatNumber(int64(keysToVerify)), verifyWorkers)
				fmt.Println()

				verified, failed = parallelVerifySample(idx, mmapReader, keyCount, keysToVerify, verifyWorkers)
			}

			timing.Verification = time.Since(stepStart)

			fmt.Println()
			fmt.Printf("  Verified:             %s\n", formatNumber(verified))
			fmt.Printf("  Failed:               %s\n", formatNumber(failed))
			if verified+failed > 0 {
				fmt.Printf("  Success rate:         %.4f%%\n", float64(verified)*100/float64(verified+failed))
			}
			fmt.Printf("  Verification time:    %s\n", formatDuration(timing.Verification))
			fmt.Printf("  Verification rate:    %.0f keys/sec\n", float64(verified+failed)/timing.Verification.Seconds())
			fmt.Println()

			if failed > 0 {
				fmt.Println("WARNING: Some verifications failed!")
				fmt.Println("         The index may be corrupted or there's a bug.")
			}
		}
	} else {
		fmt.Println()
		fmt.Println("  Verification skipped (--verify-count 0)")
		fmt.Println()
	}

	idx.Close()

	// -------------------------------------------------------------------------
	// Summary
	// -------------------------------------------------------------------------
	timing.TotalTime = time.Since(totalStart)

	printSeparator()
	fmt.Println("BUILD COMPLETE")
	printSeparator()
	fmt.Println()
	fmt.Println("  Output:")
	fmt.Printf("    Index file:         %s\n", idxPath)
	fmt.Printf("    Index size:         %s\n", formatBytes(idxSize))
	fmt.Println()
	fmt.Println("  Statistics:")
	fmt.Printf("    Key count:          %s\n", formatNumber(int64(keyCount)))
	fmt.Printf("    Total bits/key:     %.2f\n", float64(idxSize*8)/float64(keyCount))
	fmt.Printf("    Hash function:      %.2f bits/key\n", float64(golombRice)*8/float64(keyCount))
	fmt.Printf("    Value storage:      %.2f bits/key\n", float64(offsets)*8/float64(keyCount))
	fmt.Println()
	fmt.Println("  Timing:")
	fmt.Printf("    File analysis:      %s\n", formatDuration(timing.FileOpen))
	fmt.Printf("    RecSplit init:      %s\n", formatDuration(timing.RecSplitInit))
	fmt.Printf("    Key reading:        %s\n", formatDuration(timing.KeyReading))
	fmt.Printf("    Index build:        %s\n", formatDuration(timing.IndexBuild))
	if verifyCount != 0 {
		fmt.Printf("    Verification:       %s\n", formatDuration(timing.Verification))
	}
	fmt.Printf("    -----------------------------------------\n")
	fmt.Printf("    Total time:         %s\n", formatDuration(timing.TotalTime))
	fmt.Println()
	printMemStats()
	fmt.Println()

	// Projections for scale
	printSeparator()
	fmt.Println("PROJECTIONS")
	printSeparator()
	fmt.Println()
	bitsPerKey := float64(idxSize*8) / float64(keyCount)
	fmt.Println("  Based on current bits/key ratio:")
	fmt.Println()
	fmt.Printf("    1M keys:     %s\n", formatBytes(int64(1_000_000*bitsPerKey/8)))
	fmt.Printf("    10M keys:    %s\n", formatBytes(int64(10_000_000*bitsPerKey/8)))
	fmt.Printf("    100M keys:   %s\n", formatBytes(int64(100_000_000*bitsPerKey/8)))
	fmt.Printf("    1B keys:     %s\n", formatBytes(int64(1_000_000_000*bitsPerKey/8)))
	fmt.Printf("    1.5B keys:   %s\n", formatBytes(int64(1_500_000_000*bitsPerKey/8)))
	fmt.Printf("    2.5B keys:   %s\n", formatBytes(int64(2_500_000_000*bitsPerKey/8)))
	fmt.Println()
	fmt.Println("  For 10 years of data (15B keys):")
	fmt.Printf("    Estimated index size: %s\n", formatBytes(int64(15_000_000_000*bitsPerKey/8)))
	fmt.Println()
	printSeparator()
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
		fmt.Printf("ERROR: Failed to open raw data file for verification: %v\n", err)
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
		fmt.Printf("  Verifying ALL %s keys (sequential fallback)...\n", formatNumber(int64(keyCount)))
	} else {
		keysToVerify = verifyCount
		if keysToVerify > keyCount {
			keysToVerify = keyCount
		}
		fmt.Printf("  Verifying %s keys (sequential fallback)...\n", formatNumber(int64(keysToVerify)))
	}
	fmt.Println()

	verified := 0
	failed := 0
	lastPercentReported := -1

	if verifyAll {
		// Verify all keys sequentially
		for i := 0; i < keyCount; i++ {
			n, err := io.ReadFull(bufReader, recordBuf)
			if err != nil || n != RecordSize {
				fmt.Printf("ERROR: Failed to read record %d for verification\n", i)
				break
			}

			txHash := recordBuf[:TxHashSize]
			expectedLedgerSeq := binary.BigEndian.Uint32(recordBuf[TxHashSize:])

			actualLedgerSeq, found := reader.Lookup(txHash)
			if !found {
				failed++
				if failed <= 5 {
					fmt.Printf("  NOT FOUND at record %d: expected ledgerSeq=%d\n",
						i, expectedLedgerSeq)
				}
				continue
			}

			if uint32(actualLedgerSeq) == expectedLedgerSeq {
				verified++
			} else {
				failed++
				if failed <= 5 {
					fmt.Printf("  MISMATCH at record %d: expected ledgerSeq=%d, got=%d\n",
						i, expectedLedgerSeq, actualLedgerSeq)
				}
			}

			// Report progress at each 1%
			currentPercent := ((i + 1) * 100) / keyCount
			if currentPercent > lastPercentReported {
				fmt.Printf("  [%3d%%] Verified %s keys...\n", currentPercent, formatNumber(int64(i+1)))
				lastPercentReported = currentPercent
			}
		}
	} else {
		// For sample verification without mmap, we need to load records first
		fmt.Println("  Loading records for random sampling...")
		records := make([]struct {
			txHash    [TxHashSize]byte
			ledgerSeq uint32
		}, keyCount)

		for i := 0; i < keyCount; i++ {
			n, err := io.ReadFull(bufReader, recordBuf)
			if err != nil || n != RecordSize {
				fmt.Printf("ERROR: Failed to read record %d\n", i)
				return
			}
			copy(records[i].txHash[:], recordBuf[:TxHashSize])
			records[i].ledgerSeq = binary.BigEndian.Uint32(recordBuf[TxHashSize:])
		}

		fmt.Println("  Sampling and verifying...")

		// Random sampling
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		indices := rng.Perm(keyCount)[:keysToVerify]

		for j, i := range indices {
			actualLedgerSeq, found := reader.Lookup(records[i].txHash[:])
			if !found {
				failed++
				if failed <= 5 {
					fmt.Printf("  NOT FOUND at record %d: expected ledgerSeq=%d\n",
						i, records[i].ledgerSeq)
				}
				continue
			}

			if uint32(actualLedgerSeq) == records[i].ledgerSeq {
				verified++
			} else {
				failed++
				if failed <= 5 {
					fmt.Printf("  MISMATCH at record %d: expected ledgerSeq=%d, got=%d\n",
						i, records[i].ledgerSeq, actualLedgerSeq)
				}
			}

			// Progress for sample verification
			currentPercent := ((j + 1) * 100) / keysToVerify
			if currentPercent > lastPercentReported && currentPercent%10 == 0 {
				fmt.Printf("  [%3d%%] Verified %s samples...\n", currentPercent, formatNumber(int64(j+1)))
				lastPercentReported = currentPercent
			}
		}
	}

	verificationTime := time.Since(stepStart)

	fmt.Println()
	fmt.Printf("  Verified:             %s\n", formatNumber(int64(verified)))
	fmt.Printf("  Failed:               %s\n", formatNumber(int64(failed)))
	if verified+failed > 0 {
		fmt.Printf("  Success rate:         %.4f%%\n", float64(verified)*100/float64(verified+failed))
	}
	fmt.Printf("  Verification time:    %s\n", formatDuration(verificationTime))
	fmt.Println()

	if failed > 0 {
		fmt.Println("WARNING: Some verifications failed!")
		fmt.Println("         The index may be corrupted or there's a bug.")
	}
}
