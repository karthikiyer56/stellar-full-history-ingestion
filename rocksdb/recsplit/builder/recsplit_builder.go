package main

// =============================================================================
// recsplit_builder.go
// =============================================================================
//
// PURPOSE:
//   Builds a RecSplit index from a raw binary data file that maps
//   txHash (32 bytes) -> ledgerSeq (4 bytes).
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
//     --output-dir /path/to/output \
//     --output-filename txhash.idx \
//     [--bucket-size 2000] \
//     [--leaf-size 8] \
//     [--less-false-positives] \
//     [--tmp-dir /path/to/tmp] \
//     [--verify-count 10000]
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
//   - This implementation assumes everything fits in memory (up to 64GB usable)
//
// VERIFY COUNT:
//   --verify-count controls how many keys to verify after building:
//   -  0 = skip verification entirely
//   - -1 = verify ALL keys (can take hours for billions of keys)
//   -  N = verify N randomly sampled keys (default: 10000)
//
// =============================================================================

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"time"

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

	// LedgerSeqSize is the size of a ledger sequence number
	LedgerSeqSize = 4

	// DefaultVerifyCount is the default number of keys to verify
	DefaultVerifyCount = 10000

	// ProgressInterval is how often to print progress during processing
	ProgressInterval = 1_000_000
)

// =============================================================================
// Timing structures
// =============================================================================

type BuildTiming struct {
	FileOpen     time.Duration
	KeyCounting  time.Duration
	RecSplitInit time.Duration
	KeyReading   time.Duration
	IndexBuild   time.Duration
	Verification time.Duration
	TotalTime    time.Duration
}

// =============================================================================
// Helper functions
// =============================================================================

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

func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("  Memory: Alloc=%s, Sys=%s, HeapInuse=%s, NumGC=%d\n",
		formatBytes(int64(m.Alloc)),
		formatBytes(int64(m.Sys)),
		formatBytes(int64(m.HeapInuse)),
		m.NumGC)
}

func printSeparator() {
	fmt.Println("================================================================================")
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
	)

	flag.StringVar(&rawDataFile, "raw-data-file", "", "Path to raw binary data file (required)")
	flag.StringVar(&outputDir, "output-dir", "", "Output directory for index file (required)")
	flag.StringVar(&outputFilename, "output-filename", "txhash.idx", "Name of the output index file")
	flag.StringVar(&tmpDir, "tmp-dir", "", "Temp directory for RecSplit (defaults to output-dir)")
	flag.IntVar(&bucketSize, "bucket-size", 0, "RecSplit bucket size (0 = use default)")
	flag.IntVar(&leafSize, "leaf-size", 0, "RecSplit leaf size (0 = use default)")
	flag.BoolVar(&lessFalsePositives, "less-false-positives", false, "Enable false-positive detection filter (~9 bits/key overhead)")
	flag.IntVar(&verifyCount, "verify-count", DefaultVerifyCount, "Number of keys to verify after build: 0=skip, -1=all, N=sample N keys")

	flag.Parse()

	// Validate required flags
	if rawDataFile == "" || outputDir == "" {
		fmt.Println("Usage: recsplit_builder --raw-data-file <path> --output-dir <path> [options]")
		fmt.Println()
		flag.PrintDefaults()
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  # Basic usage with defaults")
		fmt.Println("  recsplit_builder --raw-data-file txns.bin --output-dir ./index")
		fmt.Println()
		fmt.Println("  # With false-positive protection and full verification")
		fmt.Println("  recsplit_builder --raw-data-file txns.bin --output-dir ./index --less-false-positives --verify-count -1")
		fmt.Println()
		fmt.Println("  # Custom bucket/leaf size for experimentation")
		fmt.Println("  recsplit_builder --raw-data-file txns.bin --output-dir ./index --bucket-size 1000 --leaf-size 16")
		os.Exit(1)
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

	timing := BuildTiming{}
	totalStart := time.Now()

	// -------------------------------------------------------------------------
	// Print configuration
	// -------------------------------------------------------------------------
	printSeparator()
	fmt.Println("RecSplit Index Builder")
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
	fmt.Printf("  Verify count:         %d\n", verifyCount)
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

	// Create logger
	logger := log.New()

	// Calculate bucket count for information
	bucketCount := (keyCount + bucketSize - 1) / bucketSize

	fmt.Printf("  Bucket size:          %d\n", bucketSize)
	fmt.Printf("  Leaf size:            %d\n", leafSize)
	fmt.Printf("  Bucket count:         %s\n", formatNumber(int64(bucketCount)))
	fmt.Printf("  Less false positives: %v\n", lessFalsePositives)
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

	// Read buffer for one record
	recordBuf := make([]byte, RecordSize)
	keysAdded := 0
	lastProgressTime := time.Now()

	fmt.Printf("  Reading %s records...\n", formatNumber(int64(keyCount)))
	fmt.Println()

	for {
		n, err := io.ReadFull(file, recordBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("ERROR: Failed to read record %d: %v\n", keysAdded, err)
			os.Exit(1)
		}
		if n != RecordSize {
			fmt.Printf("ERROR: Short read at record %d: got %d bytes, expected %d\n", keysAdded, n, RecordSize)
			os.Exit(1)
		}

		// Extract txHash and ledgerSeq
		txHash := recordBuf[:TxHashSize]
		ledgerSeq := binary.BigEndian.Uint32(recordBuf[TxHashSize:])

		// Add to RecSplit
		if err := rs.AddKey(txHash, uint64(ledgerSeq)); err != nil {
			fmt.Printf("ERROR: Failed to add key %d: %v\n", keysAdded, err)
			os.Exit(1)
		}

		keysAdded++

		// Progress reporting
		if keysAdded%ProgressInterval == 0 {
			elapsed := time.Since(lastProgressTime)
			rate := float64(ProgressInterval) / elapsed.Seconds()
			fmt.Printf("  Added %s keys (%.0f keys/sec)...\n", formatNumber(int64(keysAdded)), rate)
			printMemStats()
			lastProgressTime = time.Now()
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

	// -------------------------------------------------------------------------
	// Step 4: Build the index
	// -------------------------------------------------------------------------
	printSeparator()
	fmt.Println("STEP 4: Building RecSplit index")
	printSeparator()
	fmt.Println()

	stepStart = time.Now()
	fmt.Println("  Building perfect hash function...")
	fmt.Println("  (This may take a while for large datasets)")
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

	// Get size breakdown
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
	// Step 6: Verification
	// -------------------------------------------------------------------------
	if verifyCount != 0 {
		printSeparator()
		fmt.Println("STEP 6: Verification")
		printSeparator()
		fmt.Println()

		stepStart = time.Now()

		// Create index reader
		reader := recsplit.NewIndexReader(idx)
		defer reader.Close()

		// Reopen raw data file
		file.Seek(0, 0)

		var keysToVerify int
		var verifyAll bool
		if verifyCount == -1 {
			keysToVerify = keyCount
			verifyAll = true
			fmt.Printf("  Verifying ALL %s keys...\n", formatNumber(int64(keyCount)))
		} else {
			keysToVerify = verifyCount
			if keysToVerify > keyCount {
				keysToVerify = keyCount
			}
			fmt.Printf("  Verifying %s randomly sampled keys...\n", formatNumber(int64(keysToVerify)))
		}
		fmt.Println()

		verified := 0
		failed := 0

		if verifyAll {
			// Verify all keys sequentially
			for i := 0; i < keyCount; i++ {
				n, err := io.ReadFull(file, recordBuf)
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

				if (i+1)%ProgressInterval == 0 {
					fmt.Printf("  Verified %s keys...\n", formatNumber(int64(i+1)))
				}
			}
		} else {
			// Verify randomly sampled keys
			// First, collect all records into memory for random access
			fmt.Println("  Loading records for random sampling...")
			records := make([]struct {
				txHash    [TxHashSize]byte
				ledgerSeq uint32
			}, keyCount)

			for i := 0; i < keyCount; i++ {
				n, err := io.ReadFull(file, recordBuf)
				if err != nil || n != RecordSize {
					fmt.Printf("ERROR: Failed to read record %d\n", i)
					os.Exit(1)
				}
				copy(records[i].txHash[:], recordBuf[:TxHashSize])
				records[i].ledgerSeq = binary.BigEndian.Uint32(recordBuf[TxHashSize:])
			}

			fmt.Println("  Sampling and verifying...")

			// Random sampling
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			indices := rng.Perm(keyCount)[:keysToVerify]

			for _, i := range indices {
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
			}
		}

		timing.Verification = time.Since(stepStart)

		fmt.Println()
		fmt.Printf("  Verified:             %s\n", formatNumber(int64(verified)))
		fmt.Printf("  Failed:               %s\n", formatNumber(int64(failed)))
		fmt.Printf("  Success rate:         %.4f%%\n", float64(verified)*100/float64(verified+failed))
		fmt.Printf("  Verification time:    %s\n", formatDuration(timing.Verification))
		fmt.Println()

		if failed > 0 {
			fmt.Println("WARNING: Some verifications failed!")
			fmt.Println("         The index may be corrupted or there's a bug.")
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
