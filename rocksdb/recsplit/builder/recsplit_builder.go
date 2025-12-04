package main

// =============================================================================
// recsplit_builder.go
// =============================================================================
//
// PURPOSE:
//   Builds a RecSplit Minimal Perfect Hash Function (MPHF) from an existing
//   RocksDB DB3 database that maps txHash -> ledgerSeq.
//
// WHAT IS RECSPLIT:
//   RecSplit is a state-of-the-art algorithm for constructing minimal perfect
//   hash functions. A perfect hash function maps N keys to N unique integers
//   [0, N-1] with no collisions. "Minimal" means there are no gaps.
//
//   RecSplit achieves ~1.8-2 bits per key for the hash function itself,
//   which is very close to the theoretical minimum of 1.44 bits/key.
//
// HOW THIS WORKS:
//   1. We iterate through all txHash->ledgerSeq pairs in RocksDB
//   2. For each txHash, we call rs.AddKey(txHash, ledgerSeq)
//      - The txHash is the key (32 bytes)
//      - The ledgerSeq is stored as the "offset" value
//   3. RecSplit builds a perfect hash function over the keys
//   4. The ledgerSeq values are stored in an Elias-Fano encoded structure
//   5. On lookup: hash(txHash) -> index -> ledgerSeq
//
// IMPORTANT: ENUMS VS OFFSETS
//   RecSplit has two modes controlled by the `Enums` flag:
//
//   Enums=false (default):
//     - Offsets can be unsorted/duplicated
//     - Each key stores its own offset value
//     - More flexible but larger index
//
//   Enums=true:
//     - Offsets MUST be monotonically increasing (sorted)
//     - Uses enumeration: hash(key) -> enum -> offset
//     - More compact for sorted data
//
//   For our use case, ledgerSeq values are NOT sorted (txHashes are sorted
//   lexicographically in RocksDB, but their ledgerSeqs are scattered).
//   Therefore we use Enums=false.
//
// EXPECTED SIZE:
//   For 1.5B keys (1 year of Stellar transactions):
//   - MPHF structure: ~375 MB (2 bits × 1.5B / 8)
//   - Elias-Fano offsets: ~750 MB - 1.5 GB (depends on value distribution)
//   - Total: ~1-2 GB
//
// LIBRARY:
//   We use github.com/ledgerwatch/erigon-lib/recsplit
//   (Note: The library has moved to github.com/erigontech/erigon/erigon-lib/recsplit
//   but the ledgerwatch import path still works for now)
//
// USAGE:
//   go build -o recsplit_builder .
//   ./recsplit_builder --db3 /path/to/db3 --output /path/to/output
//
// =============================================================================

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"time"

	// Erigon's recsplit library - the production implementation
	// Note: erigon-lib has been merged into the main erigon repo at erigontech
	// but the ledgerwatch import path is still published to pkg.go.dev
	//
	// For the latest code, see: github.com/erigontech/erigon/erigon-lib/recsplit
	"github.com/ledgerwatch/erigon-lib/recsplit"
	newLog "github.com/ledgerwatch/log/v3"

	// RocksDB Go bindings
	"github.com/linxGnu/grocksdb"

	// Your helpers package (adjust import path as needed)
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
)

// =============================================================================
// Configuration Constants
// =============================================================================

const (
	// DefaultBucketSize controls the trade-off between index size and build time.
	// Larger bucket = smaller index but slower construction and lookup.
	// Typical values: 100-2000
	// Erigon uses 2000 for their production indices.
	DefaultBucketSize = 2000

	// DefaultLeafSize is the maximum number of keys in a leaf bucket.
	// Smaller = faster lookup but larger index.
	// Typical values: 8-24
	DefaultLeafSize = 8

	// ProgressReportInterval is how often to print progress during iteration.
	ProgressReportInterval = 10_000_000

	// VerificationSampleSize is how many keys to verify after building.
	VerificationSampleSize = 10000
)

// =============================================================================
// Main Entry Point
// =============================================================================

func main() {
	// -------------------------------------------------------------------------
	// Parse command-line flags
	// -------------------------------------------------------------------------
	var (
		db3Path    string // Path to RocksDB DB3 (txHash -> ledgerSeq)
		outputDir  string // Output directory for the .idx file
		tmpDir     string // Temp directory for RecSplit (can use lots of space)
		bucketSize int    // RecSplit bucket size parameter
		leafSize   int    // RecSplit leaf size parameter
		noVerify   bool   // Skip verification step
	)

	flag.StringVar(&db3Path, "db3", "", "Path to RocksDB DB3 (txHash -> ledgerSeq)")
	flag.StringVar(&outputDir, "output", "", "Output directory for .idx file")
	flag.StringVar(&tmpDir, "tmp", "", "Temp directory for RecSplit (defaults to output dir)")
	flag.IntVar(&bucketSize, "bucket-size", DefaultBucketSize, "RecSplit bucket size (100-2000)")
	flag.IntVar(&leafSize, "leaf-size", DefaultLeafSize, "RecSplit leaf size (8-24)")
	flag.BoolVar(&noVerify, "no-verify", false, "Skip verification step")

	flag.Parse()

	// Validate required flags
	if db3Path == "" || outputDir == "" {
		fmt.Println("Usage: recsplit_builder --db3 <path> --output <path> [options]")
		fmt.Println()
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Default temp directory to output directory
	if tmpDir == "" {
		tmpDir = outputDir
	}

	// Create output directories
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		log.Fatalf("Failed to create temp directory: %v", err)
	}

	// -------------------------------------------------------------------------
	// Step 1: Count keys in RocksDB
	// -------------------------------------------------------------------------
	// We need an exact key count for RecSplit initialization.
	// RecSplit pre-allocates data structures based on this count.
	fmt.Println("================================================================================")
	fmt.Println("STEP 1: Counting keys in RocksDB")
	fmt.Println("================================================================================")
	fmt.Printf("DB3 path: %s\n", db3Path)
	fmt.Println()

	keyCount, err := countKeysInRocksDB(db3Path)
	if err != nil {
		log.Fatalf("Failed to count keys: %v", err)
	}

	fmt.Printf("Exact key count: %s\n", helpers.FormatNumber(int64(keyCount)))
	fmt.Println()

	// -------------------------------------------------------------------------
	// Step 2: Build RecSplit index
	// -------------------------------------------------------------------------
	fmt.Println("================================================================================")
	fmt.Println("STEP 2: Building RecSplit index")
	fmt.Println("================================================================================")
	fmt.Printf("Bucket size: %d\n", bucketSize)
	fmt.Printf("Leaf size:   %d\n", leafSize)
	fmt.Printf("Temp dir:    %s\n", tmpDir)
	fmt.Println()

	idxPath := filepath.Join(outputDir, "txhash.idx")
	fmt.Printf("Output file: %s\n", idxPath)
	fmt.Println()

	startBuild := time.Now()

	// Initialize RecSplit with configuration
	//
	// RecSplitArgs explanation:
	// - KeyCount: Number of keys (must be exact)
	// - BucketSize: Keys per bucket during construction (affects size/speed)
	// - IndexFile: Output file path
	// - TmpDir: Temp directory for spilling to disk
	// - LeafSize: Max keys in a leaf (affects lookup speed)
	// - Enums: If true, offsets must be sorted (we use false)
	// - Salt: Random seed for hash function (for security against DOS)
	// - StartSeed: Seeds for each recursion level
	//
	logger := newLog.New()
	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   keyCount,
		BucketSize: bucketSize,
		IndexFile:  idxPath,
		TmpDir:     tmpDir,
		LeafSize:   uint16(leafSize),
		Enums:      false, // IMPORTANT: false because ledgerSeqs are NOT sorted
		// Salt and StartSeed left as defaults - RecSplit generates random values
	}, logger)
	if err != nil {
		log.Fatalf("Failed to create RecSplit: %v", err)
	}

	// Iterate through RocksDB and add each key-value pair
	//
	// IMPORTANT: We store ledgerSeq directly as the "offset" value.
	// RecSplit will compress these using Elias-Fano encoding.
	// Since ledgerSeqs cluster by time, this compresses well.
	fmt.Println("Adding keys from RocksDB...")
	fmt.Println()

	keysAdded := 0
	err = iterateRocksDB(db3Path, func(txHash []byte, ledgerSeq uint32) error {
		// Add key to RecSplit
		// - txHash: the 32-byte transaction hash (the key)
		// - ledgerSeq: the ledger sequence number (stored as offset)
		//
		// RecSplit internally:
		// 1. Hashes txHash to determine its bucket
		// 2. During Build(), creates a perfect hash function
		// 3. Stores ledgerSeq in an Elias-Fano structure indexed by the hash
		if err := rs.AddKey(txHash, uint64(ledgerSeq)); err != nil {
			return fmt.Errorf("failed to add key %d: %v", keysAdded, err)
		}

		keysAdded++

		// Progress reporting
		if keysAdded%ProgressReportInterval == 0 {
			fmt.Printf("  Added %s keys...\n", helpers.FormatNumber(int64(keysAdded)))
			printMemoryStats()
		}

		return nil
	})
	if err != nil {
		log.Fatalf("Failed to iterate RocksDB: %v", err)
	}

	fmt.Printf("Added all %s keys\n", helpers.FormatNumber(int64(keysAdded)))
	printMemoryStats()
	fmt.Println()

	// Build the perfect hash function
	//
	// This is the expensive step. RecSplit:
	// 1. Sorts keys into buckets
	// 2. For each bucket, recursively splits until leaves are small
	// 3. Finds hash seeds that create perfect bijection at each level
	// 4. Encodes the seeds using Golomb-Rice coding
	// 5. Encodes offsets using Elias-Fano
	fmt.Println("Building perfect hash function (this may take a while)...")

	ctx := context.Background()
	if err := rs.Build(ctx); err != nil {
		log.Fatalf("Failed to build RecSplit: %v", err)
	}

	// Check for hash collisions during key->uint64 mapping
	//
	// RecSplit internally hashes []byte keys to uint64 for processing.
	// With 1.5B keys, collision probability is low but not zero.
	// If collision detected, try different salt.
	if rs.Collision() {
		log.Fatalf("Collision detected! Keys hash to same uint64. Try different salt.")
	}

	buildDuration := time.Since(startBuild)

	// Get index file statistics
	idxInfo, err := os.Stat(idxPath)
	if err != nil {
		log.Fatalf("Failed to stat index file: %v", err)
	}

	bitsPerKey := float64(idxInfo.Size()*8) / float64(keyCount)

	fmt.Println()
	fmt.Printf("RecSplit build completed!\n")
	fmt.Printf("  Build time:    %s\n", helpers.FormatDuration(buildDuration))
	fmt.Printf("  Index size:    %s\n", helpers.FormatBytes(idxInfo.Size()))
	fmt.Printf("  Bits per key:  %.2f\n", bitsPerKey)
	fmt.Println()

	// -------------------------------------------------------------------------
	// Step 3: Verification
	// -------------------------------------------------------------------------
	if !noVerify {
		fmt.Println("================================================================================")
		fmt.Println("STEP 3: Verification")
		fmt.Println("================================================================================")

		verified, failed := verifyRecSplitIndex(idxPath, db3Path, VerificationSampleSize)

		fmt.Printf("Verified: %d / %d\n", verified, verified+failed)
		if failed > 0 {
			log.Printf("WARNING: %d verifications failed!", failed)
		}
		fmt.Println()
	}

	// -------------------------------------------------------------------------
	// Summary
	// -------------------------------------------------------------------------
	fmt.Println("================================================================================")
	fmt.Println("BUILD COMPLETE")
	fmt.Println("================================================================================")
	fmt.Printf("Index file:   %s\n", idxPath)
	fmt.Printf("Index size:   %s\n", helpers.FormatBytes(idxInfo.Size()))
	fmt.Printf("Key count:    %s\n", helpers.FormatNumber(int64(keyCount)))
	fmt.Printf("Bits per key: %.2f\n", bitsPerKey)
	fmt.Printf("Build time:   %s\n", helpers.FormatDuration(buildDuration))
	fmt.Println()
	fmt.Println("Use recsplit_lookup to query transactions by hash.")
	fmt.Println("================================================================================")
}

// =============================================================================
// RocksDB Functions
// =============================================================================

// countKeysInRocksDB counts the exact number of keys in the database.
// We need this for RecSplit initialization.
func countKeysInRocksDB(dbPath string) (int, error) {
	// Open RocksDB in read-only mode
	opts := grocksdb.NewDefaultOptions()
	defer opts.Destroy()

	db, err := grocksdb.OpenDbForReadOnly(opts, dbPath, false)
	if err != nil {
		return 0, fmt.Errorf("failed to open RocksDB: %v", err)
	}
	defer db.Close()

	// First, get the estimate (fast but approximate)
	estKeys := db.GetProperty("rocksdb.estimate-num-keys")
	var estCount int64
	fmt.Sscanf(estKeys, "%d", &estCount)
	fmt.Printf("Estimated keys (from RocksDB property): %s\n", helpers.FormatNumber(estCount))

	// Now count exactly by iterating
	// This is slower but necessary for RecSplit
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := db.NewIterator(ro)
	defer iter.Close()

	count := 0
	lastReport := time.Now()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++

		// Progress reporting every 5 seconds
		if time.Since(lastReport) > 5*time.Second {
			fmt.Printf("  Counting: %s keys...\n", helpers.FormatNumber(int64(count)))
			lastReport = time.Now()
		}
	}

	if err := iter.Err(); err != nil {
		return 0, fmt.Errorf("iterator error: %v", err)
	}

	return count, nil
}

// iterateRocksDB iterates through all key-value pairs and calls the callback.
// Keys are 32-byte txHashes, values are 4-byte big-endian ledgerSeqs.
func iterateRocksDB(dbPath string, callback func(txHash []byte, ledgerSeq uint32) error) error {
	opts := grocksdb.NewDefaultOptions()
	defer opts.Destroy()

	db, err := grocksdb.OpenDbForReadOnly(opts, dbPath, false)
	if err != nil {
		return fmt.Errorf("failed to open RocksDB: %v", err)
	}
	defer db.Close()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := db.NewIterator(ro)
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keySlice := iter.Key()
		valueSlice := iter.Value()

		// Copy key (32-byte txHash)
		// We must copy because the slice is only valid until next iteration
		txHash := make([]byte, len(keySlice.Data()))
		copy(txHash, keySlice.Data())

		// Parse value (4-byte big-endian ledger sequence)
		ledgerSeq := binary.BigEndian.Uint32(valueSlice.Data())

		// Free the slices (required by grocksdb)
		keySlice.Free()
		valueSlice.Free()

		// Call the callback
		if err := callback(txHash, ledgerSeq); err != nil {
			return err
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterator error: %v", err)
	}

	return nil
}

// =============================================================================
// Verification
// =============================================================================

// verifyRecSplitIndex verifies a sample of lookups against the original RocksDB.
// Returns (verified count, failed count).
func verifyRecSplitIndex(idxPath, db3Path string, sampleSize int) (int, int) {
	// Open RecSplit index
	idx, err := recsplit.OpenIndex(idxPath)
	if err != nil {
		log.Printf("Failed to open RecSplit index: %v", err)
		return 0, 0
	}
	defer idx.Close()

	// Create reader for thread-safe lookups
	reader := recsplit.NewIndexReader(idx)
	defer reader.Close()

	// Open RocksDB
	opts := grocksdb.NewDefaultOptions()
	defer opts.Destroy()

	db, err := grocksdb.OpenDbForReadOnly(opts, db3Path, false)
	if err != nil {
		log.Printf("Failed to open RocksDB: %v", err)
		return 0, 0
	}
	defer db.Close()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := db.NewIterator(ro)
	defer iter.Close()

	verified := 0
	failed := 0
	checked := 0

	for iter.SeekToFirst(); iter.Valid() && checked < sampleSize; iter.Next() {
		keySlice := iter.Key()
		valueSlice := iter.Value()

		// Get expected ledgerSeq from RocksDB
		expectedLedgerSeq := binary.BigEndian.Uint32(valueSlice.Data())

		// Copy key
		txHash := make([]byte, len(keySlice.Data()))
		copy(txHash, keySlice.Data())

		keySlice.Free()
		valueSlice.Free()

		// Lookup via RecSplit
		// Lookup() returns the offset we stored, which is the ledgerSeq
		actualLedgerSeq := reader.Lookup(txHash)

		if uint32(actualLedgerSeq) == expectedLedgerSeq {
			verified++
		} else {
			failed++
			if failed <= 5 {
				fmt.Printf("  MISMATCH at %d: expected ledgerSeq=%d, got=%d\n",
					checked, expectedLedgerSeq, actualLedgerSeq)
			}
		}

		checked++
	}

	return verified, failed
}

// =============================================================================
// Utility Functions
// =============================================================================

// printMemoryStats prints current memory usage for monitoring.
func printMemoryStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("  Memory: Alloc=%s, Sys=%s, HeapInuse=%s\n",
		helpers.FormatBytes(int64(m.Alloc)),
		helpers.FormatBytes(int64(m.Sys)),
		helpers.FormatBytes(int64(m.HeapInuse)))
}
