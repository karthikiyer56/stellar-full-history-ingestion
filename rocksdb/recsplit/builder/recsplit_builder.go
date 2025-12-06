package main

// Filename: recsplit_builder.go
//
// Builds a RecSplit Minimal Perfect Hash Function from an existing RocksDB DB3
// (txHash -> ledgerSeq) using Erigon's production-ready RecSplit library.
//
// RecSplit advantages:
//   - Native []byte key support (no truncation to uint64!)
//   - ~1.8-2 bits per key (vs BBHash's 3-4 bits)
//   - Production-tested in Ethereum's Erigon client
//   - Handles large datasets that don't fit in RAM (spills to disk)
//
// Output: A single .idx file containing the perfect hash function
// Values are stored in a separate .values file (simple binary array)
//
// For 1.5B keys:
//   - RecSplit index: ~375 MB (1.5B × 2 bits)
//   - Values array: ~6 GB (1.5B × 4 bytes)
//   - Total: ~6.4 GB
//
// Usage:
//   go run recsplit_builder.go \
//     --db3 /path/to/rocksdb/db3 \
//     --output /path/to/output/dir \
//     --bucket-size 2000
//
// Example:
//   ./recsplit_builder --db3 /data/db3/2024 --output /data/recsplit/2024

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	newLog "github.com/ledgerwatch/log/v3"
	"github.com/linxGnu/grocksdb"
)

func main() {
	// =========================================================================
	// Parse flags
	// =========================================================================
	var db3Path, outputDir, tmpDir string
	var bucketSize int

	flag.StringVar(&db3Path, "db3", "", "Path to RocksDB DB3 (txHash -> ledgerSeq)")
	flag.StringVar(&outputDir, "output", "", "Output directory for .idx and .values files")
	flag.StringVar(&tmpDir, "tmp", "", "Temporary directory for RecSplit (defaults to output dir)")
	flag.IntVar(&bucketSize, "bucket-size", 128, "RecSplit bucket size (100-2000, larger = smaller index but slower)")

	flag.Parse()

	if db3Path == "" || outputDir == "" {
		log.Fatalf("Both --db3 and --output are required")
	}

	if tmpDir == "" {
		tmpDir = outputDir
	}

	// Create directories
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		log.Fatalf("Failed to create tmp directory: %v", err)
	}

	// =========================================================================
	// Step 1: Count keys in RocksDB
	// =========================================================================
	fmt.Println("================================================================================")
	fmt.Println("Step 1: Counting keys in RocksDB")
	fmt.Println("================================================================================")

	keyCount, err := countKeys(db3Path)
	if err != nil {
		log.Fatalf("Failed to count keys: %v", err)
	}
	fmt.Printf("Key count: %s\n", helpers.FormatNumber(int64(keyCount)))
	fmt.Println()

	// =========================================================================
	// Step 2: Build RecSplit index and collect values
	// =========================================================================
	fmt.Println("================================================================================")
	fmt.Println("Step 2: Building RecSplit index")
	fmt.Println("================================================================================")
	fmt.Printf("Bucket size: %d\n", bucketSize)
	fmt.Printf("Index file: %s\n", filepath.Join(outputDir, "txhash.idx"))

	startBuild := time.Now()

	// Create RecSplit
	logger := newLog.New()
	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   keyCount,
		BucketSize: bucketSize,
		IndexFile:  filepath.Join(outputDir, "txhash.idx"),
		TmpDir:     tmpDir,
		LeafSize:   8,
		Enums:      false,
	}, logger)
	if err != nil {
		log.Fatalf("Failed to create RecSplit: %v", err)
	}

	values := make([]uint32, keyCount)

	offsets := make([]uint64, keyCount)
	for i := 0; i < keyCount; i++ {
		offsets[i] = uint64(i)
	}

	// Shuffle offsets in a deterministic pseudo-random way
	randSrc := rand.NewSource(42) // fixed seed for reproducibility
	r := rand.New(randSrc)
	for i := len(offsets) - 1; i > 0; i-- {
		j := r.Intn(i + 1)
		offsets[i], offsets[j] = offsets[j], offsets[i]
	}

	err = iterateRocksDB(db3Path, func(txHash []byte, ledgerSeq uint32, index int) error {
		shuffledIndex := offsets[index] // shuffled offset
		values[shuffledIndex] = ledgerSeq
		// Hash the key for proper entropy and bucket uniformity
		if err := rs.AddKey(txHash, shuffledIndex); err != nil {
			return fmt.Errorf("failed to add key %d: %v", index, err)
		}

		// Collect value
		//values = append(values, ledgerSeq)

		if index > 0 && index%10_000_000 == 0 {
			fmt.Printf("  Added %s keys...\n", helpers.FormatNumber(int64(index)))
			printMemStats()
		}

		return nil
	})
	if err != nil {
		log.Fatalf("Failed to iterate RocksDB: %v", err)
	}

	fmt.Printf("Added all %s keys\n", helpers.FormatNumber(int64(len(values))))
	printMemStats()

	// Build the index
	fmt.Println("Building perfect hash function...")
	ctx := context.Background()
	if err := rs.Build(ctx); err != nil {
		log.Fatalf("Failed to build RecSplit: %v", err)
	}

	// Check for collisions
	if rs.Collision() {
		log.Fatalf("Collision detected! Try increasing bucket size or using different salt")
	}

	buildDuration := time.Since(startBuild)
	fmt.Printf("RecSplit build completed in %s\n", helpers.FormatDuration(buildDuration))

	// Get index file size
	idxInfo, _ := os.Stat(filepath.Join(outputDir, "txhash.idx"))
	fmt.Printf("Index file size: %s\n", helpers.FormatBytes(idxInfo.Size()))
	fmt.Printf("Bits per key: %.2f\n", float64(idxInfo.Size()*8)/float64(keyCount))
	fmt.Println()

	// =========================================================================
	// Step 3: Write values array
	// =========================================================================
	fmt.Println("================================================================================")
	fmt.Println("Step 3: Writing values array")
	fmt.Println("================================================================================")

	valuesPath := filepath.Join(outputDir, "values.bin")
	startWriteValues := time.Now()

	valuesFile, err := os.Create(valuesPath)
	if err != nil {
		log.Fatalf("Failed to create values file: %v", err)
	}

	// Write header: number of entries (8 bytes)
	header := make([]byte, 8)
	binary.LittleEndian.PutUint64(header, uint64(len(values)))
	if _, err := valuesFile.Write(header); err != nil {
		valuesFile.Close()
		log.Fatalf("Failed to write header: %v", err)
	}

	// Write values as binary (4 bytes each, little-endian)
	buf := make([]byte, 4*1024*1024) // 4 MB buffer
	bufIdx := 0

	for _, v := range values {
		binary.LittleEndian.PutUint32(buf[bufIdx:], v)
		bufIdx += 4

		if bufIdx >= len(buf) {
			if _, err := valuesFile.Write(buf); err != nil {
				valuesFile.Close()
				log.Fatalf("Failed to write values: %v", err)
			}
			bufIdx = 0
		}
	}

	// Write remaining
	if bufIdx > 0 {
		if _, err := valuesFile.Write(buf[:bufIdx]); err != nil {
			valuesFile.Close()
			log.Fatalf("Failed to write remaining values: %v", err)
		}
	}
	valuesFile.Close()

	valuesInfo, _ := os.Stat(valuesPath)
	writeValuesDuration := time.Since(startWriteValues)

	fmt.Printf("Wrote values to %s\n", valuesPath)
	fmt.Printf("Values file size: %s\n", helpers.FormatBytes(valuesInfo.Size()))
	fmt.Printf("Write time: %s\n", helpers.FormatDuration(writeValuesDuration))
	fmt.Println()

	// Free values memory
	values = nil
	runtime.GC()

	// =========================================================================
	// Step 4: Verification
	// =========================================================================
	fmt.Println("================================================================================")
	fmt.Println("Step 4: Verification")
	fmt.Println("================================================================================")

	verified, failed := verifyRecSplit(outputDir, db3Path, 10000)
	fmt.Printf("Verified: %d, Failed: %d\n", verified, failed)

	if failed > 0 {
		log.Printf("WARNING: %d verifications failed!", failed)
	}
	fmt.Println()

	// =========================================================================
	// Summary
	// =========================================================================
	fmt.Println("================================================================================")
	fmt.Println("BUILD COMPLETE")
	fmt.Println("================================================================================")
	fmt.Printf("Index file:    %s (%s)\n", filepath.Join(outputDir, "txhash.idx"), helpers.FormatBytes(idxInfo.Size()))
	fmt.Printf("Values file:   %s (%s)\n", valuesPath, helpers.FormatBytes(valuesInfo.Size()))
	fmt.Printf("Total size:    %s\n", helpers.FormatBytes(idxInfo.Size()+valuesInfo.Size()))
	fmt.Printf("Bits per key:  %.2f\n", float64(idxInfo.Size()*8)/float64(keyCount))
	fmt.Println()
	fmt.Printf("Build time:    %s\n", helpers.FormatDuration(buildDuration))
	fmt.Printf("Total time:    %s\n", helpers.FormatDuration(time.Since(startBuild)))
	fmt.Println()
	fmt.Println("You can now use recsplit_two_step_lookup.go for O(1) lookups!")
	fmt.Println("================================================================================")
}

// countKeys counts the number of keys in RocksDB
func countKeys(dbPath string) (int, error) {
	opts := grocksdb.NewDefaultOptions()
	defer opts.Destroy()

	db, err := grocksdb.OpenDbForReadOnly(opts, dbPath, false)
	if err != nil {
		return 0, fmt.Errorf("failed to open RocksDB: %v", err)
	}
	defer db.Close()

	// Try to get estimate first
	estKeys := db.GetProperty("rocksdb.estimate-num-keys")
	var estCount int64
	fmt.Sscanf(estKeys, "%d", &estCount)
	fmt.Printf("Estimated keys: %s\n", helpers.FormatNumber(estCount))

	// Count exactly
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := db.NewIterator(ro)
	defer iter.Close()

	count := 0
	lastReport := time.Now()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
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

// iterateRocksDB iterates through all key-value pairs and calls the callback
func iterateRocksDB(dbPath string, callback func(txHash []byte, ledgerSeq uint32, index int) error) error {
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

	index := 0

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keySlice := iter.Key()
		valueSlice := iter.Value()

		// Copy key (32-byte txHash)
		txHash := make([]byte, len(keySlice.Data()))
		copy(txHash, keySlice.Data())

		// Parse value (4-byte big-endian ledger sequence)
		ledgerSeq := binary.BigEndian.Uint32(valueSlice.Data())

		keySlice.Free()
		valueSlice.Free()

		if err := callback(txHash, ledgerSeq, index); err != nil {
			return err
		}

		index++
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterator error: %v", err)
	}

	return nil
}

// verifyRecSplit verifies a sample of lookups against the original RocksDB
func verifyRecSplit(outputDir, db3Path string, sampleSize int) (int, int) {
	// Open RecSplit index
	idx, err := recsplit.OpenIndex(filepath.Join(outputDir, "txhash.idx"))
	if err != nil {
		log.Printf("Failed to open RecSplit index: %v", err)
		return 0, 0
	}
	defer idx.Close()

	// Load values
	valuesPath := filepath.Join(outputDir, "values.bin")
	valuesFile, err := os.Open(valuesPath)
	if err != nil {
		log.Printf("Failed to open values file: %v", err)
		return 0, 0
	}
	defer valuesFile.Close()

	var numEntries uint64
	binary.Read(valuesFile, binary.LittleEndian, &numEntries)

	values := make([]uint32, numEntries)
	binary.Read(valuesFile, binary.LittleEndian, values)

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

		// Get expected value from RocksDB
		expectedLedgerSeq := binary.BigEndian.Uint32(valueSlice.Data())

		// Copy key
		txHash := make([]byte, len(keySlice.Data()))
		copy(txHash, keySlice.Data())

		keySlice.Free()
		valueSlice.Free()

		// Lookup via RecSplit
		idx := reader.Lookup(txHash)
		if idx >= uint64(len(values)) {
			failed++
			if failed <= 5 {
				fmt.Printf("  Index out of range at %d: %d >= %d\n", checked, idx, len(values))
			}
			checked++
			continue
		}

		actualLedgerSeq := values[idx]
		if actualLedgerSeq == expectedLedgerSeq {
			verified++
		} else {
			failed++
			if failed <= 5 {
				fmt.Printf("  Mismatch at %d: expected %d, got %d (idx=%d)\n",
					checked, expectedLedgerSeq, actualLedgerSeq, idx)
			}
		}

		checked++
	}

	return verified, failed
}

// printMemStats prints current memory usage
func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Memory: Alloc=%s, Sys=%s, HeapInuse=%s\n",
		helpers.FormatBytes(int64(m.Alloc)),
		helpers.FormatBytes(int64(m.Sys)),
		helpers.FormatBytes(int64(m.HeapInuse)))
}
