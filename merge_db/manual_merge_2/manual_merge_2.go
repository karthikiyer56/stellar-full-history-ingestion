package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
)

func main() {
	var finalPath string
	var sourcePaths string
	var batchSize int
	var maxParallel int

	flag.StringVar(&finalPath, "final-path", "", "Destination database path")
	flag.StringVar(&sourcePaths, "paths", "", "Space-separated source database paths")
	flag.IntVar(&batchSize, "batch-size", 1_000_000, "Number of keys per batch")
	flag.IntVar(&maxParallel, "parallel", 3, "Number of source DBs to merge in parallel")
	flag.Parse()

	if finalPath == "" || sourcePaths == "" {
		log.Fatal("Usage: go run main.go -final-path /path/to/final -paths \"/path/db1 /path/db2\"")
	}

	sources := strings.Fields(sourcePaths)
	if len(sources) == 0 {
		log.Fatal("No source paths provided")
	}

	for _, path := range sources {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			log.Fatalf("Source path does not exist: %s", path)
		}
	}

	fmt.Printf("\n========================================\n")
	fmt.Printf("Parallel RocksDB Merge (Batch Ingestion + Global Tracker)\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Destination: %s\n", finalPath)
	fmt.Printf("Source DBs: %d\n", len(sources))
	for i, src := range sources {
		fmt.Printf("  [%d] %s\n", i+1, src)
	}
	fmt.Printf("Batch size: %d keys\n", batchSize)
	fmt.Printf("Max parallel DBs: %d\n", maxParallel)
	fmt.Printf("========================================\n\n")

	startTotal := time.Now()

	destDB, destOpts, err := openDestinationDB(finalPath)
	if err != nil {
		log.Fatalf("Failed to open destination DB: %v", err)
	}
	defer destDB.Close()
	defer destOpts.Destroy()

	log.Printf("✓ Destination database ready\n\n")

	// Initialize global tracker
	var globalKeysMerged int64
	var totalKeys int64
	for _, src := range sources {
		dbKeys, _ := estimateKeys(src)
		totalKeys += dbKeys
	}

	// Start a goroutine to print global progress every few seconds
	stopGlobal := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-ticker.C:
				merged := globalKeysMerged
				percent := float64(merged) / float64(totalKeys) * 100
				elapsed := time.Since(start)
				eta := time.Duration(float64(elapsed) * (100/percent - 1))
				log.Printf("GLOBAL: %d/%d keys merged (%.2f%%) - ETA %s", merged, totalKeys, percent, eta.Round(time.Second))
			case <-stopGlobal:
				return
			}
		}
	}()

	// Parallel merge
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxParallel)
	errCh := make(chan error, len(sources))
	for _, src := range sources {
		wg.Add(1)
		go func(dbPath string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			start := time.Now()
			log.Printf("Merging source DB: %s", dbPath)
			if err := mergeSourceDBWithGlobal(destDB, dbPath, batchSize, &globalKeysMerged); err != nil {
				errCh <- fmt.Errorf("error merging %s: %w", dbPath, err)
				return
			}
			log.Printf("✓ Finished merging %s in %s\n", dbPath, time.Since(start).Round(time.Second))
		}(src)
	}

	wg.Wait()
	close(errCh)
	close(stopGlobal)

	for e := range errCh {
		log.Println(e)
	}

	log.Printf("Starting final compaction...")
	compStart := time.Now()
	destDB.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
	log.Printf("✓ Compaction completed in %s\n", time.Since(compStart).Round(time.Second))

	log.Printf("\nDestination DB Stats:")
	showStats(destDB)

	log.Printf("\n========================================\n")
	log.Printf("MERGE COMPLETE in %s", time.Since(startTotal).Round(time.Second))
	fmt.Printf("========================================\n\n")
}

// openDestinationDB creates or opens destination RocksDB
func openDestinationDB(path string) (*grocksdb.DB, *grocksdb.Options, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create directory: %w", err)
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetErrorIfExists(false)
	opts.SetWriteBufferSize(512 << 20)
	opts.SetMaxBackgroundJobs(20)
	opts.SetTargetFileSizeBase(256 << 20)
	opts.SetMaxBytesForLevelBase(1024 << 20)
	opts.SetMaxBytesForLevelMultiplier(10)
	opts.SetLevel0FileNumCompactionTrigger(50)
	opts.SetLevel0SlowdownWritesTrigger(100)
	opts.SetLevel0StopWritesTrigger(150)
	opts.SetCompression(grocksdb.NoCompression)
	opts.SetMaxOpenFiles(5000)

	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		opts.Destroy()
		return nil, nil, err
	}
	return db, opts, nil
}

// estimateKeys estimates keys in a DB
func estimateKeys(dbPath string) (int64, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)
	defer opts.Destroy()

	db, err := grocksdb.OpenDbForReadOnly(opts, dbPath, false)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	prop := db.GetProperty("rocksdb.estimate-num-keys")
	var n int64
	fmt.Sscanf(prop, "%d", &n)
	if n == 0 {
		n = 1
	}
	return n, nil
}

// mergeSourceDBWithGlobal merges keys in batches and updates global counter
func mergeSourceDBWithGlobal(destDB *grocksdb.DB, srcPath string, batchSize int, globalCounter *int64) error {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)
	defer opts.Destroy()

	db, err := grocksdb.OpenDbForReadOnly(opts, srcPath, false)
	if err != nil {
		return fmt.Errorf("failed to open source DB: %w", err)
	}
	defer db.Close()

	readOpts := grocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()

	it := db.NewIterator(readOpts)
	defer it.Close()

	// Estimate keys for per-DB progress
	totalKeys, _ := estimateKeys(srcPath)

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	writeOpts := grocksdb.NewDefaultWriteOptions()
	defer writeOpts.Destroy()

	keyCount := 0
	batchCount := 0
	startBatchTime := time.Now()
	startTotal := time.Now()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		k := it.Key()
		v := it.Value()
		batch.Put(k.Data(), v.Data())
		k.Free()
		v.Free()

		keyCount++
		if keyCount%batchSize == 0 {
			if err := destDB.Write(writeOpts, batch); err != nil {
				return fmt.Errorf("failed to write batch: %w", err)
			}
			batchCount++
			elapsed := time.Since(startBatchTime)
			percent := float64(keyCount) / float64(totalKeys) * 100
			eta := time.Duration(float64(time.Since(startTotal)) * (100/percent - 1))
			log.Printf("  [%s] Batch %d - %d keys written (%.2f%%) - ETA %s - Batch time %s",
				filepath.Base(srcPath), batchCount, keyCount, percent, eta.Round(time.Second), elapsed.Round(time.Second))
			batch.Clear()
			startBatchTime = time.Now()
		}

		// Atomically update global counter
		atomicAddInt64(globalCounter, 1)
	}

	if batch.Count() > 0 {
		if err := destDB.Write(writeOpts, batch); err != nil {
			return fmt.Errorf("failed to write final batch: %w", err)
		}
		batchCount++
		totalElapsed := time.Since(startTotal)
		log.Printf("  [%s] Final batch %d - %d keys written (100%%) - Total time %s",
			filepath.Base(srcPath), batchCount, keyCount, totalElapsed.Round(time.Second))
	}

	log.Printf("Total keys merged from %s: %d in %d batches", srcPath, keyCount, batchCount)
	return nil
}

// showStats prints RocksDB stats
func showStats(db *grocksdb.DB) {
	totalFiles := 0
	for i := 0; i <= 6; i++ {
		prop := fmt.Sprintf("rocksdb.num-files-at-level%d", i)
		count := db.GetProperty(prop)
		if count != "" && count != "0" {
			fmt.Printf("  L%d: %s files\n", i, count)
			var n int
			fmt.Sscanf(count, "%d", &n)
			totalFiles += n
		}
	}
	fmt.Printf("  Total files: %d\n", totalFiles)

	estimatedKeys := db.GetProperty("rocksdb.estimate-num-keys")
	fmt.Printf("  Estimated keys: %s\n", estimatedKeys)

	totalSSTSize := db.GetProperty("rocksdb.total-sst-files-size")
	fmt.Printf("  Total size: %s bytes (%.2f GB)\n", totalSSTSize, bytesToGB(totalSSTSize))
}

func bytesToGB(bytesStr string) float64 {
	var bytes float64
	fmt.Sscanf(bytesStr, "%f", &bytes)
	return bytes / (1024 * 1024 * 1024)
}

// atomicAddInt64 performs thread-safe increment
func atomicAddInt64(addr *int64, delta int64) {
	for {
		old := *addr
		newVal := old + delta
		if casInt64(addr, old, newVal) {
			return
		}
	}
}

// casInt64 simulates atomic compare-and-swap for int64
func casInt64(addr *int64, old, new int64) bool {
	if *addr == old {
		*addr = new
		return true
	}
	return false
}
