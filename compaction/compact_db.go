package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/linxGnu/grocksdb"
)

func main() {
	var dbPath string
	var dryRun bool

	flag.StringVar(&dbPath, "path", "", "Path to RocksDB database")
	flag.BoolVar(&dryRun, "dry-run", false, "Show stats only, don't compact")
	flag.Parse()

	if dbPath == "" {
		log.Fatal("Usage: go run compact_db.go -path /path/to/rocksdb [-dry-run]")
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Fatalf("Database path does not exist: %s", dbPath)
	}

	// Open database in read-write mode
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	// Set compaction settings for the redistribution
	opts.SetMaxBackgroundJobs(12)
	opts.SetTargetFileSizeBase(256 << 20)    // 256 MB
	opts.SetMaxBytesForLevelBase(1024 << 20) // 1 GB for L1
	opts.SetMaxBytesForLevelMultiplier(10)   // Each level 10x previous

	db, err := grocksdb.OpenDb(opts, dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	defer opts.Destroy()

	fmt.Printf("\n========================================\n")
	fmt.Printf("RocksDB Compaction Tool\n")
	fmt.Printf("Database: %s\n", dbPath)
	fmt.Printf("========================================\n\n")

	// Show current state
	fmt.Println("BEFORE Compaction:")
	showStats(db)

	if dryRun {
		fmt.Println("\n[DRY RUN MODE - No compaction performed]")
		fmt.Println("\nTo actually compact, run without -dry-run flag:")
		fmt.Printf("  go run compact_db.go -path %s\n", dbPath)
		return
	}

	// Perform full range compaction
	fmt.Println("\nStarting full database compaction...")
	fmt.Println("This will redistribute files from L1 → L2 → L3...")
	fmt.Println("(This may take several minutes to hours depending on size)\n")

	startTime := time.Now()

	// Compact the entire key range (nil, nil = all keys)
	db.CompactRange(grocksdb.Range{Start: nil, Limit: nil})

	elapsed := time.Since(startTime)
	fmt.Printf("\nCompaction completed in %s\n\n", formatDuration(elapsed))

	// Show final state
	fmt.Println("AFTER Compaction:")
	showStats(db)

	fmt.Printf("\n========================================\n")
	fmt.Println("✓ Database compaction complete")
	fmt.Printf("========================================\n\n")
}

func showStats(db *grocksdb.DB) {
	fmt.Println("SST File Distribution:")
	totalFiles := 0
	for i := 0; i <= 6; i++ {
		prop := fmt.Sprintf("rocksdb.num-files-at-level%d", i)
		count := db.GetProperty(prop)
		if count != "" {
			var numFiles int
			fmt.Sscanf(count, "%d", &numFiles)
			if numFiles > 0 {
				fmt.Printf("  L%d: %s files\n", i, count)
				totalFiles += numFiles
			}
		}
	}
	fmt.Printf("  Total: %d files\n\n", totalFiles)

	totalSSTSize := db.GetProperty("rocksdb.total-sst-files-size")
	fmt.Printf("Total SST Size: %s bytes (%.2f GB)\n", totalSSTSize, bytesToGB(totalSSTSize))

	estimatedKeys := db.GetProperty("rocksdb.estimate-num-keys")
	fmt.Printf("Estimated Keys: %s\n", estimatedKeys)

	liveDataSize := db.GetProperty("rocksdb.estimate-live-data-size")
	fmt.Printf("Live Data Size: %s bytes (%.2f GB)\n", liveDataSize, bytesToGB(liveDataSize))
}

func bytesToGB(bytesStr string) float64 {
	var bytes float64
	fmt.Sscanf(bytesStr, "%f", &bytes)
	return bytes / (1024 * 1024 * 1024)
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second

	if h > 0 {
		return fmt.Sprintf("%dh %dm %ds", h, m, s)
	} else if m > 0 {
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}
