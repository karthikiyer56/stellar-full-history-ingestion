package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/linxGnu/grocksdb"
)

func main() {
	var dbPath string
	flag.StringVar(&dbPath, "path", "", "Path to RocksDB database")
	flag.Parse()

	if dbPath == "" {
		log.Fatal("Usage: go run monitor_db.go -path /path/to/rocksdb")
	}

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Fatalf("Database path does not exist: %s", dbPath)
	}

	// Open database in read-only mode
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	db, err := grocksdb.OpenDbForReadOnly(opts, dbPath, false)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	defer opts.Destroy()

	fmt.Printf("\n========================================\n")
	fmt.Printf("RocksDB Statistics: %s\n", dbPath)
	fmt.Printf("========================================\n\n")

	monitorRocksDBStats(db)
}

func monitorRocksDBStats(db *grocksdb.DB) {
	// Level file counts
	fmt.Println("SST File Distribution:")
	for i := 0; i <= 6; i++ {
		prop := fmt.Sprintf("rocksdb.num-files-at-level%d", i)
		count := db.GetProperty(prop)
		if count != "" && count != "0" {
			fmt.Printf("  L%d: %s files\n", i, count)
		}
	}

	fmt.Println()

	// Key statistics
	estimatedKeys := db.GetProperty("rocksdb.estimate-num-keys")
	fmt.Printf("Estimated Keys: %s\n", estimatedKeys)

	// Size statistics
	totalSSTSize := db.GetProperty("rocksdb.total-sst-files-size")
	fmt.Printf("Total SST Size: %s bytes (%.2f GB)\n", totalSSTSize, bytesToGB(totalSSTSize))

	// Memtable statistics
	curMemtable := db.GetProperty("rocksdb.cur-size-all-mem-tables")
	fmt.Printf("Memtable Usage: %s bytes\n", curMemtable)

	// Block cache
	blockCacheUsage := db.GetProperty("rocksdb.block-cache-usage")
	if blockCacheUsage != "" {
		fmt.Printf("Block Cache Usage: %s bytes\n", blockCacheUsage)
	}

	// Compaction status
	compactionPending := db.GetProperty("rocksdb.compaction-pending")
	fmt.Printf("Compaction Pending: %s\n", compactionPending)

	numRunningCompactions := db.GetProperty("rocksdb.num-running-compactions")
	fmt.Printf("Running Compactions: %s\n", numRunningCompactions)

	numRunningFlushes := db.GetProperty("rocksdb.num-running-flushes")
	fmt.Printf("Running Flushes: %s\n", numRunningFlushes)

	fmt.Println()

	// Compression stats per level
	fmt.Println("Compression Type Per Level:")
	for i := 0; i <= 6; i++ {
		prop := fmt.Sprintf("rocksdb.compression-ratio-at-level%d", i)
		ratio := db.GetProperty(prop)
		if ratio != "" {
			fmt.Printf("  L%d compression ratio: %s\n", i, ratio)
		}
	}

	fmt.Println()

	// Additional stats
	fmt.Println("Additional Statistics:")

	stats := []string{
		"rocksdb.estimate-table-readers-mem",
		"rocksdb.estimate-live-data-size",
		"rocksdb.min-log-number-to-keep",
		"rocksdb.total-blob-file-size",
		"rocksdb.live-blob-file-size",
	}

	for _, stat := range stats {
		value := db.GetProperty(stat)
		if value != "" && value != "0" {
			fmt.Printf("  %s: %s\n", stat, value)
		}
	}

	fmt.Printf("\n========================================\n")
}

func bytesToGB(bytesStr string) float64 {
	var bytes float64
	fmt.Sscanf(bytesStr, "%f", &bytes)
	return bytes / (1024 * 1024 * 1024)
}
