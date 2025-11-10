package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/linxGnu/grocksdb"
)

// MergeStats tracks statistics during the merge
type MergeStats struct {
	TotalKeys           int64
	TotalBytes          int64
	TotalKeysToMerge    int64 // Total keys across all sources
	SourceDBs           int
	CurrentSource       string
	StartTime           time.Time
	LastReportTime      time.Time
	KeysSinceReport     int64
	LastReportedPercent int
}

func main() {
	var finalPath string
	var sourcePaths string
	var batchSize int
	var reportInterval int

	flag.StringVar(&finalPath, "final-path", "", "Destination database path")
	flag.StringVar(&sourcePaths, "paths", "", "Space-separated source database paths")
	flag.IntVar(&batchSize, "batch-size", 10000, "Number of keys to write per batch")
	flag.IntVar(&reportInterval, "report-interval", 100000, "Report progress every N keys")
	flag.Parse()

	if finalPath == "" || sourcePaths == "" {
		log.Fatal("Usage: go run merge_dbs.go -final-path /path/to/final -paths \"/path/db1 /path/db2 /path/db3\"")
	}

	// Parse source paths
	sources := strings.Fields(sourcePaths)
	if len(sources) == 0 {
		log.Fatal("No source paths provided")
	}

	// Validate source paths exist
	for _, path := range sources {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			log.Fatalf("Source path does not exist: %s", path)
		}
	}

	fmt.Printf("\n========================================\n")
	fmt.Printf("RocksDB Merge Tool\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Destination: %s\n", finalPath)
	fmt.Printf("Source DBs: %d\n", len(sources))
	for i, src := range sources {
		fmt.Printf("  [%d] %s\n", i+1, src)
	}
	fmt.Printf("========================================\n\n")

	// Initialize stats
	stats := &MergeStats{
		SourceDBs:           len(sources),
		StartTime:           time.Now(),
		LastReportTime:      time.Now(),
		LastReportedPercent: -1,
	}

	// Open or create destination database with optimized settings
	destDB, destOpts, err := openDestinationDB(finalPath)
	if err != nil {
		log.Fatalf("Failed to open destination DB: %v", err)
	}
	defer destDB.Close()
	defer destOpts.Destroy()

	log.Printf("✓ Destination database ready at: %s\n", finalPath)

	// Show initial state of destination
	if _, err := os.Stat(filepath.Join(finalPath, "CURRENT")); err == nil {
		log.Printf("Destination database exists, will merge into existing data")
		showStats(destDB, "Initial Destination State")
	} else {
		log.Printf("Creating new destination database")
	}

	fmt.Println()

	// Pre-scan: Count total keys across all sources for percentage tracking
	log.Printf("Scanning source databases to count total keys...\n")
	for i, srcPath := range sources {
		srcOpts := grocksdb.NewDefaultOptions()
		srcOpts.SetCreateIfMissing(false)
		defer srcOpts.Destroy()

		srcDB, err := grocksdb.OpenDbForReadOnly(srcOpts, srcPath, false)
		if err != nil {
			log.Printf("Warning: Failed to open %s for counting: %v", srcPath, err)
			continue
		}

		estimatedKeys := srcDB.GetProperty("rocksdb.estimate-num-keys")
		var keyCount int64
		fmt.Sscanf(estimatedKeys, "%d", &keyCount)
		stats.TotalKeysToMerge += keyCount

		log.Printf("  [%d/%d] %s: ~%s keys", i+1, len(sources), filepath.Base(srcPath), formatNumber(keyCount))

		srcDB.Close()
	}

	log.Printf("\nTotal keys to merge: ~%s\n", formatNumber(stats.TotalKeysToMerge))
	log.Printf("Will show stats at each 1%% of progress\n")
	fmt.Println()

	// Merge each source database
	for i, srcPath := range sources {
		stats.CurrentSource = srcPath
		log.Printf("[%d/%d] Merging: %s\n", i+1, len(sources), srcPath)

		if err := mergeDatabase(destDB, srcPath, stats, batchSize, reportInterval); err != nil {
			log.Printf("Error merging %s: %v", srcPath, err)
			continue
		}

		log.Printf("✓ Completed: %s\n\n", srcPath)
	}

	// Final flush and compaction
	log.Printf("Performing final flush...\n")
	flushDB(destDB)

	log.Printf("Performing final compaction...\n")
	compactionStart := time.Now()
	destDB.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
	log.Printf("✓ Compaction complete in %s\n\n", formatDuration(time.Since(compactionStart)))

	// Show final statistics
	elapsed := time.Since(stats.StartTime)
	fmt.Printf("\n========================================\n")
	fmt.Printf("MERGE COMPLETE\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Total keys merged:    %s\n", formatNumber(stats.TotalKeys))
	fmt.Printf("Total data size:      %s\n", formatBytes(stats.TotalBytes))
	fmt.Printf("Source databases:     %d\n", stats.SourceDBs)
	fmt.Printf("Total time:           %s\n", formatDuration(elapsed))
	fmt.Printf("Average speed:        %.2f keys/sec\n", float64(stats.TotalKeys)/elapsed.Seconds())
	fmt.Printf("========================================\n\n")

	showStats(destDB, "Final Destination State")

	fmt.Printf("\n========================================\n")
	fmt.Printf("✓ Merge complete: %s\n", finalPath)
	fmt.Printf("========================================\n\n")
}

// openDestinationDB opens or creates the destination database with optimized settings
func openDestinationDB(path string) (*grocksdb.DB, *grocksdb.Options, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create destination directory: %w", err)
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetErrorIfExists(false) // Allow appending to existing DB

	// ============================================================================
	// OPTIMIZED SETTINGS FOR MERGING/BULK LOADING
	// ============================================================================

	// 1. LARGE WRITE BUFFERS
	// Accumulate more data in memory before flushing to reduce number of L0 files
	opts.SetWriteBufferSize(512 << 20) // 512 MB per memtable
	opts.SetMaxWriteBufferNumber(6)    // Allow 6 memtables (3 GB total)
	opts.SetMinWriteBufferNumberToMerge(2)

	// 2. COMPRESSION
	// No compression since source data is already compressed
	opts.SetCompression(grocksdb.NoCompression)

	// 3. COMPACTION SETTINGS
	// Enable auto-compactions but with high thresholds to batch work
	opts.SetDisableAutoCompactions(false)
	opts.SetLevel0FileNumCompactionTrigger(50) // Start compacting at 50 L0 files
	opts.SetLevel0SlowdownWritesTrigger(100)   // Soft limit
	opts.SetLevel0StopWritesTrigger(150)       // Hard limit

	// 4. FILE SIZES
	// Larger files reduce total file count
	opts.SetTargetFileSizeBase(256 << 20) // 256 MB per file
	opts.SetTargetFileSizeMultiplier(2)
	opts.SetMaxBytesForLevelBase(1024 << 20) // 1 GB for L1
	opts.SetMaxBytesForLevelMultiplier(10)

	// 5. BACKGROUND JOBS
	opts.SetMaxBackgroundJobs(12)

	// 6. OPEN FILES
	opts.SetMaxOpenFiles(5000)

	// 7. WAL
	opts.SetMaxTotalWalSize(2048 << 20) // 2 GB max WAL

	// 8. BLOCK CACHE
	// Cache for better performance during merge
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockSize(32 << 10)                         // 32 KB blocks
	bbto.SetBlockCache(grocksdb.NewLRUCache(512 << 20)) // 512 MB cache
	opts.SetBlockBasedTableFactory(bbto)

	// 9. LOGGING
	opts.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)
	opts.SetMaxLogFileSize(20 << 20)
	opts.SetKeepLogFileNum(3)

	log.Printf("Destination DB Settings:")
	log.Printf("  Write Buffer: 512 MB × 6 = 3 GB")
	log.Printf("  L0 Trigger: 50 files")
	log.Printf("  Background Compactions: 10")
	log.Printf("  Block Cache: 512 MB")
	log.Printf("  Auto-compactions: Enabled (controlled)")

	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		opts.Destroy()
		return nil, nil, err
	}

	return db, opts, nil
}

// mergeDatabase merges a single source database into the destination
func mergeDatabase(destDB *grocksdb.DB, srcPath string, stats *MergeStats, batchSize, reportInterval int) error {
	// Open source database in read-only mode
	srcOpts := grocksdb.NewDefaultOptions()
	srcOpts.SetCreateIfMissing(false)
	defer srcOpts.Destroy()

	srcDB, err := grocksdb.OpenDbForReadOnly(srcOpts, srcPath, false)
	if err != nil {
		return fmt.Errorf("failed to open source DB: %w", err)
	}
	defer srcDB.Close()

	// Show source DB stats
	showStats(srcDB, fmt.Sprintf("Source: %s", filepath.Base(srcPath)))

	// Create iterator for source database
	readOpts := grocksdb.NewDefaultReadOptions()
	readOpts.SetFillCache(false) // Don't pollute cache during scan
	defer readOpts.Destroy()

	it := srcDB.NewIterator(readOpts)
	defer it.Close()

	// Create write options (disable WAL for speed)
	writeOpts := grocksdb.NewDefaultWriteOptions()
	writeOpts.DisableWAL(true) // Faster bulk writes
	defer writeOpts.Destroy()

	// Write batch
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	batchCount := 0
	keysInSource := int64(0)
	bytesInSource := int64(0)

	// Iterate through all keys in source
	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()

		keyData := key.Data()
		valueData := value.Data()

		// Add to batch
		batch.Put(keyData, valueData)
		batchCount++

		// Update stats
		keysInSource++
		bytesInSource += int64(len(keyData) + len(valueData))

		key.Free()
		value.Free()

		// Write batch when full
		if batchCount >= batchSize {
			if err := destDB.Write(writeOpts, batch); err != nil {
				return fmt.Errorf("failed to write batch: %w", err)
			}
			batch.Clear()
			batchCount = 0

			// Update global stats
			atomic.AddInt64(&stats.TotalKeys, int64(batchSize))
			atomic.AddInt64(&stats.TotalBytes, bytesInSource)
			bytesInSource = 0

			// Check for percentage milestone and show detailed stats
			checkPercentageMilestone(stats, destDB)

			// Regular progress report
			if stats.TotalKeys%int64(reportInterval) == 0 {
				reportProgress(stats, destDB)
			}
		}
	}

	// Write remaining batch
	if batchCount > 0 {
		if err := destDB.Write(writeOpts, batch); err != nil {
			return fmt.Errorf("failed to write final batch: %w", err)
		}
		atomic.AddInt64(&stats.TotalKeys, int64(batchCount))
		atomic.AddInt64(&stats.TotalBytes, bytesInSource)
	}

	// Check for iterator errors
	if err := it.Err(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	// Flush after each source DB
	flushDB(destDB)

	log.Printf("  Keys merged from this source: %s", formatNumber(keysInSource))

	return nil
}

// reportProgress shows current merge progress
func reportProgress(stats *MergeStats, db *grocksdb.DB) {
	elapsed := time.Since(stats.StartTime)
	sinceLastReport := time.Since(stats.LastReportTime)

	keysPerSec := float64(stats.TotalKeys) / elapsed.Seconds()
	recentKeysPerSec := float64(stats.KeysSinceReport) / sinceLastReport.Seconds()

	l0Files := db.GetProperty("rocksdb.num-files-at-level0")

	log.Printf("  Progress: %s keys | %.2f keys/sec (recent: %.2f) | %s | L0: %s files | Source: %s",
		formatNumber(stats.TotalKeys),
		keysPerSec,
		recentKeysPerSec,
		formatBytes(stats.TotalBytes),
		l0Files,
		filepath.Base(stats.CurrentSource))

	stats.LastReportTime = time.Now()
	stats.KeysSinceReport = 0
}

// checkPercentageMilestone shows detailed stats at each percentage of completion
func checkPercentageMilestone(stats *MergeStats, db *grocksdb.DB) {
	if stats.TotalKeysToMerge == 0 {
		return // Skip if we don't know total
	}

	currentPercent := int((stats.TotalKeys * 100) / stats.TotalKeysToMerge)

	// Show stats at each new percentage milestone
	if currentPercent > stats.LastReportedPercent && currentPercent <= 100 {
		stats.LastReportedPercent = currentPercent

		elapsed := time.Since(stats.StartTime)
		keysPerSec := float64(stats.TotalKeys) / elapsed.Seconds()

		// Calculate ETA
		remaining := stats.TotalKeysToMerge - stats.TotalKeys
		var eta time.Duration
		if keysPerSec > 0 {
			eta = time.Duration(float64(remaining)/keysPerSec) * time.Second
		}

		fmt.Printf("\n")
		log.Printf("========================================")
		log.Printf("PROGRESS: %d%% Complete", currentPercent)
		log.Printf("========================================")
		log.Printf("Keys merged: %s / %s", formatNumber(stats.TotalKeys), formatNumber(stats.TotalKeysToMerge))
		log.Printf("Speed: %.2f keys/sec", keysPerSec)
		log.Printf("Elapsed: %s | ETA: %s", formatDuration(elapsed), formatDuration(eta))
		log.Printf("Data size: %s", formatBytes(stats.TotalBytes))

		// Show current database state
		fmt.Println()
		log.Println("Current Destination DB State:")

		// Level distribution (compact format)
		l0 := db.GetProperty("rocksdb.num-files-at-level0")
		l1 := db.GetProperty("rocksdb.num-files-at-level1")
		l2 := db.GetProperty("rocksdb.num-files-at-level2")
		l3 := db.GetProperty("rocksdb.num-files-at-level3")
		l4 := db.GetProperty("rocksdb.num-files-at-level4")
		l5 := db.GetProperty("rocksdb.num-files-at-level5")
		l6 := db.GetProperty("rocksdb.num-files-at-level6")

		log.Printf("  SST Files: L0=%s, L1=%s, L2=%s, L3=%s, L4=%s, L5=%s, L6=%s",
			l0, l1, l2, l3, l4, l5, l6)

		// Compaction status
		compactionPending := db.GetProperty("rocksdb.compaction-pending")
		numRunningCompactions := db.GetProperty("rocksdb.num-running-compactions")
		log.Printf("  Compactions: %s running, %s pending", numRunningCompactions, compactionPending)

		// Size stats
		totalSSTSize := db.GetProperty("rocksdb.total-sst-files-size")
		log.Printf("  Total SST Size: %s (%.2f GB)", totalSSTSize, bytesToGB(totalSSTSize))

		log.Printf("========================================\n")
	}
}

// flushDB flushes the database to disk
func flushDB(db *grocksdb.DB) {
	fo := grocksdb.NewDefaultFlushOptions()
	fo.SetWait(true)
	defer fo.Destroy()

	if err := db.Flush(fo); err != nil {
		log.Printf("Warning: Failed to flush: %v", err)
	}
}

// showStats displays current database statistics
func showStats(db *grocksdb.DB, label string) {
	fmt.Printf("\n--- %s ---\n", label)

	// Level distribution
	fmt.Println("SST Files:")
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
	if totalFiles > 0 {
		fmt.Printf("  Total: %d files\n", totalFiles)
	}

	// Size and keys
	estimatedKeys := db.GetProperty("rocksdb.estimate-num-keys")
	totalSSTSize := db.GetProperty("rocksdb.total-sst-files-size")

	fmt.Printf("Estimated Keys: %s\n", estimatedKeys)
	fmt.Printf("Total Size: %s (%.2f GB)\n", totalSSTSize, bytesToGB(totalSSTSize))
	fmt.Println()
}

// Helper functions
func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	s := fmt.Sprintf("%d", n)
	result := ""
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result += ","
		}
		result += string(c)
	}
	return result
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
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
