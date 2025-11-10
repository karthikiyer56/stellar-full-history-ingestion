package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/linxGnu/grocksdb"
)

func main() {
	var dbPath string
	var showDetailed bool
	flag.StringVar(&dbPath, "path", "", "Path to RocksDB database")
	flag.BoolVar(&showDetailed, "detailed", false, "Show detailed statistics including recommendations")
	flag.Parse()

	if dbPath == "" {
		log.Fatal("Usage: go run monitor_db.go -path /path/to/rocksdb [-detailed]")
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

	monitorRocksDBStats(db, showDetailed)

	if showDetailed {
		fmt.Println("\n========================================")
		fmt.Println("OPTIMIZATION RECOMMENDATIONS")
		fmt.Println("========================================")
		analyzeAndRecommend(db)
	}
}

func monitorRocksDBStats(db *grocksdb.DB, detailed bool) {
	// Database size on disk
	fmt.Println("📊 STORAGE OVERVIEW")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	totalSSTSize := getPropertyInt(db, "rocksdb.total-sst-files-size")
	liveSSTSize := getPropertyInt(db, "rocksdb.estimate-live-data-size")

	fmt.Printf("Total SST Size:          %s\n", formatBytes(totalSSTSize))
	fmt.Printf("Live Data Size:          %s\n", formatBytes(liveSSTSize))
	if totalSSTSize > 0 && liveSSTSize > 0 {
		obsoleteRatio := float64(totalSSTSize-liveSSTSize) / float64(totalSSTSize) * 100
		fmt.Printf("Obsolete Data:           %s (%.1f%%)\n", formatBytes(totalSSTSize-liveSSTSize), obsoleteRatio)
	}

	estimatedKeys := getPropertyInt(db, "rocksdb.estimate-num-keys")
	fmt.Printf("Estimated Keys:          %s\n", formatNumber(estimatedKeys))

	if estimatedKeys > 0 && totalSSTSize > 0 {
		avgKeySize := float64(totalSSTSize) / float64(estimatedKeys)
		fmt.Printf("Avg Key+Value Size:      %.2f bytes\n", avgKeySize)
	}

	fmt.Println()

	// Level breakdown with sizes
	fmt.Println("📁 LEVEL BREAKDOWN (SST Files)")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("%-8s %-12s %-15s %-15s %-12s\n", "Level", "Files", "Size", "Avg File Size", "Comp Ratio")
	fmt.Println("────────────────────────────────────────────────────────────────")

	var totalFiles int64
	var totalLevelSize int64

	for i := 0; i <= 6; i++ {
		numFiles := getPropertyInt(db, fmt.Sprintf("rocksdb.num-files-at-level%d", i))
		if numFiles == 0 && i > 2 {
			continue
		}

		// Get level size - this is an approximation
		levelSize := getPropertyInt(db, fmt.Sprintf("rocksdb.aggregated-table-properties-at-level%d", i))
		if levelSize == 0 && numFiles > 0 {
			// Fallback estimation
			levelSize = totalSSTSize / 7 // Rough estimate
		}

		var avgFileSize int64
		if numFiles > 0 {
			avgFileSize = levelSize / numFiles
		}

		compRatio := db.GetProperty(fmt.Sprintf("rocksdb.compression-ratio-at-level%d", i))
		if compRatio == "" {
			compRatio = "N/A"
		}

		fmt.Printf("L%-7d %-12s %-15s %-15s %-12s\n",
			i,
			formatNumber(numFiles),
			formatBytes(levelSize),
			formatBytes(avgFileSize),
			compRatio)

		totalFiles += numFiles
		totalLevelSize += levelSize
	}

	fmt.Println("────────────────────────────────────────────────────────────────")
	fmt.Printf("%-8s %-12s %-15s\n", "TOTAL", formatNumber(totalFiles), formatBytes(totalLevelSize))
	fmt.Println()

	// Memory usage
	fmt.Println("💾 MEMORY USAGE")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	memtableSize := getPropertyInt(db, "rocksdb.cur-size-all-mem-tables")
	fmt.Printf("Memtable Size:           %s\n", formatBytes(memtableSize))

	blockCacheUsage := getPropertyInt(db, "rocksdb.block-cache-usage")
	if blockCacheUsage > 0 {
		fmt.Printf("Block Cache Usage:       %s\n", formatBytes(blockCacheUsage))
	}

	blockCacheCapacity := getPropertyInt(db, "rocksdb.block-cache-capacity")
	if blockCacheCapacity > 0 {
		fmt.Printf("Block Cache Capacity:    %s\n", formatBytes(blockCacheCapacity))
		if blockCacheUsage > 0 {
			cacheUtilization := float64(blockCacheUsage) / float64(blockCacheCapacity) * 100
			fmt.Printf("Cache Utilization:       %.1f%%\n", cacheUtilization)
		}
	}

	tableReadersMem := getPropertyInt(db, "rocksdb.estimate-table-readers-mem")
	if tableReadersMem > 0 {
		fmt.Printf("Table Readers Memory:    %s\n", formatBytes(tableReadersMem))
	}

	pinnedMem := getPropertyInt(db, "rocksdb.block-cache-pinned-usage")
	if pinnedMem > 0 {
		fmt.Printf("Pinned Memory:           %s\n", formatBytes(pinnedMem))
	}

	totalMemUsage := memtableSize + blockCacheUsage + tableReadersMem
	fmt.Printf("─────────────────────────────────────────\n")
	fmt.Printf("Total Memory Usage:      %s\n", formatBytes(totalMemUsage))
	fmt.Println()

	// Compaction status
	fmt.Println("⚙️  COMPACTION STATUS")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	compactionPending := db.GetProperty("rocksdb.compaction-pending")
	numRunningCompactions := db.GetProperty("rocksdb.num-running-compactions")
	numRunningFlushes := db.GetProperty("rocksdb.num-running-flushes")

	fmt.Printf("Compaction Pending:      %s\n", compactionPending)
	fmt.Printf("Running Compactions:     %s\n", numRunningCompactions)
	fmt.Printf("Running Flushes:         %s\n", numRunningFlushes)

	numImmutableMemtables := db.GetProperty("rocksdb.num-immutable-mem-table")
	if numImmutableMemtables != "" {
		fmt.Printf("Immutable Memtables:     %s\n", numImmutableMemtables)
	}

	backgroundErrors := db.GetProperty("rocksdb.background-errors")
	if backgroundErrors != "" && backgroundErrors != "0" {
		fmt.Printf("⚠️  Background Errors:    %s\n", backgroundErrors)
	}

	fmt.Println()

	// Write amplification
	fmt.Println("📈 AMPLIFICATION METRICS")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	bytesWritten := getPropertyInt(db, "rocksdb.bytes-written")
	compactionBytesWritten := getPropertyInt(db, "rocksdb.compact-write-bytes")

	if bytesWritten > 0 {
		fmt.Printf("Total Bytes Written:     %s\n", formatBytes(bytesWritten))
		fmt.Printf("Compaction Bytes:        %s\n", formatBytes(compactionBytesWritten))

		if bytesWritten > 0 {
			writeAmp := float64(bytesWritten+compactionBytesWritten) / float64(bytesWritten)
			fmt.Printf("Write Amplification:     %.2fx\n", writeAmp)
		}
	}

	if totalSSTSize > 0 && liveSSTSize > 0 {
		spaceAmp := float64(totalSSTSize) / float64(liveSSTSize)
		fmt.Printf("Space Amplification:     %.2fx\n", spaceAmp)
	}

	fmt.Println()

	if detailed {
		// Additional detailed stats
		fmt.Println("📋 DETAILED STATISTICS")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

		detailedStats := map[string]string{
			"rocksdb.num-entries-active-mem-table":      "Active Memtable Entries",
			"rocksdb.num-entries-imm-mem-tables":        "Immutable Memtable Entries",
			"rocksdb.num-deletes-active-mem-table":      "Active Memtable Deletes",
			"rocksdb.num-deletes-imm-mem-tables":        "Immutable Memtable Deletes",
			"rocksdb.estimate-oldest-key-time":          "Oldest Key Time",
			"rocksdb.min-log-number-to-keep":            "Min Log Number to Keep",
			"rocksdb.actual-delayed-write-rate":         "Delayed Write Rate",
			"rocksdb.is-write-stopped":                  "Write Stopped",
			"rocksdb.estimate-pending-compaction-bytes": "Pending Compaction Bytes",
		}

		for prop, label := range detailedStats {
			value := db.GetProperty(prop)
			if value != "" && value != "0" {
				// Try to format as bytes if it's a size metric
				if strings.Contains(prop, "bytes") {
					if val, err := strconv.ParseInt(value, 10, 64); err == nil {
						fmt.Printf("%-35s %s\n", label+":", formatBytes(val))
						continue
					}
				}
				fmt.Printf("%-35s %s\n", label+":", value)
			}
		}

		fmt.Println()

		// Blob file stats (if using BlobDB)
		totalBlobSize := getPropertyInt(db, "rocksdb.total-blob-file-size")
		if totalBlobSize > 0 {
			fmt.Println("🗂️  BLOB FILES")
			fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			liveBlobSize := getPropertyInt(db, "rocksdb.live-blob-file-size")
			numBlobFiles := getPropertyInt(db, "rocksdb.num-blob-files")

			fmt.Printf("Total Blob Size:         %s\n", formatBytes(totalBlobSize))
			fmt.Printf("Live Blob Size:          %s\n", formatBytes(liveBlobSize))
			fmt.Printf("Number of Blob Files:    %s\n", formatNumber(numBlobFiles))
			fmt.Println()
		}
	}
}

func analyzeAndRecommend(db *grocksdb.DB) {
	recommendations := []string{}

	// Check L0 files
	l0Files := getPropertyInt(db, "rocksdb.num-files-at-level0")
	if l0Files > 50 {
		recommendations = append(recommendations,
			fmt.Sprintf("⚠️  HIGH L0 FILE COUNT (%d files): Consider increasing write_buffer_size or max_write_buffer_number to reduce flush frequency", l0Files))
	} else if l0Files > 20 {
		recommendations = append(recommendations,
			fmt.Sprintf("⚡ ELEVATED L0 FILE COUNT (%d files): Monitor during ingestion, may need tuning", l0Files))
	} else {
		recommendations = append(recommendations,
			fmt.Sprintf("✅ L0 FILE COUNT HEALTHY (%d files)", l0Files))
	}

	// Check obsolete data ratio
	totalSSTSize := getPropertyInt(db, "rocksdb.total-sst-files-size")
	liveSSTSize := getPropertyInt(db, "rocksdb.estimate-live-data-size")
	if totalSSTSize > 0 && liveSSTSize > 0 {
		obsoleteRatio := float64(totalSSTSize-liveSSTSize) / float64(totalSSTSize) * 100
		if obsoleteRatio > 30 {
			recommendations = append(recommendations,
				fmt.Sprintf("⚠️  HIGH OBSOLETE DATA (%.1f%%): Run full compaction to reclaim space", obsoleteRatio))
		} else if obsoleteRatio > 15 {
			recommendations = append(recommendations,
				fmt.Sprintf("⚡ MODERATE OBSOLETE DATA (%.1f%%): Consider running compaction", obsoleteRatio))
		} else {
			recommendations = append(recommendations,
				fmt.Sprintf("✅ OBSOLETE DATA RATIO HEALTHY (%.1f%%)", obsoleteRatio))
		}
	}

	// Check space amplification
	if totalSSTSize > 0 && liveSSTSize > 0 {
		spaceAmp := float64(totalSSTSize) / float64(liveSSTSize)
		if spaceAmp > 2.0 {
			recommendations = append(recommendations,
				fmt.Sprintf("⚠️  HIGH SPACE AMPLIFICATION (%.2fx): Too much wasted space, run compaction", spaceAmp))
		} else if spaceAmp > 1.5 {
			recommendations = append(recommendations,
				fmt.Sprintf("⚡ MODERATE SPACE AMPLIFICATION (%.2fx): Consider periodic compaction", spaceAmp))
		} else {
			recommendations = append(recommendations,
				fmt.Sprintf("✅ SPACE AMPLIFICATION HEALTHY (%.2fx)", spaceAmp))
		}
	}

	// Check memtable size
	memtableSize := getPropertyInt(db, "rocksdb.cur-size-all-mem-tables")
	if memtableSize > 2*1024*1024*1024 { // > 2GB
		recommendations = append(recommendations,
			fmt.Sprintf("⚡ LARGE MEMTABLE SIZE (%s): This is normal during bulk ingestion", formatBytes(memtableSize)))
	}

	// Check compaction pending
	pendingCompactionBytes := getPropertyInt(db, "rocksdb.estimate-pending-compaction-bytes")
	if pendingCompactionBytes > 10*1024*1024*1024 { // > 10GB
		recommendations = append(recommendations,
			fmt.Sprintf("⚠️  LARGE PENDING COMPACTION (%s): May cause write stalls", formatBytes(pendingCompactionBytes)))
	}

	// Check if write stopped
	writeStopped := db.GetProperty("rocksdb.is-write-stopped")
	if writeStopped == "1" {
		recommendations = append(recommendations,
			"🛑 WRITES ARE STOPPED: L0 file limit reached or other critical condition")
	}

	// Check background errors
	bgErrors := db.GetProperty("rocksdb.background-errors")
	if bgErrors != "" && bgErrors != "0" {
		recommendations = append(recommendations,
			fmt.Sprintf("🛑 BACKGROUND ERRORS DETECTED (%s): Check RocksDB logs immediately", bgErrors))
	}

	// Check total files
	totalFiles := int64(0)
	for i := 0; i <= 6; i++ {
		totalFiles += getPropertyInt(db, fmt.Sprintf("rocksdb.num-files-at-level%d", i))
	}
	if totalFiles > 1000 {
		recommendations = append(recommendations,
			fmt.Sprintf("⚡ HIGH TOTAL FILE COUNT (%s files): Consider increasing target_file_size_base", formatNumber(totalFiles)))
	}

	// Print recommendations
	if len(recommendations) == 0 {
		fmt.Println("✅ No issues detected. Database looks healthy!")
	} else {
		for _, rec := range recommendations {
			fmt.Println(rec)
		}
	}

	fmt.Println()
	fmt.Println("💡 GENERAL OPTIMIZATION TIPS:")
	fmt.Println("   • Increase write_buffer_size (512MB+) to reduce L0 flushes")
	fmt.Println("   • Set Level0FileNumCompactionTrigger to 50+ for bulk loads")
	fmt.Println("   • Use DisableWAL(true) in write options during ingestion")
	fmt.Println("   • Run manual compaction after bulk ingestion")
	fmt.Println("   • Monitor L0 file count during ingestion (should stay < 100)")
}

func getPropertyInt(db *grocksdb.DB, property string) int64 {
	value := db.GetProperty(property)
	if value == "" {
		return 0
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return val
}

func formatBytes(bytes int64) string {
	if bytes < 0 {
		return "0 B"
	}

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

func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}

	in := strconv.FormatInt(n, 10)
	numOfDigits := len(in)
	if numOfDigits <= 3 {
		return in
	}

	out := make([]byte, numOfDigits+(numOfDigits-1)/3)
	for i, j, k := numOfDigits-1, len(out)-1, 0; ; i, j = i-1, j-1 {
		out[j] = in[i]
		if i == 0 {
			return string(out)
		}
		if k++; k == 3 {
			j, k = j-1, 0
			out[j] = ','
		}
	}
}

func getDirSize(path string) int64 {
	var size int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}
