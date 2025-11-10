package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/linxGnu/grocksdb"
)

func main() {
	var finalPath string
	var sourcePaths string
	var verify bool

	flag.StringVar(&finalPath, "final-path", "", "Destination database path")
	flag.StringVar(&sourcePaths, "paths", "", "Space-separated source database paths")
	flag.BoolVar(&verify, "verify", false, "Verify non-overlapping keys before merge")
	flag.Parse()

	if finalPath == "" || sourcePaths == "" {
		log.Fatal("Usage: go run fast_merge.go -final-path /path/to/final -paths \"/path/db1 /path/db2\"")
	}

	sources := strings.Fields(sourcePaths)
	if len(sources) == 0 {
		log.Fatal("No source paths provided")
	}

	// Validate source paths
	for _, path := range sources {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			log.Fatalf("Source path does not exist: %s", path)
		}
	}

	fmt.Printf("\n========================================\n")
	fmt.Printf("Fast RocksDB Merge (SST Ingestion)\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Destination: %s\n", finalPath)
	fmt.Printf("Source DBs: %d\n", len(sources))
	for i, src := range sources {
		fmt.Printf("  [%d] %s\n", i+1, src)
	}
	fmt.Printf("========================================\n\n")

	startTime := time.Now()

	// Open or create destination database
	destDB, destOpts, err := openDestinationDB(finalPath)
	if err != nil {
		log.Fatalf("Failed to open destination DB: %v", err)
	}
	defer destDB.Close()
	defer destOpts.Destroy()

	log.Printf("✓ Destination database ready\n\n")

	// Optional: Verify non-overlapping key ranges
	if verify {
		log.Printf("Verifying non-overlapping key ranges...\n")
		if err := verifyNonOverlapping(sources); err != nil {
			log.Fatalf("Key range verification failed: %v", err)
		}
		log.Printf("✓ Verification passed - key ranges don't overlap\n\n")
	}

	// Ingest SST files from each source
	for i, srcPath := range sources {
		log.Printf("[%d/%d] Ingesting SST files from: %s\n", i+1, len(sources), srcPath)

		ingestStart := time.Now()
		filesIngested, err := ingestSourceDB(destDB, srcPath)
		if err != nil {
			log.Printf("Error ingesting %s: %v", srcPath, err)
			continue
		}

		log.Printf("✓ Ingested %d SST files in %s\n\n", filesIngested, formatDuration(time.Since(ingestStart)))
	}

	// Final compaction (optional but recommended)
	log.Printf("Performing final compaction...\n")
	compactionStart := time.Now()
	destDB.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
	log.Printf("✓ Compaction complete in %s\n\n", formatDuration(time.Since(compactionStart)))

	elapsed := time.Since(startTime)

	// Show final stats
	showStats(destDB)

	fmt.Printf("\n========================================\n")
	fmt.Printf("MERGE COMPLETE\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Total time: %s\n", formatDuration(elapsed))
	fmt.Printf("Method: SST File Ingestion\n")
	fmt.Printf("========================================\n\n")
}

// openDestinationDB opens or creates the destination database
func openDestinationDB(path string) (*grocksdb.DB, *grocksdb.Options, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create directory: %w", err)
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetErrorIfExists(false)

	// Settings for ingestion
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

// ingestSourceDB ingests all SST files from a source database
func ingestSourceDB(destDB *grocksdb.DB, srcPath string) (int, error) {
	// Create temporary directory for SST files
	tmpDir, err := os.MkdirTemp("", "rocksdb-ingest-*")
	if err != nil {
		return 0, fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// Find all SST files in source database
	sstFiles, err := findSSTFiles(srcPath)
	if err != nil {
		return 0, fmt.Errorf("failed to find SST files: %w", err)
	}

	if len(sstFiles) == 0 {
		return 0, fmt.Errorf("no SST files found in %s", srcPath)
	}

	log.Printf("  Found %d SST files to ingest\n", len(sstFiles))

	// Copy SST files to temp directory
	copiedFiles := make([]string, 0, len(sstFiles))
	for i, sstFile := range sstFiles {
		basename := filepath.Base(sstFile)
		destFile := filepath.Join(tmpDir, basename)

		if err := copyFile(sstFile, destFile); err != nil {
			return 0, fmt.Errorf("failed to copy %s: %w", sstFile, err)
		}

		copiedFiles = append(copiedFiles, destFile)

		if (i+1)%100 == 0 {
			log.Printf("  Copied %d/%d files...\n", i+1, len(sstFiles))
		}
	}

	log.Printf("  Copied %d files to temporary directory\n", len(copiedFiles))

	// Ingest files into destination database
	ingestOpts := grocksdb.NewDefaultIngestExternalFileOptions()
	ingestOpts.SetMoveFiles(true) // Move instead of copy (faster)
	defer ingestOpts.Destroy()

	log.Printf("  Ingesting files into destination database...\n")
	if err := destDB.IngestExternalFile(copiedFiles, ingestOpts); err != nil {
		return 0, fmt.Errorf("failed to ingest files: %w", err)
	}

	return len(copiedFiles), nil
}

// findSSTFiles finds all .sst files in a RocksDB directory
func findSSTFiles(dbPath string) ([]string, error) {
	var sstFiles []string

	err := filepath.Walk(dbPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".sst") {
			sstFiles = append(sstFiles, path)
		}
		return nil
	})

	return sstFiles, err
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}

// verifyNonOverlapping checks that source databases have non-overlapping key ranges
func verifyNonOverlapping(sources []string) error {
	type keyRange struct {
		first []byte
		last  []byte
	}

	ranges := make([]keyRange, 0, len(sources))

	for _, srcPath := range sources {
		opts := grocksdb.NewDefaultOptions()
		opts.SetCreateIfMissing(false)
		defer opts.Destroy()

		db, err := grocksdb.OpenDbForReadOnly(opts, srcPath, false)
		if err != nil {
			return fmt.Errorf("failed to open %s: %w", srcPath, err)
		}

		readOpts := grocksdb.NewDefaultReadOptions()
		defer readOpts.Destroy()

		// Get first key
		it := db.NewIterator(readOpts)
		it.SeekToFirst()
		if !it.Valid() {
			db.Close()
			it.Close()
			continue // Empty database
		}
		firstKey := it.Key()
		first := make([]byte, len(firstKey.Data()))
		copy(first, firstKey.Data())
		firstKey.Free()

		// Get last key
		it.SeekToLast()
		if !it.Valid() {
			db.Close()
			it.Close()
			continue
		}
		lastKey := it.Key()
		last := make([]byte, len(lastKey.Data()))
		copy(last, lastKey.Data())
		lastKey.Free()

		it.Close()
		db.Close()

		ranges = append(ranges, keyRange{first: first, last: last})
		log.Printf("  %s: keys from %x to %x\n", filepath.Base(srcPath), first[:min(8, len(first))], last[:min(8, len(last))])
	}

	// Check for overlaps
	for i := 0; i < len(ranges); i++ {
		for j := i + 1; j < len(ranges); j++ {
			// Check if ranges[i] and ranges[j] overlap
			if bytesCompare(ranges[i].last, ranges[j].first) >= 0 &&
				bytesCompare(ranges[j].last, ranges[i].first) >= 0 {
				return fmt.Errorf("databases %d and %d have overlapping key ranges", i+1, j+1)
			}
		}
	}

	return nil
}

// Helper functions
func bytesCompare(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func showStats(db *grocksdb.DB) {
	fmt.Println("\nFinal Database Statistics:")

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
	fmt.Printf("  Total: %d files\n\n", totalFiles)

	estimatedKeys := db.GetProperty("rocksdb.estimate-num-keys")
	fmt.Printf("Estimated Keys: %s\n", estimatedKeys)

	totalSSTSize := db.GetProperty("rocksdb.total-sst-files-size")
	fmt.Printf("Total Size: %s bytes (%.2f GB)\n", totalSSTSize, bytesToGB(totalSSTSize))
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
