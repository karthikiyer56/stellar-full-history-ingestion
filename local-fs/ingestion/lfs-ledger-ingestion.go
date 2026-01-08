// main.go
// =============================================================================
// File-Based Ledger Storage Ingestion Tool
// =============================================================================
//
// This tool ingests Stellar LedgerCloseMeta from GCS and stores them in a
// file-based chunk storage format as an alternative to RocksDB.
//
// Storage Format:
// - Each chunk contains exactly 10,000 ledgers
// - Chunk N contains ledger sequences: (N * 10000) + 2 to ((N + 1) * 10000) + 1
// - Each chunk has two files:
//   - NNNNNN.data: Concatenated zstd-compressed LCM records
//   - NNNNNN.index: Byte offsets into the data file
//
// Directory Layout:
//   <data_dir>/chunks/XXXX/YYYYYY.data
//   <data_dir>/chunks/XXXX/YYYYYY.index
//   Where: XXXX = chunk_id / 1000, YYYYYY = chunk_id
//
// USAGE:
// ======
// ./file_based_ingestion \
//     --data-dir /path/to/storage \
//     --start-ledger 2 \
//     --end-ledger 5000001 \
//     [--force]
//
// =============================================================================

package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// ChunkSize is the fixed number of ledgers per chunk
	ChunkSize = 10000

	// FirstLedgerSequence is the first ledger in the Stellar blockchain
	FirstLedgerSequence = 2

	// IndexHeaderSize is the size of the index file header in bytes
	IndexHeaderSize = 8

	// IndexVersion is the current index file format version
	IndexVersion = 1

	// GCS Configuration
	GCSBucketPath = "sdf-ledger-close-meta/v1/ledgers/pubnet"
	GCSBufferSize = 10000
	GCSNumWorkers = 200
	GCSRetryLimit = 3
	GCSRetryWait  = 5 * time.Second
)

// =============================================================================
// Data Structures
// =============================================================================

// IngestionConfig holds the configuration for ingestion
type IngestionConfig struct {
	DataDir        string
	StartLedger    uint32
	EndLedger      uint32
	StartChunk     uint32
	EndChunk       uint32
	ForceOverwrite bool
}

// ChunkStats tracks statistics for a single chunk
type ChunkStats struct {
	ChunkID           uint32
	LedgerCount       int
	UncompressedBytes int64
	CompressedBytes   int64
	Duration          time.Duration
	FetchTime         time.Duration
	CompressTime      time.Duration
	WriteTime         time.Duration
}

// GlobalStats tracks cumulative statistics
type GlobalStats struct {
	mu                     sync.Mutex
	StartTime              time.Time
	TotalLedgers           int64
	TotalChunks            int64
	TotalUncompressedBytes int64
	TotalCompressedBytes   int64
	TotalFetchTime         time.Duration
	TotalCompressTime      time.Duration
	TotalWriteTime         time.Duration
}

// =============================================================================
// Main Entry Point
// =============================================================================

func main() {
	// Parse command-line arguments
	var (
		dataDir        string
		startLedger    uint
		endLedger      uint
		forceOverwrite bool
		showChunks     bool
	)

	flag.StringVar(&dataDir, "data-dir", "", "Root directory for ledger storage (required)")
	flag.UintVar(&startLedger, "start-ledger", 0, "Starting ledger sequence (must be chunk boundary)")
	flag.UintVar(&endLedger, "end-ledger", 0, "Ending ledger sequence (must be chunk boundary)")
	flag.BoolVar(&forceOverwrite, "force", false, "Overwrite existing chunks")
	flag.BoolVar(&showChunks, "show-chunks", false, "Show chunk boundaries for the given range and exit")

	flag.Parse()

	// Validate required arguments
	if dataDir == "" {
		log.Fatal("ERROR: --data-dir is required")
	}
	if startLedger == 0 || endLedger == 0 {
		log.Fatal("ERROR: --start-ledger and --end-ledger are required")
	}

	// Handle --show-chunks mode
	if showChunks {
		showChunkInfo(uint32(startLedger), uint32(endLedger))
		return
	}

	// Validate chunk boundaries
	config, err := validateAndCreateConfig(dataDir, uint32(startLedger), uint32(endLedger), forceOverwrite)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}

	// Print configuration summary
	printConfigSummary(config)

	// Run ingestion
	if err := runIngestion(config); err != nil {
		log.Fatalf("ERROR: Ingestion failed: %v", err)
	}

	log.Printf("Ingestion completed successfully!")
}

// =============================================================================
// Validation Functions
// =============================================================================

// validateAndCreateConfig validates the input parameters and creates the config
func validateAndCreateConfig(dataDir string, startLedger, endLedger uint32, force bool) (*IngestionConfig, error) {
	// Rule 1: startLedger must be >= FirstLedgerSequence
	if startLedger < FirstLedgerSequence {
		return nil, fmt.Errorf("start-ledger must be >= %d, got %d", FirstLedgerSequence, startLedger)
	}

	// Rule 2: endLedger must be >= startLedger
	if endLedger < startLedger {
		return nil, fmt.Errorf("end-ledger (%d) must be >= start-ledger (%d)", endLedger, startLedger)
	}

	// Rule 3: startLedger must be the first ledger of a chunk
	startChunk := ledgerToChunkID(startLedger)
	expectedStart := chunkFirstLedger(startChunk)
	if startLedger != expectedStart {
		return nil, fmt.Errorf(
			"start-ledger %d is not a chunk boundary\n"+
				"       Chunk %d starts at ledger %d\n"+
				"       Did you mean: --start-ledger %d",
			startLedger, startChunk, expectedStart, expectedStart)
	}

	// Rule 4: endLedger must be the last ledger of a chunk
	endChunk := ledgerToChunkID(endLedger)
	expectedEnd := chunkLastLedger(endChunk)
	if endLedger != expectedEnd {
		return nil, fmt.Errorf(
			"end-ledger %d is not a chunk boundary\n"+
				"       Chunk %d ends at ledger %d\n"+
				"       Did you mean: --end-ledger %d",
			endLedger, endChunk, expectedEnd, expectedEnd)
	}

	// Create absolute path for data directory
	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for data-dir: %w", err)
	}

	return &IngestionConfig{
		DataDir:        absDataDir,
		StartLedger:    startLedger,
		EndLedger:      endLedger,
		StartChunk:     startChunk,
		EndChunk:       endChunk,
		ForceOverwrite: force,
	}, nil
}

// showChunkInfo displays chunk boundary information for a given range
func showChunkInfo(startLedger, endLedger uint32) {
	fmt.Printf("\n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("                         CHUNK BOUNDARY INFORMATION\n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("\n")
	fmt.Printf("Requested range: %d to %d\n", startLedger, endLedger)
	fmt.Printf("Chunk size: %d ledgers\n", ChunkSize)
	fmt.Printf("\n")

	if startLedger < FirstLedgerSequence {
		fmt.Printf("ERROR: start-ledger must be >= %d\n", FirstLedgerSequence)
		return
	}

	startChunk := ledgerToChunkID(startLedger)
	endChunk := ledgerToChunkID(endLedger)

	expectedStart := chunkFirstLedger(startChunk)
	expectedEnd := chunkLastLedger(endChunk)

	startAligned := startLedger == expectedStart
	endAligned := endLedger == expectedEnd

	fmt.Printf("Start chunk: %d (ledgers %d to %d)\n", startChunk, chunkFirstLedger(startChunk), chunkLastLedger(startChunk))
	fmt.Printf("End chunk:   %d (ledgers %d to %d)\n", endChunk, chunkFirstLedger(endChunk), chunkLastLedger(endChunk))
	fmt.Printf("\n")

	if startAligned && endAligned {
		totalChunks := endChunk - startChunk + 1
		totalLedgers := endLedger - startLedger + 1
		fmt.Printf("✓ Range is aligned to chunk boundaries!\n")
		fmt.Printf("  Total chunks: %d\n", totalChunks)
		fmt.Printf("  Total ledgers: %s\n", helpers.FormatNumber(int64(totalLedgers)))
		fmt.Printf("\n")
		fmt.Printf("Command:\n")
		fmt.Printf("  ./file_based_ingestion --data-dir <path> --start-ledger %d --end-ledger %d\n", startLedger, endLedger)
	} else {
		fmt.Printf("✗ Range is NOT aligned to chunk boundaries\n")
		fmt.Printf("\n")

		if !startAligned {
			fmt.Printf("  Start issue: %d is not chunk boundary, chunk %d starts at %d\n",
				startLedger, startChunk, expectedStart)
		}
		if !endAligned {
			fmt.Printf("  End issue: %d is not chunk boundary, chunk %d ends at %d\n",
				endLedger, endChunk, expectedEnd)
		}

		fmt.Printf("\n")
		fmt.Printf("To ingest complete chunks only:\n")
		fmt.Printf("  ./file_based_ingestion --data-dir <path> --start-ledger %d --end-ledger %d\n",
			expectedStart, expectedEnd)

		// Also show option for one less chunk at the end if it's a partial
		if !endAligned && endChunk > startChunk {
			prevEnd := chunkLastLedger(endChunk - 1)
			fmt.Printf("\n")
			fmt.Printf("Or, to exclude the partial end chunk:\n")
			fmt.Printf("  ./file_based_ingestion --data-dir <path> --start-ledger %d --end-ledger %d\n",
				expectedStart, prevEnd)
		}
	}
	fmt.Printf("\n")
}

// =============================================================================
// Chunk ID / Ledger Sequence Calculations
// =============================================================================

// ledgerToChunkID returns the chunk ID for a given ledger sequence
func ledgerToChunkID(ledgerSeq uint32) uint32 {
	return (ledgerSeq - FirstLedgerSequence) / ChunkSize
}

// chunkFirstLedger returns the first ledger sequence in a chunk
func chunkFirstLedger(chunkID uint32) uint32 {
	return (chunkID * ChunkSize) + FirstLedgerSequence
}

// chunkLastLedger returns the last ledger sequence in a chunk
func chunkLastLedger(chunkID uint32) uint32 {
	return ((chunkID + 1) * ChunkSize) + FirstLedgerSequence - 1
}

// ledgerToLocalIndex returns the local index within a chunk for a given ledger
func ledgerToLocalIndex(ledgerSeq uint32) uint32 {
	return (ledgerSeq - FirstLedgerSequence) % ChunkSize
}

// =============================================================================
// File Path Functions
// =============================================================================

// getChunkDir returns the directory path for a chunk
func getChunkDir(dataDir string, chunkID uint32) string {
	parentDir := chunkID / 1000
	return filepath.Join(dataDir, "chunks", fmt.Sprintf("%04d", parentDir))
}

// getDataPath returns the data file path for a chunk
func getDataPath(dataDir string, chunkID uint32) string {
	return filepath.Join(getChunkDir(dataDir, chunkID), fmt.Sprintf("%06d.data", chunkID))
}

// getIndexPath returns the index file path for a chunk
func getIndexPath(dataDir string, chunkID uint32) string {
	return filepath.Join(getChunkDir(dataDir, chunkID), fmt.Sprintf("%06d.index", chunkID))
}

// chunkExists checks if a chunk already exists (both files present)
func chunkExists(dataDir string, chunkID uint32) bool {
	dataPath := getDataPath(dataDir, chunkID)
	indexPath := getIndexPath(dataDir, chunkID)

	_, dataErr := os.Stat(dataPath)
	_, indexErr := os.Stat(indexPath)

	return dataErr == nil && indexErr == nil
}

// =============================================================================
// Configuration Summary
// =============================================================================

func printConfigSummary(config *IngestionConfig) {
	totalChunks := config.EndChunk - config.StartChunk + 1
	totalLedgers := config.EndLedger - config.StartLedger + 1

	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("                   FILE-BASED LEDGER INGESTION CONFIGURATION")
	log.Printf("================================================================================")
	log.Printf("")
	log.Printf("DATA DIRECTORY:")
	log.Printf("  Path:                %s", config.DataDir)
	log.Printf("")
	log.Printf("LEDGER RANGE:")
	log.Printf("  Start Ledger:        %d", config.StartLedger)
	log.Printf("  End Ledger:          %d", config.EndLedger)
	log.Printf("  Total Ledgers:       %s", helpers.FormatNumber(int64(totalLedgers)))
	log.Printf("")
	log.Printf("CHUNK RANGE:")
	log.Printf("  Start Chunk:         %d", config.StartChunk)
	log.Printf("  End Chunk:           %d", config.EndChunk)
	log.Printf("  Total Chunks:        %d", totalChunks)
	log.Printf("  Chunk Size:          %d ledgers", ChunkSize)
	log.Printf("")
	log.Printf("OPTIONS:")
	log.Printf("  Force Overwrite:     %v", config.ForceOverwrite)
	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("")
}

// =============================================================================
// Ingestion Logic
// =============================================================================

func runIngestion(config *IngestionConfig) error {
	ctx := context.Background()

	// Initialize global stats
	globalStats := &GlobalStats{
		StartTime: time.Now(),
	}

	// Initialize GCS data source
	log.Printf("Initializing GCS data source...")

	datastoreConfig := datastore.DataStoreConfig{
		Type: "GCS",
		Params: map[string]string{
			"destination_bucket_path": GCSBucketPath,
		},
	}

	dataStoreSchema := datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 64000,
	}

	dataStore, err := datastore.NewDataStore(ctx, datastoreConfig)
	if err != nil {
		return errors.Wrap(err, "failed to create GCS datastore")
	}
	defer dataStore.Close()

	backendConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: GCSBufferSize,
		NumWorkers: GCSNumWorkers,
		RetryLimit: GCSRetryLimit,
		RetryWait:  GCSRetryWait,
	}

	backend, err := ledgerbackend.NewBufferedStorageBackend(backendConfig, dataStore, dataStoreSchema)
	if err != nil {
		return errors.Wrap(err, "failed to create buffered storage backend")
	}
	defer backend.Close()

	// Prepare the ledger range
	ledgerRange := ledgerbackend.BoundedRange(config.StartLedger, config.EndLedger)
	if err := backend.PrepareRange(ctx, ledgerRange); err != nil {
		return errors.Wrapf(err, "failed to prepare ledger range: %v", ledgerRange)
	}

	log.Printf("✓ GCS data source initialized")
	log.Printf("")

	// Create zstd encoder (reused across chunks)
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return errors.Wrap(err, "failed to create zstd encoder")
	}
	defer encoder.Close()

	// Process chunks
	totalChunks := config.EndChunk - config.StartChunk + 1
	chunksProcessed := uint32(0)
	chunksSkipped := uint32(0)
	lastReportedPercent := -1

	log.Printf("================================================================================")
	log.Printf("                          STARTING INGESTION")
	log.Printf("================================================================================")
	log.Printf("")

	for chunkID := config.StartChunk; chunkID <= config.EndChunk; chunkID++ {
		// Check if chunk already exists
		if chunkExists(config.DataDir, chunkID) && !config.ForceOverwrite {
			log.Printf("Chunk %d already exists, skipping (use --force to overwrite)", chunkID)
			chunksSkipped++
			continue
		}

		// Process this chunk
		chunkStats, err := processChunk(ctx, backend, encoder, config, chunkID)
		if err != nil {
			return errors.Wrapf(err, "failed to process chunk %d", chunkID)
		}

		// Update global stats
		globalStats.mu.Lock()
		globalStats.TotalLedgers += int64(chunkStats.LedgerCount)
		globalStats.TotalChunks++
		globalStats.TotalUncompressedBytes += chunkStats.UncompressedBytes
		globalStats.TotalCompressedBytes += chunkStats.CompressedBytes
		globalStats.TotalFetchTime += chunkStats.FetchTime
		globalStats.TotalCompressTime += chunkStats.CompressTime
		globalStats.TotalWriteTime += chunkStats.WriteTime
		globalStats.mu.Unlock()

		chunksProcessed++

		// Log progress at each percentage point
		currentPercent := int((chunksProcessed * 100) / totalChunks)
		if currentPercent > lastReportedPercent {
			logProgress(globalStats, config, chunksProcessed, chunksSkipped, totalChunks, currentPercent)
			lastReportedPercent = currentPercent
		}
	}

	// Final summary
	logFinalSummary(globalStats, config, chunksProcessed, chunksSkipped)

	return nil
}

// processChunk processes a single chunk of ledgers
func processChunk(
	ctx context.Context,
	backend *ledgerbackend.BufferedStorageBackend,
	encoder *zstd.Encoder,
	config *IngestionConfig,
	chunkID uint32,
) (*ChunkStats, error) {
	stats := &ChunkStats{
		ChunkID: chunkID,
	}
	chunkStart := time.Now()

	firstLedger := chunkFirstLedger(chunkID)
	lastLedger := chunkLastLedger(chunkID)

	// Ensure chunk directory exists
	chunkDir := getChunkDir(config.DataDir, chunkID)
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		return nil, errors.Wrapf(err, "failed to create chunk directory: %s", chunkDir)
	}

	// Prepare to collect compressed data and offsets
	var compressedRecords [][]byte
	offsets := make([]uint64, 0, ChunkSize+1)
	currentOffset := uint64(0)
	offsets = append(offsets, currentOffset)

	// Fetch and compress each ledger
	fetchStart := time.Now()
	var compressTime time.Duration

	for ledgerSeq := firstLedger; ledgerSeq <= lastLedger; ledgerSeq++ {
		// Fetch ledger from GCS
		ledger, err := backend.GetLedger(ctx, ledgerSeq)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to fetch ledger %d", ledgerSeq)
		}

		// Marshal to XDR
		lcmBytes, err := ledger.MarshalBinary()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to marshal ledger %d", ledgerSeq)
		}

		stats.UncompressedBytes += int64(len(lcmBytes))

		// Compress with zstd
		compressStart := time.Now()
		compressed := encoder.EncodeAll(lcmBytes, make([]byte, 0, len(lcmBytes)))
		compressTime += time.Since(compressStart)

		stats.CompressedBytes += int64(len(compressed))
		stats.LedgerCount++

		// Track offset
		currentOffset += uint64(len(compressed))
		offsets = append(offsets, currentOffset)

		compressedRecords = append(compressedRecords, compressed)
	}

	stats.FetchTime = time.Since(fetchStart) - compressTime
	stats.CompressTime = compressTime

	// Write data file
	writeStart := time.Now()

	dataPath := getDataPath(config.DataDir, chunkID)
	if err := writeDataFile(dataPath, compressedRecords); err != nil {
		return nil, errors.Wrapf(err, "failed to write data file for chunk %d", chunkID)
	}

	// Write index file
	indexPath := getIndexPath(config.DataDir, chunkID)
	if err := writeIndexFile(indexPath, offsets); err != nil {
		// Clean up data file if index write fails
		os.Remove(dataPath)
		return nil, errors.Wrapf(err, "failed to write index file for chunk %d", chunkID)
	}

	stats.WriteTime = time.Since(writeStart)
	stats.Duration = time.Since(chunkStart)

	return stats, nil
}

// =============================================================================
// File Writing Functions
// =============================================================================

// writeDataFile writes the concatenated compressed records to the data file
func writeDataFile(path string, records [][]byte) error {
	// Calculate total size
	totalSize := 0
	for _, record := range records {
		totalSize += len(record)
	}

	// Create file
	file, err := os.Create(path)
	if err != nil {
		return errors.Wrap(err, "failed to create data file")
	}
	defer file.Close()

	// Write all records
	for _, record := range records {
		if _, err := file.Write(record); err != nil {
			return errors.Wrap(err, "failed to write record to data file")
		}
	}

	return nil
}

// writeIndexFile writes the index file with header and offsets
func writeIndexFile(path string, offsets []uint64) error {
	// Determine offset size based on final data file size
	finalOffset := offsets[len(offsets)-1]
	offsetSize := uint8(4) // Use 4 bytes (u32) by default
	if finalOffset > 0xFFFFFFFF {
		offsetSize = 8 // Use 8 bytes (u64) for large files
	}

	// Create file
	file, err := os.Create(path)
	if err != nil {
		return errors.Wrap(err, "failed to create index file")
	}
	defer file.Close()

	// Write header (8 bytes)
	header := make([]byte, IndexHeaderSize)
	header[0] = IndexVersion // version
	header[1] = offsetSize   // offset_size
	// bytes 2-7 are reserved (zero)

	if _, err := file.Write(header); err != nil {
		return errors.Wrap(err, "failed to write index header")
	}

	// Write offsets (little-endian)
	for _, offset := range offsets {
		var buf []byte
		if offsetSize == 4 {
			buf = make([]byte, 4)
			binary.LittleEndian.PutUint32(buf, uint32(offset))
		} else {
			buf = make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, offset)
		}
		if _, err := file.Write(buf); err != nil {
			return errors.Wrap(err, "failed to write offset to index file")
		}
	}

	return nil
}

// =============================================================================
// Logging Functions
// =============================================================================

func logProgress(
	stats *GlobalStats,
	config *IngestionConfig,
	chunksProcessed, chunksSkipped, totalChunks uint32,
	currentPercent int,
) {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	elapsed := time.Since(stats.StartTime)
	chunksRemaining := totalChunks - chunksProcessed - chunksSkipped

	var eta time.Duration
	if chunksProcessed > 0 {
		avgTimePerChunk := elapsed / time.Duration(chunksProcessed)
		eta = avgTimePerChunk * time.Duration(chunksRemaining)
	}

	ledgersPerSec := float64(0)
	if elapsed.Seconds() > 0 {
		ledgersPerSec = float64(stats.TotalLedgers) / elapsed.Seconds()
	}

	compressionRatio := float64(0)
	if stats.TotalUncompressedBytes > 0 {
		compressionRatio = 100.0 * (1.0 - float64(stats.TotalCompressedBytes)/float64(stats.TotalUncompressedBytes))
	}

	avgUncompressedSize := float64(0)
	avgCompressedSize := float64(0)
	if stats.TotalLedgers > 0 {
		avgUncompressedSize = float64(stats.TotalUncompressedBytes) / float64(stats.TotalLedgers)
		avgCompressedSize = float64(stats.TotalCompressedBytes) / float64(stats.TotalLedgers)
	}

	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("    PROGRESS: %d%% | Chunk %d/%d | %s ledgers processed",
		currentPercent,
		chunksProcessed,
		totalChunks,
		helpers.FormatNumber(stats.TotalLedgers))
	if chunksSkipped > 0 {
		log.Printf("    Skipped: %d chunks (already exist)", chunksSkipped)
	}
	log.Printf("    Rate: %.2f ledgers/sec | Compression: %.1f%% | ETA: %s",
		ledgersPerSec, compressionRatio, helpers.FormatDuration(eta))
	log.Printf("    Data: %s uncompressed → %s compressed",
		helpers.FormatBytes(stats.TotalUncompressedBytes),
		helpers.FormatBytes(stats.TotalCompressedBytes))
	log.Printf("    Avg Ledger Size: %.2f KB uncompressed → %.2f KB compressed",
		avgUncompressedSize/1024, avgCompressedSize/1024)
	log.Printf("================================================================================")
	log.Printf("")
}

func logFinalSummary(stats *GlobalStats, config *IngestionConfig, chunksProcessed, chunksSkipped uint32) {
	stats.mu.Lock()
	defer stats.mu.Unlock()

	elapsed := time.Since(stats.StartTime)

	compressionRatio := float64(0)
	if stats.TotalUncompressedBytes > 0 {
		compressionRatio = 100.0 * (1.0 - float64(stats.TotalCompressedBytes)/float64(stats.TotalUncompressedBytes))
	}

	ledgersPerSec := float64(0)
	if elapsed.Seconds() > 0 {
		ledgersPerSec = float64(stats.TotalLedgers) / elapsed.Seconds()
	}

	log.Printf("")
	log.Printf("################################################################################")
	log.Printf("################################################################################")
	log.Printf("                           INGESTION COMPLETE")
	log.Printf("################################################################################")
	log.Printf("################################################################################")
	log.Printf("")
	log.Printf("SUMMARY:")
	log.Printf("  Total Time:              %s", helpers.FormatDuration(elapsed))
	log.Printf("  Chunks Processed:        %d", chunksProcessed)
	if chunksSkipped > 0 {
		log.Printf("  Chunks Skipped:          %d", chunksSkipped)
	}
	log.Printf("  Ledgers Processed:       %s", helpers.FormatNumber(stats.TotalLedgers))
	log.Printf("  Processing Rate:         %.2f ledgers/sec", ledgersPerSec)
	log.Printf("")
	log.Printf("STORAGE:")
	log.Printf("  Uncompressed Size:       %s", helpers.FormatBytes(stats.TotalUncompressedBytes))
	log.Printf("  Compressed Size:         %s", helpers.FormatBytes(stats.TotalCompressedBytes))
	log.Printf("  Compression Ratio:       %.1f%% reduction", compressionRatio)

	avgUncompressedSize := float64(0)
	avgCompressedSize := float64(0)
	if stats.TotalLedgers > 0 {
		avgUncompressedSize = float64(stats.TotalUncompressedBytes) / float64(stats.TotalLedgers)
		avgCompressedSize = float64(stats.TotalCompressedBytes) / float64(stats.TotalLedgers)
	}
	log.Printf("  Avg Ledger Size:         %.2f KB uncompressed → %.2f KB compressed",
		avgUncompressedSize/1024, avgCompressedSize/1024)
	log.Printf("")
	log.Printf("TIMING BREAKDOWN:")
	log.Printf("  Fetch Time:              %s (%.1f%%)",
		helpers.FormatDuration(stats.TotalFetchTime),
		100*stats.TotalFetchTime.Seconds()/elapsed.Seconds())
	log.Printf("  Compress Time:           %s (%.1f%%)",
		helpers.FormatDuration(stats.TotalCompressTime),
		100*stats.TotalCompressTime.Seconds()/elapsed.Seconds())
	log.Printf("  Write Time:              %s (%.1f%%)",
		helpers.FormatDuration(stats.TotalWriteTime),
		100*stats.TotalWriteTime.Seconds()/elapsed.Seconds())
	log.Printf("")
	log.Printf("OUTPUT LOCATION:")
	log.Printf("  Data Directory:          %s", config.DataDir)
	log.Printf("  Chunk Range:             %d to %d", config.StartChunk, config.EndChunk)
	log.Printf("")
	log.Printf("################################################################################")
	log.Printf("################################################################################")
	log.Printf("")
}

// =============================================================================
// Reading Functions (for verification/future use)
// =============================================================================

// ReadLedger reads a single ledger from the file-based storage
func ReadLedger(dataDir string, ledgerSeq uint32) (xdr.LedgerCloseMeta, error) {
	var lcm xdr.LedgerCloseMeta

	chunkID := ledgerToChunkID(ledgerSeq)
	localIndex := ledgerToLocalIndex(ledgerSeq)

	// Read index file header
	indexPath := getIndexPath(dataDir, chunkID)
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return lcm, errors.Wrap(err, "failed to open index file")
	}
	defer indexFile.Close()

	// Read header
	header := make([]byte, IndexHeaderSize)
	if _, err := indexFile.ReadAt(header, 0); err != nil {
		return lcm, errors.Wrap(err, "failed to read index header")
	}

	version := header[0]
	offsetSize := header[1]

	if version != IndexVersion {
		return lcm, fmt.Errorf("unsupported index version: %d", version)
	}

	if offsetSize != 4 && offsetSize != 8 {
		return lcm, fmt.Errorf("invalid offset size: %d", offsetSize)
	}

	// Read two adjacent offsets
	entryPos := int64(IndexHeaderSize) + int64(localIndex)*int64(offsetSize)
	offsetBuf := make([]byte, offsetSize*2)
	if _, err := indexFile.ReadAt(offsetBuf, entryPos); err != nil {
		return lcm, errors.Wrap(err, "failed to read offsets from index")
	}

	var startOffset, endOffset uint64
	if offsetSize == 4 {
		startOffset = uint64(binary.LittleEndian.Uint32(offsetBuf[0:4]))
		endOffset = uint64(binary.LittleEndian.Uint32(offsetBuf[4:8]))
	} else {
		startOffset = binary.LittleEndian.Uint64(offsetBuf[0:8])
		endOffset = binary.LittleEndian.Uint64(offsetBuf[8:16])
	}

	recordSize := endOffset - startOffset

	// Read compressed data from data file
	dataPath := getDataPath(dataDir, chunkID)
	dataFile, err := os.Open(dataPath)
	if err != nil {
		return lcm, errors.Wrap(err, "failed to open data file")
	}
	defer dataFile.Close()

	compressed := make([]byte, recordSize)
	if _, err := dataFile.ReadAt(compressed, int64(startOffset)); err != nil {
		return lcm, errors.Wrap(err, "failed to read compressed data")
	}

	// Decompress
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return lcm, errors.Wrap(err, "failed to create zstd decoder")
	}
	defer decoder.Close()

	uncompressed, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return lcm, errors.Wrap(err, "failed to decompress data")
	}

	// Unmarshal XDR
	if err := lcm.UnmarshalBinary(uncompressed); err != nil {
		return lcm, errors.Wrap(err, "failed to unmarshal LedgerCloseMeta")
	}

	return lcm, nil
}
