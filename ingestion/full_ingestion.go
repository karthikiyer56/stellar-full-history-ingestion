package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/tx_data"
	"github.com/klauspost/compress/zstd"
	"github.com/linxGnu/grocksdb"
	"github.com/pkg/errors"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/xdr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
)

// IngestionConfig holds configuration for the ingestion process
type IngestionConfig struct {
	StartLedger       uint32
	EndLedger         uint32
	BatchSize         int
	DB1Path           string // Optional: ledgerSeq -> compressed LCM
	DB2Path           string // Optional: txHash -> compressed TxData
	DB3Path           string // Optional: txHash -> ledgerSeq
	EnableDB1         bool   // Derived: true if DB1Path is provided
	EnableDB2         bool   // Derived: true if DB2Path is provided
	EnableDB3         bool   // Derived: true if DB3Path is provided
	EnableCompression bool   // true to enable compression
}

// CompressionStats tracks compression metrics for a batch
type CompressionStats struct {
	UncompressedLCM int64
	CompressedLCM   int64
	UncompressedTx  int64
	CompressedTx    int64
	TxCount         int64
}

// DBTimingStats tracks timing for individual databases
type DBTimingStats struct {
	DB1Write   time.Duration
	DB2Write   time.Duration
	DB3Write   time.Duration
	DB1Flush   time.Duration
	DB2Flush   time.Duration
	DB3Flush   time.Duration
	DB1Compact time.Duration
	DB2Compact time.Duration
	DB3Compact time.Duration
}

// BatchInfo tracks information about the current batch
type BatchInfo struct {
	StartLedger uint32
	EndLedger   uint32
}

func createDir(path string) error {
	if _, err := os.Stat(path); err == nil {
		log.Printf("Opening existing database at %s", path)
	} else {
		log.Printf("Creating new database at %s", path)
		parentDir := filepath.Dir(path)
		if err := os.MkdirAll(parentDir, 0755); err != nil {
			return errors.Wrap(err, "failed to create database parent directory")
		}
	}
	return nil
}

func main() {
	// Command-line flags
	var startLedger, endLedger uint
	var batchSize int
	var db1Path, db2Path, db3Path string
	var enableCompression bool

	flag.UintVar(&startLedger, "start-ledger", 0, "Starting ledger sequence number")
	flag.IntVar(&batchSize, "ledger-batch-size", 2000, "Ledger-Batch size for commit")
	flag.UintVar(&endLedger, "end-ledger", 0, "Ending ledger sequence number")
	flag.StringVar(&db1Path, "db1", "", "Optional path for DataStore 1 (ledgerSeq -> compressed LCM)")
	flag.StringVar(&db2Path, "db2", "", "Optional path for DataStore 2 (txHash -> compressed TxData)")
	flag.StringVar(&db3Path, "db3", "", "Optional path for DataStore 3 (txHash -> ledgerSeq)")
	flag.BoolVar(&enableCompression, "compression", true, "Enable compression (default: true)")
	flag.Parse()

	// Validate required arguments
	if startLedger == 0 || endLedger == 0 {
		log.Fatal("start-ledger and end-ledger are required")
	}
	if batchSize <= 0 {
		log.Fatal("batch-size cannot be less than or equal to 0")
	}

	enableDb1 := db1Path != ""
	enableDb2 := db2Path != ""
	enableDb3 := db3Path != ""

	// At least one database must be enabled
	if !enableDb1 && !enableDb2 && !enableDb3 {
		log.Fatal("At least one database (db1, db2, or db3) must be specified")
	}

	// Convert to full paths
	var db1FullPath string
	var err error
	if enableDb1 {
		if err := createDir(db1Path); err != nil {
			log.Fatalf("Error creating database directory: %v", err)
		}
		db1FullPath, err = filepath.Abs(db1Path)
		if err != nil {
			log.Fatalf("Error getting absolute path for db1: %v", err)
		}
	}

	var db2FullPath string
	if enableDb2 {
		if err := createDir(db2Path); err != nil {
			log.Fatalf("Error creating database directory: %v", err)
		}
		db2FullPath, err = filepath.Abs(db2Path)
		if err != nil {
			log.Fatalf("Error getting absolute path for db2: %v", err)
		}
	}

	var db3FullPath string
	if enableDb3 {
		if err := createDir(db3Path); err != nil {
			log.Fatalf("Error creating database directory: %v", err)
		}
		db3FullPath, err = filepath.Abs(db3Path)
		if err != nil {
			log.Fatalf("Error getting absolute path for db3: %v", err)
		}
	}

	// Create config
	config := IngestionConfig{
		StartLedger:       uint32(startLedger),
		EndLedger:         uint32(endLedger),
		BatchSize:         batchSize,
		DB1Path:           db1FullPath,
		DB2Path:           db2FullPath,
		DB3Path:           db3FullPath,
		EnableDB1:         enableDb1,
		EnableDB2:         enableDb2,
		EnableDB3:         enableDb3,
		EnableCompression: enableCompression,
	}

	// Initialize RocksDB instances
	var db1 *grocksdb.DB
	var opts1 *grocksdb.Options

	// Only open DB1 if path is provided
	if config.EnableDB1 {
		var err error
		db1, opts1, err = openRocksDBForBulkLoad(config.DB1Path)
		if err != nil {
			log.Fatalf("Failed to open DB1 (ledgerSeq -> LCM): %v", err)
		}
		defer func() {
			db1.Close()
			opts1.Destroy()
		}()
		log.Printf("✓ DB1 opened at: %s", config.DB1Path)
	} else {
		log.Printf("ℹ DB1 (ledgerSeq -> LCM) is disabled (no path provided)")
	}

	var db2 *grocksdb.DB
	var opts2 *grocksdb.Options
	if config.EnableDB2 {
		db2, opts2, err = openRocksDBForBulkLoad(config.DB2Path)
		if err != nil {
			log.Fatalf("Failed to open DB2 (txHash -> TxData): %v", err)
		}
		defer func() {
			db2.Close()
			opts2.Destroy()
		}()
		log.Printf("✓ DB2 opened at: %s", config.DB2Path)
	} else {
		log.Printf("ℹ DB2 (txHash -> TxData) is disabled (no path provided)")
	}

	var db3 *grocksdb.DB
	var opts3 *grocksdb.Options
	if config.EnableDB3 {
		db3, opts3, err = openRocksDBForBulkLoad(config.DB3Path)
		if err != nil {
			log.Fatalf("Failed to open DB3 (txHash -> ledgerSeq): %v", err)
		}
		defer func() {
			db3.Close()
			opts3.Destroy()
		}()
		log.Printf("✓ DB3 opened at: %s", config.DB3Path)
	} else {
		log.Printf("ℹ DB3 (txHash -> ledgerSeq) is disabled (no path provided)")
	}

	ctx := context.Background()

	// Configure the datastore
	datastoreConfig := datastore.DataStoreConfig{
		Type: "GCS",
		Params: map[string]string{
			"destination_bucket_path": "sdf-ledger-close-meta/v1/ledgers/pubnet",
		},
	}

	dataStoreSchema := datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 64000,
	}

	// Initialize the datastore
	dataStore, err := datastore.NewDataStore(ctx, datastoreConfig)
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to create datastore"))
	}
	defer dataStore.Close()

	// Configure the BufferedStorageBackend
	backendConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: 1000,
		NumWorkers: 100,
		RetryLimit: 3,
		RetryWait:  5 * time.Second,
	}

	// Initialize the backend
	backend, err := ledgerbackend.NewBufferedStorageBackend(backendConfig, dataStore, dataStoreSchema)
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to create buffered storage backend"))
	}
	defer backend.Close()

	ledgerRange := ledgerbackend.BoundedRange(config.StartLedger, config.EndLedger)
	totalLedgers := int(ledgerRange.To() - ledgerRange.From() + 1)

	err = backend.PrepareRange(ctx, ledgerRange)
	if err != nil {
		log.Fatal(errors.Wrapf(err, "failed to prepare range: %v", ledgerRange))
	}

	log.Printf("\n========================================")
	log.Printf("Starting ledger processing")
	log.Printf("========================================")
	log.Printf("Ledger range: %d - %d (%s ledgers)",
		ledgerRange.From(), ledgerRange.To(), formatNumber(int64(totalLedgers)))
	log.Printf("DB1 (LCM) storage: %v", config.EnableDB1)
	log.Printf("DB2 (TxData) storage: %v", config.EnableDB2)
	log.Printf("DB3 (Hash->Seq) storage: %v", config.EnableDB3)
	log.Printf("Compression: %v", config.EnableCompression)

	// Set up metrics tracking
	startTime := time.Now()
	processedLedgerCount := 0
	lastReportedPercent := -1

	var totalRuntime time.Duration
	var totalDBTimingStats DBTimingStats
	var totalCompressionStats CompressionStats

	// Track global min/max ledger for DB1 compaction
	var globalMinLedger uint32 = 0xFFFFFFFF
	var globalMaxLedger uint32 = 0

	// Batch maps - only create ledgerSeqToLcm if DB1 is enabled, txHashToLedgerSeq if DB3 is enabled
	var ledgerSeqToLcm map[uint32][]byte
	if config.EnableDB1 {
		ledgerSeqToLcm = make(map[uint32][]byte)
	}
	var txHashToTxData map[string][]byte
	if config.EnableDB2 {
		txHashToTxData = make(map[string][]byte)
	}
	var txHashToLedgerSeq map[string]uint32
	if config.EnableDB3 {
		txHashToLedgerSeq = make(map[string]uint32)
	}

	// Create a reusable zstd encoder for better performance (only if compression enabled)
	var encoder *zstd.Encoder
	if config.EnableCompression {
		encoder, err = zstd.NewWriter(nil)
		if err != nil {
			log.Fatal(errors.Wrap(err, "failed to create zstd encoder"))
		}
		defer encoder.Close()
	}

	// Track batch info
	var currentBatchInfo BatchInfo
	batchStartLedger := ledgerRange.From()

	// Iterate through the ledger sequence. DO NOT INCLUDE LAST LEDGER. THIS IS DELIBERATE
	for ledgerSeq := ledgerRange.From(); ledgerSeq < ledgerRange.To(); ledgerSeq++ {
		processStart := time.Now()

		ledger, err := backend.GetLedger(ctx, ledgerSeq)
		if err != nil {
			log.Fatalf("Failed to retrieve ledger %d: %v", ledgerSeq, err)
		}

		// Process ledger and update maps
		stats, err := processLedger(encoder, ledger, ledgerSeqToLcm, txHashToTxData, txHashToLedgerSeq, config.EnableDB1, config.EnableDB2, config.EnableDB3, config.EnableCompression)
		if err != nil {
			log.Printf("Error processing ledger %d: %v", ledgerSeq, err)
			continue
		}

		// Track min/max for DB1 compaction
		if config.EnableDB1 {
			if ledgerSeq < globalMinLedger {
				globalMinLedger = ledgerSeq
			}
			if ledgerSeq > globalMaxLedger {
				globalMaxLedger = ledgerSeq
			}
		}

		// Accumulate stats
		totalCompressionStats.UncompressedLCM += stats.UncompressedLCM
		totalCompressionStats.CompressedLCM += stats.CompressedLCM
		totalCompressionStats.UncompressedTx += stats.UncompressedTx
		totalCompressionStats.CompressedTx += stats.CompressedTx
		totalCompressionStats.TxCount += stats.TxCount

		totalRuntime += time.Since(processStart)
		processedLedgerCount++

		// Write batch every batchSize ledgers
		if processedLedgerCount%config.BatchSize == 0 {
			currentBatchInfo.StartLedger = batchStartLedger
			currentBatchInfo.EndLedger = ledgerSeq

			currentBatchDbProcessingStart := time.Now()

			lcmCount := 0
			if config.EnableDB1 {
				lcmCount = len(ledgerSeqToLcm)
			}
			txCount := 0
			if config.EnableDB2 {
				txCount = len(txHashToTxData)
			}

			log.Printf("\n\n===== Processing batch [Ledger %d-%d] to DB (write + flush + compact) at ledger: %d,  (%d ledgers, %d transactions) =====",
				currentBatchInfo.StartLedger, currentBatchInfo.EndLedger, ledgerSeq, lcmCount, txCount)

			// Write to databases with timing
			dbTiming, err := writeBatchToDBWithTiming(db1, db2, db3, ledgerSeqToLcm, txHashToTxData, txHashToLedgerSeq, config)
			if err != nil {
				log.Printf("Error writing batch: %v", err)
			}

			// Flush to disk with timing
			flushTiming := flushAllDBsWithTiming(db1, db2, db3, config)

			// Compact this batch range (especially important for DB1 with sequential keys)
			compactTiming := compactAllDBsWithRange(db1, db2, db3, config, currentBatchInfo.StartLedger, currentBatchInfo.EndLedger)

			currentBatchDbProcessingTotalTime := time.Since(currentBatchDbProcessingStart)

			// Accumulate timing stats
			totalDBTimingStats.DB1Write += dbTiming.DB1Write
			totalDBTimingStats.DB2Write += dbTiming.DB2Write
			totalDBTimingStats.DB3Write += dbTiming.DB3Write
			totalDBTimingStats.DB1Flush += flushTiming.DB1Flush
			totalDBTimingStats.DB2Flush += flushTiming.DB2Flush
			totalDBTimingStats.DB3Flush += flushTiming.DB3Flush
			totalDBTimingStats.DB1Compact += compactTiming.DB1Compact
			totalDBTimingStats.DB2Compact += compactTiming.DB2Compact
			totalDBTimingStats.DB3Compact += compactTiming.DB3Compact

			// Log timing breakdown
			log.Printf("\n[Ledger %d-%d] Batch timing:", currentBatchInfo.StartLedger, currentBatchInfo.EndLedger)
			if config.EnableDB1 {
				log.Printf("  DB1: write=%s, flush=%s, compact=%s. Compaction Range: [%d - %d]",
					formatDuration(dbTiming.DB1Write), formatDuration(flushTiming.DB1Flush), formatDuration(compactTiming.DB1Compact),
					currentBatchInfo.StartLedger, currentBatchInfo.EndLedger)
			}
			if config.EnableDB2 {
				log.Printf("  DB2: write=%s, flush=%s, compact=%s. Compaction Range: full)",
					formatDuration(dbTiming.DB2Write), formatDuration(flushTiming.DB2Flush), formatDuration(compactTiming.DB2Compact))
			}
			if config.EnableDB3 {
				log.Printf("  DB3: write=%s, flush=%s, compact=%s. Compactin Range: full",
					formatDuration(dbTiming.DB3Write), formatDuration(flushTiming.DB3Flush), formatDuration(compactTiming.DB3Compact))
			}
			log.Printf("  Total I/O time: %s\n", formatDuration(currentBatchDbProcessingTotalTime))

			// Clear batch maps
			if config.EnableDB1 {
				ledgerSeqToLcm = make(map[uint32][]byte)
			}
			if config.EnableDB2 {
				txHashToTxData = make(map[string][]byte)
			}
			if config.EnableDB3 {
				txHashToLedgerSeq = make(map[string]uint32)
			}

			// Update batch start ledger for next batch
			batchStartLedger = ledgerSeq + 1
			log.Printf("\n========== Batch processing complete ==========")
		}

		if processedLedgerCount%(config.BatchSize*10) == 0 {
			log.Printf("\n========================================")
			log.Printf("\n========= ROCKSDB STATS as of ledger: %d =====\n", ledgerSeq)
			if config.EnableDB1 {
				monitorRocksDBStats(db1, "DB1")
			}
			if config.EnableDB2 {
				monitorRocksDBStats(db2, "DB2")
			}
			if config.EnableDB3 {
				monitorRocksDBStats(db3, "DB3")
			}
			log.Printf("\n========================================\n")
		}

		// Report progress every 1%
		currentPercent := (processedLedgerCount * 100) / totalLedgers
		if currentPercent > lastReportedPercent {
			elapsed := time.Since(startTime)
			ledgersPerSec := float64(processedLedgerCount) / elapsed.Seconds()
			remaining := totalLedgers - processedLedgerCount
			var eta time.Duration
			if ledgersPerSec > 0 {
				eta = time.Duration(float64(remaining)/ledgersPerSec) * time.Second
			}

			processingPct := 100 * totalRuntime.Seconds() / elapsed.Seconds()
			totalIOTime := totalDBTimingStats.DB1Write + totalDBTimingStats.DB2Write + totalDBTimingStats.DB3Write +
				totalDBTimingStats.DB1Flush + totalDBTimingStats.DB2Flush + totalDBTimingStats.DB3Flush +
				totalDBTimingStats.DB1Compact + totalDBTimingStats.DB2Compact + totalDBTimingStats.DB3Compact // ADD COMPACT TIMES
			ioPct := 100 * totalIOTime.Seconds() / elapsed.Seconds()

			log.Printf("\n========================================")
			log.Printf("========== PROGRESS TIMELINE: %d/%d ledgers (%d%%) (current: %d, startLedger:%d, endLedger:%d) ==========", processedLedgerCount, totalLedgers, currentPercent,
				ledgerSeq, startLedger, endLedger)
			log.Printf("========== Speed: %.2f ledgers/sec | Transactions: %s | ETA: %s ==========",
				ledgersPerSec, formatNumber(totalCompressionStats.TxCount), formatDuration(eta))
			log.Printf("Time breakdown: Processing=%.1f%%, I/O=%.1f%%", processingPct, ioPct)

			if config.EnableDB1 {
				db1TotalTime := totalDBTimingStats.DB1Write + totalDBTimingStats.DB1Flush + totalDBTimingStats.DB1Compact
				db1Pct := 100 * db1TotalTime.Seconds() / elapsed.Seconds()
				log.Printf("  DB1: %.1f%% (write=%s, flush=%s, compact=%s)",
					db1Pct, formatDuration(totalDBTimingStats.DB1Write),
					formatDuration(totalDBTimingStats.DB1Flush), formatDuration(totalDBTimingStats.DB1Compact))
			}
			if config.EnableDB2 {
				db2TotalTime := totalDBTimingStats.DB2Write + totalDBTimingStats.DB2Flush + totalDBTimingStats.DB2Compact
				db2Pct := 100 * db2TotalTime.Seconds() / elapsed.Seconds()
				log.Printf("  DB2: %.1f%% (write=%s, flush=%s, compact=%s)",
					db2Pct, formatDuration(totalDBTimingStats.DB2Write),
					formatDuration(totalDBTimingStats.DB2Flush), formatDuration(totalDBTimingStats.DB2Compact))
			}
			if config.EnableDB3 {
				db3TotalTime := totalDBTimingStats.DB3Write + totalDBTimingStats.DB3Flush + totalDBTimingStats.DB3Compact
				db3Pct := 100 * db3TotalTime.Seconds() / elapsed.Seconds()
				log.Printf("  DB3: %.1f%% (write=%s, flush=%s, compact=%s)",
					db3Pct, formatDuration(totalDBTimingStats.DB3Write),
					formatDuration(totalDBTimingStats.DB3Flush), formatDuration(totalDBTimingStats.DB3Compact))
			}
			log.Printf("========================================\n")

			lastReportedPercent = currentPercent
		}
	}

	// Write any remaining batch data
	lcmCount := 0
	if config.EnableDB1 && len(ledgerSeqToLcm) > 0 {
		lcmCount = len(ledgerSeqToLcm)
	}
	txCount := 0
	if config.EnableDB2 && len(txHashToTxData) > 0 {
		txCount = len(txHashToTxData)
	}

	if lcmCount > 0 || txCount > 0 {
		currentBatchInfo.StartLedger = batchStartLedger
		currentBatchInfo.EndLedger = ledgerRange.To() - 1

		log.Printf("\n[Ledger %d-%d] Writing final batch to disk (%d ledgers, %d transactions)...",
			currentBatchInfo.StartLedger, currentBatchInfo.EndLedger, lcmCount, txCount)

		finalBatchDbProcessingStart := time.Now()
		dbTiming, err := writeBatchToDBWithTiming(db1, db2, db3, ledgerSeqToLcm, txHashToTxData, txHashToLedgerSeq, config)
		if err != nil {
			log.Printf("Error writing final batch: %v", err)
		}

		log.Printf("Flushing databases to disk...")
		flushTiming := flushAllDBsWithTiming(db1, db2, db3, config)

		finalBatchDbProcessingTime := time.Since(finalBatchDbProcessingStart)

		// Accumulate timing stats
		totalDBTimingStats.DB1Write += dbTiming.DB1Write
		totalDBTimingStats.DB2Write += dbTiming.DB2Write
		totalDBTimingStats.DB3Write += dbTiming.DB3Write
		totalDBTimingStats.DB1Flush += flushTiming.DB1Flush
		totalDBTimingStats.DB2Flush += flushTiming.DB2Flush
		totalDBTimingStats.DB3Flush += flushTiming.DB3Flush

		log.Printf("Final batch timing: %s", formatDuration(finalBatchDbProcessingTime))
	}

	elapsed := time.Since(startTime)

	// Trigger final compaction to ensure everything is optimized
	// Trigger final compaction to ensure everything is optimized
	log.Printf("\n\n========================================")
	log.Printf("PERFORMING FINAL COMPACTION")
	log.Printf("========================================\n")
	compactionTiming := compactAllDBsWithRange(db1, db2, db3, config, globalMinLedger, globalMaxLedger)

	// Add to total timing
	totalDBTimingStats.DB1Compact += compactionTiming.DB1Compact
	totalDBTimingStats.DB2Compact += compactionTiming.DB2Compact
	totalDBTimingStats.DB3Compact += compactionTiming.DB3Compact

	log.Printf("\n========================================")
	log.Printf("COMPACTION SUMMARY")
	log.Printf("========================================")
	if config.EnableDB1 {
		log.Printf("DB1: %s", formatDuration(compactionTiming.DB1Compact))
	}
	if config.EnableDB2 {
		log.Printf("DB2: %s", formatDuration(compactionTiming.DB2Compact))
	}
	if config.EnableDB3 {
		log.Printf("DB3: %s", formatDuration(compactionTiming.DB3Compact))
	}
	totalCompactionTime := compactionTiming.DB1Compact + compactionTiming.DB2Compact + compactionTiming.DB3Compact
	log.Printf("Total: %s", formatDuration(totalCompactionTime))

	log.Printf("\n========================================")

	log.Printf("\n========= FINAL ROCKSDB STATS after compaction =====\n")
	if config.EnableDB1 {
		monitorRocksDBStats(db1, "DB1")
	}
	if config.EnableDB2 {
		monitorRocksDBStats(db2, "DB2")
	}
	if config.EnableDB3 {
		monitorRocksDBStats(db3, "DB3")
	}
	log.Printf("\n========================================")

	// Calculate total I/O time
	totalIOTime := totalDBTimingStats.DB1Write + totalDBTimingStats.DB2Write + totalDBTimingStats.DB3Write +
		totalDBTimingStats.DB1Flush + totalDBTimingStats.DB2Flush + totalDBTimingStats.DB3Flush +
		totalDBTimingStats.DB1Compact + totalDBTimingStats.DB2Compact + totalDBTimingStats.DB3Compact

	// Print final statistics
	log.Printf("\n========================================")
	log.Printf("INGESTION COMPLETE")
	log.Printf("========================================")
	log.Printf("Total ledgers processed:  %s", formatNumber(int64(processedLedgerCount)))
	log.Printf("Total transactions:       %s", formatNumber(totalCompressionStats.TxCount))
	log.Printf("Total time:               %s", formatDuration(elapsed))
	log.Printf("Average speed:            %.2f ledgers/sec", float64(processedLedgerCount)/elapsed.Seconds())
	log.Printf("")
	log.Printf("Time breakdown:")
	log.Printf("  Processing:             %s (%.1f%%)", formatDuration(totalRuntime),
		100*totalRuntime.Seconds()/elapsed.Seconds())
	log.Printf("  Disk I/O (total):       %s (%.1f%%)", formatDuration(totalIOTime),
		100*totalIOTime.Seconds()/elapsed.Seconds())

	if config.EnableDB1 {
		db1Total := totalDBTimingStats.DB1Write + totalDBTimingStats.DB1Flush + totalDBTimingStats.DB1Compact
		log.Printf("    DB1:                  %s (write: %s, flush: %s, compact: %s)",
			formatDuration(db1Total), formatDuration(totalDBTimingStats.DB1Write),
			formatDuration(totalDBTimingStats.DB1Flush), formatDuration(totalDBTimingStats.DB1Compact))
	}
	if config.EnableDB2 {
		db2Total := totalDBTimingStats.DB2Write + totalDBTimingStats.DB2Flush + totalDBTimingStats.DB2Compact
		log.Printf("    DB2:                  %s (write: %s, flush: %s, compact: %s)",
			formatDuration(db2Total), formatDuration(totalDBTimingStats.DB2Write),
			formatDuration(totalDBTimingStats.DB2Flush), formatDuration(totalDBTimingStats.DB2Compact))
	}
	if config.EnableDB3 {
		db3Total := totalDBTimingStats.DB3Write + totalDBTimingStats.DB3Flush + totalDBTimingStats.DB3Compact
		log.Printf("    DB3:                  %s (write: %s, flush: %s, compact: %s)",
			formatDuration(db3Total), formatDuration(totalDBTimingStats.DB3Write),
			formatDuration(totalDBTimingStats.DB3Flush), formatDuration(totalDBTimingStats.DB3Compact))
	}

	log.Printf("")

	// Compression statistics
	if config.EnableCompression {
		if config.EnableDB1 && totalCompressionStats.UncompressedLCM > 0 {
			compressionRatio := 100 * (1 - float64(totalCompressionStats.CompressedLCM)/float64(totalCompressionStats.UncompressedLCM))
			log.Printf("LCM Compression:")
			log.Printf("  Original size:          %s", formatBytes(totalCompressionStats.UncompressedLCM))
			log.Printf("  Compressed size:        %s", formatBytes(totalCompressionStats.CompressedLCM))
			log.Printf("  Compression ratio:      %.2f%% reduction", compressionRatio)
			log.Printf("  Space saved:            %s", formatBytes(totalCompressionStats.UncompressedLCM-totalCompressionStats.CompressedLCM))
		}

		if config.EnableDB2 && totalCompressionStats.UncompressedTx > 0 {
			compressionRatio := 100 * (1 - float64(totalCompressionStats.CompressedTx)/float64(totalCompressionStats.UncompressedTx))
			log.Printf("")
			log.Printf("TxData Compression:")
			log.Printf("  Original size:          %s", formatBytes(totalCompressionStats.UncompressedTx))
			log.Printf("  Compressed size:        %s", formatBytes(totalCompressionStats.CompressedTx))
			log.Printf("  Compression ratio:      %.2f%% reduction", compressionRatio)
			log.Printf("  Space saved:            %s", formatBytes(totalCompressionStats.UncompressedTx-totalCompressionStats.CompressedTx))
		}
	}

	log.Printf("\n\n========================================\n")
}

// processLedger processes a single ledger and updates the batch maps
func processLedger(
	encoder *zstd.Encoder,
	lcm xdr.LedgerCloseMeta,
	ledgerSeqToLcm map[uint32][]byte,
	txHashToTxData map[string][]byte,
	txHashToLedgerSeq map[string]uint32,
	enableDB1 bool,
	enableDB2 bool,
	enableDB3 bool,
	enableCompression bool,
) (*CompressionStats, error) {
	stats := &CompressionStats{}

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, lcm)
	if err != nil {
		return stats, errors.Wrap(err, "failed to create transaction reader")
	}
	defer txReader.Close()

	ledgerSeq := lcm.LedgerSequence()
	closedAt := lcm.ClosedAt()

	// Process and compress LedgerCloseMeta only if DB1 is enabled
	if enableDB1 {
		lcmBytes, err := lcm.MarshalBinary()
		if err != nil {
			return stats, errors.Wrapf(err, "failed to marshal lcm for ledger: %d", ledgerSeq)
		}

		stats.UncompressedLCM = int64(len(lcmBytes))

		var dataToStore []byte
		if enableCompression {
			dataToStore = compressData(encoder, lcmBytes)
			stats.CompressedLCM = int64(len(dataToStore))
		} else {
			dataToStore = lcmBytes
			stats.CompressedLCM = int64(len(lcmBytes))
		}

		ledgerSeqToLcm[ledgerSeq] = dataToStore
	}

	// Process each transaction
	if enableDB2 || enableDB3 {
		for {
			tx, err := txReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return stats, fmt.Errorf("error reading transaction: %w", err)
			}

			// Marshal transaction components
			txEnvelopeBytes, err := tx.Envelope.MarshalBinary()
			if err != nil {
				return stats, fmt.Errorf("error marshalling transaction envelope: %w", err)
			}
			txResultBytes, err := tx.Result.MarshalBinary()
			if err != nil {
				return stats, fmt.Errorf("error marshalling transaction result: %w", err)
			}
			txMetaBytes, err := tx.UnsafeMeta.MarshalBinary()
			if err != nil {
				return stats, fmt.Errorf("error marshalling transaction meta: %w", err)
			}

			// Create TxData protobuf
			txDataProto := tx_data.TxData{
				LedgerSequence: ledgerSeq,
				ClosedAt:       timestamppb.New(closedAt),
				Index:          tx.Index,
				TxEnvelope:     txEnvelopeBytes,
				TxResult:       txResultBytes,
				TxMeta:         txMetaBytes,
			}

			txDataBytes, err := proto.Marshal(&txDataProto)
			if err != nil {
				return stats, errors.Wrap(err, "marshalling proto transaction data")
			}

			stats.UncompressedTx += int64(len(txDataBytes))

			// Compress TxData if compression is enabled
			var dataToStore []byte
			if enableCompression {
				dataToStore = compressData(encoder, txDataBytes)
				stats.CompressedTx += int64(len(dataToStore))
			} else {
				dataToStore = txDataBytes
				stats.CompressedTx += int64(len(txDataBytes))
			}

			// Use hex string as map key (for convenience), but we'll convert to binary when writing to DB
			txHashHex := tx.Hash.HexString()
			if enableDB2 {
				txHashToTxData[txHashHex] = dataToStore
			}
			if enableDB3 {
				txHashToLedgerSeq[txHashHex] = ledgerSeq
			}

			stats.TxCount++
		}
	}

	return stats, nil
}

// compressData compresses data using zstd (reuses encoder for efficiency)
func compressData(encoder *zstd.Encoder, data []byte) []byte {
	return encoder.EncodeAll(data, make([]byte, 0, len(data)))
}

// writeBatchToDBWithTiming writes the batch data to RocksDB instances and returns timing stats
func writeBatchToDBWithTiming(
	db1, db2, db3 *grocksdb.DB,
	ledgerSeqToLcm map[uint32][]byte,
	txHashToTxData map[string][]byte,
	txHashToLedgerSeq map[string]uint32,
	config IngestionConfig,
) (DBTimingStats, error) {
	var timing DBTimingStats
	wo := grocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true) // Disable WAL for bulk loading performance
	defer wo.Destroy()

	// Write to DB1 (ledgerSeq -> compressed LCM) only if enabled
	if config.EnableDB1 && db1 != nil {
		start := time.Now()
		for ledgerSeq, compressedLcm := range ledgerSeqToLcm {
			key := uint32ToBytes(ledgerSeq)
			if err := db1.Put(wo, key, compressedLcm); err != nil {
				return timing, errors.Wrap(err, "failed to write to DB1")
			}
		}
		timing.DB1Write = time.Since(start)
	}

	// Write to DB2 (txHash -> compressed TxData)
	if config.EnableDB2 && db2 != nil {
		start := time.Now()
		for txHashHex, compressedTxData := range txHashToTxData {
			txHashBytes, err := hexStringToBytes(txHashHex)
			if err != nil {
				return timing, errors.Wrapf(err, "failed to convert tx hash to bytes: %s", txHashHex)
			}
			if err := db2.Put(wo, txHashBytes, compressedTxData); err != nil {
				return timing, errors.Wrap(err, "failed to write to DB2")
			}
		}
		timing.DB2Write = time.Since(start)
	}

	// Write to DB3 (txHash -> ledgerSeq) only if enabled
	if config.EnableDB3 && db3 != nil {
		start := time.Now()
		for txHashHex, ledgerSeq := range txHashToLedgerSeq {
			txHashBytes, err := hexStringToBytes(txHashHex)
			if err != nil {
				return timing, errors.Wrapf(err, "failed to convert tx hash to bytes: %s", txHashHex)
			}
			value := uint32ToBytes(ledgerSeq)
			if err := db3.Put(wo, txHashBytes, value); err != nil {
				return timing, errors.Wrap(err, "failed to write to DB3")
			}
		}
		timing.DB3Write = time.Since(start)
	}

	return timing, nil
}

// flushAllDBsWithTiming flushes databases to disk and returns timing stats
func flushAllDBsWithTiming(db1, db2, db3 *grocksdb.DB, config IngestionConfig) DBTimingStats {
	var timing DBTimingStats
	fo := grocksdb.NewDefaultFlushOptions()
	fo.SetWait(true)
	defer fo.Destroy()

	if config.EnableDB1 && db1 != nil {
		start := time.Now()
		if err := db1.Flush(fo); err != nil {
			log.Printf("Warning: Failed to flush DB1: %v", err)
		}
		timing.DB1Flush = time.Since(start)
	}

	if config.EnableDB2 && db2 != nil {
		start := time.Now()
		if err := db2.Flush(fo); err != nil {
			log.Printf("Warning: Failed to flush DB2: %v", err)
		}
		timing.DB2Flush = time.Since(start)
	}

	if config.EnableDB3 && db3 != nil {
		start := time.Now()
		if err := db3.Flush(fo); err != nil {
			log.Printf("Warning: Failed to flush DB3: %v", err)
		}
		timing.DB3Flush = time.Since(start)
	}

	return timing
}

// compactAllDBsWithRange performs full compaction on databases
// For DB1, uses sequential range compaction since keys are sequential ledger numbers
func compactAllDBsWithRange(db1, db2, db3 *grocksdb.DB, config IngestionConfig, minLedger, maxLedger uint32) DBTimingStats {
	var timing DBTimingStats

	if config.EnableDB1 && db1 != nil {
		start := time.Now()
		startKey := uint32ToBytes(minLedger)
		endKey := uint32ToBytes(maxLedger + 1)
		db1.CompactRange(grocksdb.Range{Start: startKey, Limit: endKey})
		timing.DB1Compact = time.Since(start)
	}

	if config.EnableDB2 && db2 != nil {
		start := time.Now()
		db2.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
		timing.DB2Compact = time.Since(start)
	}

	if config.EnableDB3 && db3 != nil {
		start := time.Now()
		db3.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
		timing.DB3Compact = time.Since(start)
	}

	return timing
}

// uint32ToBytes converts a uint32 to big-endian bytes
func uint32ToBytes(n uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	return b
}

// hexStringToBytes converts a hex string to bytes
func hexStringToBytes(hexStr string) ([]byte, error) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	return hex.DecodeString(hexStr)
}

// formatBytes formats bytes into human-readable format
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

// formatNumber formats large numbers with commas
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

// formatDuration formats a duration in a human-readable way
func formatDuration(d time.Duration) string {
	// For very short durations, show microseconds or milliseconds
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}

	// For durations under a minute, show seconds with decimal precision
	if d < time.Minute {
		return fmt.Sprintf("%.3fs", d.Seconds())
	}

	// For longer durations, show traditional format
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

// openRocksDBForBulkLoad opens or creates a RocksDB database with settings optimized
// for large-scale bulk ingestion with periodic flushes but delayed final compaction
func openRocksDBForBulkLoad(path string) (*grocksdb.DB, *grocksdb.Options, error) {

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetErrorIfExists(false)

	// ============================================================================
	// 1. WRITE BUFFER (MEMTABLE) CONFIGURATION
	// ============================================================================
	opts.SetWriteBufferSize(512 << 20) // 512 MB per memtable
	opts.SetMaxWriteBufferNumber(6)
	opts.SetMinWriteBufferNumberToMerge(2)

	// ============================================================================
	// 2. L0 FILE MANAGEMENT
	// ============================================================================
	opts.SetLevel0FileNumCompactionTrigger(50)
	opts.SetLevel0SlowdownWritesTrigger(100)
	opts.SetLevel0StopWritesTrigger(150)

	// ============================================================================
	// 3. COMPACTION STRATEGY
	// ============================================================================
	opts.SetDisableAutoCompactions(false)
	opts.SetCompactionStyle(grocksdb.LevelCompactionStyle)
	opts.SetTargetFileSizeBase(256 << 20)
	opts.SetTargetFileSizeMultiplier(2)
	opts.SetMaxBytesForLevelBase(1024 << 20)
	opts.SetMaxBytesForLevelMultiplier(10)

	// ============================================================================
	// 4. BACKGROUND JOB CONFIGURATION
	// ============================================================================
	opts.SetMaxBackgroundJobs(20)

	// ============================================================================
	// 5. COMPRESSION SETTINGS
	// ============================================================================
	opts.SetCompression(grocksdb.NoCompression)

	// ============================================================================
	// 6. MEMORY AND FILE MANAGEMENT
	// ============================================================================
	opts.SetMaxOpenFiles(5000)

	// ============================================================================
	// 7. WRITE-AHEAD LOG (WAL) SETTINGS
	// ============================================================================
	opts.SetMaxTotalWalSize(2048 << 20)

	// ============================================================================
	// 8. LOGGING SETTINGS
	// ============================================================================
	opts.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)
	opts.SetMaxLogFileSize(20 << 20)
	opts.SetKeepLogFileNum(3)

	// ============================================================================
	// OPEN DATABASE
	// ============================================================================
	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		opts.Destroy()
		return nil, nil, errors.Wrap(err, "failed to open RocksDB")
	}

	log.Printf("RocksDB Configuration for %s:", path)
	log.Printf("  Write Buffer: 512 MB × 6 = 3 GB total")
	log.Printf("  L0 Compaction Trigger: 50 files")
	log.Printf("  L0 Slowdown: 100 files")
	log.Printf("  L0 Stop: 150 files")
	log.Printf("  Background Jobs: 20")
	log.Printf("  Max Open Files: 5000")
	log.Printf("  Auto Compactions: ENABLED (controlled)")

	return db, opts, nil
}

// AtoiIgnoreError converts a string to an int, ignoring any errors.
// If the conversion fails, the function returns 0.
func AtoiIgnoreError(s string) int {
	value, _ := strconv.Atoi(s)
	return value
}

// monitorRocksDBStats monitors and logs RocksDB statistics
func monitorRocksDBStats(db *grocksdb.DB, title string) {
	l0Files := db.GetProperty("rocksdb.num-files-at-level0")
	l1Files := db.GetProperty("rocksdb.num-files-at-level1")
	l2Files := db.GetProperty("rocksdb.num-files-at-level2")
	l3Files := db.GetProperty("rocksdb.num-files-at-level3")
	l4Files := db.GetProperty("rocksdb.num-files-at-level4")
	l5Files := db.GetProperty("rocksdb.num-files-at-level5")
	l6Files := db.GetProperty("rocksdb.num-files-at-level6")

	estimatedKeys := db.GetProperty("rocksdb.estimate-num-keys")
	totalSSTSize := db.GetProperty("rocksdb.total-sst-files-size")
	curMemtable := db.GetProperty("rocksdb.cur-size-all-mem-tables")
	compactionPending := db.GetProperty("rocksdb.compaction-pending")

	log.Printf("[%s] RocksDB Stats:", title)
	log.Printf("  L0 Files: %s", l0Files)
	log.Printf("  L1 Files: %s", l1Files)
	log.Printf("  L2 Files: %s", l2Files)
	log.Printf("  L3 Files: %s", l3Files)
	log.Printf("  L4 Files: %s", l4Files)
	log.Printf("  L5 Files: %s", l5Files)
	log.Printf("  L6 Files: %s", l6Files)

	log.Printf("  Estimated Keys: %s", formatNumber(int64(AtoiIgnoreError(estimatedKeys))))

	estimatedKeysInt := int64(AtoiIgnoreError(estimatedKeys))
	totalSSTSizeInt := int64(AtoiIgnoreError(totalSSTSize))

	if estimatedKeysInt > 0 && totalSSTSizeInt > 0 {
		avgKeySize := float64(totalSSTSizeInt) / float64(estimatedKeysInt)
		log.Printf("  Avg Key+Value Size: %.2f bytes", avgKeySize)
	}

	log.Printf("  Total SST Size: %s", formatBytes(totalSSTSizeInt))
	log.Printf("  Memtable Usage: %s", formatBytes(int64(AtoiIgnoreError(curMemtable))))
	log.Printf("  Compaction Pending: %s", compactionPending)
}
