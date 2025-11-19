package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/erigontech/mdbx-go/mdbx"
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
)

const GB = 1024 * 1024 * 1024
const TB = 1024 * GB

// IngestionConfig holds configuration for the ingestion process
type IngestionConfig struct {
	StartLedger                  uint32
	EndLedger                    uint32
	BatchSize                    int
	DbPageSize                   uint
	DB2Path                      string
	DB3Path                      string
	EnableDB2                    bool
	EnableDB3                    bool
	EnableApplicationCompression bool
	RocksDBLCMPath               string
	UseRocksDB                   bool
	SyncEveryNBatches            int
}

// CompressionStats tracks compression metrics for a batch
type CompressionStats struct {
	UncompressedTx int64
	CompressedTx   int64
	TxCount        int64
}

// DBTimingStats tracks timing for individual operations
type DBTimingStats struct {
	GetLedgerTime      time.Duration
	DB2CompressionTime time.Duration
	DB2WriteTime       time.Duration
	DB2SyncTime        time.Duration
	DB3WriteTime       time.Duration
	DB3SyncTime        time.Duration
}

// RocksDBTimingStats tracks timing for RocksDB operations
type RocksDBTimingStats struct {
	ReadTime       time.Duration
	DecompressTime time.Duration
	UnmarshalTime  time.Duration
	TotalTime      time.Duration
}

// BatchInfo tracks information about the current batch
type BatchInfo struct {
	BatchNum                uint32
	StartLedger             uint32
	EndLedger               uint32
	StartTime               time.Time
	TxCount                 int
	BatchGetLedgerTime      time.Duration
	BatchDb2CompressionTime time.Duration
	BatchRocksDBTimingStats RocksDBTimingStats
}

// MDBXDatabase wraps an MDBX environment and DBI
type MDBXDatabase struct {
	Env  *mdbx.Env
	DBI  mdbx.DBI
	Path string
}

// MDBXStats holds statistics about an MDBX database
type MDBXStats struct {
	PageSize      uint
	Depth         uint
	BranchPages   uint64
	LeafPages     uint64
	OverflowPages uint64
	Entries       uint64
	MapSize       uint64
	LastPageNum   uint64
	RecentTxnID   uint64
}

// Raw transaction data for batch processing
type rawTxData struct {
	tx        ingest.LedgerTransaction
	ledgerSeq uint32
	closedAt  time.Time
}

// RocksDBReader wraps RocksDB with a reusable zstd decoder
type RocksDBReader struct {
	db *grocksdb.DB
	ro *grocksdb.ReadOptions
}

// LedgerResult holds the result of fetching a ledger
type LedgerResult struct {
	LedgerSeq uint32
	Ledger    xdr.LedgerCloseMeta
	Timing    RocksDBTimingStats
	Err       error
}

func main() {
	// Command-line flags
	var startLedger, endLedger uint
	var dbPagesize uint
	var batchSize int
	var syncEveryNBatches int
	var db2Path, db3Path string
	var enableApplicationCompression bool
	var rocksdbLcmPath string

	flag.UintVar(&dbPagesize, "db-pagesize", 16384, "DB Page size for new dbs. Ignored if db exists")
	flag.UintVar(&startLedger, "start-ledger", 0, "Starting ledger sequence number")
	flag.UintVar(&endLedger, "end-ledger", 0, "Ending ledger sequence number")
	flag.IntVar(&batchSize, "ledger-batch-size", 5000, "Ledger batch size for commit")
	flag.IntVar(&syncEveryNBatches, "sync-every-n-batches", 20, "Sync to disk every N batches")
	flag.StringVar(&db2Path, "db2", "", "Path for DB2 (txHash -> compressed TxData)")
	flag.StringVar(&db3Path, "db3", "", "Path for DB3 (txHash -> ledgerSeq)")
	flag.BoolVar(&enableApplicationCompression, "app-compression", true, "Enable compression (default: true)")
	flag.StringVar(&rocksdbLcmPath, "rocksdb-lcm-store", "", "Path to RocksDB store containing compressed LedgerCloseMeta")
	flag.Parse()

	// Validate required arguments
	if startLedger == 0 || endLedger == 0 {
		log.Fatal("start-ledger and end-ledger are required")
	}
	if batchSize <= 0 {
		log.Fatal("batch-size must be positive")
	}

	enableDB2 := db2Path != ""
	enableDB3 := db3Path != ""

	if !enableDB2 && !enableDB3 {
		log.Fatal("At least one database (db2 or db3) must be specified")
	}

	if dbPagesize%(4*1024) != 0 {
		log.Fatal("db-pagesize must be a multiple of 4kb")
	}

	// Create config
	config := IngestionConfig{
		StartLedger:                  uint32(startLedger),
		EndLedger:                    uint32(endLedger),
		BatchSize:                    batchSize,
		DB2Path:                      db2Path,
		DB3Path:                      db3Path,
		EnableDB2:                    enableDB2,
		EnableDB3:                    enableDB3,
		EnableApplicationCompression: enableApplicationCompression,
		RocksDBLCMPath:               rocksdbLcmPath,
		UseRocksDB:                   rocksdbLcmPath != "",
		DbPageSize:                   dbPagesize,
		SyncEveryNBatches:            syncEveryNBatches,
	}

	// Initialize MDBX databases
	var db2 *MDBXDatabase
	var db3 *MDBXDatabase
	var err error

	if config.EnableDB2 {
		db2Path, err = filepath.Abs(config.DB2Path)
		if err != nil {
			log.Fatalf("Failed to get absolute path for db2: %v", err)
		}
		db2, err = openMDBXDatabase(db2Path, "DB2 (txHash->TxData)", config)
		if err != nil {
			log.Fatalf("Failed to open DB2: %v", err)
		}
		defer db2.Close()
		log.Printf("✓ DB2 (MDBX) opened at: %s", db2Path)
	}

	if config.EnableDB3 {
		db3Path, err = filepath.Abs(config.DB3Path)
		if err != nil {
			log.Fatalf("Failed to get absolute path for db3: %v", err)
		}
		db3, err = openMDBXDatabase(db3Path, "DB3 (txHash->LedgerSeq)", config)
		if err != nil {
			log.Fatalf("Failed to open DB3: %v", err)
		}
		defer db3.Close()
		log.Printf("✓ DB3 (MDBX) opened at: %s", db3Path)
	}

	ctx := context.Background()

	// Initialize RocksDB reader or GCS backend
	var rocksReader *RocksDBReader
	var backend *ledgerbackend.BufferedStorageBackend

	if config.UseRocksDB {
		rocksReader, err = openRocksDBReader(config.RocksDBLCMPath)
		if err != nil {
			log.Fatalf("Failed to open RocksDB LCM store: %v", err)
		}
		defer rocksReader.Close()
		log.Printf("✓ RocksDB LCM store opened at: %s", config.RocksDBLCMPath)
	} else {
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
			BufferSize: 10000,
			NumWorkers: 200,
			RetryLimit: 3,
			RetryWait:  5 * time.Second,
		}

		// Initialize the backend
		backend, err = ledgerbackend.NewBufferedStorageBackend(backendConfig, dataStore, dataStoreSchema)
		if err != nil {
			log.Fatal(errors.Wrap(err, "failed to create buffered storage backend"))
		}
		defer backend.Close()

		ledgerRange := ledgerbackend.BoundedRange(config.StartLedger, config.EndLedger)
		err = backend.PrepareRange(ctx, ledgerRange)
		if err != nil {
			log.Fatal(errors.Wrapf(err, "failed to prepare range: %v", ledgerRange))
		}
	}

	ledgerRange := ledgerbackend.BoundedRange(config.StartLedger, config.EndLedger)
	totalLedgers := int(ledgerRange.To() - ledgerRange.From() + 1)

	log.Printf("\n========================================")
	log.Printf("Starting MDBX ledger processing")
	log.Printf("========================================")
	log.Printf("Ledger range: %d - %d (%s ledgers)",
		ledgerRange.From(), ledgerRange.To(), helpers.FormatNumber(int64(totalLedgers)))
	log.Printf("Ledger source: %s", func() string {
		if config.UseRocksDB {
			return "RocksDB (" + config.RocksDBLCMPath + ")"
		}
		return "GCS (BufferedStorageBackend)"
	}())
	log.Printf("DB2 (TxData) storage: %v", config.EnableDB2)
	log.Printf("DB3 (Hash->Seq) storage: %v", config.EnableDB3)
	log.Printf("Application Compression: %v", config.EnableApplicationCompression)
	log.Printf("Page Size: %d bytes", config.DbPageSize)
	log.Printf("Batch Size: %d ledgers", config.BatchSize)
	log.Printf("Sync every: %d batches", config.SyncEveryNBatches)
	log.Printf("Mode: SafeNoSync (fast bulk ingestion)")
	log.Printf("========================================\n")

	// Set up metrics tracking
	startTime := time.Now()
	processedLedgerCount := 0
	skippedLedgerCount := 0
	lastReportedPercent := -1

	var totalTimingStats DBTimingStats
	var totalCompressionStats CompressionStats
	var totalRocksDBTiming RocksDBTimingStats
	var totalSyncTime time.Duration

	// Batch data
	var batchTransactions []rawTxData

	// Track current batch
	currentBatch := BatchInfo{
		BatchNum:    1,
		StartLedger: ledgerRange.From(),
		StartTime:   time.Now(),
	}

	// Helper function to process a batch worth of transactions
	// This is where compresession of tx data fields happens and so on and they get added to the in memory maps
	processTransactionsInCurrentBatch := func() {
		if len(batchTransactions) == 0 {
			return
		}

		txHashToTxData := make(map[string][]byte)
		txHashToLedgerSeq := make(map[string]uint32)

		// Process all transactions in parallel
		compressionStart := time.Now()

		stats, err := compressTransactionsFromBatchInParallel(
			batchTransactions,
			txHashToTxData,
			txHashToLedgerSeq,
			config,
		)
		if err != nil {
			log.Fatalf("Failed to process batch: %v", err)
		}

		// This is only slightly misleading. this "compressionTime" also includes time for parallelism and for loops and what not
		// But they are dwarfed in comparision to the time it takes to compress. I am ok with that negligible variance
		compressionTime := time.Since(compressionStart)

		totalTimingStats.DB2CompressionTime += compressionTime
		currentBatch.BatchDb2CompressionTime = compressionTime

		// Update stats
		totalCompressionStats.UncompressedTx += stats.UncompressedTx
		totalCompressionStats.CompressedTx += stats.CompressedTx
		totalCompressionStats.TxCount += stats.TxCount

		currentBatch.TxCount = int(stats.TxCount)

		// Write to databases
		dbTiming, err := writeBatchToDatabases(db2, db3, txHashToTxData, txHashToLedgerSeq, config)
		if err != nil {
			log.Printf("Error writing batch: %v", err)
		}

		// Accumulate timing
		totalTimingStats.DB2WriteTime += dbTiming.DB2WriteTime
		totalTimingStats.DB2SyncTime += dbTiming.DB2SyncTime
		totalTimingStats.DB3WriteTime += dbTiming.DB3WriteTime
		totalTimingStats.DB3SyncTime += dbTiming.DB3SyncTime

		// Log batch completion
		logBatchCompletion(currentBatch, dbTiming, config)

		// Periodic sync every N batches
		if currentBatch.BatchNum%uint32(config.SyncEveryNBatches) == 0 {
			log.Printf("\n========================================")
			log.Printf("\n🔄 Periodic sync at batch %d...", currentBatch.BatchNum)
			syncStart := time.Now()
			if config.EnableDB2 && db2 != nil {
				if err := db2.Env.Sync(true, false); err != nil {
					log.Printf("Warning: Failed to sync DB2: %v", err)
				}
			}
			if config.EnableDB3 && db3 != nil {
				if err := db3.Env.Sync(true, false); err != nil {
					log.Printf("Warning: Failed to sync DB3: %v", err)
				}
			}
			syncDuration := time.Since(syncStart)
			totalSyncTime += syncDuration
			log.Printf("✅ Sync complete: %s (total sync time so far: %s)\n", helpers.FormatDuration(syncDuration), helpers.FormatDuration(totalSyncTime))
			log.Printf("========================================\n")
		}

		// Clear batch
		batchTransactions = nil

		// Update for next batch
		currentBatch.BatchNum++
		currentBatch.StartTime = time.Now()
		currentBatch.BatchDb2CompressionTime = 0
		currentBatch.BatchGetLedgerTime = 0
	}

	// Process ledgers
	if config.UseRocksDB {
		// Parallel processing for RocksDB
		numWorkers := runtime.NumCPU()
		if numWorkers > 16 {
			numWorkers = 16
		}

		// Process in chunks for better memory management
		chunkSize := config.BatchSize
		for chunkStart := ledgerRange.From(); chunkStart < ledgerRange.To(); chunkStart += uint32(chunkSize) {
			chunkEnd := chunkStart + uint32(chunkSize) - 1
			if chunkEnd > ledgerRange.To() {
				chunkEnd = ledgerRange.To()
			}

			// Fetch ledgers in parallel
			getLedgerStart := time.Now()
			ledgerResults := fetchLedgersParallel(rocksReader, chunkStart, chunkEnd, numWorkers)
			getLedgerTime := time.Since(getLedgerStart)

			rocksdBTimingsForBatch := RocksDBTimingStats{}
			// Process results in order
			for _, result := range ledgerResults {
				// Accumulate RocksDB timing stats from the batch to the total time
				totalRocksDBTiming.ReadTime += result.Timing.ReadTime
				totalRocksDBTiming.DecompressTime += result.Timing.DecompressTime
				totalRocksDBTiming.UnmarshalTime += result.Timing.UnmarshalTime
				totalRocksDBTiming.TotalTime += result.Timing.TotalTime

				// Accumulate rocksdb timing stats for this batch to show at the end during batch stats reporting time
				rocksdBTimingsForBatch.ReadTime += result.Timing.ReadTime
				rocksdBTimingsForBatch.DecompressTime += result.Timing.DecompressTime
				rocksdBTimingsForBatch.UnmarshalTime += result.Timing.UnmarshalTime
				rocksdBTimingsForBatch.TotalTime += result.Timing.TotalTime

				if result.Err != nil {
					log.Printf("===== Warning: Failed to get ledger %d from RocksDB: %v, skipping =====\n", result.LedgerSeq, result.Err)
					skippedLedgerCount++
					continue
				}

				ledger := result.Ledger

				// Read all transactions from this ledger
				txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, ledger)
				if err != nil {
					log.Fatalf("Failed to create tx reader for ledger %d: %v", result.LedgerSeq, err)
				}

				closedAt := ledger.ClosedAt()
				for {
					tx, err := txReader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						log.Fatalf("Error reading transaction in ledger %d: %v", result.LedgerSeq, err)
					}
					batchTransactions = append(batchTransactions, rawTxData{
						tx:        tx,
						ledgerSeq: ledger.LedgerSequence(),
						closedAt:  closedAt,
					})
				}
				txReader.Close()

				processedLedgerCount++
			}

			totalTimingStats.GetLedgerTime += getLedgerTime
			currentBatch.BatchGetLedgerTime += getLedgerTime
			currentBatch.BatchRocksDBTimingStats = rocksdBTimingsForBatch

			//---------- Process batch ---------
			currentBatch.EndLedger = chunkEnd
			processTransactionsInCurrentBatch()
			currentBatch.StartLedger = chunkEnd + 1

			// Report database stats every 10 batches
			if currentBatch.BatchNum%10 == 0 {
				st := time.Now()
				log.Printf("\n========================================")
				log.Printf("========= MDBX STATS at ledger %d =====", chunkEnd)
				if config.EnableDB2 {
					stats, err := getMDBXStats(db2)
					if err == nil {
						logMDBXStats("DB2", stats)
					}
				}
				if config.EnableDB3 {
					stats, err := getMDBXStats(db3)
					if err == nil {
						logMDBXStats("DB3", stats)
					}
				}
				showCompressionStats(config, totalCompressionStats)
				showRocksDBTimingStats(totalRocksDBTiming, processedLedgerCount)
				log.Printf("Time taken for MDBX stats: %s", helpers.FormatDuration(time.Since(st)))
				log.Printf("========================================\n")
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

				log.Printf("\n========================================")
				log.Printf("PROGRESS: %d/%d ledgers (%d%%) | Ledger: %d",
					processedLedgerCount, totalLedgers, currentPercent, chunkEnd)
				if skippedLedgerCount > 0 {
					log.Printf("Skipped ledgers: %d", skippedLedgerCount)
				}
				log.Printf("Speed: %.2f ledgers/sec | Transactions: %s | ETA: %s",
					ledgersPerSec, helpers.FormatNumber(totalCompressionStats.TxCount), helpers.FormatDuration(eta))
				log.Printf("========================================\n")
				lastReportedPercent = currentPercent
			}
		}
	} else {
		// Sequential processing for GCS backend
		for ledgerSeq := ledgerRange.From(); ledgerSeq < ledgerRange.To(); ledgerSeq++ {
			getLedgerStart := time.Now()
			ledger, err := backend.GetLedger(ctx, ledgerSeq)
			getLedgerTime := time.Since(getLedgerStart)

			if err != nil {
				log.Fatalf("Failed to retrieve ledger %d: %v", ledgerSeq, err)
			}

			totalTimingStats.GetLedgerTime += getLedgerTime
			currentBatch.BatchGetLedgerTime += getLedgerTime

			// Read all transactions from this ledger
			txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, ledger)
			if err != nil {
				log.Fatalf("Failed to create tx reader for ledger %d: %v", ledgerSeq, err)
			}

			closedAt := ledger.ClosedAt()
			for {
				tx, err := txReader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatalf("Error reading transaction in ledger %d: %v", ledgerSeq, err)
				}
				batchTransactions = append(batchTransactions, rawTxData{
					tx:        tx,
					ledgerSeq: ledger.LedgerSequence(),
					closedAt:  closedAt,
				})
			}
			txReader.Close()

			processedLedgerCount++

			// Process batch when we hit batch size
			if processedLedgerCount%config.BatchSize == 0 {
				currentBatch.EndLedger = ledgerSeq
				processTransactionsInCurrentBatch()
				currentBatch.StartLedger = ledgerSeq + 1
			}

			// Report database stats every 10 batches
			if processedLedgerCount%(config.BatchSize*10) == 0 {
				st := time.Now()
				log.Printf("\n========================================")
				log.Printf("========= MDBX STATS at ledger %d =====", ledgerSeq)
				if config.EnableDB2 {
					stats, err := getMDBXStats(db2)
					if err == nil {
						logMDBXStats("DB2", stats)
					}
				}
				if config.EnableDB3 {
					stats, err := getMDBXStats(db3)
					if err == nil {
						logMDBXStats("DB3", stats)
					}
				}
				showCompressionStats(config, totalCompressionStats)
				log.Printf("Time taken for MDBX stats: %s", helpers.FormatDuration(time.Since(st)))
				log.Printf("========================================\n")
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

				log.Printf("\n========================================")
				log.Printf("PROGRESS: %d/%d ledgers (%d%%) | Ledger: %d",
					processedLedgerCount, totalLedgers, currentPercent, ledgerSeq)
				if skippedLedgerCount > 0 {
					log.Printf("Skipped ledgers: %d", skippedLedgerCount)
				}
				log.Printf("Speed: %.2f ledgers/sec | Transactions: %s | ETA: %s",
					ledgersPerSec, helpers.FormatNumber(totalCompressionStats.TxCount), helpers.FormatDuration(eta))
				log.Printf("========================================\n")
				lastReportedPercent = currentPercent
			}
		}

		// Process remaining batch
		if len(batchTransactions) > 0 {
			currentBatch.EndLedger = ledgerRange.To()
			processTransactionsInCurrentBatch()
		}
	}

	// Final sync to ensure all data is durably written
	log.Printf("\n========================================")
	log.Printf("Performing final sync to disk...")
	log.Printf("========================================\n")

	if config.EnableDB2 && db2 != nil {
		syncStart := time.Now()
		if err := db2.Env.Sync(true, false); err != nil {
			log.Printf("Warning: Failed to sync DB2: %v", err)
		}
		syncDuration := time.Since(syncStart)
		totalSyncTime += syncDuration
		log.Printf("DB2 final sync: %s", helpers.FormatDuration(syncDuration))
	}

	if config.EnableDB3 && db3 != nil {
		syncStart := time.Now()
		if err := db3.Env.Sync(true, false); err != nil {
			log.Printf("Warning: Failed to sync DB3: %v", err)
		}
		syncDuration := time.Since(syncStart)
		totalSyncTime += syncDuration
		log.Printf("DB3 final sync: %s", helpers.FormatDuration(syncDuration))
	}

	log.Printf("✅ Final sync complete\n")

	// Final statistics
	elapsed := time.Since(startTime)
	log.Printf("\n========================================")
	log.Printf("INGESTION COMPLETE")
	log.Printf("========================================")
	log.Printf("Total ledgers processed: %s", helpers.FormatNumber(int64(processedLedgerCount)))
	if skippedLedgerCount > 0 {
		log.Printf("Total ledgers skipped: %s", helpers.FormatNumber(int64(skippedLedgerCount)))
	}
	log.Printf("Total transactions: %s", helpers.FormatNumber(totalCompressionStats.TxCount))
	log.Printf("Total time: %s", helpers.FormatDuration(elapsed))
	log.Printf("Average speed: %.2f ledgers/sec", float64(processedLedgerCount)/elapsed.Seconds())
	log.Printf("")
	log.Printf("Time breakdown:")
	log.Printf("  GetLedger: %s (%.1f%%)", helpers.FormatDuration(totalTimingStats.GetLedgerTime),
		100*totalTimingStats.GetLedgerTime.Seconds()/elapsed.Seconds())

	if config.UseRocksDB {
		log.Printf("    RocksDB Read: %s", helpers.FormatDuration(totalRocksDBTiming.ReadTime))
		log.Printf("    Decompress: %s", helpers.FormatDuration(totalRocksDBTiming.DecompressTime))
		log.Printf("    Unmarshal: %s", helpers.FormatDuration(totalRocksDBTiming.UnmarshalTime))
	}

	log.Printf("  Compression: %s (%.1f%%)", helpers.FormatDuration(totalTimingStats.DB2CompressionTime),
		100*totalTimingStats.DB2CompressionTime.Seconds()/elapsed.Seconds())

	if config.EnableDB2 {
		db2Total := totalTimingStats.DB2WriteTime + totalTimingStats.DB2SyncTime
		log.Printf("  DB2 I/O: %s (write: %s, sync: %s)",
			helpers.FormatDuration(db2Total),
			helpers.FormatDuration(totalTimingStats.DB2WriteTime),
			helpers.FormatDuration(totalTimingStats.DB2SyncTime))
	}
	if config.EnableDB3 {
		db3Total := totalTimingStats.DB3WriteTime + totalTimingStats.DB3SyncTime
		log.Printf("  DB3 I/O: %s (write: %s, sync: %s)",
			helpers.FormatDuration(db3Total),
			helpers.FormatDuration(totalTimingStats.DB3WriteTime),
			helpers.FormatDuration(totalTimingStats.DB3SyncTime))
	}

	log.Printf("  Total Sync Time: %s (%.1f%%)", helpers.FormatDuration(totalSyncTime),
		100*totalSyncTime.Seconds()/elapsed.Seconds())

	// Final database stats
	log.Printf("\n========= FINAL MDBX STATS =====")
	if config.EnableDB2 {
		stats, err := getMDBXStats(db2)
		if err == nil {
			logMDBXStats("DB2", stats)
		}
	}
	if config.EnableDB3 {
		stats, err := getMDBXStats(db3)
		if err == nil {
			logMDBXStats("DB3", stats)
		}
	}

	showCompressionStats(config, totalCompressionStats)
	if config.UseRocksDB {
		showRocksDBTimingStats(totalRocksDBTiming, processedLedgerCount)
	}
	log.Printf("========================================")
}

// fetchLedgersParallel fetches a range of ledgers in parallel from RocksDB
func fetchLedgersParallel(reader *RocksDBReader, startSeq, endSeq uint32, numWorkers int) []LedgerResult {
	numLedgers := int(endSeq - startSeq + 1)
	results := make([]LedgerResult, numLedgers)

	// Create job channel
	jobs := make(chan uint32, numLedgers)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Each worker gets its own decoder for thread safety
			decoder, err := zstd.NewReader(nil)
			if err != nil {
				log.Printf("Failed to create decoder: %v", err)
				return
			}
			defer decoder.Close()

			for ledgerSeq := range jobs {
				idx := int(ledgerSeq - startSeq)
				results[idx] = fetchSingleLedger(reader, decoder, ledgerSeq)
			}
		}()
	}

	// Send jobs
	for seq := startSeq; seq <= endSeq; seq++ {
		jobs <- seq
	}
	close(jobs)

	// Wait for completion
	wg.Wait()

	return results
}

// fetchSingleLedger fetches and unmarshals a single ledger
func fetchSingleLedger(reader *RocksDBReader, decoder *zstd.Decoder, ledgerSeq uint32) LedgerResult {
	result := LedgerResult{
		LedgerSeq: ledgerSeq,
	}

	totalStart := time.Now()

	// Convert ledger sequence to 4-byte big-endian key
	key := helpers.Uint32ToBytes(ledgerSeq)

	// Read from RocksDB
	readStart := time.Now()
	slice, err := reader.db.Get(reader.ro, key)
	result.Timing.ReadTime = time.Since(readStart)

	if err != nil {
		result.Timing.TotalTime = time.Since(totalStart)
		result.Err = errors.Wrap(err, "failed to read from RocksDB")
		return result
	}
	defer slice.Free()

	// Check if key exists
	if !slice.Exists() {
		result.Timing.TotalTime = time.Since(totalStart)
		result.Err = fmt.Errorf("ledger %d not found in RocksDB", ledgerSeq)
		return result
	}

	// Copy data since slice will be freed
	compressedData := make([]byte, len(slice.Data()))
	copy(compressedData, slice.Data())

	// Decompress zstd data
	decompressStart := time.Now()
	uncompressedData, err := decoder.DecodeAll(compressedData, nil)
	result.Timing.DecompressTime = time.Since(decompressStart)

	if err != nil {
		result.Timing.TotalTime = time.Since(totalStart)
		result.Err = errors.Wrap(err, "failed to decompress ledger data")
		return result
	}

	// Unmarshal XDR to LedgerCloseMeta
	unmarshalStart := time.Now()
	err = result.Ledger.UnmarshalBinary(uncompressedData)
	result.Timing.UnmarshalTime = time.Since(unmarshalStart)

	if err != nil {
		result.Timing.TotalTime = time.Since(totalStart)
		result.Err = errors.Wrap(err, "failed to unmarshal LedgerCloseMeta")
		return result
	}

	result.Timing.TotalTime = time.Since(totalStart)
	return result
}

// openRocksDBReader opens a RocksDB database for reading
func openRocksDBReader(path string) (*RocksDBReader, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	// Open in read-only mode
	db, err := grocksdb.OpenDbForReadOnly(opts, path, false)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open RocksDB")
	}

	// Create reusable read options
	ro := grocksdb.NewDefaultReadOptions()

	return &RocksDBReader{
		db: db,
		ro: ro,
	}, nil
}

// Close closes the RocksDB reader and its resources
func (r *RocksDBReader) Close() {
	if r.ro != nil {
		r.ro.Destroy()
	}
	if r.db != nil {
		r.db.Close()
	}
}

// showRocksDBTimingStats displays RocksDB timing statistics
func showRocksDBTimingStats(timing RocksDBTimingStats, ledgerCount int) {
	log.Printf("\nRocksDB Timing Statistics:")
	log.Printf("  Total Read Time: %s", helpers.FormatDuration(timing.ReadTime))
	log.Printf("  Total Decompress Time: %s", helpers.FormatDuration(timing.DecompressTime))
	log.Printf("  Total Unmarshal Time: %s", helpers.FormatDuration(timing.UnmarshalTime))
	log.Printf("  Total Time: %s", helpers.FormatDuration(timing.TotalTime))

	if ledgerCount > 0 {
		avgRead := timing.ReadTime / time.Duration(ledgerCount)
		avgDecompress := timing.DecompressTime / time.Duration(ledgerCount)
		avgUnmarshal := timing.UnmarshalTime / time.Duration(ledgerCount)
		avgTotal := timing.TotalTime / time.Duration(ledgerCount)

		log.Printf("  Avg per ledger - Read: %s, Decompress: %s, Unmarshal: %s, Total: %s",
			helpers.FormatDuration(avgRead), helpers.FormatDuration(avgDecompress),
			helpers.FormatDuration(avgUnmarshal), helpers.FormatDuration(avgTotal))
	}
}

type result struct {
	hash             string
	compressedData   []byte
	uncompressedSize int64
	compressedSize   int64
	ledgerSeq        uint32
	err              error
}

// Use the same struct type for jobs
type jobData struct {
	tx        ingest.LedgerTransaction
	ledgerSeq uint32
	closedAt  time.Time
}

// compressTransactionsFromBatchInParallel processes all transactions in parallel
func compressTransactionsFromBatchInParallel(
	transactions []rawTxData,
	txHashToTxData map[string][]byte,
	txHashToLedgerSeq map[string]uint32,
	config IngestionConfig,
) (*CompressionStats, error) {
	stats := &CompressionStats{}

	if len(transactions) == 0 {
		return stats, nil
	}

	numWorkers := runtime.NumCPU()
	if numWorkers > 16 {
		numWorkers = 16
	}

	jobs := make(chan jobData, len(transactions))
	results := make(chan result, len(transactions))

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var localEncoder *zstd.Encoder
			if config.EnableApplicationCompression {
				localEncoder, _ = zstd.NewWriter(nil)
				defer localEncoder.Close()
			}

			for job := range jobs {
				res := result{
					hash:      job.tx.Hash.HexString(),
					ledgerSeq: job.ledgerSeq,
				}

				// Marshal
				txEnvelopeBytes, err := job.tx.Envelope.MarshalBinary()
				if err != nil {
					res.err = fmt.Errorf("error marshalling tx envelope: %w", err)
					results <- res
					continue
				}

				txResultBytes, err := job.tx.Result.MarshalBinary()
				if err != nil {
					res.err = fmt.Errorf("error marshalling tx result: %w", err)
					results <- res
					continue
				}

				txMetaBytes, err := job.tx.UnsafeMeta.MarshalBinary()
				if err != nil {
					res.err = fmt.Errorf("error marshalling tx meta: %w", err)
					results <- res
					continue
				}

				// Create protobuf
				txDataProto := tx_data.TxData{
					LedgerSequence: job.ledgerSeq,
					ClosedAt:       timestamppb.New(job.closedAt),
					Index:          job.tx.Index,
					TxEnvelope:     txEnvelopeBytes,
					TxResult:       txResultBytes,
					TxMeta:         txMetaBytes,
				}

				txDataBytes, err := proto.Marshal(&txDataProto)
				if err != nil {
					res.err = fmt.Errorf("error marshalling proto: %w", err)
					results <- res
					continue
				}

				res.uncompressedSize = int64(len(txDataBytes))

				// Compress
				if config.EnableApplicationCompression {
					res.compressedData = localEncoder.EncodeAll(txDataBytes, make([]byte, 0, len(txDataBytes)))
					res.compressedSize = int64(len(res.compressedData))
				} else {
					res.compressedData = txDataBytes
					res.compressedSize = int64(len(txDataBytes))
				}

				results <- res
			}
		}()
	}

	// Send ALL jobs
	for _, tx := range transactions {
		jobs <- jobData{
			tx:        tx.tx,
			ledgerSeq: tx.ledgerSeq,
			closedAt:  tx.closedAt,
		}
	}
	close(jobs)

	// Collect results
	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		if res.err != nil {
			return stats, res.err
		}

		stats.UncompressedTx += res.uncompressedSize
		stats.CompressedTx += res.compressedSize
		stats.TxCount++

		if config.EnableDB2 {
			txHashToTxData[res.hash] = res.compressedData
		}
		if config.EnableDB3 {
			txHashToLedgerSeq[res.hash] = res.ledgerSeq
		}
	}

	return stats, nil
}

// openMDBXDatabase opens or creates an MDBX database
func openMDBXDatabase(path string, name string, config IngestionConfig) (*MDBXDatabase, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, errors.Wrap(err, "failed to create directory")
	}

	// Create environment
	env, err := mdbx.NewEnv()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create mdbx environment")
	}

	// Set geometry (size limits)
	err = env.SetGeometry(
		200*GB,                 // size_lower: 200 GB initial
		-1,                     // size_now: -1 = use default
		2*TB,                   // size_upper: 2 TB maximum
		200*GB,                 // growth_step: 200 GB per growth
		-1,                     // shrink_threshold: -1 = disabled
		int(config.DbPageSize), // pagesize
	)
	if err != nil {
		env.Close()
		return nil, errors.Wrap(err, "failed to set geometry")
	}

	// Set option for max DBs BEFORE opening
	err = env.SetOption(mdbx.OptMaxDB, uint64(2))
	if err != nil {
		env.Close()
		return nil, errors.Wrap(err, "failed to set max dbs")
	}

	// Open environment with SafeNoSync for fast bulk ingestion
	// SafeNoSync: Don't sync on commit, but keep database recoverable
	err = env.Open(path, mdbx.NoSubdir|mdbx.WriteMap|mdbx.SafeNoSync, 0644)
	if err != nil {
		env.Close()
		return nil, errors.Wrap(err, "failed to open database")
	}

	// Open the DBI (database instance)
	var dbi mdbx.DBI
	err = env.Update(func(txn *mdbx.Txn) error {
		var err error
		dbi, err = txn.OpenDBI("data", mdbx.Create, nil, nil)
		return err
	})
	if err != nil {
		env.Close()
		return nil, errors.Wrap(err, "failed to open DBI")
	}

	log.Printf("✓ %s opened successfully", name)
	log.Printf("  Path: %s", path)
	log.Printf("  Page size: %d bytes", config.DbPageSize)
	log.Printf("  Mode: SafeNoSync (fast bulk ingestion)")

	return &MDBXDatabase{
		Env:  env,
		DBI:  dbi,
		Path: path,
	}, nil
}

// Close closes the MDBX database
func (db *MDBXDatabase) Close() {
	if db.Env != nil {
		db.Env.Close()
	}
}

// writeBatchToDatabases writes batch data to MDBX databases
func writeBatchToDatabases(
	db2, db3 *MDBXDatabase,
	txHashToTxData map[string][]byte,
	txHashToLedgerSeq map[string]uint32,
	config IngestionConfig,
) (DBTimingStats, error) {
	var timing DBTimingStats

	// Write to DB2 (txHash -> compressed TxData)
	if config.EnableDB2 && db2 != nil {
		writeStart := time.Now()
		err := db2.Env.Update(func(txn *mdbx.Txn) error {
			for txHashHex, compressedTxData := range txHashToTxData {
				txHashBytes, err := helpers.HexStringToBytes(txHashHex)
				if err != nil {
					return fmt.Errorf("failed to convert tx hash %s: %w", txHashHex, err)
				}
				err = txn.Put(db2.DBI, txHashBytes, compressedTxData, mdbx.Upsert)
				if err != nil {
					return fmt.Errorf("failed to put tx %s: %w", txHashHex, err)
				}
			}
			return nil
		})
		timing.DB2WriteTime = time.Since(writeStart)
		if err != nil {
			return timing, errors.Wrap(err, "failed to write to DB2")
		}
		timing.DB2SyncTime = 0
	}

	// Write to DB3 (txHash -> ledgerSeq)
	if config.EnableDB3 && db3 != nil {
		writeStart := time.Now()
		err := db3.Env.Update(func(txn *mdbx.Txn) error {
			for txHashHex, ledgerSeq := range txHashToLedgerSeq {
				txHashBytes, err := helpers.HexStringToBytes(txHashHex)
				if err != nil {
					return fmt.Errorf("failed to convert tx hash %s: %w", txHashHex, err)
				}
				value := helpers.Uint32ToBytes(ledgerSeq)
				err = txn.Put(db3.DBI, txHashBytes, value, mdbx.Upsert)
				if err != nil {
					return fmt.Errorf("failed to put tx %s: %w", txHashHex, err)
				}
			}
			return nil
		})
		timing.DB3WriteTime = time.Since(writeStart)
		if err != nil {
			return timing, errors.Wrap(err, "failed to write to DB3")
		}
		timing.DB3SyncTime = 0
	}

	return timing, nil
}

// getMDBXStats retrieves statistics about an MDBX database
func getMDBXStats(db *MDBXDatabase) (MDBXStats, error) {
	var stats MDBXStats

	err := db.Env.View(func(txn *mdbx.Txn) error {
		// Get transaction statistics
		stat, err := txn.StatDBI(db.DBI)
		if err != nil {
			return err
		}

		// Get environment info
		envInfo, err := db.Env.Info(txn)
		if err != nil {
			return err
		}

		stats.PageSize = uint(stat.PSize)
		stats.Depth = uint(stat.Depth)
		stats.BranchPages = stat.BranchPages
		stats.LeafPages = stat.LeafPages
		stats.OverflowPages = stat.OverflowPages
		stats.Entries = stat.Entries
		stats.MapSize = uint64(envInfo.MapSize)
		stats.LastPageNum = envInfo.MiLastPgNo
		stats.RecentTxnID = uint64(envInfo.LastTxnID)

		return nil
	})

	return stats, err
}

// logMDBXStats logs MDBX statistics
func logMDBXStats(name string, stats MDBXStats) {
	log.Printf("[%s] MDBX Statistics:", name)
	log.Printf("  Page size: %d bytes", stats.PageSize)
	log.Printf("  Tree depth: %d", stats.Depth)
	log.Printf("  Branch pages: %s", helpers.FormatNumber(int64(stats.BranchPages)))
	log.Printf("  Leaf pages: %s", helpers.FormatNumber(int64(stats.LeafPages)))
	log.Printf("  Overflow pages: %s", helpers.FormatNumber(int64(stats.OverflowPages)))
	log.Printf("  Total entries: %s", helpers.FormatNumber(int64(stats.Entries)))
	log.Printf("  Map size: %s", helpers.FormatBytes(int64(stats.MapSize)))
	log.Printf("  Last page: %s", helpers.FormatNumber(int64(stats.LastPageNum)))
	log.Printf("  Recent txn ID: %d", stats.RecentTxnID)

	// Calculate storage efficiency
	totalPages := stats.BranchPages + stats.LeafPages + stats.OverflowPages
	usedSize := totalPages * uint64(stats.PageSize)
	log.Printf("  Used size: %s", helpers.FormatBytes(int64(usedSize)))

	if stats.Entries > 0 {
		avgEntrySize := float64(usedSize) / float64(stats.Entries)
		log.Printf("  Avg entry size: %.2f bytes", avgEntrySize)
	}
}

// logBatchCompletion logs batch completion information
func logBatchCompletion(batch BatchInfo, timing DBTimingStats, config IngestionConfig) {
	totalBatchDuration := time.Since(batch.StartTime)
	cpuTime := batch.BatchGetLedgerTime + batch.BatchDb2CompressionTime
	ioTime := timing.DB2WriteTime + timing.DB2SyncTime + timing.DB3WriteTime + timing.DB3SyncTime
	computeTime := totalBatchDuration - cpuTime - ioTime

	log.Printf("\n===== Batch #%d Complete [Ledger %d-%d] (%d transactions) =====",
		batch.BatchNum, batch.StartLedger, batch.EndLedger, batch.TxCount)
	log.Printf("Total batch time: %s", helpers.FormatDuration(totalBatchDuration))
	log.Printf("\t GetLedger Time: %s", helpers.FormatDuration(batch.BatchGetLedgerTime))
	log.Printf("\t Compute Time: %s", helpers.FormatDuration(computeTime))

	if config.EnableDB2 {
		db2Total := timing.DB2WriteTime + timing.DB2SyncTime
		log.Printf("\t DB2:: %s", config.DB2Path)
		log.Printf("\t\t CompressionTime: %s", helpers.FormatDuration(batch.BatchDb2CompressionTime))
		log.Printf("\t\t I/O time: %s (write: %s)", helpers.FormatDuration(db2Total), helpers.FormatDuration(timing.DB2WriteTime))
	}

	if config.EnableDB3 {
		db3Total := timing.DB3WriteTime + timing.DB3SyncTime
		log.Printf("\t DB3:: %s", config.DB3Path)
		log.Printf("\t\t I/O time: %s (write: %s)", helpers.FormatDuration(db3Total), helpers.FormatDuration(timing.DB3WriteTime))
	}

	numLedgers := batch.EndLedger - batch.StartLedger
	if numLedgers > 0 {
		avgTimePerLedger := totalBatchDuration / time.Duration(numLedgers)
		log.Printf("\nAvg time per ledger in batch: %s", helpers.FormatDuration(avgTimePerLedger))
	}
	log.Printf("========================================\n")
}

// showCompressionStats shows compression statistics
func showCompressionStats(config IngestionConfig, stats CompressionStats) {
	if config.EnableApplicationCompression && stats.UncompressedTx > 0 {
		compressionRatio := 100 * (1 - float64(stats.CompressedTx)/float64(stats.UncompressedTx))
		log.Printf("\nCompression Statistics:")
		log.Printf("  Original size: %s", helpers.FormatBytes(stats.UncompressedTx))
		log.Printf("  Compressed size: %s", helpers.FormatBytes(stats.CompressedTx))
		log.Printf("  Compression ratio: %.2f%% reduction", compressionRatio)
		log.Printf("  Space saved: %s", helpers.FormatBytes(stats.UncompressedTx-stats.CompressedTx))
	}
}
