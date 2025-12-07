// main.go
// =============================================================================
// Stellar RocksDB Ingestion - Main Entry Point
// =============================================================================
//
// This is the main entry point for the Stellar RocksDB ingestion tool.
// It handles:
// - Command-line argument parsing
// - Configuration loading and validation
// - Main processing loop (batch-based)
// - Progress reporting
// - Final summary and cleanup
//
// USAGE:
// ======
// ./stellar_rocksdb_ingestion \
//     --config /path/to/config.toml \
//     --ledger-batch-size 2000 \
//     --start-ledger 50000000 \
//     --end-ledger 55000000 \
//     --start-time "2022-01-01T00:00:00+00:00" \
//     --end-time "2022-12-31T23:59:59+00:00" \
//     --enable-tx-hash-to-ledger-seq \
//     --enable-tx-hash-to-tx-data
//
// =============================================================================

package main

import (
	"context"
	"flag"
	"log"
	"runtime"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/pkg/errors"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
)

func main() {
	// =========================================================================
	// Parse Command-Line Arguments
	// =========================================================================
	var (
		configPath              string
		ledgerBatchSize         int
		startLedger, endLedger  uint
		startTime, endTime      string
		enableLedgerSeqToLcm    bool
		enableTxHashToTxData    bool
		enableTxHashToLedgerSeq bool
	)

	flag.StringVar(&configPath, "config", "", "Path to TOML configuration file (required)")
	flag.IntVar(&ledgerBatchSize, "ledger-batch-size", 0, "Number of ledgers per batch (required)")
	flag.UintVar(&startLedger, "start-ledger", 0, "Starting ledger sequence number (required)")
	flag.UintVar(&endLedger, "end-ledger", 0, "Ending ledger sequence number (required)")
	flag.StringVar(&startTime, "start-time", "", "Start time for logging (optional, from shell script)")
	flag.StringVar(&endTime, "end-time", "", "End time for logging (optional, from shell script)")
	flag.BoolVar(&enableLedgerSeqToLcm, "enable-ledger-seq-to-lcm", false, "Enable ledger_seq_to_lcm store")
	flag.BoolVar(&enableTxHashToTxData, "enable-tx-hash-to-tx-data", false, "Enable tx_hash_to_tx_data store")
	flag.BoolVar(&enableTxHashToLedgerSeq, "enable-tx-hash-to-ledger-seq", false, "Enable tx_hash_to_ledger_seq store")

	flag.Parse()

	// =========================================================================
	// Validate Required Arguments
	// =========================================================================
	if configPath == "" {
		log.Fatal("ERROR: --config is required")
	}
	if ledgerBatchSize <= 0 {
		log.Fatal("ERROR: --ledger-batch-size is required and must be positive")
	}
	if startLedger == 0 || endLedger == 0 {
		log.Fatal("ERROR: --start-ledger and --end-ledger are required")
	}
	if !enableLedgerSeqToLcm && !enableTxHashToTxData && !enableTxHashToLedgerSeq {
		log.Fatal("ERROR: At least one store must be enabled (--enable-ledger-seq-to-lcm, --enable-tx-hash-to-tx-data, or --enable-tx-hash-to-ledger-seq)")
	}

	// =========================================================================
	// Load and Validate Configuration
	// =========================================================================
	log.Printf("Loading configuration from: %s", configPath)

	tomlConfig, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("ERROR: Failed to load configuration: %v", err)
	}

	runtimeCfg := &RuntimeConfig{
		ConfigPath:              configPath,
		LedgerBatchSize:         ledgerBatchSize,
		StartLedger:             uint32(startLedger),
		EndLedger:               uint32(endLedger),
		StartTime:               startTime,
		EndTime:                 endTime,
		EnableLedgerSeqToLcm:    enableLedgerSeqToLcm,
		EnableTxHashToTxData:    enableTxHashToTxData,
		EnableTxHashToLedgerSeq: enableTxHashToLedgerSeq,
	}

	config, err := ValidateAndMerge(tomlConfig, runtimeCfg)
	if err != nil {
		log.Fatalf("ERROR: Configuration validation failed: %v", err)
	}

	// =========================================================================
	// Print Configuration Summary
	// =========================================================================
	log.Print(config.GetConfigSummary())

	// =========================================================================
	// Initialize Stores
	// =========================================================================
	log.Printf("Initializing RocksDB stores...")
	log.Printf("")

	stores, err := OpenStores(config)
	if err != nil {
		log.Fatalf("ERROR: Failed to open stores: %v", err)
	}
	defer stores.Close()

	log.Printf("")
	log.Printf("All stores initialized successfully.")
	log.Printf("")

	// =========================================================================
	// Initialize Data Source (RocksDB or GCS)
	// =========================================================================
	ctx := context.Background()
	var backend *ledgerbackend.BufferedStorageBackend

	if !config.UseRocksDBSource {
		log.Printf("Initializing GCS data source...")

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

		dataStore, err := datastore.NewDataStore(ctx, datastoreConfig)
		if err != nil {
			log.Fatal(errors.Wrap(err, "failed to create GCS datastore"))
		}
		defer dataStore.Close()

		backendConfig := ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: 10000,
			NumWorkers: 200,
			RetryLimit: 3,
			RetryWait:  5 * time.Second,
		}

		backend, err = ledgerbackend.NewBufferedStorageBackend(backendConfig, dataStore, dataStoreSchema)
		if err != nil {
			log.Fatal(errors.Wrap(err, "failed to create buffered storage backend"))
		}
		defer backend.Close()

		ledgerRange := ledgerbackend.BoundedRange(config.StartLedger, config.EndLedger)
		if err := backend.PrepareRange(ctx, ledgerRange); err != nil {
			log.Fatal(errors.Wrapf(err, "failed to prepare ledger range: %v", ledgerRange))
		}

		log.Printf("✓ GCS data source initialized")
		log.Printf("")
	}

	// =========================================================================
	// Initialize Statistics and Progress Tracking
	// =========================================================================
	globalStats := NewGlobalStats()
	numWorkers := runtime.NumCPU()

	totalLedgers := int(config.EndLedger - config.StartLedger + 1)
	totalBatches := uint32((totalLedgers + config.LedgerBatchSize - 1) / config.LedgerBatchSize)

	lastReportedPercent := -1
	var globalMinLedger uint32 = 0xFFFFFFFF
	var globalMaxLedger uint32 = 0

	log.Printf("================================================================================")
	log.Printf("                          STARTING INGESTION")
	log.Printf("================================================================================")
	log.Printf("")
	log.Printf("  Workers:             %d", numWorkers)
	log.Printf("  Total Ledgers:       %s", helpers.FormatNumber(int64(totalLedgers)))
	log.Printf("  Total Batches:       %d", totalBatches)
	log.Printf("  Ledgers per Batch:   %d", config.LedgerBatchSize)
	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("")

	// =========================================================================
	// Main Processing Loop
	// =========================================================================
	currentBatch := &BatchStats{
		BatchNum:    1,
		StartLedger: config.StartLedger,
		StartTime:   time.Now(),
	}

	if config.UseRocksDBSource {
		// =================================================================
		// RocksDB Source Path: Parallel Ledger Fetching
		// =================================================================
		processRocksDBSource(
			config,
			stores,
			globalStats,
			currentBatch,
			numWorkers,
			totalLedgers,
			totalBatches,
			&lastReportedPercent,
			&globalMinLedger,
			&globalMaxLedger,
		)
	} else {
		// =================================================================
		// GCS Source Path: Sequential Ledger Fetching
		// =================================================================
		processGCSSource(
			ctx,
			backend,
			config,
			stores,
			globalStats,
			currentBatch,
			numWorkers,
			totalLedgers,
			totalBatches,
			&lastReportedPercent,
			&globalMinLedger,
			&globalMaxLedger,
		)
	}

	// =========================================================================
	// Final Compaction
	// =========================================================================
	compactTiming := stores.CompactAll(config, globalMinLedger, globalMaxLedger)
	globalStats.mu.Lock()
	globalStats.Timing.LcmCompactTime = compactTiming.LcmCompactTime
	globalStats.Timing.TxDataCompactTime = compactTiming.TxDataCompactTime
	globalStats.Timing.HashSeqCompactTime = compactTiming.HashSeqCompactTime
	globalStats.mu.Unlock()

	// =========================================================================
	// Final Summary
	// =========================================================================
	LogFinalSummary(globalStats, config, stores.LcmDB, stores.TxDataDB, stores.HashSeqDB, numWorkers)

	log.Printf("Ingestion completed successfully!")
}

// =============================================================================
// RocksDB Source Processing
// =============================================================================

func processRocksDBSource(
	config *IngestionConfig,
	stores *Stores,
	globalStats *GlobalStats,
	currentBatch *BatchStats,
	numWorkers int,
	totalLedgers int,
	totalBatches uint32,
	lastReportedPercent *int,
	globalMinLedger, globalMaxLedger *uint32,
) {
	batchSize := config.LedgerBatchSize

	for chunkStart := config.StartLedger; chunkStart <= config.EndLedger; chunkStart += uint32(batchSize) {
		chunkEnd := chunkStart + uint32(batchSize) - 1
		if chunkEnd > config.EndLedger {
			chunkEnd = config.EndLedger
		}

		currentBatch.StartLedger = chunkStart
		currentBatch.EndLedger = chunkEnd
		currentBatch.StartTime = time.Now()

		// Fetch ledgers in parallel
		getLedgerStart := time.Now()
		ledgerResults := FetchLedgersFromRocksDB(
			stores.SourceDB,
			stores.SourceRO,
			chunkStart,
			chunkEnd,
			numWorkers,
		)
		currentBatch.GetLedgerTime = time.Since(getLedgerStart)

		// Collect successful ledgers and transactions
		var lcms []LcmWithSeq
		var transactions []RawTxData
		ledgersInBatch := 0
		txCountInBatch := 0

		for _, result := range ledgerResults {
			// Accumulate RocksDB timing
			currentBatch.RocksDBReadTiming.ReadTime += result.Timing.ReadTime
			currentBatch.RocksDBReadTiming.DecompressTime += result.Timing.DecompressTime
			currentBatch.RocksDBReadTiming.UnmarshalTime += result.Timing.UnmarshalTime
			currentBatch.RocksDBReadTiming.TotalTime += result.Timing.TotalTime

			if result.Err != nil {
				log.Printf("⚠️  Warning: Failed to fetch ledger %d: %v (skipping)", result.LedgerSeq, result.Err)
				globalStats.AddSkippedLedgers(1)
				continue
			}

			ledgersInBatch++

			// Track min/max for final compaction
			if result.LedgerSeq < *globalMinLedger {
				*globalMinLedger = result.LedgerSeq
			}
			if result.LedgerSeq > *globalMaxLedger {
				*globalMaxLedger = result.LedgerSeq
			}

			// Store LCM if enabled
			if config.EnableLedgerSeqToLcm {
				lcms = append(lcms, LcmWithSeq{
					LedgerSeq: result.LedgerSeq,
					Lcm:       result.Ledger,
				})
			}

			// Extract transactions if needed
			if config.ProcessTransactions {
				txs, err := ExtractTransactions(result.Ledger)
				if err != nil {
					log.Fatalf("ERROR: Failed to extract transactions from ledger %d: %v", result.LedgerSeq, err)
				}
				transactions = append(transactions, txs...)
				txCountInBatch += len(txs)
			} else {
				// Just count transactions for stats
				count, err := CountTransactions(result.Ledger)
				if err != nil {
					log.Printf("⚠️  Warning: Failed to count transactions in ledger %d: %v", result.LedgerSeq, err)
				}
				txCountInBatch += count
			}
		}

		currentBatch.LedgerCount = ledgersInBatch
		currentBatch.TransactionCount = txCountInBatch

		// Process and write batch
		processBatchData(config, stores, globalStats, currentBatch, lcms, transactions, numWorkers, totalBatches, globalStats.StartTime)

		// Log progress
		LogProgress(globalStats, totalLedgers, chunkEnd, lastReportedPercent)

		// Log extended stats every 10 batches
		if currentBatch.BatchNum%10 == 0 {
			LogExtendedStats(globalStats, config, chunkEnd, stores.LcmDB, stores.TxDataDB, stores.HashSeqDB, numWorkers)
		}

		// Prepare for next batch
		currentBatch.BatchNum++
		resetBatchStats(currentBatch)
	}
}

// =============================================================================
// GCS Source Processing
// =============================================================================

func processGCSSource(
	ctx context.Context,
	backend *ledgerbackend.BufferedStorageBackend,
	config *IngestionConfig,
	stores *Stores,
	globalStats *GlobalStats,
	currentBatch *BatchStats,
	numWorkers int,
	totalLedgers int,
	totalBatches uint32,
	lastReportedPercent *int,
	globalMinLedger, globalMaxLedger *uint32,
) {
	batchSize := config.LedgerBatchSize

	var lcms []LcmWithSeq
	var transactions []RawTxData
	processedInCurrentBatch := 0

	for ledgerSeq := config.StartLedger; ledgerSeq <= config.EndLedger; ledgerSeq++ {
		// Initialize batch if this is the first ledger
		if processedInCurrentBatch == 0 {
			currentBatch.StartLedger = ledgerSeq
			currentBatch.StartTime = time.Now()
		}

		// Fetch ledger
		getLedgerStart := time.Now()
		ledger, err := backend.GetLedger(ctx, ledgerSeq)
		getLedgerTime := time.Since(getLedgerStart)

		if err != nil {
			log.Fatalf("ERROR: Failed to retrieve ledger %d: %v", ledgerSeq, err)
		}

		currentBatch.GetLedgerTime += getLedgerTime

		// Track min/max for final compaction
		if ledgerSeq < *globalMinLedger {
			*globalMinLedger = ledgerSeq
		}
		if ledgerSeq > *globalMaxLedger {
			*globalMaxLedger = ledgerSeq
		}

		// Store LCM if enabled
		if config.EnableLedgerSeqToLcm {
			lcms = append(lcms, LcmWithSeq{
				LedgerSeq: ledgerSeq,
				Lcm:       ledger,
			})
		}

		// Extract transactions if needed
		if config.ProcessTransactions {
			txs, err := ExtractTransactions(ledger)
			if err != nil {
				log.Fatalf("ERROR: Failed to extract transactions from ledger %d: %v", ledgerSeq, err)
			}
			transactions = append(transactions, txs...)
			currentBatch.TransactionCount += len(txs)
		} else {
			// Just count transactions for stats
			count, err := CountTransactions(ledger)
			if err != nil {
				log.Printf("⚠️  Warning: Failed to count transactions in ledger %d: %v", ledgerSeq, err)
			}
			currentBatch.TransactionCount += count
		}

		processedInCurrentBatch++
		currentBatch.LedgerCount = processedInCurrentBatch

		// Process batch when full or at the end
		isLastLedger := ledgerSeq == config.EndLedger
		isBatchFull := processedInCurrentBatch >= batchSize

		if isBatchFull || isLastLedger {
			currentBatch.EndLedger = ledgerSeq

			// Process and write batch
			processBatchData(config, stores, globalStats, currentBatch, lcms, transactions, numWorkers, totalBatches, globalStats.StartTime)

			// Log progress
			LogProgress(globalStats, totalLedgers, ledgerSeq, lastReportedPercent)

			// Log extended stats every 10 batches
			if currentBatch.BatchNum%10 == 0 {
				LogExtendedStats(globalStats, config, ledgerSeq, stores.LcmDB, stores.TxDataDB, stores.HashSeqDB, numWorkers)
			}

			// Prepare for next batch
			currentBatch.BatchNum++
			resetBatchStats(currentBatch)
			lcms = nil
			transactions = nil
			processedInCurrentBatch = 0
		}
	}
}

// =============================================================================
// Common Batch Processing
// =============================================================================

func processBatchData(
	config *IngestionConfig,
	stores *Stores,
	globalStats *GlobalStats,
	batch *BatchStats,
	lcms []LcmWithSeq,
	transactions []RawTxData,
	numWorkers int,
	totalBatches uint32,
	globalStartTime time.Time,
) {
	// Process batch (compress LCMs and transactions)
	processResult, err := ProcessBatch(config, lcms, transactions, numWorkers)
	if err != nil {
		log.Fatalf("ERROR: Failed to process batch: %v", err)
	}

	// Update batch timing
	batch.LcmCompressionTime = processResult.LcmCompressionTime
	batch.TxDataCompressionTime = processResult.TxDataCompressionTime

	// Update batch stats
	batch.LcmStats.UncompressedBytes = processResult.LcmUncompressed
	batch.LcmStats.CompressedBytes = processResult.LcmCompressed
	batch.LcmStats.EntryCount = processResult.LcmCount

	batch.TxDataStats.UncompressedBytes = processResult.TxDataUncompressed
	batch.TxDataStats.CompressedBytes = processResult.TxDataCompressed
	batch.TxDataStats.EntryCount = processResult.TxDataCount

	batch.HashSeqCount = processResult.HashSeqCount

	// Write batch to stores
	writeTiming, err := stores.WriteBatch(
		config,
		processResult.LcmData,
		processResult.TxDataMap,
		processResult.HashSeqMap,
		processResult.RawFileData,
	)
	if err != nil {
		log.Fatalf("ERROR: Failed to write batch to stores: %v", err)
	}

	// Update batch timing
	batch.LcmWriteTime = writeTiming.LcmWriteTime
	batch.TxDataWriteTime = writeTiming.TxDataWriteTime
	batch.HashSeqWriteTime = writeTiming.HashSeqWriteTime
	batch.RawFileWriteTime = writeTiming.RawFileWriteTime

	// Add batch stats to global stats
	globalStats.AddBatchStats(batch)

	// Log batch summary
	LogBatchSummary(batch, config, totalBatches, globalStartTime)
}

// resetBatchStats resets batch statistics for the next batch.
func resetBatchStats(batch *BatchStats) {
	batch.LedgerCount = 0
	batch.TransactionCount = 0
	batch.GetLedgerTime = 0
	batch.LcmCompressionTime = 0
	batch.TxDataCompressionTime = 0
	batch.LcmWriteTime = 0
	batch.TxDataWriteTime = 0
	batch.HashSeqWriteTime = 0
	batch.RawFileWriteTime = 0
	batch.LcmStats = CompressionStats{}
	batch.TxDataStats = CompressionStats{}
	batch.HashSeqCount = 0
	batch.RocksDBReadTiming = RocksDBReadTiming{}
}
