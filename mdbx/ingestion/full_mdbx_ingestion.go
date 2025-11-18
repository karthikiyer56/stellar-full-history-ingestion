package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/mdbx-go/mdbx" // ✅ Correct import for v0.38+
	"github.com/karthikiyer56/stellar-full-history-ingestion/tx_data"
	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
)

const GB = 1024 * 1024 * 1024
const TB = 1024 * GB

// IngestionConfig holds configuration for the ingestion process
type IngestionConfig struct {
	StartLedger                  uint32
	EndLedger                    uint32
	BatchSize                    int
	DB2Path                      string
	DB3Path                      string
	EnableDB2                    bool
	EnableDB3                    bool
	EnableApplicationCompression bool
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

// BatchInfo tracks information about the current batch
type BatchInfo struct {
	BatchNum           uint32
	StartLedger        uint32
	EndLedger          uint32
	StartTime          time.Time
	TxCount            int
	GetLedgerTime      time.Duration
	Db2CompressionTime time.Duration
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

func main() {
	// Command-line flags
	var startLedger, endLedger uint
	var batchSize int
	var db2Path, db3Path string
	var enableApplicationCompression bool

	flag.UintVar(&startLedger, "start-ledger", 0, "Starting ledger sequence number")
	flag.UintVar(&endLedger, "end-ledger", 0, "Ending ledger sequence number")
	flag.IntVar(&batchSize, "ledger-batch-size", 2000, "Ledger batch size for commit")
	flag.StringVar(&db2Path, "db2", "", "Path for DB2 (txHash -> compressed TxData)")
	flag.StringVar(&db3Path, "db3", "", "Path for DB3 (txHash -> ledgerSeq)")
	flag.BoolVar(&enableApplicationCompression, "app-compression", true, "Enable compression (default: true)")
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
	log.Printf("Starting MDBX ledger processing")
	log.Printf("========================================")
	log.Printf("Ledger range: %d - %d (%s ledgers)",
		ledgerRange.From(), ledgerRange.To(), formatNumber(int64(totalLedgers)))
	log.Printf("DB2 (TxData) storage: %v", config.EnableDB2)
	log.Printf("DB3 (Hash->Seq) storage: %v", config.EnableDB3)
	log.Printf("Application Compression: %v", config.EnableApplicationCompression)
	log.Printf("========================================\n")

	// Set up metrics tracking
	startTime := time.Now()
	processedLedgerCount := 0
	lastReportedPercent := -1

	var totalTimingStats DBTimingStats
	var totalCompressionStats CompressionStats

	// Batch data
	var batchTransactions []rawTxData

	// Track current batch
	currentBatch := BatchInfo{
		BatchNum:    1,
		StartLedger: ledgerRange.From(),
		StartTime:   time.Now(),
	}

	// Helper function to process a batch
	processBatch := func() {
		if len(batchTransactions) == 0 {
			return
		}

		// Process all transactions in parallel
		compressionStart := time.Now()

		txHashToTxData := make(map[string][]byte)
		txHashToLedgerSeq := make(map[string]uint32)

		stats, err := processTransactionBatch(
			batchTransactions,
			txHashToTxData,
			txHashToLedgerSeq,
			config,
		)
		if err != nil {
			log.Fatalf("Failed to process batch: %v", err)
		}

		compressionTime := time.Since(compressionStart)
		totalTimingStats.DB2CompressionTime += compressionTime
		currentBatch.Db2CompressionTime = compressionTime

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

		// Clear batch
		batchTransactions = nil

		// Update for next batch
		currentBatch.BatchNum++
		currentBatch.StartTime = time.Now()
		currentBatch.Db2CompressionTime = 0
		currentBatch.GetLedgerTime = 0
	}

	// Iterate through ledgers
	for ledgerSeq := ledgerRange.From(); ledgerSeq < ledgerRange.To(); ledgerSeq++ {
		// Get ledger
		getLedgerStart := time.Now()
		ledger, err := backend.GetLedger(ctx, ledgerSeq)
		if err != nil {
			log.Fatalf("Failed to retrieve ledger %d: %v", ledgerSeq, err)
		}
		getLedgerTime := time.Since(getLedgerStart)

		totalTimingStats.GetLedgerTime += getLedgerTime
		currentBatch.GetLedgerTime += getLedgerTime

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
			processBatch()
			currentBatch.StartLedger = ledgerSeq + 1
		}

		// Report database stats every 10 batches
		if processedLedgerCount%(config.BatchSize*10) == 0 {
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
			log.Printf("Speed: %.2f ledgers/sec | Transactions: %s | ETA: %s",
				ledgersPerSec, formatNumber(totalCompressionStats.TxCount), formatDuration(eta))
			log.Printf("========================================\n")
			lastReportedPercent = currentPercent
		}
	}

	// Process remaining batch (the last batch that may not be full)
	if len(batchTransactions) > 0 {
		currentBatch.EndLedger = ledgerRange.To()
		processBatch()
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
		log.Printf("DB2 final sync: %s", formatDuration(time.Since(syncStart)))
	}

	if config.EnableDB3 && db3 != nil {
		syncStart := time.Now()
		if err := db3.Env.Sync(true, false); err != nil {
			log.Printf("Warning: Failed to sync DB3: %v", err)
		}
		log.Printf("DB3 final sync: %s", formatDuration(time.Since(syncStart)))
	}

	log.Printf("✅ Final sync complete\n")

	// Final statistics
	elapsed := time.Since(startTime)
	log.Printf("\n========================================")
	log.Printf("INGESTION COMPLETE")
	log.Printf("========================================")
	log.Printf("Total ledgers processed: %s", formatNumber(int64(processedLedgerCount)))
	log.Printf("Total transactions: %s", formatNumber(totalCompressionStats.TxCount))
	log.Printf("Total time: %s", formatDuration(elapsed))
	log.Printf("Average speed: %.2f ledgers/sec", float64(processedLedgerCount)/elapsed.Seconds())
	log.Printf("")
	log.Printf("Time breakdown:")
	log.Printf("  GetLedger: %s (%.1f%%)", formatDuration(totalTimingStats.GetLedgerTime),
		100*totalTimingStats.GetLedgerTime.Seconds()/elapsed.Seconds())
	log.Printf("  Compression: %s (%.1f%%)", formatDuration(totalTimingStats.DB2CompressionTime),
		100*totalTimingStats.DB2CompressionTime.Seconds()/elapsed.Seconds())

	if config.EnableDB2 {
		db2Total := totalTimingStats.DB2WriteTime + totalTimingStats.DB2SyncTime
		log.Printf("  DB2 I/O: %s (write: %s, sync: %s)",
			formatDuration(db2Total),
			formatDuration(totalTimingStats.DB2WriteTime),
			formatDuration(totalTimingStats.DB2SyncTime))
	}
	if config.EnableDB3 {
		db3Total := totalTimingStats.DB3WriteTime + totalTimingStats.DB3SyncTime
		log.Printf("  DB3 I/O: %s (write: %s, sync: %s)",
			formatDuration(db3Total),
			formatDuration(totalTimingStats.DB3WriteTime),
			formatDuration(totalTimingStats.DB3SyncTime))
	}

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
	log.Printf("========================================")
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

// processTransactionBatch processes all transactions in parallel
func processTransactionBatch(
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
		200*GB, // size_lower: 200 GB initial
		-1,     // size_now: -1 = use default
		2*TB,   // size_upper: 2 TB maximum
		200*GB, // growth_step: 200 GB per growth
		-1,     // shrink_threshold: -1 = disabled
		8192,   // pagesize: 4 KB
	)
	if err != nil {
		env.Close()
		return nil, errors.Wrap(err, "failed to set geometry")
	}

	// SetMaxReaders removed in v0.38+ - it's auto-configured

	// Set option for max DBs BEFORE opening
	// In v0.38.2, use SetOption instead of SetMaxDBs
	err = env.SetOption(mdbx.OptMaxDB, uint64(2))
	if err != nil {
		env.Close()
		return nil, errors.Wrap(err, "failed to set max dbs")
	}

	// Open environment with optimized flags
	err = env.Open(path, mdbx.NoSubdir|mdbx.Coalesce|mdbx.LifoReclaim|mdbx.WriteMap, 0644)
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
	log.Printf("  Initial size: 200 GB")
	log.Printf("  Growth step: 200 GB")
	log.Printf("  Maximum size: 6 TB")
	log.Printf("  Page size: 4096 bytes")

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

func processLedgerParallel(
	encoder *zstd.Encoder, // Not used anymore, kept for API compatibility
	lcm xdr.LedgerCloseMeta,
	txHashToTxData map[string][]byte,
	txHashToLedgerSeq map[string]uint32,
	config IngestionConfig,
) (*CompressionStats, time.Duration, error) {
	stats := &CompressionStats{}
	var compressionTime time.Duration

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, lcm)
	if err != nil {
		return stats, 0, errors.Wrap(err, "failed to create transaction reader")
	}
	defer txReader.Close()

	ledgerSeq := lcm.LedgerSequence()
	closedAt := lcm.ClosedAt()

	// Collect all raw transactions first (fast, I/O bound)
	var rawTransactions []ingest.LedgerTransaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return stats, compressionTime, fmt.Errorf("error reading transaction: %w", err)
		}
		rawTransactions = append(rawTransactions, tx)
	}

	if len(rawTransactions) == 0 {
		return stats, 0, nil
	}

	// Process all transactions in parallel
	numWorkers := runtime.NumCPU()
	if numWorkers > 16 {
		numWorkers = 16
	}

	type job struct {
		index int
		tx    ingest.LedgerTransaction
	}

	type result struct {
		hash             string
		compressedData   []byte
		uncompressedSize int64
		compressedSize   int64
		ledgerSeq        uint32
		err              error
	}

	jobs := make(chan job, len(rawTransactions))
	results := make(chan result, len(rawTransactions))

	// Start worker pool
	var wg sync.WaitGroup
	compressionStart := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each worker gets its own encoder (if compression enabled)
			var localEncoder *zstd.Encoder
			if config.EnableApplicationCompression {
				var err error
				localEncoder, err = zstd.NewWriter(nil)
				if err != nil {
					log.Printf("Worker %d: failed to create encoder: %v", workerID, err)
					return
				}
				defer localEncoder.Close()
			}

			for job := range jobs {
				res := result{
					hash:      job.tx.Hash.HexString(),
					ledgerSeq: ledgerSeq,
				}

				// Marshal transaction components
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

				// Create TxData protobuf
				txDataProto := tx_data.TxData{
					LedgerSequence: ledgerSeq,
					ClosedAt:       timestamppb.New(closedAt),
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

				// Compress if enabled
				if config.EnableApplicationCompression {
					compressed := localEncoder.EncodeAll(txDataBytes, make([]byte, 0, len(txDataBytes)))
					res.compressedData = compressed
					res.compressedSize = int64(len(compressed))
				} else {
					res.compressedData = txDataBytes
					res.compressedSize = int64(len(txDataBytes))
				}

				results <- res
			}
		}(i)
	}

	// Send all jobs
	for idx, tx := range rawTransactions {
		jobs <- job{
			index: idx,
			tx:    tx,
		}
	}
	close(jobs)

	// Collect results in background
	go func() {
		wg.Wait()
		close(results)
	}()

	// Gather all results
	for res := range results {
		if res.err != nil {
			return stats, compressionTime, res.err
		}

		stats.UncompressedTx += res.uncompressedSize
		stats.CompressedTx += res.compressedSize
		stats.TxCount++

		// Store in maps
		if config.EnableDB2 {
			txHashToTxData[res.hash] = res.compressedData
		}
		if config.EnableDB3 {
			txHashToLedgerSeq[res.hash] = res.ledgerSeq
		}
	}

	compressionTime = time.Since(compressionStart)

	return stats, compressionTime, nil
}

// processLedger processes a single ledger and updates the batch maps
func processLedger(
	encoder *zstd.Encoder,
	lcm xdr.LedgerCloseMeta,
	txHashToTxData map[string][]byte,
	txHashToLedgerSeq map[string]uint32,
	config IngestionConfig,
) (*CompressionStats, time.Duration, error) {
	stats := &CompressionStats{}
	var compressionTime time.Duration

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, lcm)
	if err != nil {
		return stats, 0, errors.Wrap(err, "failed to create transaction reader")
	}
	defer txReader.Close()

	ledgerSeq := lcm.LedgerSequence()
	closedAt := lcm.ClosedAt()

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return stats, compressionTime, fmt.Errorf("error reading transaction: %w", err)
		}

		// Marshal transaction components
		txEnvelopeBytes, err := tx.Envelope.MarshalBinary()
		if err != nil {
			return stats, compressionTime, fmt.Errorf("error marshalling tx envelope: %w", err)
		}
		txResultBytes, err := tx.Result.MarshalBinary()
		if err != nil {
			return stats, compressionTime, fmt.Errorf("error marshalling tx result: %w", err)
		}
		txMetaBytes, err := tx.UnsafeMeta.MarshalBinary()
		if err != nil {
			return stats, compressionTime, fmt.Errorf("error marshalling tx meta: %w", err)
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
			return stats, compressionTime, errors.Wrap(err, "marshalling proto tx data")
		}

		stats.UncompressedTx += int64(len(txDataBytes))

		// Compress TxData if enabled
		var dataToStore []byte
		if config.EnableApplicationCompression {
			compressStart := time.Now()
			dataToStore = encoder.EncodeAll(txDataBytes, make([]byte, 0, len(txDataBytes)))
			compressionTime += time.Since(compressStart)
			stats.CompressedTx += int64(len(dataToStore))
		} else {
			dataToStore = txDataBytes
			stats.CompressedTx += int64(len(txDataBytes))
		}

		// Store in maps
		txHashHex := tx.Hash.HexString()
		if config.EnableDB2 {
			txHashToTxData[txHashHex] = dataToStore
		}
		if config.EnableDB3 {
			txHashToLedgerSeq[txHashHex] = ledgerSeq
		}

		stats.TxCount++
	}

	return stats, compressionTime, nil
}

// writeBatchToDatabases writes batch data to MDBX databases
// Replace the writeBatchToDatabases function with this optimized version:

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
				txHashBytes, err := hexStringToBytes(txHashHex)
				if err != nil {
					return fmt.Errorf("failed to convert tx hash %s: %w", txHashHex, err)
				}
				err = txn.Put(db2.DBI, txHashBytes, compressedTxData, mdbx.Upsert)
				if err != nil {
					return fmt.Errorf("failed to put tx %s: %w", txHashHex, err)
				}
			}
			return nil
			// ← Commits transaction (writes to mmap file in page cache)
			// ← Does NOT fsync to disk
		})
		timing.DB2WriteTime = time.Since(writeStart)
		if err != nil {
			return timing, errors.Wrap(err, "failed to write to DB2")
		}

		// At this point:
		// ✅ Data is "committed" (visible to readers)
		// ✅ Data is in MDBX's memory-mapped file
		// ⚠️  Data is in OS page cache (RAM)
		// ❌ Data is NOT guaranteed on physical disk yet

		// RECOMMENDATION:::::
		// DON'T sync on every batch - just let MDBX handle it
		// Transaction commit writes to memory-mapped file (OS page cache)
		// OS will eventually flush to disk in background
		// Explicit Sync() forces immediate flush to disk
		timing.DB2SyncTime = 0
	}

	// Write to DB3 (txHash -> ledgerSeq)
	if config.EnableDB3 && db3 != nil {
		writeStart := time.Now()
		err := db3.Env.Update(func(txn *mdbx.Txn) error {
			for txHashHex, ledgerSeq := range txHashToLedgerSeq {
				txHashBytes, err := hexStringToBytes(txHashHex)
				if err != nil {
					return fmt.Errorf("failed to convert tx hash %s: %w", txHashHex, err)
				}
				value := uint32ToBytes(ledgerSeq)
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
		stats.LastPageNum = envInfo.MiLastPgNo // Changed from LastPgNo to LastPNO
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
	log.Printf("  Branch pages: %s", formatNumber(int64(stats.BranchPages)))
	log.Printf("  Leaf pages: %s", formatNumber(int64(stats.LeafPages)))
	log.Printf("  Overflow pages: %s", formatNumber(int64(stats.OverflowPages)))
	log.Printf("  Total entries: %s", formatNumber(int64(stats.Entries)))
	log.Printf("  Map size: %s", formatBytes(int64(stats.MapSize)))
	log.Printf("  Last page: %s", formatNumber(int64(stats.LastPageNum)))
	log.Printf("  Recent txn ID: %d", stats.RecentTxnID)

	// Calculate storage efficiency
	totalPages := stats.BranchPages + stats.LeafPages + stats.OverflowPages
	usedSize := totalPages * uint64(stats.PageSize)
	log.Printf("  Used size: %s", formatBytes(int64(usedSize)))

	if stats.Entries > 0 {
		avgEntrySize := float64(usedSize) / float64(stats.Entries)
		log.Printf("  Avg entry size: %.2f bytes", avgEntrySize)
	}
}

// logBatchCompletion logs batch completion information
func logBatchCompletion(batch BatchInfo, timing DBTimingStats, config IngestionConfig) {
	totalBatchDuration := time.Since(batch.StartTime)
	cpuTime := batch.GetLedgerTime + batch.Db2CompressionTime
	ioTime := timing.DB2WriteTime + timing.DB2SyncTime + timing.DB3WriteTime + timing.DB3SyncTime
	computeTime := totalBatchDuration - cpuTime - ioTime

	log.Printf("\n===== Batch #%d Complete [Ledger %d-%d] (%d transactions) =====",
		batch.BatchNum, batch.StartLedger, batch.EndLedger, batch.TxCount)
	log.Printf("Total batch time time: %s", formatDuration(totalBatchDuration))
	log.Printf("\t GetLedger Time: %s", formatDuration(batch.GetLedgerTime))
	log.Printf("\t Compute Time (?): %s", formatDuration(computeTime))

	if config.EnableDB2 {
		db2Total := timing.DB2WriteTime + timing.DB2SyncTime
		log.Printf("\t DB2:: %s", config.DB2Path)
		log.Printf("\t\t CompressionTime: %s", formatDuration(batch.Db2CompressionTime))
		log.Printf("\t\t I/O time: %s (write: %s, sync: %s)",
			formatDuration(db2Total),
			formatDuration(timing.DB2WriteTime),
			formatDuration(timing.DB2SyncTime))
	}

	if config.EnableDB3 {
		db3Total := timing.DB3WriteTime + timing.DB3SyncTime
		log.Printf("\t DB3:: %s", config.DB3Path)
		log.Printf("\t\t I/O time: %s (write: %s, sync: %s)",
			formatDuration(db3Total),
			formatDuration(timing.DB3WriteTime),
			formatDuration(timing.DB3SyncTime))
	}

	log.Printf("========================================\n")
}

// showCompressionStats shows compression statistics
func showCompressionStats(config IngestionConfig, stats CompressionStats) {
	if config.EnableApplicationCompression && stats.UncompressedTx > 0 {
		compressionRatio := 100 * (1 - float64(stats.CompressedTx)/float64(stats.UncompressedTx))
		log.Printf("\nCompression Statistics:")
		log.Printf("  Original size: %s", formatBytes(stats.UncompressedTx))
		log.Printf("  Compressed size: %s", formatBytes(stats.CompressedTx))
		log.Printf("  Compression ratio: %.2f%% reduction", compressionRatio)
		log.Printf("  Space saved: %s", formatBytes(stats.UncompressedTx-stats.CompressedTx))
	}
}

// Helper functions

func uint32ToBytes(n uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	return b
}

func hexStringToBytes(hexStr string) ([]byte, error) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	return hex.DecodeString(hexStr)
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

func formatDuration(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.3fs", d.Seconds())
	}

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
