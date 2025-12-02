package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
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
const MB = 1024 * 1024

// IngestionConfig holds configuration for the ingestion process
type IngestionConfig struct {
	StartLedger uint32
	EndLedger   uint32
	BatchSize   int
	NumWorkers  int

	// Database paths
	DB1Path string // ledgerSeq -> compressed LCM
	DB2Path string // txHash -> compressed TxData
	DB3Path string // txHash -> ledgerSeq

	// Enable flags (derived from paths)
	EnableDB1 bool
	EnableDB2 bool
	EnableDB3 bool

	// Compression settings
	EnableApplicationCompression bool
	EnableRocksdbCompression     bool

	// RocksDB source for reading LCM (optional)
	RocksDBLCMPath string
	UseRocksDB     bool

	// RocksDB tuning parameters
	WriteBufferSizeMB      int
	MaxWriteBufferNumber   int
	L0CompactionTrigger    int
	L0SlowdownTrigger      int
	L0StopTrigger          int
	MaxBackgroundJobs      int
	TargetFileSizeMB       int
	MaxBytesForLevelBaseMB int
	MaxOpenFiles           int
	DisableAutoCompactions bool
	CompactEveryNBatches   int  // 0 = disabled, only final compaction
	CompactAfterGBWritten  int  // Compact after N GB written (0 = disabled)
	DisableWAL             bool // Disable Write-Ahead Log for bulk loading
	PrepareForBulkLoad     bool // Use RocksDB bulk load optimizations
	BlockCacheSizeMB       int  // Block cache size for reads
	BloomFilterBitsPerKey  int  // Bloom filter bits (0 = disabled)
}

// CompressionStats tracks compression metrics
type CompressionStats struct {
	UncompressedLCM int64
	CompressedLCM   int64
	UncompressedTx  int64
	CompressedTx    int64
	TxCount         int64
	LedgerCount     int64
}

// DBTimingStats tracks timing for individual operations
type DBTimingStats struct {
	GetLedgerTime      time.Duration
	DB1CompressionTime time.Duration
	DB2CompressionTime time.Duration
	DB1WriteTime       time.Duration
	DB2WriteTime       time.Duration
	DB3WriteTime       time.Duration
	DB1FlushTime       time.Duration
	DB2FlushTime       time.Duration
	DB3FlushTime       time.Duration
	DB1CompactTime     time.Duration
	DB2CompactTime     time.Duration
	DB3CompactTime     time.Duration
}

// DBSizeStats tracks actual disk usage
type DBSizeStats struct {
	DB1SSTSize      int64
	DB1MemtableSize int64
	DB1TotalSize    int64
	DB2SSTSize      int64
	DB2MemtableSize int64
	DB2TotalSize    int64
	DB3SSTSize      int64
	DB3MemtableSize int64
	DB3TotalSize    int64
}

// RocksDBTimingStats tracks timing for RocksDB read operations
type RocksDBTimingStats struct {
	ReadTime       time.Duration
	DecompressTime time.Duration
	UnmarshalTime  time.Duration
	TotalTime      time.Duration
}

// BatchInfo tracks information about the current batch being processed
type BatchInfo struct {
	BatchNum                uint32
	StartLedger             uint32
	EndLedger               uint32
	StartTime               time.Time
	TxCount                 int
	LedgerCount             int
	BatchGetLedgerTime      time.Duration
	BatchDB1CompressionTime time.Duration
	BatchDB2CompressionTime time.Duration
	BatchRocksDBTimingStats RocksDBTimingStats
}

// RocksDBReader wraps RocksDB for reading ledger data
type RocksDBReader struct {
	db *grocksdb.DB
	ro *grocksdb.ReadOptions
}

// LedgerResult holds the result of fetching a single ledger
type LedgerResult struct {
	LedgerSeq uint32
	Ledger    xdr.LedgerCloseMeta
	Timing    RocksDBTimingStats
	Err       error
}

// rawTxData holds raw transaction data before processing
type rawTxData struct {
	tx        ingest.LedgerTransaction
	ledgerSeq uint32
	closedAt  time.Time
}

// txProcessResult holds the output of processing a single transaction
type txProcessResult struct {
	hash             string
	compressedData   []byte
	uncompressedSize int64
	compressedSize   int64
	ledgerSeq        uint32
	err              error
}

// lcmProcessResult holds the output of processing a single LCM
type lcmProcessResult struct {
	ledgerSeq        uint32
	compressedData   []byte
	uncompressedSize int64
	compressedSize   int64
	err              error
}

// BytesWrittenTracker tracks bytes written for compaction triggers
type BytesWrittenTracker struct {
	DB1BytesWritten int64
	DB2BytesWritten int64
	DB3BytesWritten int64
	mu              sync.Mutex
}

var startTime time.Time

func main() {
	startTime = time.Now()

	// ================================
	// COMMAND LINE FLAGS
	// ================================
	var startLedger, endLedger uint
	var batchSize int
	var db1Path, db2Path, db3Path string
	var enableApplicationCompression, enableRocksdbCompression bool
	var rocksdbLcmPath string

	// RocksDB tuning parameters
	var writeBufferSizeMB, maxWriteBufferNumber int
	var l0CompactionTrigger, l0SlowdownTrigger, l0StopTrigger int
	var maxBackgroundJobs int
	var targetFileSizeMB, maxBytesForLevelBaseMB int
	var maxOpenFiles int
	var disableAutoCompactions bool
	var compactEveryNBatches int
	var compactAfterGBWritten int
	var disableWAL bool
	var prepareForBulkLoad bool
	var blockCacheSizeMB int
	var bloomFilterBits int

	// ===================================================================================
	// Core settings
	// ===================================================================================
	flag.UintVar(&startLedger, "start-ledger", 0, "Starting ledger sequence number")
	flag.UintVar(&endLedger, "end-ledger", 0, "Ending ledger sequence number")

	// 10K ledgers ≈ 400K transactions ≈ 320 MB per batch
	// Should be tuned to roughly match write-buffer-size-mb for efficient flushing
	flag.IntVar(&batchSize, "ledger-batch-size", 10000, "Ledger batch size for commit")

	// ===================================================================================
	// Database paths
	// ===================================================================================
	flag.StringVar(&db1Path, "db1", "", "Path for DB1 (ledgerSeq -> compressed LCM)")
	flag.StringVar(&db2Path, "db2", "", "Path for DB2 (txHash -> compressed TxData)")
	flag.StringVar(&db3Path, "db3", "", "Path for DB3 (txHash -> ledgerSeq)")

	// ===================================================================================
	// Compression settings
	// ===================================================================================
	// App-level zstd compression: ~60-70% reduction before data hits RocksDB
	// This is preferred over RocksDB compression for better control and visibility
	flag.BoolVar(&enableApplicationCompression, "app-compression", true, "Enable zstd compression before writing to RocksDB")

	// RocksDB compression: disabled since we already compress at app level
	// Enabling both would double-compress (wasteful CPU, minimal gain)
	flag.BoolVar(&enableRocksdbCompression, "rocksdb-compression", false, "Enable RocksDB native compression")

	// ===================================================================================
	// RocksDB source (for reading existing LCM store)
	// ===================================================================================
	flag.StringVar(&rocksdbLcmPath, "rocksdb-lcm-store", "", "Path to RocksDB store containing compressed LedgerCloseMeta (disables DB1)")

	// ===================================================================================
	// RocksDB tuning - Write Buffer (Memtable)
	// ===================================================================================
	// 512 MB per buffer: sized to roughly match batch size after compression
	// Larger buffer = fewer L0 files = faster final compaction
	flag.IntVar(&writeBufferSizeMB, "write-buffer-size-mb", 512, "Write buffer (memtable) size in MB per buffer")

	// 4 buffers × 512 MB = 2 GB max memtable memory
	// Allows writes to continue while flushing
	flag.IntVar(&maxWriteBufferNumber, "max-write-buffer-number", 4, "Max number of write buffers")

	// ===================================================================================
	// RocksDB tuning - L0 management
	// ===================================================================================
	// Set to 999 to effectively disable L0-triggered compaction during ingestion
	// With random hash keys, compaction during ingestion is wasteful
	// All L0 files overlap with all other files = massive write amplification
	flag.IntVar(&l0CompactionTrigger, "l0-compaction-trigger", 999, "Number of L0 files to trigger compaction (999 = disable)")
	flag.IntVar(&l0SlowdownTrigger, "l0-slowdown-trigger", 999, "Number of L0 files to slow down writes (999 = disable)")
	flag.IntVar(&l0StopTrigger, "l0-stop-trigger", 999, "Number of L0 files to stop writes (999 = disable)")

	// ===================================================================================
	// RocksDB tuning - Background jobs
	// ===================================================================================
	// 8 threads for final compaction
	// During ingestion (with auto-compaction disabled), these are mostly idle
	flag.IntVar(&maxBackgroundJobs, "max-background-jobs", 8, "Max background compaction/flush jobs")

	// ===================================================================================
	// RocksDB tuning - File sizes
	// ===================================================================================
	// 1 GB target file size: 120 GB monthly data / 1 GB = ~120 SST files per month
	// For 10 years (120 months): ~14,400 total files - manageable
	flag.IntVar(&targetFileSizeMB, "target-file-size-mb", 1024, "Target SST file size in MB")

	// 2 GB L1 base: controls level depth
	// With 10x multiplier: L1=2GB, L2=20GB, L3=200GB, L4=2TB, L5=20TB
	// 120 GB monthly data lands in L3/L4, 12 TB (10yr) lands in L5
	// Larger base = fewer levels = fewer bloom filter checks on read
	flag.IntVar(&maxBytesForLevelBaseMB, "max-bytes-level-base-mb", 2048, "Max bytes for L1 in MB")

	// ===================================================================================
	// RocksDB tuning - Resources
	// ===================================================================================
	// 2000 open files: ~120 SST files per month, plenty of headroom
	// For serving 10 years, increase to 20000+ and set ulimit accordingly
	flag.IntVar(&maxOpenFiles, "max-open-files", 2000, "Max number of open files")

	// 512 MB block cache: minimal during write-heavy ingestion
	// For read serving, increase significantly (4-8 GB)
	flag.IntVar(&blockCacheSizeMB, "block-cache-size-mb", 512, "Block cache size in MB (0 = disabled)")

	// 10 bits per key ≈ 1% false positive rate
	// ~187 MB bloom filter memory per month (150M keys × 10 bits)
	// Essential for random hash key lookups - avoids scanning all SST files
	flag.IntVar(&bloomFilterBits, "bloom-filter-bits", 10, "Bloom filter bits per key (0 = disabled)")

	// ===================================================================================
	// RocksDB tuning - Compaction control
	// ===================================================================================
	// Disable auto-compaction: with random keys, compaction during ingestion is pure overhead
	// All data accumulates in L0, one final compaction at end sorts everything
	flag.BoolVar(&disableAutoCompactions, "disable-auto-compactions", true, "Disable automatic compactions")

	// No periodic compaction: defer everything to final compaction
	flag.IntVar(&compactEveryNBatches, "compact-every-n-batches", 0, "Compact every N batches (0 = only final)")
	flag.IntVar(&compactAfterGBWritten, "compact-after-gb-written", 0, "Compact after N GB written (0 = disabled)")

	// ===================================================================================
	// RocksDB tuning - Bulk load
	// ===================================================================================
	// WAL disabled: we're doing bulk ingestion, crash = restart from beginning anyway
	// Saves significant I/O overhead
	flag.BoolVar(&disableWAL, "disable-wal", true, "Disable Write-Ahead Log for bulk loading")

	// PrepareForBulkLoad disabled: we're setting custom parameters above
	// PrepareForBulkLoad() would override our carefully tuned settings
	flag.BoolVar(&prepareForBulkLoad, "prepare-bulk-load", false, "Use RocksDB bulk load optimizations (conflicts with custom tuning)")

	flag.Parse()

	// ================================
	// VALIDATE ARGUMENTS
	// ================================
	if startLedger == 0 || endLedger == 0 {
		log.Fatal("start-ledger and end-ledger are required")
	}
	if batchSize <= 0 {
		log.Fatal("batch-size must be positive")
	}

	enableDB1 := db1Path != ""
	enableDB2 := db2Path != ""
	enableDB3 := db3Path != ""

	// If RocksDB LCM store is provided, disable DB1 (we're reading from it, not writing)
	useRocksDBSource := rocksdbLcmPath != ""
	if useRocksDBSource && enableDB1 {
		log.Printf("⚠️  RocksDB LCM store provided - DB1 ingestion will be DISABLED")
		log.Printf("   (Cannot write to DB1 while reading LCM from another RocksDB store)")
		enableDB1 = false
		db1Path = ""
	}

	if !enableDB1 && !enableDB2 && !enableDB3 {
		log.Fatal("At least one database (db1, db2, or db3) must be specified")
	}

	// ================================
	// CREATE CONFIG
	// ================================
	numWorkers := runtime.NumCPU()
	if numWorkers > 16 {
		numWorkers = 16
	}

	config := IngestionConfig{
		StartLedger:                  uint32(startLedger),
		EndLedger:                    uint32(endLedger),
		BatchSize:                    batchSize,
		NumWorkers:                   numWorkers,
		DB1Path:                      db1Path,
		DB2Path:                      db2Path,
		DB3Path:                      db3Path,
		EnableDB1:                    enableDB1,
		EnableDB2:                    enableDB2,
		EnableDB3:                    enableDB3,
		EnableApplicationCompression: enableApplicationCompression,
		EnableRocksdbCompression:     enableRocksdbCompression,
		RocksDBLCMPath:               rocksdbLcmPath,
		UseRocksDB:                   useRocksDBSource,
		WriteBufferSizeMB:            writeBufferSizeMB,
		MaxWriteBufferNumber:         maxWriteBufferNumber,
		L0CompactionTrigger:          l0CompactionTrigger,
		L0SlowdownTrigger:            l0SlowdownTrigger,
		L0StopTrigger:                l0StopTrigger,
		MaxBackgroundJobs:            maxBackgroundJobs,
		TargetFileSizeMB:             targetFileSizeMB,
		MaxBytesForLevelBaseMB:       maxBytesForLevelBaseMB,
		MaxOpenFiles:                 maxOpenFiles,
		DisableAutoCompactions:       disableAutoCompactions,
		CompactEveryNBatches:         compactEveryNBatches,
		CompactAfterGBWritten:        compactAfterGBWritten,
		DisableWAL:                   disableWAL,
		PrepareForBulkLoad:           prepareForBulkLoad,
		BlockCacheSizeMB:             blockCacheSizeMB,
		BloomFilterBitsPerKey:        bloomFilterBits,
	}

	// ================================
	// INITIALIZE ROCKSDB DATABASES
	// ================================
	var db1, db2, db3 *grocksdb.DB
	var opts1, opts2, opts3 *grocksdb.Options
	var err error

	if config.EnableDB1 {
		db1Path, err = filepath.Abs(config.DB1Path)
		if err != nil {
			log.Fatalf("Failed to get absolute path for db1: %v", err)
		}
		if err := createDir(db1Path); err != nil {
			log.Fatalf("Failed to create directory for DB1: %v", err)
		}
		db1, opts1, err = openRocksDBWithConfig(db1Path, "DB1 (ledgerSeq->LCM)", config)
		if err != nil {
			log.Fatalf("Failed to open DB1: %v", err)
		}
		defer func() {
			db1.Close()
			opts1.Destroy()
		}()
		log.Printf("✓ DB1 (RocksDB) opened at: %s", db1Path)
	}

	if config.EnableDB2 {
		db2Path, err = filepath.Abs(config.DB2Path)
		if err != nil {
			log.Fatalf("Failed to get absolute path for db2: %v", err)
		}
		if err := createDir(db2Path); err != nil {
			log.Fatalf("Failed to create directory for DB2: %v", err)
		}
		db2, opts2, err = openRocksDBWithConfig(db2Path, "DB2 (txHash->TxData)", config)
		if err != nil {
			log.Fatalf("Failed to open DB2: %v", err)
		}
		defer func() {
			db2.Close()
			opts2.Destroy()
		}()
		log.Printf("✓ DB2 (RocksDB) opened at: %s", db2Path)
	}

	if config.EnableDB3 {
		db3Path, err = filepath.Abs(config.DB3Path)
		if err != nil {
			log.Fatalf("Failed to get absolute path for db3: %v", err)
		}
		if err := createDir(db3Path); err != nil {
			log.Fatalf("Failed to create directory for DB3: %v", err)
		}
		db3, opts3, err = openRocksDBWithConfig(db3Path, "DB3 (txHash->ledgerSeq)", config)
		if err != nil {
			log.Fatalf("Failed to open DB3: %v", err)
		}
		defer func() {
			db3.Close()
			opts3.Destroy()
		}()
		log.Printf("✓ DB3 (RocksDB) opened at: %s", db3Path)
	}

	ctx := context.Background()

	// ================================
	// INITIALIZE LEDGER SOURCE
	// ================================
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
			log.Fatal(errors.Wrap(err, "failed to create datastore"))
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
		err = backend.PrepareRange(ctx, ledgerRange)
		if err != nil {
			log.Fatal(errors.Wrapf(err, "failed to prepare range: %v", ledgerRange))
		}
	}

	// ================================
	// PRINT STARTUP INFO
	// ================================
	ledgerRange := ledgerbackend.BoundedRange(config.StartLedger, config.EndLedger)
	totalLedgers := int(ledgerRange.To() - ledgerRange.From() + 1)

	log.Printf("\n========================================")
	log.Printf("Starting RocksDB ledger processing")
	log.Printf("========================================")
	log.Printf("Ledger range: %d - %d (%s ledgers)",
		ledgerRange.From(), ledgerRange.To(), helpers.FormatNumber(int64(totalLedgers)))
	log.Printf("Ledger source: %s", func() string {
		if config.UseRocksDB {
			return "RocksDB (" + config.RocksDBLCMPath + ")"
		}
		return "GCS (BufferedStorageBackend)"
	}())
	log.Printf("DB1 (LCM) storage: %v", config.EnableDB1)
	log.Printf("DB2 (TxData) storage: %v", config.EnableDB2)
	log.Printf("DB3 (Hash->Seq) storage: %v", config.EnableDB3)
	log.Printf("Application Compression (zstd): %v", config.EnableApplicationCompression)
	log.Printf("RocksDB Native Compression: %v", config.EnableRocksdbCompression)
	log.Printf("Batch Size: %d ledgers", config.BatchSize)
	log.Printf("Worker threads: %d", config.NumWorkers)
	log.Printf("")
	log.Printf("RocksDB Configuration:")
	log.Printf("  Write Buffer: %d MB × %d = %d MB total",
		config.WriteBufferSizeMB, config.MaxWriteBufferNumber,
		config.WriteBufferSizeMB*config.MaxWriteBufferNumber)
	log.Printf("  L0 Trigger/Slowdown/Stop: %d/%d/%d files",
		config.L0CompactionTrigger, config.L0SlowdownTrigger, config.L0StopTrigger)
	log.Printf("  Background Jobs: %d", config.MaxBackgroundJobs)
	log.Printf("  Target File Size: %d MB", config.TargetFileSizeMB)
	log.Printf("  Max Bytes Level Base: %d MB", config.MaxBytesForLevelBaseMB)
	log.Printf("  Max Open Files: %d", config.MaxOpenFiles)
	log.Printf("  Block Cache: %d MB", config.BlockCacheSizeMB)
	log.Printf("  Bloom Filter: %d bits/key", config.BloomFilterBitsPerKey)
	log.Printf("  Auto Compactions: %v", !config.DisableAutoCompactions)
	log.Printf("  WAL: %v", !config.DisableWAL)
	log.Printf("  Compact every N batches: %d (0=final only)", config.CompactEveryNBatches)
	log.Printf("  Compact after GB written: %d (0=disabled)", config.CompactAfterGBWritten)
	log.Printf("========================================\n")

	// ================================
	// INITIALIZE METRICS TRACKING
	// ================================
	processedLedgerCount := 0
	skippedLedgerCount := 0
	lastReportedPercent := -1

	var totalTimingStats DBTimingStats
	var totalCompressionStats CompressionStats
	var totalRocksDBTiming RocksDBTimingStats

	bytesTracker := &BytesWrittenTracker{}

	// Batch data structures
	var batchTransactions []rawTxData
	var batchLCMs []struct {
		ledgerSeq uint32
		lcm       xdr.LedgerCloseMeta
	}

	currentBatch := BatchInfo{
		BatchNum:    1,
		StartLedger: ledgerRange.From(),
		StartTime:   time.Now(),
	}

	// Track global min/max ledger for final compaction
	var globalMinLedger uint32 = 0xFFFFFFFF
	var globalMaxLedger uint32 = 0

	// ================================
	// BATCH PROCESSING FUNCTION
	// ================================
	processCurrentBatch := func() {
		if len(batchTransactions) == 0 && len(batchLCMs) == 0 {
			return
		}

		// Maps to hold processed data before writing to RocksDB
		ledgerSeqToLcm := make(map[uint32][]byte)
		txHashToTxData := make(map[string][]byte)
		txHashToLedgerSeq := make(map[string]uint32)

		// Process LCMs in parallel (for DB1)
		if config.EnableDB1 && len(batchLCMs) > 0 {
			compressionStart := time.Now()
			lcmResults := compressLCMsInParallel(batchLCMs, config)

			for _, result := range lcmResults {
				if result.err != nil {
					log.Fatalf("Failed to compress LCM for ledger %d: %v", result.ledgerSeq, result.err)
				}
				ledgerSeqToLcm[result.ledgerSeq] = result.compressedData
				totalCompressionStats.UncompressedLCM += result.uncompressedSize
				totalCompressionStats.CompressedLCM += result.compressedSize
			}

			compressionTime := time.Since(compressionStart)
			currentBatch.BatchDB1CompressionTime = compressionTime
			totalTimingStats.DB1CompressionTime += compressionTime
		}

		// Build txHash -> ledgerSeq map (for DB3)
		if config.EnableDB3 {
			for _, tx := range batchTransactions {
				hash := tx.tx.Hash.HexString()
				txHashToLedgerSeq[hash] = tx.ledgerSeq
			}
		}

		// Compress transactions in parallel (for DB2)
		if config.EnableDB2 && len(batchTransactions) > 0 {
			compressionStart := time.Now()
			stats, err := compressTransactionsInParallel(batchTransactions, txHashToTxData, config)
			if err != nil {
				log.Fatalf("Failed to compress transactions: %v", err)
			}

			compressionTime := time.Since(compressionStart)
			currentBatch.BatchDB2CompressionTime = compressionTime
			totalTimingStats.DB2CompressionTime += compressionTime

			totalCompressionStats.UncompressedTx += stats.UncompressedTx
			totalCompressionStats.CompressedTx += stats.CompressedTx
			totalCompressionStats.TxCount += stats.TxCount
		}

		currentBatch.TxCount = len(batchTransactions)
		currentBatch.LedgerCount = len(batchLCMs)
		if currentBatch.LedgerCount == 0 {
			currentBatch.LedgerCount = processedLedgerCount % config.BatchSize
			if currentBatch.LedgerCount == 0 {
				currentBatch.LedgerCount = config.BatchSize
			}
		}

		// Write batch to RocksDB
		dbTiming, bytesWritten, err := writeBatchToRocksDB(db1, db2, db3, ledgerSeqToLcm, txHashToTxData, txHashToLedgerSeq, config)
		if err != nil {
			log.Printf("Error writing batch: %v", err)
		}

		// Track bytes written
		bytesTracker.mu.Lock()
		bytesTracker.DB1BytesWritten += bytesWritten.DB1BytesWritten
		bytesTracker.DB2BytesWritten += bytesWritten.DB2BytesWritten
		bytesTracker.DB3BytesWritten += bytesWritten.DB3BytesWritten
		totalDB2Written := bytesTracker.DB2BytesWritten
		bytesTracker.mu.Unlock()

		// Accumulate timing
		totalTimingStats.DB1WriteTime += dbTiming.DB1WriteTime
		totalTimingStats.DB2WriteTime += dbTiming.DB2WriteTime
		totalTimingStats.DB3WriteTime += dbTiming.DB3WriteTime

		// Flush databases
		var flushTiming DBTimingStats
		// explicitly disable flushing.
		//flushTiming = flushAllDBs(db1, db2, db3, config)
		totalTimingStats.DB1FlushTime += flushTiming.DB1FlushTime
		totalTimingStats.DB2FlushTime += flushTiming.DB2FlushTime
		totalTimingStats.DB3FlushTime += flushTiming.DB3FlushTime

		// Log batch completion
		logBatchCompletion(currentBatch, dbTiming, flushTiming, config, totalCompressionStats)

		// Check if we should compact based on bytes written
		shouldCompact := false
		compactReason := ""

		if config.CompactEveryNBatches > 0 && currentBatch.BatchNum%uint32(config.CompactEveryNBatches) == 0 {
			shouldCompact = true
			compactReason = fmt.Sprintf("every %d batches", config.CompactEveryNBatches)
		}

		if config.CompactAfterGBWritten > 0 && totalDB2Written >= int64(config.CompactAfterGBWritten)*GB {
			shouldCompact = true
			compactReason = fmt.Sprintf("after %d GB written to DB2", config.CompactAfterGBWritten)
			// Reset counter after compaction
			bytesTracker.mu.Lock()
			bytesTracker.DB2BytesWritten = 0
			bytesTracker.mu.Unlock()
		}

		if shouldCompact {
			log.Printf("\n========================================")
			log.Printf("🔄 Triggering compaction (%s)...", compactReason)
			compactTiming := compactAllDBs(db1, db2, db3, config, globalMinLedger, globalMaxLedger)
			totalTimingStats.DB1CompactTime += compactTiming.DB1CompactTime
			totalTimingStats.DB2CompactTime += compactTiming.DB2CompactTime
			totalTimingStats.DB3CompactTime += compactTiming.DB3CompactTime
			log.Printf("✅ Compaction complete")
			log.Printf("========================================\n")
		}

		// Clear batch and prepare for next
		batchTransactions = nil
		batchLCMs = nil
		currentBatch.BatchNum++
		currentBatch.StartTime = time.Now()
		currentBatch.BatchDB1CompressionTime = 0
		currentBatch.BatchDB2CompressionTime = 0
		currentBatch.BatchGetLedgerTime = 0
		currentBatch.LedgerCount = 0
		currentBatch.BatchRocksDBTimingStats = RocksDBTimingStats{}
	}

	// ================================
	// MAIN PROCESSING LOOP
	// ================================
	if config.UseRocksDB {
		// ========================================
		// ROCKSDB PATH: Parallel ledger fetching
		// ========================================
		chunkSize := config.BatchSize

		for chunkStart := ledgerRange.From(); chunkStart <= ledgerRange.To(); chunkStart += uint32(chunkSize) {
			chunkEnd := chunkStart + uint32(chunkSize) - 1
			if chunkEnd > ledgerRange.To() {
				chunkEnd = ledgerRange.To()
			}

			// Fetch all ledgers in parallel
			getLedgerStart := time.Now()
			ledgerResults := fetchLedgersParallel(rocksReader, chunkStart, chunkEnd, config.NumWorkers)
			getLedgerTime := time.Since(getLedgerStart)

			rocksDBTimingsForBatch := RocksDBTimingStats{}
			ledgersInBatch := 0

			for _, result := range ledgerResults {
				// Accumulate RocksDB timing stats
				totalRocksDBTiming.ReadTime += result.Timing.ReadTime
				totalRocksDBTiming.DecompressTime += result.Timing.DecompressTime
				totalRocksDBTiming.UnmarshalTime += result.Timing.UnmarshalTime
				totalRocksDBTiming.TotalTime += result.Timing.TotalTime

				rocksDBTimingsForBatch.ReadTime += result.Timing.ReadTime
				rocksDBTimingsForBatch.DecompressTime += result.Timing.DecompressTime
				rocksDBTimingsForBatch.UnmarshalTime += result.Timing.UnmarshalTime
				rocksDBTimingsForBatch.TotalTime += result.Timing.TotalTime

				if result.Err != nil {
					log.Printf("===== Warning: Failed to get ledger %d: %v, skipping =====\n",
						result.LedgerSeq, result.Err)
					skippedLedgerCount++
					continue
				}

				ledger := result.Ledger
				ledgersInBatch++

				// Track min/max for compaction
				if result.LedgerSeq < globalMinLedger {
					globalMinLedger = result.LedgerSeq
				}
				if result.LedgerSeq > globalMaxLedger {
					globalMaxLedger = result.LedgerSeq
				}

				// Store LCM for DB1 processing
				if config.EnableDB1 {
					batchLCMs = append(batchLCMs, struct {
						ledgerSeq uint32
						lcm       xdr.LedgerCloseMeta
					}{result.LedgerSeq, ledger})
				}

				// Extract transactions
				txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
					network.PublicNetworkPassphrase, ledger)
				if err != nil {
					log.Fatalf("Failed to create tx reader for ledger %d: %v", result.LedgerSeq, err)
				}

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
						closedAt:  ledger.ClosedAt(),
					})
				}
				txReader.Close()

				processedLedgerCount++
				totalCompressionStats.LedgerCount++
			}

			totalTimingStats.GetLedgerTime += getLedgerTime
			currentBatch.BatchGetLedgerTime = getLedgerTime
			currentBatch.BatchRocksDBTimingStats = rocksDBTimingsForBatch
			currentBatch.LedgerCount = ledgersInBatch

			// Process the batch
			currentBatch.EndLedger = chunkEnd
			processCurrentBatch()
			currentBatch.StartLedger = chunkEnd + 1

			// Report database stats every 10 batches
			if (currentBatch.BatchNum-1)%10 == 0 {
				logDatabaseStats(db1, db2, db3, config, totalCompressionStats, chunkEnd)
				if config.UseRocksDB {
					showRocksDBTimingStats(totalRocksDBTiming, processedLedgerCount, config)
				}
			}

			// Report progress every 1%
			reportProgress(processedLedgerCount, totalLedgers, skippedLedgerCount, totalCompressionStats, &lastReportedPercent, chunkEnd)
		}

	} else {
		// ========================================
		// GCS PATH: Sequential ledger fetching
		// ========================================
		for ledgerSeq := ledgerRange.From(); ledgerSeq <= ledgerRange.To(); ledgerSeq++ {
			getLedgerStart := time.Now()
			ledger, err := backend.GetLedger(ctx, ledgerSeq)
			getLedgerTime := time.Since(getLedgerStart)

			if err != nil {
				log.Fatalf("Failed to retrieve ledger %d: %v", ledgerSeq, err)
			}

			totalTimingStats.GetLedgerTime += getLedgerTime
			currentBatch.BatchGetLedgerTime += getLedgerTime

			// Track min/max for compaction
			if ledgerSeq < globalMinLedger {
				globalMinLedger = ledgerSeq
			}
			if ledgerSeq > globalMaxLedger {
				globalMaxLedger = ledgerSeq
			}

			// Store LCM for DB1 processing
			if config.EnableDB1 {
				batchLCMs = append(batchLCMs, struct {
					ledgerSeq uint32
					lcm       xdr.LedgerCloseMeta
				}{ledgerSeq, ledger})
			}

			// Extract transactions
			txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
				network.PublicNetworkPassphrase, ledger)
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
			totalCompressionStats.LedgerCount++
			currentBatch.LedgerCount++

			// Process batch when we hit batch size
			if processedLedgerCount%config.BatchSize == 0 {
				currentBatch.EndLedger = ledgerSeq
				processCurrentBatch()
				currentBatch.StartLedger = ledgerSeq + 1
			}

			// Report database stats every 10 batches
			if processedLedgerCount%(config.BatchSize*10) == 0 {
				logDatabaseStats(db1, db2, db3, config, totalCompressionStats, ledgerSeq)
			}

			// Report progress
			reportProgress(processedLedgerCount, totalLedgers, skippedLedgerCount, totalCompressionStats, &lastReportedPercent, ledgerSeq)
		}

		// Process remaining batch
		if len(batchTransactions) > 0 || len(batchLCMs) > 0 {
			currentBatch.EndLedger = ledgerRange.To()
			processCurrentBatch()
		}
	}

	// ================================
	// FINAL COMPACTION
	// ================================
	log.Printf("\n========================================")
	log.Printf("PERFORMING FINAL COMPACTION")
	log.Printf("========================================\n")

	finalCompactTiming := compactAllDBs(db1, db2, db3, config, globalMinLedger, globalMaxLedger)
	totalTimingStats.DB1CompactTime += finalCompactTiming.DB1CompactTime
	totalTimingStats.DB2CompactTime += finalCompactTiming.DB2CompactTime
	totalTimingStats.DB3CompactTime += finalCompactTiming.DB3CompactTime

	log.Printf("Final Compaction Summary:")
	if config.EnableDB1 {
		log.Printf("  DB1: %s", helpers.FormatDuration(finalCompactTiming.DB1CompactTime))
	}
	if config.EnableDB2 {
		log.Printf("  DB2: %s", helpers.FormatDuration(finalCompactTiming.DB2CompactTime))
	}
	if config.EnableDB3 {
		log.Printf("  DB3: %s", helpers.FormatDuration(finalCompactTiming.DB3CompactTime))
	}

	// ================================
	// FINAL STATISTICS
	// ================================
	elapsed := time.Since(startTime)

	log.Printf("\n========================================")
	log.Printf("FINAL ROCKSDB STATS (after compaction)")
	log.Printf("========================================")
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
		log.Printf("    RocksDB Read: %s", helpers.FormatDuration(totalRocksDBTiming.ReadTime/time.Duration(config.NumWorkers)))
		log.Printf("    Decompress: %s", helpers.FormatDuration(totalRocksDBTiming.DecompressTime/time.Duration(config.NumWorkers)))
		log.Printf("    Unmarshal: %s", helpers.FormatDuration(totalRocksDBTiming.UnmarshalTime/time.Duration(config.NumWorkers)))
	}

	totalCompressionTime := totalTimingStats.DB1CompressionTime + totalTimingStats.DB2CompressionTime
	log.Printf("  Compression: %s (%.1f%%)", helpers.FormatDuration(totalCompressionTime),
		100*totalCompressionTime.Seconds()/elapsed.Seconds())

	if config.EnableDB1 {
		db1Total := totalTimingStats.DB1WriteTime + totalTimingStats.DB1FlushTime + totalTimingStats.DB1CompactTime
		log.Printf("  DB1 I/O: %s (write: %s, flush: %s, compact: %s)",
			helpers.FormatDuration(db1Total),
			helpers.FormatDuration(totalTimingStats.DB1WriteTime),
			helpers.FormatDuration(totalTimingStats.DB1FlushTime),
			helpers.FormatDuration(totalTimingStats.DB1CompactTime))
	}
	if config.EnableDB2 {
		db2Total := totalTimingStats.DB2WriteTime + totalTimingStats.DB2FlushTime + totalTimingStats.DB2CompactTime
		log.Printf("  DB2 I/O: %s (write: %s, flush: %s, compact: %s)",
			helpers.FormatDuration(db2Total),
			helpers.FormatDuration(totalTimingStats.DB2WriteTime),
			helpers.FormatDuration(totalTimingStats.DB2FlushTime),
			helpers.FormatDuration(totalTimingStats.DB2CompactTime))
	}
	if config.EnableDB3 {
		db3Total := totalTimingStats.DB3WriteTime + totalTimingStats.DB3FlushTime + totalTimingStats.DB3CompactTime
		log.Printf("  DB3 I/O: %s (write: %s, flush: %s, compact: %s)",
			helpers.FormatDuration(db3Total),
			helpers.FormatDuration(totalTimingStats.DB3WriteTime),
			helpers.FormatDuration(totalTimingStats.DB3FlushTime),
			helpers.FormatDuration(totalTimingStats.DB3CompactTime))
	}

	// Final compression and storage statistics
	showDetailedCompressionStats(config, totalCompressionStats, db1, db2, db3)

	log.Printf("========================================")
}

// ================================
// PARALLEL PROCESSING FUNCTIONS
// ================================

func fetchLedgersParallel(reader *RocksDBReader, startSeq, endSeq uint32, numWorkers int) []LedgerResult {
	numLedgers := int(endSeq - startSeq + 1)
	results := make([]LedgerResult, numLedgers)

	jobs := make(chan uint32, numLedgers)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

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

	for seq := startSeq; seq <= endSeq; seq++ {
		jobs <- seq
	}
	close(jobs)

	wg.Wait()
	return results
}

func fetchSingleLedger(reader *RocksDBReader, decoder *zstd.Decoder, ledgerSeq uint32) LedgerResult {
	result := LedgerResult{LedgerSeq: ledgerSeq}
	totalStart := time.Now()

	key := helpers.Uint32ToBytes(ledgerSeq)

	readStart := time.Now()
	slice, err := reader.db.Get(reader.ro, key)
	result.Timing.ReadTime = time.Since(readStart)

	if err != nil {
		result.Timing.TotalTime = time.Since(totalStart)
		result.Err = errors.Wrap(err, "failed to read from RocksDB")
		return result
	}
	defer slice.Free()

	if !slice.Exists() {
		result.Timing.TotalTime = time.Since(totalStart)
		result.Err = fmt.Errorf("ledger %d not found in RocksDB", ledgerSeq)
		return result
	}

	compressedData := make([]byte, len(slice.Data()))
	copy(compressedData, slice.Data())

	decompressStart := time.Now()
	uncompressedData, err := decoder.DecodeAll(compressedData, nil)
	result.Timing.DecompressTime = time.Since(decompressStart)

	if err != nil {
		result.Timing.TotalTime = time.Since(totalStart)
		result.Err = errors.Wrap(err, "failed to decompress ledger data")
		return result
	}

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

func compressLCMsInParallel(lcms []struct {
	ledgerSeq uint32
	lcm       xdr.LedgerCloseMeta
}, config IngestionConfig) []lcmProcessResult {
	results := make([]lcmProcessResult, len(lcms))

	numWorkers := config.NumWorkers
	jobs := make(chan int, len(lcms))

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var encoder *zstd.Encoder
			if config.EnableApplicationCompression {
				encoder, _ = zstd.NewWriter(nil)
				defer encoder.Close()
			}

			for idx := range jobs {
				item := lcms[idx]
				result := lcmProcessResult{ledgerSeq: item.ledgerSeq}

				lcmBytes, err := item.lcm.MarshalBinary()
				if err != nil {
					result.err = fmt.Errorf("failed to marshal LCM for ledger %d: %w", item.ledgerSeq, err)
					results[idx] = result
					continue
				}

				result.uncompressedSize = int64(len(lcmBytes))

				if config.EnableApplicationCompression && encoder != nil {
					result.compressedData = encoder.EncodeAll(lcmBytes, make([]byte, 0, len(lcmBytes)))
					result.compressedSize = int64(len(result.compressedData))
				} else {
					result.compressedData = lcmBytes
					result.compressedSize = int64(len(lcmBytes))
				}

				results[idx] = result
			}
		}()
	}

	for i := range lcms {
		jobs <- i
	}
	close(jobs)

	wg.Wait()
	return results
}

func compressTransactionsInParallel(
	transactions []rawTxData,
	txHashToTxData map[string][]byte,
	config IngestionConfig,
) (*CompressionStats, error) {
	stats := &CompressionStats{}

	if len(transactions) == 0 {
		return stats, nil
	}

	numWorkers := config.NumWorkers
	jobs := make(chan int, len(transactions))
	results := make(chan txProcessResult, len(transactions))

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var encoder *zstd.Encoder
			if config.EnableApplicationCompression {
				encoder, _ = zstd.NewWriter(nil)
				defer encoder.Close()
			}

			for idx := range jobs {
				tx := transactions[idx]
				res := txProcessResult{
					hash:      tx.tx.Hash.HexString(),
					ledgerSeq: tx.ledgerSeq,
				}

				txEnvelopeBytes, err := tx.tx.Envelope.MarshalBinary()
				if err != nil {
					res.err = fmt.Errorf("error marshalling tx envelope: %w", err)
					results <- res
					continue
				}

				txResultBytes, err := tx.tx.Result.MarshalBinary()
				if err != nil {
					res.err = fmt.Errorf("error marshalling tx result: %w", err)
					results <- res
					continue
				}

				txMetaBytes, err := tx.tx.UnsafeMeta.MarshalBinary()
				if err != nil {
					res.err = fmt.Errorf("error marshalling tx meta: %w", err)
					results <- res
					continue
				}

				txDataProto := tx_data.TxData{
					LedgerSequence: tx.ledgerSeq,
					ClosedAt:       timestamppb.New(tx.closedAt),
					Index:          tx.tx.Index,
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

				if config.EnableApplicationCompression && encoder != nil {
					res.compressedData = encoder.EncodeAll(txDataBytes, make([]byte, 0, len(txDataBytes)))
					res.compressedSize = int64(len(res.compressedData))
				} else {
					res.compressedData = txDataBytes
					res.compressedSize = int64(len(txDataBytes))
				}

				results <- res
			}
		}()
	}

	for i := range transactions {
		jobs <- i
	}
	close(jobs)

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

		txHashToTxData[res.hash] = res.compressedData
	}

	return stats, nil
}

// ================================
// ROCKSDB FUNCTIONS
// ================================

func createDir(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.Wrap(err, "failed to create directory")
	}
	return nil
}

func openRocksDBWithConfig(path string, name string, config IngestionConfig) (*grocksdb.DB, *grocksdb.Options, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetErrorIfExists(false)

	// Write buffer configuration
	opts.SetWriteBufferSize(uint64(config.WriteBufferSizeMB * MB))
	opts.SetMaxWriteBufferNumber(config.MaxWriteBufferNumber)
	opts.SetMinWriteBufferNumberToMerge(2)

	// L0 management
	opts.SetLevel0FileNumCompactionTrigger(config.L0CompactionTrigger)
	opts.SetLevel0SlowdownWritesTrigger(config.L0SlowdownTrigger)
	opts.SetLevel0StopWritesTrigger(config.L0StopTrigger)

	// Compaction
	opts.SetDisableAutoCompactions(config.DisableAutoCompactions)
	opts.SetCompactionStyle(grocksdb.LevelCompactionStyle)
	opts.SetTargetFileSizeBase(uint64(config.TargetFileSizeMB * MB))
	opts.SetTargetFileSizeMultiplier(2)
	opts.SetMaxBytesForLevelBase(uint64(config.MaxBytesForLevelBaseMB * MB))
	opts.SetMaxBytesForLevelMultiplier(10)

	// Background jobs
	opts.SetMaxBackgroundJobs(config.MaxBackgroundJobs)

	// Compression
	if config.EnableRocksdbCompression {
		opts.SetCompression(grocksdb.ZSTDCompression)
	} else {
		opts.SetCompression(grocksdb.NoCompression)
	}

	// Resources
	opts.SetMaxOpenFiles(config.MaxOpenFiles)

	// Block-based table options
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	if config.BlockCacheSizeMB > 0 {
		cache := grocksdb.NewLRUCache(uint64(config.BlockCacheSizeMB * MB))
		bbto.SetBlockCache(cache)
	}
	if config.BloomFilterBitsPerKey > 0 {
		bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(config.BloomFilterBitsPerKey)))
	}
	opts.SetBlockBasedTableFactory(bbto)

	// Bulk load optimizations
	if config.PrepareForBulkLoad {
		opts.PrepareForBulkLoad()
	}

	// WAL settings
	if !config.DisableWAL {
		opts.SetMaxTotalWalSize(2048 * MB)
	}

	// Logging
	opts.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)
	opts.SetMaxLogFileSize(20 * MB)
	opts.SetKeepLogFileNum(3)

	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		opts.Destroy()
		return nil, nil, errors.Wrap(err, "failed to open RocksDB")
	}

	log.Printf("[%s] RocksDB Configuration:", name)
	log.Printf("  Path: %s", path)
	log.Printf("  Write Buffer: %d MB × %d", config.WriteBufferSizeMB, config.MaxWriteBufferNumber)
	log.Printf("  L0 Compaction Trigger: %d files", config.L0CompactionTrigger)
	log.Printf("  Auto Compactions: %v", !config.DisableAutoCompactions)

	return db, opts, nil
}

func openRocksDBReader(path string) (*RocksDBReader, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	db, err := grocksdb.OpenDbForReadOnly(opts, path, false)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open RocksDB for reading")
	}

	ro := grocksdb.NewDefaultReadOptions()

	return &RocksDBReader{
		db: db,
		ro: ro,
	}, nil
}

func (r *RocksDBReader) Close() {
	if r.ro != nil {
		r.ro.Destroy()
	}
	if r.db != nil {
		r.db.Close()
	}
}

func writeBatchToRocksDB(
	db1, db2, db3 *grocksdb.DB,
	ledgerSeqToLcm map[uint32][]byte,
	txHashToTxData map[string][]byte,
	txHashToLedgerSeq map[string]uint32,
	config IngestionConfig,
) (DBTimingStats, *BytesWrittenTracker, error) {
	var timing DBTimingStats
	bytesWritten := &BytesWrittenTracker{}

	wo := grocksdb.NewDefaultWriteOptions()
	if config.DisableWAL {
		wo.DisableWAL(true)
	}
	defer wo.Destroy()

	// Write to DB1
	if config.EnableDB1 && db1 != nil && len(ledgerSeqToLcm) > 0 {
		start := time.Now()
		batch := grocksdb.NewWriteBatch()
		defer batch.Destroy()

		for ledgerSeq, compressedLcm := range ledgerSeqToLcm {
			key := helpers.Uint32ToBytes(ledgerSeq)
			batch.Put(key, compressedLcm)
			bytesWritten.DB1BytesWritten += int64(len(key) + len(compressedLcm))
		}

		if err := db1.Write(wo, batch); err != nil {
			return timing, bytesWritten, errors.Wrap(err, "failed to write batch to DB1")
		}
		timing.DB1WriteTime = time.Since(start)
	}

	// Write to DB2
	if config.EnableDB2 && db2 != nil && len(txHashToTxData) > 0 {
		start := time.Now()
		batch := grocksdb.NewWriteBatch()
		defer batch.Destroy()

		for txHashHex, compressedTxData := range txHashToTxData {
			txHashBytes, err := helpers.HexStringToBytes(txHashHex)
			if err != nil {
				return timing, bytesWritten, errors.Wrapf(err, "failed to convert tx hash: %s", txHashHex)
			}
			batch.Put(txHashBytes, compressedTxData)
			bytesWritten.DB2BytesWritten += int64(len(txHashBytes) + len(compressedTxData))
		}

		if err := db2.Write(wo, batch); err != nil {
			return timing, bytesWritten, errors.Wrap(err, "failed to write batch to DB2")
		}
		timing.DB2WriteTime = time.Since(start)
	}

	// Write to DB3
	if config.EnableDB3 && db3 != nil && len(txHashToLedgerSeq) > 0 {
		start := time.Now()
		batch := grocksdb.NewWriteBatch()
		defer batch.Destroy()

		for txHashHex, ledgerSeq := range txHashToLedgerSeq {
			txHashBytes, err := helpers.HexStringToBytes(txHashHex)
			if err != nil {
				return timing, bytesWritten, errors.Wrapf(err, "failed to convert tx hash: %s", txHashHex)
			}
			value := helpers.Uint32ToBytes(ledgerSeq)
			batch.Put(txHashBytes, value)
			bytesWritten.DB3BytesWritten += int64(len(txHashBytes) + len(value))
		}

		if err := db3.Write(wo, batch); err != nil {
			return timing, bytesWritten, errors.Wrap(err, "failed to write batch to DB3")
		}
		timing.DB3WriteTime = time.Since(start)
	}

	return timing, bytesWritten, nil
}

func flushAllDBs(db1, db2, db3 *grocksdb.DB, config IngestionConfig) DBTimingStats {
	var timing DBTimingStats
	fo := grocksdb.NewDefaultFlushOptions()
	fo.SetWait(true)
	defer fo.Destroy()

	if config.EnableDB1 && db1 != nil {
		start := time.Now()
		if err := db1.Flush(fo); err != nil {
			log.Printf("Warning: Failed to flush DB1: %v", err)
		}
		timing.DB1FlushTime = time.Since(start)
	}

	if config.EnableDB2 && db2 != nil {
		start := time.Now()
		if err := db2.Flush(fo); err != nil {
			log.Printf("Warning: Failed to flush DB2: %v", err)
		}
		timing.DB2FlushTime = time.Since(start)
	}

	if config.EnableDB3 && db3 != nil {
		start := time.Now()
		if err := db3.Flush(fo); err != nil {
			log.Printf("Warning: Failed to flush DB3: %v", err)
		}
		timing.DB3FlushTime = time.Since(start)
	}

	return timing
}

func compactAllDBs(db1, db2, db3 *grocksdb.DB, config IngestionConfig, minLedger, maxLedger uint32) DBTimingStats {
	var timing DBTimingStats

	if config.EnableDB1 && db1 != nil {
		start := time.Now()
		startKey := helpers.Uint32ToBytes(minLedger)
		endKey := helpers.Uint32ToBytes(maxLedger + 1)
		db1.CompactRange(grocksdb.Range{Start: startKey, Limit: endKey})
		timing.DB1CompactTime = time.Since(start)
		log.Printf("  DB1 compacted in %s", helpers.FormatDuration(timing.DB1CompactTime))
	}

	if config.EnableDB2 && db2 != nil {
		start := time.Now()
		db2.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
		timing.DB2CompactTime = time.Since(start)
		log.Printf("  DB2 compacted in %s", helpers.FormatDuration(timing.DB2CompactTime))
	}

	if config.EnableDB3 && db3 != nil {
		start := time.Now()
		db3.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
		timing.DB3CompactTime = time.Since(start)
		log.Printf("  DB3 compacted in %s", helpers.FormatDuration(timing.DB3CompactTime))
	}

	return timing
}

// ================================
// STATISTICS AND LOGGING FUNCTIONS
// ================================

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
	liveSSTSize := db.GetProperty("rocksdb.live-sst-files-size")
	curMemtable := db.GetProperty("rocksdb.cur-size-all-mem-tables")
	compactionPending := db.GetProperty("rocksdb.compaction-pending")
	numRunningCompactions := db.GetProperty("rocksdb.num-running-compactions")
	numRunningFlushes := db.GetProperty("rocksdb.num-running-flushes")

	log.Printf("")
	log.Printf("[%s] RocksDB Statistics:", title)
	log.Printf("  Files by level: L0=%s L1=%s L2=%s L3=%s L4=%s L5=%s L6=%s",
		l0Files, l1Files, l2Files, l3Files, l4Files, l5Files, l6Files)

	estimatedKeysInt := int64(atoiIgnoreError(estimatedKeys))
	totalSSTSizeInt := int64(atoiIgnoreError(totalSSTSize))
	liveSSTSizeInt := int64(atoiIgnoreError(liveSSTSize))

	log.Printf("  Estimated Keys: %s", helpers.FormatNumber(estimatedKeysInt))
	log.Printf("  Total SST Size: %s", helpers.FormatBytes(totalSSTSizeInt))
	log.Printf("  Live SST Size: %s", helpers.FormatBytes(liveSSTSizeInt))
	log.Printf("  Memtable Usage: %s", helpers.FormatBytes(int64(atoiIgnoreError(curMemtable))))
	log.Printf("  Compaction Pending: %s", compactionPending)
	log.Printf("  Running Compactions: %s", numRunningCompactions)
	log.Printf("  Running Flushes: %s", numRunningFlushes)

	if estimatedKeysInt > 0 && totalSSTSizeInt > 0 {
		avgKeyValueSize := float64(totalSSTSizeInt) / float64(estimatedKeysInt)
		log.Printf("  Avg Key+Value Size (on disk): %.2f bytes", avgKeyValueSize)
	}
}

func logBatchCompletion(batch BatchInfo, writeTiming DBTimingStats, flushTiming DBTimingStats, config IngestionConfig, stats CompressionStats) {
	totalBatchDuration := time.Since(batch.StartTime)

	totalLedgers := config.EndLedger - config.StartLedger + 1
	totalBatches := (totalLedgers + uint32(config.BatchSize) - 1) / uint32(config.BatchSize)

	ioTime := writeTiming.DB1WriteTime + writeTiming.DB2WriteTime + writeTiming.DB3WriteTime +
		flushTiming.DB1FlushTime + flushTiming.DB2FlushTime + flushTiming.DB3FlushTime
	compressionTime := batch.BatchDB1CompressionTime + batch.BatchDB2CompressionTime
	computeTime := totalBatchDuration - batch.BatchGetLedgerTime - compressionTime - ioTime

	log.Printf("\n===== Batch #%d of %d Complete =====", batch.BatchNum, totalBatches)
	log.Printf("Ledger Range: %d - %d", batch.StartLedger, batch.EndLedger)
	log.Printf("Batch Size: %d (actual: %d ledgers, %d transactions)",
		config.BatchSize, batch.LedgerCount, batch.TxCount)
	log.Printf("Total Batch Time: %s", helpers.FormatDuration(totalBatchDuration))

	log.Printf("")
	log.Printf("\tCompute Time: %s", helpers.FormatDuration(computeTime))

	log.Printf("")
	log.Printf("===== Ledger Fetching Times =====")
	log.Printf("\tTotal GetLedger Time: %s", helpers.FormatDuration(batch.BatchGetLedgerTime))

	if batch.BatchRocksDBTimingStats.TotalTime > 0 && batch.LedgerCount > 0 {
		rocksStats := batch.BatchRocksDBTimingStats
		log.Printf("\tRocksDB Breakdown (wall-clock estimate):")
		log.Printf("\t\tRead:       %s", helpers.FormatDuration(rocksStats.ReadTime/time.Duration(config.NumWorkers)))
		log.Printf("\t\tDecompress: %s", helpers.FormatDuration(rocksStats.DecompressTime/time.Duration(config.NumWorkers)))
		log.Printf("\t\tUnmarshal:  %s", helpers.FormatDuration(rocksStats.UnmarshalTime/time.Duration(config.NumWorkers)))
	}

	if batch.LedgerCount > 0 {
		log.Printf("\tAvg GetLedger per ledger: %s",
			helpers.FormatDuration(batch.BatchGetLedgerTime/time.Duration(batch.LedgerCount)))
	}

	log.Printf("")
	if config.EnableDB1 {
		log.Printf("===== DB1 Metrics =====")
		log.Printf("\tCompression: %s", helpers.FormatDuration(batch.BatchDB1CompressionTime))
		log.Printf("\tWrite: %s", helpers.FormatDuration(writeTiming.DB1WriteTime))
		log.Printf("\tFlush: %s", helpers.FormatDuration(flushTiming.DB1FlushTime))
	}

	if config.EnableDB2 {
		log.Printf("===== DB2 Metrics =====")
		log.Printf("\tCompression: %s", helpers.FormatDuration(batch.BatchDB2CompressionTime))
		if batch.TxCount > 0 {
			log.Printf("\t\tAvg per tx: %s",
				helpers.FormatDuration(batch.BatchDB2CompressionTime/time.Duration(batch.TxCount)))
		}
		log.Printf("\tWrite: %s", helpers.FormatDuration(writeTiming.DB2WriteTime))
		if batch.TxCount > 0 {
			log.Printf("\t\tAvg per tx: %s",
				helpers.FormatDuration(writeTiming.DB2WriteTime/time.Duration(batch.TxCount)))
		}
		log.Printf("\tFlush: %s", helpers.FormatDuration(flushTiming.DB2FlushTime))
	}

	if config.EnableDB3 {
		log.Printf("===== DB3 Metrics =====")
		log.Printf("\tWrite: %s", helpers.FormatDuration(writeTiming.DB3WriteTime))
		if batch.TxCount > 0 {
			log.Printf("\t\tAvg per tx: %s",
				helpers.FormatDuration(writeTiming.DB3WriteTime/time.Duration(batch.TxCount)))
		}
		log.Printf("\tFlush: %s", helpers.FormatDuration(flushTiming.DB3FlushTime))
	}

	log.Printf("")
	log.Printf("=== BATCH AVERAGES ===")
	if batch.LedgerCount > 0 {
		avgTimePerLedger := totalBatchDuration / time.Duration(batch.LedgerCount)
		log.Printf("\tAvg time per ledger: %s", helpers.FormatDuration(avgTimePerLedger))
	}
	if batch.TxCount > 0 {
		avgTimePerTx := totalBatchDuration / time.Duration(batch.TxCount)
		log.Printf("\tAvg time per transaction: %s", helpers.FormatDuration(avgTimePerTx))
	}

	log.Printf("")
	log.Printf("Total time elapsed: %s", helpers.FormatDuration(time.Since(startTime)))
	log.Printf("========================================\n")
}

func logDatabaseStats(db1, db2, db3 *grocksdb.DB, config IngestionConfig, stats CompressionStats, currentLedger uint32) {
	st := time.Now()
	log.Printf("\n========================================")
	log.Printf("========= ROCKSDB STATS at ledger %d =====", currentLedger)
	if config.EnableDB1 && db1 != nil {
		monitorRocksDBStats(db1, "DB1")
	}
	if config.EnableDB2 && db2 != nil {
		monitorRocksDBStats(db2, "DB2")
	}
	if config.EnableDB3 && db3 != nil {
		monitorRocksDBStats(db3, "DB3")
	}
	showCompressionStats(config, stats)
	log.Printf("Time for stats: %s", helpers.FormatDuration(time.Since(st)))
	log.Printf("========================================\n")
}

func showCompressionStats(config IngestionConfig, stats CompressionStats) {
	if !config.EnableApplicationCompression {
		return
	}

	log.Printf("")
	log.Printf("Compression Statistics (running totals):")

	if config.EnableDB1 && stats.UncompressedLCM > 0 {
		compressionRatio := 100 * (1 - float64(stats.CompressedLCM)/float64(stats.UncompressedLCM))
		log.Printf("  LCM (DB1):")
		log.Printf("    Original:   %s", helpers.FormatBytes(stats.UncompressedLCM))
		log.Printf("    Compressed: %s", helpers.FormatBytes(stats.CompressedLCM))
		log.Printf("    Ratio:      %.2f%% reduction", compressionRatio)
		log.Printf("    Saved:      %s", helpers.FormatBytes(stats.UncompressedLCM-stats.CompressedLCM))
	}

	if config.EnableDB2 && stats.UncompressedTx > 0 {
		compressionRatio := 100 * (1 - float64(stats.CompressedTx)/float64(stats.UncompressedTx))
		log.Printf("  TxData (DB2):")
		log.Printf("    Original:   %s", helpers.FormatBytes(stats.UncompressedTx))
		log.Printf("    Compressed: %s", helpers.FormatBytes(stats.CompressedTx))
		log.Printf("    Ratio:      %.2f%% reduction", compressionRatio)
		log.Printf("    Saved:      %s", helpers.FormatBytes(stats.UncompressedTx-stats.CompressedTx))

		if stats.TxCount > 0 {
			avgUncompressed := float64(stats.UncompressedTx) / float64(stats.TxCount)
			avgCompressed := float64(stats.CompressedTx) / float64(stats.TxCount)
			log.Printf("    Avg per tx: %.2f bytes → %.2f bytes", avgUncompressed, avgCompressed)
		}
	}
}

func showDetailedCompressionStats(config IngestionConfig, stats CompressionStats, db1, db2, db3 *grocksdb.DB) {
	log.Printf("\n========================================")
	log.Printf("DETAILED COMPRESSION & STORAGE ANALYSIS")
	log.Printf("========================================")

	if config.EnableDB1 && stats.UncompressedLCM > 0 {
		compressionRatio := 100 * (1 - float64(stats.CompressedLCM)/float64(stats.UncompressedLCM))

		var diskSize int64
		if db1 != nil {
			diskSize = int64(atoiIgnoreError(db1.GetProperty("rocksdb.total-sst-files-size")))
		}

		log.Printf("")
		log.Printf("DB1 (LCM Storage):")
		log.Printf("  Raw data size:        %s", helpers.FormatBytes(stats.UncompressedLCM))
		log.Printf("  After zstd compress:  %s (%.2f%% reduction)", helpers.FormatBytes(stats.CompressedLCM), compressionRatio)
		log.Printf("  Actual disk usage:    %s", helpers.FormatBytes(diskSize))

		if stats.CompressedLCM > 0 && diskSize > 0 {
			overhead := float64(diskSize-stats.CompressedLCM) / float64(stats.CompressedLCM) * 100
			log.Printf("  RocksDB overhead:     %.2f%%", overhead)
		}

		if stats.LedgerCount > 0 {
			avgRaw := float64(stats.UncompressedLCM) / float64(stats.LedgerCount)
			avgCompressed := float64(stats.CompressedLCM) / float64(stats.LedgerCount)
			log.Printf("  Avg per ledger:       %.2f bytes raw → %.2f bytes compressed", avgRaw, avgCompressed)
		}
	}

	if config.EnableDB2 && stats.UncompressedTx > 0 {
		compressionRatio := 100 * (1 - float64(stats.CompressedTx)/float64(stats.UncompressedTx))

		var diskSize int64
		if db2 != nil {
			diskSize = int64(atoiIgnoreError(db2.GetProperty("rocksdb.total-sst-files-size")))
		}

		log.Printf("")
		log.Printf("DB2 (Transaction Storage):")
		log.Printf("  Raw data size:        %s", helpers.FormatBytes(stats.UncompressedTx))
		log.Printf("  After zstd compress:  %s (%.2f%% reduction)", helpers.FormatBytes(stats.CompressedTx), compressionRatio)
		log.Printf("  Actual disk usage:    %s", helpers.FormatBytes(diskSize))

		if stats.CompressedTx > 0 && diskSize > 0 {
			overhead := float64(diskSize-stats.CompressedTx) / float64(stats.CompressedTx) * 100
			log.Printf("  RocksDB overhead:     %.2f%%", overhead)
		}

		if stats.TxCount > 0 {
			avgRaw := float64(stats.UncompressedTx) / float64(stats.TxCount)
			avgCompressed := float64(stats.CompressedTx) / float64(stats.TxCount)
			avgDisk := float64(diskSize) / float64(stats.TxCount)
			log.Printf("  Avg per transaction:  %.2f bytes raw → %.2f bytes compressed → %.2f bytes on disk",
				avgRaw, avgCompressed, avgDisk)
		}
	}

	if config.EnableDB3 && stats.TxCount > 0 {
		var diskSize int64
		if db3 != nil {
			diskSize = int64(atoiIgnoreError(db3.GetProperty("rocksdb.total-sst-files-size")))
		}

		// DB3 stores: 32-byte hash key + 4-byte ledger seq value = 36 bytes per entry
		expectedSize := stats.TxCount * 36

		log.Printf("")
		log.Printf("DB3 (Hash -> LedgerSeq Mapping):")
		log.Printf("  Expected raw size:    %s (%d entries × 36 bytes)", helpers.FormatBytes(expectedSize), stats.TxCount)
		log.Printf("  Actual disk usage:    %s", helpers.FormatBytes(diskSize))

		if expectedSize > 0 && diskSize > 0 {
			overhead := float64(diskSize-expectedSize) / float64(expectedSize) * 100
			log.Printf("  RocksDB overhead:     %.2f%%", overhead)
		}
	}

	// Total storage summary
	var totalRawSize, totalCompressedSize, totalDiskSize int64
	totalRawSize = stats.UncompressedLCM + stats.UncompressedTx
	totalCompressedSize = stats.CompressedLCM + stats.CompressedTx

	if config.EnableDB1 && db1 != nil {
		totalDiskSize += int64(atoiIgnoreError(db1.GetProperty("rocksdb.total-sst-files-size")))
	}
	if config.EnableDB2 && db2 != nil {
		totalDiskSize += int64(atoiIgnoreError(db2.GetProperty("rocksdb.total-sst-files-size")))
	}
	if config.EnableDB3 && db3 != nil {
		totalDiskSize += int64(atoiIgnoreError(db3.GetProperty("rocksdb.total-sst-files-size")))
	}

	log.Printf("")
	log.Printf("TOTAL STORAGE SUMMARY:")
	log.Printf("  Raw data:             %s", helpers.FormatBytes(totalRawSize))
	log.Printf("  After compression:    %s", helpers.FormatBytes(totalCompressedSize))
	log.Printf("  Actual disk usage:    %s", helpers.FormatBytes(totalDiskSize))

	if totalCompressedSize > 0 {
		overallOverhead := float64(totalDiskSize-totalCompressedSize) / float64(totalCompressedSize) * 100
		log.Printf("  Overall overhead:     %.2f%%", overallOverhead)
	}
	if totalRawSize > 0 {
		overallCompression := 100 * (1 - float64(totalDiskSize)/float64(totalRawSize))
		log.Printf("  Effective compression: %.2f%% (raw → disk)", overallCompression)
	}
}

func showRocksDBTimingStats(timing RocksDBTimingStats, ledgerCount int, config IngestionConfig) {
	numWorkers := config.NumWorkers

	log.Printf("")
	log.Printf("RocksDB Read Timing (wall-clock estimates):")
	log.Printf("  Total Read:       %s", helpers.FormatDuration(timing.ReadTime/time.Duration(numWorkers)))
	log.Printf("  Total Decompress: %s", helpers.FormatDuration(timing.DecompressTime/time.Duration(numWorkers)))
	log.Printf("  Total Unmarshal:  %s", helpers.FormatDuration(timing.UnmarshalTime/time.Duration(numWorkers)))

	if ledgerCount > 0 {
		avgRead := timing.ReadTime / time.Duration(ledgerCount)
		avgDecompress := timing.DecompressTime / time.Duration(ledgerCount)
		avgUnmarshal := timing.UnmarshalTime / time.Duration(ledgerCount)
		log.Printf("  Avg per ledger - Read: %s, Decompress: %s, Unmarshal: %s",
			helpers.FormatDuration(avgRead), helpers.FormatDuration(avgDecompress), helpers.FormatDuration(avgUnmarshal))
	}
}

func reportProgress(processedCount, totalLedgers, skippedCount int, stats CompressionStats, lastPercent *int, currentLedger uint32) {
	currentPercent := (processedCount * 100) / totalLedgers
	if currentPercent > *lastPercent {
		elapsed := time.Since(startTime)
		ledgersPerSec := float64(processedCount) / elapsed.Seconds()
		remaining := totalLedgers - processedCount
		var eta time.Duration
		if ledgersPerSec > 0 {
			eta = time.Duration(float64(remaining)/ledgersPerSec) * time.Second
		}

		log.Printf("")
		log.Printf("\n========================================")
		log.Printf("PROGRESS: %d/%d ledgers (%d%%) | Ledger: %d",
			processedCount, totalLedgers, currentPercent, currentLedger)
		if skippedCount > 0 {
			log.Printf("Skipped ledgers: %d", skippedCount)
		}
		log.Printf("Speed: %.2f ledgers/sec | Transactions: %s | ETA: %s",
			ledgersPerSec, helpers.FormatNumber(stats.TxCount), helpers.FormatDuration(eta))
		log.Printf("========================================\n")
		*lastPercent = currentPercent
	}
}

func atoiIgnoreError(s string) int {
	value, _ := strconv.Atoi(s)
	return value
}
