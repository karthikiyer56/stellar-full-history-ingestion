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
	StartLedger        uint32
	EndLedger          uint32
	BatchSize          int
	DB1Path            string // Optional: ledgerSeq -> compressed LCM
	DB2Path            string // Required: txHash -> compressed TxData
	DB3Path            string // Required: txHash -> ledgerSeq
	EnableDB1          bool   // Derived: true if DB1Path is provided
	rocksdbCompression bool   // rely on rocksdb native compression
	dataCompression    bool   // do it in code by calling zstd

}

// CompressionStats tracks compression metrics for a batch
type CompressionStats struct {
	UncompressedLCM int64
	CompressedLCM   int64
	UncompressedTx  int64
	CompressedTx    int64
	TxCount         int64
}

func main() {
	// Command-line flags
	var startLedger, endLedger uint
	var batchSize int
	var db1Path, db2Path, db3Path string

	var rocksdbCompression, dataCompression bool

	flag.UintVar(&startLedger, "start-ledger", 0, "Starting ledger sequence number")
	flag.IntVar(&batchSize, "batch-size", 1000, "Batch size for commit")
	flag.UintVar(&endLedger, "end-ledger", 0, "Ending ledger sequence number")
	flag.StringVar(&db1Path, "db1", "", "Optional path for DataStore 1 (ledgerSeq -> compressed LCM)")
	flag.StringVar(&db2Path, "db2", "", "Path for DataStore 2 (txHash -> compressed TxData)")
	flag.StringVar(&db3Path, "db3", "", "Path for DataStore 3 (txHash -> ledgerSeq)")
	flag.BoolVar(&rocksdbCompression, "rocksdb-compression", false, "Compress using RocksDB")
	flag.BoolVar(&dataCompression, "data-compression", true, "Compress using zstd in application code")
	flag.Parse()

	// Validate required arguments
	if startLedger == 0 || endLedger == 0 {
		log.Fatal("start-ledger and end-ledger are required")
	}
	if batchSize <= 0 {
		log.Fatal("batch-size cannot be less than or equal to 0")
	}
	if db2Path == "" || db3Path == "" {
		log.Fatal("Database paths db2 and db3 are required")
	}

	// Create config
	config := IngestionConfig{
		StartLedger:        uint32(startLedger),
		EndLedger:          uint32(endLedger),
		BatchSize:          batchSize,
		DB1Path:            db1Path,
		DB2Path:            db2Path,
		DB3Path:            db3Path,
		EnableDB1:          db1Path != "",
		rocksdbCompression: rocksdbCompression,
		dataCompression:    dataCompression,
	}

	// Initialize RocksDB instances
	var db1 *grocksdb.DB
	var opts1 *grocksdb.Options

	// Only open DB1 if path is provided
	if config.EnableDB1 {
		var err error
		db1, opts1, err = openRocksDB(config.DB1Path, config.rocksdbCompression)
		if err != nil {
			log.Fatalf("Failed to open DB1 (ledgerSeq -> LCM): %v", err)
		}
		defer func() {
			db1.Close()
			opts1.Destroy()
		}()
		log.Printf("✓ DB1 opened at: %s, compressionEnabled: %v", config.DB1Path, config.rocksdbCompression)
	} else {
		log.Printf("ℹ DB1 (ledgerSeq -> LCM) is disabled (no path provided)")
	}

	db2, opts2, err := openRocksDB(config.DB2Path, config.rocksdbCompression)
	if err != nil {
		log.Fatalf("Failed to open DB2 (txHash -> TxData): %v", err)
	}
	defer func() {
		db2.Close()
		opts2.Destroy()
	}()
	log.Printf("✓ DB2 opened at: %s, compressionEnabled: %v", config.DB2Path, config.rocksdbCompression)

	db3, opts3, err := openRocksDB(config.DB3Path, config.rocksdbCompression)
	if err != nil {
		log.Fatalf("Failed to open DB3 (txHash -> ledgerSeq): %v", err)
	}
	defer func() {
		db3.Close()
		opts3.Destroy()
	}()
	log.Printf("✓ DB3 opened at: %s, compressionEnabled: %v", config.DB3Path, config.rocksdbCompression)

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

	// Set up metrics tracking
	startTime := time.Now()
	processedCount := 0
	lastReportedPercent := -1

	var totalProcessingTime time.Duration
	var totalFlushTime time.Duration
	var totalStats CompressionStats

	// Batch maps - only create ledgerSeqToLcm if DB1 is enabled
	var ledgerSeqToLcm map[uint32][]byte
	if config.EnableDB1 {
		ledgerSeqToLcm = make(map[uint32][]byte)
	}
	txHashToTxData := make(map[string][]byte)
	txHashToLedgerSeq := make(map[string]uint32)

	// Create a reusable zstd encoder for better performance
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to create zstd encoder"))
	}
	defer encoder.Close()

	// Iterate through the ledger sequence. DO NOT INCLUDE LAST LEDGER. THIS IS DELIBERATE
	for ledgerSeq := ledgerRange.From(); ledgerSeq < ledgerRange.To(); ledgerSeq++ {
		processStart := time.Now()

		ledger, err := backend.GetLedger(ctx, ledgerSeq)
		if err != nil {
			log.Fatalf("Failed to retrieve ledger %d: %v", ledgerSeq, err)
		}

		// Process ledger and update maps
		stats, err := processLedger(encoder, ledger, ledgerSeqToLcm, txHashToTxData, txHashToLedgerSeq, config)
		if err != nil {
			log.Printf("Error processing ledger %d: %v", ledgerSeq, err)
			continue
		}

		// Accumulate stats
		totalStats.UncompressedLCM += stats.UncompressedLCM
		totalStats.CompressedLCM += stats.CompressedLCM
		totalStats.UncompressedTx += stats.UncompressedTx
		totalStats.CompressedTx += stats.CompressedTx
		totalStats.TxCount += stats.TxCount

		totalProcessingTime += time.Since(processStart)
		processedCount++

		// Write batch every batchSize ledgers
		if processedCount%config.BatchSize == 0 {
			flushStart := time.Now()

			lcmCount := 0
			if config.EnableDB1 {
				lcmCount = len(ledgerSeqToLcm)
			}
			log.Printf("\n(startLedger - %d, endLedger - %d)[Ledger %d] Writing batch to disk (%d ledgers, %d transactions)...",
				config.StartLedger, config.EndLedger, ledgerSeq, lcmCount, len(txHashToTxData))

			// Write to databases
			if err := writeBatchToDB(db1, db2, db3, ledgerSeqToLcm, txHashToTxData, txHashToLedgerSeq, config.EnableDB1); err != nil {
				log.Printf("Error writing batch: %v", err)
			}

			// Flush to disk
			log.Printf("(startLedger - %d, endLedger - %d)[Ledger %d] Flushing databases to disk...", config.StartLedger, config.EndLedger, ledgerSeq)
			flushAllDBs(db1, db2, db3, config.EnableDB1)

			totalFlushTime += time.Since(flushStart)

			// Show database sizes
			showDBSizes(config)

			// Clear batch maps
			if config.EnableDB1 {
				ledgerSeqToLcm = make(map[uint32][]byte)
			}
			txHashToTxData = make(map[string][]byte)
			txHashToLedgerSeq = make(map[string]uint32)
		}

		// Report progress every 1%
		currentPercent := (processedCount * 100) / totalLedgers
		if currentPercent > lastReportedPercent {
			elapsed := time.Since(startTime)
			ledgersPerSec := float64(processedCount) / elapsed.Seconds()
			remaining := totalLedgers - processedCount
			var eta time.Duration
			if ledgersPerSec > 0 {
				eta = time.Duration(float64(remaining)/ledgersPerSec) * time.Second
			}

			log.Printf("\n(startLedger - %d, endLedger - %d) Progress: %d/%d ledgers (%d%%) | %.2f ledgers/sec | %s transactions | ETA: %s",
				config.StartLedger, config.EndLedger, processedCount, totalLedgers, currentPercent,
				ledgersPerSec, formatNumber(totalStats.TxCount), formatDuration(eta))

			lastReportedPercent = currentPercent
		}
	}

	// Write any remaining batch data
	lcmCount := 0
	if config.EnableDB1 && len(ledgerSeqToLcm) > 0 {
		lcmCount = len(ledgerSeqToLcm)
	}
	if lcmCount > 0 || len(txHashToTxData) > 0 {
		log.Printf("\nWriting final batch to disk (%d ledgers, %d transactions)...",
			lcmCount, len(txHashToTxData))

		flushStart := time.Now()
		if err := writeBatchToDB(db1, db2, db3, ledgerSeqToLcm, txHashToTxData, txHashToLedgerSeq, config.EnableDB1); err != nil {
			log.Printf("Error writing final batch: %v", err)
		}

		log.Printf("Flushing databases to disk...")
		flushAllDBs(db1, db2, db3, config.EnableDB1)

		totalFlushTime += time.Since(flushStart)
	}

	elapsed := time.Since(startTime)

	// Trigger final compaction to ensure everything is optimized
	log.Printf("\nPerforming final compaction...")

	log.Printf("\n========================================")
	log.Printf("------ DB SIZES before compaction -----")
	showDBSizes(config)
	log.Printf("\n========================================")

	finalCompactionStart := time.Now()
	compactAllDBs(db1, db2, db3, config.EnableDB1)
	totalFlushTime += time.Since(finalCompactionStart)

	log.Printf("Final compaction complete in %s", formatDuration(time.Since(finalCompactionStart)))
	log.Printf("\n========================================")
	log.Printf("------ DB SIZES after compaction -----")
	showDBSizes(config)
	log.Printf("\n========================================")

	// Print final statistics
	log.Printf("\n========================================")
	log.Printf("INGESTION COMPLETE")
	log.Printf("========================================")
	log.Printf("Total ledgers processed:  %s", formatNumber(int64(processedCount)))
	log.Printf("Total transactions:       %s", formatNumber(totalStats.TxCount))
	log.Printf("Total time:               %s", formatDuration(elapsed))
	log.Printf("Average speed:            %.2f ledgers/sec", float64(processedCount)/elapsed.Seconds())
	log.Printf("")
	log.Printf("Time breakdown:")
	log.Printf("  Processing:             %s (%.1f%%)", formatDuration(totalProcessingTime),
		100*totalProcessingTime.Seconds()/elapsed.Seconds())
	log.Printf("  Disk I/O (flush+compact): %s (%.1f%%)", formatDuration(totalFlushTime),
		100*totalFlushTime.Seconds()/elapsed.Seconds())
	log.Printf("")

	// Compression statistics
	if config.dataCompression {
		if config.EnableDB1 && totalStats.UncompressedLCM > 0 {
			compressionRatio := 100 * (1 - float64(totalStats.CompressedLCM)/float64(totalStats.UncompressedLCM))
			log.Printf("LCM Compression:")
			log.Printf("  Original size:          %s", formatBytes(totalStats.UncompressedLCM))
			log.Printf("  Compressed size:        %s", formatBytes(totalStats.CompressedLCM))
			log.Printf("  Compression ratio:      %.2f%% reduction", compressionRatio)
			log.Printf("  Space saved:            %s", formatBytes(totalStats.UncompressedLCM-totalStats.CompressedLCM))
		}

		if totalStats.UncompressedTx > 0 {
			compressionRatio := 100 * (1 - float64(totalStats.CompressedTx)/float64(totalStats.UncompressedTx))
			log.Printf("")
			log.Printf("TxData Compression:")
			log.Printf("  Original size:          %s", formatBytes(totalStats.UncompressedTx))
			log.Printf("  Compressed size:        %s", formatBytes(totalStats.CompressedTx))
			log.Printf("  Compression ratio:      %.2f%% reduction", compressionRatio)
			log.Printf("  Space saved:            %s", formatBytes(totalStats.UncompressedTx-totalStats.CompressedTx))
		}
	}

	log.Printf("")
	log.Printf("========================================\n")
}

// processLedger processes a single ledger and updates the batch maps
func processLedger(
	encoder *zstd.Encoder,
	lcm xdr.LedgerCloseMeta,
	ledgerSeqToLcm map[uint32][]byte,
	txHashToTxData map[string][]byte,
	txHashToLedgerSeq map[string]uint32,
	config IngestionConfig,
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
	if config.EnableDB1 {
		lcmBytes, err := lcm.MarshalBinary()
		if err != nil {
			return stats, errors.Wrapf(err, "failed to marshal lcm for ledger: %d", ledgerSeq)
		}

		if config.dataCompression {
			stats.UncompressedLCM = int64(len(lcmBytes))

			compressedLcm := compressData(encoder, lcmBytes)
			stats.CompressedLCM = int64(len(compressedLcm))

			ledgerSeqToLcm[ledgerSeq] = compressedLcm
		} else {
			ledgerSeqToLcm[ledgerSeq] = lcmBytes
		}
	}

	// Process each transaction
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

		txHashHex := tx.Hash.HexString()

		if config.dataCompression {
			stats.UncompressedTx += int64(len(txDataBytes))

			// Compress TxData
			compressedTxData := compressData(encoder, txDataBytes)
			stats.CompressedTx += int64(len(compressedTxData))

			// Use hex string as map key (for convenience), but we'll convert to binary when writing to DB
			txHashToTxData[txHashHex] = compressedTxData

		} else {
			txHashToTxData[txHashHex] = txDataBytes
		}

		txHashToLedgerSeq[txHashHex] = ledgerSeq
		stats.TxCount++
	}

	return stats, nil
}

// compressData compresses data using zstd (reuses encoder for efficiency)
func compressData(encoder *zstd.Encoder, data []byte) []byte {
	return encoder.EncodeAll(data, make([]byte, 0, len(data)))
}

// writeBatchToDB writes the batch data to RocksDB instances
func writeBatchToDB(
	db1, db2, db3 *grocksdb.DB,
	ledgerSeqToLcm map[uint32][]byte,
	txHashToTxData map[string][]byte,
	txHashToLedgerSeq map[string]uint32,
	enableDB1 bool,
) error {
	wo := grocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true) // Disable WAL for bulk loading performance
	defer wo.Destroy()

	// Write to DB1 (ledgerSeq -> compressed LCM) only if enabled
	if enableDB1 && db1 != nil {
		for ledgerSeq, compressedLcm := range ledgerSeqToLcm {
			key := uint32ToBytes(ledgerSeq)
			if err := db1.Put(wo, key, compressedLcm); err != nil {
				return errors.Wrap(err, "failed to write to DB1")
			}
		}
	}

	// Write to DB2 (txHash -> compressed TxData)
	for txHashHex, compressedTxData := range txHashToTxData {
		txHashBytes, err := hexStringToBytes(txHashHex)
		if err != nil {
			return errors.Wrapf(err, "failed to convert tx hash to bytes: %s", txHashHex)
		}
		if err := db2.Put(wo, txHashBytes, compressedTxData); err != nil {
			return errors.Wrap(err, "failed to write to DB2")
		}
	}

	// Write to DB3 (txHash -> ledgerSeq)
	for txHashHex, ledgerSeq := range txHashToLedgerSeq {
		txHashBytes, err := hexStringToBytes(txHashHex)
		if err != nil {
			return errors.Wrapf(err, "failed to convert tx hash to bytes: %s", txHashHex)
		}
		value := uint32ToBytes(ledgerSeq)
		if err := db3.Put(wo, txHashBytes, value); err != nil {
			return errors.Wrap(err, "failed to write to DB3")
		}
	}

	return nil
}

// flushAllDBs flushes databases to disk
func flushAllDBs(db1, db2, db3 *grocksdb.DB, enableDB1 bool) {
	fo := grocksdb.NewDefaultFlushOptions()
	fo.SetWait(true)
	defer fo.Destroy()

	if enableDB1 && db1 != nil {
		if err := db1.Flush(fo); err != nil {
			log.Printf("Warning: Failed to flush DB1: %v", err)
		}
	}
	if err := db2.Flush(fo); err != nil {
		log.Printf("Warning: Failed to flush DB2: %v", err)
	}
	if err := db3.Flush(fo); err != nil {
		log.Printf("Warning: Failed to flush DB3: %v", err)
	}
}

// compactAllDBs performs full compaction on databases
func compactAllDBs(db1, db2, db3 *grocksdb.DB, enableDB1 bool) {
	if enableDB1 && db1 != nil {
		db1.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
	}
	db2.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
	db3.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
}

// showDBSizes displays the current sizes of databases
func showDBSizes(config IngestionConfig) {
	log.Printf("Database sizes:")
	if config.EnableDB1 {
		size1, _ := getDirSize(config.DB1Path)
		log.Printf("  DB1 (LCM):              %s", formatBytes(size1))
	}
	size2, _ := getDirSize(config.DB2Path)
	size3, _ := getDirSize(config.DB3Path)
	log.Printf("  DB2 (TxData):           %s", formatBytes(size2))
	log.Printf("  DB3 (Hash->Seq):        %s", formatBytes(size3))

	total := size2 + size3
	if config.EnableDB1 {
		size1, _ := getDirSize(config.DB1Path)
		total += size1
	}
	log.Printf("  Total:                  %s", formatBytes(total))
}

// openRocksDB opens or creates a RocksDB database with optimized settings
func openRocksDB(path string, isCompressionEnabled bool) (*grocksdb.DB, *grocksdb.Options, error) {
	// Check if database exists
	if _, err := os.Stat(path); err == nil {
		log.Printf("Opening existing database at %s", path)
	} else {
		log.Printf("Creating new database at %s", path)
		// Create parent directory if it doesn't exist
		parentDir := filepath.Dir(path)
		if err := os.MkdirAll(parentDir, 0755); err != nil {
			return nil, nil, errors.Wrap(err, "failed to create database parent directory")
		}
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetErrorIfExists(false)

	// Optimize for bulk writes and large datasets
	if isCompressionEnabled {
		opts.SetCompression(grocksdb.ZSTDCompression)
	} else {
		opts.SetCompression(grocksdb.NoCompression)

	}
	opts.SetWriteBufferSize(128 << 20) // 128 MB
	opts.SetMaxWriteBufferNumber(3)
	opts.SetTargetFileSizeBase(128 << 20) // 128 MB
	opts.SetMaxBackgroundJobs(6)
	opts.SetMaxOpenFiles(1000)

	// Reduce RocksDB logging
	opts.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)
	opts.SetMaxLogFileSize(10 << 20) // 10 MB
	opts.SetKeepLogFileNum(2)

	opts.SetDisableAutoCompactions(false)
	opts.SetLevel0FileNumCompactionTrigger(4)
	opts.SetLevel0SlowdownWritesTrigger(20)
	opts.SetLevel0StopWritesTrigger(30)

	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		opts.Destroy()
		return nil, nil, errors.Wrap(err, "failed to open RocksDB")
	}

	return db, opts, nil
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

// getDirSize calculates the total size of a directory
func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
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
