package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	var db1Path, db2Path, db3Path string

	flag.UintVar(&startLedger, "start-ledger", 0, "Starting ledger sequence number")
	flag.UintVar(&endLedger, "end-ledger", 0, "Ending ledger sequence number")
	flag.StringVar(&db1Path, "db1", "", "Path for DataStore 1 (ledgerSeq -> compressed LCM)")
	flag.StringVar(&db2Path, "db2", "", "Path for DataStore 2 (txHash -> compressed TxData)")
	flag.StringVar(&db3Path, "db3", "", "Path for DataStore 3 (txHash -> ledgerSeq)")
	flag.Parse()

	// Validate required arguments
	if startLedger == 0 || endLedger == 0 {
		log.Fatal("start-ledger and end-ledger are required")
	}
	if db1Path == "" || db2Path == "" || db3Path == "" {
		log.Fatal("All three database paths (db1, db2, db3) are required")
	}

	// Initialize RocksDB instances
	db1, opts1, err := openRocksDB(db1Path, true)
	if err != nil {
		log.Fatalf("Failed to open DB1 (ledgerSeq -> LCM): %v", err)
	}
	defer func() {
		db1.Close()
		opts1.Destroy()
	}()
	log.Printf("✓ DB1 opened at: %s", db1Path)

	db2, opts2, err := openRocksDB(db2Path, true)
	if err != nil {
		log.Fatalf("Failed to open DB2 (txHash -> TxData): %v", err)
	}
	defer func() {
		db2.Close()
		opts2.Destroy()
	}()
	log.Printf("✓ DB2 opened at: %s", db2Path)

	db3, opts3, err := openRocksDB(db3Path, true)
	if err != nil {
		log.Fatalf("Failed to open DB3 (txHash -> ledgerSeq): %v", err)
	}
	defer func() {
		db3.Close()
		opts3.Destroy()
	}()
	log.Printf("✓ DB3 opened at: %s", db3Path)

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

	start, end := uint32(startLedger), uint32(endLedger)
	ledgerRange := ledgerbackend.BoundedRange(start, end)
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

	// Set up metrics tracking
	startTime := time.Now()
	processedCount := 0
	lastReportedPercent := -1

	var totalProcessingTime time.Duration
	var totalFlushTime time.Duration

	var totalStats CompressionStats

	// Batch maps
	const batchSize = 1000
	ledgerSeqToLcm := make(map[uint32][]byte)
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
		stats, err := processLedger(encoder, ledger, ledgerSeqToLcm, txHashToTxData, txHashToLedgerSeq)
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
		if processedCount%batchSize == 0 {
			flushStart := time.Now()

			log.Printf("\n[Ledger %d] Writing batch to disk (%d ledgers, %d transactions)...",
				ledgerSeq, len(ledgerSeqToLcm), len(txHashToTxData))

			// Write to all three databases
			if err := writeBatchToDB(db1, db2, db3, ledgerSeqToLcm, txHashToTxData, txHashToLedgerSeq); err != nil {
				log.Printf("Error writing batch: %v", err)
			}

			// Flush to disk
			log.Printf("[Ledger %d] Flushing databases to disk...", ledgerSeq)
			flushAllDBs(db1, db2, db3)

			// Compact to remove duplicates and optimize storage
			log.Printf("[Ledger %d] Compacting databases...", ledgerSeq)
			compactAllDBs(db1, db2, db3)

			totalFlushTime += time.Since(flushStart)

			// Show database sizes
			showDBSizes(db1Path, db2Path, db3Path)

			// Clear batch maps
			ledgerSeqToLcm = make(map[uint32][]byte)
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

			log.Printf("Progress: %d/%d ledgers (%d%%) | %.2f ledgers/sec | %s transactions | ETA: %s",
				processedCount, totalLedgers, currentPercent,
				ledgersPerSec, formatNumber(totalStats.TxCount), formatDuration(eta))

			lastReportedPercent = currentPercent
		}
	}

	// Write any remaining batch data
	if len(ledgerSeqToLcm) > 0 {
		log.Printf("\nWriting final batch to disk (%d ledgers, %d transactions)...",
			len(ledgerSeqToLcm), len(txHashToTxData))

		flushStart := time.Now()
		if err := writeBatchToDB(db1, db2, db3, ledgerSeqToLcm, txHashToTxData, txHashToLedgerSeq); err != nil {
			log.Printf("Error writing final batch: %v", err)
		}

		log.Printf("Flushing databases to disk...")
		flushAllDBs(db1, db2, db3)

		totalFlushTime += time.Since(flushStart)
	}

	elapsed := time.Since(startTime)

	// Trigger final compaction to ensure everything is optimized
	log.Printf("\nPerforming final compaction...")
	finalCompactionStart := time.Now()
	compactAllDBs(db1, db2, db3)
	totalFlushTime += time.Since(finalCompactionStart)
	log.Printf("Final compaction complete in %s", formatDuration(time.Since(finalCompactionStart)))

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
	if totalStats.UncompressedLCM > 0 {
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

	log.Printf("")
	showDBSizes(db1Path, db2Path, db3Path)
	log.Printf("========================================\n")
}

// processLedger processes a single ledger and updates the batch maps
func processLedger(
	encoder *zstd.Encoder,
	lcm xdr.LedgerCloseMeta,
	ledgerSeqToLcm map[uint32][]byte,
	txHashToTxData map[string][]byte,
	txHashToLedgerSeq map[string]uint32,
) (*CompressionStats, error) {
	stats := &CompressionStats{}

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, lcm)
	if err != nil {
		return stats, errors.Wrap(err, "failed to create transaction reader")
	}
	defer txReader.Close()

	ledgerSeq := lcm.LedgerSequence()
	closedAt := lcm.ClosedAt()

	// Process and compress LedgerCloseMeta
	lcmBytes, err := lcm.MarshalBinary()
	if err != nil {
		return stats, errors.Wrapf(err, "failed to marshal lcm for ledger: %d", ledgerSeq)
	}

	stats.UncompressedLCM = int64(len(lcmBytes))

	compressedLcm := compressData(encoder, lcmBytes)
	stats.CompressedLCM = int64(len(compressedLcm))

	ledgerSeqToLcm[ledgerSeq] = compressedLcm

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

		stats.UncompressedTx += int64(len(txDataBytes))

		// Compress TxData
		compressedTxData := compressData(encoder, txDataBytes)
		stats.CompressedTx += int64(len(compressedTxData))

		// Use hex string as map key (for convenience), but we'll convert to binary when writing to DB
		txHashHex := tx.Hash.HexString()
		txHashToTxData[txHashHex] = compressedTxData
		txHashToLedgerSeq[txHashHex] = ledgerSeq

		stats.TxCount++
	}

	return stats, nil
}

// compressData compresses data using zstd (reuses encoder for efficiency)
func compressData(encoder *zstd.Encoder, data []byte) []byte {
	return encoder.EncodeAll(data, make([]byte, 0, len(data)))
}

// writeBatchToDB writes the batch data to all three RocksDB instances
func writeBatchToDB(
	db1, db2, db3 *grocksdb.DB,
	ledgerSeqToLcm map[uint32][]byte,
	txHashToTxData map[string][]byte,
	txHashToLedgerSeq map[string]uint32,
) error {
	wo := grocksdb.NewDefaultWriteOptions()
	wo.DisableWAL(true) // Disable WAL for bulk loading performance
	defer wo.Destroy()

	// Write to DB1 (ledgerSeq -> compressed LCM)
	for ledgerSeq, compressedLcm := range ledgerSeqToLcm {
		key := uint32ToBytes(ledgerSeq)
		if err := db1.Put(wo, key, compressedLcm); err != nil {
			return errors.Wrap(err, "failed to write to DB1")
		}
	}

	// Write to DB2 (txHash -> compressed TxData)
	// Convert hex string keys to binary
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
	// Convert hex string keys to binary
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

// flushAllDBs flushes all three databases to disk
func flushAllDBs(db1, db2, db3 *grocksdb.DB) {
	fo := grocksdb.NewDefaultFlushOptions()
	fo.SetWait(true)
	defer fo.Destroy()

	if err := db1.Flush(fo); err != nil {
		log.Printf("Warning: Failed to flush DB1: %v", err)
	}
	if err := db2.Flush(fo); err != nil {
		log.Printf("Warning: Failed to flush DB2: %v", err)
	}
	if err := db3.Flush(fo); err != nil {
		log.Printf("Warning: Failed to flush DB3: %v", err)
	}
}

// compactAllDBs performs full compaction on all three databases
// This removes duplicate keys and optimizes storage
func compactAllDBs(db1, db2, db3 *grocksdb.DB) {
	db1.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
	db2.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
	db3.CompactRange(grocksdb.Range{Start: nil, Limit: nil})
}

// showDBSizes displays the current sizes of all three databases
func showDBSizes(db1Path, db2Path, db3Path string) {
	size1, _ := getDirSize(db1Path)
	size2, _ := getDirSize(db2Path)
	size3, _ := getDirSize(db3Path)

	log.Printf("Database sizes:")
	log.Printf("  DB1 (LCM):              %s", formatBytes(size1))
	log.Printf("  DB2 (TxData):           %s", formatBytes(size2))
	log.Printf("  DB3 (Hash->Seq):        %s", formatBytes(size3))
	log.Printf("  Total:                  %s", formatBytes(size1+size2+size3))
}

// openRocksDB opens or creates a RocksDB database with optimized settings
func openRocksDB(path string, createNew bool) (*grocksdb.DB, *grocksdb.Options, error) {
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
	opts.SetCreateIfMissing(true) // Create if missing
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetErrorIfExists(false) // Don't error if exists - we want to append

	// Optimize for bulk writes and large datasets
	// Use NoCompression since we already compress data with Zstd at application level
	opts.SetCompression(grocksdb.NoCompression)
	opts.SetWriteBufferSize(128 << 20) // 128 MB
	opts.SetMaxWriteBufferNumber(3)
	opts.SetTargetFileSizeBase(128 << 20) // 128 MB
	opts.SetMaxBackgroundJobs(6)
	opts.SetMaxOpenFiles(1000)

	// Reduce RocksDB logging to prevent LOG file bloat
	opts.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)
	opts.SetMaxLogFileSize(10 << 20) // 10 MB
	opts.SetKeepLogFileNum(2)

	// Keep auto compactions enabled for better long-term performance
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
	// Remove "0x" prefix if present
	if len(hexStr) >= 2 && hexStr[0:2] == "0x" {
		hexStr = hexStr[2:]
	}

	// Decode hex string to bytes
	bytes := make([]byte, len(hexStr)/2)
	for i := 0; i < len(bytes); i++ {
		var b byte
		_, err := fmt.Sscanf(hexStr[i*2:i*2+2], "%02x", &b)
		if err != nil {
			return nil, err
		}
		bytes[i] = b
	}
	return bytes, nil
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
