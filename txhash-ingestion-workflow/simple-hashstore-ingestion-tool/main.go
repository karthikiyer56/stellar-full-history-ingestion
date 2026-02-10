package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers/lfs"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/cf"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/logging"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/memory"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/store"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/types"
	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Architecture constants
const (
	BatchSize            = 5000
	NumWorkers           = 16
	NumReaders           = 4
	WorkChanBuffer       = 200
	EntryChanBuffer      = 100
	ProgressInterval     = 60 * time.Second
	DefaultGCSBufferSize = 10000
	DefaultGCSNumWorkers = 200
)

// LedgerWork represents compressed ledger data ready for processing
type LedgerWork struct {
	LedgerSeq      uint32
	CompressedData []byte
}

// LedgerEntries represents extracted entries from a single ledger
type LedgerEntries struct {
	LedgerSeq   uint32
	EntriesByCF map[string][]types.Entry
	TxCount     int
}

// copyBytes creates a copy of the byte slice
func copyBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// formatDurationShort formats duration with short units
func formatDurationShort(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	} else if d < time.Millisecond {
		return fmt.Sprintf("%.0fµs", float64(d.Nanoseconds())/1000.0)
	} else if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d.Nanoseconds())/1e6)
	} else if d < time.Minute {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	return helpers.FormatDuration(d)
}

// extractTxHashesFromLCM extracts transaction hashes from a LedgerCloseMeta
func extractTxHashesFromLCM(lcm xdr.LedgerCloseMeta, ledgerSeq uint32) (map[string][]types.Entry, int, error) {
	entriesByCF := make(map[string][]types.Entry)
	for _, cfName := range cf.Names {
		entriesByCF[cfName] = make([]types.Entry, 0, 32)
	}

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		network.PublicNetworkPassphrase, lcm)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create tx reader for ledger %d: %w", ledgerSeq, err)
	}
	defer txReader.Close()

	ledgerSeqBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(ledgerSeqBytes, ledgerSeq)

	txCount := 0
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read tx from ledger %d: %w", ledgerSeq, err)
		}

		txHash := tx.Result.TransactionHash[:]
		cfName := cf.GetName(txHash)

		entriesByCF[cfName] = append(entriesByCF[cfName], types.Entry{
			Key:   copyBytes(txHash),
			Value: copyBytes(ledgerSeqBytes),
		})
		txCount++
	}

	return entriesByCF, txCount, nil
}

// reader reads compressed ledger data from LFS
func reader(
	id int,
	lfsPath string,
	startSeq, endSeq uint32,
	workChan chan<- LedgerWork,
	errChan chan<- error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	iterator, err := lfs.NewLFSRawLedgerIterator(lfsPath, startSeq, endSeq)
	if err != nil {
		select {
		case errChan <- fmt.Errorf("reader %d: failed to create iterator: %w", id, err):
		default:
		}
		return
	}
	defer iterator.Close()

	for {
		data, hasMore, err := iterator.Next()
		if err != nil {
			select {
			case errChan <- fmt.Errorf("reader %d: failed to read ledger: %w", id, err):
			default:
			}
			return
		}
		if !hasMore {
			break
		}

		workChan <- LedgerWork{
			LedgerSeq:      data.LedgerSeq,
			CompressedData: data.CompressedData,
		}
	}
}

// worker processes ledger data (decompress, unmarshal, extract)
func worker(
	id int,
	workChan <-chan LedgerWork,
	entryChan chan<- LedgerEntries,
	errChan chan<- error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		select {
		case errChan <- fmt.Errorf("worker %d: failed to create zstd decoder: %w", id, err):
		default:
		}
		return
	}
	defer decoder.Close()

	ledgerSeqBytes := make([]byte, 4)

	for work := range workChan {
		// 1. Decompress
		uncompressed, err := decoder.DecodeAll(work.CompressedData, nil)
		if err != nil {
			select {
			case errChan <- fmt.Errorf("worker %d: decompress failed for ledger %d: %w", id, work.LedgerSeq, err):
			default:
			}
			return
		}

		// 2. Unmarshal XDR
		var lcm xdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(uncompressed); err != nil {
			select {
			case errChan <- fmt.Errorf("worker %d: unmarshal failed for ledger %d: %w", id, work.LedgerSeq, err):
			default:
			}
			return
		}

		// 3. Extract transaction hashes
		entriesByCF := make(map[string][]types.Entry)
		for _, cfName := range cf.Names {
			entriesByCF[cfName] = make([]types.Entry, 0, 32)
		}

		txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
			network.PublicNetworkPassphrase, lcm)
		if err != nil {
			select {
			case errChan <- fmt.Errorf("worker %d: failed to create tx reader for ledger %d: %w", id, work.LedgerSeq, err):
			default:
			}
			return
		}

		binary.BigEndian.PutUint32(ledgerSeqBytes, work.LedgerSeq)

		txCount := 0
		for {
			tx, err := txReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				txReader.Close()
				select {
				case errChan <- fmt.Errorf("worker %d: failed to read tx from ledger %d: %w", id, work.LedgerSeq, err):
				default:
				}
				return
			}

			txHash := tx.Result.TransactionHash[:]
			cfName := cf.GetName(txHash)

			entriesByCF[cfName] = append(entriesByCF[cfName], types.Entry{
				Key:   copyBytes(txHash),
				Value: copyBytes(ledgerSeqBytes),
			})
			txCount++
		}
		txReader.Close()

		entryChan <- LedgerEntries{
			LedgerSeq:   work.LedgerSeq,
			EntriesByCF: entriesByCF,
			TxCount:     txCount,
		}
	}
}

// processBatchLFS processes a single batch of ledgers using the Reader→Worker→Collector pipeline
func processBatchLFS(
	lfsPath string,
	batchStart, batchEnd uint32,
	numWorkers, numReaders int,
	txStore interfaces.TxHashStore,
	logger interfaces.Logger,
) (int64, time.Duration, error) {
	workChan := make(chan LedgerWork, WorkChanBuffer)
	entryChan := make(chan LedgerEntries, EntryChanBuffer)
	errChan := make(chan error, numReaders+numWorkers)

	entriesByCF := make(map[string][]types.Entry)
	for _, cfName := range cf.Names {
		entriesByCF[cfName] = make([]types.Entry, 0)
	}

	var totalTxCount int64
	var readerWg, workerWg sync.WaitGroup

	// Start workers first (before readers, so they're ready)
	for w := 0; w < numWorkers; w++ {
		workerWg.Add(1)
		go worker(w, workChan, entryChan, errChan, &workerWg)
	}

	// Calculate ledger distribution across readers
	batchLedgers := int(batchEnd - batchStart + 1)
	ledgersPerReader := batchLedgers / numReaders
	remainder := batchLedgers % numReaders

	// Start readers
	readerStart := batchStart
	for r := 0; r < numReaders; r++ {
		count := ledgersPerReader
		if r < remainder {
			count++
		}
		readerEnd := readerStart + uint32(count) - 1
		readerWg.Add(1)
		go reader(r, lfsPath, readerStart, readerEnd, workChan, errChan, &readerWg)
		readerStart = readerEnd + 1
	}

	// Start collector
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		for entries := range entryChan {
			for cfName, cfEntries := range entries.EntriesByCF {
				entriesByCF[cfName] = append(entriesByCF[cfName], cfEntries...)
			}
			totalTxCount += int64(entries.TxCount)
		}
	}()

	// Wait sequence
	readerWg.Wait()
	close(workChan)

	workerWg.Wait()
	close(entryChan)

	<-collectorDone

	// Check for errors
	select {
	case err := <-errChan:
		return 0, 0, err
	default:
	}

	// Write batch to RocksDB and time it
	writeStart := time.Now()
	if _, err := txStore.WriteBatch(entriesByCF); err != nil {
		return 0, 0, fmt.Errorf("failed to write batch: %w", err)
	}
	writeDuration := time.Since(writeStart)

	return totalTxCount, writeDuration, nil
}

func runLFSIngestion(
	lfsPath string,
	startLedger, endLedger uint32,
	numWorkers, numReaders int,
	txStore interfaces.TxHashStore,
	logger interfaces.Logger,
	memMonitor *memory.MemoryMonitor,
) error {
	startTime := time.Now()
	totalLedgers := int64(endLedger - startLedger + 1)
	ledgersCompleted := atomic.Int64{}
	txHashesFound := atomic.Int64{}

	logger.Separator()
	logger.Info("                    STARTING INGESTION (PARALLEL)")
	logger.Separator()
	logger.Info("")

	// Process ledgers in batches
	currentLedger := startLedger
	var hasError bool

	// Track WriteBatch timing for progress logs
	var writeBatchTotalDuration time.Duration
	var recentWriteBatchTime time.Duration
	var recentWriteBatchCount int64

	onePercent := totalLedgers / 100
	if onePercent == 0 {
		onePercent = 1
	}
	lastProgressPercent := int64(0)

	for currentLedger <= endLedger {
		batchStart := currentLedger
		batchEnd := currentLedger + uint32(BatchSize) - 1
		if batchEnd > endLedger {
			batchEnd = endLedger
		}

		txCount, writeDuration, err := processBatchLFS(lfsPath, batchStart, batchEnd, numWorkers, numReaders, txStore, logger)
		if err != nil {
			logger.Error("Failed to process batch %d-%d: %v", batchStart, batchEnd, err)
			hasError = true
			break
		}

		ledgersCompleted.Add(int64(batchEnd - batchStart + 1))
		txHashesFound.Add(txCount)
		writeBatchTotalDuration += writeDuration
		recentWriteBatchTime += writeDuration
		recentWriteBatchCount++

		currentCompleted := ledgersCompleted.Load()
		if currentCompleted > 0 {
			currentPercent := currentCompleted / onePercent
			if currentPercent > lastProgressPercent {
				elapsed := time.Since(startTime)
				rate := float64(currentCompleted) / elapsed.Seconds()
				remainingLedgers := totalLedgers - currentCompleted
				etaSeconds := time.Duration(int64(float64(remainingLedgers)/rate)) * time.Second

				avgWriteBatch := time.Duration(int64(recentWriteBatchTime) / recentWriteBatchCount)

				logger.Info("[PROGRESS] Ledgers: %s/%d (%d%%) | Rate: %.0f/s | WriteBatch avg: %s | ETA: %s",
					helpers.FormatNumber(currentCompleted),
					totalLedgers,
					currentPercent,
					rate,
					formatDurationShort(avgWriteBatch),
					helpers.FormatDuration(etaSeconds),
				)

				// Reset recent tracking for next 1% boundary
				recentWriteBatchTime = 0
				recentWriteBatchCount = 0
				lastProgressPercent = currentPercent
			}
		}

		currentLedger = batchEnd + 1
	}

	if hasError {
		logger.Error("Ingestion failed")
		return fmt.Errorf("ingestion failed")
	}

	totalElapsed := time.Since(startTime)
	totalTxHashes := txHashesFound.Load()
	txHashRate := float64(totalTxHashes) / totalElapsed.Seconds()

	logger.Separator()
	logger.Info("                    INGESTION COMPLETE")
	logger.Separator()
	logger.Info("")
	logger.Info("Statistics:")
	logger.Info("  Total Ledgers:     %d", totalLedgers)
	logger.Info("  Total TxHashes:    %s", helpers.FormatNumber(totalTxHashes))
	logger.Info("  Duration:          %s", helpers.FormatDuration(totalElapsed))
	logger.Info("  WriteBatch Total:  %s", helpers.FormatDuration(writeBatchTotalDuration))
	logger.Info("  Throughput:        %s/s", helpers.FormatNumber(int64(txHashRate)))
	logger.Info("  Memory (RSS):      %.2f GB", memMonitor.CurrentRSSGB())
	logger.Info("")

	return nil
}

func runGCSIngestion(
	ctx context.Context,
	gcsBucketPath string,
	gcsBufferSize, gcsNumWorkers int,
	startLedger, endLedger uint32,
	txStore interfaces.TxHashStore,
	logger interfaces.Logger,
	memMonitor *memory.MemoryMonitor,
) error {
	// Set up GCS backend
	datastoreConfig := datastore.DataStoreConfig{
		Type:   "GCS",
		Params: map[string]string{"destination_bucket_path": gcsBucketPath},
	}
	dataStoreSchema := datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 64000,
	}

	dataStore, err := datastore.NewDataStore(ctx, datastoreConfig)
	if err != nil {
		return fmt.Errorf("failed to create GCS datastore: %w", err)
	}

	backendConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: uint32(gcsBufferSize),
		NumWorkers: uint32(gcsNumWorkers),
		RetryLimit: 3,
		RetryWait:  5 * time.Second,
	}

	backend, err := ledgerbackend.NewBufferedStorageBackend(backendConfig, dataStore, dataStoreSchema)
	if err != nil {
		return fmt.Errorf("failed to create GCS backend: %w", err)
	}
	defer backend.Close()

	startTime := time.Now()
	totalLedgers := int64(endLedger - startLedger + 1)
	ledgersCompleted := atomic.Int64{}
	txHashesFound := atomic.Int64{}

	logger.Separator()
	logger.Info("                    STARTING INGESTION (GCS)")
	logger.Separator()
	logger.Info("")

	// Track timing metrics
	var getLedgerTotalDuration time.Duration
	var writeBatchTotalDuration time.Duration
	var recentGetLedgerTime time.Duration
	var recentGetLedgerCount int64
	var recentWriteBatchTime time.Duration
	var recentWriteBatchCount int64

	entriesByCF := make(map[string][]types.Entry)
	for _, cfName := range cf.Names {
		entriesByCF[cfName] = make([]types.Entry, 0)
	}

	var totalTxCount int64
	onePercent := totalLedgers / 100
	if onePercent == 0 {
		onePercent = 1
	}
	lastProgressPercent := int64(0)

	// Sequential loop - SDK handles buffering internally
	for ledgerSeq := startLedger; ledgerSeq <= endLedger; ledgerSeq++ {
		// Get ledger and time it
		getLedgerStart := time.Now()
		ledger, err := backend.GetLedger(ctx, ledgerSeq)
		getLedgerDuration := time.Since(getLedgerStart)

		if err != nil {
			return fmt.Errorf("failed to get ledger %d from GCS: %w", ledgerSeq, err)
		}

		getLedgerTotalDuration += getLedgerDuration
		recentGetLedgerTime += getLedgerDuration
		recentGetLedgerCount++

		// Extract transaction hashes
		entries, txCount, err := extractTxHashesFromLCM(ledger, ledgerSeq)
		if err != nil {
			return fmt.Errorf("failed to extract hashes from ledger %d: %w", ledgerSeq, err)
		}

		for cfName, cfEntries := range entries {
			entriesByCF[cfName] = append(entriesByCF[cfName], cfEntries...)
		}
		totalTxCount += int64(txCount)

		// Write batch when size reached
		if int64(ledgerSeq-startLedger+1)%int64(BatchSize) == 0 || ledgerSeq == endLedger {
			writeBatchStart := time.Now()
			if _, err := txStore.WriteBatch(entriesByCF); err != nil {
				return fmt.Errorf("failed to write batch: %w", err)
			}
			writeBatchDuration := time.Since(writeBatchStart)
			writeBatchTotalDuration += writeBatchDuration
			recentWriteBatchTime += writeBatchDuration
			recentWriteBatchCount++

			// Reset batch
			entriesByCF = make(map[string][]types.Entry)
			for _, cfName := range cf.Names {
				entriesByCF[cfName] = make([]types.Entry, 0)
			}
		}

		ledgersCompleted.Add(1)
		txHashesFound.Add(int64(txCount))

		// Progress logging every 1%
		currentCompleted := ledgersCompleted.Load()
		if currentCompleted > 0 {
			currentPercent := currentCompleted / onePercent
			if currentPercent > lastProgressPercent {
				elapsed := time.Since(startTime)
				rate := float64(currentCompleted) / elapsed.Seconds()
				remainingLedgers := totalLedgers - currentCompleted
				etaSeconds := time.Duration(int64(float64(remainingLedgers)/rate)) * time.Second

				avgGetLedger := time.Duration(int64(recentGetLedgerTime) / recentGetLedgerCount)
				avgWriteBatch := time.Duration(int64(recentWriteBatchTime) / recentWriteBatchCount)

				logger.Info("[PROGRESS] Ledgers: %s/%d (%d%%) | Rate: %.0f/s | GetLedger avg: %s | WriteBatch avg: %s | ETA: %s",
					helpers.FormatNumber(currentCompleted),
					totalLedgers,
					currentPercent,
					rate,
					formatDurationShort(avgGetLedger),
					formatDurationShort(avgWriteBatch),
					helpers.FormatDuration(etaSeconds),
				)

				// Reset recent tracking for next 1% boundary
				recentGetLedgerTime = 0
				recentGetLedgerCount = 0
				recentWriteBatchTime = 0
				recentWriteBatchCount = 0
				lastProgressPercent = currentPercent
			}
		}
	}

	totalElapsed := time.Since(startTime)
	totalTxHashes := txHashesFound.Load()
	txHashRate := float64(totalTxHashes) / totalElapsed.Seconds()

	logger.Separator()
	logger.Info("                    INGESTION COMPLETE")
	logger.Separator()
	logger.Info("")
	logger.Info("Statistics:")
	logger.Info("  Total Ledgers:     %d", totalLedgers)
	logger.Info("  Total TxHashes:    %s", helpers.FormatNumber(totalTxHashes))
	logger.Info("  Duration:          %s", helpers.FormatDuration(totalElapsed))
	logger.Info("  GetLedger Total:   %s", helpers.FormatDuration(getLedgerTotalDuration))
	logger.Info("  WriteBatch Total:  %s", helpers.FormatDuration(writeBatchTotalDuration))
	logger.Info("  Throughput:        %s/s", helpers.FormatNumber(int64(txHashRate)))
	logger.Info("  Memory (RSS):      %.2f GB", memMonitor.CurrentRSSGB())
	logger.Info("")

	return nil
}

func main() {
	lfsStore := flag.String("lfs-store", "", "Path to LFS ledger store")
	gcsBucketPath := flag.String("gcs-bucket-path", "", "GCS bucket path (e.g., 'sdf-ledger-close-meta/v1/ledgers/pubnet')")
	startLedger := flag.Uint64("start-ledger", 0, "First ledger to ingest (required)")
	endLedger := flag.Uint64("end-ledger", 0, "Last ledger to ingest (required)")
	outputDir := flag.String("output-dir", "", "Base output directory (required)")
	logFile := flag.String("log-file", "", "Path to log file (required)")
	errorFile := flag.String("error-file", "", "Path to error file (required)")
	numWorkers := flag.Int("workers", NumWorkers, "Number of workers (default 16)")
	numReaders := flag.Int("readers", NumReaders, "Number of LFS readers (default 4)")
	gcsBufferSize := flag.Int("gcs-buffer-size", DefaultGCSBufferSize, "GCS BufferedStorageBackend buffer size")
	gcsNumWorkers := flag.Int("gcs-workers", DefaultGCSNumWorkers, "GCS BufferedStorageBackend num workers")

	flag.Parse()

	// Mode detection: exactly one of LFS or GCS must be specified
	useGCS := *gcsBucketPath != ""
	useLFS := *lfsStore != ""

	if !useGCS && !useLFS {
		fmt.Fprintln(os.Stderr, "Error: must specify either --lfs-store or --gcs-bucket-path")
		flag.Usage()
		os.Exit(1)
	}
	if useGCS && useLFS {
		fmt.Fprintln(os.Stderr, "Error: cannot specify both --lfs-store and --gcs-bucket-path (choose one)")
		flag.Usage()
		os.Exit(1)
	}

	if *startLedger == 0 || *endLedger == 0 ||
		*outputDir == "" || *logFile == "" || *errorFile == "" {
		fmt.Fprintln(os.Stderr, "Error: --start-ledger, --end-ledger, --output-dir, --log-file, and --error-file are required")
		flag.Usage()
		os.Exit(1)
	}

	logger, err := logging.NewDualLogger(*logFile, *errorFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	rockdbPath := filepath.Join(*outputDir, "rocksdb")
	if helpers.FileExists(rockdbPath) {
		fmt.Fprintf(os.Stderr, "Error: RocksDB store already exists at %s\n", rockdbPath)
		os.Exit(1)
	}

	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	logger.Separator()
	logger.Info("                    SIMPLE HASHSTORE INGESTION TOOL")
	logger.Separator()
	logger.Info("")
	logger.Info("Configuration:")
	if useGCS {
		logger.Info("  Mode:            GCS (Google Cloud Storage)")
		logger.Info("  Bucket Path:     %s", *gcsBucketPath)
		logger.Info("  Buffer Size:     %d", *gcsBufferSize)
		logger.Info("  GCS Workers:     %d", *gcsNumWorkers)
	} else {
		logger.Info("  Mode:            LFS (Local File System)")
		logger.Info("  LFS Store:       %s", *lfsStore)
		logger.Info("  Workers:         %d (decompress/unmarshal/extract)", *numWorkers)
		logger.Info("  Readers:         %d (LFS I/O)", *numReaders)
	}
	logger.Info("  Start Ledger:    %d", *startLedger)
	logger.Info("  End Ledger:      %d", *endLedger)
	logger.Info("  Output Dir:      %s", *outputDir)
	logger.Info("  Batch Size:      %d ledgers", BatchSize)
	logger.Info("")

	memMonitor := memory.NewMemoryMonitor(logger, memory.DefaultRAMWarningThresholdGB)
	defer memMonitor.Stop()

	settings := types.DefaultRocksDBSettings()
	settings.ReadOnly = false

	txStore, err := store.OpenRocksDBTxHashStore(rockdbPath, &settings, logger)
	if err != nil {
		logger.Error("Failed to open RocksDB: %v", err)
		os.Exit(1)
	}
	defer txStore.Close()

	logger.Info("RocksDB store created with 16 column families")
	logger.Info("")

	// Run appropriate ingestion mode
	var ingestionErr error
	if useGCS {
		ctx := context.Background()
		ingestionErr = runGCSIngestion(ctx, *gcsBucketPath, *gcsBufferSize, *gcsNumWorkers,
			uint32(*startLedger), uint32(*endLedger), txStore, logger, memMonitor)
	} else {
		ingestionErr = runLFSIngestion(*lfsStore, uint32(*startLedger), uint32(*endLedger),
			*numWorkers, *numReaders, txStore, logger, memMonitor)
	}

	if ingestionErr != nil {
		logger.Error("Ingestion failed: %v", ingestionErr)
		os.Exit(1)
	}

	logger.Sync()
}
