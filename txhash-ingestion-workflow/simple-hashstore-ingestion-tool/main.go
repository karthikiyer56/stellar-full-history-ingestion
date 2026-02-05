package main

import (
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
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Architecture constants
const (
	BatchSize        = 5000
	NumWorkers       = 16
	NumReaders       = 4
	WorkChanBuffer   = 200
	EntryChanBuffer  = 100
	ProgressInterval = 60 * time.Second
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

// processBatch processes a single batch of ledgers using the Reader→Worker→Collector pipeline
func processBatch(
	lfsPath string,
	batchStart, batchEnd uint32,
	numWorkers, numReaders int,
	txStore interfaces.TxHashStore,
	logger interfaces.Logger,
) (int64, error) {
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
		return 0, err
	default:
	}

	// Write batch to RocksDB
	if err := txStore.WriteBatch(entriesByCF); err != nil {
		return 0, fmt.Errorf("failed to write batch: %w", err)
	}

	return totalTxCount, nil
}

func main() {
	lfsStore := flag.String("lfs-store", "", "Path to LFS ledger store (required)")
	startLedger := flag.Uint64("start-ledger", 0, "First ledger to ingest (required)")
	endLedger := flag.Uint64("end-ledger", 0, "Last ledger to ingest (required)")
	outputDir := flag.String("output-dir", "", "Base output directory (required)")
	logFile := flag.String("log-file", "", "Path to log file (required)")
	errorFile := flag.String("error-file", "", "Path to error file (required)")
	numWorkers := flag.Int("workers", NumWorkers, "Number of workers (default 16)")
	numReaders := flag.Int("readers", NumReaders, "Number of LFS readers (default 4)")

	flag.Parse()

	if *lfsStore == "" || *startLedger == 0 || *endLedger == 0 ||
		*outputDir == "" || *logFile == "" || *errorFile == "" {
		fmt.Fprintln(os.Stderr, "Error: --lfs-store, --start-ledger, --end-ledger, --output-dir, --log-file, and --error-file are required")
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
	logger.Info("  LFS Store:       %s", *lfsStore)
	logger.Info("  Start Ledger:    %d", *startLedger)
	logger.Info("  End Ledger:      %d", *endLedger)
	logger.Info("  Output Dir:      %s", *outputDir)
	logger.Info("  Workers:         %d (decompress/unmarshal/extract)", *numWorkers)
	logger.Info("  Readers:         %d (LFS I/O)", *numReaders)
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

	startTime := time.Now()
	totalLedgers := *endLedger - *startLedger + 1
	ledgersCompleted := atomic.Int64{}
	txHashesFound := atomic.Int64{}

	logger.Separator()
	logger.Info("                    STARTING INGESTION (PARALLEL)")
	logger.Separator()
	logger.Info("")

	ticker := time.NewTicker(ProgressInterval)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			completed := ledgersCompleted.Load()
			if completed > 0 {
				elapsed := time.Since(startTime)
				rate := float64(completed) / elapsed.Seconds()
				remainingLedgers := int64(totalLedgers) - completed
				etaSeconds := time.Duration(int64(float64(remainingLedgers)/rate)) * time.Second

				logger.Info("[PROGRESS] Ledgers: %d/%d (%.1f%%) | Rate: %.0f ledgers/s | ETA: %s",
					completed,
					totalLedgers,
					float64(completed)*100.0/float64(totalLedgers),
					rate,
					helpers.FormatDuration(etaSeconds),
				)
			}
		}
	}()

	// Process ledgers in batches
	currentLedger := uint32(*startLedger)
	endLedgerSeq := uint32(*endLedger)
	var hasError bool

	for currentLedger <= endLedgerSeq {
		batchStart := currentLedger
		batchEnd := currentLedger + uint32(BatchSize) - 1
		if batchEnd > endLedgerSeq {
			batchEnd = endLedgerSeq
		}

		txCount, err := processBatch(*lfsStore, batchStart, batchEnd, *numWorkers, *numReaders, txStore, logger)
		if err != nil {
			logger.Error("Failed to process batch %d-%d: %v", batchStart, batchEnd, err)
			hasError = true
			break
		}

		ledgersCompleted.Add(int64(batchEnd - batchStart + 1))
		txHashesFound.Add(txCount)

		currentLedger = batchEnd + 1
	}

	ticker.Stop()

	if hasError {
		logger.Error("Ingestion failed")
		os.Exit(1)
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
	logger.Info("  Throughput:        %s/s", helpers.FormatNumber(int64(txHashRate)))
	logger.Info("  Memory (RSS):      %.2f GB", memMonitor.CurrentRSSGB())
	logger.Info("")
	logger.Info("Output: %s", *outputDir)
	logger.Info("")

	logger.Sync()
}
