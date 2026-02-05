package main

import (
	"encoding/binary"
	"flag"
	"fmt"
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

const BatchSize = 1000
const ProgressInterval = 60 * time.Second

type WorkerResult struct {
	WorkerID      int
	StartLedger   uint32
	EndLedger     uint32
	LedgersRead   int64
	TxHashesFound int64
	Duration      time.Duration
	Err           error
}

func runWorker(
	workerID int,
	startLedger, endLedger uint32,
	lfsPath string,
	txStore interfaces.TxHashStore,
	progressChan chan<- WorkerResult,
	globalProgressChan chan<- struct{},
) {
	var ledgersRead, txHashesFound int64
	startTime := time.Now()

	iterator, err := lfs.NewLFSRawLedgerIterator(lfsPath, startLedger, endLedger)
	if err != nil {
		progressChan <- WorkerResult{
			WorkerID: workerID,
			Err:      fmt.Errorf("worker %d: failed to create iterator: %w", workerID, err),
		}
		return
	}
	defer iterator.Close()

	entriesByCF := make(map[string][]types.Entry)
	for _, cfName := range cf.Names {
		entriesByCF[cfName] = make([]types.Entry, 0)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		progressChan <- WorkerResult{
			WorkerID: workerID,
			Err:      fmt.Errorf("worker %d: failed to create zstd decoder: %w", workerID, err),
		}
		return
	}
	defer decoder.Close()

	batchLedgerCount := int64(0)

	for {
		rawData, hasMore, err := iterator.Next()
		if err != nil {
			progressChan <- WorkerResult{
				WorkerID: workerID,
				Err:      fmt.Errorf("worker %d: iterator error: %w", workerID, err),
			}
			return
		}

		if !hasMore {
			break
		}

		decompressed, err := decoder.DecodeAll(rawData.CompressedData, nil)
		if err != nil {
			progressChan <- WorkerResult{
				WorkerID: workerID,
				Err:      fmt.Errorf("worker %d: decompress error: %w", workerID, err),
			}
			return
		}

		var xdrLedger xdr.LedgerCloseMeta
		if err := xdrLedger.UnmarshalBinary(decompressed); err != nil {
			progressChan <- WorkerResult{
				WorkerID: workerID,
				Err:      fmt.Errorf("worker %d: unmarshal error: %w", workerID, err),
			}
			return
		}

		txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
			network.PublicNetworkPassphrase, xdrLedger)
		if err != nil {
			progressChan <- WorkerResult{
				WorkerID: workerID,
				Err:      fmt.Errorf("worker %d: failed to create tx reader: %w", workerID, err),
			}
			return
		}

		ledgerSeqBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(ledgerSeqBytes, rawData.LedgerSeq)

		for {
			tx, err := txReader.Read()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				progressChan <- WorkerResult{
					WorkerID: workerID,
					Err:      fmt.Errorf("worker %d: failed to read tx: %w", workerID, err),
				}
				txReader.Close()
				return
			}

			txHash := tx.Result.TransactionHash[:]
			cfName := cf.GetName(txHash)

			entry := types.Entry{
				Key:   txHash,
				Value: ledgerSeqBytes,
			}

			entriesByCF[cfName] = append(entriesByCF[cfName], entry)
			txHashesFound++
		}
		txReader.Close()

		ledgersRead++
		batchLedgerCount++

		select {
		case globalProgressChan <- struct{}{}:
		default:
		}

		if batchLedgerCount >= BatchSize {
			if err := txStore.WriteBatch(entriesByCF); err != nil {
				progressChan <- WorkerResult{
					WorkerID: workerID,
					Err:      fmt.Errorf("worker %d: write batch error: %w", workerID, err),
				}
				return
			}

			for cfName := range entriesByCF {
				entriesByCF[cfName] = make([]types.Entry, 0)
			}
			batchLedgerCount = 0
		}
	}

	if err := txStore.WriteBatch(entriesByCF); err != nil {
		progressChan <- WorkerResult{
			WorkerID: workerID,
			Err:      fmt.Errorf("worker %d: write final batch error: %w", workerID, err),
		}
		return
	}

	progressChan <- WorkerResult{
		WorkerID:      workerID,
		StartLedger:   startLedger,
		EndLedger:     endLedger,
		LedgersRead:   ledgersRead,
		TxHashesFound: txHashesFound,
		Duration:      time.Since(startTime),
	}
}

func main() {
	lfsStore := flag.String("lfs-store", "", "Path to LFS ledger store (required)")
	startLedger := flag.Uint64("start-ledger", 0, "First ledger to ingest (required)")
	endLedger := flag.Uint64("end-ledger", 0, "Last ledger to ingest (required)")
	outputDir := flag.String("output-dir", "", "Base output directory (required)")
	logFile := flag.String("log-file", "", "Path to log file (required)")
	errorFile := flag.String("error-file", "", "Path to error file (required)")
	numWorkers := flag.Int("workers", 16, "Number of parallel workers (default 16)")

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
	logger.Info("  Workers:         %d", *numWorkers)
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
	totalLedgersCompleted := atomic.Int64{}
	totalTxHashesFound := atomic.Int64{}

	logger.Separator()
	logger.Info("                    STARTING INGESTION")
	logger.Separator()
	logger.Info("")

	ledgersPerWorker := totalLedgers / uint64(*numWorkers)
	remainder := totalLedgers % uint64(*numWorkers)

	progressChan := make(chan WorkerResult, *numWorkers)
	globalProgressChan := make(chan struct{}, 100)

	ticker := time.NewTicker(ProgressInterval)
	defer ticker.Stop()

	var wg sync.WaitGroup
	currentStart := *startLedger

	for i := 0; i < *numWorkers; i++ {
		count := ledgersPerWorker
		if i < int(remainder) {
			count++
		}

		workerStart := currentStart
		workerEnd := workerStart + count - 1

		wg.Add(1)
		go runWorker(
			i,
			uint32(workerStart), uint32(workerEnd),
			*lfsStore,
			txStore,
			progressChan,
			globalProgressChan,
		)

		currentStart = workerEnd + 1
	}

	go func() {
		for range globalProgressChan {
			totalLedgersCompleted.Add(1)
		}
	}()

	go func() {
		for range ticker.C {
			completed := totalLedgersCompleted.Load()
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

	wg.Wait()
	ticker.Stop()
	close(progressChan)

	var hasError bool
	totalDuration := time.Duration(0)
	for result := range progressChan {
		if result.Err != nil {
			logger.Error("Worker %d error: %v", result.WorkerID, result.Err)
			hasError = true
		} else {
			logger.Info("Worker %d completed: %d ledgers, %d txHashes (%.2f ledgers/s)",
				result.WorkerID,
				result.LedgersRead,
				result.TxHashesFound,
				float64(result.LedgersRead)/result.Duration.Seconds(),
			)
			totalLedgersCompleted.Store(result.LedgersRead)
			totalTxHashesFound.Add(result.TxHashesFound)
			if result.Duration > totalDuration {
				totalDuration = result.Duration
			}
		}
	}

	if hasError {
		logger.Error("Ingestion failed")
		os.Exit(1)
	}

	totalElapsed := time.Since(startTime)
	totalTxHashes := totalTxHashesFound.Load()
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
