// Package main provides the LFS input to RocksDB output processor.
//
// This tool reads ledgers from an LFS (Local Filesystem) store, extracts
// transaction hashes, and writes tx_hash -> ledger_seq mappings to a RocksDB
// store with 16 column families (partitioned by first hex character).
//
// Features:
//   - Multi-worker parallelism for LFS reading
//   - Single writer goroutine for RocksDB writes
//   - WAL enabled for crash recovery
//   - No automatic compaction (use compact-store utility when ready)
package main

import (
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	txhashrework "github.com/karthikiyer56/stellar-full-history-ingestion/rocksdb/tx-hash-ledger-sequence-rework"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers/lfs"
	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// Types
// =============================================================================

// WriteBatch represents a batch of entries to write to the output store.
type WriteBatch struct {
	EntriesByCF map[string][]txhashrework.Entry
	EntryCount  int
}

// WorkerRange represents a ledger range assignment for a worker.
type WorkerRange struct {
	WorkerID    int
	StartLedger uint32
	EndLedger   uint32
}

// =============================================================================
// LFS Processor
// =============================================================================

// LfsProcessor handles reading from LFS and writing tx_hash -> ledger_seq mappings.
type LfsProcessor struct {
	config      *txhashrework.Config
	outputStore *txhashrework.OutputStore
	logger      *txhashrework.Logger
	stats       *txhashrework.Stats

	// Atomic counters for progress tracking
	ledgersProcessed  int64
	transactionsFound int64

	// Channels
	batchChan chan *WriteBatch
	errorChan chan error
}

// NewLfsProcessor creates a new LFS processor.
func NewLfsProcessor(config *txhashrework.Config, outputStore *txhashrework.OutputStore, logger *txhashrework.Logger) *LfsProcessor {
	return &LfsProcessor{
		config:      config,
		outputStore: outputStore,
		logger:      logger,
		stats:       txhashrework.NewStats(),
		batchChan:   make(chan *WriteBatch, config.LFS.Workers*2),
		errorChan:   make(chan error, config.LFS.Workers),
	}
}

// Process runs the LFS processing with multiple workers.
func (p *LfsProcessor) Process() (*txhashrework.Stats, error) {
	startTime := time.Now()

	totalLedgers := int64(p.config.LFS.EndLedger - p.config.LFS.StartLedger + 1)
	p.logger.Info("Starting LFS processing with %d workers", p.config.LFS.Workers)
	p.logger.Info("Ledger range: %d - %d (%s ledgers)",
		p.config.LFS.StartLedger, p.config.LFS.EndLedger, helpers.FormatNumber(totalLedgers))

	// Split work among workers
	ranges := p.splitWork()
	for _, r := range ranges {
		p.logger.Info("  Worker %d: ledgers %d - %d (%s ledgers)",
			r.WorkerID, r.StartLedger, r.EndLedger,
			helpers.FormatNumber(int64(r.EndLedger-r.StartLedger+1)))
	}

	// Start progress logger
	progressLogger := txhashrework.NewProgressLogger(p.logger, totalLedgers, 60*time.Second)

	// Start writer goroutine
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go p.writerLoop(&writerWg)

	// Start workers
	var workerWg sync.WaitGroup
	for _, r := range ranges {
		workerWg.Add(1)
		go p.workerLoop(r, &workerWg)
	}

	// Start progress monitoring
	progressDone := make(chan struct{})
	go p.progressLoop(progressLogger, totalLedgers, progressDone)

	// Wait for workers to complete
	workerWg.Wait()

	// Close batch channel to signal writer to finish
	close(p.batchChan)

	// Wait for writer to finish
	writerWg.Wait()

	// Stop progress monitoring
	close(progressDone)

	// Check for errors
	select {
	case err := <-p.errorChan:
		return nil, err
	default:
		// No errors
	}

	// Final stats
	p.stats.LedgersProcessed = atomic.LoadInt64(&p.ledgersProcessed)
	p.stats.TransactionsFound = atomic.LoadInt64(&p.transactionsFound)
	p.stats.ReadTime = time.Since(startTime)

	// Log final progress
	progressLogger.LogProgress(p.stats.LedgersProcessed, p.stats.TransactionsFound)

	return p.stats, nil
}

// splitWork divides the ledger range among workers.
func (p *LfsProcessor) splitWork() []WorkerRange {
	numWorkers := p.config.LFS.Workers
	totalLedgers := p.config.LFS.EndLedger - p.config.LFS.StartLedger + 1
	ledgersPerWorker := totalLedgers / uint32(numWorkers)
	remainder := totalLedgers % uint32(numWorkers)

	ranges := make([]WorkerRange, numWorkers)
	currentStart := p.config.LFS.StartLedger

	for i := 0; i < numWorkers; i++ {
		rangeSize := ledgersPerWorker
		if uint32(i) < remainder {
			rangeSize++ // Distribute remainder among first workers
		}

		ranges[i] = WorkerRange{
			WorkerID:    i,
			StartLedger: currentStart,
			EndLedger:   currentStart + rangeSize - 1,
		}

		currentStart += rangeSize
	}

	return ranges
}

// workerLoop processes a range of ledgers.
func (p *LfsProcessor) workerLoop(r WorkerRange, wg *sync.WaitGroup) {
	defer wg.Done()

	// Create iterator for this worker's range
	iterator, err := lfs.NewLedgerIterator(p.config.LFS.LfsStorePath, r.StartLedger, r.EndLedger)
	if err != nil {
		p.logger.Error("Worker %d: failed to create iterator: %v", r.WorkerID, err)
		p.errorChan <- err
		return
	}
	defer iterator.Close()

	// Batch accumulator
	batchSize := p.config.BatchSize / p.config.LFS.Workers
	if batchSize < 1000 {
		batchSize = 1000
	}

	entriesByCF := make(map[string][]txhashrework.Entry)
	for _, cfName := range txhashrework.ColumnFamilyNames {
		entriesByCF[cfName] = make([]txhashrework.Entry, 0, batchSize/16)
	}
	entriesInBatch := 0

	// Process ledgers
	for {
		lcm, ledgerSeq, _, hasMore, err := iterator.Next()
		if err != nil {
			p.logger.Error("Worker %d: failed to read ledger %d: %v", r.WorkerID, ledgerSeq, err)
			p.stats.IncrementErrors()
			continue
		}
		if !hasMore {
			break
		}

		// Extract transactions and their hashes
		txCount, err := p.extractTxHashes(lcm, ledgerSeq, entriesByCF)
		if err != nil {
			p.logger.Error("Worker %d: failed to extract transactions from ledger %d: %v",
				r.WorkerID, ledgerSeq, err)
			p.stats.IncrementErrors()
			continue
		}

		entriesInBatch += txCount
		atomic.AddInt64(&p.ledgersProcessed, 1)
		atomic.AddInt64(&p.transactionsFound, int64(txCount))

		// Send batch if full
		if entriesInBatch >= batchSize {
			p.sendBatch(entriesByCF, entriesInBatch)

			// Reset batch
			entriesByCF = make(map[string][]txhashrework.Entry)
			for _, cfName := range txhashrework.ColumnFamilyNames {
				entriesByCF[cfName] = make([]txhashrework.Entry, 0, batchSize/16)
			}
			entriesInBatch = 0
		}
	}

	// Send final batch
	if entriesInBatch > 0 {
		p.sendBatch(entriesByCF, entriesInBatch)
	}
}

// extractTxHashes extracts transaction hashes from a ledger and adds them to the batch.
func (p *LfsProcessor) extractTxHashes(lcm xdr.LedgerCloseMeta, ledgerSeq uint32, entriesByCF map[string][]txhashrework.Entry) (int, error) {
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		network.PublicNetworkPassphrase, lcm)
	if err != nil {
		return 0, err
	}
	defer txReader.Close()

	// Encode ledger sequence as 4 bytes (big-endian)
	ledgerSeqBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(ledgerSeqBytes, ledgerSeq)

	txCount := 0
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return txCount, err
		}

		// Get transaction hash (32 bytes)
		txHash := tx.Result.TransactionHash[:]

		// Determine column family
		cfName := txhashrework.GetColumnFamilyName(txHash)

		// Add to batch
		entriesByCF[cfName] = append(entriesByCF[cfName], txhashrework.Entry{
			Key:   txHash,
			Value: ledgerSeqBytes,
		})
		txCount++
	}

	return txCount, nil
}

// sendBatch sends a batch to the writer channel.
func (p *LfsProcessor) sendBatch(entriesByCF map[string][]txhashrework.Entry, entryCount int) {
	// Make a copy of the entries for the batch
	batchEntries := make(map[string][]txhashrework.Entry)
	for cfName, entries := range entriesByCF {
		if len(entries) > 0 {
			entriesCopy := make([]txhashrework.Entry, len(entries))
			copy(entriesCopy, entries)
			batchEntries[cfName] = entriesCopy
		}
	}

	batch := &WriteBatch{
		EntriesByCF: batchEntries,
		EntryCount:  entryCount,
	}

	p.batchChan <- batch
}

// writerLoop receives batches and writes them to the output store.
func (p *LfsProcessor) writerLoop(wg *sync.WaitGroup) {
	defer wg.Done()

	for batch := range p.batchChan {
		writeStart := time.Now()

		// Create RocksDB batch
		rocksBatch := grocksdb.NewWriteBatch()

		for cfName, entries := range batch.EntriesByCF {
			cfHandle := p.outputStore.GetCFHandleByName(cfName)
			for _, entry := range entries {
				rocksBatch.PutCF(cfHandle, entry.Key, entry.Value)
			}
		}

		if err := p.outputStore.WriteBatchRaw(rocksBatch); err != nil {
			rocksBatch.Destroy()
			p.logger.Error("Writer: failed to write batch: %v", err)
			p.errorChan <- err
			return
		}
		rocksBatch.Destroy()

		writeTime := time.Since(writeStart)
		p.stats.AddWrittenEntries(int64(batch.EntryCount))
		p.stats.AddWriteTime(writeTime)

		// Track per-CF stats
		perCF := make(map[string]int64)
		for cfName, entries := range batch.EntriesByCF {
			perCF[cfName] = int64(len(entries))
		}
		p.stats.AddCFEntries(perCF)
	}
}

// progressLoop logs progress at regular intervals.
func (p *LfsProcessor) progressLoop(progressLogger *txhashrework.ProgressLogger, totalLedgers int64, done chan struct{}) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	lastLoggedLedgers := int64(0)

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			ledgers := atomic.LoadInt64(&p.ledgersProcessed)
			txs := atomic.LoadInt64(&p.transactionsFound)

			// Log if at least 1% progress or significant change
			if ledgers-lastLoggedLedgers >= totalLedgers/100 || ledgers-lastLoggedLedgers > 100000 {
				progressLogger.LogProgress(ledgers, txs)
				lastLoggedLedgers = ledgers
			}
		}
	}
}
