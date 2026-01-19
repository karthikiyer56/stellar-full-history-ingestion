// lfs_processor.go
// =============================================================================
// LFS (Local Filesystem) Input Source Processor
// =============================================================================
//
// This module handles reading ledgers from an LFS store, extracting transactions,
// and building the tx_hash → ledger_seq mapping with multi-worker parallelism.
//
// Architecture:
// - Main goroutine coordinates workers and collects results
// - N worker goroutines each process a contiguous ledger range
// - Each worker uses its own LedgerIterator instance
// - Workers batch entries and send to a channel for writing
// - A single writer goroutine handles RocksDB writes
//
// =============================================================================

package main

import (
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers/lfs"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// LFS Processing Types
// =============================================================================

// LfsStats holds statistics for LFS processing.
type LfsStats struct {
	LedgersProcessed  int64
	TransactionsFound int64
	EntriesWritten    int64
	WorkerErrors      int64
	EntriesPerCF      map[string]int64
	ProcessingTime    time.Duration
	WriteTime         time.Duration
	mu                sync.Mutex
}

// NewLfsStats creates a new LfsStats instance.
func NewLfsStats() *LfsStats {
	stats := &LfsStats{
		EntriesPerCF: make(map[string]int64),
	}
	for _, cfName := range ColumnFamilyNames {
		stats.EntriesPerCF[cfName] = 0
	}
	return stats
}

// AddCFEntries adds column family entry counts (thread-safe).
func (s *LfsStats) AddCFEntries(perCF map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for cfName, count := range perCF {
		s.EntriesPerCF[cfName] += count
	}
}

// WriteBatch represents a batch of entries to write to the output store.
type WriteBatch struct {
	EntriesByCF map[string][]Entry
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

// LfsProcessor handles reading from LFS and extracting tx_hash → ledger_seq.
type LfsProcessor struct {
	config      *Config
	outputStore *OutputStore
	logger      *DualLogger
	stats       *LfsStats

	// Atomic counters for progress tracking
	ledgersProcessed  int64
	transactionsFound int64

	// Channels
	batchChan chan *WriteBatch
	errorChan chan error
	doneChan  chan struct{}
}

// NewLfsProcessor creates a new LFS processor.
func NewLfsProcessor(config *Config, outputStore *OutputStore, logger *DualLogger) *LfsProcessor {
	return &LfsProcessor{
		config:      config,
		outputStore: outputStore,
		logger:      logger,
		stats:       NewLfsStats(),
		batchChan:   make(chan *WriteBatch, config.LfsWorkers*2), // Buffer for 2x workers
		errorChan:   make(chan error, config.LfsWorkers),
		doneChan:    make(chan struct{}),
	}
}

// Process runs the LFS processing with multiple workers.
func (p *LfsProcessor) Process(dryRun bool) (*LfsStats, error) {
	startTime := time.Now()

	totalLedgers := int64(p.config.LfsEndLedger - p.config.LfsStartLedger + 1)
	p.logger.Info("Starting LFS processing with %d workers", p.config.LfsWorkers)
	p.logger.Info("Ledger range: %d - %d (%s ledgers)",
		p.config.LfsStartLedger, p.config.LfsEndLedger, formatNumber(totalLedgers))

	// Split work among workers
	ranges := p.splitWork()
	for _, r := range ranges {
		p.logger.Info("  Worker %d: ledgers %d - %d (%s ledgers)",
			r.WorkerID, r.StartLedger, r.EndLedger,
			formatNumber(int64(r.EndLedger-r.StartLedger+1)))
	}

	// Start progress logger
	progressLogger := NewProgressLogger(p.logger, totalLedgers, 60*time.Second)

	// Start writer goroutine (unless dry run)
	var writerWg sync.WaitGroup
	if !dryRun && p.outputStore != nil {
		writerWg.Add(1)
		go p.writerLoop(&writerWg)
	}

	// Start workers
	var workerWg sync.WaitGroup
	for _, r := range ranges {
		workerWg.Add(1)
		go p.workerLoop(r, dryRun, &workerWg)
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
	p.stats.ProcessingTime = time.Since(startTime)

	// Log final progress
	progressLogger.LogProgress(p.stats.LedgersProcessed, p.stats.TransactionsFound)

	return p.stats, nil
}

// splitWork divides the ledger range among workers.
func (p *LfsProcessor) splitWork() []WorkerRange {
	numWorkers := p.config.LfsWorkers
	totalLedgers := p.config.LfsEndLedger - p.config.LfsStartLedger + 1
	ledgersPerWorker := totalLedgers / uint32(numWorkers)
	remainder := totalLedgers % uint32(numWorkers)

	ranges := make([]WorkerRange, numWorkers)
	currentStart := p.config.LfsStartLedger

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
func (p *LfsProcessor) workerLoop(r WorkerRange, dryRun bool, wg *sync.WaitGroup) {
	defer wg.Done()

	// Create iterator for this worker's range
	iterator, err := lfs.NewLedgerIterator(p.config.LfsLedgerStorePath, r.StartLedger, r.EndLedger)
	if err != nil {
		p.logger.Error("Worker %d: failed to create iterator: %v", r.WorkerID, err)
		p.errorChan <- err
		return
	}
	defer iterator.Close()

	// Batch accumulator
	batchSize := p.config.BatchSize / p.config.LfsWorkers // Per-worker batch size
	if batchSize < 1000 {
		batchSize = 1000
	}

	entriesByCF := make(map[string][]Entry)
	for _, cfName := range ColumnFamilyNames {
		entriesByCF[cfName] = make([]Entry, 0, batchSize/16)
	}
	entriesInBatch := 0

	// Process ledgers
	for {
		lcm, ledgerSeq, _, hasMore, err := iterator.Next()
		if err != nil {
			p.logger.Error("Worker %d: failed to read ledger %d: %v", r.WorkerID, ledgerSeq, err)
			atomic.AddInt64(&p.stats.WorkerErrors, 1)
			// Continue processing despite errors
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
			atomic.AddInt64(&p.stats.WorkerErrors, 1)
			continue
		}

		entriesInBatch += txCount
		atomic.AddInt64(&p.ledgersProcessed, 1)
		atomic.AddInt64(&p.transactionsFound, int64(txCount))

		// Send batch if full
		if entriesInBatch >= batchSize && !dryRun {
			p.sendBatch(entriesByCF, entriesInBatch)

			// Reset batch
			entriesByCF = make(map[string][]Entry)
			for _, cfName := range ColumnFamilyNames {
				entriesByCF[cfName] = make([]Entry, 0, batchSize/16)
			}
			entriesInBatch = 0
		}
	}

	// Send final batch
	if entriesInBatch > 0 && !dryRun {
		p.sendBatch(entriesByCF, entriesInBatch)
	}
}

// extractTxHashes extracts transaction hashes from a ledger and adds them to the batch.
func (p *LfsProcessor) extractTxHashes(lcm xdr.LedgerCloseMeta, ledgerSeq uint32, entriesByCF map[string][]Entry) (int, error) {
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		network.PublicNetworkPassphrase, lcm)
	if err != nil {
		return 0, err
	}
	defer txReader.Close()

	// Encode ledger sequence as 4 bytes (big-endian for proper sorting)
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
		cfName := GetColumnFamilyName(txHash)

		// Add to batch
		entriesByCF[cfName] = append(entriesByCF[cfName], Entry{
			Key:   txHash,
			Value: ledgerSeqBytes,
		})
		txCount++
	}

	return txCount, nil
}

// sendBatch sends a batch to the writer channel.
func (p *LfsProcessor) sendBatch(entriesByCF map[string][]Entry, entryCount int) {
	// Make a copy of the entries for the batch
	batchEntries := make(map[string][]Entry)
	for cfName, entries := range entriesByCF {
		if len(entries) > 0 {
			entriesCopy := make([]Entry, len(entries))
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

		if err := p.outputStore.WriteBatch(batch.EntriesByCF); err != nil {
			p.logger.Error("Writer: failed to write batch: %v", err)
			p.errorChan <- err
			return
		}

		writeTime := time.Since(writeStart)
		atomic.AddInt64(&p.stats.EntriesWritten, int64(batch.EntryCount))

		// Track per-CF stats
		perCF := make(map[string]int64)
		for cfName, entries := range batch.EntriesByCF {
			perCF[cfName] = int64(len(entries))
		}
		p.stats.AddCFEntries(perCF)

		// Accumulate write time
		p.stats.mu.Lock()
		p.stats.WriteTime += writeTime
		p.stats.mu.Unlock()
	}
}

// progressLoop logs progress at regular intervals.
func (p *LfsProcessor) progressLoop(progressLogger *ProgressLogger, totalLedgers int64, done chan struct{}) {
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

// =============================================================================
// Execute LFS Merge
// =============================================================================

// ExecuteLfsMerge performs the merge operation using LFS input.
func ExecuteLfsMerge(config *Config, logger *DualLogger, dryRun bool) (*MergeStats, error) {
	stats := NewMergeStats()

	// =========================================================================
	// Open Output Store (unless dry run)
	// =========================================================================
	var outputStore *OutputStore
	var err error

	if !dryRun {
		logger.Info("Opening output store with column families...")
		outputStore, err = OpenOutputStore(config)
		if err != nil {
			return nil, err
		}
		defer outputStore.Close()
		logger.Info("Output store opened successfully")
		logger.Info("")
	}

	// =========================================================================
	// Process LFS Input
	// =========================================================================
	processor := NewLfsProcessor(config, outputStore, logger)
	lfsStats, err := processor.Process(dryRun)
	if err != nil {
		return nil, err
	}

	// Convert LFS stats to MergeStats
	stats.TotalEntriesRead = lfsStats.TransactionsFound
	stats.TotalEntriesWritten = lfsStats.EntriesWritten
	stats.ReadTime = lfsStats.ProcessingTime
	stats.WriteTime = lfsStats.WriteTime
	for cfName, count := range lfsStats.EntriesPerCF {
		stats.EntriesPerCF[cfName] = count
	}

	// =========================================================================
	// Final Compaction (unless dry run)
	// =========================================================================
	if !dryRun && outputStore != nil {
		stats.CompactionTime = outputStore.CompactAll()

		// Generate active store configuration for Phase 2
		if err := GenerateActiveStoreConfig(config); err != nil {
			logger.Error("Warning: failed to generate active store config: %v", err)
		}

		// Print instructions for using the store in active mode
		PrintActiveStoreInstructions(config)
	}

	return stats, nil
}
