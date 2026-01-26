// =============================================================================
// parallel_ingest.go - Parallel LFS to RocksDB Ingestion
// =============================================================================
//
// This file implements parallel ingestion that uses multiple goroutines to
// read, decompress, unmarshal, and extract transaction hashes concurrently.
//
// ARCHITECTURE:
//
//   ┌─────────────────┐    ┌─────────────────────┐    ┌──────────────┐
//   │   LFS READERS   │───>│    WORKER POOL      │───>│   COLLECTOR  │
//   │  (4 goroutines) │    │   (16 goroutines)   │    │ (1 goroutine)│
//   └─────────────────┘    └─────────────────────┘    └──────────────┘
//          │                        │                        │
//     Read compressed      Decompress+Unmarshal        Accumulate by CF
//     data from LFS        Extract txHashes            Track completion
//
// PARALLEL PIPELINE:
//
//   1. LFS Readers (4 goroutines):
//      - Read compressed data from LFS store
//      - Send LedgerWork items to workChan
//
//   2. Worker Pool (16 goroutines):
//      - Receive LedgerWork from workChan
//      - Decompress zstd data
//      - Unmarshal XDR to LedgerCloseMeta
//      - Extract txHashes and create entries
//      - Send LedgerEntries to entryChan
//
//   3. Collector (1 goroutine):
//      - Receive LedgerEntries from entryChan
//      - Accumulate entries by column family
//      - Track completion count for batch
//
// BATCH PROCESSING:
//
//   Each batch contains 5000 ledgers (configurable).
//   Within a batch:
//     1. All readers/workers process in parallel
//     2. Collector accumulates until batch complete
//     3. Write batch to RocksDB
//     4. Checkpoint to meta store
//     5. Log batch stats (every 10 batches)
//
// METRICS HANDLING:
//
//   - Wall-clock time: Tracked per batch for actual elapsed time
//   - Worker time: Summed across workers, divided by parallelism for "effective" time
//   - Per-ledger latency: Based on wall-clock time / ledger count
//   - Throughput: Ledgers per second based on wall-clock time
//
// CRASH RECOVERY:
//
//   Same as sequential mode:
//   - Resume from last_committed_ledger + 1
//   - Up to batch_size-1 ledgers may be re-ingested
//   - Duplicates handled by RocksDB compaction
//
// =============================================================================

package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers/lfs"
	"github.com/klauspost/compress/zstd"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// DefaultParallelBatchSize is the number of ledgers per batch in parallel mode
	DefaultParallelBatchSize = 5000

	// DefaultParallelWorkers is the number of decompress/unmarshal/extract workers
	DefaultParallelWorkers = 16

	// DefaultParallelReaders is the number of LFS reader goroutines
	DefaultParallelReaders = 4

	// WorkChanBuffer is the buffer size for the work channel (compressed data)
	// ~200 items × ~150KB average = ~30 MB buffer
	WorkChanBuffer = 200

	// EntryChanBuffer is the buffer size for the entry channel
	// ~100 items × ~20KB average = ~2 MB buffer
	EntryChanBuffer = 100

	// ProgressLogInterval is how often to log progress (in batches)
	ProgressLogInterval = 10
)

// =============================================================================
// Types
// =============================================================================

// LedgerWork represents compressed ledger data ready for processing
type LedgerWork struct {
	LedgerSeq      uint32
	CompressedData []byte
	ReadTime       time.Duration // Time spent reading from LFS
}

// LedgerEntries represents extracted entries from a single ledger
type LedgerEntries struct {
	LedgerSeq   uint32
	EntriesByCF map[string][]Entry
	TxCount     int
	Timing      WorkerTiming
}

// WorkerTiming tracks timing for a single ledger's processing
type WorkerTiming struct {
	DecompressTime time.Duration
	UnmarshalTime  time.Duration
	ExtractTime    time.Duration
	TotalTime      time.Duration
}

// BatchTiming aggregates timing for an entire batch
type BatchTiming struct {
	// Wall-clock times (actual elapsed time)
	WallClockStart time.Time
	WallClockEnd   time.Time

	// Cumulative worker times (sum across all workers)
	TotalReadTime       time.Duration
	TotalDecompressTime time.Duration
	TotalUnmarshalTime  time.Duration
	TotalExtractTime    time.Duration
	TotalWriteTime      time.Duration
	TotalCheckpointTime time.Duration

	// Counts
	LedgerCount int
	TxCount     int
}

// WallClockDuration returns the wall-clock duration of the batch
func (bt *BatchTiming) WallClockDuration() time.Duration {
	return bt.WallClockEnd.Sub(bt.WallClockStart)
}

// LedgersPerSecond returns the throughput based on wall-clock time
func (bt *BatchTiming) LedgersPerSecond() float64 {
	wallSec := bt.WallClockDuration().Seconds()
	if wallSec == 0 {
		return 0
	}
	return float64(bt.LedgerCount) / wallSec
}

// TxPerSecond returns the transaction throughput based on wall-clock time
func (bt *BatchTiming) TxPerSecond() float64 {
	wallSec := bt.WallClockDuration().Seconds()
	if wallSec == 0 {
		return 0
	}
	return float64(bt.TxCount) / wallSec
}

// AvgLedgerLatency returns average wall-clock time per ledger
func (bt *BatchTiming) AvgLedgerLatency() time.Duration {
	if bt.LedgerCount == 0 {
		return 0
	}
	return bt.WallClockDuration() / time.Duration(bt.LedgerCount)
}

// =============================================================================
// ParallelIngester
// =============================================================================

// ParallelIngester handles parallel ingestion from LFS to RocksDB
type ParallelIngester struct {
	config ParallelIngestionConfig
	store  TxHashStore
	meta   MetaStore
	logger Logger
	memory *MemoryMonitor

	// Channels
	workChan  chan LedgerWork
	entryChan chan LedgerEntries

	// Statistics
	aggStats *ParallelAggregatedStats
	cfCounts map[string]uint64 // Cumulative counts from meta store

	// Synchronization
	readerWg  sync.WaitGroup
	workerWg  sync.WaitGroup
	closeOnce sync.Once
}

// ParallelIngestionConfig holds configuration for parallel ingestion
type ParallelIngestionConfig struct {
	LFSStorePath    string
	StartLedger     uint32
	EndLedger       uint32
	BatchSize       int
	ParallelWorkers int
	ParallelReaders int
}

// NewParallelIngester creates a new parallel ingester
func NewParallelIngester(
	config ParallelIngestionConfig,
	store TxHashStore,
	meta MetaStore,
	logger Logger,
	memory *MemoryMonitor,
) *ParallelIngester {
	// Initialize CF counts from meta store
	cfCounts, err := meta.GetCFCounts()
	if err != nil || cfCounts == nil {
		cfCounts = make(map[string]uint64)
		for _, cf := range ColumnFamilyNames {
			cfCounts[cf] = 0
		}
	}

	return &ParallelIngester{
		config:    config,
		store:     store,
		meta:      meta,
		logger:    logger,
		memory:    memory,
		workChan:  make(chan LedgerWork, WorkChanBuffer),
		entryChan: make(chan LedgerEntries, EntryChanBuffer),
		aggStats:  NewParallelAggregatedStats(config.ParallelWorkers, config.ParallelReaders),
		cfCounts:  cfCounts,
	}
}

// Run executes the parallel ingestion
func (pi *ParallelIngester) Run() error {
	pi.logger.Separator()
	pi.logger.Info("                    PARALLEL INGESTION PHASE")
	pi.logger.Separator()
	pi.logger.Info("")
	pi.logger.Info("LFS Store:        %s", pi.config.LFSStorePath)
	pi.logger.Info("Ledger Range:     %d - %d", pi.config.StartLedger, pi.config.EndLedger)
	pi.logger.Info("Total Ledgers:    %s", helpers.FormatNumber(int64(pi.config.EndLedger-pi.config.StartLedger+1)))
	pi.logger.Info("Batch Size:       %d ledgers", pi.config.BatchSize)
	pi.logger.Info("Parallel Workers: %d (decompress + unmarshal + extract)", pi.config.ParallelWorkers)
	pi.logger.Info("Parallel Readers: %d (LFS read)", pi.config.ParallelReaders)
	pi.logger.Info("")

	totalLedgers := int(pi.config.EndLedger - pi.config.StartLedger + 1)
	totalBatches := (totalLedgers + pi.config.BatchSize - 1) / pi.config.BatchSize

	pi.logger.Info("Total Batches:    %d", totalBatches)
	pi.logger.Info("")

	// Process ledgers in batches
	currentLedger := pi.config.StartLedger
	batchNum := 0

	for currentLedger <= pi.config.EndLedger {
		batchNum++

		// Calculate batch range
		batchStart := currentLedger
		batchEnd := currentLedger + uint32(pi.config.BatchSize) - 1
		if batchEnd > pi.config.EndLedger {
			batchEnd = pi.config.EndLedger
		}

		// Process batch
		timing, err := pi.processBatch(batchNum, batchStart, batchEnd)
		if err != nil {
			return fmt.Errorf("failed to process batch %d: %w", batchNum, err)
		}

		// Update aggregate stats
		pi.aggStats.AddBatch(timing)

		// Log progress every N batches
		if batchNum%ProgressLogInterval == 0 || batchEnd == pi.config.EndLedger {
			pi.logProgress(batchNum, totalBatches, batchEnd)
		}

		// Memory check every 10 batches
		if batchNum%10 == 0 {
			pi.memory.Check()
		}

		currentLedger = batchEnd + 1
	}

	// Flush all MemTables to disk
	pi.logger.Info("")
	pi.logger.Info("Flushing all MemTables to disk...")
	flushStart := time.Now()
	if err := pi.store.FlushAll(); err != nil {
		return fmt.Errorf("failed to flush MemTables: %w", err)
	}
	pi.logger.Info("Flush completed in %v", time.Since(flushStart))

	// Log final summary
	pi.aggStats.LogSummary(pi.logger)
	pi.logCFSummary()
	pi.memory.LogSummary(pi.logger)

	pi.logger.Sync()

	return nil
}

// processBatch processes a single batch of ledgers
func (pi *ParallelIngester) processBatch(batchNum int, startLedger, endLedger uint32) (*BatchTiming, error) {
	timing := &BatchTiming{
		WallClockStart: time.Now(),
		LedgerCount:    int(endLedger - startLedger + 1),
	}

	// Prepare collection structures
	entriesByCF := make(map[string][]Entry)
	for _, cf := range ColumnFamilyNames {
		entriesByCF[cf] = make([]Entry, 0, timing.LedgerCount*25) // ~25 tx per ledger avg
	}
	txCountByCF := make(map[string]int)

	// Error channel for readers/workers
	errChan := make(chan error, pi.config.ParallelReaders+pi.config.ParallelWorkers)

	// Completion tracking
	var completedLedgers int64

	// Start workers (before readers to be ready to consume)
	for w := 0; w < pi.config.ParallelWorkers; w++ {
		pi.workerWg.Add(1)
		go pi.worker(w, errChan)
	}

	// Start readers
	ledgersPerReader := timing.LedgerCount / pi.config.ParallelReaders
	remainder := timing.LedgerCount % pi.config.ParallelReaders

	readerStart := startLedger
	for r := 0; r < pi.config.ParallelReaders; r++ {
		count := ledgersPerReader
		if r < remainder {
			count++
		}
		if count == 0 {
			continue
		}

		readerEnd := readerStart + uint32(count) - 1
		pi.readerWg.Add(1)
		go pi.reader(r, readerStart, readerEnd, errChan)
		readerStart = readerEnd + 1
	}

	// Collector: accumulate entries until batch complete
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)

		for entries := range pi.entryChan {
			// Accumulate entries
			for cf, cfEntries := range entries.EntriesByCF {
				entriesByCF[cf] = append(entriesByCF[cf], cfEntries...)
				txCountByCF[cf] += len(cfEntries)
			}

			// Accumulate timing
			timing.TotalDecompressTime += entries.Timing.DecompressTime
			timing.TotalUnmarshalTime += entries.Timing.UnmarshalTime
			timing.TotalExtractTime += entries.Timing.ExtractTime
			timing.TxCount += entries.TxCount

			atomic.AddInt64(&completedLedgers, 1)
		}
	}()

	// Wait for all readers to finish
	pi.readerWg.Wait()
	close(pi.workChan)

	// Wait for all workers to finish
	pi.workerWg.Wait()
	close(pi.entryChan)

	// Wait for collector to finish
	<-collectorDone

	// Check for errors
	select {
	case err := <-errChan:
		return nil, err
	default:
	}

	// Reset channels for next batch
	pi.workChan = make(chan LedgerWork, WorkChanBuffer)
	pi.entryChan = make(chan LedgerEntries, EntryChanBuffer)

	// Write to RocksDB
	writeStart := time.Now()
	if err := pi.store.WriteBatch(entriesByCF); err != nil {
		return nil, fmt.Errorf("failed to write batch to RocksDB: %w", err)
	}
	timing.TotalWriteTime = time.Since(writeStart)

	// Update cumulative CF counts
	for cf, count := range txCountByCF {
		pi.cfCounts[cf] += uint64(count)
	}

	// Checkpoint to meta store
	checkpointStart := time.Now()
	if err := pi.meta.CommitBatchProgress(endLedger, pi.cfCounts); err != nil {
		return nil, fmt.Errorf("failed to checkpoint batch: %w", err)
	}
	timing.TotalCheckpointTime = time.Since(checkpointStart)

	timing.WallClockEnd = time.Now()

	// Log batch summary
	pi.logBatchSummary(batchNum, startLedger, endLedger, timing)

	return timing, nil
}

// reader reads RAW compressed ledger data from LFS (no decompression).
// Workers handle the CPU-intensive decompression and unmarshaling.
func (pi *ParallelIngester) reader(id int, startSeq, endSeq uint32, errChan chan<- error) {
	defer pi.readerWg.Done()

	// Use RawLedgerIterator - only reads compressed bytes, no decompression
	iterator, err := lfs.NewRawLedgerIterator(pi.config.LFSStorePath, startSeq, endSeq)
	if err != nil {
		select {
		case errChan <- fmt.Errorf("reader %d: failed to create iterator: %w", id, err):
		default:
		}
		return
	}
	defer iterator.Close()

	for {
		// Read raw compressed data - NO decompression here
		data, hasMore, err := iterator.Next()
		if err != nil {
			select {
			case errChan <- fmt.Errorf("reader %d: failed to read ledger %d: %w", id, data.LedgerSeq, err):
			default:
			}
			return
		}
		if !hasMore {
			break
		}

		// Send raw compressed bytes to worker for processing
		pi.workChan <- LedgerWork{
			LedgerSeq:      data.LedgerSeq,
			CompressedData: data.CompressedData, // Actually compressed now!
			ReadTime:       data.ReadTime,
		}
	}
}

// worker processes ledger data (decompress, unmarshal, extract)
func (pi *ParallelIngester) worker(id int, errChan chan<- error) {
	defer pi.workerWg.Done()

	// Create a zstd decoder for this worker - workers do actual decompression
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		select {
		case errChan <- fmt.Errorf("worker %d: failed to create zstd decoder: %w", id, err):
		default:
		}
		return
	}
	defer decoder.Close()

	// Reusable ledger sequence bytes
	ledgerSeqBytes := make([]byte, 4)

	for work := range pi.workChan {
		timing := WorkerTiming{}
		totalStart := time.Now()

		// Decompress - workers now do actual decompression (parallelized!)
		decompressStart := time.Now()
		uncompressed, err := decoder.DecodeAll(work.CompressedData, nil)
		if err != nil {
			select {
			case errChan <- fmt.Errorf("worker %d: decompress failed for ledger %d: %w", id, work.LedgerSeq, err):
			default:
			}
			return
		}
		timing.DecompressTime = time.Since(decompressStart)

		// Unmarshal XDR
		unmarshalStart := time.Now()
		var lcm xdr.LedgerCloseMeta
		if err := lcm.UnmarshalBinary(uncompressed); err != nil {
			select {
			case errChan <- fmt.Errorf("worker %d: failed to unmarshal ledger %d: %w", id, work.LedgerSeq, err):
			default:
			}
			return
		}
		timing.UnmarshalTime = time.Since(unmarshalStart)

		// Extract transaction hashes
		extractStart := time.Now()
		entriesByCF := make(map[string][]Entry)
		for _, cf := range ColumnFamilyNames {
			entriesByCF[cf] = make([]Entry, 0, 32)
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
			cfName := GetColumnFamilyName(txHash)

			entriesByCF[cfName] = append(entriesByCF[cfName], Entry{
				Key:   copyBytes(txHash),
				Value: copyBytes(ledgerSeqBytes),
			})
			txCount++
		}
		txReader.Close()
		timing.ExtractTime = time.Since(extractStart)
		timing.TotalTime = time.Since(totalStart)

		// Send to collector
		pi.entryChan <- LedgerEntries{
			LedgerSeq:   work.LedgerSeq,
			EntriesByCF: entriesByCF,
			TxCount:     txCount,
			Timing:      timing,
		}
	}
}

// logBatchSummary logs a summary of a single batch
func (pi *ParallelIngester) logBatchSummary(batchNum int, startLedger, endLedger uint32, timing *BatchTiming) {
	// Calculate effective per-phase time (cumulative worker time / parallelism)
	// This gives a sense of "if we had 1 worker, how long would each phase take"
	effectiveWorkers := float64(pi.config.ParallelWorkers)

	pi.logger.Info("Batch %d: ledgers %d-%d | %d txs | wall=%v | %.0f ledgers/sec | %.0f tx/sec",
		batchNum, startLedger, endLedger, timing.TxCount,
		timing.WallClockDuration().Truncate(time.Millisecond),
		timing.LedgersPerSecond(), timing.TxPerSecond())

	// Only log detailed timing breakdown occasionally (every ProgressLogInterval batches)
	if batchNum%ProgressLogInterval == 0 {
		pi.logger.Info("  Timing breakdown (cumulative across %d workers, effective=cumulative/%d):",
			pi.config.ParallelWorkers, pi.config.ParallelWorkers)
		pi.logger.Info("    Decompress: %v (eff: %v/ledger)",
			timing.TotalDecompressTime,
			time.Duration(float64(timing.TotalDecompressTime)/float64(timing.LedgerCount)))
		pi.logger.Info("    Unmarshal:  %v (eff: %v/ledger)",
			timing.TotalUnmarshalTime,
			time.Duration(float64(timing.TotalUnmarshalTime)/float64(timing.LedgerCount)))
		pi.logger.Info("    Extract:    %v (eff: %v/ledger)",
			timing.TotalExtractTime,
			time.Duration(float64(timing.TotalExtractTime)/float64(timing.LedgerCount)))
		pi.logger.Info("    Write:      %v (eff: %v/ledger)",
			timing.TotalWriteTime,
			time.Duration(float64(timing.TotalWriteTime)/float64(timing.LedgerCount)))
		pi.logger.Info("    Checkpoint: %v", timing.TotalCheckpointTime)
		pi.logger.Info("  Parallelism efficiency: %.1f%% (wall=%v vs serial_work=%v)",
			100.0*float64(timing.TotalDecompressTime+timing.TotalUnmarshalTime+timing.TotalExtractTime)/
				(effectiveWorkers*float64(timing.WallClockDuration()-timing.TotalWriteTime-timing.TotalCheckpointTime)),
			timing.WallClockDuration()-timing.TotalWriteTime-timing.TotalCheckpointTime,
			timing.TotalDecompressTime+timing.TotalUnmarshalTime+timing.TotalExtractTime)
	}
}

// logProgress logs overall progress
func (pi *ParallelIngester) logProgress(batchNum, totalBatches int, currentLedger uint32) {
	stats := pi.aggStats

	pct := float64(batchNum) / float64(totalBatches) * 100
	elapsed := stats.Elapsed()

	// Calculate ETA
	var eta time.Duration
	if batchNum > 0 {
		remainingBatches := totalBatches - batchNum
		avgBatchTime := elapsed / time.Duration(batchNum)
		eta = avgBatchTime * time.Duration(remainingBatches)
	}

	pi.logger.Info("")
	pi.logger.Separator()
	pi.logger.Info("PROGRESS: %d/%d batches (%.1f%%) | elapsed=%v | ETA=%v",
		batchNum, totalBatches, pct,
		elapsed.Truncate(time.Second), eta.Truncate(time.Second))
	pi.logger.Info("  Ledgers: %s | Transactions: %s",
		helpers.FormatNumber(int64(stats.TotalLedgers)),
		helpers.FormatNumber(int64(stats.TotalTx)))
	pi.logger.Info("  Throughput: %.0f ledgers/sec | %.0f tx/sec",
		stats.OverallLedgersPerSecond(), stats.OverallTxPerSecond())
	pi.logger.Info("  Avg latency: %v/ledger (wall-clock)",
		stats.AvgWallClockPerLedger())
	pi.logger.Separator()
	pi.logger.Info("")
}

// logCFSummary logs transaction counts per column family
func (pi *ParallelIngester) logCFSummary() {
	var total uint64
	for _, count := range pi.cfCounts {
		total += count
	}

	pi.logger.Info("TRANSACTIONS BY COLUMN FAMILY:")
	for _, cf := range ColumnFamilyNames {
		count := pi.cfCounts[cf]
		pct := float64(count) / float64(total) * 100
		pi.logger.Info("  CF %s: %12s (%.2f%%)", cf, helpers.FormatNumber(int64(count)), pct)
	}
	pi.logger.Info("")
}

// GetCFCounts returns the current CF counts
func (pi *ParallelIngester) GetCFCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	for cf, count := range pi.cfCounts {
		counts[cf] = count
	}
	return counts
}

// =============================================================================
// ParallelAggregatedStats - Statistics across all batches
// =============================================================================

// ParallelAggregatedStats accumulates statistics across batches for parallel ingestion
type ParallelAggregatedStats struct {
	mu sync.Mutex

	// Configuration
	ParallelWorkers int
	ParallelReaders int

	// Counts
	TotalBatches int
	TotalLedgers int
	TotalTx      int

	// Wall-clock timing (actual elapsed)
	TotalWallClock time.Duration

	// Cumulative worker times (sum across all workers)
	TotalReadTime       time.Duration
	TotalDecompressTime time.Duration
	TotalUnmarshalTime  time.Duration
	TotalExtractTime    time.Duration
	TotalWriteTime      time.Duration
	TotalCheckpointTime time.Duration

	// Start time
	StartTime time.Time
}

// NewParallelAggregatedStats creates a new stats accumulator
func NewParallelAggregatedStats(workers, readers int) *ParallelAggregatedStats {
	return &ParallelAggregatedStats{
		ParallelWorkers: workers,
		ParallelReaders: readers,
		StartTime:       time.Now(),
	}
}

// AddBatch adds a batch's timing to the aggregate
func (pas *ParallelAggregatedStats) AddBatch(timing *BatchTiming) {
	pas.mu.Lock()
	defer pas.mu.Unlock()

	pas.TotalBatches++
	pas.TotalLedgers += timing.LedgerCount
	pas.TotalTx += timing.TxCount
	pas.TotalWallClock += timing.WallClockDuration()
	pas.TotalReadTime += timing.TotalReadTime
	pas.TotalDecompressTime += timing.TotalDecompressTime
	pas.TotalUnmarshalTime += timing.TotalUnmarshalTime
	pas.TotalExtractTime += timing.TotalExtractTime
	pas.TotalWriteTime += timing.TotalWriteTime
	pas.TotalCheckpointTime += timing.TotalCheckpointTime
}

// Elapsed returns time since start
func (pas *ParallelAggregatedStats) Elapsed() time.Duration {
	pas.mu.Lock()
	defer pas.mu.Unlock()
	return time.Since(pas.StartTime)
}

// OverallLedgersPerSecond returns overall throughput based on wall-clock
func (pas *ParallelAggregatedStats) OverallLedgersPerSecond() float64 {
	pas.mu.Lock()
	defer pas.mu.Unlock()

	elapsed := time.Since(pas.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(pas.TotalLedgers) / elapsed
}

// OverallTxPerSecond returns overall transaction throughput
func (pas *ParallelAggregatedStats) OverallTxPerSecond() float64 {
	pas.mu.Lock()
	defer pas.mu.Unlock()

	elapsed := time.Since(pas.StartTime).Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(pas.TotalTx) / elapsed
}

// AvgWallClockPerLedger returns average wall-clock time per ledger
func (pas *ParallelAggregatedStats) AvgWallClockPerLedger() time.Duration {
	pas.mu.Lock()
	defer pas.mu.Unlock()

	if pas.TotalLedgers == 0 {
		return 0
	}
	return pas.TotalWallClock / time.Duration(pas.TotalLedgers)
}

// LogSummary logs the final summary
func (pas *ParallelAggregatedStats) LogSummary(logger Logger) {
	pas.mu.Lock()
	defer pas.mu.Unlock()

	elapsed := time.Since(pas.StartTime)

	logger.Separator()
	logger.Info("                    PARALLEL INGESTION SUMMARY")
	logger.Separator()
	logger.Info("")
	logger.Info("CONFIGURATION:")
	logger.Info("  Parallel Workers:  %d", pas.ParallelWorkers)
	logger.Info("  Parallel Readers:  %d", pas.ParallelReaders)
	logger.Info("")
	logger.Info("TOTALS:")
	logger.Info("  Batches:           %d", pas.TotalBatches)
	logger.Info("  Ledgers:           %s", helpers.FormatNumber(int64(pas.TotalLedgers)))
	logger.Info("  Transactions:      %s", helpers.FormatNumber(int64(pas.TotalTx)))
	logger.Info("")
	logger.Info("WALL-CLOCK TIMING:")
	logger.Info("  Total Elapsed:     %v", elapsed.Truncate(time.Second))
	logger.Info("  Avg per Ledger:    %v", pas.TotalWallClock/time.Duration(pas.TotalLedgers))
	logger.Info("")
	logger.Info("THROUGHPUT (based on wall-clock):")
	logger.Info("  Ledgers/sec:       %.0f", float64(pas.TotalLedgers)/elapsed.Seconds())
	logger.Info("  Transactions/sec:  %.0f", float64(pas.TotalTx)/elapsed.Seconds())
	logger.Info("")

	// Calculate effective times (cumulative / parallelism)
	// This shows how much work was done per component
	logger.Info("CUMULATIVE WORKER TIME (sum across all %d workers):", pas.ParallelWorkers)
	logger.Info("  Decompress:        %v", pas.TotalDecompressTime)
	logger.Info("  Unmarshal:         %v", pas.TotalUnmarshalTime)
	logger.Info("  Extract:           %v", pas.TotalExtractTime)
	logger.Info("  Write (serial):    %v", pas.TotalWriteTime)
	logger.Info("  Checkpoint:        %v", pas.TotalCheckpointTime)
	logger.Info("")

	// Effective per-ledger times (cumulative / ledger count)
	// This shows average time spent per ledger across all workers
	if pas.TotalLedgers > 0 {
		logger.Info("EFFECTIVE TIME PER LEDGER (cumulative / ledger count):")
		logger.Info("  Decompress:        %v", pas.TotalDecompressTime/time.Duration(pas.TotalLedgers))
		logger.Info("  Unmarshal:         %v", pas.TotalUnmarshalTime/time.Duration(pas.TotalLedgers))
		logger.Info("  Extract:           %v", pas.TotalExtractTime/time.Duration(pas.TotalLedgers))
		logger.Info("  Write:             %v", pas.TotalWriteTime/time.Duration(pas.TotalLedgers))
		logger.Info("")
	}

	// Parallelism efficiency
	totalWork := pas.TotalDecompressTime + pas.TotalUnmarshalTime + pas.TotalExtractTime
	parallelizableElapsed := elapsed - pas.TotalWriteTime - pas.TotalCheckpointTime
	if parallelizableElapsed > 0 {
		efficiency := float64(totalWork) / (float64(pas.ParallelWorkers) * float64(parallelizableElapsed)) * 100
		logger.Info("PARALLELISM:")
		logger.Info("  Theoretical serial time: %v", totalWork)
		logger.Info("  Actual parallel time:    %v", parallelizableElapsed)
		logger.Info("  Speedup:                 %.1fx", float64(totalWork)/float64(parallelizableElapsed))
		logger.Info("  Efficiency:              %.1f%%", efficiency)
		logger.Info("")
	}
}

// =============================================================================
// Convenience Function
// =============================================================================

// RunParallelIngestion is a convenience function to run parallel ingestion
//
// PARAMETERS:
//   - config: Main configuration
//   - store: RocksDB data store
//   - meta: Meta store for checkpointing
//   - logger: Logger
//   - memory: Memory monitor
//   - startFromLedger: First ledger to ingest (may be > config.StartLedger if resuming)
//
// RETURNS:
//   - error if ingestion fails
func RunParallelIngestion(
	config *Config,
	store TxHashStore,
	meta MetaStore,
	logger Logger,
	memory *MemoryMonitor,
	startFromLedger uint32,
) error {
	ingesterConfig := ParallelIngestionConfig{
		LFSStorePath:    config.LFSStorePath,
		StartLedger:     startFromLedger,
		EndLedger:       config.EndLedger,
		BatchSize:       config.ParallelBatchSize,
		ParallelWorkers: config.ParallelWorkers,
		ParallelReaders: config.ParallelReaders,
	}

	ingester := NewParallelIngester(ingesterConfig, store, meta, logger, memory)
	return ingester.Run()
}
