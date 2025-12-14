package main

// =============================================================================
// benchmark.go - Stellar Transaction Hash Lookup Benchmark Tool
// =============================================================================
//
// PURPOSE:
//   High-performance benchmarking tool for querying Stellar transactions across
//   multiple RecSplit indexes and RocksDB stores. Designed to handle 10-100M
//   transaction hashes with parallel processing.
//
// ARCHITECTURE:
//   - Producer-Consumer model with buffered channels (Go idiomatic)
//   - Pre-created worker goroutines (default 16) with dedicated zstd decoders
//   - Shared LRU block cache across all RocksDB instances
//   - Sequential RecSplit search with false-positive handling
//   - Range-based RocksDB store selection (pre-indexed at startup)
//
// DATA FLOW:
//   1. Main thread reads input file, pushes txHashes to work channel
//   2. Workers pull batches (1k txs), process them, push results to results channel
//   3. Stats aggregator collects results, prints progress at ~1% intervals
//   4. Final summary printed at completion
//
// FALSE POSITIVE HANDLING:
//   RecSplit is a Minimal Perfect Hash Function that can return false positives.
//   This tool handles multiple scenarios:
//   - FALSE_POSITIVE_NORMAL: Single RecSplit returns wrong ledger
//   - FALSE_POSITIVE_COMPOUND: Multiple RecSplits return wrong ledgers (all false)
//   - FALSE_POSITIVE_PARTIAL: Found in one, false positive in others (counts as success)
//
// USAGE:
//   ./benchmark \
//     --input-file /path/to/txhashes.txt \
//     --recsplit-index-files /path/to/2014.idx,/path/to/2015.idx,... \
//     --ledger-seq-to-lcm-dbs /path/to/2014-rocksdb,/path/to/2015-rocksdb,... \
//     --error-file /path/to/errors.log \
//     --log-file /path/to/benchmark.log \
//     --parallelism 16 \
//     --read-buffer-mb 16384
//
// =============================================================================

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/erigontech/erigon/db/recsplit"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/klauspost/compress/zstd"
	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
)

// =============================================================================
// CONSTANTS
// =============================================================================

const (
	// WorkerBatchSize is the number of transactions each worker processes at a time
	WorkerBatchSize = 1000

	// ChannelBufferMultiplier determines how far ahead the producer reads
	// Buffer size = parallelism * WorkerBatchSize * ChannelBufferMultiplier
	ChannelBufferMultiplier = 2

	// ErrorFileFlushInterval is how often we flush the error file buffer
	ErrorFileFlushInterval = 1000

	// ProgressReportPercent is the interval for progress reporting (1%)
	ProgressReportPercent = 1
)

// =============================================================================
// ERROR TYPES
// =============================================================================

// ErrorType represents the category of error for logging and statistics
type ErrorType int

const (
	ErrTypeNone ErrorType = iota
	ErrTypeInvalidInput
	ErrTypeRecSplitNotFound
	ErrTypeLedgerNotFound
	ErrTypeFalsePositiveNormal
	ErrTypeFalsePositiveCompound
	ErrTypeFalsePositivePartial // This is actually a success, but logged for analysis
	ErrTypeLedgerInMultipleDBs
	ErrTypeUnforeseen
)

func (e ErrorType) String() string {
	switch e {
	case ErrTypeNone:
		return "NONE"
	case ErrTypeInvalidInput:
		return "INVALID_INPUT"
	case ErrTypeRecSplitNotFound:
		return "RECSPLIT_NOT_FOUND"
	case ErrTypeLedgerNotFound:
		return "LEDGER_NOT_FOUND"
	case ErrTypeFalsePositiveNormal:
		return "FALSE_POSITIVE_NORMAL"
	case ErrTypeFalsePositiveCompound:
		return "FALSE_POSITIVE_COMPOUND"
	case ErrTypeFalsePositivePartial:
		return "FALSE_POSITIVE_PARTIAL"
	case ErrTypeLedgerInMultipleDBs:
		return "LEDGER_IN_MULTIPLE_DBS"
	case ErrTypeUnforeseen:
		return "UNFORESEEN_ERROR"
	default:
		return "UNKNOWN"
	}
}

// BenchmarkError encapsulates all error information for logging
type BenchmarkError struct {
	Type            ErrorType
	TxHash          string
	LedgerSeq       uint32   // For single ledger errors
	LedgerSeqs      []uint32 // For compound/partial false positives
	SuccessLedger   uint32   // For partial false positives (the one that worked)
	DBNames         []string // For ledger-in-multiple-dbs
	TxsScanned      int
	TotalDuration   time.Duration
	UnderlyingError error
}

func (e *BenchmarkError) Error() string {
	return fmt.Sprintf("%s: %s", e.Type.String(), e.formatDetails())
}

// FormatForLog formats the error as a pipe-delimited line for the error file
func (e *BenchmarkError) FormatForLog() string {
	switch e.Type {
	case ErrTypeRecSplitNotFound:
		return fmt.Sprintf("%s|%s|%s",
			e.Type.String(), e.TxHash, helpers.FormatDuration(e.TotalDuration))

	case ErrTypeLedgerNotFound:
		return fmt.Sprintf("%s|%s|%d|%s",
			e.Type.String(), e.TxHash, e.LedgerSeq, helpers.FormatDuration(e.TotalDuration))

	case ErrTypeFalsePositiveNormal:
		return fmt.Sprintf("%s|%s|%d|%d|%s",
			e.Type.String(), e.TxHash, e.LedgerSeq, e.TxsScanned, helpers.FormatDuration(e.TotalDuration))

	case ErrTypeFalsePositiveCompound:
		ledgers := make([]string, len(e.LedgerSeqs))
		for i, l := range e.LedgerSeqs {
			ledgers[i] = fmt.Sprintf("%d", l)
		}
		return fmt.Sprintf("%s|%s|%s|%s",
			e.Type.String(), e.TxHash, strings.Join(ledgers, ","), helpers.FormatDuration(e.TotalDuration))

	case ErrTypeFalsePositivePartial:
		falsePositives := make([]string, 0, len(e.LedgerSeqs))
		for _, l := range e.LedgerSeqs {
			if l != e.SuccessLedger {
				falsePositives = append(falsePositives, fmt.Sprintf("%d", l))
			}
		}
		return fmt.Sprintf("%s|%s|%d|%s|%s",
			e.Type.String(), e.TxHash, e.SuccessLedger, strings.Join(falsePositives, ","), helpers.FormatDuration(e.TotalDuration))

	case ErrTypeLedgerInMultipleDBs:
		return fmt.Sprintf("%s|%s|%d|%s|%s",
			e.Type.String(), e.TxHash, e.LedgerSeq, strings.Join(e.DBNames, ","), helpers.FormatDuration(e.TotalDuration))

	case ErrTypeUnforeseen:
		errMsg := ""
		if e.UnderlyingError != nil {
			errMsg = e.UnderlyingError.Error()
		}
		return fmt.Sprintf("%s|%s|%s|%s",
			e.Type.String(), e.TxHash, errMsg, helpers.FormatDuration(e.TotalDuration))

	default:
		return fmt.Sprintf("%s|%s|%s",
			e.Type.String(), e.TxHash, helpers.FormatDuration(e.TotalDuration))
	}
}

func (e *BenchmarkError) formatDetails() string {
	switch e.Type {
	case ErrTypeRecSplitNotFound:
		return fmt.Sprintf("txHash %s not found in any RecSplit index", e.TxHash)
	case ErrTypeLedgerNotFound:
		return fmt.Sprintf("ledger %d not found in any RocksDB store", e.LedgerSeq)
	case ErrTypeFalsePositiveNormal:
		return fmt.Sprintf("txHash %s not found in ledger %d (scanned %d txs)", e.TxHash, e.LedgerSeq, e.TxsScanned)
	case ErrTypeFalsePositiveCompound:
		return fmt.Sprintf("txHash %s not found in any of ledgers %v", e.TxHash, e.LedgerSeqs)
	case ErrTypeFalsePositivePartial:
		return fmt.Sprintf("txHash %s found in ledger %d but false positives in %v", e.TxHash, e.SuccessLedger, e.LedgerSeqs)
	case ErrTypeLedgerInMultipleDBs:
		return fmt.Sprintf("ledger %d found in multiple DBs: %v", e.LedgerSeq, e.DBNames)
	case ErrTypeUnforeseen:
		if e.UnderlyingError != nil {
			return e.UnderlyingError.Error()
		}
		return "unknown error"
	default:
		return ""
	}
}

// =============================================================================
// TIMING STRUCTURES
// =============================================================================

// QueryTiming tracks timing for each step of a single query
type QueryTiming struct {
	RecSplitLookup time.Duration // Time spent searching RecSplit indexes
	RocksDBQuery   time.Duration // Time spent querying RocksDB
	Decompress     time.Duration // Zstd decompression time
	Unmarshal      time.Duration // XDR unmarshaling time
	TxReaderCreate time.Duration // Transaction reader creation
	TxSearch       time.Duration // Time to find matching hash in ledger
	TxExtract      time.Duration // Time to extract transaction data
	Total          time.Duration // Total end-to-end time
	TxCount        int           // Number of transactions scanned
}

// TimingAccumulator accumulates timing data for averaging
// All durations are stored as totals (not averages) for accurate weighted averaging
type TimingAccumulator struct {
	RecSplitLookup time.Duration
	RocksDBQuery   time.Duration
	Decompress     time.Duration
	Unmarshal      time.Duration
	TxReaderCreate time.Duration
	TxSearch       time.Duration
	TxExtract      time.Duration
	Total          time.Duration
	TxCount        int64 // Total transactions scanned across all queries
	QueryCount     int64 // Number of queries
}

// Add adds a QueryTiming to the accumulator
func (a *TimingAccumulator) Add(t QueryTiming) {
	a.RecSplitLookup += t.RecSplitLookup
	a.RocksDBQuery += t.RocksDBQuery
	a.Decompress += t.Decompress
	a.Unmarshal += t.Unmarshal
	a.TxReaderCreate += t.TxReaderCreate
	a.TxSearch += t.TxSearch
	a.TxExtract += t.TxExtract
	a.Total += t.Total
	a.TxCount += int64(t.TxCount)
	a.QueryCount++
}

// Merge merges another accumulator into this one
func (a *TimingAccumulator) Merge(other *TimingAccumulator) {
	a.RecSplitLookup += other.RecSplitLookup
	a.RocksDBQuery += other.RocksDBQuery
	a.Decompress += other.Decompress
	a.Unmarshal += other.Unmarshal
	a.TxReaderCreate += other.TxReaderCreate
	a.TxSearch += other.TxSearch
	a.TxExtract += other.TxExtract
	a.Total += other.Total
	a.TxCount += other.TxCount
	a.QueryCount += other.QueryCount
}

// Average returns the average timing per query
func (a *TimingAccumulator) Average() QueryTiming {
	if a.QueryCount == 0 {
		return QueryTiming{}
	}
	return QueryTiming{
		RecSplitLookup: time.Duration(int64(a.RecSplitLookup) / a.QueryCount),
		RocksDBQuery:   time.Duration(int64(a.RocksDBQuery) / a.QueryCount),
		Decompress:     time.Duration(int64(a.Decompress) / a.QueryCount),
		Unmarshal:      time.Duration(int64(a.Unmarshal) / a.QueryCount),
		TxReaderCreate: time.Duration(int64(a.TxReaderCreate) / a.QueryCount),
		TxSearch:       time.Duration(int64(a.TxSearch) / a.QueryCount),
		TxExtract:      time.Duration(int64(a.TxExtract) / a.QueryCount),
		Total:          time.Duration(int64(a.Total) / a.QueryCount),
		TxCount:        int(a.TxCount / a.QueryCount),
	}
}

// WorkerStats holds statistics for a single worker
type WorkerStats struct {
	Success               TimingAccumulator
	RecSplitNotFound      TimingAccumulator
	LedgerNotFound        TimingAccumulator
	FalsePositiveNormal   TimingAccumulator
	FalsePositiveCompound TimingAccumulator
	FalsePositivePartial  TimingAccumulator // Counted as success, but tracked separately
	LedgerInMultipleDBs   TimingAccumulator
	Unforeseen            TimingAccumulator
}

// GlobalStats holds aggregated statistics across all workers
type GlobalStats struct {
	mu sync.RWMutex
	WorkerStats

	// Counters for quick access without computing from accumulators
	TotalProcessed int64
	TotalSuccess   int64
	TotalFailed    int64
	InvalidInputs  int64 // Silently skipped, not counted in totals
}

// Merge merges worker stats into global stats (thread-safe)
func (g *GlobalStats) Merge(w *WorkerStats) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.Success.Merge(&w.Success)
	g.RecSplitNotFound.Merge(&w.RecSplitNotFound)
	g.LedgerNotFound.Merge(&w.LedgerNotFound)
	g.FalsePositiveNormal.Merge(&w.FalsePositiveNormal)
	g.FalsePositiveCompound.Merge(&w.FalsePositiveCompound)
	g.FalsePositivePartial.Merge(&w.FalsePositivePartial)
	g.LedgerInMultipleDBs.Merge(&w.LedgerInMultipleDBs)
	g.Unforeseen.Merge(&w.Unforeseen)

	// Update counters
	// Success includes FalsePositivePartial (it's a successful lookup)
	successCount := w.Success.QueryCount + w.FalsePositivePartial.QueryCount
	failedCount := w.RecSplitNotFound.QueryCount + w.LedgerNotFound.QueryCount +
		w.FalsePositiveNormal.QueryCount + w.FalsePositiveCompound.QueryCount +
		w.LedgerInMultipleDBs.QueryCount + w.Unforeseen.QueryCount

	g.TotalSuccess += successCount
	g.TotalFailed += failedCount
	g.TotalProcessed += successCount + failedCount
}

// Snapshot returns a copy of current stats for reporting
func (g *GlobalStats) Snapshot() GlobalStats {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return GlobalStats{
		WorkerStats:    g.WorkerStats,
		TotalProcessed: g.TotalProcessed,
		TotalSuccess:   g.TotalSuccess,
		TotalFailed:    g.TotalFailed,
		InvalidInputs:  g.InvalidInputs,
	}
}

// =============================================================================
// ROCKSDB STORE WITH RANGE
// =============================================================================

// RocksDBStore wraps a RocksDB instance with its ledger range
type RocksDBStore struct {
	DB        *grocksdb.DB
	Path      string
	Name      string // Just the directory name for logging
	MinLedger uint32
	MaxLedger uint32
}

// RocksDBStoreManager manages multiple RocksDB stores with shared resources
type RocksDBStoreManager struct {
	Stores     []*RocksDBStore
	ReadOpts   *grocksdb.ReadOptions
	BlockCache *grocksdb.Cache
	Options    *grocksdb.Options
}

// FindStoreForLedger finds the RocksDB store that contains the given ledger
// Returns nil if no store contains the ledger
func (m *RocksDBStoreManager) FindStoreForLedger(ledgerSeq uint32) *RocksDBStore {
	for _, store := range m.Stores {
		if ledgerSeq >= store.MinLedger && ledgerSeq <= store.MaxLedger {
			return store
		}
	}
	return nil
}

// FindAllStoresForLedger finds ALL stores that claim to contain the ledger
// Used to detect data inconsistency (ledger in multiple DBs)
func (m *RocksDBStoreManager) FindAllStoresForLedger(ledgerSeq uint32) []*RocksDBStore {
	var stores []*RocksDBStore
	for _, store := range m.Stores {
		if ledgerSeq >= store.MinLedger && ledgerSeq <= store.MaxLedger {
			stores = append(stores, store)
		}
	}
	return stores
}

// Close closes all stores and frees resources
func (m *RocksDBStoreManager) Close() {
	for _, store := range m.Stores {
		store.DB.Close()
	}
	if m.ReadOpts != nil {
		m.ReadOpts.Destroy()
	}
	if m.Options != nil {
		m.Options.Destroy()
	}
	// Note: BlockCache is destroyed when Options is destroyed
}

// =============================================================================
// RECSPLIT INDEX MANAGER
// =============================================================================

// RecSplitIndex wraps a RecSplit index with its reader
type RecSplitIndex struct {
	Index  *recsplit.Index
	Reader *recsplit.IndexReader
	Path   string
	Name   string // Just the filename for logging
}

// RecSplitManager manages multiple RecSplit indexes
type RecSplitManager struct {
	Indexes []*RecSplitIndex
}

// Close closes all indexes
func (m *RecSplitManager) Close() {
	for _, idx := range m.Indexes {
		idx.Reader.Close()
		idx.Index.Close()
	}
}

// =============================================================================
// WORKER AND BATCH STRUCTURES
// =============================================================================

// WorkItem represents a single transaction hash to process
type WorkItem struct {
	TxHashHex string
}

// BatchResult represents the result of processing a batch of transactions
type BatchResult struct {
	Stats        WorkerStats
	Errors       []*BenchmarkError // Errors to write to file
	InvalidCount int64             // Count of invalid inputs (skipped silently)
}

// =============================================================================
// BENCHMARK CONTEXT
// =============================================================================

// BenchmarkContext holds all shared resources for the benchmark
type BenchmarkContext struct {
	// Data stores
	RecSplitMgr *RecSplitManager
	RocksDBMgr  *RocksDBStoreManager

	// Configuration
	NetworkPassphrase string
	Parallelism       int

	// Channels
	WorkChan   chan []WorkItem  // Batches of work items
	ResultChan chan BatchResult // Results from workers

	// Global state
	Stats        *GlobalStats
	TotalTxCount int64 // Total transactions in input file (for progress)
	StartTime    time.Time

	// Synchronization
	Ctx        context.Context
	Cancel     context.CancelFunc
	WG         sync.WaitGroup
	ShutdownWG sync.WaitGroup // For graceful shutdown

	// Logging
	Logger    *log.Logger
	ErrorFile *bufio.Writer
	ErrorMu   sync.Mutex // Protects ErrorFile writes
}

// =============================================================================
// INITIALIZATION FUNCTIONS
// =============================================================================

// OpenRecSplitIndexes opens all RecSplit index files
func OpenRecSplitIndexes(paths []string) (*RecSplitManager, time.Duration, error) {
	start := time.Now()
	manager := &RecSplitManager{
		Indexes: make([]*RecSplitIndex, 0, len(paths)),
	}

	for _, path := range paths {
		idx, err := recsplit.OpenIndex(path)
		if err != nil {
			manager.Close()
			return nil, 0, fmt.Errorf("failed to open RecSplit index %s: %w", path, err)
		}

		reader := recsplit.NewIndexReader(idx)

		manager.Indexes = append(manager.Indexes, &RecSplitIndex{
			Index:  idx,
			Reader: reader,
			Path:   path,
			Name:   filepath.Base(path),
		})
	}

	return manager, time.Since(start), nil
}

// OpenRocksDBStores opens all RocksDB stores with shared block cache
func OpenRocksDBStores(paths []string, blockCacheSizeMB int) (*RocksDBStoreManager, time.Duration, error) {
	start := time.Now()

	// Create shared block cache
	// This is the recommended way to share memory across multiple RocksDB instances
	cache := grocksdb.NewLRUCache(uint64(blockCacheSizeMB) * 1024 * 1024)

	// Create shared options
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	// Configure block-based table options with shared cache
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(cache)
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10))
	bbto.SetCacheIndexAndFilterBlocks(true)
	opts.SetBlockBasedTableFactory(bbto)

	// Create read options (shared across all stores)
	readOpts := grocksdb.NewDefaultReadOptions()

	manager := &RocksDBStoreManager{
		Stores:     make([]*RocksDBStore, 0, len(paths)),
		ReadOpts:   readOpts,
		BlockCache: cache,
		Options:    opts,
	}

	for _, path := range paths {
		// Open in read-only mode
		db, err := grocksdb.OpenDbForReadOnly(opts, path, false)
		if err != nil {
			manager.Close()
			return nil, 0, fmt.Errorf("failed to open RocksDB %s: %w", path, err)
		}

		// Determine ledger range using SeekToFirst and SeekToLast
		minLedger, maxLedger, err := getLedgerRange(db, readOpts)
		if err != nil {
			db.Close()
			manager.Close()
			return nil, 0, fmt.Errorf("failed to get ledger range for %s: %w", path, err)
		}

		manager.Stores = append(manager.Stores, &RocksDBStore{
			DB:        db,
			Path:      path,
			Name:      filepath.Base(path),
			MinLedger: minLedger,
			MaxLedger: maxLedger,
		})
	}

	// Sort stores by MinLedger for efficient range searching
	sort.Slice(manager.Stores, func(i, j int) bool {
		return manager.Stores[i].MinLedger < manager.Stores[j].MinLedger
	})

	return manager, time.Since(start), nil
}

// getLedgerRange determines the min and max ledger sequence in a RocksDB store
func getLedgerRange(db *grocksdb.DB, readOpts *grocksdb.ReadOptions) (uint32, uint32, error) {
	iter := db.NewIterator(readOpts)
	defer iter.Close()

	// Get minimum ledger
	iter.SeekToFirst()
	if !iter.Valid() {
		return 0, 0, fmt.Errorf("empty database or iteration error")
	}
	minKey := iter.Key()
	if minKey.Size() != 4 {
		minKey.Free()
		return 0, 0, fmt.Errorf("invalid key size: expected 4, got %d", minKey.Size())
	}
	minLedger := binary.BigEndian.Uint32(minKey.Data())
	minKey.Free()

	// Get maximum ledger
	iter.SeekToLast()
	if !iter.Valid() {
		return 0, 0, fmt.Errorf("iteration error on SeekToLast")
	}
	maxKey := iter.Key()
	if maxKey.Size() != 4 {
		maxKey.Free()
		return 0, 0, fmt.Errorf("invalid key size: expected 4, got %d", maxKey.Size())
	}
	maxLedger := binary.BigEndian.Uint32(maxKey.Data())
	maxKey.Free()

	return minLedger, maxLedger, nil
}

// CountInputTransactions counts the total number of valid transactions in the input file
// This is used for progress reporting
func CountInputTransactions(inputPath string) (int64, error) {
	file, err := os.Open(inputPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var count int64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 64 && isValidHex(line) {
			count++
		}
	}

	return count, scanner.Err()
}

// isValidHex checks if a string is valid hexadecimal
func isValidHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// =============================================================================
// WORKER FUNCTION
// =============================================================================

// Worker processes batches of transaction hashes
func Worker(ctx *BenchmarkContext, workerID int) {
	defer ctx.WG.Done()

	// Create dedicated zstd decoder for this worker (avoids contention)
	zstdDecoder, err := zstd.NewReader(nil)
	if err != nil {
		ctx.Logger.Printf("Worker %d: Failed to create zstd decoder: %v", workerID, err)
		return
	}
	defer zstdDecoder.Close()

	for {
		select {
		case <-ctx.Ctx.Done():
			return
		case batch, ok := <-ctx.WorkChan:
			if !ok {
				return // Channel closed, exit
			}

			result := processBatch(ctx, zstdDecoder, batch)

			select {
			case ctx.ResultChan <- result:
			case <-ctx.Ctx.Done():
				return
			}
		}
	}
}

// processBatch processes a batch of transaction hashes and returns results
func processBatch(ctx *BenchmarkContext, zstdDecoder *zstd.Decoder, batch []WorkItem) BatchResult {
	result := BatchResult{
		Errors: make([]*BenchmarkError, 0),
	}

	for _, item := range batch {
		// Validate input
		if len(item.TxHashHex) != 64 || !isValidHex(item.TxHashHex) {
			result.InvalidCount++
			continue
		}

		timing, benchErr := processTransaction(ctx, zstdDecoder, item.TxHashHex)

		// Update stats based on result
		if benchErr == nil {
			result.Stats.Success.Add(timing)
		} else {
			result.Errors = append(result.Errors, benchErr)
			switch benchErr.Type {
			case ErrTypeRecSplitNotFound:
				result.Stats.RecSplitNotFound.Add(timing)
			case ErrTypeLedgerNotFound:
				result.Stats.LedgerNotFound.Add(timing)
			case ErrTypeFalsePositiveNormal:
				result.Stats.FalsePositiveNormal.Add(timing)
			case ErrTypeFalsePositiveCompound:
				result.Stats.FalsePositiveCompound.Add(timing)
			case ErrTypeFalsePositivePartial:
				// This is actually a success, but we track it separately
				result.Stats.FalsePositivePartial.Add(timing)
				result.Stats.Success.Add(timing) // Also count in success
			case ErrTypeLedgerInMultipleDBs:
				result.Stats.LedgerInMultipleDBs.Add(timing)
			case ErrTypeUnforeseen:
				result.Stats.Unforeseen.Add(timing)
			}
		}
	}

	return result
}

// processTransaction processes a single transaction hash
// Returns timing information and an error if the lookup failed
func processTransaction(ctx *BenchmarkContext, zstdDecoder *zstd.Decoder, txHashHex string) (QueryTiming, *BenchmarkError) {
	timing := QueryTiming{}
	totalStart := time.Now()

	// Parse transaction hash
	txHashBytes, err := hex.DecodeString(txHashHex)
	if err != nil {
		timing.Total = time.Since(totalStart)
		return timing, &BenchmarkError{
			Type:            ErrTypeUnforeseen,
			TxHash:          txHashHex,
			TotalDuration:   timing.Total,
			UnderlyingError: err,
		}
	}

	// Step 1: Search RecSplit indexes sequentially
	// Collect all potential matches (for false positive handling)
	t1 := time.Now()
	var candidateLedgers []uint32
	var candidateIndexes []string

	for _, idx := range ctx.RecSplitMgr.Indexes {
		ledgerSeqU64, found := idx.Reader.Lookup(txHashBytes)
		if found {
			candidateLedgers = append(candidateLedgers, uint32(ledgerSeqU64))
			candidateIndexes = append(candidateIndexes, idx.Name)
		}
	}
	timing.RecSplitLookup = time.Since(t1)

	// No matches found in any RecSplit index
	if len(candidateLedgers) == 0 {
		timing.Total = time.Since(totalStart)
		return timing, &BenchmarkError{
			Type:          ErrTypeRecSplitNotFound,
			TxHash:        txHashHex,
			TotalDuration: timing.Total,
		}
	}

	// Step 2: For each candidate ledger, try to find the transaction
	// Track which lookups succeeded and which were false positives
	var successLedger uint32
	var successTiming QueryTiming
	var falsePosLedgers []uint32
	var totalRocksDBTime, totalDecompTime, totalUnmarshalTime time.Duration
	var totalTxReaderTime, totalTxSearchTime, totalTxExtractTime time.Duration
	var totalTxCount int

	for i, ledgerSeq := range candidateLedgers {
		// Find the RocksDB store for this ledger
		stores := ctx.RocksDBMgr.FindAllStoresForLedger(ledgerSeq)

		if len(stores) == 0 {
			// Ledger not found in any store - this is unusual
			timing.Total = time.Since(totalStart)
			return timing, &BenchmarkError{
				Type:          ErrTypeLedgerNotFound,
				TxHash:        txHashHex,
				LedgerSeq:     ledgerSeq,
				TotalDuration: timing.Total,
			}
		}

		if len(stores) > 1 {
			// Data inconsistency: ledger in multiple stores
			dbNames := make([]string, len(stores))
			for j, s := range stores {
				dbNames[j] = s.Name
			}
			timing.Total = time.Since(totalStart)
			return timing, &BenchmarkError{
				Type:          ErrTypeLedgerInMultipleDBs,
				TxHash:        txHashHex,
				LedgerSeq:     ledgerSeq,
				DBNames:       dbNames,
				TotalDuration: timing.Total,
			}
		}

		store := stores[0]

		// Query RocksDB
		ledgerKey := make([]byte, 4)
		binary.BigEndian.PutUint32(ledgerKey, ledgerSeq)

		t2 := time.Now()
		slice, err := store.DB.Get(ctx.RocksDBMgr.ReadOpts, ledgerKey)
		rocksDBTime := time.Since(t2)
		totalRocksDBTime += rocksDBTime

		if err != nil {
			timing.Total = time.Since(totalStart)
			return timing, &BenchmarkError{
				Type:            ErrTypeUnforeseen,
				TxHash:          txHashHex,
				TotalDuration:   timing.Total,
				UnderlyingError: fmt.Errorf("RocksDB error: %w", err),
			}
		}

		if !slice.Exists() {
			slice.Free()
			// Key doesn't exist despite being in range - treat as ledger not found
			timing.Total = time.Since(totalStart)
			return timing, &BenchmarkError{
				Type:          ErrTypeLedgerNotFound,
				TxHash:        txHashHex,
				LedgerSeq:     ledgerSeq,
				TotalDuration: timing.Total,
			}
		}

		// Copy compressed data
		compressedLCM := make([]byte, len(slice.Data()))
		copy(compressedLCM, slice.Data())
		slice.Free()

		// Decompress
		t3 := time.Now()
		uncompressedLCM, err := zstdDecoder.DecodeAll(compressedLCM, nil)
		decompTime := time.Since(t3)
		totalDecompTime += decompTime

		if err != nil {
			timing.Total = time.Since(totalStart)
			return timing, &BenchmarkError{
				Type:            ErrTypeUnforeseen,
				TxHash:          txHashHex,
				TotalDuration:   timing.Total,
				UnderlyingError: fmt.Errorf("zstd decompress error: %w", err),
			}
		}

		// Unmarshal XDR
		t4 := time.Now()
		var lcm xdr.LedgerCloseMeta
		err = lcm.UnmarshalBinary(uncompressedLCM)
		unmarshalTime := time.Since(t4)
		totalUnmarshalTime += unmarshalTime

		if err != nil {
			timing.Total = time.Since(totalStart)
			return timing, &BenchmarkError{
				Type:            ErrTypeUnforeseen,
				TxHash:          txHashHex,
				TotalDuration:   timing.Total,
				UnderlyingError: fmt.Errorf("XDR unmarshal error: %w", err),
			}
		}

		// Create transaction reader
		t5 := time.Now()
		txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ctx.NetworkPassphrase, lcm)
		txReaderTime := time.Since(t5)
		totalTxReaderTime += txReaderTime

		if err != nil {
			timing.Total = time.Since(totalStart)
			return timing, &BenchmarkError{
				Type:            ErrTypeUnforeseen,
				TxHash:          txHashHex,
				TotalDuration:   timing.Total,
				UnderlyingError: fmt.Errorf("tx reader error: %w", err),
			}
		}

		// Search for transaction
		t6 := time.Now()
		var found bool
		txCount := 0
		txHashLower := strings.ToLower(txHashHex)

		for {
			tx, err := txReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				txReader.Close()
				timing.Total = time.Since(totalStart)
				return timing, &BenchmarkError{
					Type:            ErrTypeUnforeseen,
					TxHash:          txHashHex,
					TotalDuration:   timing.Total,
					UnderlyingError: fmt.Errorf("tx read error: %w", err),
				}
			}
			txCount++
			if tx.Hash.HexString() == txHashLower {
				found = true
				// Extract transaction data timing
				t7 := time.Now()
				// We don't actually need the data, just measure the extraction time
				_, _ = tx.Envelope.MarshalBinary()
				_, _ = tx.Result.MarshalBinary()
				_, _ = tx.UnsafeMeta.MarshalBinary()
				totalTxExtractTime += time.Since(t7)
				break
			}
		}
		txReader.Close()

		txSearchTime := time.Since(t6)
		totalTxSearchTime += txSearchTime
		totalTxCount += txCount

		if found {
			successLedger = ledgerSeq
			successTiming = QueryTiming{
				RecSplitLookup: timing.RecSplitLookup / time.Duration(i+1), // Amortize over lookups so far
				RocksDBQuery:   rocksDBTime,
				Decompress:     decompTime,
				Unmarshal:      unmarshalTime,
				TxReaderCreate: txReaderTime,
				TxSearch:       txSearchTime,
				TxExtract:      totalTxExtractTime,
				TxCount:        txCount,
			}
		} else {
			falsePosLedgers = append(falsePosLedgers, ledgerSeq)
		}
	}

	// Determine result based on what we found
	timing.RecSplitLookup = timing.RecSplitLookup // Already set
	timing.RocksDBQuery = totalRocksDBTime
	timing.Decompress = totalDecompTime
	timing.Unmarshal = totalUnmarshalTime
	timing.TxReaderCreate = totalTxReaderTime
	timing.TxSearch = totalTxSearchTime
	timing.TxExtract = totalTxExtractTime
	timing.TxCount = totalTxCount
	timing.Total = time.Since(totalStart)

	// Case 1: Found transaction (possibly with false positives from other indexes)
	if successLedger > 0 {
		if len(falsePosLedgers) > 0 {
			// FALSE_POSITIVE_PARTIAL: Found in one, false positives in others
			return timing, &BenchmarkError{
				Type:          ErrTypeFalsePositivePartial,
				TxHash:        txHashHex,
				SuccessLedger: successLedger,
				LedgerSeqs:    append([]uint32{successLedger}, falsePosLedgers...),
				TotalDuration: timing.Total,
			}
		}
		// Clean success
		return timing, nil
	}

	// Case 2: No success, all were false positives
	if len(candidateLedgers) == 1 {
		// FALSE_POSITIVE_NORMAL: Single false positive
		return timing, &BenchmarkError{
			Type:          ErrTypeFalsePositiveNormal,
			TxHash:        txHashHex,
			LedgerSeq:     candidateLedgers[0],
			TxsScanned:    totalTxCount,
			TotalDuration: timing.Total,
		}
	}

	// FALSE_POSITIVE_COMPOUND: Multiple false positives
	return timing, &BenchmarkError{
		Type:          ErrTypeFalsePositiveCompound,
		TxHash:        txHashHex,
		LedgerSeqs:    candidateLedgers,
		TotalDuration: timing.Total,
	}
}

// =============================================================================
// PRODUCER FUNCTION
// =============================================================================

// Producer reads transaction hashes from the input file and sends them to workers
func Producer(ctx *BenchmarkContext, inputPath string) {
	defer close(ctx.WorkChan)

	file, err := os.Open(inputPath)
	if err != nil {
		ctx.Logger.Printf("Failed to open input file: %v", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	batch := make([]WorkItem, 0, WorkerBatchSize)

	for scanner.Scan() {
		select {
		case <-ctx.Ctx.Done():
			return
		default:
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		batch = append(batch, WorkItem{TxHashHex: line})

		if len(batch) >= WorkerBatchSize {
			select {
			case ctx.WorkChan <- batch:
				batch = make([]WorkItem, 0, WorkerBatchSize)
			case <-ctx.Ctx.Done():
				return
			}
		}
	}

	// Send remaining items
	if len(batch) > 0 {
		select {
		case ctx.WorkChan <- batch:
		case <-ctx.Ctx.Done():
		}
	}

	if err := scanner.Err(); err != nil {
		ctx.Logger.Printf("Error reading input file: %v", err)
	}
}

// =============================================================================
// STATS AGGREGATOR
// =============================================================================

// StatsAggregator collects results from workers and reports progress
func StatsAggregator(ctx *BenchmarkContext) {
	defer ctx.ShutdownWG.Done()

	lastReportCount := int64(0)
	reportInterval := ctx.TotalTxCount / 100 // ~1%
	if reportInterval < 1 {
		reportInterval = 1
	}

	errorBuffer := make([]*BenchmarkError, 0, ErrorFileFlushInterval)

	flushErrors := func() {
		if len(errorBuffer) == 0 {
			return
		}
		ctx.ErrorMu.Lock()
		for _, err := range errorBuffer {
			ctx.ErrorFile.WriteString(err.FormatForLog() + "\n")
		}
		ctx.ErrorFile.Flush()
		ctx.ErrorMu.Unlock()
		errorBuffer = errorBuffer[:0]
	}

	for {
		select {
		case result, ok := <-ctx.ResultChan:
			if !ok {
				// Channel closed, flush remaining errors and return
				flushErrors()
				return
			}

			// Merge stats
			ctx.Stats.Merge(&result.Stats)
			atomic.AddInt64(&ctx.Stats.InvalidInputs, result.InvalidCount)

			// Buffer errors
			errorBuffer = append(errorBuffer, result.Errors...)
			if len(errorBuffer) >= ErrorFileFlushInterval {
				flushErrors()
			}

			// Check if we should report progress
			currentCount := atomic.LoadInt64(&ctx.Stats.TotalProcessed)
			if currentCount-lastReportCount >= reportInterval {
				printProgress(ctx)
				lastReportCount = currentCount
			}

		case <-ctx.Ctx.Done():
			// Graceful shutdown - drain remaining results
			for result := range ctx.ResultChan {
				ctx.Stats.Merge(&result.Stats)
				atomic.AddInt64(&ctx.Stats.InvalidInputs, result.InvalidCount)
				errorBuffer = append(errorBuffer, result.Errors...)
			}
			flushErrors()
			return
		}
	}
}

// =============================================================================
// PROGRESS REPORTING
// =============================================================================

func printProgress(ctx *BenchmarkContext) {
	stats := ctx.Stats.Snapshot()
	elapsed := time.Since(ctx.StartTime)

	// Calculate progress
	processed := stats.TotalProcessed
	total := ctx.TotalTxCount
	percentage := float64(processed) / float64(total) * 100

	// Calculate ETA
	var eta time.Duration
	if processed > 0 {
		avgPerTx := elapsed / time.Duration(processed)
		remaining := total - processed
		eta = avgPerTx * time.Duration(remaining)
	}

	// Calculate throughput
	throughput := float64(processed) / elapsed.Seconds()

	// Format timestamp
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	ctx.Logger.Printf("%s | Progress: %s/%s (%.2f%%) | Elapsed: %s | ETA: %s",
		timestamp,
		helpers.FormatNumber(processed),
		helpers.FormatNumber(total),
		percentage,
		helpers.FormatDuration(elapsed),
		helpers.FormatDuration(eta))

	ctx.Logger.Printf("%s | Throughput: %.2f/s | Success: %s | Failed: %s",
		timestamp,
		throughput,
		helpers.FormatNumber(stats.TotalSuccess),
		helpers.FormatNumber(stats.TotalFailed))

	// Print successful query breakdown
	if stats.Success.QueryCount > 0 {
		avg := stats.Success.Average()
		ctx.Logger.Printf("%s | Avg Successful Query Breakdown:", timestamp)
		ctx.Logger.Printf("%s |   RecSplit: %s | RocksDB: %s | Decompress: %s",
			timestamp,
			helpers.FormatDuration(avg.RecSplitLookup),
			helpers.FormatDuration(avg.RocksDBQuery),
			helpers.FormatDuration(avg.Decompress))
		ctx.Logger.Printf("%s |   Unmarshal: %s | TxSearch: %s | TxExtract: %s",
			timestamp,
			helpers.FormatDuration(avg.Unmarshal),
			helpers.FormatDuration(avg.TxSearch),
			helpers.FormatDuration(avg.TxExtract))
		ctx.Logger.Printf("%s |   Total: %s",
			timestamp,
			helpers.FormatDuration(avg.Total))
	}

	// Print failed query breakdown
	if stats.TotalFailed > 0 {
		ctx.Logger.Printf("%s | Avg Failed Query Breakdown:", timestamp)
		printFailureStats(ctx.Logger, timestamp, "RECSPLIT_NOT_FOUND", &stats.RecSplitNotFound)
		printFailureStats(ctx.Logger, timestamp, "LEDGER_NOT_FOUND", &stats.LedgerNotFound)
		printFailureStats(ctx.Logger, timestamp, "FALSE_POSITIVE_NORMAL", &stats.FalsePositiveNormal)
		printFailureStats(ctx.Logger, timestamp, "FALSE_POSITIVE_COMPOUND", &stats.FalsePositiveCompound)
		printFailureStats(ctx.Logger, timestamp, "LEDGER_IN_MULTIPLE_DBS", &stats.LedgerInMultipleDBs)
		printFailureStats(ctx.Logger, timestamp, "UNFORESEEN", &stats.Unforeseen)
	}

	// Note about FALSE_POSITIVE_PARTIAL (logged but counted as success)
	if stats.FalsePositivePartial.QueryCount > 0 {
		ctx.Logger.Printf("%s | Note: %d FALSE_POSITIVE_PARTIAL (counted in success, avg %s)",
			timestamp,
			stats.FalsePositivePartial.QueryCount,
			helpers.FormatDuration(stats.FalsePositivePartial.Average().Total))
	}

	ctx.Logger.Println()
}

func printFailureStats(logger *log.Logger, timestamp, name string, acc *TimingAccumulator) {
	if acc.QueryCount > 0 {
		avg := acc.Average()
		logger.Printf("%s |   %s: %d (avg %s)",
			timestamp, name, acc.QueryCount, helpers.FormatDuration(avg.Total))
	}
}

// =============================================================================
// FINAL SUMMARY
// =============================================================================

// FinalSummary holds the final benchmark results
type FinalSummary struct {
	// Basic stats
	TotalTransactions int64         `json:"total_transactions"`
	TotalSuccess      int64         `json:"total_success"`
	TotalFailed       int64         `json:"total_failed"`
	InvalidInputs     int64         `json:"invalid_inputs"`
	WallClockTime     time.Duration `json:"wall_clock_time_ns"`
	WallClockTimeStr  string        `json:"wall_clock_time"`
	Throughput        float64       `json:"throughput_per_second"`

	// Success breakdown
	SuccessAvg           QueryTimingJSON `json:"success_avg"`
	FalsePositivePartial QueryTimingJSON `json:"false_positive_partial_avg,omitempty"`

	// Failure breakdown
	Failures map[string]FailureStats `json:"failures"`
}

type QueryTimingJSON struct {
	RecSplitLookup string `json:"recsplit_lookup"`
	RocksDBQuery   string `json:"rocksdb_query"`
	Decompress     string `json:"decompress"`
	Unmarshal      string `json:"unmarshal"`
	TxReaderCreate string `json:"tx_reader_create"`
	TxSearch       string `json:"tx_search"`
	TxExtract      string `json:"tx_extract"`
	Total          string `json:"total"`
	AvgTxScanned   int    `json:"avg_tx_scanned"`
}

type FailureStats struct {
	Count   int64  `json:"count"`
	AvgTime string `json:"avg_time"`
}

func timingToJSON(t QueryTiming) QueryTimingJSON {
	return QueryTimingJSON{
		RecSplitLookup: helpers.FormatDuration(t.RecSplitLookup),
		RocksDBQuery:   helpers.FormatDuration(t.RocksDBQuery),
		Decompress:     helpers.FormatDuration(t.Decompress),
		Unmarshal:      helpers.FormatDuration(t.Unmarshal),
		TxReaderCreate: helpers.FormatDuration(t.TxReaderCreate),
		TxSearch:       helpers.FormatDuration(t.TxSearch),
		TxExtract:      helpers.FormatDuration(t.TxExtract),
		Total:          helpers.FormatDuration(t.Total),
		AvgTxScanned:   t.TxCount,
	}
}

func printFinalSummary(ctx *BenchmarkContext) {
	stats := ctx.Stats.Snapshot()
	elapsed := time.Since(ctx.StartTime)

	ctx.Logger.Println()
	ctx.Logger.Println("================================================================================")
	ctx.Logger.Println("FINAL BENCHMARK SUMMARY")
	ctx.Logger.Println("================================================================================")
	ctx.Logger.Println()

	// Basic statistics
	ctx.Logger.Printf("Total Transactions:    %s", helpers.FormatNumber(stats.TotalProcessed))
	ctx.Logger.Printf("  Successful:          %s (%.2f%%)",
		helpers.FormatNumber(stats.TotalSuccess),
		float64(stats.TotalSuccess)/float64(stats.TotalProcessed)*100)
	ctx.Logger.Printf("  Failed:              %s (%.2f%%)",
		helpers.FormatNumber(stats.TotalFailed),
		float64(stats.TotalFailed)/float64(stats.TotalProcessed)*100)
	ctx.Logger.Printf("  Invalid (skipped):   %s", helpers.FormatNumber(stats.InvalidInputs))
	ctx.Logger.Println()

	ctx.Logger.Printf("Wall Clock Time:       %s", helpers.FormatDuration(elapsed))
	ctx.Logger.Printf("Throughput:            %.2f tx/s", float64(stats.TotalProcessed)/elapsed.Seconds())
	ctx.Logger.Println()

	// Successful queries breakdown
	ctx.Logger.Println("=== SUCCESSFUL QUERIES ===")
	if stats.Success.QueryCount > 0 {
		avg := stats.Success.Average()
		ctx.Logger.Printf("Count: %s", helpers.FormatNumber(stats.Success.QueryCount))
		ctx.Logger.Println("Average Timing Breakdown:")
		ctx.Logger.Printf("  RecSplit Lookup:     %s", helpers.FormatDuration(avg.RecSplitLookup))
		ctx.Logger.Printf("  RocksDB Query:       %s", helpers.FormatDuration(avg.RocksDBQuery))
		ctx.Logger.Printf("  Decompress (zstd):   %s", helpers.FormatDuration(avg.Decompress))
		ctx.Logger.Printf("  Unmarshal (XDR):     %s", helpers.FormatDuration(avg.Unmarshal))
		ctx.Logger.Printf("  TxReader Create:     %s", helpers.FormatDuration(avg.TxReaderCreate))
		ctx.Logger.Printf("  TxSearch (%d avg):   %s", avg.TxCount, helpers.FormatDuration(avg.TxSearch))
		ctx.Logger.Printf("  TxExtract:           %s", helpers.FormatDuration(avg.TxExtract))
		ctx.Logger.Printf("  TOTAL:               %s", helpers.FormatDuration(avg.Total))
	} else {
		ctx.Logger.Println("No successful queries")
	}
	ctx.Logger.Println()

	// False positive partial (counted as success but noteworthy)
	if stats.FalsePositivePartial.QueryCount > 0 {
		ctx.Logger.Println("=== FALSE POSITIVE PARTIAL (counted as success) ===")
		avg := stats.FalsePositivePartial.Average()
		ctx.Logger.Printf("Count: %s", helpers.FormatNumber(stats.FalsePositivePartial.QueryCount))
		ctx.Logger.Printf("Average Total Time: %s", helpers.FormatDuration(avg.Total))
		ctx.Logger.Println()
	}

	// Failed queries breakdown
	ctx.Logger.Println("=== FAILED QUERIES ===")
	printFinalFailureStats(ctx.Logger, "RECSPLIT_NOT_FOUND", &stats.RecSplitNotFound)
	printFinalFailureStats(ctx.Logger, "LEDGER_NOT_FOUND", &stats.LedgerNotFound)
	printFinalFailureStats(ctx.Logger, "FALSE_POSITIVE_NORMAL", &stats.FalsePositiveNormal)
	printFinalFailureStats(ctx.Logger, "FALSE_POSITIVE_COMPOUND", &stats.FalsePositiveCompound)
	printFinalFailureStats(ctx.Logger, "LEDGER_IN_MULTIPLE_DBS", &stats.LedgerInMultipleDBs)
	printFinalFailureStats(ctx.Logger, "UNFORESEEN_ERROR", &stats.Unforeseen)
	ctx.Logger.Println()

	// Memory stats
	printMemStats(ctx.Logger)

	ctx.Logger.Println("================================================================================")

	// JSON Summary
	summary := buildJSONSummary(stats, elapsed)
	jsonBytes, err := json.MarshalIndent(summary, "", "  ")
	if err == nil {
		ctx.Logger.Println()
		ctx.Logger.Println("JSON SUMMARY:")
		ctx.Logger.Println(string(jsonBytes))
	}

	ctx.Logger.Println("================================================================================")
}

func printFinalFailureStats(logger *log.Logger, name string, acc *TimingAccumulator) {
	if acc.QueryCount > 0 {
		avg := acc.Average()
		logger.Printf("%s:", name)
		logger.Printf("  Count:             %s", helpers.FormatNumber(acc.QueryCount))
		logger.Printf("  Average Time:      %s", helpers.FormatDuration(avg.Total))
	}
}

func buildJSONSummary(stats GlobalStats, elapsed time.Duration) FinalSummary {
	summary := FinalSummary{
		TotalTransactions: stats.TotalProcessed,
		TotalSuccess:      stats.TotalSuccess,
		TotalFailed:       stats.TotalFailed,
		InvalidInputs:     stats.InvalidInputs,
		WallClockTime:     elapsed,
		WallClockTimeStr:  helpers.FormatDuration(elapsed),
		Throughput:        float64(stats.TotalProcessed) / elapsed.Seconds(),
		Failures:          make(map[string]FailureStats),
	}

	if stats.Success.QueryCount > 0 {
		summary.SuccessAvg = timingToJSON(stats.Success.Average())
	}

	if stats.FalsePositivePartial.QueryCount > 0 {
		summary.FalsePositivePartial = timingToJSON(stats.FalsePositivePartial.Average())
	}

	addFailureStat := func(name string, acc *TimingAccumulator) {
		if acc.QueryCount > 0 {
			summary.Failures[name] = FailureStats{
				Count:   acc.QueryCount,
				AvgTime: helpers.FormatDuration(acc.Average().Total),
			}
		}
	}

	addFailureStat("RECSPLIT_NOT_FOUND", &stats.RecSplitNotFound)
	addFailureStat("LEDGER_NOT_FOUND", &stats.LedgerNotFound)
	addFailureStat("FALSE_POSITIVE_NORMAL", &stats.FalsePositiveNormal)
	addFailureStat("FALSE_POSITIVE_COMPOUND", &stats.FalsePositiveCompound)
	addFailureStat("LEDGER_IN_MULTIPLE_DBS", &stats.LedgerInMultipleDBs)
	addFailureStat("UNFORESEEN_ERROR", &stats.Unforeseen)

	return summary
}

func printMemStats(logger *log.Logger) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	logger.Println("Memory Usage:")
	logger.Printf("  Alloc:       %s", helpers.FormatBytes(int64(m.Alloc)))
	logger.Printf("  HeapInuse:   %s", helpers.FormatBytes(int64(m.HeapInuse)))
	logger.Printf("  Sys:         %s", helpers.FormatBytes(int64(m.Sys)))
	logger.Printf("  NumGC:       %d", m.NumGC)
}

// =============================================================================
// INITIALIZATION SUMMARY
// =============================================================================

func printInitSummary(
	logger *log.Logger,
	rsMgr *RecSplitManager, rsLoadTime time.Duration,
	dbMgr *RocksDBStoreManager, dbLoadTime time.Duration,
	totalTxCount int64, countTime time.Duration,
	config *Config,
) {
	logger.Println()
	logger.Println("================================================================================")
	logger.Println("Stellar Transaction Benchmark - Initialization")
	logger.Println("================================================================================")
	logger.Println()

	// RecSplit indexes
	logger.Printf("RecSplit Indexes (%d files):", len(rsMgr.Indexes))
	logger.Printf("  Load time:           %s", helpers.FormatDuration(rsLoadTime))
	var totalKeys uint64
	for _, idx := range rsMgr.Indexes {
		totalKeys += idx.Index.KeyCount()
		logger.Printf("  - %s: %s keys, %s",
			idx.Name,
			helpers.FormatNumber(int64(idx.Index.KeyCount())),
			helpers.FormatBytes(idx.Index.Size()))
	}
	logger.Printf("  Total keys:          %s", helpers.FormatNumber(int64(totalKeys)))
	logger.Println()

	// RocksDB stores
	logger.Printf("RocksDB Stores (%d databases):", len(dbMgr.Stores))
	logger.Printf("  Load time:           %s", helpers.FormatDuration(dbLoadTime))
	logger.Printf("  Shared cache:        %d MB", config.ReadBufferMB)
	for _, store := range dbMgr.Stores {
		logger.Printf("  - %s: ledgers %d - %d",
			store.Name, store.MinLedger, store.MaxLedger)
	}
	logger.Println()

	// Input file
	logger.Printf("Input File:")
	logger.Printf("  Path:                %s", config.InputFile)
	logger.Printf("  Transaction count:   %s", helpers.FormatNumber(totalTxCount))
	logger.Printf("  Count time:          %s", helpers.FormatDuration(countTime))
	logger.Println()

	// Configuration
	logger.Printf("Configuration:")
	logger.Printf("  Parallelism:         %d workers", config.Parallelism)
	logger.Printf("  Worker batch size:   %d", WorkerBatchSize)
	logger.Printf("  Network passphrase:  %s", config.NetworkPassphrase)
	logger.Println()

	logger.Println("================================================================================")
	logger.Println()
}

// =============================================================================
// CONFIG AND MAIN
// =============================================================================

type Config struct {
	InputFile         string
	RecSplitFiles     []string
	RocksDBPaths      []string
	ErrorFile         string
	LogFile           string
	ReadBufferMB      int
	Parallelism       int
	NetworkPassphrase string
}

func parseFlags() (*Config, error) {
	var (
		inputFile         string
		recsplitFiles     string
		rocksdbPaths      string
		errorFile         string
		logFile           string
		readBufferMB      int
		parallelism       int
		networkPassphrase string
	)

	flag.StringVar(&inputFile, "input-file", "",
		"Path to input file containing transaction hashes (one per line, hex encoded)")
	flag.StringVar(&recsplitFiles, "recsplit-index-files", "",
		"Comma-separated list of RecSplit index files")
	flag.StringVar(&rocksdbPaths, "ledger-seq-to-lcm-dbs", "",
		"Comma-separated list of RocksDB database paths")
	flag.StringVar(&errorFile, "error-file", "",
		"Path to error output file (required)")
	flag.StringVar(&logFile, "log-file", "",
		"Path to log file (optional, defaults to stdout)")
	flag.IntVar(&readBufferMB, "read-buffer-mb", 16384,
		"Shared RocksDB block cache size in MB")
	flag.IntVar(&parallelism, "parallelism", 16,
		"Number of parallel worker goroutines")
	flag.StringVar(&networkPassphrase, "network-passphrase", network.PublicNetworkPassphrase,
		"Stellar network passphrase")

	flag.Parse()

	// Validate required flags
	if inputFile == "" {
		return nil, fmt.Errorf("--input-file is required")
	}
	if recsplitFiles == "" {
		return nil, fmt.Errorf("--recsplit-index-files is required")
	}
	if rocksdbPaths == "" {
		return nil, fmt.Errorf("--ledger-seq-to-lcm-dbs is required")
	}
	if errorFile == "" {
		return nil, fmt.Errorf("--error-file is required")
	}
	if parallelism < 1 {
		return nil, fmt.Errorf("--parallelism must be at least 1")
	}
	if readBufferMB < 1 {
		return nil, fmt.Errorf("--read-buffer-mb must be at least 1")
	}

	// Parse file lists
	rsFiles := strings.Split(recsplitFiles, ",")
	dbPaths := strings.Split(rocksdbPaths, ",")

	// Trim whitespace
	for i := range rsFiles {
		rsFiles[i] = strings.TrimSpace(rsFiles[i])
	}
	for i := range dbPaths {
		dbPaths[i] = strings.TrimSpace(dbPaths[i])
	}

	// Validate files exist
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		return nil, fmt.Errorf("input file does not exist: %s", inputFile)
	}
	for _, f := range rsFiles {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			return nil, fmt.Errorf("RecSplit file does not exist: %s", f)
		}
	}
	for _, p := range dbPaths {
		if _, err := os.Stat(p); os.IsNotExist(err) {
			return nil, fmt.Errorf("RocksDB path does not exist: %s", p)
		}
	}

	return &Config{
		InputFile:         inputFile,
		RecSplitFiles:     rsFiles,
		RocksDBPaths:      dbPaths,
		ErrorFile:         errorFile,
		LogFile:           logFile,
		ReadBufferMB:      readBufferMB,
		Parallelism:       parallelism,
		NetworkPassphrase: networkPassphrase,
	}, nil
}

func main() {
	config, err := parseFlags()
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	// Set up logger
	var logger *log.Logger
	if config.LogFile != "" {
		logFile, err := os.Create(config.LogFile)
		if err != nil {
			log.Fatalf("Failed to create log file: %v", err)
		}
		defer logFile.Close()
		logger = log.New(logFile, "", 0)
	} else {
		logger = log.New(os.Stdout, "", 0)
	}

	// Set up error file
	errorFile, err := os.Create(config.ErrorFile)
	if err != nil {
		log.Fatalf("Failed to create error file: %v", err)
	}
	defer errorFile.Close()
	errorWriter := bufio.NewWriter(errorFile)
	defer errorWriter.Flush()

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Open RecSplit indexes
	logger.Println("Opening RecSplit indexes...")
	rsMgr, rsLoadTime, err := OpenRecSplitIndexes(config.RecSplitFiles)
	if err != nil {
		log.Fatalf("Failed to open RecSplit indexes: %v", err)
	}
	defer rsMgr.Close()

	// Open RocksDB stores
	logger.Println("Opening RocksDB stores...")
	dbMgr, dbLoadTime, err := OpenRocksDBStores(config.RocksDBPaths, config.ReadBufferMB)
	if err != nil {
		log.Fatalf("Failed to open RocksDB stores: %v", err)
	}
	defer dbMgr.Close()

	// Count input transactions
	logger.Println("Counting input transactions...")
	countStart := time.Now()
	totalTxCount, err := CountInputTransactions(config.InputFile)
	if err != nil {
		log.Fatalf("Failed to count input transactions: %v", err)
	}
	countTime := time.Since(countStart)

	if totalTxCount == 0 {
		log.Fatalf("No valid transactions found in input file")
	}

	// Print initialization summary
	printInitSummary(logger, rsMgr, rsLoadTime, dbMgr, dbLoadTime, totalTxCount, countTime, config)

	// Create benchmark context
	benchCtx := &BenchmarkContext{
		RecSplitMgr:       rsMgr,
		RocksDBMgr:        dbMgr,
		NetworkPassphrase: config.NetworkPassphrase,
		Parallelism:       config.Parallelism,
		WorkChan:          make(chan []WorkItem, config.Parallelism*ChannelBufferMultiplier),
		ResultChan:        make(chan BatchResult, config.Parallelism*ChannelBufferMultiplier),
		Stats:             &GlobalStats{},
		TotalTxCount:      totalTxCount,
		Ctx:               ctx,
		Cancel:            cancel,
		Logger:            logger,
		ErrorFile:         errorWriter,
	}

	// Start workers
	logger.Printf("Starting %d workers...", config.Parallelism)
	for i := 0; i < config.Parallelism; i++ {
		benchCtx.WG.Add(1)
		go Worker(benchCtx, i)
	}

	// Start stats aggregator
	benchCtx.ShutdownWG.Add(1)
	go StatsAggregator(benchCtx)

	// Record start time
	benchCtx.StartTime = time.Now()
	logger.Println()
	logger.Printf("Benchmark started at %s", benchCtx.StartTime.Format("2006-01-02 15:04:05"))
	logger.Println()

	// Handle signals in a separate goroutine
	go func() {
		select {
		case <-sigChan:
			logger.Println()
			logger.Println("Received interrupt signal, finishing current batches...")
			cancel()
		case <-ctx.Done():
		}
	}()

	// Start producer (blocks until done or cancelled)
	Producer(benchCtx, config.InputFile)

	// Wait for workers to finish
	benchCtx.WG.Wait()

	// Close result channel to signal stats aggregator
	close(benchCtx.ResultChan)

	// Wait for stats aggregator to finish
	benchCtx.ShutdownWG.Wait()

	// Print final summary
	printFinalSummary(benchCtx)

	logger.Printf("Benchmark completed at %s", time.Now().Format("2006-01-02 15:04:05"))
}
