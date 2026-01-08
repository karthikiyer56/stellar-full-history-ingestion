package main

// =============================================================================
// Stellar Transaction Hash Lookup Benchmark Tool
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
//   - Parallel RecSplit batch lookups (default batch size 4) within each worker
//   - Shared LRU block cache across all RocksDB instances
//   - Range-based RocksDB store selection (pre-indexed at startup)
//   - Lazy sequential processing: stop on first successful transaction find
//
// DATA FLOW:
//   1. Main thread reads input file, pushes txHashes to work channel
//   2. Workers pull batches (1k txs), process them, push results to results channel
//   3. Stats aggregator collects results, prints progress at ~1% intervals
//   4. Final summary printed at completion
//
// RECSPLIT PARALLEL BATCH LOOKUP:
//   For each transaction, we search RecSplit files in parallel batches:
//   - Batch 1: Search files 0-3 in parallel, wait for all to complete
//   - If any match found, try to find tx in that ledger
//   - If success, return immediately (early termination)
//   - If false positive, continue to next batch
//   - Repeat until success or all files exhausted
//
// FALSE POSITIVE HANDLING:
//   RecSplit's Perfect Hash Function can return false positives.
//   This tool handles multiple scenarios:
//   - FALSE_POSITIVE_NORMAL: Single RecSplit returns wrong ledger
//   - FALSE_POSITIVE_COMPOUND: Multiple RecSplits return wrong ledgers (all false)
//   - FALSE_POSITIVE_PARTIAL: Found in one, false positive in others (counts as success)
//
//		Explanation with examples:
//			Premise:
//				I have 10 recsplit index files
//				and correspondingly 10 rocksdb compressed lcm stores - one for each year since 2016.
//				I am searching for txHash - abc123
//
//			FALSE_POSITIVE_NORMAL:
//				I found abc123 in "exactly one recsplit file out of 10", and it returned ledger 555
//				I parsed all transactions in 555 and concluded that abc123 doesnt exist irl.
//				I call this a single(normal) false positive.
//				This will accrue almost the same timing as a "successfully found" lookup, since you end up doing - rockdb lookup + uncompress + parse etc.
//
//			FALSE_POSITIVE_COMPOUND:
//				I found abc123 in "2 out of 10 recsplit files". they returned - 555, 666 respectively
//				I parsed all transactions in each of the 2 ledgers, and concluded that abc123 doesnt exist irl
//				I call this a rather expensive(compound) false positive
//				This will take atleast 2x the time as a "successfully found lookup",
//				assuming that you find the same tx in 2 stores on average when this particular flavor of error arises.
//				If you are out of luck, it could also appear in 3 or 4 ledgers :shrug:
//
//			FALSE_POSITIVE_PARTIAL:
//				I found abc123 in "2 out of 10 recsplit files". they returned - 555, 666 respectively.
//				I parsed all transactions in 555 and concluded that abc123 doesnt exist in it.
//				However, abc123 does exist in 666. So, overall, this qualifies as a successful lookup. As in, txHash exists irl
//				To deduce that it is truly successful though, I MUST CHECK all ledgers that are returned by the recsplit lookups.
//				If I fail at the first false positive, I might never find the txHash if it really does exist in some other index
//				Even though, this is a successful lookup, I am logging this in the error case as well, to gather logs to see how many of such things show up.
//				Couldnt come up with an appropriate name. Hence the _PARTIAL suffix.
//				This will take atleast 2x the time as a "successfully found lookup",
//				assuming that you find the same tx in 2 stores on average when this particular flavor arises - one for truly successful tx and one for false positive
//				If you are out of luck, it could also appear in 3 or 4 ledgers :shrug:

//

// TIMING CALCULATION FOR PARALLEL BATCHES:
//   When searching RecSplit files in parallel batches:
//   - Each batch's time = average of individual lookup times in that batch
//   - Total RecSplit time = average of all batch times
//   Example: Batch1 (50,60,40,70us) avg=55us, Batch2 (45,55,50,48us) avg=49.5us
//            Final RecSplit time = (55+49.5)/2 = 52.25us
//
// USAGE:
//   ./benchmark \
//     --input-file /path/to/txhashes.txt \
//     --recsplit-index-files /path/to/2014.idx,/path/to/2015.idx,... \
//     --ledger-seq-to-lcm-dbs /path/to/2014-rocksdb,/path/to/2015-rocksdb,... \
//     --error-file /path/to/errors.log \
//     --log-file /path/to/benchmark.log \
//     --parallelism 16 \
//     --recsplit-parallel-batch 4 \
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
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// CONSTANTS
// =============================================================================

const (
	// WorkerBatchSize is the number of transactions each worker processes at a time
	// before sending results back to the stats aggregator
	WorkerBatchSize = 1000

	// ChannelBufferMultiplier determines how far ahead the producer reads
	// Buffer size = parallelism * WorkerBatchSize * ChannelBufferMultiplier
	// This ensures workers don't block waiting for work
	ChannelBufferMultiplier = 2

	// ErrorFileFlushInterval is how often we flush the error file buffer (in error count)
	// Buffering improves I/O performance for high error rates
	ErrorFileFlushInterval = 1000

	// ProgressReportPercent is the interval for progress reporting
	// We report at approximately every 1% of total transactions
	ProgressReportPercent = 1
)

// =============================================================================
// ERROR TYPES
// =============================================================================

// ErrorType represents the category of error for logging and statistics
type ErrorType int

const (
	ErrTypeNone                  ErrorType = iota
	ErrTypeInvalidInput                    // Invalid hex or wrong length - silently skipped
	ErrTypeRecSplitNotFound                // TxHash not found in any RecSplit index
	ErrTypeLedgerNotFound                  // LedgerSeq not found in any RocksDB store
	ErrTypeFalsePositiveNormal             // Single RecSplit returned wrong ledger
	ErrTypeFalsePositiveCompound           // Multiple RecSplits returned wrong ledgers (all false)
	ErrTypeFalsePositivePartial            // Found in one, false positive in others (counts as success)
	ErrTypeLedgerInMultipleDBs             // Data inconsistency: ledger in multiple RocksDB stores
	ErrTypeUnforeseen                      // Unexpected errors (RocksDB, decompression, unmarshal, etc.)
)

// String returns the string representation of the error type
// Used for error log formatting
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

// LedgerInfo holds ledger sequence and its close time for error reporting
// The ClosedAt time is extracted from xdr.LedgerCloseMeta.ClosedAt()
type LedgerInfo struct {
	Seq      uint32
	ClosedAt time.Time
}

// BenchmarkError encapsulates all error information for logging
// This struct is designed to capture all the context needed for debugging
// and analysis of failed lookups
type BenchmarkError struct {
	Type            ErrorType
	TxHash          string
	LedgerSeq       uint32       // For single ledger errors (LEDGER_NOT_FOUND)
	LedgerInfo      LedgerInfo   // For FALSE_POSITIVE_NORMAL (includes ClosedAt)
	LedgerInfos     []LedgerInfo // For compound/partial false positives
	SuccessLedger   LedgerInfo   // For FALSE_POSITIVE_PARTIAL (the one that worked)
	DBNames         []string     // For LEDGER_IN_MULTIPLE_DBS
	TxsScanned      int          // Number of transactions scanned in ledger(s)
	TotalDuration   time.Duration
	UnderlyingError error
}

// Error implements the error interface
func (e *BenchmarkError) Error() string {
	return fmt.Sprintf("%s: %s", e.Type.String(), e.formatDetails())
}

// FormatForLog formats the error as a pipe-delimited line for the error file
// Format varies by error type to include all relevant information
// ClosedAt times are printed using %v formatter as requested
func (e *BenchmarkError) FormatForLog() string {
	switch e.Type {
	case ErrTypeRecSplitNotFound:
		// RECSPLIT_NOT_FOUND|<txhash>|<duration>
		return fmt.Sprintf("%s|%s|%s",
			e.Type.String(), e.TxHash, helpers.FormatDuration(e.TotalDuration))

	case ErrTypeLedgerNotFound:
		// LEDGER_NOT_FOUND|<txhash>|<ledger>|<duration>
		// Note: No ClosedAt because ledger doesn't exist
		return fmt.Sprintf("%s|%s|%d|%s",
			e.Type.String(), e.TxHash, e.LedgerSeq, helpers.FormatDuration(e.TotalDuration))

	case ErrTypeFalsePositiveNormal:
		// FALSE_POSITIVE_NORMAL|<txhash>|(ledger,closedAt)|<txs_scanned>|<duration>
		return fmt.Sprintf("%s|%s|(%d,%v)|%d|%s",
			e.Type.String(), e.TxHash,
			e.LedgerInfo.Seq, e.LedgerInfo.ClosedAt,
			e.TxsScanned, helpers.FormatDuration(e.TotalDuration))

	case ErrTypeFalsePositiveCompound:
		// FALSE_POSITIVE_COMPOUND|<txhash>|(ledger1,closedAt1),(ledger2,closedAt2),...|<duration>
		tuples := make([]string, len(e.LedgerInfos))
		for i, info := range e.LedgerInfos {
			tuples[i] = fmt.Sprintf("(%d,%v)", info.Seq, info.ClosedAt)
		}
		return fmt.Sprintf("%s|%s|%s|%s",
			e.Type.String(), e.TxHash,
			strings.Join(tuples, ","),
			helpers.FormatDuration(e.TotalDuration))

	case ErrTypeFalsePositivePartial:
		// FALSE_POSITIVE_PARTIAL|<txhash>|(success_ledger,closedAt)|(fp1,closedAt1),(fp2,closedAt2),...|<duration>
		fpTuples := make([]string, 0, len(e.LedgerInfos))
		for _, info := range e.LedgerInfos {
			if info.Seq != e.SuccessLedger.Seq {
				fpTuples = append(fpTuples, fmt.Sprintf("(%d,%v)", info.Seq, info.ClosedAt))
			}
		}
		return fmt.Sprintf("%s|%s|(%d,%v)|%s|%s",
			e.Type.String(), e.TxHash,
			e.SuccessLedger.Seq, e.SuccessLedger.ClosedAt,
			strings.Join(fpTuples, ","),
			helpers.FormatDuration(e.TotalDuration))

	case ErrTypeLedgerInMultipleDBs:
		// LEDGER_IN_MULTIPLE_DBS|<txhash>|<ledger>|<db1,db2,...>|<duration>
		return fmt.Sprintf("%s|%s|%d|%s|%s",
			e.Type.String(), e.TxHash, e.LedgerSeq,
			strings.Join(e.DBNames, ","),
			helpers.FormatDuration(e.TotalDuration))

	case ErrTypeUnforeseen:
		// UNFORESEEN_ERROR|<txhash>|<error_message>|<duration>
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

// formatDetails returns a human-readable description of the error
func (e *BenchmarkError) formatDetails() string {
	switch e.Type {
	case ErrTypeRecSplitNotFound:
		return fmt.Sprintf("txHash %s not found in any RecSplit index", e.TxHash)
	case ErrTypeLedgerNotFound:
		return fmt.Sprintf("ledger %d not found in any RocksDB store", e.LedgerSeq)
	case ErrTypeFalsePositiveNormal:
		return fmt.Sprintf("txHash %s not found in ledger %d (scanned %d txs)",
			e.TxHash, e.LedgerInfo.Seq, e.TxsScanned)
	case ErrTypeFalsePositiveCompound:
		seqs := make([]uint32, len(e.LedgerInfos))
		for i, info := range e.LedgerInfos {
			seqs[i] = info.Seq
		}
		return fmt.Sprintf("txHash %s not found in any of ledgers %v", e.TxHash, seqs)
	case ErrTypeFalsePositivePartial:
		return fmt.Sprintf("txHash %s found in ledger %d but had false positives",
			e.TxHash, e.SuccessLedger.Seq)
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
// All durations represent the actual time spent in each phase
type QueryTiming struct {
	RecSplitLookup time.Duration // Time spent searching RecSplit indexes (averaged across parallel batches)
	RocksDBQuery   time.Duration // Time spent querying RocksDB
	Decompress     time.Duration // Zstd decompression time
	Unmarshal      time.Duration // XDR unmarshaling time
	TxReaderCreate time.Duration // Transaction reader creation
	TxSearch       time.Duration // Time to find matching hash in ledger
	TxExtract      time.Duration // Time to extract/marshal transaction data
	Total          time.Duration // Total end-to-end time
	TxCount        int           // Number of transactions scanned
}

// TimingAccumulator accumulates timing data for averaging
// All durations are stored as totals (not averages) for accurate weighted averaging
// Final averages are computed by dividing totals by QueryCount
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
// Each worker maintains its own stats to avoid lock contention
// Stats are merged into GlobalStats after each batch
type WorkerStats struct {
	Success               TimingAccumulator
	RecSplitNotFound      TimingAccumulator
	LedgerNotFound        TimingAccumulator
	FalsePositiveNormal   TimingAccumulator
	FalsePositiveCompound TimingAccumulator
	FalsePositivePartial  TimingAccumulator // Counted as success, but tracked separately for analysis
	LedgerInMultipleDBs   TimingAccumulator
	Unforeseen            TimingAccumulator
}

// GlobalStats holds aggregated statistics across all workers
// Protected by mutex for thread-safe updates from multiple workers
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
	// Note: FALSE_POSITIVE_PARTIAL is counted as success (tx was found)
	// but also tracked separately for analysis
	successCount := w.Success.QueryCount + w.FalsePositivePartial.QueryCount
	failedCount := w.RecSplitNotFound.QueryCount + w.LedgerNotFound.QueryCount +
		w.FalsePositiveNormal.QueryCount + w.FalsePositiveCompound.QueryCount +
		w.LedgerInMultipleDBs.QueryCount + w.Unforeseen.QueryCount

	g.TotalSuccess += successCount
	g.TotalFailed += failedCount
	g.TotalProcessed += successCount + failedCount
}

// Snapshot returns a copy of current stats for reporting
// Uses RLock to allow concurrent reads during progress reporting
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
// The range is determined at startup by scanning first/last keys
type RocksDBStore struct {
	DB        *grocksdb.DB
	Path      string
	Name      string // Just the directory name for logging
	MinLedger uint32
	MaxLedger uint32
}

// RocksDBStoreManager manages multiple RocksDB stores with shared resources
// The shared block cache is the key optimization - it allows frequently accessed
// blocks from any store to be cached, improving hit rates across all stores
type RocksDBStoreManager struct {
	Stores     []*RocksDBStore
	ReadOpts   *grocksdb.ReadOptions
	BlockCache *grocksdb.Cache
	Options    *grocksdb.Options
	BBTOptions *grocksdb.BlockBasedTableOptions
}

// FindStoreForLedger finds the RocksDB store that contains the given ledger
// Uses the pre-computed range to do O(n) lookup where n is number of stores
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
// In normal operation, this should return at most 1 store
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
	if m.BBTOptions != nil {
		m.BBTOptions.Destroy()
	}
	if m.Options != nil {
		m.Options.Destroy()
	}
	// Note: BlockCache is destroyed when BBTOptions is destroyed
}

// =============================================================================
// RECSPLIT INDEX MANAGER
// =============================================================================

// RecSplitIndex wraps a RecSplit index with its reader
// The Reader is thread-safe and can be used concurrently by multiple goroutines
type RecSplitIndex struct {
	Index  *recsplit.Index
	Reader *recsplit.IndexReader
	Path   string
	Name   string // Just the filename for logging
}

// RecSplitManager manages multiple RecSplit indexes
type RecSplitManager struct {
	Indexes []*RecSplitIndex

	// MmapSizeMB is a placeholder for future memory management
	// Currently, the erigon RecSplit library mmaps the entire file
	// This field is reserved for future optimization where we might
	// implement partial mmap or memory budgeting across indexes
	MmapSizeMB int
}

// Close closes all indexes
func (m *RecSplitManager) Close() {
	for _, idx := range m.Indexes {
		idx.Reader.Close()
		idx.Index.Close()
	}
}

// =============================================================================
// RECSPLIT LOOKUP RESULT
// =============================================================================

// RecSplitLookupResult holds the result of a single RecSplit lookup
type RecSplitLookupResult struct {
	IndexName string
	IndexIdx  int           // Index position in the manager's list
	LedgerSeq uint32        // The ledger sequence returned (only valid if Found)
	Found     bool          // Whether the lookup returned a match
	Duration  time.Duration // Time taken for this lookup
}

// =============================================================================
// LEDGER LOOKUP RESULT
// =============================================================================

// LedgerLookupResult holds the result of looking up and parsing a ledger
type LedgerLookupResult struct {
	Found         bool          // Whether the transaction was found in this ledger
	LedgerInfo    LedgerInfo    // Ledger sequence and close time
	TxsScanned    int           // Number of transactions scanned
	RocksDBTime   time.Duration // Time for RocksDB query
	DecompTime    time.Duration // Time for decompression
	UnmarshalTime time.Duration // Time for XDR unmarshal
	TxReaderTime  time.Duration // Time for tx reader creation
	TxSearchTime  time.Duration // Time for searching transactions
	TxExtractTime time.Duration // Time for extracting tx data (only if found)
	Error         error         // Any error that occurred
	DBNames       []string      // For multiple DB detection
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
	NetworkPassphrase     string
	Parallelism           int
	RecSplitParallelBatch int // Number of RecSplit files to search in parallel

	// Channels for producer-consumer pattern
	WorkChan   chan []WorkItem  // Batches of work items from producer to workers
	ResultChan chan BatchResult // Results from workers to stats aggregator

	// Global state
	Stats        *GlobalStats
	TotalTxCount int64 // Total transactions in input file (for progress reporting)
	StartTime    time.Time

	// Synchronization
	Ctx        context.Context
	Cancel     context.CancelFunc
	WG         sync.WaitGroup // Tracks worker goroutines
	ShutdownWG sync.WaitGroup // Tracks stats aggregator for graceful shutdown

	// Logging
	Logger    *log.Logger
	ErrorFile *bufio.Writer
	ErrorMu   sync.Mutex // Protects ErrorFile writes (though only aggregator writes)
}

// =============================================================================
// INITIALIZATION FUNCTIONS
// =============================================================================

// OpenRecSplitIndexes opens all RecSplit index files
// The indexes use mmap internally (handled by the erigon library)
//
// Future optimization opportunity (--recsplit-mmap-size-mb):
// The erigon RecSplit library mmaps the entire file. For very large indexes
// (2B+ keys), this could cause memory pressure. Future versions could:
// 1. Implement a custom RecSplit reader with partial mmap support
// 2. Use madvise() hints to control page residency
// 3. Implement an LRU-based page manager across all indexes
//
// For now, we rely on the OS virtual memory manager to handle paging.
func OpenRecSplitIndexes(paths []string, mmapSizeMB int) (*RecSplitManager, time.Duration, error) {
	start := time.Now()
	manager := &RecSplitManager{
		Indexes:    make([]*RecSplitIndex, 0, len(paths)),
		MmapSizeMB: mmapSizeMB, // Stored for future use
	}

	// TODO: Future optimization - implement memory budgeting across indexes
	// When mmapSizeMB is set, we could:
	// 1. Track total mapped memory across all indexes
	// 2. Prioritize keeping hash structures + fuse filters in memory
	// 3. Use madvise(MADV_DONTNEED) to release pages when over budget

	for _, path := range paths {
		// OpenIndex uses mmap internally
		// The entire file is mapped into virtual memory
		// Actual physical memory usage depends on access patterns
		idx, err := recsplit.OpenIndex(path)
		if err != nil {
			manager.Close()
			return nil, 0, fmt.Errorf("failed to open RecSplit index %s: %w", path, err)
		}

		// IndexReader wraps the index for lookups
		// It's thread-safe and can be used by multiple goroutines
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
//
// IMPORTANT: Shared Block Cache Implementation
// We create a SINGLE LRU cache and use it across ALL RocksDB instances.
// This is done by:
// 1. Creating one LRUCache with the total size budget
// 2. Creating one BlockBasedTableOptions that references this cache
// 3. Creating one Options that uses these table options
// 4. Opening each DB with the SAME Options object
//
// This means all DBs compete for the same cache space, which is optimal
// because frequently accessed blocks from ANY store will be cached.
func OpenRocksDBStores(paths []string, blockCacheSizeMB int) (*RocksDBStoreManager, time.Duration, error) {
	start := time.Now()

	// Step 1: Create shared block cache
	// This single cache will be shared across ALL RocksDB instances
	// The cache uses LRU eviction - least recently used blocks are evicted first
	cache := grocksdb.NewLRUCache(uint64(blockCacheSizeMB) * 1024 * 1024)

	// Step 2: Create block-based table options with shared cache
	// These options configure how data blocks are stored and cached
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(cache)                         // <-- This is the key line for cache sharing
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10)) // 10 bits per key bloom filter
	bbto.SetCacheIndexAndFilterBlocks(true)           // Cache index/filter blocks in the shared cache

	// Step 3: Create database options using the table options
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)       // Fail if DB doesn't exist
	opts.SetBlockBasedTableFactory(bbto) // <-- Uses the shared cache via bbto

	// Step 4: Create read options (shared across all reads)
	readOpts := grocksdb.NewDefaultReadOptions()

	manager := &RocksDBStoreManager{
		Stores:     make([]*RocksDBStore, 0, len(paths)),
		ReadOpts:   readOpts,
		BlockCache: cache,
		Options:    opts,
		BBTOptions: bbto,
	}

	// Step 5: Open each database with the SAME opts object
	// This ensures they all share the same block cache
	for _, path := range paths {
		// Open in read-only mode (second param false = don't error if no write lock)
		// All instances share the same opts, and therefore the same block cache
		db, err := grocksdb.OpenDbForReadOnly(opts, path, false)
		if err != nil {
			manager.Close()
			return nil, 0, fmt.Errorf("failed to open RocksDB %s: %w", path, err)
		}

		// Determine ledger range using SeekToFirst and SeekToLast
		// This is done once at startup for efficient range-based store selection
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

	// Sort stores by MinLedger for potentially more efficient searching
	// (though we do linear scan, having them sorted helps debugging)
	sort.Slice(manager.Stores, func(i, j int) bool {
		return manager.Stores[i].MinLedger < manager.Stores[j].MinLedger
	})

	return manager, time.Since(start), nil
}

// getLedgerRange determines the min and max ledger sequence in a RocksDB store
// by seeking to the first and last keys
func getLedgerRange(db *grocksdb.DB, readOpts *grocksdb.ReadOptions) (uint32, uint32, error) {
	iter := db.NewIterator(readOpts)
	defer iter.Close()

	// Get minimum ledger (first key)
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

	// Get maximum ledger (last key)
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
// This is used for progress reporting and ETA calculation
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
		// Only count lines that are valid 64-character hex strings
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
// Each worker has its own zstd decoder to avoid contention
// Workers pull batches from WorkChan, process them, and send results to ResultChan
func Worker(ctx *BenchmarkContext, workerID int) {
	defer ctx.WG.Done()

	// Create dedicated zstd decoder for this worker
	// This avoids contention on a shared decoder
	zstdDecoder, err := zstd.NewReader(nil)
	if err != nil {
		ctx.Logger.Printf("Worker %d: Failed to create zstd decoder: %v", workerID, err)
		return
	}
	defer zstdDecoder.Close()

	for {
		select {
		case <-ctx.Ctx.Done():
			// Context cancelled (e.g., Ctrl+C), exit gracefully
			return
		case batch, ok := <-ctx.WorkChan:
			if !ok {
				// Channel closed, no more work
				return
			}

			// Process the batch and send results
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
		// Validate input - skip invalid hex strings silently
		if len(item.TxHashHex) != 64 || !isValidHex(item.TxHashHex) {
			result.InvalidCount++
			continue
		}

		timing, benchErr := processTransaction(ctx, zstdDecoder, item.TxHashHex)

		// Update stats based on result type
		if benchErr == nil {
			// Clean success - no errors at all
			result.Stats.Success.Add(timing)
		} else {
			// Add error to list for logging
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
				// FALSE_POSITIVE_PARTIAL means we DID find the transaction
				// We track it separately for analysis, but also count as success
				result.Stats.FalsePositivePartial.Add(timing)
			case ErrTypeLedgerInMultipleDBs:
				result.Stats.LedgerInMultipleDBs.Add(timing)
			case ErrTypeUnforeseen:
				result.Stats.Unforeseen.Add(timing)
			}
		}
	}

	return result
}

// =============================================================================
// TRANSACTION PROCESSING - MAIN LOOKUP LOGIC
// =============================================================================

// processTransaction processes a single transaction hash using lazy sequential search
//
// ALGORITHM:
// 1. Search RecSplit indexes in parallel batches (default batch size: 4)
// 2. For each batch, wait for all lookups to complete
// 3. For any matches found, try to find the transaction in the corresponding ledger
// 4. If transaction found -> SUCCESS, return immediately (early termination)
// 5. If false positive -> continue to next match or next batch
// 6. If all batches exhausted with no success -> determine error type
//
// TIMING CALCULATION:
//   - RecSplit time = average of batch averages
//     (each batch average = sum of lookup times in batch / batch size)
//   - Other timings are summed across all ledger lookups attempted
func processTransaction(ctx *BenchmarkContext, zstdDecoder *zstd.Decoder, txHashHex string) (QueryTiming, *BenchmarkError) {
	timing := QueryTiming{}
	totalStart := time.Now()

	// Parse transaction hash from hex to bytes
	txHashBytes, err := hex.DecodeString(txHashHex)
	if err != nil {
		timing.Total = time.Since(totalStart)
		return timing, &BenchmarkError{
			Type:            ErrTypeUnforeseen,
			TxHash:          txHashHex,
			TotalDuration:   timing.Total,
			UnderlyingError: fmt.Errorf("hex decode error: %w", err),
		}
	}

	// Track all RecSplit batch timings for averaging
	var recSplitBatchAvgs []time.Duration

	// Track all ledger lookups for error reporting (with ClosedAt times)
	var allLedgerInfos []LedgerInfo
	var totalTxsScanned int

	// Track timing for ledger operations (accumulated across all lookups)
	var totalRocksDBTime, totalDecompTime, totalUnmarshalTime time.Duration
	var totalTxReaderTime, totalTxSearchTime, totalTxExtractTime time.Duration

	// Process RecSplit indexes in batches
	indexes := ctx.RecSplitMgr.Indexes
	batchSize := ctx.RecSplitParallelBatch

	for batchStart := 0; batchStart < len(indexes); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(indexes) {
			batchEnd = len(indexes)
		}

		// Search this batch of RecSplit indexes in parallel
		batchResults := searchRecSplitBatch(indexes[batchStart:batchEnd], txHashBytes, batchStart)

		// Calculate batch average time
		batchAvg := calculateBatchAverage(batchResults)
		recSplitBatchAvgs = append(recSplitBatchAvgs, batchAvg)

		// Collect matches from this batch
		var matches []RecSplitLookupResult
		for _, r := range batchResults {
			if r.Found {
				matches = append(matches, r)
			}
		}

		// Process matches in order (by index position for determinism)
		sort.Slice(matches, func(i, j int) bool {
			return matches[i].IndexIdx < matches[j].IndexIdx
		})

		for _, match := range matches {
			// Try to find the transaction in this ledger
			ledgerResult := lookupLedger(ctx, zstdDecoder, txHashHex, match.LedgerSeq)

			// Accumulate timing regardless of result
			totalRocksDBTime += ledgerResult.RocksDBTime
			totalDecompTime += ledgerResult.DecompTime
			totalUnmarshalTime += ledgerResult.UnmarshalTime
			totalTxReaderTime += ledgerResult.TxReaderTime
			totalTxSearchTime += ledgerResult.TxSearchTime
			totalTxExtractTime += ledgerResult.TxExtractTime
			totalTxsScanned += ledgerResult.TxsScanned

			// Handle errors from ledger lookup
			if ledgerResult.Error != nil {
				// Check for "ledger in multiple DBs" error
				if len(ledgerResult.DBNames) > 0 {
					timing.RecSplitLookup = calculateOverallRecSplitTime(recSplitBatchAvgs)
					timing.RocksDBQuery = totalRocksDBTime
					timing.Total = time.Since(totalStart)
					return timing, &BenchmarkError{
						Type:          ErrTypeLedgerInMultipleDBs,
						TxHash:        txHashHex,
						LedgerSeq:     match.LedgerSeq,
						DBNames:       ledgerResult.DBNames,
						TotalDuration: timing.Total,
					}
				}

				// Check if it's a "ledger not found" type error
				if ledgerResult.LedgerInfo.Seq == 0 {
					// Ledger doesn't exist in any RocksDB store
					timing.RecSplitLookup = calculateOverallRecSplitTime(recSplitBatchAvgs)
					timing.RocksDBQuery = totalRocksDBTime
					timing.Total = time.Since(totalStart)
					return timing, &BenchmarkError{
						Type:          ErrTypeLedgerNotFound,
						TxHash:        txHashHex,
						LedgerSeq:     match.LedgerSeq,
						TotalDuration: timing.Total,
					}
				}

				// Other unforeseen error
				timing.RecSplitLookup = calculateOverallRecSplitTime(recSplitBatchAvgs)
				timing.RocksDBQuery = totalRocksDBTime
				timing.Decompress = totalDecompTime
				timing.Unmarshal = totalUnmarshalTime
				timing.TxReaderCreate = totalTxReaderTime
				timing.TxSearch = totalTxSearchTime
				timing.Total = time.Since(totalStart)

				return timing, &BenchmarkError{
					Type:            ErrTypeUnforeseen,
					TxHash:          txHashHex,
					TotalDuration:   timing.Total,
					UnderlyingError: ledgerResult.Error,
				}
			}

			// Track this ledger for potential error reporting (includes ClosedAt)
			allLedgerInfos = append(allLedgerInfos, ledgerResult.LedgerInfo)

			if ledgerResult.Found {
				// SUCCESS! Transaction found
				timing.RecSplitLookup = calculateOverallRecSplitTime(recSplitBatchAvgs)
				timing.RocksDBQuery = totalRocksDBTime
				timing.Decompress = totalDecompTime
				timing.Unmarshal = totalUnmarshalTime
				timing.TxReaderCreate = totalTxReaderTime
				timing.TxSearch = totalTxSearchTime
				timing.TxExtract = totalTxExtractTime
				timing.TxCount = totalTxsScanned
				timing.Total = time.Since(totalStart)

				// Check if we had any false positives before this success
				if len(allLedgerInfos) > 1 {
					// FALSE_POSITIVE_PARTIAL - found in this ledger, but had false positives
					return timing, &BenchmarkError{
						Type:          ErrTypeFalsePositivePartial,
						TxHash:        txHashHex,
						SuccessLedger: ledgerResult.LedgerInfo,
						LedgerInfos:   allLedgerInfos,
						TotalDuration: timing.Total,
					}
				}

				// Clean success - no false positives
				return timing, nil
			}

			// False positive - transaction not in this ledger
			// Continue to next match or next batch
		}
	}

	// Exhausted all RecSplit indexes
	timing.RecSplitLookup = calculateOverallRecSplitTime(recSplitBatchAvgs)
	timing.RocksDBQuery = totalRocksDBTime
	timing.Decompress = totalDecompTime
	timing.Unmarshal = totalUnmarshalTime
	timing.TxReaderCreate = totalTxReaderTime
	timing.TxSearch = totalTxSearchTime
	timing.TxCount = totalTxsScanned
	timing.Total = time.Since(totalStart)

	// Determine the type of failure
	if len(allLedgerInfos) == 0 {
		// No RecSplit index returned a match
		return timing, &BenchmarkError{
			Type:          ErrTypeRecSplitNotFound,
			TxHash:        txHashHex,
			TotalDuration: timing.Total,
		}
	} else if len(allLedgerInfos) == 1 {
		// Single false positive
		return timing, &BenchmarkError{
			Type:          ErrTypeFalsePositiveNormal,
			TxHash:        txHashHex,
			LedgerInfo:    allLedgerInfos[0],
			TxsScanned:    totalTxsScanned,
			TotalDuration: timing.Total,
		}
	} else {
		// Multiple false positives (compound)
		return timing, &BenchmarkError{
			Type:          ErrTypeFalsePositiveCompound,
			TxHash:        txHashHex,
			LedgerInfos:   allLedgerInfos,
			TotalDuration: timing.Total,
		}
	}
}

// searchRecSplitBatch searches a batch of RecSplit indexes in parallel
// Returns results for all indexes in the batch
// Each goroutine uses the shared, thread-safe IndexReader
func searchRecSplitBatch(indexes []*RecSplitIndex, txHashBytes []byte, startIdx int) []RecSplitLookupResult {
	results := make([]RecSplitLookupResult, len(indexes))
	var wg sync.WaitGroup

	for i, idx := range indexes {
		wg.Add(1)
		go func(i int, idx *RecSplitIndex) {
			defer wg.Done()

			start := time.Now()
			ledgerSeqU64, found := idx.Reader.Lookup(txHashBytes)
			duration := time.Since(start)

			results[i] = RecSplitLookupResult{
				IndexName: idx.Name,
				IndexIdx:  startIdx + i,
				LedgerSeq: uint32(ledgerSeqU64),
				Found:     found,
				Duration:  duration,
			}
		}(i, idx)
	}

	wg.Wait()
	return results
}

// calculateBatchAverage calculates the average lookup time for a batch
// This is step 1 of the timing calculation: average within each batch
func calculateBatchAverage(results []RecSplitLookupResult) time.Duration {
	if len(results) == 0 {
		return 0
	}

	var total time.Duration
	for _, r := range results {
		total += r.Duration
	}

	return total / time.Duration(len(results))
}

// calculateOverallRecSplitTime calculates the overall RecSplit time
// as the average of batch averages
// This is step 2 of the timing calculation: average across all batches
func calculateOverallRecSplitTime(batchAvgs []time.Duration) time.Duration {
	if len(batchAvgs) == 0 {
		return 0
	}

	var total time.Duration
	for _, avg := range batchAvgs {
		total += avg
	}

	return total / time.Duration(len(batchAvgs))
}

// lookupLedger looks up a ledger in RocksDB, decompresses it, and searches for the transaction
// Returns detailed timing and result information
func lookupLedger(ctx *BenchmarkContext, zstdDecoder *zstd.Decoder, txHashHex string, ledgerSeq uint32) LedgerLookupResult {
	result := LedgerLookupResult{}

	// Find the RocksDB store(s) for this ledger
	stores := ctx.RocksDBMgr.FindAllStoresForLedger(ledgerSeq)

	if len(stores) == 0 {
		// Ledger not found in any store
		result.Error = fmt.Errorf("ledger %d not found in any RocksDB store", ledgerSeq)
		return result
	}

	if len(stores) > 1 {
		// Data inconsistency: ledger in multiple stores
		dbNames := make([]string, len(stores))
		for i, s := range stores {
			dbNames[i] = s.Name
		}
		result.Error = fmt.Errorf("ledger %d found in multiple DBs: %v", ledgerSeq, dbNames)
		result.DBNames = dbNames
		result.LedgerInfo.Seq = ledgerSeq // Set seq so caller knows this isn't "not found"
		return result
	}

	store := stores[0]

	// Query RocksDB
	ledgerKey := make([]byte, 4)
	binary.BigEndian.PutUint32(ledgerKey, ledgerSeq)

	t1 := time.Now()
	slice, err := store.DB.Get(ctx.RocksDBMgr.ReadOpts, ledgerKey)
	result.RocksDBTime = time.Since(t1)

	if err != nil {
		result.Error = fmt.Errorf("RocksDB error for ledger %d: %w", ledgerSeq, err)
		return result
	}

	if !slice.Exists() {
		slice.Free()
		result.Error = fmt.Errorf("ledger %d key not found in RocksDB (within range but missing)", ledgerSeq)
		return result
	}

	// Copy compressed data (slice is only valid until Free)
	compressedLCM := make([]byte, len(slice.Data()))
	copy(compressedLCM, slice.Data())
	slice.Free()

	// Decompress
	t2 := time.Now()
	uncompressedLCM, err := zstdDecoder.DecodeAll(compressedLCM, nil)
	result.DecompTime = time.Since(t2)

	if err != nil {
		result.Error = fmt.Errorf("zstd decompress error for ledger %d: %w", ledgerSeq, err)
		return result
	}

	// Unmarshal XDR
	t3 := time.Now()
	var lcm xdr.LedgerCloseMeta
	err = lcm.UnmarshalBinary(uncompressedLCM)
	result.UnmarshalTime = time.Since(t3)

	if err != nil {
		result.Error = fmt.Errorf("XDR unmarshal error for ledger %d: %w", ledgerSeq, err)
		return result
	}

	// Set ledger info now that we have ClosedAt from the LedgerCloseMeta
	result.LedgerInfo = LedgerInfo{
		Seq:      ledgerSeq,
		ClosedAt: lcm.ClosedAt(), // This is the key call to get close time
	}

	// Create transaction reader
	t4 := time.Now()
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ctx.NetworkPassphrase, lcm)
	result.TxReaderTime = time.Since(t4)

	if err != nil {
		result.Error = fmt.Errorf("tx reader error for ledger %d: %w", ledgerSeq, err)
		return result
	}
	defer txReader.Close()

	// Search for transaction
	t5 := time.Now()
	txHashLower := strings.ToLower(txHashHex)

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			result.TxSearchTime = time.Since(t5)
			result.Error = fmt.Errorf("tx read error in ledger %d: %w", ledgerSeq, err)
			return result
		}

		result.TxsScanned++

		if tx.Hash.HexString() == txHashLower {
			// Found the transaction!
			result.TxSearchTime = time.Since(t5)

			// Extract transaction data (measure time even though we don't use the data)
			t6 := time.Now()
			_, _ = tx.Envelope.MarshalBinary()
			_, _ = tx.Result.MarshalBinary()
			_, _ = tx.UnsafeMeta.MarshalBinary()
			result.TxExtractTime = time.Since(t6)

			result.Found = true
			return result
		}
	}

	result.TxSearchTime = time.Since(t5)
	// Transaction not found in this ledger (false positive)
	return result
}

// =============================================================================
// PRODUCER FUNCTION
// =============================================================================

// Producer reads transaction hashes from the input file and sends them to workers
// It batches transactions into WorkerBatchSize chunks for efficient processing
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
			continue // Skip empty lines
		}

		batch = append(batch, WorkItem{TxHashHex: line})

		// Send batch when full
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
// This runs as a single goroutine to avoid lock contention
// It's the only goroutine that writes to the error file and log
func StatsAggregator(ctx *BenchmarkContext) {
	defer ctx.ShutdownWG.Done()

	// Calculate progress report interval (approximately 1%)
	lastReportCount := int64(0)
	reportInterval := ctx.TotalTxCount / 100
	if reportInterval < 1 {
		reportInterval = 1
	}

	// Buffer errors for efficient file I/O
	errorBuffer := make([]*BenchmarkError, 0, ErrorFileFlushInterval)

	// Helper function to flush error buffer to file
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

			// Merge worker stats into global stats (thread-safe)
			ctx.Stats.Merge(&result.Stats)
			atomic.AddInt64(&ctx.Stats.InvalidInputs, result.InvalidCount)

			// Buffer errors for batch writing
			errorBuffer = append(errorBuffer, result.Errors...)
			if len(errorBuffer) >= ErrorFileFlushInterval {
				flushErrors()
			}

			// Check if we should report progress (approximately every 1%)
			currentCount := atomic.LoadInt64(&ctx.Stats.TotalProcessed)
			if currentCount-lastReportCount >= reportInterval {
				printProgress(ctx)
				lastReportCount = currentCount
			}

		case <-ctx.Ctx.Done():
			// Graceful shutdown - drain remaining results from channel
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

// printProgress prints current progress statistics to the log
func printProgress(ctx *BenchmarkContext) {
	stats := ctx.Stats.Snapshot()
	elapsed := time.Since(ctx.StartTime)

	// Calculate progress percentage
	processed := stats.TotalProcessed
	total := ctx.TotalTxCount
	percentage := float64(processed) / float64(total) * 100

	// Calculate ETA based on current throughput
	var eta time.Duration
	if processed > 0 {
		avgPerTx := elapsed / time.Duration(processed)
		remaining := total - processed
		eta = avgPerTx * time.Duration(remaining)
	}

	// Calculate throughput (transactions per second)
	throughput := float64(processed) / elapsed.Seconds()

	// Format timestamp for log entries
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	// Print main progress line
	ctx.Logger.Printf("%s | Progress: %s/%s (%.2f%%) | Elapsed: %s | ETA: %s",
		timestamp,
		helpers.FormatNumber(processed),
		helpers.FormatNumber(total),
		percentage,
		formatDurationShort(elapsed),
		formatDurationShort(eta))

	// Print throughput and success/fail counts
	ctx.Logger.Printf("%s | Throughput: %.2f/s | Success: %s | Failed: %s",
		timestamp,
		throughput,
		helpers.FormatNumber(stats.TotalSuccess),
		helpers.FormatNumber(stats.TotalFailed))

	// Print successful query timing breakdown
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

	// Print failed query breakdown by type
	if stats.TotalFailed > 0 {
		ctx.Logger.Printf("%s | Avg Failed Query Breakdown:", timestamp)
		printFailureStats(ctx.Logger, timestamp, "RECSPLIT_NOT_FOUND", &stats.RecSplitNotFound)
		printFailureStats(ctx.Logger, timestamp, "LEDGER_NOT_FOUND", &stats.LedgerNotFound)
		printFailureStats(ctx.Logger, timestamp, "FALSE_POSITIVE_NORMAL", &stats.FalsePositiveNormal)
		printFailureStats(ctx.Logger, timestamp, "FALSE_POSITIVE_COMPOUND", &stats.FalsePositiveCompound)
		printFailureStats(ctx.Logger, timestamp, "LEDGER_IN_MULTIPLE_DBS", &stats.LedgerInMultipleDBs)
		printFailureStats(ctx.Logger, timestamp, "UNFORESEEN", &stats.Unforeseen)
	}

	// Note about FALSE_POSITIVE_PARTIAL (logged separately since it's counted as success)
	if stats.FalsePositivePartial.QueryCount > 0 {
		ctx.Logger.Printf("%s | Note: %d FALSE_POSITIVE_PARTIAL (counted in success, avg %s)",
			timestamp,
			stats.FalsePositivePartial.QueryCount,
			helpers.FormatDuration(stats.FalsePositivePartial.Average().Total))
	}

	ctx.Logger.Println()
}

// printFailureStats prints stats for a specific failure type
func printFailureStats(logger *log.Logger, timestamp, name string, acc *TimingAccumulator) {
	if acc.QueryCount > 0 {
		avg := acc.Average()
		logger.Printf("%s |   %s: %d (avg %s)",
			timestamp, name, acc.QueryCount, helpers.FormatDuration(avg.Total))
	}
}

// formatDurationShort formats a duration in a compact human-readable format
func formatDurationShort(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%dh %dm %ds", int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60)
}

// =============================================================================
// FINAL SUMMARY
// =============================================================================

// FinalSummary holds the final benchmark results for JSON output
type FinalSummary struct {
	TotalTransactions    int64                   `json:"total_transactions"`
	TotalSuccess         int64                   `json:"total_success"`
	TotalFailed          int64                   `json:"total_failed"`
	InvalidInputs        int64                   `json:"invalid_inputs"`
	WallClockTime        time.Duration           `json:"wall_clock_time_ns"`
	WallClockTimeStr     string                  `json:"wall_clock_time"`
	Throughput           float64                 `json:"throughput_per_second"`
	SuccessAvg           QueryTimingJSON         `json:"success_avg"`
	FalsePositivePartial QueryTimingJSON         `json:"false_positive_partial_avg,omitempty"`
	Failures             map[string]FailureStats `json:"failures"`
}

// QueryTimingJSON is the JSON-serializable version of QueryTiming
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

// FailureStats holds statistics for a failure type
type FailureStats struct {
	Count   int64  `json:"count"`
	AvgTime string `json:"avg_time"`
}

// timingToJSON converts a QueryTiming to its JSON representation
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

// printFinalSummary prints the final benchmark summary to the log
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
	if stats.TotalProcessed > 0 {
		ctx.Logger.Printf("  Successful:          %s (%.2f%%)",
			helpers.FormatNumber(stats.TotalSuccess),
			float64(stats.TotalSuccess)/float64(stats.TotalProcessed)*100)
		ctx.Logger.Printf("  Failed:              %s (%.2f%%)",
			helpers.FormatNumber(stats.TotalFailed),
			float64(stats.TotalFailed)/float64(stats.TotalProcessed)*100)
	}
	ctx.Logger.Printf("  Invalid (skipped):   %s", helpers.FormatNumber(stats.InvalidInputs))
	ctx.Logger.Println()

	ctx.Logger.Printf("Wall Clock Time:       %s", helpers.FormatDuration(elapsed))
	if elapsed.Seconds() > 0 {
		ctx.Logger.Printf("Throughput:            %.2f tx/s", float64(stats.TotalProcessed)/elapsed.Seconds())
	}
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

	// JSON Summary (appended to same log file as requested)
	summary := buildJSONSummary(stats, elapsed)
	jsonBytes, err := json.MarshalIndent(summary, "", "  ")
	if err == nil {
		ctx.Logger.Println()
		ctx.Logger.Println("JSON SUMMARY:")
		ctx.Logger.Println(string(jsonBytes))
	}

	ctx.Logger.Println("================================================================================")
}

// printFinalFailureStats prints detailed stats for a failure type in the final summary
func printFinalFailureStats(logger *log.Logger, name string, acc *TimingAccumulator) {
	if acc.QueryCount > 0 {
		avg := acc.Average()
		logger.Printf("%s:", name)
		logger.Printf("  Count:             %s", helpers.FormatNumber(acc.QueryCount))
		logger.Printf("  Average Time:      %s", helpers.FormatDuration(avg.Total))
	}
}

// buildJSONSummary builds the JSON summary structure
func buildJSONSummary(stats GlobalStats, elapsed time.Duration) FinalSummary {
	summary := FinalSummary{
		TotalTransactions: stats.TotalProcessed,
		TotalSuccess:      stats.TotalSuccess,
		TotalFailed:       stats.TotalFailed,
		InvalidInputs:     stats.InvalidInputs,
		WallClockTime:     elapsed,
		WallClockTimeStr:  helpers.FormatDuration(elapsed),
		Failures:          make(map[string]FailureStats),
	}

	if elapsed.Seconds() > 0 {
		summary.Throughput = float64(stats.TotalProcessed) / elapsed.Seconds()
	}

	if stats.Success.QueryCount > 0 {
		summary.SuccessAvg = timingToJSON(stats.Success.Average())
	}

	if stats.FalsePositivePartial.QueryCount > 0 {
		summary.FalsePositivePartial = timingToJSON(stats.FalsePositivePartial.Average())
	}

	// Add failure stats
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

// printMemStats prints memory statistics to the log
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

// printInitSummary prints the initialization summary before starting the benchmark
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

	// RecSplit indexes summary
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
	if config.RecSplitMmapSizeMB > 0 {
		logger.Printf("  Mmap size limit:     %d MB (placeholder - not yet implemented)", config.RecSplitMmapSizeMB)
	}
	logger.Println()

	// RocksDB stores summary
	logger.Printf("RocksDB Stores (%d databases):", len(dbMgr.Stores))
	logger.Printf("  Load time:           %s", helpers.FormatDuration(dbLoadTime))
	logger.Printf("  Shared block cache:  %d MB", config.ReadBufferMB)
	for _, store := range dbMgr.Stores {
		logger.Printf("  - %s: ledgers %s - %s",
			store.Name,
			helpers.FormatNumber(int64(store.MinLedger)),
			helpers.FormatNumber(int64(store.MaxLedger)))
	}
	logger.Println()

	// Input file summary
	logger.Printf("Input File:")
	logger.Printf("  Path:                %s", config.InputFile)
	logger.Printf("  Transaction count:   %s", helpers.FormatNumber(totalTxCount))
	logger.Printf("  Count time:          %s", helpers.FormatDuration(countTime))
	logger.Println()

	// Configuration summary
	logger.Printf("Configuration:")
	logger.Printf("  Parallelism:         %d workers", config.Parallelism)
	logger.Printf("  RecSplit batch:      %d files in parallel", config.RecSplitParallelBatch)
	logger.Printf("  Worker batch size:   %d txs", WorkerBatchSize)
	logger.Printf("  Network passphrase:  %s", config.NetworkPassphrase)
	logger.Println()

	logger.Println("================================================================================")
	logger.Println()
}

// =============================================================================
// CONFIG AND MAIN
// =============================================================================

// Config holds all configuration options for the benchmark
type Config struct {
	InputFile             string
	RecSplitFiles         []string
	RocksDBPaths          []string
	ErrorFile             string
	LogFile               string
	ReadBufferMB          int
	Parallelism           int
	RecSplitParallelBatch int
	RecSplitMmapSizeMB    int // Placeholder for future memory management
	NetworkPassphrase     string
}

// parseFlags parses command-line flags and validates configuration
func parseFlags() (*Config, error) {
	var (
		inputFile             string
		recsplitFiles         string
		rocksdbPaths          string
		errorFile             string
		logFile               string
		readBufferMB          int
		parallelism           int
		recsplitParallelBatch int
		recsplitMmapSizeMB    int
		networkPassphrase     string
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
	flag.IntVar(&recsplitParallelBatch, "recsplit-parallel-batch", 4,
		"Number of RecSplit files to search in parallel per transaction")
	flag.IntVar(&recsplitMmapSizeMB, "recsplit-mmap-size-mb", 0,
		"Placeholder for future RecSplit memory management (not yet implemented)")
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
	if recsplitParallelBatch < 1 {
		return nil, fmt.Errorf("--recsplit-parallel-batch must be at least 1")
	}

	// Parse file lists (comma-separated)
	rsFiles := strings.Split(recsplitFiles, ",")
	dbPaths := strings.Split(rocksdbPaths, ",")

	// Trim whitespace from paths
	for i := range rsFiles {
		rsFiles[i] = strings.TrimSpace(rsFiles[i])
	}
	for i := range dbPaths {
		dbPaths[i] = strings.TrimSpace(dbPaths[i])
	}

	// Validate all files exist before proceeding
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
		InputFile:             inputFile,
		RecSplitFiles:         rsFiles,
		RocksDBPaths:          dbPaths,
		ErrorFile:             errorFile,
		LogFile:               logFile,
		ReadBufferMB:          readBufferMB,
		Parallelism:           parallelism,
		RecSplitParallelBatch: recsplitParallelBatch,
		RecSplitMmapSizeMB:    recsplitMmapSizeMB,
		NetworkPassphrase:     networkPassphrase,
	}, nil
}

func main() {
	// Parse and validate configuration
	config, err := parseFlags()
	if err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	// Set up logger (to file or stdout)
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

	// Set up error file (required)
	errorFile, err := os.Create(config.ErrorFile)
	if err != nil {
		log.Fatalf("Failed to create error file: %v", err)
	}
	defer errorFile.Close()
	errorWriter := bufio.NewWriter(errorFile)
	defer errorWriter.Flush()

	// Set up signal handling for graceful shutdown (Ctrl+C)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Open RecSplit indexes
	logger.Println("Opening RecSplit indexes...")
	rsMgr, rsLoadTime, err := OpenRecSplitIndexes(config.RecSplitFiles, config.RecSplitMmapSizeMB)
	if err != nil {
		log.Fatalf("Failed to open RecSplit indexes: %v", err)
	}
	defer rsMgr.Close()

	// Open RocksDB stores with shared block cache
	logger.Println("Opening RocksDB stores...")
	dbMgr, dbLoadTime, err := OpenRocksDBStores(config.RocksDBPaths, config.ReadBufferMB)
	if err != nil {
		log.Fatalf("Failed to open RocksDB stores: %v", err)
	}
	defer dbMgr.Close()

	// Count input transactions for progress reporting
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

	// Create benchmark context with all shared resources
	benchCtx := &BenchmarkContext{
		RecSplitMgr:           rsMgr,
		RocksDBMgr:            dbMgr,
		NetworkPassphrase:     config.NetworkPassphrase,
		Parallelism:           config.Parallelism,
		RecSplitParallelBatch: config.RecSplitParallelBatch,
		WorkChan:              make(chan []WorkItem, config.Parallelism*ChannelBufferMultiplier),
		ResultChan:            make(chan BatchResult, config.Parallelism*ChannelBufferMultiplier),
		Stats:                 &GlobalStats{},
		TotalTxCount:          totalTxCount,
		Ctx:                   ctx,
		Cancel:                cancel,
		Logger:                logger,
		ErrorFile:             errorWriter,
	}

	// Start worker goroutines
	logger.Printf("Starting %d workers (each with up to %d parallel RecSplit lookups)...",
		config.Parallelism, config.RecSplitParallelBatch)
	for i := 0; i < config.Parallelism; i++ {
		benchCtx.WG.Add(1)
		go Worker(benchCtx, i)
	}

	// Start stats aggregator goroutine
	benchCtx.ShutdownWG.Add(1)
	go StatsAggregator(benchCtx)

	// Record start time
	benchCtx.StartTime = time.Now()
	logger.Println()
	logger.Printf("Benchmark started at %s", benchCtx.StartTime.Format("2006-01-02 15:04:05"))
	logger.Println()

	// Handle signals (Ctrl+C) in a separate goroutine
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

	// Wait for all workers to finish
	benchCtx.WG.Wait()

	// Close result channel to signal stats aggregator to finish
	close(benchCtx.ResultChan)

	// Wait for stats aggregator to finish
	benchCtx.ShutdownWG.Wait()

	// Print final summary
	printFinalSummary(benchCtx)

	logger.Printf("Benchmark completed at %s", time.Now().Format("2006-01-02 15:04:05"))
}
