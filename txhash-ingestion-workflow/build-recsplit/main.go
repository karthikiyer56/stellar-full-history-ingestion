// =============================================================================
// build-recsplit - Standalone RecSplit Index Builder
// =============================================================================
//
// This tool builds RecSplit minimal perfect hash indexes from a RocksDB store
// containing txHash→ledgerSeq mappings.
//
// USAGE:
//
//	build-recsplit \
//	  --rocksdb-path /path/to/rocksdb \
//	  --output-dir /path/to/indexes \
//	  --log /path/to/build.log \
//	  --error /path/to/build.err \
//	  [--compact] \
//	  [--parallel] \
//	  [--verify] \
//	  [--block-cache-mb 8192]
//
// FEATURES:
//
//   - Discovers key counts by iterating each CF (no meta store dependency)
//   - Optional compaction before building (--compact)
//   - Optional verification after building (--verify)
//   - Sequential or parallel building (--parallel)
//
// OUTPUT:
//
//	16 index files: cf-0.idx through cf-f.idx
//
// =============================================================================

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/cf"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/compact"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/logging"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/memory"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/recsplit"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/types"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/verify"
	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// Iterator Adapter - wraps grocksdb.Iterator to implement interfaces.Iterator
// =============================================================================

type iteratorAdapter struct {
	iter *grocksdb.Iterator
}

func (a *iteratorAdapter) SeekToFirst() {
	a.iter.SeekToFirst()
}

func (a *iteratorAdapter) Valid() bool {
	return a.iter.Valid()
}

func (a *iteratorAdapter) Next() {
	a.iter.Next()
}

func (a *iteratorAdapter) Key() []byte {
	return a.iter.Key().Data()
}

func (a *iteratorAdapter) Value() []byte {
	return a.iter.Value().Data()
}

func (a *iteratorAdapter) Error() error {
	return a.iter.Err()
}

func (a *iteratorAdapter) Close() {
	a.iter.Close()
}

// scanIteratorAdapter wraps grocksdb.Iterator with scan-optimized ReadOptions.
// Unlike iteratorAdapter, this also owns and destroys the ReadOptions on Close().
type scanIteratorAdapter struct {
	iter     *grocksdb.Iterator
	scanOpts *grocksdb.ReadOptions
}

func (a *scanIteratorAdapter) SeekToFirst() {
	a.iter.SeekToFirst()
}

func (a *scanIteratorAdapter) Valid() bool {
	return a.iter.Valid()
}

func (a *scanIteratorAdapter) Next() {
	a.iter.Next()
}

func (a *scanIteratorAdapter) Key() []byte {
	return a.iter.Key().Data()
}

func (a *scanIteratorAdapter) Value() []byte {
	return a.iter.Value().Data()
}

func (a *scanIteratorAdapter) Error() error {
	return a.iter.Err()
}

func (a *scanIteratorAdapter) Close() {
	a.iter.Close()
	if a.scanOpts != nil {
		a.scanOpts.Destroy()
	}
}

// =============================================================================
// RocksDB Store (supports both read-only and read-write modes)
// =============================================================================

type Store struct {
	db         *grocksdb.DB
	opts       *grocksdb.Options
	cfHandles  []*grocksdb.ColumnFamilyHandle
	cfOpts     []*grocksdb.Options
	readOpts   *grocksdb.ReadOptions
	blockCache *grocksdb.Cache
	cfIndexMap map[string]int
	readOnly   bool
	path       string
}

func OpenStore(path string, blockCacheMB int, readOnly bool) (*Store, error) {
	var blockCache *grocksdb.Cache
	if blockCacheMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(blockCacheMB * types.MB))
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	cfNames := []string{"default"}
	cfNames = append(cfNames, cf.Names...)

	cfOptsList := make([]*grocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts := grocksdb.NewDefaultOptions()
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetFilterPolicy(grocksdb.NewBloomFilter(12))
		if blockCache != nil {
			bbto.SetBlockCache(blockCache)
		}
		cfOpts.SetBlockBasedTableFactory(bbto)
		cfOptsList[i] = cfOpts
	}

	var db *grocksdb.DB
	var cfHandles []*grocksdb.ColumnFamilyHandle
	var err error

	if readOnly {
		db, cfHandles, err = grocksdb.OpenDbForReadOnlyColumnFamilies(opts, path, cfNames, cfOptsList, false)
	} else {
		db, cfHandles, err = grocksdb.OpenDbColumnFamilies(opts, path, cfNames, cfOptsList)
	}

	if err != nil {
		opts.Destroy()
		for _, cfOpt := range cfOptsList {
			cfOpt.Destroy()
		}
		if blockCache != nil {
			blockCache.Destroy()
		}
		return nil, fmt.Errorf("failed to open RocksDB: %w", err)
	}

	cfIndexMap := make(map[string]int)
	for i, name := range cfNames {
		cfIndexMap[name] = i
	}

	readOpts := grocksdb.NewDefaultReadOptions()

	return &Store{
		db:         db,
		opts:       opts,
		cfHandles:  cfHandles,
		cfOpts:     cfOptsList,
		readOpts:   readOpts,
		blockCache: blockCache,
		cfIndexMap: cfIndexMap,
		readOnly:   readOnly,
		path:       path,
	}, nil
}

// NewIteratorCF implements interfaces.TxHashStore (partial).
// Returns an interfaces.Iterator for use with pkg/recsplit and pkg/verify.
func (s *Store) NewIteratorCF(cfName string) interfaces.Iterator {
	cfHandle := s.cfHandles[s.cfIndexMap[cfName]]
	rawIter := s.db.NewIteratorCF(s.readOpts, cfHandle)
	return &iteratorAdapter{iter: rawIter}
}

// NewScanIteratorCF creates a scan-optimized iterator for sequential scans.
// Uses readahead prefetching and avoids polluting the block cache.
func (s *Store) NewScanIteratorCF(cfName string) interfaces.Iterator {
	cfHandle := s.cfHandles[s.cfIndexMap[cfName]]

	// Create scan-optimized read options
	scanOpts := grocksdb.NewDefaultReadOptions()
	scanOpts.SetReadaheadSize(2 * 1024 * 1024) // 2MB readahead
	scanOpts.SetFillCache(false)               // Don't pollute block cache

	rawIter := s.db.NewIteratorCF(scanOpts, cfHandle)
	return &scanIteratorAdapter{
		iter:     rawIter,
		scanOpts: scanOpts,
	}
}

// CompactCF implements compact.Compactable.
func (s *Store) CompactCF(cfName string) time.Duration {
	if s.readOnly {
		return 0
	}
	start := time.Now()
	cfHandle := s.cfHandles[s.cfIndexMap[cfName]]
	s.db.CompactRangeCF(cfHandle, grocksdb.Range{Start: nil, Limit: nil})
	return time.Since(start)
}

// Get retrieves a value from the store.
func (s *Store) Get(cfName string, key []byte) ([]byte, bool, error) {
	cfHandle := s.cfHandles[s.cfIndexMap[cfName]]
	slice, err := s.db.GetCF(s.readOpts, cfHandle, key)
	if err != nil {
		return nil, false, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil, false, nil
	}

	value := make([]byte, slice.Size())
	copy(value, slice.Data())
	return value, true, nil
}

// Path returns the store path.
func (s *Store) Path() string {
	return s.path
}

func (s *Store) Close() {
	if s.readOpts != nil {
		s.readOpts.Destroy()
	}
	for _, cfHandle := range s.cfHandles {
		if cfHandle != nil {
			cfHandle.Destroy()
		}
	}
	if s.db != nil {
		s.db.Close()
	}
	if s.opts != nil {
		s.opts.Destroy()
	}
	for _, cfOpt := range s.cfOpts {
		if cfOpt != nil {
			cfOpt.Destroy()
		}
	}
	if s.blockCache != nil {
		s.blockCache.Destroy()
	}
}

// =============================================================================
// Minimal TxHashStore Adapter
// =============================================================================

// TxHashStoreAdapter wraps Store to implement interfaces.TxHashStore.
// Only the methods needed by pkg/recsplit and pkg/verify are actually used.
type TxHashStoreAdapter struct {
	store *Store
}

func NewTxHashStoreAdapter(store *Store) *TxHashStoreAdapter {
	return &TxHashStoreAdapter{store: store}
}

func (a *TxHashStoreAdapter) NewIteratorCF(cfName string) interfaces.Iterator {
	return a.store.NewIteratorCF(cfName)
}

func (a *TxHashStoreAdapter) NewScanIteratorCF(cfName string) interfaces.Iterator {
	return a.store.NewScanIteratorCF(cfName)
}

func (a *TxHashStoreAdapter) Get(txHash []byte) ([]byte, bool, error) {
	cfName := cf.GetName(txHash)
	return a.store.Get(cfName, txHash)
}

func (a *TxHashStoreAdapter) Path() string {
	return a.store.Path()
}

// Unused methods - implement as no-ops or panics
func (a *TxHashStoreAdapter) WriteBatch(entriesByCF map[string][]types.Entry) error {
	panic("WriteBatch not supported in read-only mode")
}

func (a *TxHashStoreAdapter) FlushAll() error {
	panic("FlushAll not supported in read-only mode")
}

func (a *TxHashStoreAdapter) CompactAll() time.Duration {
	panic("CompactAll not supported - use compact.CompactAllCFs instead")
}

func (a *TxHashStoreAdapter) CompactCF(cfName string) time.Duration {
	return a.store.CompactCF(cfName)
}

func (a *TxHashStoreAdapter) GetAllCFStats() []types.CFStats {
	return nil
}

func (a *TxHashStoreAdapter) GetCFStats(cfName string) types.CFStats {
	return types.CFStats{}
}

func (a *TxHashStoreAdapter) Close() {
	// Don't close here - the underlying store is closed separately
}

// =============================================================================
// No-Op MetaStore for standalone verification
// =============================================================================

// NoOpMetaStore implements interfaces.MetaStore with no-op operations.
// Used for standalone verification where no crash recovery is needed.
type NoOpMetaStore struct{}

func (m *NoOpMetaStore) GetStartLedger() (uint32, error)               { return 0, nil }
func (m *NoOpMetaStore) GetEndLedger() (uint32, error)                 { return 0, nil }
func (m *NoOpMetaStore) SetConfig(startLedger, endLedger uint32) error { return nil }
func (m *NoOpMetaStore) GetPhase() (types.Phase, error)                { return "", nil }
func (m *NoOpMetaStore) SetPhase(phase types.Phase) error              { return nil }
func (m *NoOpMetaStore) GetLastCommittedLedger() (uint32, error)       { return 0, nil }
func (m *NoOpMetaStore) GetCFCounts() (map[string]uint64, error)       { return nil, nil }
func (m *NoOpMetaStore) CommitBatchProgress(lastLedger uint32, cfCounts map[string]uint64) error {
	return nil
}
func (m *NoOpMetaStore) GetVerifyCF() (string, error)      { return "", nil }
func (m *NoOpMetaStore) SetVerifyCF(cf string) error       { return nil }
func (m *NoOpMetaStore) Exists() bool                      { return false }
func (m *NoOpMetaStore) LogState(logger interfaces.Logger) {}
func (m *NoOpMetaStore) Close()                            {}

// =============================================================================
// Key Counter (discovers key counts by iterating)
// =============================================================================

func countKeys(store *Store, cfName string) (uint64, error) {
	iter := store.NewIteratorCF(cfName)
	defer iter.Close()

	var count uint64
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}
	return count, nil
}

// =============================================================================
// Main
// =============================================================================

func main() {
	// Parse flags
	rocksdbPath := flag.String("rocksdb-path", "", "Path to RocksDB store (required)")
	outputDir := flag.String("output-dir", "", "Directory for RecSplit index files (required)")
	logPath := flag.String("log", "", "Path to log file (required)")
	errorPath := flag.String("error", "", "Path to error file (required)")
	parallel := flag.Bool("parallel", false, "Build 16 indexes in parallel")
	doCompact := flag.Bool("compact", false, "Compact RocksDB before building")
	doVerify := flag.Bool("verify", false, "Verify indexes after building")
	blockCacheMB := flag.Int("block-cache-mb", 8192, "RocksDB block cache size in MB")

	flag.Parse()

	// Validate flags
	if *rocksdbPath == "" || *outputDir == "" || *logPath == "" || *errorPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --rocksdb-path, --output-dir, --log, and --error are required")
		flag.Usage()
		os.Exit(1)
	}

	// Initialize logger
	logger, err := logging.NewDualLogger(*logPath, *errorPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()

	// Initialize memory monitor
	memMonitor := memory.NewMemoryMonitor(logger, memory.DefaultRAMWarningThresholdGB)
	defer memMonitor.Stop()

	logger.Separator()
	logger.Info("                    BUILD RECSPLIT TOOL")
	logger.Separator()
	logger.Info("")
	logger.Info("Configuration:")
	logger.Info("  RocksDB Path:    %s", *rocksdbPath)
	logger.Info("  Output Dir:      %s", *outputDir)
	logger.Info("  Parallel Mode:   %v", *parallel)
	logger.Info("  Compact First:   %v", *doCompact)
	logger.Info("  Verify After:    %v", *doVerify)
	logger.Info("  Block Cache:     %d MB", *blockCacheMB)
	logger.Info("")

	// Create output and temp directories
	indexPath := filepath.Join(*outputDir, "index")
	tmpPath := filepath.Join(*outputDir, "tmp")

	if err := os.MkdirAll(indexPath, 0755); err != nil {
		logger.Error("Failed to create index directory: %v", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(tmpPath, 0755); err != nil {
		logger.Error("Failed to create temp directory: %v", err)
		os.Exit(1)
	}

	// Open RocksDB (read-write if compaction requested, read-only otherwise)
	needWrite := *doCompact
	logger.Info("Opening RocksDB store (read-only=%v)...", !needWrite)
	store, err := OpenStore(*rocksdbPath, *blockCacheMB, !needWrite)
	if err != nil {
		logger.Error("Failed to open RocksDB: %v", err)
		os.Exit(1)
	}
	defer store.Close()
	logger.Info("RocksDB store opened successfully")
	logger.Info("")

	// Phase 1: Compaction (optional)
	if *doCompact {
		logger.Separator()
		logger.Info("                    COMPACTION PHASE")
		logger.Separator()
		logger.Info("")

		compact.CompactAllCFs(store, logger)
		logger.Info("Current RSS: %.2f GB", memMonitor.CurrentRSSGB())
		logger.Info("")
	}

	// Discover key counts
	logger.Separator()
	logger.Info("                    DISCOVERING KEY COUNTS")
	logger.Separator()
	logger.Info("")

	cfCounts := make(map[string]uint64)
	var totalKeys uint64
	countStart := time.Now()

	for i, cfName := range cf.Names {
		countCFStart := time.Now()
		count, err := countKeys(store, cfName)
		if err != nil {
			logger.Error("Failed to count keys in CF %s: %v", cfName, err)
			os.Exit(1)
		}
		cfCounts[cfName] = count
		totalKeys += count
		logger.Info("[%2d/16] CF [%s]: %s keys (counted in %v)",
			i+1, cfName, helpers.FormatNumber(int64(count)), time.Since(countCFStart))
	}

	logger.Info("")
	logger.Info("Total keys: %s (discovered in %v)", helpers.FormatNumber(int64(totalKeys)), time.Since(countStart))
	logger.Info("Current RSS: %.2f GB", memMonitor.CurrentRSSGB())
	logger.Info("")

	// Phase 2: Build indexes using pkg/recsplit
	// Create adapter for pkg/recsplit
	storeAdapter := NewTxHashStoreAdapter(store)

	// Use pkg/recsplit.Builder
	builder := recsplit.NewBuilder(
		storeAdapter,
		cfCounts,
		indexPath,
		tmpPath,
		*parallel,
		logger,
		memMonitor,
	)

	buildStats, err := builder.Run()
	if err != nil {
		logger.Error("RecSplit building failed: %v", err)
		os.Exit(1)
	}

	// Phase 3: Verification (optional) using pkg/verify
	if *doVerify {
		// Use pkg/verify.Verifier with no-op meta store
		noOpMeta := &NoOpMetaStore{}
		verifier := verify.NewVerifier(
			storeAdapter,
			noOpMeta,
			indexPath,
			"", // no resume
			logger,
			memMonitor,
		)

		verifyStats, err := verifier.Run()
		if err != nil {
			logger.Error("Verification failed: %v", err)
			os.Exit(1)
		}

		if verifyStats.TotalFailures > 0 {
			logger.Error("Verification completed with %d failures", verifyStats.TotalFailures)
		}
	}

	// Final summary
	logger.Separator()
	memMonitor.LogSummary(logger)
	logger.Sync()

	// Clean up temp directory
	os.RemoveAll(tmpPath)

	fmt.Printf("Build complete: %s keys in %v\n", helpers.FormatNumber(int64(buildStats.TotalKeys)), buildStats.TotalTime)
	fmt.Printf("Index files: %s\n", indexPath)

	// Calculate total size
	var totalSize int64
	for _, stats := range buildStats.PerCFStats {
		totalSize += stats.IndexSize
	}
	fmt.Printf("Total index size: %s\n", helpers.FormatBytes(totalSize))

	if *doVerify {
		fmt.Println("Verification: PASSED")
	}
}
