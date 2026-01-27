// =============================================================================
// pkg/store/store.go - RocksDB TxHash Store Implementation
// =============================================================================
//
// This package implements the TxHashStore interface using RocksDB with 16 column
// families partitioned by the first hex character of the transaction hash.
//
// =============================================================================

package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/cf"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/types"
	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// RocksDBTxHashStore - Implementation of TxHashStore Interface
// =============================================================================

// RocksDBTxHashStore implements the TxHashStore interface using RocksDB.
type RocksDBTxHashStore struct {
	// mu protects concurrent access to the store
	mu sync.RWMutex

	// db is the RocksDB database instance
	db *grocksdb.DB

	// opts is the database options
	opts *grocksdb.Options

	// cfHandles holds all column family handles (index 0 = "default", 1-16 = "0"-"f")
	cfHandles []*grocksdb.ColumnFamilyHandle

	// cfOpts holds options for each column family
	cfOpts []*grocksdb.Options

	// writeOpts is the write options for batch writes
	writeOpts *grocksdb.WriteOptions

	// readOpts is the shared read options for all reads
	readOpts *grocksdb.ReadOptions

	// blockCache is the shared block cache across all CFs
	blockCache *grocksdb.Cache

	// path is the filesystem path to the RocksDB store
	path string

	// cfIndexMap maps CF name to index in cfHandles
	cfIndexMap map[string]int

	// logger for logging operations
	logger interfaces.Logger
}

// OpenRocksDBTxHashStore opens or creates a RocksDB store at the specified path.
//
// If the store exists, it is opened with the existing column families.
// If it doesn't exist, it is created with 16 column families.
func OpenRocksDBTxHashStore(path string, settings *types.RocksDBSettings, logger interfaces.Logger) (*RocksDBTxHashStore, error) {
	// Create Shared Block Cache
	var blockCache *grocksdb.Cache
	if settings.BlockCacheSizeMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(settings.BlockCacheSizeMB * types.MB))
	}

	// Create Database Options
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetErrorIfExists(false)

	// Global settings
	opts.SetMaxBackgroundJobs(settings.MaxBackgroundJobs)
	opts.SetMaxOpenFiles(settings.MaxOpenFiles)

	// Logging settings (reduce RocksDB log noise)
	opts.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)
	opts.SetMaxLogFileSize(20 * types.MB)
	opts.SetKeepLogFileNum(3)

	// Prepare Column Family Names and Options
	// Note: "default" CF is required by RocksDB, but we don't use it for data
	cfNames := []string{"default"}
	cfNames = append(cfNames, cf.Names...)

	// Create options for each CF
	cfOptsList := make([]*grocksdb.Options, len(cfNames))

	// Default CF uses minimal options (we don't use it)
	cfOptsList[0] = grocksdb.NewDefaultOptions()

	// Data CFs (0-f) use full configuration
	for i := 1; i < len(cfNames); i++ {
		cfOptsList[i] = createCFOptions(settings, blockCache)
	}

	// Open Database with Column Families
	logger.Info("Opening RocksDB store at: %s", path)

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(opts, path, cfNames, cfOptsList)
	if err != nil {
		// Cleanup on failure
		opts.Destroy()
		for _, cfOpt := range cfOptsList {
			if cfOpt != nil {
				cfOpt.Destroy()
			}
		}
		if blockCache != nil {
			blockCache.Destroy()
		}
		return nil, fmt.Errorf("failed to open RocksDB store at %s: %w", path, err)
	}

	// Create Write and Read Options
	writeOpts := grocksdb.NewDefaultWriteOptions()
	writeOpts.SetSync(false) // Async writes for performance (WAL handles durability)

	readOpts := grocksdb.NewDefaultReadOptions()

	// Build CF Index Map
	cfIndexMap := make(map[string]int)
	for i, name := range cfNames {
		cfIndexMap[name] = i
	}

	// Log Configuration
	memtables := settings.WriteBufferSizeMB * settings.MaxWriteBufferNumber * len(cf.Names)
	logger.Info("RocksDB store opened successfully:")
	logger.Info("  Column Families: %d", len(cfHandles)-1) // -1 for default
	logger.Info("  MemTable RAM:    %d MB", memtables)
	logger.Info("  Block Cache:     %d MB", settings.BlockCacheSizeMB)
	logger.Info("  Bloom Filter:    %d bits/key", settings.BloomFilterBitsPerKey)
	logger.Info("  WAL:             ENABLED (always)")
	logger.Info("  Auto-Compaction: DISABLED (manual phase)")

	return &RocksDBTxHashStore{
		db:         db,
		opts:       opts,
		cfHandles:  cfHandles,
		cfOpts:     cfOptsList,
		writeOpts:  writeOpts,
		readOpts:   readOpts,
		blockCache: blockCache,
		path:       path,
		cfIndexMap: cfIndexMap,
		logger:     logger,
	}, nil
}

// createCFOptions creates options for a data column family.
func createCFOptions(settings *types.RocksDBSettings, blockCache *grocksdb.Cache) *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()

	// MemTable Configuration
	opts.SetWriteBufferSize(uint64(settings.WriteBufferSizeMB * types.MB))
	opts.SetMaxWriteBufferNumber(settings.MaxWriteBufferNumber)
	opts.SetMinWriteBufferNumberToMerge(settings.MinWriteBufferNumberToMerge)

	// Compaction Configuration - CRITICAL: Disable auto-compaction during ingestion
	opts.SetDisableAutoCompactions(true)
	opts.SetCompactionStyle(grocksdb.LevelCompactionStyle)
	opts.SetTargetFileSizeBase(uint64(settings.TargetFileSizeMB * types.MB))
	opts.SetTargetFileSizeMultiplier(1)

	// These settings apply during explicit compaction
	opts.SetMaxBytesForLevelBase(uint64(settings.TargetFileSizeMB * types.MB * 10))
	opts.SetMaxBytesForLevelMultiplier(10)

	// Compression: No compression - values are only 4 bytes
	opts.SetCompression(grocksdb.NoCompression)

	// Block-Based Table Options
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()

	// Bloom filter for fast negative lookups
	if settings.BloomFilterBitsPerKey > 0 {
		bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(settings.BloomFilterBitsPerKey)))
	}

	// Shared block cache
	if blockCache != nil {
		bbto.SetBlockCache(blockCache)
	}

	opts.SetBlockBasedTableFactory(bbto)

	return opts
}

// =============================================================================
// TxHashStore Interface Implementation
// =============================================================================

// WriteBatch writes a batch of entries to the appropriate column families.
func (s *RocksDBTxHashStore) WriteBatch(entriesByCF map[string][]types.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for cfName, entries := range entriesByCF {
		cfHandle := s.getCFHandleByName(cfName)
		for _, entry := range entries {
			batch.PutCF(cfHandle, entry.Key, entry.Value)
		}
	}

	return s.db.Write(s.writeOpts, batch)
}

// Get retrieves the ledger sequence for a transaction hash.
func (s *RocksDBTxHashStore) Get(txHash []byte) (value []byte, found bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cfHandle := s.getCFHandle(txHash)

	slice, err := s.db.GetCF(s.readOpts, cfHandle, txHash)
	if err != nil {
		return nil, false, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil, false, nil
	}

	// Make a copy since slice data is invalidated after Free()
	value = make([]byte, slice.Size())
	copy(value, slice.Data())
	return value, true, nil
}

// NewIteratorCF creates a new iterator for a specific column family.
func (s *RocksDBTxHashStore) NewIteratorCF(cfName string) interfaces.Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cfHandle := s.getCFHandleByName(cfName)
	iter := s.db.NewIteratorCF(s.readOpts, cfHandle)

	return &rocksDBIterator{iter: iter}
}

// NewScanIteratorCF creates a scan-optimized iterator for a specific column family.
//
// This iterator is optimized for full sequential scans (e.g., counting entries):
//   - 2MB readahead buffer for prefetching sequential reads
//   - Does not fill block cache (avoids cache pollution during scans)
//   - Auto-tunes readahead size based on access patterns
//
// Use this for operations that iterate through all entries in a CF, such as
// count verification after compaction. For point lookups or small range scans,
// use NewIteratorCF() instead.
func (s *RocksDBTxHashStore) NewScanIteratorCF(cfName string) interfaces.Iterator {
	s.mu.RLock()
	cfHandle := s.getCFHandleByName(cfName)
	s.mu.RUnlock()

	// Create scan-optimized read options
	scanOpts := grocksdb.NewDefaultReadOptions()
	scanOpts.SetReadaheadSize(2 * 1024 * 1024) // 2MB readahead for sequential access
	scanOpts.SetFillCache(false)               // Don't pollute block cache with scan data

	iter := s.db.NewIteratorCF(scanOpts, cfHandle)

	return &rocksDBScanIterator{
		iter:     iter,
		scanOpts: scanOpts, // Must destroy when iterator closes
	}
}

// FlushAll flushes all column family MemTables to SST files on disk.
func (s *RocksDBTxHashStore) FlushAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	flushOpts := grocksdb.NewDefaultFlushOptions()
	defer flushOpts.Destroy()
	flushOpts.SetWait(true) // Block until flush completes

	s.logger.Info("Flushing all column families to disk...")

	for i, cfName := range cf.Names {
		cfHandle := s.cfHandles[i+1] // +1 because index 0 is "default"
		if err := s.db.FlushCF(cfHandle, flushOpts); err != nil {
			return fmt.Errorf("failed to flush CF %s: %w", cfName, err)
		}
	}

	s.logger.Info("All column families flushed successfully")
	return nil
}

// CompactAll performs full compaction on all 16 column families in parallel.
//
// NOTE: This method does NOT hold a lock during compaction. RocksDB is thread-safe
// and handles concurrent reads/writes during compaction internally. All 16 CFs are
// compacted in parallel using goroutines, with SetExclusiveManualCompaction(false)
// to allow concurrent manual compactions. Queries continue to work during compaction.
func (s *RocksDBTxHashStore) CompactAll() time.Duration {
	// Get all CF handles under read lock (quick operation)
	s.mu.RLock()
	cfHandles := make([]*grocksdb.ColumnFamilyHandle, len(cf.Names))
	for i := range cf.Names {
		cfHandles[i] = s.cfHandles[i+1] // +1 because index 0 is "default"
	}
	s.mu.RUnlock()

	s.logger.Separator()
	s.logger.Info("                    COMPACTING ALL COLUMN FAMILIES (PARALLEL)")
	s.logger.Separator()
	s.logger.Info("")
	s.logger.Info("Starting parallel compaction of all 16 column families...")

	totalStart := time.Now()

	// Compact all CFs in parallel - RocksDB is thread-safe
	var wg sync.WaitGroup
	for i, cfName := range cf.Names {
		wg.Add(1)
		go func(idx int, name string) {
			defer wg.Done()

			// Create options that allow parallel manual compactions
			opts := grocksdb.NewCompactRangeOptions()
			opts.SetExclusiveManualCompaction(false) // Allow concurrent compactions
			defer opts.Destroy()

			start := time.Now()
			s.db.CompactRangeCFOpt(cfHandles[idx], grocksdb.Range{Start: nil, Limit: nil}, opts)
			s.logger.Info("  CF [%s] compacted in %s", name, helpers.FormatDuration(time.Since(start)))
		}(i, cfName)
	}
	wg.Wait()

	totalTime := time.Since(totalStart)
	s.logger.Info("")
	s.logger.Info("All column families compacted in %s (parallel)", helpers.FormatDuration(totalTime))

	return totalTime
}

// CompactCF compacts a single column family by name.
//
// NOTE: This method does NOT hold a lock during compaction. RocksDB is thread-safe
// and handles concurrent reads/writes during compaction internally. Uses
// SetExclusiveManualCompaction(false) to allow concurrent compactions with other CFs.
func (s *RocksDBTxHashStore) CompactCF(cfName string) time.Duration {
	// Get CF handle under read lock (quick operation)
	s.mu.RLock()
	cfHandle := s.getCFHandleByName(cfName)
	s.mu.RUnlock()

	// Create options that allow parallel manual compactions
	opts := grocksdb.NewCompactRangeOptions()
	opts.SetExclusiveManualCompaction(false) // Allow concurrent compactions
	defer opts.Destroy()

	// Compaction runs WITHOUT holding any lock - RocksDB is thread-safe
	start := time.Now()
	s.db.CompactRangeCFOpt(cfHandle, grocksdb.Range{Start: nil, Limit: nil}, opts)

	return time.Since(start)
}

// GetAllCFStats returns statistics for all 16 column families.
func (s *RocksDBTxHashStore) GetAllCFStats() []types.CFStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := make([]types.CFStats, len(cf.Names))

	for i, cfName := range cf.Names {
		stats[i] = s.getCFStatsLocked(cfName)
	}

	return stats
}

// GetCFStats returns statistics for a specific column family.
func (s *RocksDBTxHashStore) GetCFStats(cfName string) types.CFStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.getCFStatsLocked(cfName)
}

// getCFStatsLocked returns CF stats (caller must hold lock).
func (s *RocksDBTxHashStore) getCFStatsLocked(cfName string) types.CFStats {
	cfHandle := s.getCFHandleByName(cfName)

	// Get estimated key count
	keyCountStr := s.db.GetPropertyCF("rocksdb.estimate-num-keys", cfHandle)
	var keyCount int64
	fmt.Sscanf(keyCountStr, "%d", &keyCount)

	// Get SST file stats (simplified - could add per-level breakdown)
	sstFileCountStr := s.db.GetPropertyCF("rocksdb.num-files-at-level0", cfHandle)
	var l0Files int64
	fmt.Sscanf(sstFileCountStr, "%d", &l0Files)

	// Get level stats
	levelStats := make([]types.CFLevelStats, 7) // L0-L6
	var totalFiles, totalSize int64

	for level := 0; level <= 6; level++ {
		filesProp := fmt.Sprintf("rocksdb.num-files-at-level%d", level)
		filesStr := s.db.GetPropertyCF(filesProp, cfHandle)
		var files int64
		fmt.Sscanf(filesStr, "%d", &files)

		levelStats[level] = types.CFLevelStats{
			Level:     level,
			FileCount: files,
		}

		totalFiles += files
	}

	// Get total size
	totalSizeStr := s.db.GetPropertyCF("rocksdb.total-sst-files-size", cfHandle)
	fmt.Sscanf(totalSizeStr, "%d", &totalSize)

	return types.CFStats{
		Name:          cfName,
		EstimatedKeys: keyCount,
		TotalSize:     totalSize,
		TotalFiles:    totalFiles,
		LevelStats:    levelStats,
	}
}

// Close releases all resources associated with the store.
func (s *RocksDBTxHashStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Info("Closing RocksDB store...")

	// Destroy write options
	if s.writeOpts != nil {
		s.writeOpts.Destroy()
		s.writeOpts = nil
	}

	// Destroy read options
	if s.readOpts != nil {
		s.readOpts.Destroy()
		s.readOpts = nil
	}

	// Destroy column family handles
	for _, cfHandle := range s.cfHandles {
		if cfHandle != nil {
			cfHandle.Destroy()
		}
	}
	s.cfHandles = nil

	// Close database
	if s.db != nil {
		s.db.Close()
		s.db = nil
	}

	// Destroy database options
	if s.opts != nil {
		s.opts.Destroy()
		s.opts = nil
	}

	// Destroy CF options
	for _, cfOpt := range s.cfOpts {
		if cfOpt != nil {
			cfOpt.Destroy()
		}
	}
	s.cfOpts = nil

	// Destroy block cache
	if s.blockCache != nil {
		s.blockCache.Destroy()
		s.blockCache = nil
	}

	s.logger.Info("RocksDB store closed")
}

// Path returns the filesystem path to the RocksDB store.
func (s *RocksDBTxHashStore) Path() string {
	return s.path
}

// =============================================================================
// Helper Methods
// =============================================================================

// getCFHandle returns the CF handle for a transaction hash.
func (s *RocksDBTxHashStore) getCFHandle(txHash []byte) *grocksdb.ColumnFamilyHandle {
	cfName := cf.GetName(txHash)
	return s.getCFHandleByName(cfName)
}

// getCFHandleByName returns the CF handle by name.
func (s *RocksDBTxHashStore) getCFHandleByName(cfName string) *grocksdb.ColumnFamilyHandle {
	idx, ok := s.cfIndexMap[cfName]
	if !ok {
		// Fall back to default CF (should never happen for valid cfName)
		return s.cfHandles[0]
	}
	return s.cfHandles[idx]
}

// =============================================================================
// RocksDB Iterator Wrapper
// =============================================================================

// rocksDBIterator wraps grocksdb.Iterator to implement our Iterator interface.
type rocksDBIterator struct {
	iter *grocksdb.Iterator
}

func (it *rocksDBIterator) SeekToFirst() {
	it.iter.SeekToFirst()
}

func (it *rocksDBIterator) Valid() bool {
	return it.iter.Valid()
}

func (it *rocksDBIterator) Next() {
	it.iter.Next()
}

func (it *rocksDBIterator) Key() []byte {
	return it.iter.Key().Data()
}

func (it *rocksDBIterator) Value() []byte {
	return it.iter.Value().Data()
}

func (it *rocksDBIterator) Error() error {
	return it.iter.Err()
}

func (it *rocksDBIterator) Close() {
	it.iter.Close()
}

// =============================================================================
// RocksDB Scan Iterator Wrapper (optimized for sequential scans)
// =============================================================================

// rocksDBScanIterator wraps grocksdb.Iterator with scan-optimized ReadOptions.
// Unlike rocksDBIterator, this also owns and destroys the ReadOptions on Close().
type rocksDBScanIterator struct {
	iter     *grocksdb.Iterator
	scanOpts *grocksdb.ReadOptions
}

func (it *rocksDBScanIterator) SeekToFirst() {
	it.iter.SeekToFirst()
}

func (it *rocksDBScanIterator) Valid() bool {
	return it.iter.Valid()
}

func (it *rocksDBScanIterator) Next() {
	it.iter.Next()
}

func (it *rocksDBScanIterator) Key() []byte {
	return it.iter.Key().Data()
}

func (it *rocksDBScanIterator) Value() []byte {
	return it.iter.Value().Data()
}

func (it *rocksDBScanIterator) Error() error {
	return it.iter.Err()
}

func (it *rocksDBScanIterator) Close() {
	it.iter.Close()
	if it.scanOpts != nil {
		it.scanOpts.Destroy()
	}
}

// Compile-Time Interface Checks
var _ interfaces.TxHashStore = (*RocksDBTxHashStore)(nil)
var _ interfaces.Iterator = (*rocksDBIterator)(nil)
var _ interfaces.Iterator = (*rocksDBScanIterator)(nil)

// =============================================================================
// Utility Functions
// =============================================================================

// LogAllCFStats logs statistics for all column families.
func LogAllCFStats(store interfaces.TxHashStore, logger interfaces.Logger, label string) {
	stats := store.GetAllCFStats()

	logger.Separator()
	logger.Info("                    %s", label)
	logger.Separator()
	logger.Info("")

	var totalKeys, totalSize, totalFiles int64

	logger.Info("%-4s %15s %15s %10s", "CF", "Est. Keys", "Size", "Files")
	logger.Info("%-4s %15s %15s %10s", "----", "---------------", "---------------", "----------")

	for _, cfStats := range stats {
		logger.Info("%-4s %15s %15s %10d",
			cfStats.Name,
			helpers.FormatNumber(cfStats.EstimatedKeys),
			helpers.FormatBytes(cfStats.TotalSize),
			cfStats.TotalFiles)

		totalKeys += cfStats.EstimatedKeys
		totalSize += cfStats.TotalSize
		totalFiles += cfStats.TotalFiles
	}

	logger.Info("%-4s %15s %15s %10s", "----", "---------------", "---------------", "----------")
	logger.Info("%-4s %15s %15s %10d", "TOT", helpers.FormatNumber(totalKeys), helpers.FormatBytes(totalSize), totalFiles)
	logger.Info("")
}

// LogCFLevelStats logs the level distribution for all column families.
func LogCFLevelStats(store interfaces.TxHashStore, logger interfaces.Logger) {
	stats := store.GetAllCFStats()

	logger.Info("LEVEL DISTRIBUTION BY COLUMN FAMILY:")
	logger.Info("")

	// Header
	header := "CF  "
	for level := 0; level <= 6; level++ {
		header += fmt.Sprintf("  L%d", level)
	}
	logger.Info("%s", header)
	logger.Info("--- " + "-----" + "-----" + "-----" + "-----" + "-----" + "-----" + "-----")

	for _, cfStats := range stats {
		line := fmt.Sprintf("%-3s ", cfStats.Name)
		for _, ls := range cfStats.LevelStats {
			line += fmt.Sprintf("%5d", ls.FileCount)
		}
		logger.Info("%s", line)
	}
	logger.Info("")
}
