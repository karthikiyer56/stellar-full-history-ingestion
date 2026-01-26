// =============================================================================
// store.go - RocksDB TxHash Store Implementation
// =============================================================================
//
// This file implements the TxHashStore interface using RocksDB with 16 column
// families partitioned by the first hex character of the transaction hash.
//
// ARCHITECTURE:
//
//	┌─────────────────────────────────────────────────────────────────────────┐
//	│                           RocksDB Instance                              │
//	├─────────────────────────────────────────────────────────────────────────┤
//	│  default CF │ CF "0" │ CF "1" │ ... │ CF "e" │ CF "f"                   │
//	│  (unused)   │ 0x0... │ 0x1... │     │ 0xe... │ 0xf...                   │
//	└─────────────────────────────────────────────────────────────────────────┘
//
// COLUMN FAMILY ROUTING:
//
//	The first byte of the txHash determines the CF:
//	  cfIndex = txHash[0] >> 4  (high nibble gives 0-15)
//	  cfName = ColumnFamilyNames[cfIndex]  ("0" through "f")
//
// ROCKSDB CONFIGURATION:
//
//	This store is optimized for bulk ingestion:
//	  - WAL enabled (for crash recovery)
//	  - Auto-compaction DISABLED (explicit compaction phase)
//	  - Large MemTables (256 MB per CF)
//	  - Bloom filters (12 bits per key)
//	  - Shared block cache (configurable, default 8 GB)
//	  - No compression (values are only 4 bytes)
//
// THREAD SAFETY:
//
//	All public methods are safe for concurrent use. RocksDB handles internal
//	locking. We maintain a single ReadOptions instance for all reads.
//
// =============================================================================

package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// RocksDBTxHashStore - Implementation of TxHashStore Interface
// =============================================================================

// RocksDBTxHashStore implements the TxHashStore interface using RocksDB.
//
// LIFECYCLE:
//
//	store, err := OpenRocksDBTxHashStore(config)
//	if err != nil {
//	    // handle error
//	}
//	defer store.Close()
//
// CRASH RECOVERY:
//
//	When reopened after a crash, RocksDB replays the WAL to recover
//	any committed transactions that weren't yet flushed to SST files.
//	This typically takes ~15-30 seconds for the default configuration.
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
	// IMPORTANT: We reuse a single ReadOptions instance for efficiency
	readOpts *grocksdb.ReadOptions

	// blockCache is the shared block cache across all CFs
	blockCache *grocksdb.Cache

	// path is the filesystem path to the RocksDB store
	path string

	// cfIndexMap maps CF name to index in cfHandles
	cfIndexMap map[string]int

	// logger for logging operations
	logger Logger
}

// OpenRocksDBTxHashStore opens or creates a RocksDB store at the specified path.
//
// If the store exists, it is opened with the existing column families.
// If it doesn't exist, it is created with 16 column families.
//
// PARAMETERS:
//   - path: Filesystem path for the RocksDB store
//   - settings: RocksDB configuration settings
//   - logger: Logger for operation logging
//
// RETURNS:
//   - A new RocksDBTxHashStore instance
//   - An error if opening/creation fails
func OpenRocksDBTxHashStore(path string, settings *RocksDBSettings, logger Logger) (*RocksDBTxHashStore, error) {
	// =========================================================================
	// Create Shared Block Cache
	// =========================================================================
	var blockCache *grocksdb.Cache
	if settings.BlockCacheSizeMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(settings.BlockCacheSizeMB * MB))
	}

	// =========================================================================
	// Create Database Options
	// =========================================================================
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetErrorIfExists(false)

	// Global settings
	opts.SetMaxBackgroundJobs(settings.MaxBackgroundJobs)
	opts.SetMaxOpenFiles(settings.MaxOpenFiles)

	// Logging settings (reduce RocksDB log noise)
	opts.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)
	opts.SetMaxLogFileSize(20 * MB)
	opts.SetKeepLogFileNum(3)

	// =========================================================================
	// Prepare Column Family Names and Options
	// =========================================================================
	// Note: "default" CF is required by RocksDB, but we don't use it for data
	cfNames := []string{"default"}
	cfNames = append(cfNames, ColumnFamilyNames...)

	// Create options for each CF
	cfOptsList := make([]*grocksdb.Options, len(cfNames))

	// Default CF uses minimal options (we don't use it)
	cfOptsList[0] = grocksdb.NewDefaultOptions()

	// Data CFs (0-f) use full configuration
	for i := 1; i < len(cfNames); i++ {
		cfOptsList[i] = createCFOptions(settings, blockCache)
	}

	// =========================================================================
	// Open Database with Column Families
	// =========================================================================
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

	// =========================================================================
	// Create Write and Read Options
	// =========================================================================

	// Write options: WAL is always enabled (not configurable for crash safety)
	writeOpts := grocksdb.NewDefaultWriteOptions()
	writeOpts.SetSync(false) // Async writes for performance (WAL handles durability)

	// Read options: Single shared instance for all reads
	readOpts := grocksdb.NewDefaultReadOptions()

	// =========================================================================
	// Build CF Index Map
	// =========================================================================
	cfIndexMap := make(map[string]int)
	for i, name := range cfNames {
		cfIndexMap[name] = i
	}

	// =========================================================================
	// Log Configuration
	// =========================================================================
	memtables := settings.WriteBufferSizeMB * settings.MaxWriteBufferNumber * len(ColumnFamilyNames)
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
func createCFOptions(settings *RocksDBSettings, blockCache *grocksdb.Cache) *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()

	// =========================================================================
	// MemTable Configuration
	// =========================================================================
	opts.SetWriteBufferSize(uint64(settings.WriteBufferSizeMB * MB))
	opts.SetMaxWriteBufferNumber(settings.MaxWriteBufferNumber)
	opts.SetMinWriteBufferNumberToMerge(settings.MinWriteBufferNumberToMerge)

	// =========================================================================
	// Compaction Configuration
	// =========================================================================
	// CRITICAL: Disable auto-compaction during ingestion
	// Compaction is performed explicitly after ingestion completes
	opts.SetDisableAutoCompactions(true)

	opts.SetCompactionStyle(grocksdb.LevelCompactionStyle)
	opts.SetTargetFileSizeBase(uint64(settings.TargetFileSizeMB * MB))
	opts.SetTargetFileSizeMultiplier(1)

	// These settings apply during explicit compaction
	opts.SetMaxBytesForLevelBase(uint64(settings.TargetFileSizeMB * MB * 10))
	opts.SetMaxBytesForLevelMultiplier(10)

	// =========================================================================
	// Compression
	// =========================================================================
	// No compression: values are only 4 bytes, compression adds overhead
	opts.SetCompression(grocksdb.NoCompression)

	// =========================================================================
	// Block-Based Table Options
	// =========================================================================
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
//
// ATOMICITY:
//
//	The entire batch is written atomically. Either all entries are written
//	or none are (in case of error).
//
// IDEMPOTENCY:
//
//	Writing the same key-value pair multiple times is safe. RocksDB keeps
//	the latest value, and compaction removes duplicates.
func (s *RocksDBTxHashStore) WriteBatch(entriesByCF map[string][]Entry) error {
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
//
// RETURNS:
//   - value: 4-byte ledger sequence (big-endian), or nil if not found
//   - found: true if the key exists, false otherwise
//   - err: non-nil if a RocksDB error occurred
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
//
// The caller is responsible for calling Close() on the returned iterator.
func (s *RocksDBTxHashStore) NewIteratorCF(cfName string) Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cfHandle := s.getCFHandleByName(cfName)
	iter := s.db.NewIteratorCF(s.readOpts, cfHandle)

	return &rocksDBIterator{iter: iter}
}

// FlushAll flushes all column family MemTables to SST files on disk.
func (s *RocksDBTxHashStore) FlushAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	flushOpts := grocksdb.NewDefaultFlushOptions()
	defer flushOpts.Destroy()
	flushOpts.SetWait(true) // Block until flush completes

	s.logger.Info("Flushing all column families to disk...")

	for i, cfName := range ColumnFamilyNames {
		cfHandle := s.cfHandles[i+1] // +1 because index 0 is "default"
		if err := s.db.FlushCF(cfHandle, flushOpts); err != nil {
			return fmt.Errorf("failed to flush CF %s: %w", cfName, err)
		}
	}

	s.logger.Info("All column families flushed successfully")
	return nil
}

// CompactAll performs full compaction on all 16 column families.
//
// WHAT COMPACTION DOES:
//   - Merges all L0 files into sorted, non-overlapping files
//   - Removes duplicate keys (keeps latest value)
//   - Organizes data into levels (typically L6 after full compaction)
//
// Returns the total time spent compacting all CFs.
func (s *RocksDBTxHashStore) CompactAll() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Separator()
	s.logger.Info("                    COMPACTING ALL COLUMN FAMILIES")
	s.logger.Separator()

	totalStart := time.Now()

	for i, cfName := range ColumnFamilyNames {
		s.logger.Info("Compacting CF [%s]...", cfName)
		start := time.Now()

		cfHandle := s.cfHandles[i+1] // +1 because index 0 is "default"
		s.db.CompactRangeCF(cfHandle, grocksdb.Range{Start: nil, Limit: nil})

		elapsed := time.Since(start)
		s.logger.Info("  CF [%s] compacted in %v", cfName, elapsed)
	}

	totalTime := time.Since(totalStart)
	s.logger.Info("")
	s.logger.Info("All column families compacted in %v", totalTime)

	return totalTime
}

// CompactCF compacts a single column family by name.
func (s *RocksDBTxHashStore) CompactCF(cfName string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	start := time.Now()

	cfHandle := s.getCFHandleByName(cfName)
	s.db.CompactRangeCF(cfHandle, grocksdb.Range{Start: nil, Limit: nil})

	return time.Since(start)
}

// GetAllCFStats returns statistics for all 16 column families.
func (s *RocksDBTxHashStore) GetAllCFStats() []CFStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := make([]CFStats, len(ColumnFamilyNames))

	for i, cfName := range ColumnFamilyNames {
		stats[i] = s.getCFStatsLocked(cfName)
	}

	return stats
}

// GetCFStats returns statistics for a specific column family.
func (s *RocksDBTxHashStore) GetCFStats(cfName string) CFStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.getCFStatsLocked(cfName)
}

// getCFStatsLocked returns CF stats (caller must hold lock).
func (s *RocksDBTxHashStore) getCFStatsLocked(cfName string) CFStats {
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
	levelStats := make([]CFLevelStats, 7) // L0-L6
	var totalFiles, totalSize int64

	for level := 0; level <= 6; level++ {
		filesProp := fmt.Sprintf("rocksdb.num-files-at-level%d", level)
		filesStr := s.db.GetPropertyCF(filesProp, cfHandle)
		var files int64
		fmt.Sscanf(filesStr, "%d", &files)

		// Note: Per-level size requires parsing rocksdb.levelstats or similar
		levelStats[level] = CFLevelStats{
			Level:     level,
			FileCount: files,
		}

		totalFiles += files
	}

	// Get total size
	totalSizeStr := s.db.GetPropertyCF("rocksdb.total-sst-files-size", cfHandle)
	fmt.Sscanf(totalSizeStr, "%d", &totalSize)

	return CFStats{
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
	cfName := GetColumnFamilyName(txHash)
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
// Compile-Time Interface Check
// =============================================================================

var _ TxHashStore = (*RocksDBTxHashStore)(nil)

// =============================================================================
// Utility Functions
// =============================================================================

// LogAllCFStats logs statistics for all column families.
func LogAllCFStats(store TxHashStore, logger Logger, label string) {
	stats := store.GetAllCFStats()

	logger.Separator()
	logger.Info("                    %s", label)
	logger.Separator()
	logger.Info("")

	var totalKeys, totalSize, totalFiles int64

	logger.Info("%-4s %15s %15s %10s", "CF", "Est. Keys", "Size", "Files")
	logger.Info("%-4s %15s %15s %10s", "----", "---------------", "---------------", "----------")

	for _, cf := range stats {
		logger.Info("%-4s %15s %15s %10d",
			cf.Name,
			FormatCount(cf.EstimatedKeys),
			FormatBytes(cf.TotalSize),
			cf.TotalFiles)

		totalKeys += cf.EstimatedKeys
		totalSize += cf.TotalSize
		totalFiles += cf.TotalFiles
	}

	logger.Info("%-4s %15s %15s %10s", "----", "---------------", "---------------", "----------")
	logger.Info("%-4s %15s %15s %10d", "TOT", FormatCount(totalKeys), FormatBytes(totalSize), totalFiles)
	logger.Info("")
}

// LogCFLevelStats logs the level distribution for all column families.
func LogCFLevelStats(store TxHashStore, logger Logger) {
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

	for _, cf := range stats {
		line := fmt.Sprintf("%-3s ", cf.Name)
		for _, ls := range cf.LevelStats {
			line += fmt.Sprintf("%5d", ls.FileCount)
		}
		logger.Info("%s", line)
	}
	logger.Info("")
}

// ParseLedgerSeq converts a 4-byte big-endian value to uint32.
func ParseLedgerSeq(value []byte) uint32 {
	if len(value) != 4 {
		return 0
	}
	return uint32(value[0])<<24 | uint32(value[1])<<16 | uint32(value[2])<<8 | uint32(value[3])
}

// EncodeLedgerSeq converts a uint32 to 4-byte big-endian.
func EncodeLedgerSeq(ledgerSeq uint32) []byte {
	return []byte{
		byte(ledgerSeq >> 24),
		byte(ledgerSeq >> 16),
		byte(ledgerSeq >> 8),
		byte(ledgerSeq),
	}
}

// HexToBytes converts a hex string to bytes.
// Returns nil if the string is not valid hex.
func HexToBytes(hexStr string) []byte {
	if len(hexStr)%2 != 0 {
		return nil
	}

	bytes := make([]byte, len(hexStr)/2)
	for i := 0; i < len(hexStr); i += 2 {
		b, err := strconv.ParseUint(hexStr[i:i+2], 16, 8)
		if err != nil {
			return nil
		}
		bytes[i/2] = byte(b)
	}
	return bytes
}

// BytesToHex converts bytes to a hex string.
func BytesToHex(bytes []byte) string {
	const hexChars = "0123456789abcdef"
	result := make([]byte, len(bytes)*2)
	for i, b := range bytes {
		result[i*2] = hexChars[b>>4]
		result[i*2+1] = hexChars[b&0x0f]
	}
	return string(result)
}
