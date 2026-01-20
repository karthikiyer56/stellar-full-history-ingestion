package txhashrework

import (
	"fmt"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/linxGnu/grocksdb"
)

const (
	MB = 1024 * 1024
	GB = 1024 * 1024 * 1024
)

// =============================================================================
// Entry represents a key-value pair for batch writes
// =============================================================================

// Entry represents a tx_hash -> ledger_seq mapping.
type Entry struct {
	Key   []byte // 32-byte transaction hash
	Value []byte // 4-byte ledger sequence (big-endian)
}

// =============================================================================
// OutputStore - RocksDB Store with 16 Column Families
// =============================================================================

// OutputStore represents the output RocksDB store with 16 column families.
// Each column family handles transactions whose hash starts with a specific
// hex character (0-9, a-f).
//
// This store is designed for realtime ingestion:
//   - WAL enabled for crash recovery
//   - No automatic compaction (use compact-store utility when ready)
//   - 16GB memtables + 8GB block cache (24GB total)
//   - 12-bit bloom filters for better read performance
type OutputStore struct {
	DB         *grocksdb.DB
	Opts       *grocksdb.Options
	CFHandles  []*grocksdb.ColumnFamilyHandle
	CFOpts     []*grocksdb.Options
	WriteOpts  *grocksdb.WriteOptions
	BlockCache *grocksdb.Cache
	Path       string
	CFIndexMap map[string]int // Map CF name -> index in CFHandles
	Settings   *RocksDBSettings
	Logger     *Logger

	mu sync.Mutex
}

// OpenOutputStore opens or creates the output store with 16 column families.
// Uses settings from config.RocksDB.
func OpenOutputStore(config *Config, logger *Logger) (*OutputStore, error) {
	settings := &config.RocksDB

	// Create shared block cache
	var blockCache *grocksdb.Cache
	if settings.BlockCacheSizeMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(settings.BlockCacheSizeMB * MB))
	}

	// Create options for the database
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetErrorIfExists(false)

	// =========================================================================
	// Global Database Options
	// =========================================================================
	opts.SetMaxBackgroundJobs(settings.MaxBackgroundJobs)
	opts.SetMaxOpenFiles(settings.MaxOpenFiles)
	opts.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)
	opts.SetMaxLogFileSize(20 * MB)
	opts.SetKeepLogFileNum(3)

	// Prepare column family names (16 CFs + default)
	// Note: "default" CF is required by RocksDB
	cfNames := []string{"default"}
	cfNames = append(cfNames, ColumnFamilyNames...)

	// Create options for each CF
	cfOptsList := make([]*grocksdb.Options, len(cfNames))
	cfOptsList[0] = grocksdb.NewDefaultOptions() // default CF uses minimal options
	for i := 1; i < len(cfNames); i++ {
		cfOptsList[i] = createColumnFamilyOptions(settings, blockCache)
	}

	// =========================================================================
	// Open Database with Column Families
	// =========================================================================
	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(opts, config.OutputPath, cfNames, cfOptsList)
	if err != nil {
		opts.Destroy()
		for _, cfOpt := range cfOptsList {
			if cfOpt != nil {
				cfOpt.Destroy()
			}
		}
		if blockCache != nil {
			blockCache.Destroy()
		}
		return nil, fmt.Errorf("failed to open output store at %s: %w", config.OutputPath, err)
	}

	// Create write options
	writeOpts := grocksdb.NewDefaultWriteOptions()
	if settings.DisableWAL {
		writeOpts.DisableWAL(true)
	}

	// Build CF index map
	cfIndexMap := make(map[string]int)
	for i, name := range cfNames {
		cfIndexMap[name] = i
	}

	// Calculate memory usage
	memtables := settings.WriteBufferSizeMB * settings.MaxWriteBufferNumber * 16
	logger.Info("Opened output store with %d column families at: %s", len(cfHandles), config.OutputPath)
	logger.Info("  MemTable RAM: %d MB (%d MB x %d x 16 CFs)",
		memtables, settings.WriteBufferSizeMB, settings.MaxWriteBufferNumber)
	logger.Info("  Block Cache:  %d MB", settings.BlockCacheSizeMB)
	logger.Info("  Bloom Filter: %d bits/key", settings.BloomFilterBitsPerKey)
	logger.Info("  WAL:          %s", map[bool]string{true: "DISABLED", false: "ENABLED"}[settings.DisableWAL])
	logger.Info("  Compaction:   %s", map[bool]string{true: "DISABLED (manual)", false: "ENABLED (auto)"}[settings.DisableAutoCompactions])

	return &OutputStore{
		DB:         db,
		Opts:       opts,
		CFHandles:  cfHandles,
		CFOpts:     cfOptsList,
		WriteOpts:  writeOpts,
		BlockCache: blockCache,
		Path:       config.OutputPath,
		CFIndexMap: cfIndexMap,
		Settings:   settings,
		Logger:     logger,
	}, nil
}

// createColumnFamilyOptions creates options for a column family.
func createColumnFamilyOptions(settings *RocksDBSettings, blockCache *grocksdb.Cache) *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()

	// =========================================================================
	// Write Buffer (MemTable) Configuration
	// =========================================================================
	opts.SetWriteBufferSize(uint64(settings.WriteBufferSizeMB * MB))
	opts.SetMaxWriteBufferNumber(settings.MaxWriteBufferNumber)
	opts.SetMinWriteBufferNumberToMerge(4)

	// =========================================================================
	// L0 Management
	// =========================================================================
	opts.SetLevel0FileNumCompactionTrigger(settings.L0CompactionTrigger)
	opts.SetLevel0SlowdownWritesTrigger(settings.L0SlowdownWritesTrigger)
	opts.SetLevel0StopWritesTrigger(settings.L0StopWritesTrigger)

	// =========================================================================
	// Compaction Configuration
	// =========================================================================
	opts.SetDisableAutoCompactions(settings.DisableAutoCompactions)
	opts.SetCompactionStyle(grocksdb.LevelCompactionStyle)
	opts.SetTargetFileSizeBase(uint64(settings.TargetFileSizeMB * MB))
	opts.SetTargetFileSizeMultiplier(1)
	opts.SetMaxBytesForLevelBase(uint64(settings.MaxBytesForLevelBaseMB * MB))
	opts.SetMaxBytesForLevelMultiplier(10)

	// =========================================================================
	// No compression (values are only 4 bytes, compression adds overhead)
	// =========================================================================
	opts.SetCompression(grocksdb.NoCompression)

	// =========================================================================
	// Block-Based Table Options with Bloom Filter and Block Cache
	// =========================================================================
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	if settings.BloomFilterBitsPerKey > 0 {
		bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(settings.BloomFilterBitsPerKey)))
	}
	if blockCache != nil {
		bbto.SetBlockCache(blockCache)
	}
	opts.SetBlockBasedTableFactory(bbto)

	return opts
}

// Close closes the output store and releases all resources.
func (s *OutputStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.WriteOpts != nil {
		s.WriteOpts.Destroy()
		s.WriteOpts = nil
	}
	for _, cfHandle := range s.CFHandles {
		if cfHandle != nil {
			cfHandle.Destroy()
		}
	}
	s.CFHandles = nil

	if s.DB != nil {
		s.DB.Close()
		s.DB = nil
	}
	if s.Opts != nil {
		s.Opts.Destroy()
		s.Opts = nil
	}
	for _, cfOpt := range s.CFOpts {
		if cfOpt != nil {
			cfOpt.Destroy()
		}
	}
	s.CFOpts = nil

	if s.BlockCache != nil {
		s.BlockCache.Destroy()
		s.BlockCache = nil
	}
}

// GetCFHandle returns the column family handle for a transaction hash.
func (s *OutputStore) GetCFHandle(txHash []byte) *grocksdb.ColumnFamilyHandle {
	cfName := GetColumnFamilyName(txHash)
	idx := s.CFIndexMap[cfName]
	return s.CFHandles[idx]
}

// GetCFHandleByName returns the column family handle by name.
func (s *OutputStore) GetCFHandleByName(cfName string) *grocksdb.ColumnFamilyHandle {
	idx, ok := s.CFIndexMap[cfName]
	if !ok {
		return s.CFHandles[0] // default
	}
	return s.CFHandles[idx]
}

// WriteBatch writes a batch of entries to the appropriate column families.
// entriesByCF is a map: cfName -> list of (key, value) pairs
func (s *OutputStore) WriteBatch(entriesByCF map[string][]Entry) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for cfName, entries := range entriesByCF {
		cfHandle := s.GetCFHandleByName(cfName)
		for _, entry := range entries {
			batch.PutCF(cfHandle, entry.Key, entry.Value)
		}
	}

	return s.DB.Write(s.WriteOpts, batch)
}

// WriteBatchRaw writes a single batch directly.
func (s *OutputStore) WriteBatchRaw(batch *grocksdb.WriteBatch) error {
	return s.DB.Write(s.WriteOpts, batch)
}

// Get retrieves the ledger sequence for a transaction hash.
// Returns nil if not found.
func (s *OutputStore) Get(txHash []byte) ([]byte, error) {
	cfHandle := s.GetCFHandle(txHash)
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	slice, err := s.DB.GetCF(ro, cfHandle, txHash)
	if err != nil {
		return nil, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil, nil
	}

	// Make a copy since slice will be freed
	value := make([]byte, slice.Size())
	copy(value, slice.Data())
	return value, nil
}

// CompactAll performs compaction on all column families.
// Returns the total time spent compacting.
func (s *OutputStore) CompactAll() time.Duration {
	s.Logger.Info("")
	s.Logger.Info("================================================================================")
	s.Logger.Info("                         PERFORMING FINAL COMPACTION")
	s.Logger.Info("================================================================================")
	s.Logger.Info("")

	totalStart := time.Now()

	for i, cfName := range ColumnFamilyNames {
		s.Logger.Info("Compacting column family [%s]...", cfName)
		start := time.Now()

		cfHandle := s.CFHandles[i+1] // +1 because index 0 is "default"
		s.DB.CompactRangeCF(cfHandle, grocksdb.Range{Start: nil, Limit: nil})

		elapsed := time.Since(start)
		s.Logger.Info("  Completed in %s", helpers.FormatDuration(elapsed))
	}

	totalTime := time.Since(totalStart)
	s.Logger.Info("")
	s.Logger.Info("All column families compacted in %s", helpers.FormatDuration(totalTime))
	s.Logger.Info("")

	return totalTime
}

// CompactColumnFamily compacts a single column family by name.
func (s *OutputStore) CompactColumnFamily(cfName string) time.Duration {
	s.Logger.Info("Compacting column family [%s]...", cfName)
	start := time.Now()

	cfHandle := s.GetCFHandleByName(cfName)
	s.DB.CompactRangeCF(cfHandle, grocksdb.Range{Start: nil, Limit: nil})

	elapsed := time.Since(start)
	s.Logger.Info("  Completed in %s", helpers.FormatDuration(elapsed))
	return elapsed
}

// GetEstimatedKeyCount returns estimated key counts per column family.
func (s *OutputStore) GetEstimatedKeyCount() map[string]int64 {
	stats := make(map[string]int64)

	for _, cfName := range ColumnFamilyNames {
		cfHandle := s.GetCFHandleByName(cfName)
		prop := s.DB.GetPropertyCF("rocksdb.estimate-num-keys", cfHandle)
		var count int64
		fmt.Sscanf(prop, "%d", &count)
		stats[cfName] = count
	}

	return stats
}

// FlushAll flushes all column families to disk.
func (s *OutputStore) FlushAll() error {
	flushOpts := grocksdb.NewDefaultFlushOptions()
	defer flushOpts.Destroy()
	flushOpts.SetWait(true)

	for i, cfName := range ColumnFamilyNames {
		cfHandle := s.CFHandles[i+1]
		if err := s.DB.FlushCF(cfHandle, flushOpts); err != nil {
			return fmt.Errorf("failed to flush CF %s: %w", cfName, err)
		}
	}

	return nil
}

// =============================================================================
// InputStore - Read-only RocksDB Store for Merging
// =============================================================================

// InputStore represents a read-only source RocksDB store.
// Used when merging multiple stores.
type InputStore struct {
	DB         *grocksdb.DB
	Opts       *grocksdb.Options
	RO         *grocksdb.ReadOptions
	CFHandles  []*grocksdb.ColumnFamilyHandle
	CFOpts     []*grocksdb.Options
	BlockCache *grocksdb.Cache
	Path       string
	CFIndexMap map[string]int
}

// OpenInputStore opens an existing RocksDB store for reading.
// The store must have the same 16 column families structure.
func OpenInputStore(path string, blockCacheSizeMB int) (*InputStore, error) {
	// Create block cache for reading
	var blockCache *grocksdb.Cache
	if blockCacheSizeMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(blockCacheSizeMB * MB))
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	// Prepare column family names
	cfNames := []string{"default"}
	cfNames = append(cfNames, ColumnFamilyNames...)

	// Create minimal options for each CF (read-only)
	cfOptsList := make([]*grocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts := grocksdb.NewDefaultOptions()

		// Block-based table options with cache
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		if blockCache != nil {
			bbto.SetBlockCache(blockCache)
		}
		cfOpts.SetBlockBasedTableFactory(bbto)

		cfOptsList[i] = cfOpts
	}

	db, cfHandles, err := grocksdb.OpenDbForReadOnlyColumnFamilies(opts, path, cfNames, cfOptsList, false)
	if err != nil {
		opts.Destroy()
		for _, cfOpt := range cfOptsList {
			if cfOpt != nil {
				cfOpt.Destroy()
			}
		}
		if blockCache != nil {
			blockCache.Destroy()
		}
		return nil, fmt.Errorf("failed to open input store at %s: %w", path, err)
	}

	// Build CF index map
	cfIndexMap := make(map[string]int)
	for i, name := range cfNames {
		cfIndexMap[name] = i
	}

	ro := grocksdb.NewDefaultReadOptions()

	return &InputStore{
		DB:         db,
		Opts:       opts,
		RO:         ro,
		CFHandles:  cfHandles,
		CFOpts:     cfOptsList,
		BlockCache: blockCache,
		Path:       path,
		CFIndexMap: cfIndexMap,
	}, nil
}

// Close closes the input store.
func (s *InputStore) Close() {
	if s.RO != nil {
		s.RO.Destroy()
		s.RO = nil
	}
	for _, cfHandle := range s.CFHandles {
		if cfHandle != nil {
			cfHandle.Destroy()
		}
	}
	s.CFHandles = nil

	if s.DB != nil {
		s.DB.Close()
		s.DB = nil
	}
	if s.Opts != nil {
		s.Opts.Destroy()
		s.Opts = nil
	}
	for _, cfOpt := range s.CFOpts {
		if cfOpt != nil {
			cfOpt.Destroy()
		}
	}
	s.CFOpts = nil

	if s.BlockCache != nil {
		s.BlockCache.Destroy()
		s.BlockCache = nil
	}
}

// GetCFHandleByName returns the column family handle by name.
func (s *InputStore) GetCFHandleByName(cfName string) *grocksdb.ColumnFamilyHandle {
	idx, ok := s.CFIndexMap[cfName]
	if !ok {
		return s.CFHandles[0]
	}
	return s.CFHandles[idx]
}

// NewIteratorCF creates a new iterator for a specific column family.
func (s *InputStore) NewIteratorCF(cfName string) *grocksdb.Iterator {
	cfHandle := s.GetCFHandleByName(cfName)
	return s.DB.NewIteratorCF(s.RO, cfHandle)
}
