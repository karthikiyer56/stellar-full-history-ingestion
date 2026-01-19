// stores.go
// =============================================================================
// RocksDB Store Management with Column Family Support
// =============================================================================
//
// This module handles:
// - Opening source RocksDB stores for reading (default column family)
// - Opening/creating the output RocksDB store with 16 column families
// - Writing batches to column families
// - Final compaction
// - Generating OPTIONS file for active use phase
//
// =============================================================================

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/linxGnu/grocksdb"
	"github.com/pkg/errors"
)

const (
	MB = 1024 * 1024
	GB = 1024 * 1024 * 1024

	// SourceBlockCacheSizeMB for reading from source stores
	SourceBlockCacheSizeMB = 512
)

// =============================================================================
// Source Store (Read-Only)
// =============================================================================

// SourceStore represents a read-only source tx_hash_to_ledger_seq store.
type SourceStore struct {
	DB   *grocksdb.DB
	Opts *grocksdb.Options
	RO   *grocksdb.ReadOptions
	Path string
}

// OpenSourceStore opens a source RocksDB store for reading.
func OpenSourceStore(path string) (*SourceStore, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	// Block-based table options with block cache for reading
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	cache := grocksdb.NewLRUCache(uint64(SourceBlockCacheSizeMB * MB))
	bbto.SetBlockCache(cache)
	opts.SetBlockBasedTableFactory(bbto)

	db, err := grocksdb.OpenDbForReadOnly(opts, path, false)
	if err != nil {
		opts.Destroy()
		return nil, errors.Wrapf(err, "failed to open source store at %s", path)
	}

	ro := grocksdb.NewDefaultReadOptions()

	return &SourceStore{
		DB:   db,
		Opts: opts,
		RO:   ro,
		Path: path,
	}, nil
}

// Close closes the source store.
func (s *SourceStore) Close() {
	if s.RO != nil {
		s.RO.Destroy()
	}
	if s.DB != nil {
		s.DB.Close()
	}
	if s.Opts != nil {
		s.Opts.Destroy()
	}
}

// NewIterator creates a new iterator for the source store.
func (s *SourceStore) NewIterator() *grocksdb.Iterator {
	return s.DB.NewIterator(s.RO)
}

// =============================================================================
// Output Store with Column Families
// =============================================================================

// OutputStore represents the output RocksDB store with 16 column families.
type OutputStore struct {
	DB          *grocksdb.DB
	Opts        *grocksdb.Options
	CFHandles   []*grocksdb.ColumnFamilyHandle
	CFOpts      []*grocksdb.Options
	WriteOpts   *grocksdb.WriteOptions
	BlockCache  *grocksdb.Cache
	Path        string
	CFIndexMap  map[string]int // Map CF name -> index in CFHandles
	BulkLoadCfg *RocksDBSettings
}

// OpenOutputStore opens or creates the output store with 16 column families.
// Uses bulk-load optimized settings from config.RocksDB.
func OpenOutputStore(config *Config) (*OutputStore, error) {
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
		return nil, errors.Wrapf(err, "failed to open output store at %s", config.OutputPath)
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
	log.Printf("Opened output store with %d column families at: %s", len(cfHandles), config.OutputPath)
	log.Printf("  MemTable RAM: %d MB (%d MB × %d × 16 CFs)",
		memtables, settings.WriteBufferSizeMB, settings.MaxWriteBufferNumber)
	log.Printf("  Block Cache:  %d MB", settings.BlockCacheSizeMB)
	log.Printf("  WAL:          %s", map[bool]string{true: "DISABLED", false: "ENABLED"}[settings.DisableWAL])

	return &OutputStore{
		DB:          db,
		Opts:        opts,
		CFHandles:   cfHandles,
		CFOpts:      cfOptsList,
		WriteOpts:   writeOpts,
		BlockCache:  blockCache,
		Path:        config.OutputPath,
		CFIndexMap:  cfIndexMap,
		BulkLoadCfg: settings,
	}, nil
}

// createColumnFamilyOptions creates options for a column family during bulk load.
func createColumnFamilyOptions(settings *RocksDBSettings, blockCache *grocksdb.Cache) *grocksdb.Options {
	opts := grocksdb.NewDefaultOptions()

	// =========================================================================
	// Write Buffer (MemTable) Configuration
	// =========================================================================
	opts.SetWriteBufferSize(uint64(settings.WriteBufferSizeMB * MB))
	opts.SetMaxWriteBufferNumber(settings.MaxWriteBufferNumber)
	opts.SetMinWriteBufferNumberToMerge(2)

	// =========================================================================
	// L0 Management (disable compaction during bulk load)
	// =========================================================================
	opts.SetLevel0FileNumCompactionTrigger(settings.L0CompactionTrigger)
	opts.SetLevel0SlowdownWritesTrigger(settings.L0SlowdownWritesTrigger)
	opts.SetLevel0StopWritesTrigger(settings.L0StopWritesTrigger)

	// =========================================================================
	// Compaction Configuration
	// =========================================================================
	opts.SetDisableAutoCompactions(true)
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

// Close closes the output store.
func (s *OutputStore) Close() {
	if s.WriteOpts != nil {
		s.WriteOpts.Destroy()
	}
	for _, cfHandle := range s.CFHandles {
		if cfHandle != nil {
			cfHandle.Destroy()
		}
	}
	if s.DB != nil {
		s.DB.Close()
	}
	if s.Opts != nil {
		s.Opts.Destroy()
	}
	for _, cfOpt := range s.CFOpts {
		if cfOpt != nil {
			cfOpt.Destroy()
		}
	}
	if s.BlockCache != nil {
		s.BlockCache.Destroy()
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
// entries is a map: cfName -> list of (key, value) pairs
type Entry struct {
	Key   []byte
	Value []byte
}

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

// CompactAll performs compaction on all column families.
func (s *OutputStore) CompactAll() time.Duration {
	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("                         PERFORMING FINAL COMPACTION")
	log.Printf("================================================================================")
	log.Printf("")

	totalStart := time.Now()

	for i, cfName := range ColumnFamilyNames {
		log.Printf("Compacting column family [%s]...", cfName)
		start := time.Now()

		cfHandle := s.CFHandles[i+1] // +1 because index 0 is "default"
		s.DB.CompactRangeCF(cfHandle, grocksdb.Range{Start: nil, Limit: nil})

		elapsed := time.Since(start)
		log.Printf("  Completed in %s", helpers.FormatDuration(elapsed))
	}

	totalTime := time.Since(totalStart)
	log.Printf("")
	log.Printf("All column families compacted in %s", helpers.FormatDuration(totalTime))
	log.Printf("")

	return totalTime
}

// GetStats returns statistics for the output store.
func (s *OutputStore) GetStats() map[string]int64 {
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

// =============================================================================
// Active Store Configuration Generation
// =============================================================================

// GenerateActiveStoreConfig writes a configuration file for reopening the store
// in active mode (with WAL enabled, normal compaction, etc.)
func GenerateActiveStoreConfig(config *Config) error {
	active := &config.ActiveStore
	rocksdb := &config.RocksDB

	configContent := fmt.Sprintf(`# =============================================================================
# ACTIVE STORE CONFIGURATION
# =============================================================================
# Generated after bulk load. Use these settings when opening the store for
# real-time ingestion and queries.
#
# IMPORTANT: These settings enable WAL and auto-compaction for production use.
# =============================================================================

# Store path (same as bulk load output)
store_path = "%s"

[rocksdb]
# -------------------------------------------------------------------------
# WRITE BUFFER (MEMTABLE) SETTINGS
# -------------------------------------------------------------------------
# Total RAM = write_buffer_size_mb × max_write_buffer_number × 16 CFs
# With defaults: %d MB × %d × 16 = %d MB
# -------------------------------------------------------------------------
write_buffer_size_mb = %d
max_write_buffer_number = %d

# -------------------------------------------------------------------------
# L0 COMPACTION TRIGGERS (ACTIVE MODE)
# -------------------------------------------------------------------------
# These trigger background compaction to maintain read performance.
# - l0_compaction_trigger = 4: Compact when 4 L0 files accumulate
# - l0_slowdown_writes_trigger = 20: Slow writes at 20 L0 files
# - l0_stop_writes_trigger = 36: Stop writes at 36 L0 files
# -------------------------------------------------------------------------
l0_compaction_trigger = %d
l0_slowdown_writes_trigger = %d
l0_stop_writes_trigger = %d

# -------------------------------------------------------------------------
# SST FILE SETTINGS (same as bulk load)
# -------------------------------------------------------------------------
target_file_size_mb = %d
max_bytes_for_level_base_mb = %d

# -------------------------------------------------------------------------
# RESOURCE LIMITS
# -------------------------------------------------------------------------
max_background_jobs = %d
max_open_files = %d

# -------------------------------------------------------------------------
# BLOOM FILTER
# -------------------------------------------------------------------------
bloom_filter_bits_per_key = %d

# -------------------------------------------------------------------------
# BLOCK CACHE (LARGE for read performance)
# -------------------------------------------------------------------------
# 16 GB block cache for caching frequently accessed data
block_cache_size_mb = %d

# -------------------------------------------------------------------------
# WAL (WRITE-AHEAD LOG) - ENABLED for crash recovery
# -------------------------------------------------------------------------
disable_wal = false

# -------------------------------------------------------------------------
# AUTO-COMPACTION - ENABLED for active use
# -------------------------------------------------------------------------
disable_auto_compactions = false
`,
		config.OutputPath,
		active.WriteBufferSizeMB, active.MaxWriteBufferNumber,
		active.WriteBufferSizeMB*active.MaxWriteBufferNumber*16,
		active.WriteBufferSizeMB,
		active.MaxWriteBufferNumber,
		active.L0CompactionTrigger,
		active.L0SlowdownWritesTrigger,
		active.L0StopWritesTrigger,
		rocksdb.TargetFileSizeMB,
		rocksdb.MaxBytesForLevelBaseMB,
		rocksdb.MaxBackgroundJobs,
		rocksdb.MaxOpenFiles,
		rocksdb.BloomFilterBitsPerKey,
		active.BlockCacheSizeMB,
	)

	// Write to file next to the store
	configPath := filepath.Join(config.OutputPath, "active_store_config.toml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		return errors.Wrapf(err, "failed to write active store config to %s", configPath)
	}

	log.Printf("Generated active store configuration: %s", configPath)
	return nil
}

// PrintActiveStoreInstructions prints instructions for using the store in active mode.
func PrintActiveStoreInstructions(config *Config) {
	active := &config.ActiveStore

	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("                    ACTIVE STORE USAGE INSTRUCTIONS")
	log.Printf("================================================================================")
	log.Printf("")
	log.Printf("The bulk load is complete. To use this store for real-time ingestion")
	log.Printf("and queries, reopen it with the following settings:")
	log.Printf("")
	log.Printf("KEY CHANGES FROM BULK LOAD:")
	log.Printf("  - WAL:              ENABLED (for crash recovery)")
	log.Printf("  - Auto-Compaction:  ENABLED (for balanced read/write)")
	log.Printf("  - L0 Triggers:      %d / %d / %d (compact/slow/stop)",
		active.L0CompactionTrigger, active.L0SlowdownWritesTrigger, active.L0StopWritesTrigger)
	log.Printf("  - Block Cache:      %d MB (for read performance)", active.BlockCacheSizeMB)
	log.Printf("  - MemTables:        %d MB × %d × 16 = %d MB",
		active.WriteBufferSizeMB, active.MaxWriteBufferNumber,
		active.WriteBufferSizeMB*active.MaxWriteBufferNumber*16)
	log.Printf("")
	log.Printf("MEMORY USAGE ESTIMATE (Active Mode):")
	log.Printf("  - MemTables:        %d MB", active.WriteBufferSizeMB*active.MaxWriteBufferNumber*16)
	log.Printf("  - Block Cache:      %d MB", active.BlockCacheSizeMB)
	log.Printf("  - Bloom Filters:    Loaded on demand per SST file")
	log.Printf("  - Total:            ~%d MB + bloom filters",
		active.WriteBufferSizeMB*active.MaxWriteBufferNumber*16+active.BlockCacheSizeMB)
	log.Printf("")
	log.Printf("See: %s/active_store_config.toml", config.OutputPath)
	log.Printf("")
	log.Printf("================================================================================")
}
