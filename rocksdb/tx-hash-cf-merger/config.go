// config.go
// =============================================================================
// Configuration Management for TX Hash Column Family Merger
// =============================================================================
//
// This tool supports a two-phase workflow:
//
// PHASE 1 (BULK LOAD): Merge multiple existing stores into one with CFs
//   - WAL disabled for speed
//   - Auto-compaction disabled (one final compaction)
//   - High L0 triggers to prevent write stalls
//
// PHASE 2 (ACTIVE USE): The merged store is used for real-time ingestion + queries
//   - WAL enabled for crash recovery
//   - Auto-compaction enabled
//   - Normal L0 triggers for balanced read/write performance
//   - Block cache for read performance
//
// =============================================================================

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/BurntSushi/toml"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers/lfs"
)

// =============================================================================
// Configuration Structures
// =============================================================================

// Config represents the complete TOML configuration.
type Config struct {
	// ==========================================================================
	// Input Source Option 1: Existing RocksDB Stores
	// ==========================================================================

	// InputStores is a list of paths to existing tx_hash_to_ledger_seq RocksDB stores.
	// Each store will be read sequentially and merged into the output store.
	// MUTUALLY EXCLUSIVE with LfsLedgerStorePath.
	InputStores []string `toml:"input_stores"`

	// ==========================================================================
	// Input Source Option 2: Local Filesystem (LFS) Ledger Store
	// ==========================================================================

	// LfsLedgerStorePath is the path to the LFS ledger store directory.
	// The tool will read ledgers from this store, extract transactions,
	// and build the tx_hash → ledger_seq mapping.
	// MUTUALLY EXCLUSIVE with InputStores.
	LfsLedgerStorePath string `toml:"lfs_ledger_store_path"`

	// LfsStartLedger is the first ledger sequence to process (inclusive).
	// If 0, auto-detect from the store.
	LfsStartLedger uint32 `toml:"lfs_start_ledger"`

	// LfsEndLedger is the last ledger sequence to process (inclusive).
	// If 0, auto-detect from the store.
	LfsEndLedger uint32 `toml:"lfs_end_ledger"`

	// LfsWorkers is the number of parallel workers to use for LFS processing.
	// Default: runtime.NumCPU()
	LfsWorkers int `toml:"lfs_workers"`

	// ==========================================================================
	// Output Configuration
	// ==========================================================================

	// OutputPath is the directory where the merged RocksDB store with column families
	// will be created.
	OutputPath string `toml:"output_path"`

	// BatchSize is the number of entries to accumulate before writing a batch.
	// Default: 100000
	BatchSize int `toml:"batch_size"`

	// RocksDB tuning parameters for BULK LOAD phase
	RocksDB RocksDBSettings `toml:"rocksdb"`

	// ActiveStore settings for ACTIVE USE phase (after bulk load completes)
	// These are applied when the store is reopened for real-time use
	ActiveStore ActiveStoreSettings `toml:"active_store"`

	// ==========================================================================
	// Runtime State (populated during validation, not from TOML)
	// ==========================================================================

	// IsLfsMode is true when using LFS input source (set during validation)
	IsLfsMode bool `toml:"-"`

	// LfsLedgerRange is the discovered/validated ledger range (set during validation)
	LfsLedgerRange *lfs.LedgerRange `toml:"-"`
}

// RocksDBSettings contains tunable RocksDB parameters for BULK LOAD phase.
// These settings prioritize write throughput over read performance.
type RocksDBSettings struct {
	// WriteBufferSizeMB controls the size of each MemTable in megabytes.
	// Applies to each column family separately.
	// Total MemTable RAM = WriteBufferSizeMB × MaxWriteBufferNumber × 16 CFs
	//
	// For 64GB RAM: 128 MB × 4 × 16 = 8 GB (reasonable for bulk load)
	// Default: 128 MB
	WriteBufferSizeMB int `toml:"write_buffer_size_mb"`

	// MaxWriteBufferNumber is the maximum number of MemTables per column family.
	// Default: 4
	MaxWriteBufferNumber int `toml:"max_write_buffer_number"`

	// L0CompactionTrigger: Number of L0 files that triggers compaction.
	// Set high (999) for bulk loading to disable compaction during writes.
	// Default: 999
	L0CompactionTrigger int `toml:"l0_compaction_trigger"`

	// L0SlowdownWritesTrigger: Number of L0 files that slows down writes.
	// Set high for bulk loading.
	// Default: 999
	L0SlowdownWritesTrigger int `toml:"l0_slowdown_writes_trigger"`

	// L0StopWritesTrigger: Number of L0 files that stops writes.
	// Set high for bulk loading.
	// Default: 999
	L0StopWritesTrigger int `toml:"l0_stop_writes_trigger"`

	// TargetFileSizeMB is the target size for SST files.
	// For 15B entries across 16 CFs (~37GB per CF), 256MB files = ~145 files per CF
	// Default: 256 MB
	TargetFileSizeMB int `toml:"target_file_size_mb"`

	// MaxBytesForLevelBaseMB is the maximum total size for L1 per column family.
	// L2 = L1 × 10, L3 = L2 × 10, etc.
	//
	// For ~37GB per CF: L1=2.5GB, L2=25GB, L3=250GB covers growth nicely
	// Default: 2560 MB (2.5 GB)
	MaxBytesForLevelBaseMB int `toml:"max_bytes_for_level_base_mb"`

	// MaxBackgroundJobs is the maximum number of concurrent background operations.
	// Used for flush and compaction. Higher = faster final compaction.
	// Default: 8
	MaxBackgroundJobs int `toml:"max_background_jobs"`

	// MaxOpenFiles limits the number of file handles.
	// With 16 CFs × ~150 files each = ~2400 files total, 10000 is plenty.
	// Default: 10000
	MaxOpenFiles int `toml:"max_open_files"`

	// BloomFilterBitsPerKey configures the bloom filter.
	// Essential for random key (hash) lookups.
	// 10 bits/key = ~1% false positive rate
	// Memory: 15B keys × 10 bits = ~18.75 GB (loaded on demand per SST)
	// Default: 10
	BloomFilterBitsPerKey int `toml:"bloom_filter_bits_per_key"`

	// DisableWAL disables the Write-Ahead Log during bulk load.
	// Safe because source data is intact; if crash, restart from scratch.
	// Default: true (disabled during bulk load)
	DisableWAL bool `toml:"disable_wal"`

	// BlockCacheSizeMB is the size of the block cache for reading.
	// During bulk load, this can be small since we're mostly writing.
	// Default: 512 MB (minimal for bulk load)
	BlockCacheSizeMB int `toml:"block_cache_size_mb"`
}

// ActiveStoreSettings contains settings for when the store is used actively
// for real-time ingestion and queries (Phase 2).
type ActiveStoreSettings struct {
	// EnableWAL enables Write-Ahead Log for crash recovery.
	// MUST be true for active use to prevent data loss.
	// Default: true
	EnableWAL bool `toml:"enable_wal"`

	// L0CompactionTrigger for active use. Lower = more frequent compaction.
	// 4 is typical for balanced read/write workloads.
	// Default: 4
	L0CompactionTrigger int `toml:"l0_compaction_trigger"`

	// L0SlowdownWritesTrigger for active use.
	// Default: 20
	L0SlowdownWritesTrigger int `toml:"l0_slowdown_writes_trigger"`

	// L0StopWritesTrigger for active use.
	// Default: 36
	L0StopWritesTrigger int `toml:"l0_stop_writes_trigger"`

	// BlockCacheSizeMB for active use. Larger = better read performance.
	// For 64GB RAM with 15B entries: 16-24GB is reasonable.
	// Default: 16384 MB (16 GB)
	BlockCacheSizeMB int `toml:"block_cache_size_mb"`

	// WriteBufferSizeMB for active use (per CF).
	// Can be smaller than bulk load since write rate is lower.
	// Total = WriteBufferSizeMB × MaxWriteBufferNumber × 16 CFs
	// Default: 64 MB (64 × 4 × 16 = 4 GB total)
	WriteBufferSizeMB int `toml:"write_buffer_size_mb"`

	// MaxWriteBufferNumber for active use.
	// Default: 4
	MaxWriteBufferNumber int `toml:"max_write_buffer_number"`
}

// =============================================================================
// Column Family Constants
// =============================================================================

// ColumnFamilyNames returns the names of all 16 column families.
// Each column family is named by the hex character it handles (0-9, a-f).
var ColumnFamilyNames = []string{
	"0", "1", "2", "3", "4", "5", "6", "7",
	"8", "9", "a", "b", "c", "d", "e", "f",
}

// ColumnFamilyName returns the column family name for index 0-15.
func ColumnFamilyName(idx int) string {
	if idx < 0 || idx > 15 {
		return "default"
	}
	return ColumnFamilyNames[idx]
}

// GetColumnFamilyIndex returns the column family index (0-15) for a transaction hash.
// The hash is 32 bytes, and we use the first hex character (high nibble of first byte).
func GetColumnFamilyIndex(txHash []byte) int {
	if len(txHash) < 1 {
		return 0
	}
	// First hex char = high nibble of first byte
	return int(txHash[0] >> 4)
}

// GetColumnFamilyName returns the column family name for a transaction hash.
func GetColumnFamilyName(txHash []byte) string {
	return ColumnFamilyName(GetColumnFamilyIndex(txHash))
}

// =============================================================================
// Default Values
// =============================================================================

// DefaultRocksDBSettings returns default RocksDB settings for BULK LOAD phase.
// Optimized for 64GB RAM machine with 15B entries target.
func DefaultRocksDBSettings() RocksDBSettings {
	return RocksDBSettings{
		// MemTable: 128 MB × 4 × 16 CFs = 8 GB total
		WriteBufferSizeMB:    128,
		MaxWriteBufferNumber: 4,

		// Disable compaction triggers during bulk load
		L0CompactionTrigger:     999,
		L0SlowdownWritesTrigger: 999,
		L0StopWritesTrigger:     999,

		// File organization
		TargetFileSizeMB:       256,  // 256 MB SST files
		MaxBytesForLevelBaseMB: 2560, // 2.5 GB L1 per CF

		// Resources
		MaxBackgroundJobs: 8,
		MaxOpenFiles:      10000,

		// Bloom filter for hash lookups
		BloomFilterBitsPerKey: 10,

		// WAL disabled during bulk load
		DisableWAL: true,

		// Minimal block cache during bulk load (mostly writing)
		BlockCacheSizeMB: 512,
	}
}

// DefaultActiveStoreSettings returns default settings for ACTIVE USE phase.
// Optimized for 64GB RAM machine with balanced read/write workload.
func DefaultActiveStoreSettings() ActiveStoreSettings {
	return ActiveStoreSettings{
		// WAL MUST be enabled for active use
		EnableWAL: true,

		// Normal compaction triggers for balanced performance
		L0CompactionTrigger:     4,
		L0SlowdownWritesTrigger: 20,
		L0StopWritesTrigger:     36,

		// Large block cache for read performance (16 GB)
		BlockCacheSizeMB: 16384,

		// Smaller write buffers for active use (4 GB total)
		WriteBufferSizeMB:    64,
		MaxWriteBufferNumber: 4,
	}
}

// =============================================================================
// Configuration Loading
// =============================================================================

// LoadConfig loads and validates the TOML configuration file.
func LoadConfig(path string) (*Config, error) {
	config := &Config{
		BatchSize:   100000,
		RocksDB:     DefaultRocksDBSettings(),
		ActiveStore: DefaultActiveStoreSettings(),
	}

	// Read and parse TOML file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	if _, err := toml.Decode(string(data), config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	// Validate configuration
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	return config, nil
}

// validateConfig validates the configuration.
func validateConfig(config *Config) error {
	// ==========================================================================
	// Input Source Validation (mutually exclusive)
	// ==========================================================================
	hasInputStores := len(config.InputStores) > 0
	hasLfsPath := config.LfsLedgerStorePath != ""

	if hasInputStores && hasLfsPath {
		return fmt.Errorf("input_stores and lfs_ledger_store_path are mutually exclusive; use one or the other")
	}

	if !hasInputStores && !hasLfsPath {
		return fmt.Errorf("either input_stores or lfs_ledger_store_path is required")
	}

	// Set mode flag
	config.IsLfsMode = hasLfsPath

	// ==========================================================================
	// Validate Input Source
	// ==========================================================================
	if config.IsLfsMode {
		if err := validateLfsConfig(config); err != nil {
			return err
		}
	} else {
		// Validate each input store path
		for i, storePath := range config.InputStores {
			if storePath == "" {
				return fmt.Errorf("input_store[%d] is empty", i)
			}
			// We'll validate existence later in main
		}
	}

	// ==========================================================================
	// Output Configuration
	// ==========================================================================

	// Output path is required
	if config.OutputPath == "" {
		return fmt.Errorf("output_path is required")
	}

	// Create output directory if it doesn't exist
	absPath, err := filepath.Abs(config.OutputPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for output_path: %w", err)
	}
	config.OutputPath = absPath

	if err := os.MkdirAll(absPath, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Validate batch size
	if config.BatchSize <= 0 {
		config.BatchSize = 100000
	}

	// Validate RocksDB settings
	if err := validateRocksDBSettings(&config.RocksDB); err != nil {
		return err
	}

	// Validate ActiveStore settings
	if err := validateActiveStoreSettings(&config.ActiveStore); err != nil {
		return err
	}

	return nil
}

// validateLfsConfig validates LFS-specific configuration.
func validateLfsConfig(config *Config) error {
	// Validate LFS store path exists
	absPath, err := filepath.Abs(config.LfsLedgerStorePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for lfs_ledger_store_path: %w", err)
	}
	config.LfsLedgerStorePath = absPath

	if err := lfs.ValidateLfsStore(absPath); err != nil {
		return err
	}

	// Discover ledger range if not specified
	discoveredRange, err := lfs.DiscoverLedgerRange(absPath)
	if err != nil {
		return fmt.Errorf("failed to discover ledger range: %w", err)
	}

	// Use discovered range as defaults, allow override
	startLedger := config.LfsStartLedger
	endLedger := config.LfsEndLedger

	if startLedger == 0 {
		startLedger = discoveredRange.StartLedger
	}
	if endLedger == 0 {
		endLedger = discoveredRange.EndLedger
	}

	// Validate range is within discovered range
	if startLedger < discoveredRange.StartLedger {
		return fmt.Errorf("lfs_start_ledger %d is before first available ledger %d",
			startLedger, discoveredRange.StartLedger)
	}
	if endLedger > discoveredRange.EndLedger {
		return fmt.Errorf("lfs_end_ledger %d is after last available ledger %d",
			endLedger, discoveredRange.EndLedger)
	}
	if startLedger > endLedger {
		return fmt.Errorf("lfs_start_ledger %d is after lfs_end_ledger %d",
			startLedger, endLedger)
	}

	// Update config with resolved values
	config.LfsStartLedger = startLedger
	config.LfsEndLedger = endLedger

	// Store the ledger range for use during processing
	config.LfsLedgerRange = &lfs.LedgerRange{
		StartLedger: startLedger,
		EndLedger:   endLedger,
		StartChunk:  lfs.LedgerToChunkID(startLedger),
		EndChunk:    lfs.LedgerToChunkID(endLedger),
		TotalChunks: lfs.LedgerToChunkID(endLedger) - lfs.LedgerToChunkID(startLedger) + 1,
	}

	// Set default workers if not specified
	if config.LfsWorkers <= 0 {
		config.LfsWorkers = runtime.NumCPU()
	}

	return nil
}

// validateRocksDBSettings validates RocksDB settings for bulk load.
func validateRocksDBSettings(settings *RocksDBSettings) error {
	if settings.WriteBufferSizeMB <= 0 {
		return fmt.Errorf("write_buffer_size_mb must be positive")
	}
	if settings.MaxWriteBufferNumber <= 0 {
		return fmt.Errorf("max_write_buffer_number must be positive")
	}
	if settings.TargetFileSizeMB <= 0 {
		return fmt.Errorf("target_file_size_mb must be positive")
	}
	if settings.MaxBytesForLevelBaseMB <= 0 {
		return fmt.Errorf("max_bytes_for_level_base_mb must be positive")
	}
	if settings.MaxBackgroundJobs <= 0 {
		return fmt.Errorf("max_background_jobs must be positive")
	}
	if settings.MaxOpenFiles <= 0 {
		return fmt.Errorf("max_open_files must be positive")
	}
	if settings.BlockCacheSizeMB < 0 {
		return fmt.Errorf("block_cache_size_mb cannot be negative")
	}
	return nil
}

// validateActiveStoreSettings validates settings for active use.
func validateActiveStoreSettings(settings *ActiveStoreSettings) error {
	if settings.L0CompactionTrigger <= 0 {
		return fmt.Errorf("active_store.l0_compaction_trigger must be positive")
	}
	if settings.L0SlowdownWritesTrigger <= 0 {
		return fmt.Errorf("active_store.l0_slowdown_writes_trigger must be positive")
	}
	if settings.L0StopWritesTrigger <= 0 {
		return fmt.Errorf("active_store.l0_stop_writes_trigger must be positive")
	}
	if settings.BlockCacheSizeMB < 0 {
		return fmt.Errorf("active_store.block_cache_size_mb cannot be negative")
	}
	if settings.WriteBufferSizeMB <= 0 {
		return fmt.Errorf("active_store.write_buffer_size_mb must be positive")
	}
	if settings.MaxWriteBufferNumber <= 0 {
		return fmt.Errorf("active_store.max_write_buffer_number must be positive")
	}
	return nil
}

// =============================================================================
// Memory Usage Calculation
// =============================================================================

// CalculateMemoryUsage returns estimated memory usage in MB for the given config.
func CalculateMemoryUsage(config *Config, phase string) (memtables, blockCache, total int) {
	if phase == "bulk_load" {
		memtables = config.RocksDB.WriteBufferSizeMB * config.RocksDB.MaxWriteBufferNumber * 16
		blockCache = config.RocksDB.BlockCacheSizeMB
	} else {
		memtables = config.ActiveStore.WriteBufferSizeMB * config.ActiveStore.MaxWriteBufferNumber * 16
		blockCache = config.ActiveStore.BlockCacheSizeMB
	}
	total = memtables + blockCache
	return
}
