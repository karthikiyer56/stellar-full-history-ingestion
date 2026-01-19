// Package txhashrework provides tools for building tx_hash -> ledger_seq RocksDB stores.
//
// This package supports two input sources:
//   - LFS (Local Filesystem) ledger store: Read ledgers, extract transactions
//   - Existing RocksDB stores: Merge multiple stores into one with column families
//
// The output is always a RocksDB store with 16 column families, partitioned by
// the first hex character (high nibble of first byte) of the transaction hash.
//
// RocksDB Configuration Rationale:
//
// This configuration is designed for REALTIME INGESTION with the following goals:
//   - WAL enabled for durability across reboots
//   - No automatic compaction (deferred until external trigger)
//   - Idempotent writes (duplicates handled via last-write-wins)
//   - Controlled memory usage (~24GB total: 16GB memtables + 8GB block cache)
//
// When ready to create an immutable store for recsplit, run compaction externally
// using the compact-store utility.
package txhashrework

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/BurntSushi/toml"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers/lfs"
)

// =============================================================================
// Column Family Constants
// =============================================================================

// ColumnFamilyNames contains the names of all 16 column families.
// Each column family is named by the hex character it handles (0-9, a-f).
var ColumnFamilyNames = []string{
	"0", "1", "2", "3", "4", "5", "6", "7",
	"8", "9", "a", "b", "c", "d", "e", "f",
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
	idx := GetColumnFamilyIndex(txHash)
	if idx < 0 || idx > 15 {
		return "0"
	}
	return ColumnFamilyNames[idx]
}

// =============================================================================
// RocksDB Settings
// =============================================================================

// RocksDBSettings contains tunable RocksDB parameters for the output store.
// These settings are optimized for realtime ingestion with no automatic compaction.
type RocksDBSettings struct {
	// WriteBufferSizeMB controls the size of each MemTable in megabytes.
	// Applies to each column family separately.
	// Total MemTable RAM = WriteBufferSizeMB x MaxWriteBufferNumber x 16 CFs
	//
	// Default: 256 MB (256 x 4 x 16 = 16 GB total)
	WriteBufferSizeMB int

	// MaxWriteBufferNumber is the maximum number of MemTables per column family.
	// Default: 4
	MaxWriteBufferNumber int

	// L0CompactionTrigger: Number of L0 files that triggers compaction.
	// Set extremely high to disable automatic compaction.
	// Default: 999999
	L0CompactionTrigger int

	// L0SlowdownWritesTrigger: Number of L0 files that slows down writes.
	// Set extremely high to prevent write slowdowns.
	// Default: 999999
	L0SlowdownWritesTrigger int

	// L0StopWritesTrigger: Number of L0 files that stops writes.
	// Set extremely high to prevent write stalls.
	// Default: 999999
	L0StopWritesTrigger int

	// DisableAutoCompactions disables all automatic background compaction.
	// Compaction must be triggered externally via compact-store utility.
	// Default: true
	DisableAutoCompactions bool

	// DisableWAL disables the Write-Ahead Log.
	// MUST be false for durability across reboots.
	// Default: false (WAL enabled)
	DisableWAL bool

	// TargetFileSizeMB is the target size for SST files.
	// Default: 256 MB
	TargetFileSizeMB int

	// MaxBytesForLevelBaseMB is the maximum total size for L1 per column family.
	// Default: 2560 MB (2.5 GB)
	MaxBytesForLevelBaseMB int

	// MaxBackgroundJobs is the maximum number of concurrent background operations.
	// Default: 8
	MaxBackgroundJobs int

	// MaxOpenFiles limits the number of file handles.
	// Default: 10000
	MaxOpenFiles int

	// BloomFilterBitsPerKey configures the bloom filter.
	// 12 bits/key = ~0.3% false positive rate (better than 10 bits = ~1%)
	// Default: 12
	BloomFilterBitsPerKey int

	// BlockCacheSizeMB is the size of the block cache for reading.
	// Shared across all column families.
	// Default: 8192 MB (8 GB)
	BlockCacheSizeMB int
}

// DefaultRocksDBSettings returns the default RocksDB settings for realtime ingestion.
//
// Memory budget breakdown:
//   - MemTables: 256 MB x 4 x 16 CFs = 16 GB
//   - Block Cache: 8 GB
//   - Total: ~24 GB
//
// Key characteristics:
//   - WAL enabled for crash recovery
//   - No automatic compaction (use compact-store utility when ready)
//   - 12-bit bloom filters for better read performance (~0.3% false positive)
func DefaultRocksDBSettings() RocksDBSettings {
	return RocksDBSettings{
		// 16GB total memtables: 256MB x 4 x 16 CFs
		WriteBufferSizeMB:    256,
		MaxWriteBufferNumber: 4,

		// Disable automatic compaction entirely
		L0CompactionTrigger:     999999,
		L0SlowdownWritesTrigger: 999999,
		L0StopWritesTrigger:     999999,
		DisableAutoCompactions:  true,

		// WAL enabled for durability
		DisableWAL: false,

		// SST file organization
		TargetFileSizeMB:       256,
		MaxBytesForLevelBaseMB: 2560,

		// Resources
		MaxBackgroundJobs: 8,
		MaxOpenFiles:      10000,

		// 12-bit bloom filter for better read performance
		BloomFilterBitsPerKey: 12,

		// 8GB block cache
		BlockCacheSizeMB: 8192,
	}
}

// =============================================================================
// LFS Input Configuration
// =============================================================================

// LfsConfig holds configuration for LFS input source.
type LfsConfig struct {
	// LfsStorePath is the path to the LFS ledger store directory.
	LfsStorePath string `toml:"lfs_store_path"`

	// StartLedger is the first ledger sequence to process (inclusive).
	// If 0, auto-detect from the store.
	StartLedger uint32 `toml:"start_ledger"`

	// EndLedger is the last ledger sequence to process (inclusive).
	// If 0, auto-detect from the store.
	EndLedger uint32 `toml:"end_ledger"`

	// Workers is the number of parallel workers for LFS processing.
	// Default: runtime.NumCPU()
	Workers int `toml:"workers"`

	// LedgerRange is the discovered/validated ledger range (set during validation)
	LedgerRange *lfs.LedgerRange `toml:"-"`
}

// =============================================================================
// Common Configuration
// =============================================================================

// Config represents the configuration for tx-hash ingestion tools.
type Config struct {
	// OutputPath is the directory where the RocksDB store will be created.
	OutputPath string `toml:"output_path"`

	// BatchSize is the number of entries to accumulate before writing a batch.
	// Default: 100000
	BatchSize int `toml:"batch_size"`

	// RocksDB contains tunable RocksDB parameters.
	RocksDB RocksDBSettings `toml:"rocksdb"`

	// LFS contains LFS-specific configuration (for LFS input mode).
	LFS LfsConfig `toml:"lfs"`

	// InputStores is a list of paths to existing RocksDB stores (for RocksDB input mode).
	InputStores []string `toml:"input_stores"`
}

// DefaultConfig returns a Config with sane defaults.
func DefaultConfig() Config {
	return Config{
		BatchSize: 100000,
		RocksDB:   DefaultRocksDBSettings(),
		LFS: LfsConfig{
			Workers: runtime.NumCPU(),
		},
	}
}

// LoadConfig loads configuration from a TOML file, applying defaults for missing values.
func LoadConfig(path string) (*Config, error) {
	config := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	if _, err := toml.Decode(string(data), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	return &config, nil
}

// ValidateForLfsInput validates the configuration for LFS input mode.
func (c *Config) ValidateForLfsInput() error {
	if c.LFS.LfsStorePath == "" {
		return fmt.Errorf("lfs_store_path is required")
	}

	// Get absolute path
	absPath, err := filepath.Abs(c.LFS.LfsStorePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for lfs_store_path: %w", err)
	}
	c.LFS.LfsStorePath = absPath

	// Validate LFS store exists
	if err := lfs.ValidateLfsStore(absPath); err != nil {
		return err
	}

	// Discover ledger range
	discoveredRange, err := lfs.DiscoverLedgerRange(absPath)
	if err != nil {
		return fmt.Errorf("failed to discover ledger range: %w", err)
	}

	// Use discovered range as defaults, allow override
	startLedger := c.LFS.StartLedger
	endLedger := c.LFS.EndLedger

	if startLedger == 0 {
		startLedger = discoveredRange.StartLedger
	}
	if endLedger == 0 {
		endLedger = discoveredRange.EndLedger
	}

	// Validate range
	if startLedger < discoveredRange.StartLedger {
		return fmt.Errorf("start_ledger %d is before first available ledger %d",
			startLedger, discoveredRange.StartLedger)
	}
	if endLedger > discoveredRange.EndLedger {
		return fmt.Errorf("end_ledger %d is after last available ledger %d",
			endLedger, discoveredRange.EndLedger)
	}
	if startLedger > endLedger {
		return fmt.Errorf("start_ledger %d is after end_ledger %d", startLedger, endLedger)
	}

	c.LFS.StartLedger = startLedger
	c.LFS.EndLedger = endLedger
	c.LFS.LedgerRange = &lfs.LedgerRange{
		StartLedger: startLedger,
		EndLedger:   endLedger,
		StartChunk:  lfs.LedgerToChunkID(startLedger),
		EndChunk:    lfs.LedgerToChunkID(endLedger),
		TotalChunks: lfs.LedgerToChunkID(endLedger) - lfs.LedgerToChunkID(startLedger) + 1,
	}

	// Set default workers
	if c.LFS.Workers <= 0 {
		c.LFS.Workers = runtime.NumCPU()
	}

	// Validate output path
	return c.validateOutputPath()
}

// ValidateForRocksDBInput validates the configuration for RocksDB input mode.
func (c *Config) ValidateForRocksDBInput() error {
	if len(c.InputStores) == 0 {
		return fmt.Errorf("at least one input_store is required")
	}

	// Validate each input store exists
	for i, storePath := range c.InputStores {
		if storePath == "" {
			return fmt.Errorf("input_store[%d] is empty", i)
		}
		absPath, err := filepath.Abs(storePath)
		if err != nil {
			return fmt.Errorf("failed to get absolute path for input_store[%d]: %w", i, err)
		}
		c.InputStores[i] = absPath

		if _, err := os.Stat(absPath); os.IsNotExist(err) {
			return fmt.Errorf("input_store[%d] does not exist: %s", i, absPath)
		}
	}

	return c.validateOutputPath()
}

// validateOutputPath validates and creates the output directory.
func (c *Config) validateOutputPath() error {
	if c.OutputPath == "" {
		return fmt.Errorf("output_path is required")
	}

	absPath, err := filepath.Abs(c.OutputPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for output_path: %w", err)
	}
	c.OutputPath = absPath

	if err := os.MkdirAll(absPath, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Validate batch size
	if c.BatchSize <= 0 {
		c.BatchSize = 100000
	}

	return nil
}

// CalculateMemoryUsage returns estimated memory usage in MB.
func (c *Config) CalculateMemoryUsage() (memtables, blockCache, total int) {
	memtables = c.RocksDB.WriteBufferSizeMB * c.RocksDB.MaxWriteBufferNumber * 16
	blockCache = c.RocksDB.BlockCacheSizeMB
	total = memtables + blockCache
	return
}
