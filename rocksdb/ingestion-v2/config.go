// config.go
// =============================================================================
// Configuration Management for Stellar RocksDB Ingestion
// =============================================================================
//
// This module handles:
// - TOML configuration file parsing
// - Command-line argument parsing
// - Configuration validation
// - Default value management
//
// The configuration is split between:
// - TOML file: All RocksDB settings, paths, and store-specific options
// - Command line: Runtime controls (batch size, which stores to enable, time range info)
//
// =============================================================================

package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// =============================================================================
// TOML Configuration Structures
// =============================================================================

// Config represents the complete TOML configuration file structure.
// It contains global settings and per-store configurations.
type Config struct {
	// Global section contains settings that apply across all stores
	Global GlobalConfig `toml:"global"`

	// Per-store configurations
	// Each store has its own RocksDB tuning parameters and paths
	LedgerSeqToLcm    LedgerSeqToLcmConfig    `toml:"ledger_seq_to_lcm"`
	TxHashToTxData    TxHashToTxDataConfig    `toml:"tx_hash_to_tx_data"`
	TxHashToLedgerSeq TxHashToLedgerSeqConfig `toml:"tx_hash_to_ledger_seq"`
}

// GlobalConfig contains settings that apply globally across all operations.
type GlobalConfig struct {
	// RocksDBLcmStorePath is the path to an existing RocksDB store containing
	// compressed LedgerCloseMeta data. If specified, ledgers will be read from
	// this store instead of GCS. When this is set, ledger_seq_to_lcm store
	// cannot be enabled (you can't write LCMs while reading from another LCM store).
	//
	// Leave empty to read from GCS (Google Cloud Storage).
	RocksDBLcmStorePath string `toml:"rocksdb_lcm_store_path"`
}

// =============================================================================
// RocksDB Settings Structure (shared across all stores)
// =============================================================================

// RocksDBSettings contains all tunable RocksDB parameters.
// These settings control memory usage, file organization, and compaction behavior.
//
// UNDERSTANDING ROCKSDB WRITE PATH:
// =================================
// 1. Writes go to the active MemTable (in-memory, sized by WriteBufferSizeMB)
// 2. When MemTable is full, it becomes immutable and a new one is created
// 3. Immutable MemTables are flushed to disk as L0 SST files
// 4. L0 files are compacted into L1, L1 into L2, etc.
//
// KEY RELATIONSHIPS:
// ==================
// - WriteBufferSizeMB: Size of each MemTable. Larger = fewer flushes, more RAM
// - MaxWriteBufferNumber: Max MemTables in memory. Total RAM = Size × Number
// - TargetFileSizeMB: Target size for SST files at L0/L1
// - MaxBytesForLevelBaseMB: Total size limit for L1. L2 = L1 × multiplier, etc.
//
// For bulk ingestion with random keys (like tx hashes):
// - Disable auto-compaction (L0 files accumulate)
// - Set high L0 triggers to prevent write stalls
// - Do one final compaction at the end
type RocksDBSettings struct {
	// =========================================================================
	// Write Buffer (MemTable) Settings
	// =========================================================================

	// WriteBufferSizeMB controls the size of each MemTable in megabytes.
	// When a MemTable fills up, it's marked immutable and flushed to disk.
	//
	// Guidelines:
	// - Larger values = fewer flush operations = better write throughput
	// - Larger values = more RAM usage per buffer
	// - Should roughly match your batch size after compression
	// - For 2000 ledgers/batch with 250 tx each at 1.5KB = ~750MB data
	//   So 512-1024 MB is reasonable
	//
	// Default: 512 MB
	WriteBufferSizeMB int `toml:"write_buffer_size_mb"`

	// MaxWriteBufferNumber is the maximum number of MemTables to keep in memory.
	// This includes both the active (writable) and immutable (being flushed) ones.
	//
	// Total MemTable RAM = WriteBufferSizeMB × MaxWriteBufferNumber
	//
	// Guidelines:
	// - Minimum 2: one for writing, one for flushing
	// - 3-4 provides headroom during flush operations
	// - Higher values allow writes to continue during slow flushes
	//
	// Default: 4
	MaxWriteBufferNumber int `toml:"max_write_buffer_number"`

	// =========================================================================
	// L0 File Management (Critical for Bulk Ingestion)
	// =========================================================================

	// L0CompactionTrigger: Number of L0 files that triggers compaction to L1.
	// For bulk ingestion with random keys, set this very high (e.g., 999)
	// to effectively disable compaction during ingestion.
	//
	// Why? Random keys (like tx hashes) cause massive write amplification
	// during compaction because every L0 file overlaps with every other file.
	// Better to accumulate all L0 files and do one big compaction at the end.
	//
	// Default for bulk load: 999 (effectively disabled)
	L0CompactionTrigger int `toml:"l0_compaction_trigger"`

	// L0SlowdownWritesTrigger: Number of L0 files that slows down writes.
	// Set very high for bulk ingestion to prevent artificial slowdowns.
	//
	// Default for bulk load: 999
	L0SlowdownWritesTrigger int `toml:"l0_slowdown_writes_trigger"`

	// L0StopWritesTrigger: Number of L0 files that completely stops writes.
	// Set very high for bulk ingestion.
	//
	// Default for bulk load: 999
	L0StopWritesTrigger int `toml:"l0_stop_writes_trigger"`

	// =========================================================================
	// SST File Size Settings
	// =========================================================================

	// TargetFileSizeMB is the target size for SST files at level 0 and level 1.
	// Files at higher levels are larger (multiplied by TargetFileSizeMultiplier).
	//
	// This setting directly affects the number of files in your database:
	// - Total data / TargetFileSizeMB ≈ number of L1 files (before higher levels)
	//
	// For ledger_seq_to_lcm (sequential keys, ~900GB/year):
	//   4096 MB (4GB) → ~225 files/year → ~2250 files for 10 years
	//
	// For tx_hash_to_tx_data (random keys, ~1.5TB/year):
	//   2048 MB (2GB) → ~750 files/year → ~7500 files for 10 years
	//
	// For tx_hash_to_ledger_seq (random keys, ~54GB/year raw):
	//   1024 MB (1GB) → ~54 files/year → ~540 files for 10 years
	//
	// Note: Final file count depends on compaction and compression.
	TargetFileSizeMB int `toml:"target_file_size_mb"`

	// MaxBytesForLevelBaseMB is the maximum total size for level 1 in megabytes.
	// Higher levels have exponentially more capacity:
	//   L1 = MaxBytesForLevelBaseMB
	//   L2 = L1 × 10 (default multiplier)
	//   L3 = L2 × 10
	//   ... and so on
	//
	// This controls the "shape" of the LSM tree:
	// - Smaller base = more levels = more read amplification
	// - Larger base = fewer levels = faster reads, but L1 compaction is bigger
	//
	// Rule of thumb: Set to ~10× your TargetFileSizeMB
	// This means L1 will hold about 10 files before pushing to L2.
	//
	// For ledger_seq_to_lcm:  40960 MB (40GB) with 4GB files
	// For tx_hash_to_tx_data: 20480 MB (20GB) with 2GB files
	// For tx_hash_to_ledger_seq: 10240 MB (10GB) with 1GB files
	MaxBytesForLevelBaseMB int `toml:"max_bytes_for_level_base_mb"`

	// =========================================================================
	// Resource Limits
	// =========================================================================

	// MaxBackgroundJobs is the maximum number of concurrent background operations.
	// This includes both flush and compaction operations.
	//
	// Guidelines:
	// - Set to number of CPU cores for final compaction
	// - During bulk ingestion (with auto-compaction disabled), these are mostly idle
	//
	// Default: 8
	MaxBackgroundJobs int `toml:"max_background_jobs"`

	// MaxOpenFiles limits the number of file handles RocksDB can keep open.
	// Each SST file requires a file handle when accessed.
	//
	// Guidelines:
	// - Should be > total number of SST files for best performance
	// - For 10 years of data, you might have 10K+ files total
	// - Set system ulimit accordingly (ulimit -n)
	//
	// Default: 10000
	MaxOpenFiles int `toml:"max_open_files"`

	// BloomFilterBitsPerKey configures the bloom filter for faster lookups.
	// Bloom filters allow RocksDB to skip SST files that definitely don't
	// contain a key, without reading them from disk.
	//
	// Bits per key | False positive rate | Memory per 1M keys
	// -------------|---------------------|-------------------
	//     10       |        ~1%          |     ~1.2 MB
	//     15       |       ~0.1%         |     ~1.8 MB
	//     20       |       ~0.01%        |     ~2.4 MB
	//
	// For tx hash lookups (random keys), bloom filters are essential.
	// For sequential keys (ledger sequence), less critical but still helpful.
	//
	// Set to 0 to disable bloom filters.
	//
	// Default: 10
	BloomFilterBitsPerKey int `toml:"bloom_filter_bits_per_key"`

	// =========================================================================
	// WAL (Write-Ahead Log) Settings
	// =========================================================================

	// DisableWAL disables the Write-Ahead Log for faster writes.
	// WAL provides crash recovery but adds I/O overhead.
	//
	// For bulk ingestion, it's safe to disable WAL because:
	// - If the process crashes, you restart from the beginning anyway
	// - The source data (GCS or existing RocksDB) is still intact
	//
	// Default: true (WAL is disabled for bulk ingestion)
	DisableWAL bool `toml:"disable_wal"`
}

// =============================================================================
// Per-Store Configuration Structures
// =============================================================================

// LedgerSeqToLcmConfig configures the ledgerSequence → LedgerCloseMeta store.
//
// This store maps: uint32 ledger sequence → zstd-compressed LedgerCloseMeta
//
// Data characteristics:
// - Key: 4 bytes (uint32, big-endian)
// - Value: ~200 KB compressed LCM (varies by ledger)
// - Keys are sequential (optimal for RocksDB)
// - ~5.5 million ledgers/year
// - ~900 GB/year after compression
//
// Recommended settings for yearly ingestion:
// - TargetFileSizeMB: 4096 (4GB files) → ~225 files/year
// - MaxBytesForLevelBaseMB: 40960 (40GB)
// - WriteBufferSizeMB: 1024 (1GB) - can be larger due to sequential access
type LedgerSeqToLcmConfig struct {
	// Enabled controls whether this store is active.
	// Can be overridden by --enable-ledger-seq-to-lcm command line flag.
	Enabled bool `toml:"enabled"`

	// OutputPath is the directory where the RocksDB database will be stored.
	// Will be created if it doesn't exist.
	// If it exists, data will be appended to the existing database.
	OutputPath string `toml:"output_path"`

	// RocksDB tuning parameters for this store
	RocksDB RocksDBSettings `toml:"rocksdb"`
}

// TxHashToTxDataConfig configures the txHash → CompositeTxData store.
//
// This store maps: 32-byte tx hash → zstd-compressed TxData protobuf
//
// Data characteristics:
// - Key: 32 bytes (transaction hash)
// - Value: ~1.5 KB compressed TxData (varies by transaction)
// - Keys are random (hash distribution)
// - ~1.5 billion transactions/year
// - ~1.5 TB/year after compression
//
// Recommended settings for yearly ingestion:
// - TargetFileSizeMB: 2048 (2GB files) → ~750 files/year
// - MaxBytesForLevelBaseMB: 20480 (20GB)
// - WriteBufferSizeMB: 512 - balance between RAM and flush frequency
// - BloomFilterBitsPerKey: 10 (essential for random key lookups)
type TxHashToTxDataConfig struct {
	// Enabled controls whether this store is active.
	// Can be overridden by --enable-tx-hash-to-tx-data command line flag.
	Enabled bool `toml:"enabled"`

	// OutputPath is the directory where the RocksDB database will be stored.
	OutputPath string `toml:"output_path"`

	// RocksDB tuning parameters for this store
	RocksDB RocksDBSettings `toml:"rocksdb"`
}

// TxHashToLedgerSeqConfig configures the txHash → ledgerSequence store.
//
// This store maps: 32-byte tx hash → uint32 ledger sequence
//
// Data characteristics:
// - Key: 32 bytes (transaction hash)
// - Value: 4 bytes (uint32 ledger sequence, big-endian)
// - Keys are random (hash distribution)
// - ~1.5 billion transactions/year
// - ~54 GB/year raw data (before RocksDB overhead)
//
// Recommended settings for yearly ingestion:
// - TargetFileSizeMB: 1024 (1GB files) → ~54 files/year (raw), more with overhead
// - MaxBytesForLevelBaseMB: 10240 (10GB)
// - WriteBufferSizeMB: 256 - smaller values OK due to small record size
// - BloomFilterBitsPerKey: 10 (essential for random key lookups)
type TxHashToLedgerSeqConfig struct {
	// Enabled controls whether this store is active.
	// Can be overridden by --enable-tx-hash-to-ledger-seq command line flag.
	Enabled bool `toml:"enabled"`

	// OutputPath is the directory where the RocksDB database will be stored.
	OutputPath string `toml:"output_path"`

	// RawDataFilePath, if specified, writes all txHash→ledgerSeq mappings to a
	// binary file in addition to (or instead of) the RocksDB store.
	//
	// File format: Each entry is exactly 36 bytes:
	// - Bytes 0-31: Transaction hash (32 bytes)
	// - Bytes 32-35: Ledger sequence (4 bytes, big-endian)
	//
	// The file is written sequentially and can be used for:
	// - Building perfect hash tables
	// - External processing pipelines
	// - Backup/verification
	//
	// If the file exists, it will be DELETED and recreated from scratch.
	// Writes are buffered and flushed at the end of each batch.
	//
	// Expected size: ~54 GB/year (1.5B tx × 36 bytes)
	//
	// Leave empty to disable raw file output.
	RawDataFilePath string `toml:"raw_data_file_path"`

	// RocksDB tuning parameters for this store
	RocksDB RocksDBSettings `toml:"rocksdb"`
}

// =============================================================================
// Runtime Configuration (from command line)
// =============================================================================

// RuntimeConfig holds command-line specified runtime parameters.
// These are combined with the TOML config to create the final configuration.
type RuntimeConfig struct {
	// ConfigPath is the path to the TOML configuration file
	ConfigPath string

	// LedgerBatchSize is the number of ledgers to process in each batch
	// This affects memory usage and checkpoint frequency
	LedgerBatchSize int

	// Store enable flags (override TOML settings if specified)
	EnableLedgerSeqToLcm    bool
	EnableTxHashToTxData    bool
	EnableTxHashToLedgerSeq bool

	// Time range information (for logging purposes)
	// These are passed from the shell script after stellar-etl conversion
	StartTime string
	EndTime   string

	// Ledger range (derived from time range by shell script)
	StartLedger uint32
	EndLedger   uint32
}

// =============================================================================
// Combined Configuration
// =============================================================================

// IngestionConfig is the final, validated configuration used during ingestion.
// It combines settings from both TOML and command line.
type IngestionConfig struct {
	// From command line
	LedgerBatchSize int
	StartLedger     uint32
	EndLedger       uint32
	StartTime       string
	EndTime         string

	// Global settings
	RocksDBLcmStorePath string
	UseRocksDBSource    bool

	// Store configurations (with enabled flags resolved)
	LedgerSeqToLcm    LedgerSeqToLcmConfig
	TxHashToTxData    TxHashToTxDataConfig
	TxHashToLedgerSeq TxHashToLedgerSeqConfig

	// Derived flags for convenience
	EnableLedgerSeqToLcm    bool
	EnableTxHashToTxData    bool
	EnableTxHashToLedgerSeq bool
	ProcessTransactions     bool // True if either tx store is enabled
}

// =============================================================================
// Default Values
// =============================================================================

// DefaultRocksDBSettings returns default RocksDB settings suitable for bulk ingestion.
// These can be overridden per-store in the TOML file.
func DefaultRocksDBSettings() RocksDBSettings {
	return RocksDBSettings{
		WriteBufferSizeMB:       512,
		MaxWriteBufferNumber:    4,
		L0CompactionTrigger:     999, // Effectively disabled for bulk load
		L0SlowdownWritesTrigger: 999,
		L0StopWritesTrigger:     999,
		TargetFileSizeMB:        1024,  // 1GB default, should be tuned per store
		MaxBytesForLevelBaseMB:  10240, // 10GB default
		MaxBackgroundJobs:       8,
		MaxOpenFiles:            10000,
		BloomFilterBitsPerKey:   10,
		DisableWAL:              true, // Disabled for bulk ingestion
	}
}

// =============================================================================
// Configuration Loading
// =============================================================================

// LoadConfig loads and validates the TOML configuration file.
func LoadConfig(path string) (*Config, error) {
	config := &Config{}

	// Set defaults for all stores
	config.LedgerSeqToLcm.RocksDB = DefaultRocksDBSettings()
	config.TxHashToTxData.RocksDB = DefaultRocksDBSettings()
	config.TxHashToLedgerSeq.RocksDB = DefaultRocksDBSettings()

	// Customize defaults per store type
	// LedgerSeqToLcm: Sequential keys, large values, target 4GB files
	config.LedgerSeqToLcm.RocksDB.TargetFileSizeMB = 4096
	config.LedgerSeqToLcm.RocksDB.MaxBytesForLevelBaseMB = 40960
	config.LedgerSeqToLcm.RocksDB.WriteBufferSizeMB = 1024

	// TxHashToTxData: Random keys, medium values, target 2GB files
	config.TxHashToTxData.RocksDB.TargetFileSizeMB = 2048
	config.TxHashToTxData.RocksDB.MaxBytesForLevelBaseMB = 20480
	config.TxHashToTxData.RocksDB.WriteBufferSizeMB = 512

	// TxHashToLedgerSeq: Random keys, tiny values, target 1GB files
	config.TxHashToLedgerSeq.RocksDB.TargetFileSizeMB = 1024
	config.TxHashToLedgerSeq.RocksDB.MaxBytesForLevelBaseMB = 10240
	config.TxHashToLedgerSeq.RocksDB.WriteBufferSizeMB = 256

	// Read and parse TOML file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	if _, err := toml.Decode(string(data), config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	return config, nil
}

// =============================================================================
// Configuration Validation
// =============================================================================

// ValidateAndMerge validates the TOML config against runtime config and merges them.
// It returns the final IngestionConfig ready for use.
func ValidateAndMerge(tomlConfig *Config, runtime *RuntimeConfig) (*IngestionConfig, error) {
	config := &IngestionConfig{
		LedgerBatchSize:     runtime.LedgerBatchSize,
		StartLedger:         runtime.StartLedger,
		EndLedger:           runtime.EndLedger,
		StartTime:           runtime.StartTime,
		EndTime:             runtime.EndTime,
		RocksDBLcmStorePath: tomlConfig.Global.RocksDBLcmStorePath,
		LedgerSeqToLcm:      tomlConfig.LedgerSeqToLcm,
		TxHashToTxData:      tomlConfig.TxHashToTxData,
		TxHashToLedgerSeq:   tomlConfig.TxHashToLedgerSeq,
	}

	// Determine if we're using RocksDB as source
	config.UseRocksDBSource = config.RocksDBLcmStorePath != ""

	// Resolve enabled flags: command line overrides TOML
	config.EnableLedgerSeqToLcm = runtime.EnableLedgerSeqToLcm || tomlConfig.LedgerSeqToLcm.Enabled
	config.EnableTxHashToTxData = runtime.EnableTxHashToTxData || tomlConfig.TxHashToTxData.Enabled
	config.EnableTxHashToLedgerSeq = runtime.EnableTxHashToLedgerSeq || tomlConfig.TxHashToLedgerSeq.Enabled

	// Update the store configs with resolved enabled flags
	config.LedgerSeqToLcm.Enabled = config.EnableLedgerSeqToLcm
	config.TxHashToTxData.Enabled = config.EnableTxHashToTxData
	config.TxHashToLedgerSeq.Enabled = config.EnableTxHashToLedgerSeq

	// Determine if we need to process transactions
	config.ProcessTransactions = config.EnableTxHashToTxData || config.EnableTxHashToLedgerSeq

	// =========================================================================
	// Validation
	// =========================================================================

	// Validate batch size
	if runtime.LedgerBatchSize <= 0 {
		return nil, fmt.Errorf("ledger-batch-size must be positive, got %d", runtime.LedgerBatchSize)
	}

	// Validate ledger range
	if runtime.StartLedger == 0 || runtime.EndLedger == 0 {
		return nil, fmt.Errorf("start-ledger and end-ledger are required")
	}
	if runtime.StartLedger > runtime.EndLedger {
		return nil, fmt.Errorf("start-ledger (%d) must be <= end-ledger (%d)",
			runtime.StartLedger, runtime.EndLedger)
	}

	// At least one store must be enabled
	if !config.EnableLedgerSeqToLcm && !config.EnableTxHashToTxData && !config.EnableTxHashToLedgerSeq {
		return nil, fmt.Errorf("at least one store must be enabled")
	}

	// Cannot enable ledger_seq_to_lcm when reading from RocksDB source
	if config.UseRocksDBSource && config.EnableLedgerSeqToLcm {
		return nil, fmt.Errorf("cannot enable ledger_seq_to_lcm store when rocksdb_lcm_store_path is specified " +
			"(you cannot write LCMs while reading from another LCM store)")
	}

	// Validate RocksDB source path exists
	if config.UseRocksDBSource {
		if err := validateDirectoryExists(config.RocksDBLcmStorePath); err != nil {
			return nil, fmt.Errorf("rocksdb_lcm_store_path validation failed: %w", err)
		}
	}

	// Validate enabled stores have required configuration
	if config.EnableLedgerSeqToLcm {
		if err := validateStoreConfig("ledger_seq_to_lcm", config.LedgerSeqToLcm.OutputPath,
			&config.LedgerSeqToLcm.RocksDB); err != nil {
			return nil, err
		}
	}

	if config.EnableTxHashToTxData {
		if err := validateStoreConfig("tx_hash_to_tx_data", config.TxHashToTxData.OutputPath,
			&config.TxHashToTxData.RocksDB); err != nil {
			return nil, err
		}
	}

	if config.EnableTxHashToLedgerSeq {
		if err := validateStoreConfig("tx_hash_to_ledger_seq", config.TxHashToLedgerSeq.OutputPath,
			&config.TxHashToLedgerSeq.RocksDB); err != nil {
			return nil, err
		}
	}

	return config, nil
}

// validateStoreConfig validates a store's configuration
func validateStoreConfig(storeName, outputPath string, rocksdb *RocksDBSettings) error {
	if outputPath == "" {
		return fmt.Errorf("%s is enabled but output_path is not specified", storeName)
	}

	// Ensure output directory can be created
	if err := ensureDirectory(outputPath); err != nil {
		return fmt.Errorf("%s output_path validation failed: %w", storeName, err)
	}

	// Validate RocksDB settings
	if rocksdb.WriteBufferSizeMB <= 0 {
		return fmt.Errorf("%s: write_buffer_size_mb must be positive", storeName)
	}
	if rocksdb.MaxWriteBufferNumber <= 0 {
		return fmt.Errorf("%s: max_write_buffer_number must be positive", storeName)
	}
	if rocksdb.TargetFileSizeMB <= 0 {
		return fmt.Errorf("%s: target_file_size_mb must be positive", storeName)
	}
	if rocksdb.MaxBytesForLevelBaseMB <= 0 {
		return fmt.Errorf("%s: max_bytes_for_level_base_mb must be positive", storeName)
	}
	if rocksdb.MaxBackgroundJobs <= 0 {
		return fmt.Errorf("%s: max_background_jobs must be positive", storeName)
	}
	if rocksdb.MaxOpenFiles <= 0 {
		return fmt.Errorf("%s: max_open_files must be positive", storeName)
	}

	return nil
}

// validateDirectoryExists checks if a directory exists
func validateDirectoryExists(path string) error {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return fmt.Errorf("directory does not exist: %s", path)
	}
	if err != nil {
		return fmt.Errorf("cannot access directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("path is not a directory: %s", path)
	}
	return nil
}

// ensureDirectory creates a directory if it doesn't exist
func ensureDirectory(path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	if err := os.MkdirAll(absPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", absPath, err)
	}

	return nil
}

// =============================================================================
// Configuration Summary (for logging)
// =============================================================================

// GetConfigSummary returns a human-readable summary of the configuration
func (c *IngestionConfig) GetConfigSummary() string {
	summary := ""

	// Calculate totals
	totalLedgers := c.EndLedger - c.StartLedger + 1
	totalBatches := (totalLedgers + uint32(c.LedgerBatchSize) - 1) / uint32(c.LedgerBatchSize)

	summary += "================================================================================\n"
	summary += "                     STELLAR ROCKSDB INGESTION CONFIGURATION                    \n"
	summary += "================================================================================\n\n"

	summary += "TIME RANGE:\n"
	summary += fmt.Sprintf("  Start Time:      %s\n", c.StartTime)
	summary += fmt.Sprintf("  End Time:        %s\n", c.EndTime)
	summary += "\n"

	summary += "LEDGER RANGE:\n"
	summary += fmt.Sprintf("  Start Ledger:    %d\n", c.StartLedger)
	summary += fmt.Sprintf("  End Ledger:      %d\n", c.EndLedger)
	summary += fmt.Sprintf("  Total Ledgers:   %d\n", totalLedgers)
	summary += fmt.Sprintf("  Batch Size:      %d ledgers\n", c.LedgerBatchSize)
	summary += fmt.Sprintf("  Total Batches:   %d\n", totalBatches)
	summary += "\n"

	summary += "DATA SOURCE:\n"
	if c.UseRocksDBSource {
		summary += fmt.Sprintf("  Type:            RocksDB (local)\n")
		summary += fmt.Sprintf("  Path:            %s\n", c.RocksDBLcmStorePath)
	} else {
		summary += fmt.Sprintf("  Type:            GCS (Google Cloud Storage)\n")
	}
	summary += "\n"

	summary += "--------------------------------------------------------------------------------\n"
	summary += "ENABLED STORES:\n"
	summary += "--------------------------------------------------------------------------------\n\n"

	if c.EnableLedgerSeqToLcm {
		summary += c.getLedgerSeqToLcmSummary()
	}

	if c.EnableTxHashToTxData {
		summary += c.getTxHashToTxDataSummary()
	}

	if c.EnableTxHashToLedgerSeq {
		summary += c.getTxHashToLedgerSeqSummary()
	}

	summary += "================================================================================\n"

	return summary
}

func (c *IngestionConfig) getLedgerSeqToLcmSummary() string {
	s := c.LedgerSeqToLcm
	r := s.RocksDB

	summary := "STORE: ledger_seq_to_lcm (Ledger Sequence → Compressed LCM)\n"
	summary += "  Description:     Maps ledger sequence numbers to compressed LedgerCloseMeta\n"
	summary += "  Key Size:        4 bytes (uint32)\n"
	summary += "  Value Size:      ~200 KB (compressed LCM, varies)\n"
	summary += fmt.Sprintf("  Output Path:     %s\n", s.OutputPath)
	summary += "\n"
	summary += "  RocksDB Settings:\n"
	summary += fmt.Sprintf("    Write Buffer:          %d MB × %d = %d MB total\n",
		r.WriteBufferSizeMB, r.MaxWriteBufferNumber, r.WriteBufferSizeMB*r.MaxWriteBufferNumber)
	summary += fmt.Sprintf("    Target File Size:      %d MB\n", r.TargetFileSizeMB)
	summary += fmt.Sprintf("    L1 Max Size:           %d MB\n", r.MaxBytesForLevelBaseMB)
	summary += fmt.Sprintf("    L0 Triggers:           %d / %d / %d (compact/slow/stop)\n",
		r.L0CompactionTrigger, r.L0SlowdownWritesTrigger, r.L0StopWritesTrigger)
	summary += fmt.Sprintf("    Background Jobs:       %d\n", r.MaxBackgroundJobs)
	summary += fmt.Sprintf("    Max Open Files:        %d\n", r.MaxOpenFiles)
	summary += fmt.Sprintf("    Bloom Filter:          %d bits/key\n", r.BloomFilterBitsPerKey)
	summary += fmt.Sprintf("    WAL Disabled:          %v\n", r.DisableWAL)
	summary += "\n"

	return summary
}

func (c *IngestionConfig) getTxHashToTxDataSummary() string {
	s := c.TxHashToTxData
	r := s.RocksDB

	summary := "STORE: tx_hash_to_tx_data (Transaction Hash → Compressed TxData)\n"
	summary += "  Description:     Maps transaction hashes to compressed TxData protobufs\n"
	summary += "  Key Size:        32 bytes (tx hash)\n"
	summary += "  Value Size:      ~1.5 KB (compressed TxData, varies)\n"
	summary += fmt.Sprintf("  Output Path:     %s\n", s.OutputPath)
	summary += "\n"
	summary += "  RocksDB Settings:\n"
	summary += fmt.Sprintf("    Write Buffer:          %d MB × %d = %d MB total\n",
		r.WriteBufferSizeMB, r.MaxWriteBufferNumber, r.WriteBufferSizeMB*r.MaxWriteBufferNumber)
	summary += fmt.Sprintf("    Target File Size:      %d MB\n", r.TargetFileSizeMB)
	summary += fmt.Sprintf("    L1 Max Size:           %d MB\n", r.MaxBytesForLevelBaseMB)
	summary += fmt.Sprintf("    L0 Triggers:           %d / %d / %d (compact/slow/stop)\n",
		r.L0CompactionTrigger, r.L0SlowdownWritesTrigger, r.L0StopWritesTrigger)
	summary += fmt.Sprintf("    Background Jobs:       %d\n", r.MaxBackgroundJobs)
	summary += fmt.Sprintf("    Max Open Files:        %d\n", r.MaxOpenFiles)
	summary += fmt.Sprintf("    Bloom Filter:          %d bits/key\n", r.BloomFilterBitsPerKey)
	summary += fmt.Sprintf("    WAL Disabled:          %v\n", r.DisableWAL)
	summary += "\n"

	return summary
}

func (c *IngestionConfig) getTxHashToLedgerSeqSummary() string {
	s := c.TxHashToLedgerSeq
	r := s.RocksDB

	summary := "STORE: tx_hash_to_ledger_seq (Transaction Hash → Ledger Sequence)\n"
	summary += "  Description:     Maps transaction hashes to their containing ledger sequence\n"
	summary += "  Key Size:        32 bytes (tx hash)\n"
	summary += "  Value Size:      4 bytes (uint32 ledger sequence)\n"
	summary += fmt.Sprintf("  Output Path:     %s\n", s.OutputPath)
	if s.RawDataFilePath != "" {
		summary += fmt.Sprintf("  Raw Data File:   %s\n", s.RawDataFilePath)
		summary += "                   (Binary file: 36 bytes per entry = 32-byte hash + 4-byte seq)\n"
	}
	summary += "\n"
	summary += "  RocksDB Settings:\n"
	summary += fmt.Sprintf("    Write Buffer:          %d MB × %d = %d MB total\n",
		r.WriteBufferSizeMB, r.MaxWriteBufferNumber, r.WriteBufferSizeMB*r.MaxWriteBufferNumber)
	summary += fmt.Sprintf("    Target File Size:      %d MB\n", r.TargetFileSizeMB)
	summary += fmt.Sprintf("    L1 Max Size:           %d MB\n", r.MaxBytesForLevelBaseMB)
	summary += fmt.Sprintf("    L0 Triggers:           %d / %d / %d (compact/slow/stop)\n",
		r.L0CompactionTrigger, r.L0SlowdownWritesTrigger, r.L0StopWritesTrigger)
	summary += fmt.Sprintf("    Background Jobs:       %d\n", r.MaxBackgroundJobs)
	summary += fmt.Sprintf("    Max Open Files:        %d\n", r.MaxOpenFiles)
	summary += fmt.Sprintf("    Bloom Filter:          %d bits/key\n", r.BloomFilterBitsPerKey)
	summary += fmt.Sprintf("    WAL Disabled:          %v\n", r.DisableWAL)
	summary += "\n"

	return summary
}
