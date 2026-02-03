package config

import (
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

// Constants for configuration defaults and calculations.
const (
	// LedgersPerRange is the number of ledgers per range for backfill operations.
	// Ranges are the fundamental unit of work for the backfill coordinator.
	LedgersPerRange = 10_000_000 // 10 million ledgers per range
)

// Config represents the complete TOML configuration file structure.
// It contains all settings needed for the ingestion workflow (v4 - simplified).
type Config struct {
	// Service section: Global service settings
	Service ServiceConfig `toml:"service"`

	// Backfill section: Backfill operation settings
	Backfill BackfillConfig `toml:"backfill"`

	// ActiveStores section: Configuration for active RocksDB stores
	ActiveStores ActiveStoresConfig `toml:"active_stores"`

	// ImmutableStores section: Configuration for immutable store locations
	ImmutableStores ImmutableStoresConfig `toml:"immutable_stores"`

	// RocksDB section: Global RocksDB tuning parameters
	RocksDB RocksDBConfig `toml:"rocksdb"`

	// Transition section: Transition phase settings
	Transition TransitionConfig `toml:"transition"`

	// Metrics section: Metrics collection settings
	Metrics MetricsConfig `toml:"metrics"`
}

// ServiceConfig contains global service settings.
type ServiceConfig struct {
	// DataDir is the base directory for all data operations.
	// Required.
	DataDir string `toml:"data_dir"`

	// Mode specifies the operation mode (e.g., "backfill").
	// Required.
	Mode string `toml:"mode"`
}

// BackfillConfig contains backfill operation settings.
type BackfillConfig struct {
	// StartLedger is the first ledger sequence to backfill (inclusive).
	// Required.
	StartLedger uint32 `toml:"start_ledger"`

	// EndLedger is the last ledger sequence to backfill (inclusive).
	// Required.
	EndLedger uint32 `toml:"end_ledger"`

	// LedgerBackend specifies the backend for reading ledgers.
	// Supported: "buffered_storage"
	// Required.
	LedgerBackend string `toml:"ledger_backend"`

	// ParallelRanges controls the number of ranges to process in parallel.
	// Optional. Default: 1
	ParallelRanges int `toml:"parallel_ranges"`

	// CheckpointInterval specifies how frequently to checkpoint progress.
	// Specified in number of ledgers processed per checkpoint.
	// Optional. Default: 1000
	CheckpointInterval int `toml:"checkpoint_interval"`

	// BufferedStorage contains buffered storage backend settings.
	// Required if ledger_backend is "buffered_storage".
	BufferedStorage BufferedStorageConfig `toml:"buffered_storage"`
}

// BufferedStorageConfig contains settings for the buffered storage backend.
type BufferedStorageConfig struct {
	// BucketPath is the GCS bucket path for ledger close metadata.
	// Example: "sdf-ledger-close-meta/v1/ledgers/pubnet"
	// Required if backend=buffered_storage.
	BucketPath string `toml:"bucket_path"`

	// BufferSize is the buffer size for storage operations.
	// Optional. Default: 10000
	BufferSize int `toml:"buffer_size"`

	// NumWorkers is the number of worker goroutines for storage operations.
	// Optional. Default: 200
	NumWorkers int `toml:"num_workers"`
}

// ActiveStoresConfig contains configuration for active RocksDB stores.
type ActiveStoresConfig struct {
	// BasePath is the base directory for active RocksDB stores.
	// Individual stores are named {rangeID:04d}-ledger-store and {rangeID:04d}-txhash-store.
	// All paths are relative to service.data_dir.
	// Optional. Default: "active/rocksdb"
	BasePath string `toml:"base_path"`
}

// ImmutableStoresConfig contains configuration for immutable store locations.
type ImmutableStoresConfig struct {
	// LedgersBase is the base directory for immutable ledger stores.
	// Optional. Default: "immutable/ledgers"
	LedgersBase string `toml:"ledgers_base"`

	// TxHashBase is the base directory for immutable txhash stores.
	// Optional. Default: "immutable/txhash"
	TxHashBase string `toml:"txhash_base"`
}

// RocksDBConfig contains RocksDB tuning parameters.
type RocksDBConfig struct {
	// BlockCacheMB is the block cache size in megabytes.
	// Optional. Default: 8192
	BlockCacheMB int `toml:"block_cache_mb"`

	// WriteBufferMB is the write buffer size in megabytes.
	// Optional. Default: 512
	WriteBufferMB int `toml:"write_buffer_mb"`

	// MaxWriteBufferNumber is the maximum number of write buffers.
	// Optional. Default: 2
	MaxWriteBufferNumber int `toml:"max_write_buffer_number"`
}

// TransitionConfig contains transition phase settings.
type TransitionConfig struct {
	// PreserveRocksDBAfterTransition controls whether to keep active RocksDB stores
	// after transitioning to immutable stores.
	// Optional. Default: false
	PreserveRocksDBAfterTransition bool `toml:"preserve_rocksdb_after_transition"`
}

// MetricsConfig contains metrics collection settings.
type MetricsConfig struct {
	// CSVOutput is the path for CSV metrics output.
	// Optional. Default: "" (disabled)
	CSVOutput string `toml:"csv_output"`

	// LogIntervalBatches specifies how often to log metrics.
	// Specified in number of batches processed.
	// Optional. Default: 10
	LogIntervalBatches int `toml:"log_interval_batches"`
}

// LoadConfig loads and parses a TOML configuration file.
// Returns an error if the file cannot be read or parsed.
// Applies default values for optional fields after parsing.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	config := &Config{}
	if _, err := toml.Decode(string(data), config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	// Apply defaults for optional fields
	setDefaults(config)

	return config, nil
}

// setDefaults sets default values for optional fields.
func setDefaults(c *Config) {
	if c.Backfill.ParallelRanges == 0 {
		c.Backfill.ParallelRanges = 1
	}
	if c.Backfill.CheckpointInterval == 0 {
		c.Backfill.CheckpointInterval = 1000
	}
	if c.Backfill.BufferedStorage.BufferSize == 0 {
		c.Backfill.BufferedStorage.BufferSize = 10000
	}
	if c.Backfill.BufferedStorage.NumWorkers == 0 {
		c.Backfill.BufferedStorage.NumWorkers = 200
	}
	if c.ActiveStores.BasePath == "" {
		c.ActiveStores.BasePath = "active/rocksdb"
	}
	if c.ImmutableStores.LedgersBase == "" {
		c.ImmutableStores.LedgersBase = "immutable/ledgers"
	}
	if c.ImmutableStores.TxHashBase == "" {
		c.ImmutableStores.TxHashBase = "immutable/txhash"
	}
	if c.RocksDB.BlockCacheMB == 0 {
		c.RocksDB.BlockCacheMB = 8192
	}
	if c.RocksDB.WriteBufferMB == 0 {
		c.RocksDB.WriteBufferMB = 512
	}
	if c.RocksDB.MaxWriteBufferNumber == 0 {
		c.RocksDB.MaxWriteBufferNumber = 2
	}
	if c.Metrics.LogIntervalBatches == 0 {
		c.Metrics.LogIntervalBatches = 10
	}
}

// Validate validates the configuration for correctness.
// Returns an error with a clear message if validation fails.
// Assumes defaults have already been applied (by LoadConfig).
func (c *Config) Validate() error {
	// Validate service section
	if c.Service.DataDir == "" {
		return fmt.Errorf("service.data_dir is required")
	}
	if c.Service.Mode == "" {
		return fmt.Errorf("service.mode is required")
	}

	// Validate backfill section
	if c.Backfill.StartLedger == 0 {
		return fmt.Errorf("backfill.start_ledger is required")
	}
	if c.Backfill.EndLedger == 0 {
		return fmt.Errorf("backfill.end_ledger is required")
	}
	if c.Backfill.StartLedger >= c.Backfill.EndLedger {
		return fmt.Errorf("backfill.start_ledger (%d) must be less than backfill.end_ledger (%d)",
			c.Backfill.StartLedger, c.Backfill.EndLedger)
	}
	if c.Backfill.LedgerBackend == "" {
		return fmt.Errorf("backfill.ledger_backend is required")
	}

	// Validate ledger backend specific requirements
	if c.Backfill.LedgerBackend == "buffered_storage" {
		if c.Backfill.BufferedStorage.BucketPath == "" {
			return fmt.Errorf("backfill.buffered_storage.bucket_path is required when ledger_backend=buffered_storage")
		}
	}

	// Validate parallel_ranges is at least 1 (0 means user error or missing value)
	if c.Backfill.ParallelRanges < 1 {
		return fmt.Errorf("backfill.parallel_ranges must be >= 1, got %d", c.Backfill.ParallelRanges)
	}

	return nil
}

// CalculateRangeCount returns the number of ranges needed for the backfill operation.
// Ranges are chunks of LedgersPerRange ledgers each.
// Used by coordinator for determining how many range orchestrators to spawn.
func (c *Config) CalculateRangeCount() uint32 {
	totalLedgers := c.Backfill.EndLedger - c.Backfill.StartLedger + 1
	return uint32((totalLedgers + LedgersPerRange - 1) / LedgersPerRange)
}
