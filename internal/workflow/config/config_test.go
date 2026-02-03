package config

import (
	"os"
	"path/filepath"
	"testing"
	"text/template"
)

func TestLoadValidConfig(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir := t.TempDir()

	// Create a valid TOML config file
	configPath := filepath.Join(tmpDir, "config.toml")
	configContent := `
[service]
data_dir = "/tmp/test"
mode = "backfill"

[backfill]
start_ledger = 2
end_ledger = 10000001
ledger_backend = "buffered_storage"
parallel_ranges = 1

[backfill.buffered_storage]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Verify parsed values
	if cfg.Service.DataDir != "/tmp/test" {
		t.Errorf("Expected data_dir='/tmp/test', got '%s'", cfg.Service.DataDir)
	}
	if cfg.Service.Mode != "backfill" {
		t.Errorf("Expected mode='backfill', got '%s'", cfg.Service.Mode)
	}
	if cfg.Backfill.StartLedger != 2 {
		t.Errorf("Expected start_ledger=2, got %d", cfg.Backfill.StartLedger)
	}
	if cfg.Backfill.EndLedger != 10000001 {
		t.Errorf("Expected end_ledger=10000001, got %d", cfg.Backfill.EndLedger)
	}
	if cfg.Backfill.BufferedStorage.BucketPath != "sdf-ledger-close-meta/v1/ledgers/pubnet" {
		t.Errorf("Expected bucket_path='sdf-ledger-close-meta/v1/ledgers/pubnet', got '%s'",
			cfg.Backfill.BufferedStorage.BucketPath)
	}

	// Verify defaults were applied
	if cfg.Backfill.ParallelRanges != 1 {
		t.Errorf("Expected parallel_ranges default=1, got %d", cfg.Backfill.ParallelRanges)
	}
	if cfg.Backfill.CheckpointInterval != 1000 {
		t.Errorf("Expected checkpoint_interval default=1000, got %d", cfg.Backfill.CheckpointInterval)
	}
	if cfg.Backfill.BufferedStorage.BufferSize != 10000 {
		t.Errorf("Expected buffer_size default=10000, got %d", cfg.Backfill.BufferedStorage.BufferSize)
	}
	if cfg.Backfill.BufferedStorage.NumWorkers != 200 {
		t.Errorf("Expected num_workers default=200, got %d", cfg.Backfill.BufferedStorage.NumWorkers)
	}
	if cfg.ActiveStores.BasePath != "active/rocksdb" {
		t.Errorf("Expected base_path default='active/rocksdb', got '%s'", cfg.ActiveStores.BasePath)
	}
	if cfg.ImmutableStores.LedgersBase != "immutable/ledgers" {
		t.Errorf("Expected ledgers_base default='immutable/ledgers', got '%s'", cfg.ImmutableStores.LedgersBase)
	}
	if cfg.ImmutableStores.TxHashBase != "immutable/txhash" {
		t.Errorf("Expected txhash_base default='immutable/txhash', got '%s'", cfg.ImmutableStores.TxHashBase)
	}
	if cfg.RocksDB.BlockCacheMB != 8192 {
		t.Errorf("Expected block_cache_mb default=8192, got %d", cfg.RocksDB.BlockCacheMB)
	}
	if cfg.RocksDB.WriteBufferMB != 512 {
		t.Errorf("Expected write_buffer_mb default=512, got %d", cfg.RocksDB.WriteBufferMB)
	}
	if cfg.RocksDB.MaxWriteBufferNumber != 2 {
		t.Errorf("Expected max_write_buffer_number default=2, got %d", cfg.RocksDB.MaxWriteBufferNumber)
	}
	if cfg.Metrics.LogIntervalBatches != 10 {
		t.Errorf("Expected log_interval_batches default=10, got %d", cfg.Metrics.LogIntervalBatches)
	}
}

func TestValidateRequiredFields(t *testing.T) {
	tests := []struct {
		name        string
		configTOML  string
		shouldFail  bool
		errorSubstr string
	}{
		{
			name: "valid config",
			configTOML: `
[service]
data_dir = "/tmp/test"
mode = "backfill"

[backfill]
start_ledger = 2
end_ledger = 10000001
ledger_backend = "buffered_storage"

[backfill.buffered_storage]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
`,
			shouldFail: false,
		},
		{
			name: "missing data_dir",
			configTOML: `
[service]
mode = "backfill"

[backfill]
start_ledger = 2
end_ledger = 10000001
ledger_backend = "buffered_storage"

[backfill.buffered_storage]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
`,
			shouldFail:  true,
			errorSubstr: "service.data_dir is required",
		},
		{
			name: "missing mode",
			configTOML: `
[service]
data_dir = "/tmp/test"

[backfill]
start_ledger = 2
end_ledger = 10000001
ledger_backend = "buffered_storage"

[backfill.buffered_storage]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
`,
			shouldFail:  true,
			errorSubstr: "service.mode is required",
		},
		{
			name: "missing start_ledger",
			configTOML: `
[service]
data_dir = "/tmp/test"
mode = "backfill"

[backfill]
end_ledger = 10000001
ledger_backend = "buffered_storage"

[backfill.buffered_storage]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
`,
			shouldFail:  true,
			errorSubstr: "backfill.start_ledger is required",
		},
		{
			name: "missing end_ledger",
			configTOML: `
[service]
data_dir = "/tmp/test"
mode = "backfill"

[backfill]
start_ledger = 2
ledger_backend = "buffered_storage"

[backfill.buffered_storage]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
`,
			shouldFail:  true,
			errorSubstr: "backfill.end_ledger is required",
		},
		{
			name: "missing ledger_backend",
			configTOML: `
[service]
data_dir = "/tmp/test"
mode = "backfill"

[backfill]
start_ledger = 2
end_ledger = 10000001

[backfill.buffered_storage]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
`,
			shouldFail:  true,
			errorSubstr: "backfill.ledger_backend is required",
		},
		{
			name: "missing bucket_path for buffered_storage backend",
			configTOML: `
[service]
data_dir = "/tmp/test"
mode = "backfill"

[backfill]
start_ledger = 2
end_ledger = 10000001
ledger_backend = "buffered_storage"
`,
			shouldFail:  true,
			errorSubstr: "backfill.buffered_storage.bucket_path is required",
		},
		{
			name: "start_ledger >= end_ledger",
			configTOML: `
[service]
data_dir = "/tmp/test"
mode = "backfill"

[backfill]
start_ledger = 10000001
end_ledger = 10000001
ledger_backend = "buffered_storage"

[backfill.buffered_storage]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
`,
			shouldFail:  true,
			errorSubstr: "must be less than",
		},
		{
			name: "parallel_ranges zero (invalid)",
			configTOML: `
[service]
data_dir = "/tmp/test"
mode = "backfill"

[backfill]
start_ledger = 2
end_ledger = 10000001
ledger_backend = "buffered_storage"

[backfill.buffered_storage]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
`,
			shouldFail: false, // Will NOT fail because 0 gets defaulted to 1 in LoadConfig
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configPath := filepath.Join(tmpDir, "config.toml")

			err := os.WriteFile(configPath, []byte(tt.configTOML), 0644)
			if err != nil {
				t.Fatalf("Failed to write test config: %v", err)
			}

			cfg, err := LoadConfig(configPath)
			if err != nil {
				t.Fatalf("LoadConfig failed: %v", err)
			}

			validErr := cfg.Validate()

			if tt.shouldFail && validErr == nil {
				t.Errorf("Expected validation to fail but it succeeded")
			}
			if !tt.shouldFail && validErr != nil {
				t.Errorf("Expected validation to succeed but got error: %v", validErr)
			}
			if tt.shouldFail && validErr != nil && tt.errorSubstr != "" {
				if !contains(validErr.Error(), tt.errorSubstr) {
					t.Errorf("Expected error to contain '%s', got: %v", tt.errorSubstr, validErr)
				}
			}
		})
	}
}

func TestCalculateRangeCount(t *testing.T) {
	tests := []struct {
		name        string
		startLedger uint32
		endLedger   uint32
		expected    uint32
	}{
		{
			name:        "single range",
			startLedger: 2,
			endLedger:   10000001,
			expected:    1,
		},
		{
			name:        "exactly two ranges",
			startLedger: 2,
			endLedger:   20000001,
			expected:    2,
		},
		{
			name:        "three ranges with remainder",
			startLedger: 2,
			endLedger:   30000001,
			expected:    3,
		},
		{
			name:        "small ledger count",
			startLedger: 100,
			endLedger:   200,
			expected:    1,
		},
		{
			name:        "exactly LedgersPerRange ledgers",
			startLedger: 1,
			endLedger:   10_000_000,
			expected:    1,
		},
		{
			name:        "LedgersPerRange + 1",
			startLedger: 1,
			endLedger:   10_000_001,
			expected:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				Backfill: BackfillConfig{
					StartLedger: tt.startLedger,
					EndLedger:   tt.endLedger,
				},
			}

			rangeCount := cfg.CalculateRangeCount()
			if rangeCount != tt.expected {
				t.Errorf("Expected %d ranges, got %d (ledgers: %d to %d, total: %d)",
					tt.expected, rangeCount, tt.startLedger, tt.endLedger,
					tt.endLedger-tt.startLedger+1)
			}
		})
	}
}

func TestDefaultsAreAppliedAfterParsing(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")

	// Minimal config - only required fields, no optional fields specified
	configContent := `
[service]
data_dir = "/tmp/test"
mode = "backfill"

[backfill]
start_ledger = 2
end_ledger = 10000001
ledger_backend = "buffered_storage"

[backfill.buffered_storage]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// All defaults should be applied
	defaults := map[string]interface{}{
		"parallel_ranges":         int(1),
		"checkpoint_interval":     int(1000),
		"buffer_size":             int(10000),
		"num_workers":             int(200),
		"base_path":               "active/rocksdb",
		"ledgers_base":            "immutable/ledgers",
		"txhash_base":             "immutable/txhash",
		"block_cache_mb":          int(8192),
		"write_buffer_mb":         int(512),
		"max_write_buffer_number": int(2),
		"log_interval_batches":    int(10),
	}

	if cfg.Backfill.ParallelRanges != defaults["parallel_ranges"] {
		t.Errorf("parallel_ranges: expected %v, got %v", defaults["parallel_ranges"], cfg.Backfill.ParallelRanges)
	}
	if cfg.Backfill.CheckpointInterval != defaults["checkpoint_interval"] {
		t.Errorf("checkpoint_interval: expected %v, got %v", defaults["checkpoint_interval"], cfg.Backfill.CheckpointInterval)
	}
	if cfg.Backfill.BufferedStorage.BufferSize != defaults["buffer_size"] {
		t.Errorf("buffer_size: expected %v, got %v", defaults["buffer_size"], cfg.Backfill.BufferedStorage.BufferSize)
	}
	if cfg.Backfill.BufferedStorage.NumWorkers != defaults["num_workers"] {
		t.Errorf("num_workers: expected %v, got %v", defaults["num_workers"], cfg.Backfill.BufferedStorage.NumWorkers)
	}
	if cfg.ActiveStores.BasePath != defaults["base_path"] {
		t.Errorf("base_path: expected %v, got %v", defaults["base_path"], cfg.ActiveStores.BasePath)
	}
	if cfg.ImmutableStores.LedgersBase != defaults["ledgers_base"] {
		t.Errorf("ledgers_base: expected %v, got %v", defaults["ledgers_base"], cfg.ImmutableStores.LedgersBase)
	}
	if cfg.ImmutableStores.TxHashBase != defaults["txhash_base"] {
		t.Errorf("txhash_base: expected %v, got %v", defaults["txhash_base"], cfg.ImmutableStores.TxHashBase)
	}
	if cfg.RocksDB.BlockCacheMB != defaults["block_cache_mb"] {
		t.Errorf("block_cache_mb: expected %v, got %v", defaults["block_cache_mb"], cfg.RocksDB.BlockCacheMB)
	}
	if cfg.RocksDB.WriteBufferMB != defaults["write_buffer_mb"] {
		t.Errorf("write_buffer_mb: expected %v, got %v", defaults["write_buffer_mb"], cfg.RocksDB.WriteBufferMB)
	}
	if cfg.RocksDB.MaxWriteBufferNumber != defaults["max_write_buffer_number"] {
		t.Errorf("max_write_buffer_number: expected %v, got %v", defaults["max_write_buffer_number"], cfg.RocksDB.MaxWriteBufferNumber)
	}
	if cfg.Metrics.LogIntervalBatches != defaults["log_interval_batches"] {
		t.Errorf("log_interval_batches: expected %v, got %v", defaults["log_interval_batches"], cfg.Metrics.LogIntervalBatches)
	}
}

func TestLoadConfigFileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.toml")
	if err == nil {
		t.Errorf("Expected error for nonexistent file, got nil")
	}
	if !contains(err.Error(), "failed to read config file") {
		t.Errorf("Expected error to contain 'failed to read config file', got: %v", err)
	}
}

func TestLoadConfigInvalidTOML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")

	// Invalid TOML
	invalidTOML := `
[service
data_dir = "/tmp/test"  # missing ]
`

	err := os.WriteFile(configPath, []byte(invalidTOML), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	_, err = LoadConfig(configPath)
	if err == nil {
		t.Errorf("Expected error for invalid TOML, got nil")
	}
	if !contains(err.Error(), "failed to parse config file") {
		t.Errorf("Expected error to contain 'failed to parse config file', got: %v", err)
	}
}

func contains(s, substr string) bool {
	for i := 0; i < len(s)-len(substr)+1; i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestOverrideDefaults(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")

	// Override some defaults
	configContent := `
[service]
data_dir = "/tmp/test"
mode = "backfill"

[backfill]
start_ledger = 2
end_ledger = 30000001
ledger_backend = "buffered_storage"
parallel_ranges = 4
checkpoint_interval = 5000

[backfill.buffered_storage]
bucket_path = "sdf-ledger-close-meta/v1/ledgers/pubnet"
buffer_size = 20000
num_workers = 400

[active_stores]
base_path = "custom/rocksdb"

[immutable_stores]
ledgers_base = "custom/ledgers"
txhash_base = "custom/txhash"

[rocksdb]
block_cache_mb = 16384
write_buffer_mb = 1024
max_write_buffer_number = 4

[metrics]
csv_output = "/tmp/metrics.csv"
log_interval_batches = 20
`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Verify overridden values
	if cfg.Backfill.ParallelRanges != 4 {
		t.Errorf("Expected parallel_ranges=4, got %d", cfg.Backfill.ParallelRanges)
	}
	if cfg.Backfill.CheckpointInterval != 5000 {
		t.Errorf("Expected checkpoint_interval=5000, got %d", cfg.Backfill.CheckpointInterval)
	}
	if cfg.Backfill.BufferedStorage.BufferSize != 20000 {
		t.Errorf("Expected buffer_size=20000, got %d", cfg.Backfill.BufferedStorage.BufferSize)
	}
	if cfg.Backfill.BufferedStorage.NumWorkers != 400 {
		t.Errorf("Expected num_workers=400, got %d", cfg.Backfill.BufferedStorage.NumWorkers)
	}
	if cfg.ActiveStores.BasePath != "custom/rocksdb" {
		t.Errorf("Expected base_path='custom/rocksdb', got '%s'", cfg.ActiveStores.BasePath)
	}
	if cfg.ImmutableStores.LedgersBase != "custom/ledgers" {
		t.Errorf("Expected ledgers_base='custom/ledgers', got '%s'", cfg.ImmutableStores.LedgersBase)
	}
	if cfg.ImmutableStores.TxHashBase != "custom/txhash" {
		t.Errorf("Expected txhash_base='custom/txhash', got '%s'", cfg.ImmutableStores.TxHashBase)
	}
	if cfg.RocksDB.BlockCacheMB != 16384 {
		t.Errorf("Expected block_cache_mb=16384, got %d", cfg.RocksDB.BlockCacheMB)
	}
	if cfg.RocksDB.WriteBufferMB != 1024 {
		t.Errorf("Expected write_buffer_mb=1024, got %d", cfg.RocksDB.WriteBufferMB)
	}
	if cfg.RocksDB.MaxWriteBufferNumber != 4 {
		t.Errorf("Expected max_write_buffer_number=4, got %d", cfg.RocksDB.MaxWriteBufferNumber)
	}
	if cfg.Metrics.CSVOutput != "/tmp/metrics.csv" {
		t.Errorf("Expected csv_output='/tmp/metrics.csv', got '%s'", cfg.Metrics.CSVOutput)
	}
	if cfg.Metrics.LogIntervalBatches != 20 {
		t.Errorf("Expected log_interval_batches=20, got %d", cfg.Metrics.LogIntervalBatches)
	}

	// Verify validation still works
	validErr := cfg.Validate()
	if validErr != nil {
		t.Errorf("Expected validation to succeed, got error: %v", validErr)
	}
}

var _ = template.FuncMap{}

// TestDirectConfigValidation tests validation on directly-constructed configs.
// This allows testing cases like parallel_ranges=0 that can't be represented in TOML
// (since 0 gets defaulted to 1 during LoadConfig).
func TestDirectConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		setupCfg    func() *Config
		shouldFail  bool
		errorSubstr string
	}{
		{
			name: "parallel_ranges explicitly 0 should fail",
			setupCfg: func() *Config {
				return &Config{
					Service: ServiceConfig{
						DataDir: "/tmp/test",
						Mode:    "backfill",
					},
					Backfill: BackfillConfig{
						StartLedger:    2,
						EndLedger:      10000001,
						LedgerBackend:  "buffered_storage",
						ParallelRanges: 0, // Explicitly set to 0
						BufferedStorage: BufferedStorageConfig{
							BucketPath: "sdf-ledger-close-meta/v1/ledgers/pubnet",
						},
					},
				}
			},
			shouldFail:  true,
			errorSubstr: "parallel_ranges must be >= 1",
		},
		{
			name: "parallel_ranges = 1 should pass",
			setupCfg: func() *Config {
				return &Config{
					Service: ServiceConfig{
						DataDir: "/tmp/test",
						Mode:    "backfill",
					},
					Backfill: BackfillConfig{
						StartLedger:    2,
						EndLedger:      10000001,
						LedgerBackend:  "buffered_storage",
						ParallelRanges: 1,
						BufferedStorage: BufferedStorageConfig{
							BucketPath: "sdf-ledger-close-meta/v1/ledgers/pubnet",
						},
					},
				}
			},
			shouldFail: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.setupCfg()
			err := cfg.Validate()

			if tt.shouldFail && err == nil {
				t.Errorf("Expected validation to fail but it succeeded")
			}
			if !tt.shouldFail && err != nil {
				t.Errorf("Expected validation to succeed but got error: %v", err)
			}
			if tt.shouldFail && err != nil && tt.errorSubstr != "" {
				if !contains(err.Error(), tt.errorSubstr) {
					t.Errorf("Expected error to contain '%s', got: %v", tt.errorSubstr, err)
				}
			}
		})
	}
}
