package types

// DEPRECATED: RocksDBSettings is the old global RocksDB configuration.
// Use LedgerRocksDBSettings, TxHashRocksDBSettings, and MetaRocksDBSettings instead.
type RocksDBSettings struct {
	// BlockCacheMB is the size of the RocksDB block cache in MB.
	// Used for read operations to cache frequently accessed blocks.
	BlockCacheMB int

	// WriteBufferMB is the size of each MemTable in MB.
	// Total memtable RAM = WriteBufferMB × MaxWriteBufferNumber.
	WriteBufferMB int

	// MaxWriteBufferNumber is the maximum number of MemTables before flushing.
	// Total memtable RAM = WriteBufferMB × MaxWriteBufferNumber.
	MaxWriteBufferNumber int
}

// LedgerRocksDBSettings contains RocksDB tuning parameters for the ledger store.
// Ledger store holds large LedgerCloseMeta objects (~1MB each).
type LedgerRocksDBSettings struct {
	// WriteBufferMB is the size of each MemTable in MB.
	// Total memtable RAM = WriteBufferMB × MaxWriteBufferNumber.
	// Larger buffers reduce flush frequency for high-throughput writes.
	WriteBufferMB int `toml:"write_buffer_mb"`

	// MaxWriteBufferNumber is the maximum number of MemTables before flushing.
	// Total memtable RAM = WriteBufferMB × MaxWriteBufferNumber.
	MaxWriteBufferNumber int `toml:"max_write_buffer_number"`

	// TargetFileSizeMB is the target size for SST files in MB.
	// Larger files reduce the number of files but may impact read latency.
	TargetFileSizeMB int `toml:"target_file_size_mb"`

	// BlockCacheMB is the size of the RocksDB block cache in MB.
	// Used for read operations to cache frequently accessed blocks.
	BlockCacheMB int `toml:"block_cache_mb"`

	// DisableWAL disables the Write-Ahead Log for writes.
	// Faster ingestion but less crash safety.
	DisableWAL bool `toml:"disable_wal"`
}

// TxHashRocksDBSettings contains RocksDB tuning parameters for the transaction hash store.
// TxHash store is partitioned into 16 column families by hash prefix.
// Uses 16 CFs + "default" CF = 17 total, so multiply memory needs accordingly.
type TxHashRocksDBSettings struct {
	// WriteBufferMB is the size of each MemTable in MB.
	// Total memtable RAM = WriteBufferMB × MaxWriteBufferNumber × 17 (column families).
	WriteBufferMB int `toml:"write_buffer_mb"`

	// MaxWriteBufferNumber is the maximum number of MemTables before flushing.
	// Total memtable RAM = WriteBufferMB × MaxWriteBufferNumber × 17 (column families).
	MaxWriteBufferNumber int `toml:"max_write_buffer_number"`

	// TargetFileSizeMB is the target size for SST files in MB.
	TargetFileSizeMB int `toml:"target_file_size_mb"`

	// BlockCacheMB is the size of the RocksDB block cache in MB.
	// Shared across all column families.
	BlockCacheMB int `toml:"block_cache_mb"`

	// DisableWAL disables the Write-Ahead Log for writes.
	// Faster ingestion but less crash safety.
	DisableWAL bool `toml:"disable_wal"`
}

// MetaRocksDBSettings contains RocksDB tuning parameters for the meta store.
// Meta store holds checkpoint and progress data (tiny datasets).
// Prioritizes durability and small data size over throughput.
type MetaRocksDBSettings struct {
	// WriteBufferMB is the size of each MemTable in MB.
	// Total memtable RAM = WriteBufferMB × MaxWriteBufferNumber.
	WriteBufferMB int `toml:"write_buffer_mb"`

	// MaxWriteBufferNumber is the maximum number of MemTables before flushing.
	// Total memtable RAM = WriteBufferMB × MaxWriteBufferNumber.
	MaxWriteBufferNumber int `toml:"max_write_buffer_number"`

	// TargetFileSizeMB is the target size for SST files in MB.
	TargetFileSizeMB int `toml:"target_file_size_mb"`

	// BlockCacheMB is the size of the RocksDB block cache in MB.
	BlockCacheMB int `toml:"block_cache_mb"`
}
