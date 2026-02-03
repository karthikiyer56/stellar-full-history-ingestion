package types

// RocksDBSettings contains configuration for RocksDB stores.
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
