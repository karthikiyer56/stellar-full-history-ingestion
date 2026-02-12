package rocksdb

import "github.com/linxGnu/grocksdb"

// BaseStore contains common fields and cleanup logic for RocksDB stores.
// Embed this struct in store implementations (lcmStore, metaStore, txHashStore)
// to reduce code duplication and ensure consistent resource cleanup.
type BaseStore struct {
	DB         *grocksdb.DB
	Opts       *grocksdb.Options
	WriteOpts  *grocksdb.WriteOptions
	ReadOpts   *grocksdb.ReadOptions
	BlockCache *grocksdb.Cache
	Path       string
}

// CloseBase cleans up all RocksDB resources.
// Call this from the embedding store's Close() method.
// Order matters: write/read options first (lightweight), then DB close,
// then DB options, then block cache (can be destroyed last).
func (b *BaseStore) CloseBase() error {
	if b.WriteOpts != nil {
		b.WriteOpts.Destroy()
		b.WriteOpts = nil
	}

	if b.ReadOpts != nil {
		b.ReadOpts.Destroy()
		b.ReadOpts = nil
	}

	if b.DB != nil {
		b.DB.Close()
		b.DB = nil
	}

	if b.Opts != nil {
		b.Opts.Destroy()
		b.Opts = nil
	}

	if b.BlockCache != nil {
		b.BlockCache.Destroy()
		b.BlockCache = nil
	}

	return nil
}
