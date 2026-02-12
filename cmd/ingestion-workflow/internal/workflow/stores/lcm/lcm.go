package lcm

import (
	"fmt"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/stores/rocksdb"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/types"
	"path/filepath"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/linxGnu/grocksdb"
)

// RocksDbLedgerStore is a RocksDB-backed implementation of LedgerStore interface.
// It stores serialized LCM (Ledger Close Meta) data indexed by ledger sequence number.
type RocksDbLedgerStore struct {
	rocksdb.BaseStore
}

// NewRocksDbLedgerStore creates a new RocksDB ledger store for the given range.
func NewRocksDbLedgerStore(dataDir string, rangeID uint32, settings *types.LedgerRocksDBSettings) (*RocksDbLedgerStore, error) {
	path := filepath.Join(dataDir, "active", "rocksdb", fmt.Sprintf("%04d-ledger-store", rangeID))

	if err := helpers.EnsureDir(path); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetDisableAutoCompactions(true)

	var blockCache *grocksdb.Cache
	if settings.BlockCacheMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(settings.BlockCacheMB * 1024 * 1024))
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		bbto.SetBlockCache(blockCache)
		opts.SetBlockBasedTableFactory(bbto)
	}

	if settings.WriteBufferMB > 0 {
		opts.SetWriteBufferSize(uint64(settings.WriteBufferMB * 1024 * 1024))
	}

	if settings.MaxWriteBufferNumber > 0 {
		opts.SetMaxWriteBufferNumber(settings.MaxWriteBufferNumber)
	}

	if settings.TargetFileSizeMB > 0 {
		opts.SetTargetFileSizeBase(uint64(settings.TargetFileSizeMB * 1024 * 1024))
	}

	return &RocksDbLedgerStore{
		BaseStore: rocksdb.BaseStore{
			Opts:       opts,
			BlockCache: blockCache,
			Path:       path,
		},
	}, nil
}

func (s *RocksDbLedgerStore) Open() (time.Duration, error) {
	start := time.Now()

	db, err := grocksdb.OpenDb(s.Opts, s.Path)
	if err != nil {
		return 0, fmt.Errorf("failed to open RocksDB at %s: %w", s.Path, err)
	}

	s.DB = db
	s.WriteOpts = grocksdb.NewDefaultWriteOptions()
	s.WriteOpts.SetSync(false)
	s.ReadOpts = grocksdb.NewDefaultReadOptions()

	return time.Since(start), nil
}

func (s *RocksDbLedgerStore) WriteBatch(entries map[uint32][]byte) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for ledgerSeq, lcmBytes := range entries {
		key := helpers.Uint32ToBytes(ledgerSeq)
		batch.Put(key, lcmBytes)
	}

	return s.DB.Write(s.WriteOpts, batch)
}

func (s *RocksDbLedgerStore) Get(ledgerSeq uint32) ([]byte, error) {
	key := helpers.Uint32ToBytes(ledgerSeq)

	slice, err := s.DB.Get(s.ReadOpts, key)
	if err != nil {
		return nil, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil, fmt.Errorf("ledger %d not found", ledgerSeq)
	}

	value := make([]byte, slice.Size())
	copy(value, slice.Data())
	return value, nil
}

func (s *RocksDbLedgerStore) NewIterator() interfaces.Iterator {
	readOpts := grocksdb.NewDefaultReadOptions()
	readOpts.SetReadaheadSize(2 * 1024 * 1024) // 2MB prefetch for sequential scans
	readOpts.SetFillCache(false)               // Don't pollute block cache during iteration

	iter := s.DB.NewIterator(readOpts)
	return &RocksDbLedgerStoreIterator{iter: iter, readOpts: readOpts}
}

func (s *RocksDbLedgerStore) Compact() (time.Duration, error) {
	start := time.Now()
	s.DB.CompactRange(grocksdb.Range{})
	return time.Since(start), nil
}

func (s *RocksDbLedgerStore) GetPath() string {
	return s.Path
}

func (s *RocksDbLedgerStore) GetSize() (int64, error) {
	return helpers.GetDirSize(s.Path), nil
}

func (s *RocksDbLedgerStore) Close() error {
	return s.CloseBase()
}

// RocksDbLedgerStoreIterator provides optimized sequential iteration over ledgers.
// It uses 2MB readahead and disables block cache insertion to improve throughput
// for full-range scans without affecting point lookup performance.
type RocksDbLedgerStoreIterator struct {
	iter     *grocksdb.Iterator
	readOpts *grocksdb.ReadOptions
}

func (it *RocksDbLedgerStoreIterator) SeekToFirst() {
	it.iter.SeekToFirst()
}

func (it *RocksDbLedgerStoreIterator) Valid() bool {
	return it.iter.Valid()
}

func (it *RocksDbLedgerStoreIterator) Next() {
	it.iter.Next()
}

func (it *RocksDbLedgerStoreIterator) Key() []byte {
	return it.iter.Key().Data()
}

func (it *RocksDbLedgerStoreIterator) Value() []byte {
	return it.iter.Value().Data()
}

func (it *RocksDbLedgerStoreIterator) Error() error {
	return it.iter.Err()
}

func (it *RocksDbLedgerStoreIterator) Close() {
	it.iter.Close()
	if it.readOpts != nil {
		it.readOpts.Destroy()
	}
}

var _ interfaces.LedgerStore = (*RocksDbLedgerStore)(nil)
var _ interfaces.Iterator = (*RocksDbLedgerStoreIterator)(nil)
