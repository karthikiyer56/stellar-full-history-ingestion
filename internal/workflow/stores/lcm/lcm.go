package lcm

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/stores/rocksdb"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/types"
	"github.com/linxGnu/grocksdb"
)

type lcmStore struct {
	rocksdb.BaseStore
}

func NewLCMStore(dataDir string, rangeID uint32, settings *types.LedgerRocksDBSettings) (*lcmStore, error) {
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

	return &lcmStore{
		BaseStore: rocksdb.BaseStore{
			Opts:       opts,
			BlockCache: blockCache,
			Path:       path,
		},
	}, nil
}

func (s *lcmStore) Open() (time.Duration, error) {
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

func (s *lcmStore) WriteBatch(entries map[uint32][]byte) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for ledgerSeq, lcmBytes := range entries {
		key := helpers.Uint32ToBytes(ledgerSeq)
		batch.Put(key, lcmBytes)
	}

	return s.DB.Write(s.WriteOpts, batch)
}

func (s *lcmStore) Get(ledgerSeq uint32) ([]byte, error) {
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

func (s *lcmStore) NewIterator() interfaces.Iterator {
	iter := s.DB.NewIterator(s.ReadOpts)
	return &lcmIterator{iter: iter}
}

func (s *lcmStore) Compact() (time.Duration, error) {
	start := time.Now()
	s.DB.CompactRange(grocksdb.Range{})
	return time.Since(start), nil
}

func (s *lcmStore) GetPath() string {
	return s.Path
}

func (s *lcmStore) GetSize() (int64, error) {
	return helpers.GetDirSize(s.Path), nil
}

func (s *lcmStore) Close() error {
	return s.CloseBase()
}

type lcmIterator struct {
	iter *grocksdb.Iterator
}

func (it *lcmIterator) SeekToFirst() {
	it.iter.SeekToFirst()
}

func (it *lcmIterator) Valid() bool {
	return it.iter.Valid()
}

func (it *lcmIterator) Next() {
	it.iter.Next()
}

func (it *lcmIterator) Key() []byte {
	return it.iter.Key().Data()
}

func (it *lcmIterator) Value() []byte {
	return it.iter.Value().Data()
}

func (it *lcmIterator) Error() error {
	return it.iter.Err()
}

func (it *lcmIterator) Close() {
	it.iter.Close()
}

var _ interfaces.LCMStore = (*lcmStore)(nil)
var _ interfaces.Iterator = (*lcmIterator)(nil)
