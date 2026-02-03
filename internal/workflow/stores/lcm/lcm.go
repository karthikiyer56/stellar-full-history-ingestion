package lcm

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/types"
	"github.com/linxGnu/grocksdb"
)

type lcmStore struct {
	db        *grocksdb.DB
	opts      *grocksdb.Options
	writeOpts *grocksdb.WriteOptions
	readOpts  *grocksdb.ReadOptions
	path      string
}

func NewLCMStore(dataDir string, rangeID uint32, settings *types.RocksDBSettings) (*lcmStore, error) {
	path := filepath.Join(dataDir, "active", "rocksdb", fmt.Sprintf("%04d-ledger-store", rangeID))

	if err := helpers.EnsureDir(path); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetDisableAutoCompactions(true)

	if settings.BlockCacheMB > 0 {
		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		cache := grocksdb.NewLRUCache(uint64(settings.BlockCacheMB * 1024 * 1024))
		bbto.SetBlockCache(cache)
		opts.SetBlockBasedTableFactory(bbto)
	}

	if settings.WriteBufferMB > 0 {
		opts.SetWriteBufferSize(uint64(settings.WriteBufferMB * 1024 * 1024))
	}

	if settings.MaxWriteBufferNumber > 0 {
		opts.SetMaxWriteBufferNumber(settings.MaxWriteBufferNumber)
	}

	return &lcmStore{
		opts: opts,
		path: path,
	}, nil
}

func (s *lcmStore) Open() (time.Duration, error) {
	start := time.Now()

	db, err := grocksdb.OpenDb(s.opts, s.path)
	if err != nil {
		return 0, fmt.Errorf("failed to open RocksDB at %s: %w", s.path, err)
	}

	s.db = db
	s.writeOpts = grocksdb.NewDefaultWriteOptions()
	s.writeOpts.SetSync(false)
	s.readOpts = grocksdb.NewDefaultReadOptions()

	return time.Since(start), nil
}

func (s *lcmStore) WriteBatch(entries map[uint32][]byte) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for ledgerSeq, lcmBytes := range entries {
		key := helpers.Uint32ToBytes(ledgerSeq)
		batch.Put(key, lcmBytes)
	}

	return s.db.Write(s.writeOpts, batch)
}

func (s *lcmStore) Get(ledgerSeq uint32) ([]byte, error) {
	key := helpers.Uint32ToBytes(ledgerSeq)

	slice, err := s.db.Get(s.readOpts, key)
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
	iter := s.db.NewIterator(s.readOpts)
	return &lcmIterator{iter: iter}
}

func (s *lcmStore) Compact() (time.Duration, error) {
	start := time.Now()
	s.db.CompactRange(grocksdb.Range{})
	return time.Since(start), nil
}

func (s *lcmStore) GetPath() string {
	return s.path
}

func (s *lcmStore) GetSize() (int64, error) {
	return helpers.GetDirSize(s.path), nil
}

func (s *lcmStore) Close() error {
	if s.writeOpts != nil {
		s.writeOpts.Destroy()
		s.writeOpts = nil
	}

	if s.readOpts != nil {
		s.readOpts.Destroy()
		s.readOpts = nil
	}

	if s.db != nil {
		s.db.Close()
		s.db = nil
	}

	if s.opts != nil {
		s.opts.Destroy()
		s.opts = nil
	}

	return nil
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
