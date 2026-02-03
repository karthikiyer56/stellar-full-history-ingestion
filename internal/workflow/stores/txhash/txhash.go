package txhash

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/stores/rocksdb"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/stores/txhash/cf"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/types"
	"github.com/linxGnu/grocksdb"
)

type txHashStore struct {
	rocksdb.BaseStore
	cfHandles map[string]*grocksdb.ColumnFamilyHandle
	cfOpts    []*grocksdb.Options
	settings  *types.TxHashRocksDBSettings
}

func NewTxHashStore(dataDir string, rangeID uint32, settings *types.TxHashRocksDBSettings) (*txHashStore, error) {
	path := filepath.Join(dataDir, "active", "rocksdb", fmt.Sprintf("%04d-txhash-store", rangeID))

	if err := helpers.EnsureDir(path); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	var blockCache *grocksdb.Cache
	if settings.BlockCacheMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(settings.BlockCacheMB * 1024 * 1024))
	}

	return &txHashStore{
		BaseStore: rocksdb.BaseStore{
			Opts:       opts,
			BlockCache: blockCache,
			Path:       path,
		},
		settings: settings,
	}, nil
}

func (s *txHashStore) Open() (time.Duration, error) {
	start := time.Now()

	cfNames := []string{"default"}
	cfNames = append(cfNames, cf.Names...)

	cfOptsList := make([]*grocksdb.Options, len(cfNames))
	cfOptsList[0] = grocksdb.NewDefaultOptions()

	for i := 1; i < len(cfNames); i++ {
		cfOpt := grocksdb.NewDefaultOptions()
		cfOpt.SetDisableAutoCompactions(true)

		// Apply per-store settings
		if s.settings.WriteBufferMB > 0 {
			cfOpt.SetWriteBufferSize(uint64(s.settings.WriteBufferMB * 1024 * 1024))
		}
		if s.settings.MaxWriteBufferNumber > 0 {
			cfOpt.SetMaxWriteBufferNumber(s.settings.MaxWriteBufferNumber)
		}
		if s.settings.TargetFileSizeMB > 0 {
			cfOpt.SetTargetFileSizeBase(uint64(s.settings.TargetFileSizeMB * 1024 * 1024))
		}

		if s.BlockCache != nil {
			bbto := grocksdb.NewDefaultBlockBasedTableOptions()
			bbto.SetBlockCache(s.BlockCache)
			bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10))
			cfOpt.SetBlockBasedTableFactory(bbto)
		}

		cfOptsList[i] = cfOpt
	}

	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(s.Opts, s.Path, cfNames, cfOptsList)
	if err != nil {
		s.Opts.Destroy()
		for _, opt := range cfOptsList {
			if opt != nil {
				opt.Destroy()
			}
		}
		if s.BlockCache != nil {
			s.BlockCache.Destroy()
		}
		return 0, fmt.Errorf("failed to open RocksDB at %s: %w", s.Path, err)
	}

	s.DB = db
	s.cfOpts = cfOptsList
	s.WriteOpts = grocksdb.NewDefaultWriteOptions()
	s.WriteOpts.SetSync(false)
	s.ReadOpts = grocksdb.NewDefaultReadOptions()

	s.cfHandles = make(map[string]*grocksdb.ColumnFamilyHandle)
	for i, name := range cfNames {
		s.cfHandles[name] = cfHandles[i]
	}

	return time.Since(start), nil
}

func (s *txHashStore) WriteBatch(entriesByCF map[string][]interfaces.Entry) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for cfName, entries := range entriesByCF {
		cfHandle, ok := s.cfHandles[cfName]
		if !ok {
			return fmt.Errorf("unknown column family: %s", cfName)
		}

		for _, entry := range entries {
			batch.PutCF(cfHandle, entry.Key, entry.Value)
		}
	}

	return s.DB.Write(s.WriteOpts, batch)
}

func (s *txHashStore) Get(txHash []byte) (uint32, bool, error) {
	cfName := cf.GetName(txHash)
	cfHandle, ok := s.cfHandles[cfName]
	if !ok {
		return 0, false, fmt.Errorf("unknown column family: %s", cfName)
	}

	slice, err := s.DB.GetCF(s.ReadOpts, cfHandle, txHash)
	if err != nil {
		return 0, false, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return 0, false, nil
	}

	if slice.Size() != 4 {
		return 0, false, fmt.Errorf("invalid value size: %d", slice.Size())
	}

	ledgerSeq := helpers.BytesToUint32(slice.Data())
	return ledgerSeq, true, nil
}

func (s *txHashStore) NewScanIteratorCF(cfName string) interfaces.Iterator {
	cfHandle, ok := s.cfHandles[cfName]
	if !ok {
		return nil
	}

	scanOpts := grocksdb.NewDefaultReadOptions()
	scanOpts.SetReadaheadSize(2 * 1024 * 1024)
	scanOpts.SetFillCache(false)

	iter := s.DB.NewIteratorCF(scanOpts, cfHandle)
	return &txHashIterator{
		iter:     iter,
		scanOpts: scanOpts,
	}
}

func (s *txHashStore) CompactAll() (map[string]time.Duration, error) {
	results := make(map[string]time.Duration)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, cfName := range cf.Names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()

			cfHandle := s.cfHandles[name]
			start := time.Now()
			s.DB.CompactRangeCF(cfHandle, grocksdb.Range{})
			duration := time.Since(start)

			mu.Lock()
			results[name] = duration
			mu.Unlock()
		}(cfName)
	}

	wg.Wait()
	return results, nil
}

func (s *txHashStore) GetPath() string {
	return s.Path
}

func (s *txHashStore) GetSize() (int64, error) {
	return helpers.GetDirSize(s.Path), nil
}

func (s *txHashStore) Close() error {
	for _, cfHandle := range s.cfHandles {
		if cfHandle != nil {
			cfHandle.Destroy()
		}
	}
	s.cfHandles = nil

	for _, cfOpt := range s.cfOpts {
		if cfOpt != nil {
			cfOpt.Destroy()
		}
	}
	s.cfOpts = nil

	return s.CloseBase()
}

type txHashIterator struct {
	iter     *grocksdb.Iterator
	scanOpts *grocksdb.ReadOptions
}

func (it *txHashIterator) SeekToFirst() {
	it.iter.SeekToFirst()
}

func (it *txHashIterator) Valid() bool {
	return it.iter.Valid()
}

func (it *txHashIterator) Next() {
	it.iter.Next()
}

func (it *txHashIterator) Key() []byte {
	return it.iter.Key().Data()
}

func (it *txHashIterator) Value() []byte {
	return it.iter.Value().Data()
}

func (it *txHashIterator) Error() error {
	return it.iter.Err()
}

func (it *txHashIterator) Close() {
	it.iter.Close()
	if it.scanOpts != nil {
		it.scanOpts.Destroy()
	}
}

var _ interfaces.TxHashStore = (*txHashStore)(nil)
var _ interfaces.Iterator = (*txHashIterator)(nil)
