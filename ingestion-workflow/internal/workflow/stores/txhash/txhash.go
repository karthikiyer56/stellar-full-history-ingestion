// =============================================================================
// txhash.go - Transaction Hash Store (RocksDB Implementation)
// =============================================================================
//
// PURPOSE:
//   Maps transaction hashes (32 bytes) to ledger sequence numbers (4 bytes).
//   Enables fast lookups: "Which ledger contains this transaction?"
//
// STORAGE DESIGN:
//   - Backend: RocksDB with 16 column families (CFs)
//   - Partitioning: By first hex character of tx hash (high nibble)
//   - Key: 32-byte transaction hash (raw bytes)
//   - Value: 4-byte ledger sequence number (big-endian uint32)
//
// COLUMN FAMILY PARTITIONING:
//   - 16 CFs: "0", "1", ..., "9", "a", "b", ..., "f"
//   - Why 16: Balance between parallelism (good for compaction/RecSplit) and overhead
//   - Partition by high nibble: SHA-256 hashes are uniformly distributed
//
// ITERATOR OPTIMIZATION:
//   - Custom ReadOptions per iterator (owned, destroyed in Close)
//   - 2MB readahead: Prefetches data for sequential scans (I/O optimization)
//   - FillCache=false: Prevents cache pollution during full-table scans
//
// =============================================================================

package txhash

import (
	"fmt"
	"github.com/karthikiyer56/stellar-full-history-ingestion/ingestion-workflow/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/ingestion-workflow/internal/workflow/stores/rocksdb"
	"github.com/karthikiyer56/stellar-full-history-ingestion/ingestion-workflow/internal/workflow/stores/txhash/cf"
	"github.com/karthikiyer56/stellar-full-history-ingestion/ingestion-workflow/internal/workflow/types"
	"path/filepath"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/linxGnu/grocksdb"
)

// RocksDbTxHashStore is a RocksDB-backed implementation of transaction hash storage.
// It partitions transaction hashes across column families for efficient lookups.
type RocksDbTxHashStore struct {
	rocksdb.BaseStore
	cfHandles map[string]*grocksdb.ColumnFamilyHandle
	cfOpts    []*grocksdb.Options
	settings  *types.TxHashRocksDBSettings
}

// NewRocksDbTxHashStore creates a new RocksDB-backed transaction hash store.
func NewRocksDbTxHashStore(dataDir string, rangeID uint32, settings *types.TxHashRocksDBSettings) (*RocksDbTxHashStore, error) {
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

	return &RocksDbTxHashStore{
		BaseStore: rocksdb.BaseStore{
			Opts:       opts,
			BlockCache: blockCache,
			Path:       path,
		},
		settings: settings,
	}, nil
}

func (s *RocksDbTxHashStore) Open() (time.Duration, error) {
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

func (s *RocksDbTxHashStore) WriteBatch(entriesByCF map[string][]interfaces.Entry) (map[string]time.Duration, error) {
	timings := make(map[string]time.Duration)

	batchStart := time.Now()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	for cfName, entries := range entriesByCF {
		if len(entries) == 0 {
			continue
		}
		cfHandle := s.cfHandles[cfName]
		for _, entry := range entries {
			batch.PutCF(cfHandle, entry.Key, entry.Value)
		}
	}
	timings["batch_build"] = time.Since(batchStart)

	writeStart := time.Now()
	wo := grocksdb.NewDefaultWriteOptions()
	wo.SetSync(false)
	defer wo.Destroy()

	if err := s.DB.Write(wo, batch); err != nil {
		return timings, err
	}
	timings["total"] = time.Since(writeStart)

	return timings, nil
}

func (s *RocksDbTxHashStore) Get(txHash []byte) (uint32, bool, error) {
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

func (s *RocksDbTxHashStore) NewScanIteratorCF(cfName string) interfaces.Iterator {
	cfHandle, ok := s.cfHandles[cfName]
	if !ok {
		return nil
	}

	scanOpts := grocksdb.NewDefaultReadOptions()
	scanOpts.SetReadaheadSize(2 * 1024 * 1024)
	scanOpts.SetFillCache(false)

	iter := s.DB.NewIteratorCF(scanOpts, cfHandle)
	return &RocksDbTxHashStoreIterator{
		iter:     iter,
		scanOpts: scanOpts,
	}
}

func (s *RocksDbTxHashStore) CompactAll() (map[string]time.Duration, error) {
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

func (s *RocksDbTxHashStore) GetPath() string {
	return s.Path
}

func (s *RocksDbTxHashStore) GetSize() (int64, error) {
	return helpers.GetDirSize(s.Path), nil
}

func (s *RocksDbTxHashStore) Close() error {
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

// RocksDbTxHashStoreIterator efficiently iterates over transaction hashes in a column family.
// It uses optimized read settings: 2MB readahead and disabled fill cache for sequential scans.
type RocksDbTxHashStoreIterator struct {
	iter     *grocksdb.Iterator
	scanOpts *grocksdb.ReadOptions
}

func (it *RocksDbTxHashStoreIterator) SeekToFirst() {
	it.iter.SeekToFirst()
}

func (it *RocksDbTxHashStoreIterator) Valid() bool {
	return it.iter.Valid()
}

func (it *RocksDbTxHashStoreIterator) Next() {
	it.iter.Next()
}

func (it *RocksDbTxHashStoreIterator) Key() []byte {
	return it.iter.Key().Data()
}

func (it *RocksDbTxHashStoreIterator) Value() []byte {
	return it.iter.Value().Data()
}

func (it *RocksDbTxHashStoreIterator) Error() error {
	return it.iter.Err()
}

func (it *RocksDbTxHashStoreIterator) Close() {
	it.iter.Close()
	if it.scanOpts != nil {
		it.scanOpts.Destroy()
	}
}

var _ interfaces.TxHashStore = (*RocksDbTxHashStore)(nil)
var _ interfaces.Iterator = (*RocksDbTxHashStoreIterator)(nil)
