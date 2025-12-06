// stores.go
// =============================================================================
// RocksDB Store Management for Stellar Ingestion
// =============================================================================
//
// This module handles:
// - Opening/closing RocksDB databases with appropriate configurations
// - Opening RocksDB reader for source LCM data
// - Writing batches to RocksDB stores
// - Final compaction of stores
//
// =============================================================================

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/linxGnu/grocksdb"
	"github.com/pkg/errors"
)

const (
	MB = 1024 * 1024
	GB = 1024 * 1024 * 1024

	// BlockCacheSizeMB is the fixed block cache size for reading from RocksDB source.
	// This is used only when reading LCMs from an existing RocksDB store.
	BlockCacheSizeMB = 512

	// RawDataFileBufferSize is the buffer size for the raw data file writer.
	// 64MB buffer balances memory usage with write frequency.
	RawDataFileBufferSize = 64 * MB
)

// =============================================================================
// Store Handles
// =============================================================================

// Stores holds all the RocksDB database handles and their options.
type Stores struct {
	// Output stores (writing)
	LcmDB   *grocksdb.DB
	LcmOpts *grocksdb.Options

	TxDataDB   *grocksdb.DB
	TxDataOpts *grocksdb.Options

	HashSeqDB   *grocksdb.DB
	HashSeqOpts *grocksdb.Options

	// Source store (reading)
	SourceDB   *grocksdb.DB
	SourceOpts *grocksdb.Options
	SourceRO   *grocksdb.ReadOptions

	// Raw data file for tx_hash_to_ledger_seq
	RawDataFile   *os.File
	RawDataWriter *bufio.Writer
}

// =============================================================================
// Store Initialization
// =============================================================================

// OpenStores opens all required RocksDB stores based on configuration.
func OpenStores(config *IngestionConfig) (*Stores, error) {
	stores := &Stores{}
	var err error

	// Open source store if configured
	if config.UseRocksDBSource {
		stores.SourceDB, stores.SourceOpts, stores.SourceRO, err = openRocksDBReader(config.RocksDBLcmStorePath)
		if err != nil {
			return nil, fmt.Errorf("failed to open RocksDB source store: %w", err)
		}
		log.Printf("✓ Opened RocksDB source store: %s", config.RocksDBLcmStorePath)
	}

	// Open ledger_seq_to_lcm store
	if config.EnableLedgerSeqToLcm {
		absPath, err := filepath.Abs(config.LedgerSeqToLcm.OutputPath)
		if err != nil {
			stores.Close()
			return nil, fmt.Errorf("failed to get absolute path for ledger_seq_to_lcm: %w", err)
		}

		if err := os.MkdirAll(absPath, 0755); err != nil {
			stores.Close()
			return nil, fmt.Errorf("failed to create directory for ledger_seq_to_lcm: %w", err)
		}

		stores.LcmDB, stores.LcmOpts, err = openRocksDBWithSettings(
			absPath,
			"ledger_seq_to_lcm",
			&config.LedgerSeqToLcm.RocksDB,
		)
		if err != nil {
			stores.Close()
			return nil, fmt.Errorf("failed to open ledger_seq_to_lcm store: %w", err)
		}
		log.Printf("✓ Opened ledger_seq_to_lcm store: %s", absPath)
	}

	// Open tx_hash_to_tx_data store
	if config.EnableTxHashToTxData {
		absPath, err := filepath.Abs(config.TxHashToTxData.OutputPath)
		if err != nil {
			stores.Close()
			return nil, fmt.Errorf("failed to get absolute path for tx_hash_to_tx_data: %w", err)
		}

		if err := os.MkdirAll(absPath, 0755); err != nil {
			stores.Close()
			return nil, fmt.Errorf("failed to create directory for tx_hash_to_tx_data: %w", err)
		}

		stores.TxDataDB, stores.TxDataOpts, err = openRocksDBWithSettings(
			absPath,
			"tx_hash_to_tx_data",
			&config.TxHashToTxData.RocksDB,
		)
		if err != nil {
			stores.Close()
			return nil, fmt.Errorf("failed to open tx_hash_to_tx_data store: %w", err)
		}
		log.Printf("✓ Opened tx_hash_to_tx_data store: %s", absPath)
	}

	// Open tx_hash_to_ledger_seq store
	if config.EnableTxHashToLedgerSeq {
		absPath, err := filepath.Abs(config.TxHashToLedgerSeq.OutputPath)
		if err != nil {
			stores.Close()
			return nil, fmt.Errorf("failed to get absolute path for tx_hash_to_ledger_seq: %w", err)
		}

		if err := os.MkdirAll(absPath, 0755); err != nil {
			stores.Close()
			return nil, fmt.Errorf("failed to create directory for tx_hash_to_ledger_seq: %w", err)
		}

		stores.HashSeqDB, stores.HashSeqOpts, err = openRocksDBWithSettings(
			absPath,
			"tx_hash_to_ledger_seq",
			&config.TxHashToLedgerSeq.RocksDB,
		)
		if err != nil {
			stores.Close()
			return nil, fmt.Errorf("failed to open tx_hash_to_ledger_seq store: %w", err)
		}
		log.Printf("✓ Opened tx_hash_to_ledger_seq store: %s", absPath)

		// Open raw data file if configured
		if config.TxHashToLedgerSeq.RawDataFilePath != "" {
			rawPath := config.TxHashToLedgerSeq.RawDataFilePath

			// Ensure parent directory exists
			if err := os.MkdirAll(filepath.Dir(rawPath), 0755); err != nil {
				stores.Close()
				return nil, fmt.Errorf("failed to create directory for raw data file: %w", err)
			}

			// Delete existing file if present (start fresh)
			if _, err := os.Stat(rawPath); err == nil {
				log.Printf("⚠️  Raw data file exists, deleting: %s", rawPath)
				if err := os.Remove(rawPath); err != nil {
					stores.Close()
					return nil, fmt.Errorf("failed to delete existing raw data file: %w", err)
				}
			}

			// Create new file
			stores.RawDataFile, err = os.Create(rawPath)
			if err != nil {
				stores.Close()
				return nil, fmt.Errorf("failed to create raw data file: %w", err)
			}

			// Create buffered writer
			stores.RawDataWriter = bufio.NewWriterSize(stores.RawDataFile, RawDataFileBufferSize)

			log.Printf("✓ Created raw data file: %s (buffer size: %d MB)",
				rawPath, RawDataFileBufferSize/MB)
		}
	}

	return stores, nil
}

// Close closes all open stores and files.
func (s *Stores) Close() {
	// Close raw data file first (flush buffer)
	if s.RawDataWriter != nil {
		if err := s.RawDataWriter.Flush(); err != nil {
			log.Printf("Warning: failed to flush raw data file: %v", err)
		}
	}
	if s.RawDataFile != nil {
		if err := s.RawDataFile.Close(); err != nil {
			log.Printf("Warning: failed to close raw data file: %v", err)
		}
	}

	// Close output stores
	if s.LcmDB != nil {
		s.LcmDB.Close()
	}
	if s.LcmOpts != nil {
		s.LcmOpts.Destroy()
	}

	if s.TxDataDB != nil {
		s.TxDataDB.Close()
	}
	if s.TxDataOpts != nil {
		s.TxDataOpts.Destroy()
	}

	if s.HashSeqDB != nil {
		s.HashSeqDB.Close()
	}
	if s.HashSeqOpts != nil {
		s.HashSeqOpts.Destroy()
	}

	// Close source store
	if s.SourceRO != nil {
		s.SourceRO.Destroy()
	}
	if s.SourceDB != nil {
		s.SourceDB.Close()
	}
	if s.SourceOpts != nil {
		s.SourceOpts.Destroy()
	}
}

// =============================================================================
// RocksDB Opening Functions
// =============================================================================

// openRocksDBWithSettings opens a RocksDB database with the specified settings.
// This is used for the output stores (writing).
func openRocksDBWithSettings(path, name string, settings *RocksDBSettings) (*grocksdb.DB, *grocksdb.Options, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	opts.SetErrorIfExists(false)

	// =========================================================================
	// Write Buffer (MemTable) Configuration
	// =========================================================================
	// Total memtable RAM = WriteBufferSizeMB × MaxWriteBufferNumber
	opts.SetWriteBufferSize(uint64(settings.WriteBufferSizeMB * MB))
	opts.SetMaxWriteBufferNumber(settings.MaxWriteBufferNumber)
	// Merge at least 2 memtables before flushing (reduces write amplification)
	opts.SetMinWriteBufferNumberToMerge(2)

	// =========================================================================
	// L0 Management
	// =========================================================================
	// For bulk ingestion, we set these very high to effectively disable
	// compaction during ingestion. All data accumulates in L0, and we do
	// one final compaction at the end.
	opts.SetLevel0FileNumCompactionTrigger(settings.L0CompactionTrigger)
	opts.SetLevel0SlowdownWritesTrigger(settings.L0SlowdownWritesTrigger)
	opts.SetLevel0StopWritesTrigger(settings.L0StopWritesTrigger)

	// =========================================================================
	// Compaction Configuration
	// =========================================================================
	// Disable auto-compaction for bulk ingestion
	opts.SetDisableAutoCompactions(true)

	// Use level-style compaction (default, most suitable for our use case)
	opts.SetCompactionStyle(grocksdb.LevelCompactionStyle)

	// Target file size at L0/L1
	opts.SetTargetFileSizeBase(uint64(settings.TargetFileSizeMB * MB))
	// Files at each subsequent level are 2x larger
	opts.SetTargetFileSizeMultiplier(2)

	// L1 max size (L2 = L1 × 10, L3 = L2 × 10, etc.)
	opts.SetMaxBytesForLevelBase(uint64(settings.MaxBytesForLevelBaseMB * MB))
	opts.SetMaxBytesForLevelMultiplier(10)

	// =========================================================================
	// Background Jobs
	// =========================================================================
	// Used primarily for final compaction
	opts.SetMaxBackgroundJobs(settings.MaxBackgroundJobs)

	// =========================================================================
	// Compression
	// =========================================================================
	// We do application-level zstd compression, so disable RocksDB compression
	// to avoid double-compression overhead
	opts.SetCompression(grocksdb.NoCompression)

	// =========================================================================
	// File Resources
	// =========================================================================
	opts.SetMaxOpenFiles(settings.MaxOpenFiles)

	// =========================================================================
	// Block-Based Table Options
	// =========================================================================
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()

	// No block cache for write-heavy workload (we're not reading during ingestion)
	// The block cache would just consume memory without benefit

	// Bloom filter for faster lookups (especially important for random keys)
	if settings.BloomFilterBitsPerKey > 0 {
		bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(settings.BloomFilterBitsPerKey)))
	}

	opts.SetBlockBasedTableFactory(bbto)

	// =========================================================================
	// WAL (Write-Ahead Log)
	// =========================================================================
	// Disable WAL for bulk ingestion - if we crash, we restart from scratch anyway
	// The source data (GCS or existing RocksDB) remains intact
	// Note: SetDisableWAL is set per-write operation, not globally

	// =========================================================================
	// Logging
	// =========================================================================
	opts.SetInfoLogLevel(grocksdb.WarnInfoLogLevel)
	opts.SetMaxLogFileSize(20 * MB)
	opts.SetKeepLogFileNum(3)

	// =========================================================================
	// Open Database
	// =========================================================================
	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		opts.Destroy()
		return nil, nil, errors.Wrapf(err, "failed to open RocksDB at %s", path)
	}

	// Log configuration summary
	log.Printf("")
	log.Printf("  [%s] RocksDB Configuration:", name)
	log.Printf("    Path:              %s", path)
	log.Printf("    Write Buffer:      %d MB × %d = %d MB total",
		settings.WriteBufferSizeMB, settings.MaxWriteBufferNumber,
		settings.WriteBufferSizeMB*settings.MaxWriteBufferNumber)
	log.Printf("    Target File Size:  %d MB", settings.TargetFileSizeMB)
	log.Printf("    L1 Max Size:       %d MB", settings.MaxBytesForLevelBaseMB)
	log.Printf("    L0 Triggers:       %d/%d/%d (compact/slow/stop)",
		settings.L0CompactionTrigger, settings.L0SlowdownWritesTrigger, settings.L0StopWritesTrigger)
	log.Printf("    Bloom Filter:      %d bits/key", settings.BloomFilterBitsPerKey)
	log.Printf("    Background Jobs:   %d", settings.MaxBackgroundJobs)
	log.Printf("")

	return db, opts, nil
}

// openRocksDBReader opens a RocksDB database in read-only mode.
// This is used for reading LCM data from an existing store.
func openRocksDBReader(path string) (*grocksdb.DB, *grocksdb.Options, *grocksdb.ReadOptions, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	// Block-based table options with block cache for reading
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	cache := grocksdb.NewLRUCache(uint64(BlockCacheSizeMB * MB))
	bbto.SetBlockCache(cache)
	opts.SetBlockBasedTableFactory(bbto)

	db, err := grocksdb.OpenDbForReadOnly(opts, path, false)
	if err != nil {
		opts.Destroy()
		return nil, nil, nil, errors.Wrapf(err, "failed to open RocksDB for reading at %s", path)
	}

	ro := grocksdb.NewDefaultReadOptions()

	return db, opts, ro, nil
}

// =============================================================================
// Batch Writing Functions
// =============================================================================

// WriteBatchTiming tracks timing for batch write operations.
type WriteBatchTiming struct {
	LcmWriteTime     time.Duration
	TxDataWriteTime  time.Duration
	HashSeqWriteTime time.Duration
	RawFileWriteTime time.Duration
}

// WriteBatch writes all batch data to the appropriate stores IN PARALLEL.
// Each store write and the raw file write run in separate goroutines.
// This significantly improves throughput when multiple stores are enabled.
func (s *Stores) WriteBatch(
	config *IngestionConfig,
	lcmData map[uint32][]byte, // ledgerSeq -> compressed LCM
	txData map[string][]byte, // txHash (hex) -> compressed TxData
	hashSeqData map[string]uint32, // txHash (hex) -> ledgerSeq
	rawFileData []byte, // Raw binary data for file (36 bytes per entry)
) (*WriteBatchTiming, error) {
	timing := &WriteBatchTiming{}

	// Use a wait group to wait for all parallel writes to complete
	var wg sync.WaitGroup

	// Use a mutex to safely collect errors from goroutines
	var errMu sync.Mutex
	var firstErr error

	// Helper to record the first error encountered
	recordError := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	// =========================================================================
	// Write to ledger_seq_to_lcm store (goroutine 1)
	// =========================================================================
	if config.EnableLedgerSeqToLcm && s.LcmDB != nil && len(lcmData) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			start := time.Now()

			// Each goroutine needs its own WriteOptions
			wo := grocksdb.NewDefaultWriteOptions()
			wo.DisableWAL(true)
			defer wo.Destroy()

			batch := grocksdb.NewWriteBatch()
			defer batch.Destroy()

			for ledgerSeq, compressedLcm := range lcmData {
				key := helpers.Uint32ToBytes(ledgerSeq)
				batch.Put(key, compressedLcm)
			}

			if err := s.LcmDB.Write(wo, batch); err != nil {
				recordError(errors.Wrap(err, "failed to write batch to ledger_seq_to_lcm"))
				return
			}

			timing.LcmWriteTime = time.Since(start)
		}()
	}

	// =========================================================================
	// Write to tx_hash_to_tx_data store (goroutine 2)
	// =========================================================================
	if config.EnableTxHashToTxData && s.TxDataDB != nil && len(txData) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			start := time.Now()

			// Each goroutine needs its own WriteOptions
			wo := grocksdb.NewDefaultWriteOptions()
			wo.DisableWAL(true)
			defer wo.Destroy()

			batch := grocksdb.NewWriteBatch()
			defer batch.Destroy()

			for txHashHex, compressedTxData := range txData {
				txHashBytes, err := helpers.HexStringToBytes(txHashHex)
				if err != nil {
					recordError(errors.Wrapf(err, "failed to convert tx hash: %s", txHashHex))
					return
				}
				batch.Put(txHashBytes, compressedTxData)
			}

			if err := s.TxDataDB.Write(wo, batch); err != nil {
				recordError(errors.Wrap(err, "failed to write batch to tx_hash_to_tx_data"))
				return
			}

			timing.TxDataWriteTime = time.Since(start)
		}()
	}

	// =========================================================================
	// Write to tx_hash_to_ledger_seq store (goroutine 3)
	// =========================================================================
	if config.EnableTxHashToLedgerSeq && s.HashSeqDB != nil && len(hashSeqData) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			start := time.Now()

			// Each goroutine needs its own WriteOptions
			wo := grocksdb.NewDefaultWriteOptions()
			wo.DisableWAL(true)
			defer wo.Destroy()

			batch := grocksdb.NewWriteBatch()
			defer batch.Destroy()

			for txHashHex, ledgerSeq := range hashSeqData {
				txHashBytes, err := helpers.HexStringToBytes(txHashHex)
				if err != nil {
					recordError(errors.Wrapf(err, "failed to convert tx hash: %s", txHashHex))
					return
				}
				value := helpers.Uint32ToBytes(ledgerSeq)
				batch.Put(txHashBytes, value)
			}

			if err := s.HashSeqDB.Write(wo, batch); err != nil {
				recordError(errors.Wrap(err, "failed to write batch to tx_hash_to_ledger_seq"))
				return
			}

			timing.HashSeqWriteTime = time.Since(start)
		}()
	}

	// =========================================================================
	// Write to raw data file (goroutine 4)
	// =========================================================================
	if s.RawDataWriter != nil && len(rawFileData) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			start := time.Now()

			if _, err := s.RawDataWriter.Write(rawFileData); err != nil {
				recordError(errors.Wrap(err, "failed to write to raw data file"))
				return
			}

			// Flush at end of each batch to ensure data is persisted
			if err := s.RawDataWriter.Flush(); err != nil {
				recordError(errors.Wrap(err, "failed to flush raw data file"))
				return
			}

			timing.RawFileWriteTime = time.Since(start)
		}()
	}

	// =========================================================================
	// Wait for all writes to complete
	// =========================================================================
	wg.Wait()

	// Return the first error if any occurred
	if firstErr != nil {
		return timing, firstErr
	}

	return timing, nil
}

// =============================================================================
// Compaction Functions
// =============================================================================

// CompactTiming tracks timing for compaction operations.
type CompactTiming struct {
	LcmCompactTime     time.Duration
	TxDataCompactTime  time.Duration
	HashSeqCompactTime time.Duration
}

// CompactAll performs final compaction on all stores.
// This should be called at the end of ingestion to consolidate all L0 files.
func (s *Stores) CompactAll(config *IngestionConfig, minLedger, maxLedger uint32) *CompactTiming {
	timing := &CompactTiming{}

	log.Printf("")
	log.Printf("================================================================================")
	log.Printf("                         PERFORMING FINAL COMPACTION")
	log.Printf("================================================================================")
	log.Printf("")

	// Compact ledger_seq_to_lcm store
	// For sequential keys, we can use a specific range
	if config.EnableLedgerSeqToLcm && s.LcmDB != nil {
		log.Printf("Compacting ledger_seq_to_lcm...")
		start := time.Now()

		startKey := helpers.Uint32ToBytes(minLedger)
		endKey := helpers.Uint32ToBytes(maxLedger + 1)
		s.LcmDB.CompactRange(grocksdb.Range{Start: startKey, Limit: endKey})

		timing.LcmCompactTime = time.Since(start)
		log.Printf("  ✓ ledger_seq_to_lcm compacted in %s", helpers.FormatDuration(timing.LcmCompactTime))
	}

	// Compact tx_hash_to_tx_data store
	// For random keys (hashes), we compact the entire range
	if config.EnableTxHashToTxData && s.TxDataDB != nil {
		log.Printf("Compacting tx_hash_to_tx_data...")
		start := time.Now()

		s.TxDataDB.CompactRange(grocksdb.Range{Start: nil, Limit: nil})

		timing.TxDataCompactTime = time.Since(start)
		log.Printf("  ✓ tx_hash_to_tx_data compacted in %s", helpers.FormatDuration(timing.TxDataCompactTime))
	}

	// Compact tx_hash_to_ledger_seq store
	if config.EnableTxHashToLedgerSeq && s.HashSeqDB != nil {
		log.Printf("Compacting tx_hash_to_ledger_seq...")
		start := time.Now()

		s.HashSeqDB.CompactRange(grocksdb.Range{Start: nil, Limit: nil})

		timing.HashSeqCompactTime = time.Since(start)
		log.Printf("  ✓ tx_hash_to_ledger_seq compacted in %s", helpers.FormatDuration(timing.HashSeqCompactTime))
	}

	log.Printf("")
	log.Printf("Final compaction complete.")
	log.Printf("")

	return timing
}

// =============================================================================
// Utility Functions
// =============================================================================

// GetDBSizes returns the current SST file sizes for all stores.
func (s *Stores) GetDBSizes() (lcmSize, txDataSize, hashSeqSize int64) {
	if s.LcmDB != nil {
		lcmSize = getRocksDBSSTSize(s.LcmDB)
	}
	if s.TxDataDB != nil {
		txDataSize = getRocksDBSSTSize(s.TxDataDB)
	}
	if s.HashSeqDB != nil {
		hashSeqSize = getRocksDBSSTSize(s.HashSeqDB)
	}
	return
}
