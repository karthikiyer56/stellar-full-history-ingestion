// processing.go
// =============================================================================
// Ledger and Transaction Processing for Stellar RocksDB Ingestion
// =============================================================================
//
// This module handles:
// - Fetching ledgers from RocksDB source (parallel) or GCS (sequential)
// - Extracting transactions from ledgers
// - Compressing LCM data
// - Compressing transaction data
// - Building raw data file entries
//
// PARALLELISM STRATEGY:
// =====================
// - RocksDB source: Ledgers are fetched in parallel across worker goroutines
// - GCS source: Ledgers are fetched sequentially (GCS backend handles buffering)
// - Transaction processing: Always parallelized regardless of source
//
// =============================================================================

package main

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/klauspost/compress/zstd"
	"github.com/linxGnu/grocksdb"
	"github.com/pkg/errors"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// Data Structures
// =============================================================================

// LedgerFetchResult holds the result of fetching a single ledger.
type LedgerFetchResult struct {
	LedgerSeq uint32
	Ledger    xdr.LedgerCloseMeta
	Timing    LedgerFetchTiming
	Err       error
}

// LedgerFetchTiming tracks timing for fetching a single ledger from RocksDB.
type LedgerFetchTiming struct {
	ReadTime       time.Duration
	DecompressTime time.Duration
	UnmarshalTime  time.Duration
	TotalTime      time.Duration
}

// RawTxData holds raw transaction data before processing.
type RawTxData struct {
	Tx        ingest.LedgerTransaction
	LedgerSeq uint32
	ClosedAt  time.Time
}

// TxProcessResult holds the result of processing a single transaction.
type TxProcessResult struct {
	Hash             string
	CompressedData   []byte
	UncompressedSize int64
	CompressedSize   int64
	LedgerSeq        uint32
	Err              error
}

// LcmProcessResult holds the result of processing a single LCM.
type LcmProcessResult struct {
	LedgerSeq        uint32
	CompressedData   []byte
	UncompressedSize int64
	CompressedSize   int64
	Err              error
}

// LcmWithSeq holds an LCM and its sequence number.
type LcmWithSeq struct {
	LedgerSeq uint32
	Lcm       xdr.LedgerCloseMeta
}

// =============================================================================
// Ledger Fetching from RocksDB Source (Parallel)
// =============================================================================

// FetchLedgersFromRocksDB fetches ledgers in parallel from a RocksDB source.
// This is used when rocksdb_lcm_store_path is configured.
func FetchLedgersFromRocksDB(
	db *grocksdb.DB,
	ro *grocksdb.ReadOptions,
	startSeq, endSeq uint32,
	numWorkers int,
) []LedgerFetchResult {
	numLedgers := int(endSeq - startSeq + 1)
	results := make([]LedgerFetchResult, numLedgers)

	// Create job channel
	jobs := make(chan uint32, numLedgers)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Each worker gets its own zstd decoder
			decoder, err := zstd.NewReader(nil)
			if err != nil {
				// This shouldn't happen, but handle it gracefully
				for ledgerSeq := range jobs {
					idx := int(ledgerSeq - startSeq)
					results[idx] = LedgerFetchResult{
						LedgerSeq: ledgerSeq,
						Err:       errors.Wrap(err, "failed to create zstd decoder"),
					}
				}
				return
			}
			defer decoder.Close()

			for ledgerSeq := range jobs {
				idx := int(ledgerSeq - startSeq)
				results[idx] = fetchSingleLedgerFromRocksDB(db, ro, decoder, ledgerSeq)
			}
		}()
	}

	// Send jobs
	for seq := startSeq; seq <= endSeq; seq++ {
		jobs <- seq
	}
	close(jobs)

	wg.Wait()
	return results
}

// fetchSingleLedgerFromRocksDB fetches and decompresses a single ledger from RocksDB.
func fetchSingleLedgerFromRocksDB(
	db *grocksdb.DB,
	ro *grocksdb.ReadOptions,
	decoder *zstd.Decoder,
	ledgerSeq uint32,
) LedgerFetchResult {
	result := LedgerFetchResult{LedgerSeq: ledgerSeq}
	totalStart := time.Now()

	// Read from RocksDB
	key := helpers.Uint32ToBytes(ledgerSeq)

	readStart := time.Now()
	slice, err := db.Get(ro, key)
	result.Timing.ReadTime = time.Since(readStart)

	if err != nil {
		result.Timing.TotalTime = time.Since(totalStart)
		result.Err = errors.Wrap(err, "failed to read from RocksDB")
		return result
	}
	defer slice.Free()

	if !slice.Exists() {
		result.Timing.TotalTime = time.Since(totalStart)
		result.Err = fmt.Errorf("ledger %d not found in RocksDB source", ledgerSeq)
		return result
	}

	// Copy data (slice data is only valid until Free())
	compressedData := make([]byte, len(slice.Data()))
	copy(compressedData, slice.Data())

	// Decompress
	decompressStart := time.Now()
	uncompressedData, err := decoder.DecodeAll(compressedData, nil)
	result.Timing.DecompressTime = time.Since(decompressStart)

	if err != nil {
		result.Timing.TotalTime = time.Since(totalStart)
		result.Err = errors.Wrap(err, "failed to decompress ledger data")
		return result
	}

	// Unmarshal XDR
	unmarshalStart := time.Now()
	err = result.Ledger.UnmarshalBinary(uncompressedData)
	result.Timing.UnmarshalTime = time.Since(unmarshalStart)

	if err != nil {
		result.Timing.TotalTime = time.Since(totalStart)
		result.Err = errors.Wrap(err, "failed to unmarshal LedgerCloseMeta")
		return result
	}

	result.Timing.TotalTime = time.Since(totalStart)
	return result
}

// =============================================================================
// Transaction Extraction
// =============================================================================

// ExtractTransactions extracts all transactions from a ledger.
// It returns the transactions and the count.
func ExtractTransactions(ledger xdr.LedgerCloseMeta) ([]RawTxData, error) {
	var transactions []RawTxData

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		network.PublicNetworkPassphrase, ledger)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create tx reader for ledger %d", ledger.LedgerSequence())
	}
	defer txReader.Close()

	closedAt := ledger.ClosedAt()
	ledgerSeq := ledger.LedgerSequence()

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrapf(err, "error reading transaction in ledger %d", ledgerSeq)
		}

		transactions = append(transactions, RawTxData{
			Tx:        tx,
			LedgerSeq: ledgerSeq,
			ClosedAt:  closedAt,
		})
	}

	return transactions, nil
}

// CountTransactions counts transactions in a ledger without extracting full data.
// This is used when only ledger_seq_to_lcm is enabled and we need tx count for stats.
func CountTransactions(ledger xdr.LedgerCloseMeta) (int, error) {
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		network.PublicNetworkPassphrase, ledger)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create tx reader for ledger %d", ledger.LedgerSequence())
	}
	defer txReader.Close()

	count := 0
	for {
		_, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, errors.Wrapf(err, "error reading transaction in ledger %d", ledger.LedgerSequence())
		}
		count++
	}

	return count, nil
}

// =============================================================================
// LCM Compression (Parallel)
// =============================================================================

// LcmCompressionResult holds the aggregated results of compressing LCMs.
type LcmCompressionResult struct {
	CompressedData   map[uint32][]byte // ledgerSeq -> compressed data
	UncompressedSize int64
	CompressedSize   int64
	EntryCount       int64
}

// CompressLCMsInParallel compresses multiple LCMs in parallel.
func CompressLCMsInParallel(lcms []LcmWithSeq, numWorkers int) (*LcmCompressionResult, error) {
	if len(lcms) == 0 {
		return &LcmCompressionResult{
			CompressedData: make(map[uint32][]byte),
		}, nil
	}

	results := make([]LcmProcessResult, len(lcms))
	jobs := make(chan int, len(lcms))

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Each worker gets its own zstd encoder
			encoder, err := zstd.NewWriter(nil)
			if err != nil {
				// Handle error for all remaining jobs
				for idx := range jobs {
					results[idx] = LcmProcessResult{
						LedgerSeq: lcms[idx].LedgerSeq,
						Err:       errors.Wrap(err, "failed to create zstd encoder"),
					}
				}
				return
			}
			defer encoder.Close()

			for idx := range jobs {
				item := lcms[idx]
				result := LcmProcessResult{LedgerSeq: item.LedgerSeq}

				// Marshal LCM to binary
				lcmBytes, err := item.Lcm.MarshalBinary()
				if err != nil {
					result.Err = errors.Wrapf(err, "failed to marshal LCM for ledger %d", item.LedgerSeq)
					results[idx] = result
					continue
				}

				result.UncompressedSize = int64(len(lcmBytes))

				// Compress with zstd
				result.CompressedData = encoder.EncodeAll(lcmBytes, make([]byte, 0, len(lcmBytes)))
				result.CompressedSize = int64(len(result.CompressedData))

				results[idx] = result
			}
		}()
	}

	// Send jobs
	for i := range lcms {
		jobs <- i
	}
	close(jobs)

	wg.Wait()

	// Aggregate results
	aggregated := &LcmCompressionResult{
		CompressedData: make(map[uint32][]byte, len(lcms)),
	}

	for _, result := range results {
		if result.Err != nil {
			return nil, result.Err
		}
		aggregated.CompressedData[result.LedgerSeq] = result.CompressedData
		aggregated.UncompressedSize += result.UncompressedSize
		aggregated.CompressedSize += result.CompressedSize
		aggregated.EntryCount++
	}

	return aggregated, nil
}

// =============================================================================
// Transaction Compression (Parallel)
// =============================================================================

// TxCompressionResult holds the aggregated results of compressing transactions.
type TxCompressionResult struct {
	// For tx_hash_to_ledger_seq store
	TxHashToLedgerSeq map[string]uint32 // txHash (hex) -> ledgerSeq

	// For raw data file (binary: 32-byte hash + 4-byte seq)
	RawFileData []byte
}

// CompressTransactionsInParallel compresses transactions and builds mappings in parallel.
// It handles tx_hash_to_ledger_seq store.
func CompressTransactionsInParallel(
	transactions []RawTxData,
	numWorkers int,
	enableHashSeq bool, // Build tx_hash_to_ledger_seq mapping
	enableRawFile bool, // Build raw file data
) (*TxCompressionResult, error) {
	if len(transactions) == 0 {
		return &TxCompressionResult{
			TxHashToLedgerSeq: make(map[string]uint32),
		}, nil
	}

	// Results slice for parallel processing
	type workerResult struct {
		Hash        string
		LedgerSeq   uint32
		TxHashBytes []byte // 32-byte hash
		Err         error
	}

	results := make([]workerResult, len(transactions))
	jobs := make(chan int, len(transactions))

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for idx := range jobs {
				tx := transactions[idx]
				result := workerResult{
					Hash:      tx.Tx.Hash.HexString(),
					LedgerSeq: tx.LedgerSeq,
				}

				// Get hash bytes for raw file
				if enableRawFile || enableHashSeq {
					result.TxHashBytes = tx.Tx.Hash[:]
				}

				results[idx] = result
			}
		}()
	}

	// Send jobs
	for i := range transactions {
		jobs <- i
	}
	close(jobs)

	wg.Wait()

	// Aggregate results
	aggregated := &TxCompressionResult{
		TxHashToLedgerSeq: make(map[string]uint32, len(transactions)),
	}

	// Pre-allocate raw file data buffer if needed
	if enableRawFile {
		aggregated.RawFileData = make([]byte, 0, len(transactions)*36)
	}

	for _, result := range results {
		if result.Err != nil {
			return nil, result.Err
		}

		if enableHashSeq {
			aggregated.TxHashToLedgerSeq[result.Hash] = result.LedgerSeq
		}

		if enableRawFile {
			// Append 32-byte hash + 4-byte ledger sequence (big-endian)
			aggregated.RawFileData = append(aggregated.RawFileData, result.TxHashBytes...)
			aggregated.RawFileData = append(aggregated.RawFileData, helpers.Uint32ToBytes(result.LedgerSeq)...)
		}
	}

	return aggregated, nil
}

// =============================================================================
// Batch Processing Helper
// =============================================================================

// BatchProcessingResult holds all data needed to write a batch to stores.
type BatchProcessingResult struct {
	// For ledger_seq_to_lcm
	LcmData         map[uint32][]byte
	LcmUncompressed int64
	LcmCompressed   int64
	LcmCount        int64

	// For tx_hash_to_ledger_seq
	HashSeqMap   map[string]uint32
	HashSeqCount int64

	// For raw data file
	RawFileData []byte

	// Timing
	LcmCompressionTime time.Duration
}

// ProcessBatch processes all ledgers in a batch and prepares data for writing.
func ProcessBatch(
	config *IngestionConfig,
	lcms []LcmWithSeq,
	transactions []RawTxData,
	numWorkers int,
) (*BatchProcessingResult, error) {
	result := &BatchProcessingResult{
		LcmData:    make(map[uint32][]byte),
		HashSeqMap: make(map[string]uint32),
	}

	// Process LCMs if ledger_seq_to_lcm is enabled
	if config.EnableLedgerSeqToLcm && len(lcms) > 0 {
		start := time.Now()
		lcmResult, err := CompressLCMsInParallel(lcms, numWorkers)
		if err != nil {
			return nil, errors.Wrap(err, "failed to compress LCMs")
		}
		result.LcmCompressionTime = time.Since(start)

		result.LcmData = lcmResult.CompressedData
		result.LcmUncompressed = lcmResult.UncompressedSize
		result.LcmCompressed = lcmResult.CompressedSize
		result.LcmCount = lcmResult.EntryCount
	}

	// Process transactions if any tx store is enabled
	if config.ProcessTransactions && len(transactions) > 0 {
		enableRawFile := config.TxHashToLedgerSeq.RawDataFilePath != ""

		txResult, err := CompressTransactionsInParallel(
			transactions,
			numWorkers,
			config.EnableTxHashToLedgerSeq,
			enableRawFile,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to compress transactions")
		}

		result.HashSeqMap = txResult.TxHashToLedgerSeq
		result.HashSeqCount = int64(len(txResult.TxHashToLedgerSeq))

		result.RawFileData = txResult.RawFileData
	}

	return result, nil
}
