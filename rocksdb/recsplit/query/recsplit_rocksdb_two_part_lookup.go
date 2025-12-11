package main

// =============================================================================
// recsplit_rocksdb_two_part_lookup.go
// =============================================================================
//
// PURPOSE:
//   Query Stellar transactions using a two-part lookup:
//     Part 1: RecSplit index lookup (txHash -> ledgerSeq)
//     Part 2: RocksDB lookup (ledgerSeq -> compressed LedgerCloseMeta)
//
// RECSPLIT FALSE-POSITIVE HANDLING:
//   RecSplit is a Minimal Perfect Hash Function (MPHF). By design, it maps ANY
//   input to some output index - even keys that were never added to the index.
//   This means for non-existent txHashes:
//
//   WITH false-positive filter:
//     - Uses a fuse filter (~9 bits/key overhead) to detect non-existent keys
//     - Lookup returns (0, false) for keys that definitely don't exist
//     - ~0.4% false positive rate (1/256) - some non-existent keys still pass
//     - This is tracked separately from ledger-scan false positives
//
// USAGE:
//   Single query:
//     ./recsplit_rocksdb_two_part_lookup \
//       --recsplit-index-file /path/to/txhash.idx \
//       --ledger-seq-to-lcm-db /path/to/rocksdb \
//       --tx abc123def456...
//
//   Ongoing stdin mode:
//     ./recsplit_rocksdb_two_part_lookup \
//       --recsplit-index-file /path/to/txhash.idx \
//       --ledger-seq-to-lcm-db /path/to/rocksdb \
//       --ongoing
//
//   Batch mode (reads from stdin, outputs stats, then exits):
//     cat tx_hashes.txt | ./recsplit_rocksdb_two_part_lookup \
//       --recsplit-index-file /path/to/txhash.idx \
//       --ledger-seq-to-lcm-db /path/to/rocksdb \
//       --ongoing --quiet
//
// INTERACTIVE COMMANDS:
//   STATS - Print cumulative statistics
//   STOP  - Print final statistics and exit
//
// =============================================================================

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/erigontech/erigon/db/recsplit"
	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/klauspost/compress/zstd"
	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
)

// These structures track detailed timing for each phase of the lookup process.
// This is crucial for performance analysis and identifying bottlenecks.

// OpenTiming tracks the time spent opening/initializing all resources
type OpenTiming struct {
	// RecSplit index loading time (includes mmap setup)
	RecSplitLoad time.Duration

	// RocksDB opening times broken down by phase
	RocksDBOptsCreate time.Duration // Time to create RocksDB options
	RocksDBOpen       time.Duration // Time to actually open the database
	RocksDBROCreate   time.Duration // Time to create read options
	RocksDBTotal      time.Duration // Sum of above

	// Zstd decoder creation time
	DecoderCreate time.Duration

	// Total time from start to ready
	GrandTotal time.Duration
}

type QueryPrintOption func(*queryPrintConfig)

type queryPrintConfig struct {
	scale int
}

func WithScale(n int) QueryPrintOption {
	return func(cfg *queryPrintConfig) {
		if n > 0 {
			cfg.scale = n
		}
	}
}

// QueryTiming tracks the time for each step of a single query
type QueryTiming struct {
	RecSplitLookup time.Duration // Part 1: RecSplit lookup (txHash -> ledgerSeq)
	RocksDBQuery   time.Duration // Part 2: RocksDB lookup (ledgerSeq -> compressed LCM)
	Decompress     time.Duration // Decompression of LedgerCloseMeta using zstd
	Unmarshal      time.Duration // XDR unmarshaling
	TxReaderCreate time.Duration // Transaction reader creation
	TxSearch       time.Duration // Time to find matching hash
	TxCount        int           // Number of transactions scanned
	TxExtract      time.Duration // Transaction data extraction
	Total          time.Duration // Total end-to-end query time
}

// QueryResult holds all the data we extract for a successful transaction lookup
type QueryResult struct {
	TxHash    string
	LedgerSeq uint32
	TxIndex   uint32
	ClosedAt  time.Time

	// Raw transaction data in XDR binary format
	EnvelopeBytes []byte
	ResultBytes   []byte
	MetaBytes     []byte

	// Size metrics for analysis
	CompressedLCMSize   int // Size of compressed LedgerCloseMeta from RocksDB
	UncompressedLCMSize int
}

// These track "cumulative" statistics across multiple queries in ongoing mode.
// We separate "found" queries from various "not found" scenarios.

type QueryStats struct {
	Count  int
	Timing QueryTiming
}

type RunningStats struct {
	SuccessfulQueryStats    QueryStats
	NotFoundInRecSplitStats QueryStats
	NotFoundInRocksDbStats  QueryStats
	FalsePositiveStats      QueryStats
}

// Error reporting
type ErrInvalidHex struct {
	Reason error
}

func (e ErrInvalidHex) Error() string {
	return fmt.Sprintf("invalid hex string: %v", e.Reason)
}

func (e ErrInvalidHex) Unwrap() error {
	return e.Reason
}

type ErrInvalidHashLength struct {
	Got int
}

func (e ErrInvalidHashLength) Error() string {
	return fmt.Sprintf("invalid hash length: expected 32 bytes, got %d", e.Got)
}

type ErrRecSplitNotFound struct{}

func (e ErrRecSplitNotFound) Error() string {
	return "not found in RecSplit filter"
}

type ErrLedgerNotFound struct {
	Ledger uint32
}

func (e ErrLedgerNotFound) Error() string {
	return fmt.Sprintf("ledger %d not found in RocksDB", e.Ledger)
}

type ErrRecSplitFalsePositive struct {
	Ledger  uint32
	TxHash  string
	Scanned int
}

func (e ErrRecSplitFalsePositive) Error() string {
	return fmt.Sprintf("txHash not found in ledger %d (scanned %d txs)", e.Ledger, e.Scanned)
}

type config struct {
	inputfile string
	quiet     bool
	ongoing   bool
}

func main() {
	var (
		// Required: Path to the RecSplit index file (.idx)
		recsplitIndexFile string

		// Required: Path to RocksDB database (ledgerSeq -> compressed LCM)
		ledgerSeqToLCMDB string

		// For single query mode: the transaction hash to look up
		txHashHex string

		// Mode flags
		ongoing bool // Read txHashes from stdin continuously
		quiet   bool // Minimal output

		// RocksDB tuning
		blockCacheSizeMB int // Size of block cache in MB
	)

	flag.StringVar(&recsplitIndexFile, "recsplit-index-file", "",
		"Path to RecSplit index file (.idx) mapping txHash -> ledgerSeq")
	flag.StringVar(&ledgerSeqToLCMDB, "ledger-seq-to-lcm-db", "",
		"Path to RocksDB database mapping ledgerSeq -> compressed LedgerCloseMeta")
	flag.StringVar(&txHashHex, "tx", "",
		"Transaction hash in hex (for single query mode)")
	flag.BoolVar(&ongoing, "ongoing", false,
		"Read transaction hashes from stdin continuously (batch/interactive mode)")
	flag.BoolVar(&quiet, "quiet", false,
		"Minimal output - only timing metrics")
	flag.IntVar(&blockCacheSizeMB, "block-cache-mb", 512,
		"RocksDB block cache size in MB")

	flag.Parse()

	if recsplitIndexFile == "" || ledgerSeqToLCMDB == "" {
		log.Fatalf("Both --recsplit-index-file and --ledger-seq-to-lcm-db are required")
	}
	if ongoing && txHashHex != "" {
		log.Fatalf("--tx cannot be used with --ongoing mode")
	}
	if !ongoing && txHashHex == "" {
		log.Fatalf("Provide either --tx <hash> for single query OR --ongoing for batch mode")
	}

	if _, err := os.Stat(recsplitIndexFile); os.IsNotExist(err) {
		log.Fatalf("RecSplit index file does not exist: %s", recsplitIndexFile)
	}
	if _, err := os.Stat(ledgerSeqToLCMDB); os.IsNotExist(err) {
		log.Fatalf("RocksDB path does not exist: %s", ledgerSeqToLCMDB)
	}

	// Open resources with timing
	openTiming := OpenTiming{}
	grandStart := time.Now()

	// Open RecSplit Index
	// -------------------------------------------------------------------------
	// RecSplit uses mmap internally, so "opening" the index is very fast.
	// The actual data is loaded on-demand from disk via page faults.
	t1 := time.Now()
	rsIndex, err := recsplit.OpenIndex(recsplitIndexFile)
	if err != nil {
		log.Fatalf("Failed to open RecSplit index: %v", err)
	}
	defer rsIndex.Close()
	openTiming.RecSplitLoad = time.Since(t1)

	// Create an IndexReader for thread-safe lookups
	// The reader wraps the index and provides the Lookup method
	rsReader := recsplit.NewIndexReader(rsIndex)
	defer rsReader.Close()

	// Open RocksDB
	// -------------------------------------------------------------------------
	// RocksDB stores ledgerSeq -> compressed LedgerCloseMeta
	// We configure it with:
	//   - Block cache: LRU cache for frequently accessed blocks
	//   - Bloom filter: Reduces disk reads for non-existent keys
	//   - Read-only mode: We're only querying, not writing

	t2 := time.Now()
	rocksOpts := grocksdb.NewDefaultOptions()
	rocksOpts.SetCreateIfMissing(false)
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	if blockCacheSizeMB > 0 {
		// LRU cache for frequently accessed data blocks
		cache := grocksdb.NewLRUCache(uint64(blockCacheSizeMB) * 1024 * 1024)
		bbto.SetBlockCache(cache)
	}
	// Bloom filter reduces disk reads for keys that don't exist
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10))
	// Cache index and filter blocks in block cache (improves memory efficiency)
	bbto.SetCacheIndexAndFilterBlocks(true)
	rocksOpts.SetBlockBasedTableFactory(bbto)
	openTiming.RocksDBOptsCreate = time.Since(t2)

	t3 := time.Now()
	// Open in read-only mode (second param false = don't error if no write lock)
	rocksDB, err := grocksdb.OpenDbForReadOnly(rocksOpts, ledgerSeqToLCMDB, false)
	if err != nil {
		log.Fatalf("Failed to open RocksDB: %v", err)
	}
	defer rocksDB.Close()
	defer rocksOpts.Destroy()
	openTiming.RocksDBOpen = time.Since(t3)

	t4 := time.Now()
	rocksRO := grocksdb.NewDefaultReadOptions()
	defer rocksRO.Destroy()
	openTiming.RocksDBROCreate = time.Since(t4)
	openTiming.RocksDBTotal = openTiming.RocksDBOptsCreate + openTiming.RocksDBOpen + openTiming.RocksDBROCreate

	// Create Zstd Decoder
	// -------------------------------------------------------------------------
	// LedgerCloseMeta data is compressed with zstd for storage efficiency.
	// We create a reusable decoder to avoid allocation overhead per query.
	t5 := time.Now()
	zstdDecoder, err := zstd.NewReader(nil)
	if err != nil {
		log.Fatalf("Failed to create zstd decoder: %v", err)
	}
	defer zstdDecoder.Close()
	openTiming.DecoderCreate = time.Since(t5)
	openTiming.GrandTotal = time.Since(grandStart)

	// Print initialization summary
	printInitSummary(recsplitIndexFile, ledgerSeqToLCMDB, rsIndex, rocksDB, openTiming, blockCacheSizeMB)

	if ongoing {
		stats := RunningStats{}
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Println("Ongoing mode: enter tx hashes (one per line)")
		fmt.Println("Commands: STATS (show statistics), STOP (exit)")
		fmt.Println()

		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}

			// Handle special commands
			if strings.EqualFold(line, "STOP") {
				printFinalStats(stats)
				fmt.Println("Stopping.")
				return
			}
			if strings.EqualFold(line, "STATS") {
				printFinalStats(stats)
				continue
			}

			fmt.Println(line)
			timing, result, queryErr := runQuery(rsReader, rocksDB, rocksRO, zstdDecoder, line)
			updateStats(&stats, timing, queryErr)

			if queryErr != nil {
				printNotFoundResult(timing, queryErr, quiet)
			} else {
				printQueryFoundResult(timing, result, quiet)
			}
		}
		printFinalStats(stats)
		return
	}

	// Single query mode
	fmt.Println(txHashHex)
	timing, result, err := runQuery(rsReader, rocksDB, rocksRO, zstdDecoder, txHashHex)
	if err != nil {
		printNotFoundResult(timing, err, quiet)
		os.Exit(1)
	}
	printQueryFoundResult(timing, result, quiet)
}

// CORE QUERY FUNCTION

// runQuery executes the two-part lookup for a transaction hash.
//
// The lookup process:
//  1. Parse the hex-encoded transaction hash
//  2. Query RecSplit index: txHash -> ledgerSeq (with optional false-positive check)
//  3. Query RocksDB: ledgerSeq -> compressed LedgerCloseMeta
//  4. Decompress the LedgerCloseMeta using zstd
//  5. Unmarshal the XDR binary into Go structs
//  6. Create a transaction reader and search for the matching txHash
//  7. Extract and marshal the transaction data
//
// Error types returned:
//   - "not found in RecSplit filter" - TxHash not found in recsplit index. Quickest rejection
//   - "ledger X not found in RocksDB" - index/database inconsistency. This shudnt happen when you run this code.
//   - "txHash not found in ledger X" - This was a false positive from recsplit index that required lookup in rocksdb store.
func runQuery(
	rsReader *recsplit.IndexReader,
	rocksDB *grocksdb.DB,
	rocksRO *grocksdb.ReadOptions,
	zstdDecoder *zstd.Decoder,
	txHashHex string,
) (QueryTiming, *QueryResult, error) {
	timing := QueryTiming{}
	totalStart := time.Now()

	// Step 1: Parse the transaction hash
	// -------------------------------------------------------------------------
	// Transaction hashes are 32 bytes (256 bits), represented as 64 hex characters
	txHashBytes, err := hex.DecodeString(txHashHex)
	if err != nil {
		timing.Total = time.Since(totalStart)
		return timing, nil, ErrInvalidHex{Reason: err}
	}
	if len(txHashBytes) != 32 {
		timing.Total = time.Since(totalStart)
		return timing, nil, ErrInvalidHashLength{Got: len(txHashBytes)}
	}

	// Step 2: RecSplit Lookup (txHash -> ledgerSeq)
	// -------------------------------------------------------------------------
	// RecSplit provides O(1) lookup with ~2 bits per key overhead.
	//
	// The Lookup method returns (value, found):
	//   - With false-positive filter: found=false means key definitely doesn't exist
	//   - Without filter: found is always true, but value may be garbage for non-existent keys

	//  NOTE::::: WE ALWAYS RUN QUERY ON FILES THAT ARE BUILD WITH FALSE POSITIVE SUPPORT
	// The value is the ledger sequence number where this transaction was included.
	t1 := time.Now()
	ledgerSeqU64, found := rsReader.Lookup(txHashBytes)
	timing.RecSplitLookup = time.Since(t1)

	// This is a clean rejection - the key was never added to the index
	if !found {
		timing.Total = time.Since(totalStart)
		return timing, nil, ErrRecSplitNotFound{}
	}
	ledgerSeq := uint32(ledgerSeqU64)

	// Step 3: RocksDB Lookup (ledgerSeq -> compressed LCM)
	// -------------------------------------------------------------------------
	// The key is a 4-byte big-endian encoding of the ledger sequence number.
	// The value is the zstd-compressed LedgerCloseMeta in XDR format.
	ledgerKey := make([]byte, 4)
	binary.BigEndian.PutUint32(ledgerKey, ledgerSeq)

	t2 := time.Now()
	slice, err := rocksDB.Get(rocksRO, ledgerKey)
	timing.RocksDBQuery = time.Since(t2)

	if err != nil {
		timing.Total = time.Since(totalStart)
		return timing, nil, fmt.Errorf("RocksDB query error: %v", err)
	}
	defer slice.Free()

	if !slice.Exists() {
		timing.Total = time.Since(totalStart)
		return timing, nil, ErrLedgerNotFound{Ledger: ledgerSeq}
	}

	// Copy the compressed data (slice is only valid until Free() is called)
	compressedLCM := make([]byte, len(slice.Data()))
	copy(compressedLCM, slice.Data())

	// Step 4: Decompress LedgerCloseMeta
	// -------------------------------------------------------------------------
	// LedgerCloseMeta contains all the information about a closed ledger,
	// including all transactions and their results.
	// Compression ratio is typically 3-5x, so this is a significant step.
	t3 := time.Now()
	uncompressedLCM, err := zstdDecoder.DecodeAll(compressedLCM, nil)
	timing.Decompress = time.Since(t3)
	if err != nil {
		timing.Total = time.Since(totalStart)
		return timing, nil, fmt.Errorf("zstd decompression failed: %v", err)
	}

	// Step 5: Unmarshal XDR to LedgerCloseMeta
	t4 := time.Now()
	var lcm xdr.LedgerCloseMeta
	err = lcm.UnmarshalBinary(uncompressedLCM)
	timing.Unmarshal = time.Since(t4)
	if err != nil {
		timing.Total = time.Since(totalStart)
		return timing, nil, fmt.Errorf("XDR unmarshal failed: %v", err)
	}

	// Step 6: Search for Transaction in Ledger
	t5 := time.Now()
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, lcm)
	timing.TxReaderCreate = time.Since(t5)
	if err != nil {
		timing.Total = time.Since(totalStart)
		return timing, nil, fmt.Errorf("failed to create transaction reader: %v", err)
	}
	defer txReader.Close()

	t6 := time.Now()
	var foundTx *ingest.LedgerTransaction
	txCount := 0
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			timing.Total = time.Since(totalStart)
			return timing, nil, fmt.Errorf("error reading transaction: %v", err)
		}
		txCount++
		if tx.Hash.HexString() == strings.ToLower(txHashHex) {
			foundTx = &tx
			break
		}
	}
	timing.TxSearch = time.Since(t6)
	timing.TxCount = txCount

	// If we didn't find the transaction, this is a RecSplit false positive
	// The key was never in the index, but RecSplit mapped it to this ledger anyway
	// This can happen even when recsplit is built with false positive supprt, like in our case with a 0.4% chance
	if foundTx == nil {
		timing.Total = time.Since(totalStart)
		return timing, nil, ErrRecSplitFalsePositive{
			Ledger:  ledgerSeq,
			TxHash:  txHashHex,
			Scanned: txCount,
		}
	}

	// Step 7: Extract Transaction Data
	t7 := time.Now()
	envelopeBytes, _ := foundTx.Envelope.MarshalBinary()
	resultBytes, _ := foundTx.Result.MarshalBinary()
	metaBytes, _ := foundTx.UnsafeMeta.MarshalBinary()
	timing.TxExtract = time.Since(t7)
	timing.Total = time.Since(totalStart)

	return timing, &QueryResult{
		TxHash:              txHashHex,
		LedgerSeq:           ledgerSeq,
		TxIndex:             foundTx.Index,
		ClosedAt:            lcm.ClosedAt(),
		EnvelopeBytes:       envelopeBytes,
		ResultBytes:         resultBytes,
		MetaBytes:           metaBytes,
		CompressedLCMSize:   len(compressedLCM),
		UncompressedLCMSize: len(uncompressedLCM),
	}, nil
}

// STATISTICS UPDATE

// updateStats updates the running statistics based on the query result.
// It categorizes the query into one of four buckets:
//  1. Successful query (txHash found)
//  2. Not found in RecSplit filter (tx does not exist in recsplit, clean exit)
//  3. Not found in RocksDB (data inconsistency)
//  4. False positive from RecSplit (txHash not in ledger)
func updateStats(stats *RunningStats, timing QueryTiming, err error) {
	if err == nil {
		st := stats.SuccessfulQueryStats
		st.Count++ // count of successful queries so far
		st.Timing.RecSplitLookup += timing.RecSplitLookup
		st.Timing.RocksDBQuery += timing.RocksDBQuery
		st.Timing.Decompress += timing.Decompress
		st.Timing.Unmarshal += timing.Unmarshal
		st.Timing.TxReaderCreate += timing.TxReaderCreate
		st.Timing.TxSearch += timing.TxSearch
		st.Timing.TxExtract += timing.TxExtract
		st.Timing.TxCount += timing.TxCount // transaction scanned so far as part of successful queries
		st.Timing.Total += timing.Total
		return
	}

	var (
		eRSNF ErrRecSplitNotFound
		eLNF  ErrLedgerNotFound
		eFPos ErrRecSplitFalsePositive
	)

	switch {
	case errors.As(err, &eRSNF):
		st := stats.NotFoundInRecSplitStats
		st.Count++
		st.Timing.RecSplitLookup += timing.RecSplitLookup
		st.Timing.Total += timing.Total

	case errors.As(err, &eLNF):
		st := stats.NotFoundInRocksDbStats
		st.Count++
		st.Timing.RecSplitLookup += timing.RecSplitLookup
		st.Timing.RocksDBQuery += timing.RocksDBQuery
		st.Timing.Total += timing.Total

	case errors.As(err, &eFPos):
		st := stats.FalsePositiveStats
		st.Count++
		st.Timing.RecSplitLookup += timing.RecSplitLookup
		st.Timing.RocksDBQuery += timing.RocksDBQuery
		st.Timing.Decompress += timing.Decompress
		st.Timing.Unmarshal += timing.Unmarshal
		st.Timing.TxReaderCreate += timing.TxReaderCreate
		st.Timing.TxSearch += timing.TxSearch
		st.Timing.TxExtract += timing.TxExtract
		st.Timing.TxCount += timing.TxCount // transaction scanned so far as part of false positive adjudication
		st.Timing.Total += timing.Total
	default:
		// nothing to do
	}
}

func printInitSummary(rsFile, dbPath string, rsIndex *recsplit.Index, rocksDB *grocksdb.DB, t OpenTiming, cacheMB int) {
	fmt.Println()
	printDelimiterLine()
	fmt.Println("RecSplit + RocksDB Two-Part Lookup - Initialization")
	printDelimiterLine()
	fmt.Println()
	fmt.Printf("RecSplit Index: %s\n", rsFile)
	fmt.Printf("  Load time:                    %s\n", helpers.FormatDuration(t.RecSplitLoad))
	fmt.Printf("  Keys indexed:                 %s\n", helpers.FormatNumber(int64(rsIndex.KeyCount())))
	fmt.Printf("  Index size:                   %s\n", helpers.FormatBytes(rsIndex.Size()))
	fmt.Printf("  Bits per key:                 %.2f\n", float64(rsIndex.Size()*8)/float64(rsIndex.KeyCount()))
	fmt.Printf("  Bucket size:                  %d\n", rsIndex.BucketSize())
	fmt.Printf("  Leaf size:                    %d\n", rsIndex.LeafSize())
	fmt.Println()
	fmt.Printf("RocksDB (ledgerSeq -> LCM): %s\n", dbPath)
	fmt.Printf("  Options create:               %s\n", helpers.FormatDuration(t.RocksDBOptsCreate))
	fmt.Printf("  DB open:                      %s\n", helpers.FormatDuration(t.RocksDBOpen))
	fmt.Printf("  ReadOpts create:              %s\n", helpers.FormatDuration(t.RocksDBROCreate))
	fmt.Printf("  Subtotal:                     %s\n", helpers.FormatDuration(t.RocksDBTotal))
	fmt.Printf("  Block cache:                  %d MB\n", cacheMB)
	fmt.Println()
	fmt.Println("RocksDB Statistics:")
	fmt.Printf("  Estimated keys:               %s\n", rocksDB.GetProperty("rocksdb.estimate-num-keys"))
	fmt.Printf("  Total SST size:               %s\n", formatBytesFromProperty(rocksDB.GetProperty("rocksdb.total-sst-files-size")))
	fmt.Printf("  Live SST size:                %s\n", formatBytesFromProperty(rocksDB.GetProperty("rocksdb.live-sst-files-size")))
	fmt.Printf("  Files by level:               ")
	for i := 0; i < 7; i++ {
		files := rocksDB.GetProperty(fmt.Sprintf("rocksdb.num-files-at-level%d", i))
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("L%d=%s", i, files)
	}
	fmt.Println()
	fmt.Println()
	fmt.Printf("Zstd decoder create:            %s\n", helpers.FormatDuration(t.DecoderCreate))
	fmt.Println()
	fmt.Printf("TOTAL INITIALIZATION TIME:      %s\n", helpers.FormatDuration(t.GrandTotal))
	printDelimiterLine()
	fmt.Println()
	printMemStats()
	fmt.Println()
}

func printNotFoundResult(timing QueryTiming, err error, quiet bool) {
	var (
		eRSNF ErrRecSplitNotFound
		eLNF  ErrLedgerNotFound
		eFPos ErrRecSplitFalsePositive
	)
	fmt.Println()

	switch {
	case errors.As(err, &eRSNF):
		fmt.Printf("TXHASH NOT FOUND in RecSplit filter -  Total time (recsplit lookup): %s\n", helpers.FormatDuration(timing.Total))

	case errors.As(err, &eLNF):
		fmt.Printf("LEDGER NOT FOUND in RocksDB\n")
		printQueryResultTimes(timing)

	case errors.As(err, &eFPos):
		fmt.Printf("FALSE POSITIVE - RecSplit returned ledger %d, but txHash %s not found\n", eFPos.Ledger, eFPos.TxHash)
		printQueryResultTimes(timing)
	default:
		fmt.Printf("Unexpected Error: %v - Total time: %s\n", err, helpers.FormatDuration(timing.Total))
	}
	fmt.Println()
}

func printDelimiterLine() {
	fmt.Println("================================================================================")
}

func printQueryResultTimes(timing QueryTiming, opts ...QueryPrintOption) {
	// default config
	cfg := queryPrintConfig{
		scale: 1,
	}
	// apply options
	for _, opt := range opts {
		opt(&cfg)
	}
	scale := time.Duration(cfg.scale)

	fmt.Printf("TOTAL TIME:                      %s\n", helpers.FormatDuration(timing.Total/scale))
	fmt.Println("Timing Breakdown:")

	if timing.RecSplitLookup > 0 {
		fmt.Printf("  RecSplit (hash->seq):      %s\n", helpers.FormatDuration(timing.RecSplitLookup/scale))
	}
	if timing.RocksDBQuery > 0 {
		fmt.Printf("  RocksDB (seq->LCM):        %s\n", helpers.FormatDuration(timing.RocksDBQuery/scale))
	}
	if timing.Decompress > 0 {
		fmt.Printf("  Decompress (zstd):         %s\n", helpers.FormatDuration(timing.Decompress/scale))
	}
	if timing.Unmarshal > 0 {
		fmt.Printf("  Unmarshal (XDR):           %s\n", helpers.FormatDuration(timing.Unmarshal/scale))
	}
	if timing.TxReaderCreate > 0 {
		fmt.Printf("  TxReader Create:           %s\n", helpers.FormatDuration(timing.TxReaderCreate/scale))
	}
	if timing.TxSearch > 0 {
		fmt.Printf("  TxSearch (%d txs):         %s\n", timing.TxCount, helpers.FormatDuration(timing.TxSearch/scale))
	}
	if timing.TxExtract > 0 {
		fmt.Printf("  TxExtract (marshal):       %s\n", helpers.FormatDuration(timing.TxExtract/scale))
	}

	fmt.Println()
}

func printQueryFoundResult(timing QueryTiming, result *QueryResult, quiet bool) {
	compressionRatio := 100 * (1 - float64(result.CompressedLCMSize)/float64(result.UncompressedLCMSize))

	fmt.Println()
	printDelimiterLine()
	fmt.Println("Query Result")
	fmt.Printf("TX: %s\n", result.TxHash)
	fmt.Printf("Ledger: %d | ClosedAt: %v | TxIndex: %d | TxScanned: %d\n",
		result.LedgerSeq, result.ClosedAt, result.TxIndex, timing.TxCount)
	fmt.Printf("LCM Size: %s compressed -> %s uncompressed (%.1f%% ratio)\n",
		helpers.FormatBytes(int64(result.CompressedLCMSize)),
		helpers.FormatBytes(int64(result.UncompressedLCMSize)),
		compressionRatio)

	if quiet {
		printQueryResultTimes(timing)
		printDelimiterLine()
		return
	}

	// Full verbose output
	fmt.Println()
	fmt.Println("Transaction Data Sizes:")
	fmt.Printf("  Envelope:           %s\n", helpers.FormatBytes(int64(len(result.EnvelopeBytes))))
	fmt.Printf("  Result:             %s\n", helpers.FormatBytes(int64(len(result.ResultBytes))))
	fmt.Printf("  Meta:               %s\n", helpers.FormatBytes(int64(len(result.MetaBytes))))
	totalTxSize := len(result.EnvelopeBytes) + len(result.ResultBytes) + len(result.MetaBytes)
	fmt.Printf("  Total:              %s\n", helpers.FormatBytes(int64(totalTxSize)))

	fmt.Println()
	fmt.Println("Transaction Data (Hex Encoded)")
	fmt.Println()
	fmt.Printf("TxEnvelope:\n%s\n\n", helpers.WrapText(hex.EncodeToString(result.EnvelopeBytes), 80))
	fmt.Printf("TxResult:\n%s\n\n", helpers.WrapText(hex.EncodeToString(result.ResultBytes), 80))
	fmt.Printf("TxMeta:\n%s\n\n", helpers.WrapText(hex.EncodeToString(result.MetaBytes), 80))

	printDelimiterLine()
	printQueryResultTimes(timing)
	printDelimiterLine()
}

// printFinalStats prints cumulative statistics for all queries in ongoing mode.
// Separates successful queries from various "not found" scenarios.
func printFinalStats(stats RunningStats) {
	totalQueries := stats.SuccessfulQueryStats.Count + stats.NotFoundInRecSplitStats.Count
	totalQueries += stats.NotFoundInRecSplitStats.Count + stats.FalsePositiveStats.Count
	if totalQueries == 0 {
		fmt.Println("No queries executed.")
		return
	}

	fmt.Println()
	printDelimiterLine()
	fmt.Printf("Cumulative Statistics (%d total queries)\n", totalQueries)
	printDelimiterLine()
	fmt.Println()

	// Successful queries
	n := stats.SuccessfulQueryStats.Count
	fmt.Printf("=== SUCCESSFUL QUERIES: %d ===\n", n)
	if n > 0 {
		printQueryResultTimes(stats.SuccessfulQueryStats.Timing)
		fmt.Println()
		fmt.Println("Average Per Query:")
		printQueryResultTimes(stats.SuccessfulQueryStats.Timing, WithScale(n))
		printDelimiterLine()
		fmt.Println()
	}

	// Not found in RecSplit filter
	n = stats.NotFoundInRecSplitStats.Count
	fmt.Printf("=== NOT FOUND IN RECSPLIT FILTER: %d ===\n", n)
	if n > 0 {
		fmt.Printf("All hail power of recsplit fuse filter !!!")
		printQueryResultTimes(stats.NotFoundInRecSplitStats.Timing)
		fmt.Println()
		fmt.Println("Average Per Query:")
		printQueryResultTimes(stats.NotFoundInRecSplitStats.Timing, WithScale(n))
		printDelimiterLine()
		fmt.Println()
	}

	// Not found in RocksDB
	n = stats.NotFoundInRocksDbStats.Count
	fmt.Printf("=== NOT FOUND IN ROCKSDB: %d ===\n", n)
	if n > 0 {
		fmt.Println("  (RecSplit returned ledgerSeq, but ledger not in database. this shud never happen IRL !!)")
		printQueryResultTimes(stats.NotFoundInRocksDbStats.Timing)
		fmt.Println()
		fmt.Println("Average Per Query:")
		printQueryResultTimes(stats.NotFoundInRocksDbStats.Timing, WithScale(n))
		printDelimiterLine()
		fmt.Println()
	}

	// False positives
	n = stats.FalsePositiveStats.Count
	fmt.Printf("=== FALSE POSITIVES (detected during ledger scan): %d ===\n", n)
	if n > 0 {
		fmt.Println("  (RecSplit mapped non-existent key to a valid ledger)")
		printQueryResultTimes(stats.FalsePositiveStats.Timing)
		fmt.Println()
		fmt.Println("Average Per Query:")
		printQueryResultTimes(stats.FalsePositiveStats.Timing, WithScale(n))
		printDelimiterLine()
		fmt.Println()
	}

	// Summary
	fmt.Printf("\n\n")
	printDelimiterLine()
	fmt.Println("SUMMARY")
	printDelimiterLine()

	fmt.Printf("  Successful:           %d (%.2f%%)\n",
		stats.SuccessfulQueryStats.Count, 100*float64(stats.SuccessfulQueryStats.Count)/float64(totalQueries))
	fmt.Printf("  Not in filter:        %d (%.2f%%)\n",
		stats.NotFoundInRecSplitStats.Count, 100*float64(stats.NotFoundInRecSplitStats.Count)/float64(totalQueries))
	fmt.Printf("  Not in RocksDB:       %d (%.2f%%)\n",
		stats.NotFoundInRocksDbStats.Count, 100*float64(stats.NotFoundInRocksDbStats.Count)/float64(totalQueries))
	fmt.Printf("  False positives:      %d (%.2f%%)\n",
		stats.FalsePositiveStats.Count, 100*float64(stats.FalsePositiveStats.Count)/float64(totalQueries))
	printDelimiterLine()
	fmt.Println()
}

func formatBytesFromProperty(s string) string {
	var bytes int64
	fmt.Sscanf(s, "%d", &bytes)
	return helpers.FormatBytes(bytes)
}

func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Memory Usage:\n")
	fmt.Printf("  Alloc:      %s\n", helpers.FormatBytes(int64(m.Alloc)))
	fmt.Printf("  HeapInuse:  %s\n", helpers.FormatBytes(int64(m.HeapInuse)))
	fmt.Printf("  Sys:        %s\n", helpers.FormatBytes(int64(m.Sys)))
	fmt.Printf("  NumGC:      %d\n", m.NumGC)
}
