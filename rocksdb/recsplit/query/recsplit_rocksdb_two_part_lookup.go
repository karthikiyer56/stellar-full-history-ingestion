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
//   WITHOUT false-positive filter (--built-with-false-positive-support=false):
//     - Lookup returns (some_ledger_seq, true) - a "false positive"
//     - We only detect this when we fail to find the txHash in the ledger
//     - This is tracked as "false positive detected during ledger scan"
//
//   WITH false-positive filter (--built-with-false-positive-support=true):
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
//   With false-positive filter support:
//     ./recsplit_rocksdb_two_part_lookup \
//       --recsplit-index-file /path/to/txhash-v1-lfp.idx \
//       --ledger-seq-to-lcm-db /path/to/rocksdb \
//       --built-with-false-positive-support \
//       --ongoing --quiet
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

// =============================================================================
// TIMING STRUCTURES
// =============================================================================
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

// =============================================================================
// QUERY RESULT STRUCTURE
// =============================================================================

// QueryResult holds all the data we extract for a successful transaction lookup
type QueryResult struct {
	// The transaction hash we searched for
	TxHash string

	// The ledger sequence number where this transaction was included
	LedgerSeq uint32

	// Index of the transaction within the ledger (0-based)
	TxIndex uint32

	// When this ledger was closed (timestamp from the network)
	ClosedAt time.Time

	// Raw transaction data in XDR binary format
	EnvelopeBytes []byte // The transaction envelope (source account, operations, signatures)
	ResultBytes   []byte // The transaction result (success/failure, operation results)
	MetaBytes     []byte // The transaction meta (ledger changes caused by this tx)

	// Size metrics for analysis
	CompressedLCMSize   int // Size of compressed LedgerCloseMeta from RocksDB
	UncompressedLCMSize int // Size after zstd decompression
}

// =============================================================================
// RUNNING STATISTICS
// =============================================================================
// These track cumulative statistics across multiple queries in ongoing mode.
// We separate "found" queries from various "not found" scenarios.

type RunningStats struct {
	// Successful queries
	QueryCount          int
	TotalRecSplit       time.Duration
	TotalRocksDB        time.Duration
	TotalDecompress     time.Duration
	TotalUnmarshal      time.Duration
	TotalTxReaderCreate time.Duration
	TotalTxSearch       time.Duration
	TotalTxExtract      time.Duration
	TotalTime           time.Duration
	TotalTxScanned      int

	// Not found in RecSplit filter
	NotFoundInRecSplit          int
	TotalTimeNotFoundFilter     time.Duration
	TotalRecSplitNotFoundFilter time.Duration

	// Not found in RocksDB
	NotFoundInRocksDB       int
	TotalTimeNotFoundDB     time.Duration
	TotalRecSplitNotFoundDB time.Duration
	TotalRocksDBNotFoundDB  time.Duration

	// False positives (found in RecSplit, but not in ledger)
	FalsePositiveLedgerScan          int
	TotalTimeFalsePositive           time.Duration
	TotalRecSplitFalsePositive       time.Duration
	TotalRocksDBFalsePositive        time.Duration
	TotalDecompressFalsePositive     time.Duration
	TotalUnmarshalFalsePositive      time.Duration
	TotalTxReaderCreateFalsePositive time.Duration
	TotalTxSearchFalsePositive       time.Duration
	TotalTxScannedFalsePositive      int
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
		quiet   bool // Minimal output (for benchmarking)

		// RecSplit index configuration
		// If true, the index was built with Version=1 and LessFalsePositives=true
		// This enables the fuse filter for detecting non-existent keys
		builtWithFalsePositiveSupport bool

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
		"Minimal output - only timing metrics (for benchmarking)")
	flag.BoolVar(&builtWithFalsePositiveSupport, "built-with-false-positive-support", false,
		"Set if index was built with Version=1 and LessFalsePositives=true")
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
	printInitSummary(recsplitIndexFile, ledgerSeqToLCMDB, rsIndex, rocksDB, openTiming, blockCacheSizeMB, builtWithFalsePositiveSupport)

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
				printFinalStats(stats, builtWithFalsePositiveSupport)
				fmt.Println("Stopping.")
				return
			}
			if strings.EqualFold(line, "STATS") {
				printFinalStats(stats, builtWithFalsePositiveSupport)
				continue
			}

			fmt.Println(line)
			timing, result, queryErr := runQuery(rsReader, rocksDB, rocksRO, zstdDecoder, line, builtWithFalsePositiveSupport)
			updateStats(&stats, timing, result, queryErr)

			if queryErr != nil {
				printNotFoundResult(result, timing, queryErr, quiet)
			} else {
				printQueryResult(timing, result, quiet)
			}
		}
		printFinalStats(stats, builtWithFalsePositiveSupport)
		return
	}

	// Single query mode
	fmt.Println(txHashHex)
	timing, result, err := runQuery(rsReader, rocksDB, rocksRO, zstdDecoder, txHashHex, builtWithFalsePositiveSupport)
	if err != nil {
		printNotFoundResult(result, timing, err, quiet)
		os.Exit(1)
	}
	printQueryResult(timing, result, quiet)
}

// =============================================================================
// CORE QUERY FUNCTION
// =============================================================================

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
//   - "not found in RecSplit filter" - false-positive filter rejected the key
//   - "ledger X not found in RocksDB" - index/database inconsistency
//   - "txHash not found in ledger X" - RecSplit false positive (key doesn't exist)
func runQuery(
	rsReader *recsplit.IndexReader,
	rocksDB *grocksdb.DB,
	rocksRO *grocksdb.ReadOptions,
	zstdDecoder *zstd.Decoder,
	txHashHex string,
	hasFalsePositiveFilter bool,
) (QueryTiming, *QueryResult, error) {
	timing := QueryTiming{}
	totalStart := time.Now()

	// Step 1: Parse the transaction hash
	// -------------------------------------------------------------------------
	// Transaction hashes are 32 bytes (256 bits), represented as 64 hex characters
	txHashBytes, err := hex.DecodeString(txHashHex)
	if err != nil {
		timing.Total = time.Since(totalStart)
		return timing, nil, fmt.Errorf("invalid hex string: %v", err)
	}
	if len(txHashBytes) != 32 {
		timing.Total = time.Since(totalStart)
		return timing, nil, fmt.Errorf("invalid hash length: expected 32 bytes, got %d", len(txHashBytes))
	}

	// Step 2: RecSplit Lookup (txHash -> ledgerSeq)
	// -------------------------------------------------------------------------
	// RecSplit provides O(1) lookup with ~2 bits per key overhead.
	//
	// The Lookup method returns (value, found):
	//   - With false-positive filter: found=false means key definitely doesn't exist
	//   - Without filter: found is always true, but value may be garbage for non-existent keys
	//
	// The value is the ledger sequence number where this transaction was included.
	t1 := time.Now()
	ledgerSeqU64, found := rsReader.Lookup(txHashBytes)
	timing.RecSplitLookup = time.Since(t1)

	// If we have a false-positive filter and it says "not found", we're done
	// This is a clean rejection - the key was never added to the index
	if hasFalsePositiveFilter && !found {
		timing.Total = time.Since(totalStart)
		return timing, nil, fmt.Errorf("not found in RecSplit filter")
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
		return timing, nil, fmt.Errorf("ledger %d not found in RocksDB", ledgerSeq)
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
	if foundTx == nil {
		timing.Total = time.Since(totalStart)
		// hack.. include the incorrectly returned ledgerSequence so that you can print it to console
		badResult := &QueryResult{
			TxHash:    txHashHex,
			LedgerSeq: ledgerSeq,
		}
		return timing, badResult, fmt.Errorf("txHash not found in ledger %d (scanned %d txs) - RecSplit false positive", ledgerSeq, txCount)
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

// =============================================================================
// STATISTICS UPDATE
// =============================================================================

// updateStats updates the running statistics based on the query result.
// It categorizes the query into one of four buckets:
//  1. Successful query (txHash found)
//  2. Not found in RecSplit filter (only with false-positive support)
//  3. Not found in RocksDB (data inconsistency)
//  4. False positive from RecSplit (txHash not in ledger)
func updateStats(stats *RunningStats, timing QueryTiming, result *QueryResult, err error) {
	if err == nil {
		stats.QueryCount++
		stats.TotalRecSplit += timing.RecSplitLookup
		stats.TotalRocksDB += timing.RocksDBQuery
		stats.TotalDecompress += timing.Decompress
		stats.TotalUnmarshal += timing.Unmarshal
		stats.TotalTxReaderCreate += timing.TxReaderCreate
		stats.TotalTxSearch += timing.TxSearch
		stats.TotalTxExtract += timing.TxExtract
		stats.TotalTime += timing.Total
		stats.TotalTxScanned += timing.TxCount
		return
	}

	errStr := err.Error()

	// Case 1: Not found in RecSplit filter
	if strings.Contains(errStr, "not found in RecSplit filter") {
		stats.NotFoundInRecSplit++
		stats.TotalTimeNotFoundFilter += timing.Total
		stats.TotalRecSplitNotFoundFilter += timing.RecSplitLookup
	} else if strings.Contains(errStr, "not found in RocksDB") {
		stats.NotFoundInRocksDB++
		stats.TotalTimeNotFoundDB += timing.Total
		stats.TotalRecSplitNotFoundDB += timing.RecSplitLookup
		stats.TotalRocksDBNotFoundDB += timing.RocksDBQuery
	} else if strings.Contains(errStr, "RecSplit false positive") {
		stats.FalsePositiveLedgerScan++
		stats.TotalTimeFalsePositive += timing.Total
		stats.TotalRecSplitFalsePositive += timing.RecSplitLookup
		stats.TotalRocksDBFalsePositive += timing.RocksDBQuery
		stats.TotalDecompressFalsePositive += timing.Decompress
		stats.TotalUnmarshalFalsePositive += timing.Unmarshal
		stats.TotalTxReaderCreateFalsePositive += timing.TxReaderCreate
		stats.TotalTxSearchFalsePositive += timing.TxSearch
		stats.TotalTxScannedFalsePositive += timing.TxCount
	} else {
		stats.NotFoundInRecSplit++
		stats.TotalTimeNotFoundFilter += timing.Total
	}
}

func printInitSummary(rsFile, dbPath string, rsIndex *recsplit.Index, rocksDB *grocksdb.DB, t OpenTiming, cacheMB int, hasFP bool) {
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Println("RecSplit + RocksDB Two-Part Lookup - Initialization")
	fmt.Println("================================================================================")
	fmt.Println()
	fmt.Printf("RecSplit Index: %s\n", rsFile)
	fmt.Printf("  Load time:                    %s\n", helpers.FormatDuration(t.RecSplitLoad))
	fmt.Printf("  Keys indexed:                 %s\n", helpers.FormatNumber(int64(rsIndex.KeyCount())))
	fmt.Printf("  Index size:                   %s\n", helpers.FormatBytes(rsIndex.Size()))
	fmt.Printf("  Bits per key:                 %.2f\n", float64(rsIndex.Size()*8)/float64(rsIndex.KeyCount()))
	fmt.Printf("  Bucket size:                  %d\n", rsIndex.BucketSize())
	fmt.Printf("  Leaf size:                    %d\n", rsIndex.LeafSize())
	fmt.Printf("  False-positive filter:        %v\n", hasFP)
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
	fmt.Println("================================================================================")
	fmt.Println()
	printMemStats()
	fmt.Println()
}

func printNotFoundResult(result *QueryResult, timing QueryTiming, err error, quiet bool) {
	errStr := err.Error()

	if strings.Contains(errStr, "not found in RecSplit filter") {
		fmt.Printf("NOT FOUND in RecSplit filter - Query took: %s\n", helpers.FormatDuration(timing.RecSplitLookup))
		if !quiet {
			fmt.Printf("Error: %v\n", err)
		}
	} else if strings.Contains(errStr, "not found in RocksDB") {
		fmt.Printf("NOT FOUND in RocksDB - Query took: %s\n", helpers.FormatDuration(timing.Total))
		if !quiet {
			fmt.Println()
			fmt.Println("Timing Breakdown:")
			fmt.Printf("  RecSplit (hash->seq):      %s\n", helpers.FormatDuration(timing.RecSplitLookup))
			fmt.Printf("  RocksDB (seq->LCM):        %s\n", helpers.FormatDuration(timing.RocksDBQuery))
			fmt.Printf("  ----------------------------------------\n")
			fmt.Printf("  TOTAL:                     %s\n", helpers.FormatDuration(timing.Total))
			fmt.Println()
			fmt.Printf("Error: %v\n", err)
		}
	} else if strings.Contains(errStr, "RecSplit false positive") {
		// AHA, this case will have returned a random ledgerSequence number, that recsplit thought contained the txHash
		// Just print it for kicks
		falseLedgerSeq := result.LedgerSeq
		txHash := result.TxHash
		fmt.Printf("FALSE POSITIVE - RecSplit returned ledger %d, but txHash: %s not found. Query took %s\n",
			falseLedgerSeq, txHash, helpers.FormatDuration(timing.Total))
		if !quiet {
			fmt.Println()
			fmt.Println("Timing Breakdown:")
			fmt.Printf("  RecSplit (hash->seq):      %s\n", helpers.FormatDuration(timing.RecSplitLookup))
			fmt.Printf("  RocksDB (seq->LCM):        %s\n", helpers.FormatDuration(timing.RocksDBQuery))
			fmt.Printf("  Decompress (zstd):         %s\n", helpers.FormatDuration(timing.Decompress))
			fmt.Printf("  Unmarshal (XDR):           %s\n", helpers.FormatDuration(timing.Unmarshal))
			fmt.Printf("  TxReader Create:           %s\n", helpers.FormatDuration(timing.TxReaderCreate))
			fmt.Printf("  TxSearch (%d txs):         %s\n", timing.TxCount, helpers.FormatDuration(timing.TxSearch))
			fmt.Printf("  ----------------------------------------\n")
			fmt.Printf("  TOTAL:                     %s\n", helpers.FormatDuration(timing.Total))
			fmt.Println()
			fmt.Printf("Error: %v\n", err)
		}
	} else {
		fmt.Printf("Error: %v - Query took: %s\n", err, helpers.FormatDuration(timing.Total))
	}
	fmt.Println()
}

func printQueryResult(timing QueryTiming, result *QueryResult, quiet bool) {
	compressionRatio := 100 * (1 - float64(result.CompressedLCMSize)/float64(result.UncompressedLCMSize))

	if quiet {
		fmt.Printf("TX: %s\n", result.TxHash)
		fmt.Printf("Ledger: %d | ClosedAt: %v | TxIndex: %d | TxScanned: %d\n",
			result.LedgerSeq, result.ClosedAt, result.TxIndex, timing.TxCount)
		fmt.Printf("LCM Size: %s compressed -> %s uncompressed (%.1f%% ratio)\n",
			helpers.FormatBytes(int64(result.CompressedLCMSize)),
			helpers.FormatBytes(int64(result.UncompressedLCMSize)),
			compressionRatio)
		fmt.Println()
		fmt.Println("Timing Breakdown:")
		fmt.Printf("  RecSplit (hash->seq):      %s\n", helpers.FormatDuration(timing.RecSplitLookup))
		fmt.Printf("  RocksDB (seq->LCM):        %s\n", helpers.FormatDuration(timing.RocksDBQuery))
		fmt.Printf("  Decompress (zstd):         %s\n", helpers.FormatDuration(timing.Decompress))
		fmt.Printf("  Unmarshal (XDR):           %s\n", helpers.FormatDuration(timing.Unmarshal))
		fmt.Printf("  TxReader Create:           %s\n", helpers.FormatDuration(timing.TxReaderCreate))
		fmt.Printf("  TxSearch (%d txs):         %s\n", timing.TxCount, helpers.FormatDuration(timing.TxSearch))
		fmt.Printf("  TxExtract (marshal):       %s\n", helpers.FormatDuration(timing.TxExtract))
		fmt.Printf("  ----------------------------------------\n")
		fmt.Printf("  TOTAL:                     %s\n", helpers.FormatDuration(timing.Total))
		fmt.Println()
		fmt.Println("================================================================================")
		fmt.Println()
		return
	}

	// Full verbose output
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Println("Query Result")
	fmt.Println("================================================================================")
	fmt.Println()
	fmt.Printf("Transaction Hash:     %s\n", result.TxHash)
	fmt.Printf("Ledger Sequence:      %d\n", result.LedgerSeq)
	fmt.Printf("Transaction Index:    %d\n", result.TxIndex)
	fmt.Printf("Closed At:            %v\n", result.ClosedAt)
	fmt.Println()
	fmt.Println("LCM Sizes:")
	fmt.Printf("  Compressed:         %s\n", helpers.FormatBytes(int64(result.CompressedLCMSize)))
	fmt.Printf("  Uncompressed:       %s\n", helpers.FormatBytes(int64(result.UncompressedLCMSize)))
	fmt.Printf("  Compression Ratio:  %.2f%%\n", compressionRatio)
	fmt.Println()
	fmt.Println("Transaction Data Sizes:")
	fmt.Printf("  Envelope:           %s\n", helpers.FormatBytes(int64(len(result.EnvelopeBytes))))
	fmt.Printf("  Result:             %s\n", helpers.FormatBytes(int64(len(result.ResultBytes))))
	fmt.Printf("  Meta:               %s\n", helpers.FormatBytes(int64(len(result.MetaBytes))))
	totalTxSize := len(result.EnvelopeBytes) + len(result.ResultBytes) + len(result.MetaBytes)
	fmt.Printf("  Total:              %s\n", helpers.FormatBytes(int64(totalTxSize)))
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Println("Timing Breakdown")
	fmt.Println("================================================================================")
	fmt.Println()
	fmt.Printf("Step 1: RecSplit Query (txHash -> ledgerSeq)\n")
	fmt.Printf("        Time: %s\n", helpers.FormatDuration(timing.RecSplitLookup))
	fmt.Println()
	fmt.Printf("Step 2: RocksDB Query (ledgerSeq -> compressed LCM)\n")
	fmt.Printf("        Time: %s\n", helpers.FormatDuration(timing.RocksDBQuery))
	fmt.Println()
	fmt.Printf("Step 3: Decompress LCM (zstd)\n")
	fmt.Printf("        Time: %s\n", helpers.FormatDuration(timing.Decompress))
	fmt.Println()
	fmt.Printf("Step 4: Unmarshal LCM (XDR)\n")
	fmt.Printf("        Time: %s\n", helpers.FormatDuration(timing.Unmarshal))
	fmt.Println()
	fmt.Printf("Step 5: Search for Transaction\n")
	fmt.Printf("        TxReader Create: %s\n", helpers.FormatDuration(timing.TxReaderCreate))
	fmt.Printf("        Search (%d txs): %s\n", timing.TxCount, helpers.FormatDuration(timing.TxSearch))
	fmt.Println()
	fmt.Printf("Step 6: Extract Transaction Data\n")
	fmt.Printf("        Time: %s\n", helpers.FormatDuration(timing.TxExtract))
	fmt.Println()
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("TOTAL QUERY TIME: %s\n", helpers.FormatDuration(timing.Total))
	fmt.Println("================================================================================")
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Println("Transaction Data (Hex Encoded)")
	fmt.Println("================================================================================")
	fmt.Println()
	fmt.Printf("TxEnvelope:\n%s\n", helpers.WrapText(hex.EncodeToString(result.EnvelopeBytes), 80))
	fmt.Printf("TxResult:\n%s\n", helpers.WrapText(hex.EncodeToString(result.ResultBytes), 80))
	fmt.Printf("TxMeta:\n%s\n", helpers.WrapText(hex.EncodeToString(result.MetaBytes), 80))
	fmt.Println("================================================================================")
	fmt.Println()
}

// printFinalStats prints cumulative statistics for all queries in ongoing mode.
// Separates successful queries from various "not found" scenarios.
func printFinalStats(stats RunningStats, hasFalsePositiveFilter bool) {
	totalQueries := stats.QueryCount + stats.NotFoundInRecSplit + stats.NotFoundInRocksDB + stats.FalsePositiveLedgerScan
	if totalQueries == 0 {
		fmt.Println("No queries executed.")
		return
	}

	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Printf("Cumulative Statistics (%d total queries)\n", totalQueries)
	fmt.Println("================================================================================")
	fmt.Println()

	// Successful queries
	fmt.Printf("=== SUCCESSFUL QUERIES: %d ===\n", stats.QueryCount)
	if stats.QueryCount > 0 {
		fmt.Println()
		fmt.Println("Total Time Breakdown:")
		fmt.Printf("  RecSplit Lookups:     %s\n", helpers.FormatDuration(stats.TotalRecSplit))
		fmt.Printf("  RocksDB Queries:      %s\n", helpers.FormatDuration(stats.TotalRocksDB))
		fmt.Printf("  Decompress:           %s\n", helpers.FormatDuration(stats.TotalDecompress))
		fmt.Printf("  Unmarshal:            %s\n", helpers.FormatDuration(stats.TotalUnmarshal))
		fmt.Printf("  TxReader Create:      %s\n", helpers.FormatDuration(stats.TotalTxReaderCreate))
		fmt.Printf("  Tx Search:            %s\n", helpers.FormatDuration(stats.TotalTxSearch))
		fmt.Printf("  Tx Extract:           %s\n", helpers.FormatDuration(stats.TotalTxExtract))
		fmt.Printf("  ----------------------------------------\n")
		fmt.Printf("  TOTAL:                %s\n", helpers.FormatDuration(stats.TotalTime))
		fmt.Println()

		n := time.Duration(stats.QueryCount)
		fmt.Println("Average Per Query:")
		fmt.Printf("  RecSplit Lookup:      %s\n", helpers.FormatDuration(stats.TotalRecSplit/n))
		fmt.Printf("  RocksDB Query:        %s\n", helpers.FormatDuration(stats.TotalRocksDB/n))
		fmt.Printf("  Decompress:           %s\n", helpers.FormatDuration(stats.TotalDecompress/n))
		fmt.Printf("  Unmarshal:            %s\n", helpers.FormatDuration(stats.TotalUnmarshal/n))
		fmt.Printf("  TxReader Create:      %s\n", helpers.FormatDuration(stats.TotalTxReaderCreate/n))
		fmt.Printf("  Tx Search:            %s\n", helpers.FormatDuration(stats.TotalTxSearch/n))
		fmt.Printf("  Tx Extract:           %s\n", helpers.FormatDuration(stats.TotalTxExtract/n))
		fmt.Printf("  ----------------------------------------\n")
		fmt.Printf("  TOTAL:                %s\n", helpers.FormatDuration(stats.TotalTime/n))
		fmt.Println()
		avgTxScanned := float64(stats.TotalTxScanned) / float64(stats.QueryCount)
		fmt.Printf("Average Transactions Scanned: %.1f\n", avgTxScanned)
	}
	fmt.Println()

	// Not found in RecSplit filter
	if hasFalsePositiveFilter || stats.NotFoundInRecSplit > 0 {
		fmt.Printf("=== NOT FOUND IN RECSPLIT FILTER: %d ===\n", stats.NotFoundInRecSplit)
		if stats.NotFoundInRecSplit > 0 {
			fmt.Println("  (Key rejected by fuse filter - definitely not in index)")
			n := time.Duration(stats.NotFoundInRecSplit)
			fmt.Printf("  Total time:           %s\n", helpers.FormatDuration(stats.TotalTimeNotFoundFilter))
			fmt.Printf("  Avg RecSplit:         %s\n", helpers.FormatDuration(stats.TotalRecSplitNotFoundFilter/n))
			fmt.Printf("  Avg total:            %s\n", helpers.FormatDuration(stats.TotalTimeNotFoundFilter/n))
		}
		fmt.Println()
	}

	// Not found in RocksDB
	fmt.Printf("=== NOT FOUND IN ROCKSDB: %d ===\n", stats.NotFoundInRocksDB)
	if stats.NotFoundInRocksDB > 0 {
		fmt.Println("  (RecSplit returned ledgerSeq, but ledger not in database. this shud never happen IRL !!)")
		n := time.Duration(stats.NotFoundInRocksDB)
		fmt.Printf("  Total time:           %s\n", helpers.FormatDuration(stats.TotalTimeNotFoundDB))
		fmt.Printf("  Avg RecSplit:         %s\n", helpers.FormatDuration(stats.TotalRecSplitNotFoundDB/n))
		fmt.Printf("  Avg RocksDB:          %s\n", helpers.FormatDuration(stats.TotalRocksDBNotFoundDB/n))
		fmt.Printf("  Avg total:            %s\n", helpers.FormatDuration(stats.TotalTimeNotFoundDB/n))
	}
	fmt.Println()

	// False positives
	fmt.Printf("=== FALSE POSITIVES (detected during ledger scan): %d ===\n", stats.FalsePositiveLedgerScan)
	if stats.FalsePositiveLedgerScan > 0 {
		fmt.Println("  (RecSplit mapped non-existent key to a valid ledger)")
		fmt.Println()
		fmt.Println("  Total Time Breakdown:")
		fmt.Printf("    RecSplit Lookups:   %s\n", helpers.FormatDuration(stats.TotalRecSplitFalsePositive))
		fmt.Printf("    RocksDB Queries:    %s\n", helpers.FormatDuration(stats.TotalRocksDBFalsePositive))
		fmt.Printf("    Decompress:         %s\n", helpers.FormatDuration(stats.TotalDecompressFalsePositive))
		fmt.Printf("    Unmarshal:          %s\n", helpers.FormatDuration(stats.TotalUnmarshalFalsePositive))
		fmt.Printf("    TxReader Create:    %s\n", helpers.FormatDuration(stats.TotalTxReaderCreateFalsePositive))
		fmt.Printf("    Tx Search:          %s\n", helpers.FormatDuration(stats.TotalTxSearchFalsePositive))
		fmt.Printf("    ----------------------------------------\n")
		fmt.Printf("    TOTAL:              %s\n", helpers.FormatDuration(stats.TotalTimeFalsePositive))
		fmt.Println()

		n := time.Duration(stats.FalsePositiveLedgerScan)
		fmt.Println("  Average Per False Positive:")
		fmt.Printf("    RecSplit Lookup:    %s\n", helpers.FormatDuration(stats.TotalRecSplitFalsePositive/n))
		fmt.Printf("    RocksDB Query:      %s\n", helpers.FormatDuration(stats.TotalRocksDBFalsePositive/n))
		fmt.Printf("    Decompress:         %s\n", helpers.FormatDuration(stats.TotalDecompressFalsePositive/n))
		fmt.Printf("    Unmarshal:          %s\n", helpers.FormatDuration(stats.TotalUnmarshalFalsePositive/n))
		fmt.Printf("    TxReader Create:    %s\n", helpers.FormatDuration(stats.TotalTxReaderCreateFalsePositive/n))
		fmt.Printf("    Tx Search:          %s\n", helpers.FormatDuration(stats.TotalTxSearchFalsePositive/n))
		fmt.Printf("    ----------------------------------------\n")
		fmt.Printf("    TOTAL:              %s\n", helpers.FormatDuration(stats.TotalTimeFalsePositive/n))
		fmt.Println()
		avgTxScanned := float64(stats.TotalTxScannedFalsePositive) / float64(stats.FalsePositiveLedgerScan)
		fmt.Printf("  Avg Transactions Scanned: %.1f\n", avgTxScanned)
	}
	fmt.Println()

	// Summary
	fmt.Println("================================================================================")
	fmt.Println("SUMMARY")
	fmt.Println("================================================================================")
	fmt.Printf("  Successful:           %d (%.2f%%)\n", stats.QueryCount, 100*float64(stats.QueryCount)/float64(totalQueries))
	if hasFalsePositiveFilter || stats.NotFoundInRecSplit > 0 {
		fmt.Printf("  Not in filter:        %d (%.2f%%)\n", stats.NotFoundInRecSplit, 100*float64(stats.NotFoundInRecSplit)/float64(totalQueries))
	}
	fmt.Printf("  Not in RocksDB:       %d (%.2f%%)\n", stats.NotFoundInRocksDB, 100*float64(stats.NotFoundInRocksDB)/float64(totalQueries))
	fmt.Printf("  False positives:      %d (%.2f%%)\n", stats.FalsePositiveLedgerScan, 100*float64(stats.FalsePositiveLedgerScan)/float64(totalQueries))
	fmt.Println("================================================================================")
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
