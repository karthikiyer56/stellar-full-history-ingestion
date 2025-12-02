package main

// Filename: rocksdb_two_step_lookup.go
//
// A tool to query transactions using a two-step lookup:
//   1. Look up txHash in DB3 to get ledgerSequence
//   2. Look up ledgerSequence in DB1 to get compressed LedgerCloseMeta
//   3. Decompress LCM, iterate through transactions to find matching txHash
//
// This approach uses less storage (no duplicate tx data) but slower lookups.
//
// Usage:
//
//   Single query:
//     go run rocksdb_two_step_lookup.go \
//       --db1 /path/to/db1 \
//       --db3 /path/to/db3 \
//       --tx abc123def456...
//
//   Ongoing stdin mode (DB open cost is one-time):
//     go run rocksdb_two_step_lookup.go \
//       --db1 /path/to/db1 \
//       --db3 /path/to/db3 \
//       --ongoing
//     (then type transaction hashes, one per line)
//
//   Quiet mode (metrics only):
//     go run rocksdb_two_step_lookup.go \
//       --db1 /path/to/db1 \
//       --db3 /path/to/db3 \
//       --ongoing --quiet
//
// Examples:
//
//   # Single lookup
//   ./rocksdb_two_step_lookup --db1 /data/lcm/2024 --db3 /data/hash_seq/2024 --tx 4f3c8a7b...
//
//   # Benchmark multiple lookups
//   cat tx_hashes.txt | ./rocksdb_two_step_lookup --db1 /data/lcm/2024 --db3 /data/hash_seq/2024 --ongoing --quiet
//
//   # Interactive mode
//   ./rocksdb_two_step_lookup --db1 /data/lcm/2024 --db3 /data/hash_seq/2024 --ongoing
//   > 4f3c8a7b...
//   > 9a8b7c6d...
// 	 > STATS
//   > dfd4edf2...
//   > STOP

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/klauspost/compress/zstd"
	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
)

// =============================================================================
// Structs for tracking statistics
// =============================================================================

// DBHandle holds RocksDB database and read options
type DBHandle struct {
	DB   *grocksdb.DB
	Opts *grocksdb.Options
	RO   *grocksdb.ReadOptions
}

// OpenTiming tracks time to open databases
type OpenTiming struct {
	DB1OptsCreate time.Duration
	DB1Open       time.Duration
	DB1ROCreate   time.Duration
	DB1Total      time.Duration

	DB3OptsCreate time.Duration
	DB3Open       time.Duration
	DB3ROCreate   time.Duration
	DB3Total      time.Duration

	DecoderCreate time.Duration
	GrandTotal    time.Duration
}

// QueryTiming tracks time for each step of a query
type QueryTiming struct {
	// Step 1: DB3 lookup (txHash -> ledgerSeq)
	DB3Query time.Duration

	// Step 2: DB1 lookup (ledgerSeq -> compressed LCM)
	DB1Query time.Duration

	// Step 3: Decompress LCM
	Decompress time.Duration

	// Step 4: Unmarshal XDR to LedgerCloseMeta
	Unmarshal time.Duration

	// Step 5: Create transaction reader and find matching tx
	TxReaderCreate time.Duration
	TxSearch       time.Duration
	TxCount        int // Number of transactions scanned

	// Step 6: Extract transaction data
	TxExtract time.Duration

	// Totals
	Total time.Duration
}

// QueryResult holds the result of a successful query
type QueryResult struct {
	TxHash        string
	LedgerSeq     uint32
	TxIndex       uint32
	ClosedAt      time.Time
	EnvelopeBytes []byte
	ResultBytes   []byte
	MetaBytes     []byte

	// Size metrics
	CompressedLCMSize   int
	UncompressedLCMSize int
}

// RunningStats tracks cumulative statistics for ongoing mode
type RunningStats struct {
	QueryCount      int
	TotalDB3Query   time.Duration
	TotalDB1Query   time.Duration
	TotalDecompress time.Duration
	TotalUnmarshal  time.Duration
	TotalTxSearch   time.Duration
	TotalTxExtract  time.Duration
	TotalTime       time.Duration
	TotalTxScanned  int
}

// =============================================================================
// Main
// =============================================================================

func main() {
	// =========================================================================
	// Parse command-line flags
	// =========================================================================
	var db1Path, db3Path, txHashHex string
	var ongoing, quiet bool
	var blockCacheSizeMB int

	flag.StringVar(&db1Path, "db1", "", "Path to DB1 (ledgerSeq -> compressed LCM)")
	flag.StringVar(&db3Path, "db3", "", "Path to DB3 (txHash -> ledgerSeq)")
	flag.StringVar(&txHashHex, "tx", "", "Transaction hash in hex (omit when using --ongoing)")
	flag.BoolVar(&ongoing, "ongoing", false, "Read transaction hashes from stdin continuously")
	flag.BoolVar(&quiet, "quiet", false, "Print only metrics instead of full output")
	flag.IntVar(&blockCacheSizeMB, "block-cache-mb", 512, "Block cache size in MB per database")

	flag.Parse()

	// =========================================================================
	// Validate arguments
	// =========================================================================
	if db1Path == "" || db3Path == "" {
		log.Fatalf("Both --db1 and --db3 are required")
	}
	if ongoing && txHashHex != "" {
		log.Fatalf("--tx cannot be used when --ongoing is provided")
	}
	if !ongoing && txHashHex == "" {
		log.Fatalf("Provide --tx OR --ongoing")
	}

	// Check paths exist
	if _, err := os.Stat(db1Path); os.IsNotExist(err) {
		log.Fatalf("DB1 path does not exist: %s", db1Path)
	}
	if _, err := os.Stat(db3Path); os.IsNotExist(err) {
		log.Fatalf("DB3 path does not exist: %s", db3Path)
	}

	// =========================================================================
	// Open databases with timing
	// =========================================================================
	openTiming := OpenTiming{}
	grandStart := time.Now()

	// --- Open DB1 (ledgerSeq -> LCM) ---
	db1, db1Timing := openRocksDB(db1Path, "DB1", blockCacheSizeMB)
	defer db1.DB.Close()
	defer db1.Opts.Destroy()
	defer db1.RO.Destroy()
	openTiming.DB1OptsCreate = db1Timing.optsCreate
	openTiming.DB1Open = db1Timing.dbOpen
	openTiming.DB1ROCreate = db1Timing.roCreate
	openTiming.DB1Total = db1Timing.total

	// --- Open DB3 (txHash -> ledgerSeq) ---
	db3, db3Timing := openRocksDB(db3Path, "DB3", blockCacheSizeMB)
	defer db3.DB.Close()
	defer db3.Opts.Destroy()
	defer db3.RO.Destroy()
	openTiming.DB3OptsCreate = db3Timing.optsCreate
	openTiming.DB3Open = db3Timing.dbOpen
	openTiming.DB3ROCreate = db3Timing.roCreate
	openTiming.DB3Total = db3Timing.total

	// --- Create zstd decoder (reusable) ---
	t := time.Now()
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		log.Fatalf("Failed to create zstd decoder: %v", err)
	}
	defer decoder.Close()
	openTiming.DecoderCreate = time.Since(t)

	openTiming.GrandTotal = time.Since(grandStart)

	// =========================================================================
	// Print initialization timing
	// =========================================================================
	fmt.Printf("\n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("RocksDB Two-Step Lookup - Initialization\n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("\n")
	fmt.Printf("DB1 (ledgerSeq -> LCM): %s\n", db1Path)
	fmt.Printf("  Options create:     %s\n", helpers.FormatDuration(openTiming.DB1OptsCreate))
	fmt.Printf("  DB open:            %s\n", helpers.FormatDuration(openTiming.DB1Open))
	fmt.Printf("  ReadOpts create:    %s\n", helpers.FormatDuration(openTiming.DB1ROCreate))
	fmt.Printf("  Subtotal:           %s\n", helpers.FormatDuration(openTiming.DB1Total))
	fmt.Printf("\n")
	fmt.Printf("DB3 (txHash -> ledgerSeq): %s\n", db3Path)
	fmt.Printf("  Options create:     %s\n", helpers.FormatDuration(openTiming.DB3OptsCreate))
	fmt.Printf("  DB open:            %s\n", helpers.FormatDuration(openTiming.DB3Open))
	fmt.Printf("  ReadOpts create:    %s\n", helpers.FormatDuration(openTiming.DB3ROCreate))
	fmt.Printf("  Subtotal:           %s\n", helpers.FormatDuration(openTiming.DB3Total))
	fmt.Printf("\n")
	fmt.Printf("Zstd decoder create:  %s\n", helpers.FormatDuration(openTiming.DecoderCreate))
	fmt.Printf("Block cache per DB:   %d MB\n", blockCacheSizeMB)
	fmt.Printf("\n")
	fmt.Printf("TOTAL OPEN TIME:      %s\n", helpers.FormatDuration(openTiming.GrandTotal))
	fmt.Printf("================================================================================\n")
	fmt.Printf("\n")

	// Print DB stats
	printDBStats(db1.DB, "DB1")
	printDBStats(db3.DB, "DB3")
	fmt.Printf("\n")

	// =========================================================================
	// ONGOING MODE
	// =========================================================================
	if ongoing {
		runningStats := RunningStats{}
		reader := bufio.NewScanner(os.Stdin)

		fmt.Println("Ongoing mode: enter tx hashes (or STOP to quit, STATS for summary):")
		fmt.Println()

		for reader.Scan() {
			line := strings.TrimSpace(reader.Text())
			if line == "" {
				continue
			}
			if strings.EqualFold(line, "STOP") {
				printFinalStats(runningStats)
				fmt.Println("Stopping.")
				return
			}
			if strings.EqualFold(line, "STATS") {
				printFinalStats(runningStats)
				continue
			}

			timing, result, err := runQuery(db1, db3, decoder, line)
			if err != nil {
				fmt.Printf("Error: %v\n\n", err)
				continue
			}

			// Update running stats
			runningStats.QueryCount++
			runningStats.TotalDB3Query += timing.DB3Query
			runningStats.TotalDB1Query += timing.DB1Query
			runningStats.TotalDecompress += timing.Decompress
			runningStats.TotalUnmarshal += timing.Unmarshal
			runningStats.TotalTxSearch += timing.TxSearch
			runningStats.TotalTxExtract += timing.TxExtract
			runningStats.TotalTime += timing.Total
			runningStats.TotalTxScanned += timing.TxCount

			printQueryResult(timing, result, quiet)
		}

		printFinalStats(runningStats)
		return
	}

	// =========================================================================
	// SINGLE QUERY MODE
	// =========================================================================
	timing, result, err := runQuery(db1, db3, decoder, txHashHex)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	printQueryResult(timing, result, quiet)
}

// =============================================================================
// openRocksDB opens a RocksDB database with timing
// =============================================================================

type dbOpenTiming struct {
	optsCreate time.Duration
	dbOpen     time.Duration
	roCreate   time.Duration
	total      time.Duration
}

func openRocksDB(path, name string, blockCacheMB int) (*DBHandle, dbOpenTiming) {
	timing := dbOpenTiming{}
	start := time.Now()

	// Create options
	t1 := time.Now()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	// Block-based table options with cache and bloom filter
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	if blockCacheMB > 0 {
		cache := grocksdb.NewLRUCache(uint64(blockCacheMB) * 1024 * 1024)
		bbto.SetBlockCache(cache)
	}
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10))
	opts.SetBlockBasedTableFactory(bbto)
	timing.optsCreate = time.Since(t1)

	// Open database (read-only)
	t2 := time.Now()
	db, err := grocksdb.OpenDbForReadOnly(opts, path, false)
	if err != nil {
		log.Fatalf("Failed to open %s at %s: %v", name, path, err)
	}
	timing.dbOpen = time.Since(t2)

	// Create read options
	t3 := time.Now()
	ro := grocksdb.NewDefaultReadOptions()
	timing.roCreate = time.Since(t3)

	timing.total = time.Since(start)

	return &DBHandle{DB: db, Opts: opts, RO: ro}, timing
}

// =============================================================================
// printDBStats prints RocksDB statistics
// =============================================================================

func printDBStats(db *grocksdb.DB, name string) {
	fmt.Printf("%s Statistics:\n", name)
	fmt.Printf("  Estimated keys:   %s\n", db.GetProperty("rocksdb.estimate-num-keys"))
	fmt.Printf("  Total SST size:   %s\n", formatBytes(db.GetProperty("rocksdb.total-sst-files-size")))
	fmt.Printf("  Live SST size:    %s\n", formatBytes(db.GetProperty("rocksdb.live-sst-files-size")))

	fmt.Printf("  Files by level:   ")
	for i := 0; i < 7; i++ {
		files := db.GetProperty(fmt.Sprintf("rocksdb.num-files-at-level%d", i))
		if i > 0 {
			fmt.Printf(" ")
		}
		fmt.Printf("L%d=%s", i, files)
	}
	fmt.Printf("\n")
}

func formatBytes(s string) string {
	var bytes int64
	fmt.Sscanf(s, "%d", &bytes)
	return helpers.FormatBytes(bytes)
}

// =============================================================================
// runQuery executes the two-step lookup
// =============================================================================

func runQuery(db1, db3 *DBHandle, decoder *zstd.Decoder, txHashHex string) (QueryTiming, *QueryResult, error) {
	timing := QueryTiming{}
	totalStart := time.Now()

	// -------------------------------------------------------------------------
	// Parse transaction hash
	// -------------------------------------------------------------------------
	txHashBytes, err := helpers.HexStringToBytes(txHashHex)
	if err != nil {
		return timing, nil, fmt.Errorf("invalid hex: %s - %v", txHashHex, err)
	}

	// -------------------------------------------------------------------------
	// Step 1: Query DB3 (txHash -> ledgerSeq)
	// -------------------------------------------------------------------------
	t1 := time.Now()
	slice3, err := db3.DB.Get(db3.RO, txHashBytes)
	timing.DB3Query = time.Since(t1)

	if err != nil {
		return timing, nil, fmt.Errorf("DB3 query error: %v", err)
	}
	defer slice3.Free()

	if !slice3.Exists() {
		timing.Total = time.Since(totalStart)
		return timing, nil, fmt.Errorf("txHash not found in DB3: %s", txHashHex)
	}

	// Parse ledger sequence (4-byte big-endian)
	ledgerSeq := helpers.BytesToUint32(slice3.Data())

	// -------------------------------------------------------------------------
	// Step 2: Query DB1 (ledgerSeq -> compressed LCM)
	// -------------------------------------------------------------------------
	ledgerKey := helpers.Uint32ToBytes(ledgerSeq)

	t2 := time.Now()
	slice1, err := db1.DB.Get(db1.RO, ledgerKey)
	timing.DB1Query = time.Since(t2)

	if err != nil {
		return timing, nil, fmt.Errorf("DB1 query error: %v", err)
	}
	defer slice1.Free()

	if !slice1.Exists() {
		timing.Total = time.Since(totalStart)
		return timing, nil, fmt.Errorf("ledger %d not found in DB1", ledgerSeq)
	}

	// Copy compressed data
	compressedLCM := make([]byte, len(slice1.Data()))
	copy(compressedLCM, slice1.Data())

	// -------------------------------------------------------------------------
	// Step 3: Decompress LCM
	// -------------------------------------------------------------------------
	t3 := time.Now()
	uncompressedLCM, err := decoder.DecodeAll(compressedLCM, nil)
	timing.Decompress = time.Since(t3)

	if err != nil {
		return timing, nil, fmt.Errorf("decompress failed: %v", err)
	}

	// -------------------------------------------------------------------------
	// Step 4: Unmarshal XDR to LedgerCloseMeta
	// -------------------------------------------------------------------------
	t4 := time.Now()
	var lcm xdr.LedgerCloseMeta
	err = lcm.UnmarshalBinary(uncompressedLCM)
	timing.Unmarshal = time.Since(t4)

	if err != nil {
		return timing, nil, fmt.Errorf("unmarshal LCM failed: %v", err)
	}

	// -------------------------------------------------------------------------
	// Step 5: Create transaction reader and search for matching tx
	// -------------------------------------------------------------------------
	t5 := time.Now()
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		network.PublicNetworkPassphrase, lcm)
	timing.TxReaderCreate = time.Since(t5)

	if err != nil {
		return timing, nil, fmt.Errorf("failed to create tx reader: %v", err)
	}
	defer txReader.Close()

	// Search for matching transaction
	t6 := time.Now()
	var foundTx *ingest.LedgerTransaction
	txCount := 0

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return timing, nil, fmt.Errorf("error reading transaction: %v", err)
		}
		txCount++

		// Compare hash
		if tx.Hash.HexString() == txHashHex {
			foundTx = &tx
			break
		}
	}
	timing.TxSearch = time.Since(t6)
	timing.TxCount = txCount

	if foundTx == nil {
		timing.Total = time.Since(totalStart)
		return timing, nil, fmt.Errorf("txHash %s not found in ledger %d (scanned %d transactions)",
			txHashHex, ledgerSeq, txCount)
	}

	// -------------------------------------------------------------------------
	// Step 6: Extract transaction data
	// -------------------------------------------------------------------------
	t7 := time.Now()

	envelopeBytes, err := foundTx.Envelope.MarshalBinary()
	if err != nil {
		return timing, nil, fmt.Errorf("failed to marshal envelope: %v", err)
	}

	resultBytes, err := foundTx.Result.MarshalBinary()
	if err != nil {
		return timing, nil, fmt.Errorf("failed to marshal result: %v", err)
	}

	metaBytes, err := foundTx.UnsafeMeta.MarshalBinary()
	if err != nil {
		return timing, nil, fmt.Errorf("failed to marshal meta: %v", err)
	}

	timing.TxExtract = time.Since(t7)
	timing.Total = time.Since(totalStart)

	// Build result
	result := &QueryResult{
		TxHash:              txHashHex,
		LedgerSeq:           ledgerSeq,
		TxIndex:             foundTx.Index,
		ClosedAt:            lcm.ClosedAt(),
		EnvelopeBytes:       envelopeBytes,
		ResultBytes:         resultBytes,
		MetaBytes:           metaBytes,
		CompressedLCMSize:   len(compressedLCM),
		UncompressedLCMSize: len(uncompressedLCM),
	}

	return timing, result, nil
}

// =============================================================================
// printQueryResult prints the result of a query
// =============================================================================

func printQueryResult(timing QueryTiming, result *QueryResult, quiet bool) {
	if quiet {
		// Compact output for benchmarking
		fmt.Printf("TX: %s\n", result.TxHash)
		fmt.Printf("Ledger: %d | TxIndex: %d | TxScanned: %d\n",
			result.LedgerSeq, result.TxIndex, timing.TxCount)
		fmt.Printf("LCM Size: %s compressed -> %s uncompressed (%.1f%% ratio)\n",
			helpers.FormatBytes(int64(result.CompressedLCMSize)),
			helpers.FormatBytes(int64(result.UncompressedLCMSize)),
			100*(1-float64(result.CompressedLCMSize)/float64(result.UncompressedLCMSize)))
		fmt.Printf("\n")
		fmt.Printf("Timing Breakdown:\n")
		fmt.Printf("  DB3 Query (hash->seq):     %s\n", helpers.FormatDuration(timing.DB3Query))
		fmt.Printf("  DB1 Query (seq->LCM):      %s\n", helpers.FormatDuration(timing.DB1Query))
		fmt.Printf("  Decompress (zstd):         %s\n", helpers.FormatDuration(timing.Decompress))
		fmt.Printf("  Unmarshal (XDR):           %s\n", helpers.FormatDuration(timing.Unmarshal))
		fmt.Printf("  TxReader Create:           %s\n", helpers.FormatDuration(timing.TxReaderCreate))
		fmt.Printf("  TxSearch (%d txs):         %s\n", timing.TxCount, helpers.FormatDuration(timing.TxSearch))
		fmt.Printf("  TxExtract (marshal):       %s\n", helpers.FormatDuration(timing.TxExtract))
		fmt.Printf("  ----------------------------------------\n")
		fmt.Printf("  TOTAL:                     %s\n", helpers.FormatDuration(timing.Total))
		fmt.Printf("\n")
		fmt.Printf("================================================================================\n\n")
		return
	}

	// Full verbose output
	fmt.Printf("\n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("Query Result\n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("\n")
	fmt.Printf("Transaction Hash:     %s\n", result.TxHash)
	fmt.Printf("Ledger Sequence:      %d\n", result.LedgerSeq)
	fmt.Printf("Transaction Index:    %d\n", result.TxIndex)
	fmt.Printf("Closed At:            %v\n", result.ClosedAt)
	fmt.Printf("\n")

	fmt.Printf("LCM Sizes:\n")
	fmt.Printf("  Compressed:         %s\n", helpers.FormatBytes(int64(result.CompressedLCMSize)))
	fmt.Printf("  Uncompressed:       %s\n", helpers.FormatBytes(int64(result.UncompressedLCMSize)))
	fmt.Printf("  Compression Ratio:  %.2f%%\n",
		100*(1-float64(result.CompressedLCMSize)/float64(result.UncompressedLCMSize)))
	fmt.Printf("\n")

	fmt.Printf("Transaction Data Sizes:\n")
	fmt.Printf("  Envelope:           %s\n", helpers.FormatBytes(int64(len(result.EnvelopeBytes))))
	fmt.Printf("  Result:             %s\n", helpers.FormatBytes(int64(len(result.ResultBytes))))
	fmt.Printf("  Meta:               %s\n", helpers.FormatBytes(int64(len(result.MetaBytes))))
	fmt.Printf("  Total:              %s\n", helpers.FormatBytes(int64(
		len(result.EnvelopeBytes)+len(result.ResultBytes)+len(result.MetaBytes))))
	fmt.Printf("\n")

	fmt.Printf("================================================================================\n")
	fmt.Printf("Timing Breakdown\n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("\n")
	fmt.Printf("Step 1: DB3 Query (txHash -> ledgerSeq)\n")
	fmt.Printf("        Time: %s\n", helpers.FormatDuration(timing.DB3Query))
	fmt.Printf("\n")
	fmt.Printf("Step 2: DB1 Query (ledgerSeq -> compressed LCM)\n")
	fmt.Printf("        Time: %s\n", helpers.FormatDuration(timing.DB1Query))
	fmt.Printf("\n")
	fmt.Printf("Step 3: Decompress LCM (zstd)\n")
	fmt.Printf("        Time: %s\n", helpers.FormatDuration(timing.Decompress))
	fmt.Printf("\n")
	fmt.Printf("Step 4: Unmarshal LCM (XDR)\n")
	fmt.Printf("        Time: %s\n", helpers.FormatDuration(timing.Unmarshal))
	fmt.Printf("\n")
	fmt.Printf("Step 5: Search for Transaction\n")
	fmt.Printf("        TxReader Create: %s\n", helpers.FormatDuration(timing.TxReaderCreate))
	fmt.Printf("        Search (%d txs): %s\n", timing.TxCount, helpers.FormatDuration(timing.TxSearch))
	fmt.Printf("\n")
	fmt.Printf("Step 6: Extract Transaction Data\n")
	fmt.Printf("        Time: %s\n", helpers.FormatDuration(timing.TxExtract))
	fmt.Printf("\n")
	fmt.Printf("--------------------------------------------------------------------------------\n")
	fmt.Printf("TOTAL QUERY TIME: %s\n", helpers.FormatDuration(timing.Total))
	fmt.Printf("================================================================================\n")
	fmt.Printf("\n")

	// Print hex-encoded transaction data
	fmt.Printf("================================================================================\n")
	fmt.Printf("Transaction Data (Hex Encoded)\n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("\n")
	fmt.Printf("TxEnvelope:\n%s\n\n", helpers.WrapText(hex.EncodeToString(result.EnvelopeBytes), 80))
	fmt.Printf("TxResult:\n%s\n\n", helpers.WrapText(hex.EncodeToString(result.ResultBytes), 80))
	fmt.Printf("TxMeta:\n%s\n\n", helpers.WrapText(hex.EncodeToString(result.MetaBytes), 80))
	fmt.Printf("================================================================================\n\n")
}

// =============================================================================
// printFinalStats prints cumulative statistics for ongoing mode
// =============================================================================

func printFinalStats(stats RunningStats) {
	if stats.QueryCount == 0 {
		fmt.Println("No queries executed.")
		return
	}

	fmt.Printf("\n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("Cumulative Statistics (%d queries)\n", stats.QueryCount)
	fmt.Printf("================================================================================\n")
	fmt.Printf("\n")

	fmt.Printf("Total Time Breakdown:\n")
	fmt.Printf("  DB3 Queries:        %s\n", helpers.FormatDuration(stats.TotalDB3Query))
	fmt.Printf("  DB1 Queries:        %s\n", helpers.FormatDuration(stats.TotalDB1Query))
	fmt.Printf("  Decompress:         %s\n", helpers.FormatDuration(stats.TotalDecompress))
	fmt.Printf("  Unmarshal:          %s\n", helpers.FormatDuration(stats.TotalUnmarshal))
	fmt.Printf("  Tx Search:          %s\n", helpers.FormatDuration(stats.TotalTxSearch))
	fmt.Printf("  Tx Extract:         %s\n", helpers.FormatDuration(stats.TotalTxExtract))
	fmt.Printf("  ----------------------------------------\n")
	fmt.Printf("  TOTAL:              %s\n", helpers.FormatDuration(stats.TotalTime))
	fmt.Printf("\n")

	fmt.Printf("Average Per Query:\n")
	n := time.Duration(stats.QueryCount)
	fmt.Printf("  DB3 Query:          %s\n", helpers.FormatDuration(stats.TotalDB3Query/n))
	fmt.Printf("  DB1 Query:          %s\n", helpers.FormatDuration(stats.TotalDB1Query/n))
	fmt.Printf("  Decompress:         %s\n", helpers.FormatDuration(stats.TotalDecompress/n))
	fmt.Printf("  Unmarshal:          %s\n", helpers.FormatDuration(stats.TotalUnmarshal/n))
	fmt.Printf("  Tx Search:          %s\n", helpers.FormatDuration(stats.TotalTxSearch/n))
	fmt.Printf("  Tx Extract:         %s\n", helpers.FormatDuration(stats.TotalTxExtract/n))
	fmt.Printf("  ----------------------------------------\n")
	fmt.Printf("  TOTAL:              %s\n", helpers.FormatDuration(stats.TotalTime/n))
	fmt.Printf("\n")

	avgTxScanned := float64(stats.TotalTxScanned) / float64(stats.QueryCount)
	fmt.Printf("Average Transactions Scanned: %.1f\n", avgTxScanned)
	fmt.Printf("================================================================================\n\n")
}
