package main

// Filename: rocksdb_tx_lookup.go
//
// A tool to query transactions from a RocksDB database (txHash -> compressed TxData).
// Provides detailed timing breakdown separating DB open time from query time.
//
// Usage:
//
//   Single query:
//     go run rocksdb_tx_lookup.go --db /path/to/db2 --tx abc123def456...
//
//   Ongoing stdin mode (for multiple queries without reopening DB):
//     go run rocksdb_tx_lookup.go --db /path/to/db2 --ongoing
//     (then type transaction hashes, one per line, or pipe them in)
//
//   Quiet mode (metrics only, no full tx data output):
//     go run rocksdb_tx_lookup.go --db /path/to/db2 --ongoing --quiet
//
//   Batch mode from file:
//     cat tx_hashes.txt | go run rocksdb_tx_lookup.go --db /path/to/db2 --ongoing --quiet
//
// Examples:
//
//   # Query a single transaction
//   ./rocksdb_tx_lookup --db /data/tx/2024_01 --tx 4f3c8a7b2e1d...
//
//   # Interactive mode - query multiple transactions
//   ./rocksdb_tx_lookup --db /data/tx/2024_01 --ongoing
//   > 4f3c8a7b2e1d...
//   > 9a8b7c6d5e4f...
//   > STOP
//
//   # Benchmark mode - pipe hashes and get timing only
//   cat sample_hashes.txt | ./rocksdb_tx_lookup --db /data/tx/2024_01 --ongoing --quiet

import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/tx_data"
	"github.com/klauspost/compress/zstd"
	"github.com/linxGnu/grocksdb"
	"google.golang.org/protobuf/proto"
)

// RocksDBStats holds database statistics for reporting
type RocksDBStats struct {
	EstimatedKeys    string
	TotalSSTSize     string
	LiveSSTSize      string
	MemtableSize     string
	BlockCacheUsage  string
	NumFilesAtLevel  [7]string // L0-L6
	BackgroundErrors string
}

func main() {
	// =========================================================================
	// Parse command-line flags
	// =========================================================================
	var dbPath, txHashHex string
	var ongoing bool
	var quiet bool
	var blockCacheSizeMB int

	flag.StringVar(&dbPath, "db", "", "Path to RocksDB database (txHash->txData)")
	flag.StringVar(&txHashHex, "tx", "", "Transaction hash in hex (omit when using --ongoing)")
	flag.BoolVar(&ongoing, "ongoing", false, "Read transaction hashes from stdin continuously")
	flag.BoolVar(&quiet, "quiet", false, "Print only metrics instead of full tx data output")
	flag.IntVar(&blockCacheSizeMB, "block-cache-mb", 512, "Block cache size in MB for read performance")

	flag.Parse()

	// =========================================================================
	// Validate arguments
	// =========================================================================
	if dbPath == "" {
		log.Fatalf("Missing required --db path")
	}
	if ongoing && txHashHex != "" {
		log.Fatalf("--tx cannot be used when --ongoing is provided")
	}
	if !ongoing && txHashHex == "" {
		log.Fatalf("Provide --tx OR --ongoing")
	}

	// Check DB exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		log.Fatalf("Error: DB directory does not exist: %s", dbPath)
	}

	// =========================================================================
	// Open RocksDB with timing
	// =========================================================================

	// --- Create options ---
	t1 := time.Now()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false) // DB must exist

	// Configure block cache for read performance
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	if blockCacheSizeMB > 0 {
		cache := grocksdb.NewLRUCache(uint64(blockCacheSizeMB) * 1024 * 1024)
		bbto.SetBlockCache(cache)
	}
	// Enable bloom filter for faster negative lookups
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10))
	opts.SetBlockBasedTableFactory(bbto)

	optsCreateTime := time.Since(t1)

	// --- Open database ---
	t2 := time.Now()
	db, err := grocksdb.OpenDbForReadOnly(opts, dbPath, false)
	if err != nil {
		log.Fatalf("Failed to open RocksDB: %v", err)
	}
	defer db.Close()
	defer opts.Destroy()
	dbOpenTime := time.Since(t2)

	// --- Create read options ---
	t3 := time.Now()
	ro := grocksdb.NewDefaultReadOptions()
	// Disable fill cache if doing bulk reads to avoid cache thrashing
	// ro.SetFillCache(false) // Uncomment for bulk benchmarking
	defer ro.Destroy()
	readOptsTime := time.Since(t3)

	totalOpenTime := optsCreateTime + dbOpenTime + readOptsTime

	// =========================================================================
	// Gather database statistics
	// =========================================================================
	stats := getRocksDBStats(db)

	// =========================================================================
	// Print initialization info (once)
	// =========================================================================
	fmt.Printf("\n=== RocksDB Initialization ===\n")
	fmt.Printf("Options create:   %s\n", helpers.FormatDuration(optsCreateTime))
	fmt.Printf("DB open:          %s\n", helpers.FormatDuration(dbOpenTime))
	fmt.Printf("ReadOpts create:  %s\n", helpers.FormatDuration(readOptsTime))
	fmt.Printf("Total open:       %s\n", helpers.FormatDuration(totalOpenTime))
	fmt.Printf("Block cache:      %d MB\n\n", blockCacheSizeMB)

	// Print DB stats
	fmt.Printf("=== RocksDB Database Stats ===\n")
	fmt.Printf("Estimated keys:   %s\n", stats.EstimatedKeys)
	fmt.Printf("Total SST size:   %s\n", stats.TotalSSTSize)
	fmt.Printf("Live SST size:    %s\n", stats.LiveSSTSize)
	fmt.Printf("Memtable size:    %s\n", stats.MemtableSize)
	fmt.Printf("Files by level:   L0=%s L1=%s L2=%s L3=%s L4=%s L5=%s L6=%s\n",
		stats.NumFilesAtLevel[0], stats.NumFilesAtLevel[1], stats.NumFilesAtLevel[2],
		stats.NumFilesAtLevel[3], stats.NumFilesAtLevel[4], stats.NumFilesAtLevel[5],
		stats.NumFilesAtLevel[6])
	fmt.Printf("\n")

	// =========================================================================
	// Create reusable zstd decoder (amortize initialization cost)
	// =========================================================================
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		log.Fatalf("Failed to create zstd decoder: %v", err)
	}
	defer decoder.Close()

	// =========================================================================
	// ONGOING MODE - read hashes from stdin
	// =========================================================================
	if ongoing {
		reader := bufio.NewScanner(os.Stdin)

		fmt.Println("Ongoing mode: enter tx hashes (or STOP to quit):")
		for reader.Scan() {
			line := strings.TrimSpace(reader.Text())
			if line == "" {
				continue
			}
			if strings.EqualFold(line, "STOP") {
				fmt.Println("Stopping.")
				return
			}

			runQuery(db, ro, decoder, stats, line, quiet)
		}
		return
	}

	// =========================================================================
	// SINGLE QUERY MODE
	// =========================================================================
	runQuery(db, ro, decoder, stats, txHashHex, quiet)
}

// =============================================================================
// getRocksDBStats retrieves various statistics from RocksDB
// =============================================================================
func getRocksDBStats(db *grocksdb.DB) RocksDBStats {
	stats := RocksDBStats{}

	// Key counts and sizes
	stats.EstimatedKeys = db.GetProperty("rocksdb.estimate-num-keys")
	stats.TotalSSTSize = formatBytesProperty(db.GetProperty("rocksdb.total-sst-files-size"))
	stats.LiveSSTSize = formatBytesProperty(db.GetProperty("rocksdb.live-sst-files-size"))
	stats.MemtableSize = formatBytesProperty(db.GetProperty("rocksdb.cur-size-all-mem-tables"))
	stats.BackgroundErrors = db.GetProperty("rocksdb.background-errors")

	// Files per level
	for i := 0; i < 7; i++ {
		stats.NumFilesAtLevel[i] = db.GetProperty(fmt.Sprintf("rocksdb.num-files-at-level%d", i))
	}

	return stats
}

// =============================================================================
// formatBytesProperty converts a string byte count to human-readable format
// =============================================================================
func formatBytesProperty(s string) string {
	var bytes int64
	fmt.Sscanf(s, "%d", &bytes)
	return helpers.FormatBytes(bytes)
}

// =============================================================================
// runQuery executes a single transaction lookup with detailed timing
// =============================================================================
func runQuery(
	db *grocksdb.DB,
	ro *grocksdb.ReadOptions,
	decoder *zstd.Decoder,
	stats RocksDBStats,
	txHashHex string,
	quiet bool,
) {
	// -------------------------------------------------------------------------
	// Parse the transaction hash from hex to bytes
	// -------------------------------------------------------------------------
	keyBytes, err := helpers.HexStringToBytes(txHashHex)
	if err != nil {
		fmt.Printf("Invalid hex: %s - %v\n", txHashHex, err)
		return
	}

	// -------------------------------------------------------------------------
	// Query RocksDB
	// -------------------------------------------------------------------------
	queryStart := time.Now()
	slice, err := db.Get(ro, keyBytes)
	queryTime := time.Since(queryStart)

	if err != nil {
		fmt.Printf("DB error for %s: %v\n", txHashHex, err)
		return
	}
	defer slice.Free()

	// Check if key exists
	if !slice.Exists() {
		fmt.Printf("TX %s not found.\n", txHashHex)
		return
	}

	// Copy data (slice is freed when we return)
	data := make([]byte, len(slice.Data()))
	copy(data, slice.Data())

	// -------------------------------------------------------------------------
	// Decompress with zstd
	// -------------------------------------------------------------------------
	decStart := time.Now()
	decompressed, err := decoder.DecodeAll(data, nil)
	if err != nil {
		fmt.Printf("Decompress failed for %s: %v\n", txHashHex, err)
		return
	}
	decTime := time.Since(decStart)

	// -------------------------------------------------------------------------
	// Unmarshal protobuf
	// -------------------------------------------------------------------------
	unmarshalStart := time.Now()
	var txData tx_data.TxData
	if err := proto.Unmarshal(decompressed, &txData); err != nil {
		fmt.Printf("Unmarshal failed for %s: %v\n", txHashHex, err)
		return
	}
	unmarshalTime := time.Since(unmarshalStart)

	// -------------------------------------------------------------------------
	// Calculate totals
	// -------------------------------------------------------------------------
	totalTime := queryTime + decTime + unmarshalTime

	// -------------------------------------------------------------------------
	// QUIET MODE OUTPUT - just metrics
	// -------------------------------------------------------------------------
	if quiet {
		fmt.Printf("TX: %s\n", txHashHex)
		fmt.Printf("Compressed:         %s\n", helpers.FormatBytes(int64(len(data))))
		fmt.Printf("Uncompressed:       %s\n", helpers.FormatBytes(int64(len(decompressed))))
		fmt.Printf("Compression Ratio:  %.2f%%\n\n", 100*(1-float64(len(data))/float64(len(decompressed))))

		fmt.Printf("Query (RocksDB):    %s\n", helpers.FormatDuration(queryTime))
		fmt.Printf("Decompress (zstd):  %s\n", helpers.FormatDuration(decTime))
		fmt.Printf("Unmarshal (proto):  %s\n", helpers.FormatDuration(unmarshalTime))
		fmt.Printf("Total:              %s\n", helpers.FormatDuration(totalTime))
		fmt.Printf("----------------------------------------\n\n")
		return
	}

	// -------------------------------------------------------------------------
	// FULL VERBOSE MODE - all details
	// -------------------------------------------------------------------------

	// Encode components as base64 for display
	txEnvelopeBase64 := base64.StdEncoding.EncodeToString(txData.TxEnvelope)
	txResultBase64 := base64.StdEncoding.EncodeToString(txData.TxResult)
	txMetaBase64 := base64.StdEncoding.EncodeToString(txData.TxMeta)

	fmt.Printf("\n========================================\n")
	fmt.Printf("RocksDB Transaction Lookup\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Transaction Hash: %s\n", txHashHex)
	fmt.Printf("========================================\n")

	fmt.Printf("\nTransaction Details:\n")
	fmt.Printf("  Ledger sequence:    %d\n", txData.LedgerSequence)
	fmt.Printf("  Transaction index:  %d\n", txData.Index)
	fmt.Printf("  Closed at:          %v\n", txData.ClosedAt.AsTime())

	fmt.Printf("\nData Sizes:\n")
	fmt.Printf("  Compressed:         %s\n", helpers.FormatBytes(int64(len(data))))
	fmt.Printf("  Uncompressed:       %s\n", helpers.FormatBytes(int64(len(decompressed))))
	fmt.Printf("  Compression ratio:  %.2f%%\n", 100*(1-float64(len(data))/float64(len(decompressed))))

	fmt.Printf("\nComponent Sizes:\n")
	fmt.Printf("  Envelope:           %s\n", helpers.FormatBytes(int64(len(txData.TxEnvelope))))
	fmt.Printf("  Result:             %s\n", helpers.FormatBytes(int64(len(txData.TxResult))))
	fmt.Printf("  Meta:               %s\n", helpers.FormatBytes(int64(len(txData.TxMeta))))

	fmt.Printf("\n========================================\n")
	fmt.Printf("RocksDB Database Statistics\n")
	fmt.Printf("========================================\n")
	fmt.Printf("  Estimated keys:     %s\n", stats.EstimatedKeys)
	fmt.Printf("  Total SST size:     %s\n", stats.TotalSSTSize)
	fmt.Printf("  Live SST size:      %s\n", stats.LiveSSTSize)
	fmt.Printf("  Memtable size:      %s\n", stats.MemtableSize)
	fmt.Printf("  Files by level:     L0=%s L1=%s L2=%s L3=%s L4=%s L5=%s L6=%s\n",
		stats.NumFilesAtLevel[0], stats.NumFilesAtLevel[1], stats.NumFilesAtLevel[2],
		stats.NumFilesAtLevel[3], stats.NumFilesAtLevel[4], stats.NumFilesAtLevel[5],
		stats.NumFilesAtLevel[6])

	fmt.Printf("\n========================================\n")
	fmt.Printf("Query Performance Metrics\n")
	fmt.Printf("========================================\n")
	fmt.Printf("  Query time:         %s\n", helpers.FormatDuration(queryTime))
	fmt.Printf("  Decompress time:    %s\n", helpers.FormatDuration(decTime))
	fmt.Printf("  Unmarshal time:     %s\n", helpers.FormatDuration(unmarshalTime))
	fmt.Printf("  Total time:         %s\n", helpers.FormatDuration(totalTime))

	fmt.Printf("\n========================================\n")
	fmt.Printf("Transaction Data (Base64 Encoded)\n")
	fmt.Printf("========================================\n")
	fmt.Printf("\nTxEnvelope (base64):\n%s\n", helpers.WrapText(txEnvelopeBase64, 80))
	fmt.Printf("\nTxResult (base64):\n%s\n", helpers.WrapText(txResultBase64, 80))
	fmt.Printf("\nTxMeta (base64):\n%s\n", helpers.WrapText(txMetaBase64, 80))
	fmt.Printf("========================================\n\n")
}
