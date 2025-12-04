package main

// Filename: recsplit_two_step_lookup.go
//
// A tool to query transactions using RecSplit for the first step:
//   1. Look up txHash in RecSplit to get array index (O(1), ~100-200ns)
//   2. Look up index in values array to get ledgerSequence
//   3. Look up ledgerSequence in RocksDB DB1 to get compressed LedgerCloseMeta
//   4. Decompress and search for matching transaction
//
// RecSplit advantages over BBHash:
//   - Native []byte key support (no collision risk from truncation!)
//   - ~1.8-2 bits per key (vs BBHash's 3-4 bits)
//   - Production-tested in Ethereum's Erigon client
//
// Memory Usage:
//   - RecSplit index: mmap'd (~375 MB for 1.5B keys)
//   - Values array: ~6 GB for 1.5B keys (can be mmap'd)
//   - Total: ~6.4 GB
//
// Usage:
//   ./recsplit_two_step_lookup \
//     --db1 /path/to/db1 \
//     --recsplit-dir /path/to/recsplit \
//     --tx abc123...
//
//   ./recsplit_two_step_lookup \
//     --db1 /path/to/db1 \
//     --recsplit-dir /path/to/recsplit \
//     --ongoing --quiet

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/klauspost/compress/zstd"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/linxGnu/grocksdb"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/xdr"
)

// =============================================================================
// RecSplit Index with Values
// =============================================================================

type RecSplitIndex struct {
	Index      *recsplit.Index
	Reader     *recsplit.IndexReader
	Values     []uint32 // ledgerSeq values
	NumEntries uint64

	// Mmap fields
	useMmap    bool
	mmapData   []byte
	mmapValues []uint32
	mmapFile   *os.File
}

// LoadRecSplitIndex loads the RecSplit index and values into RAM
func LoadRecSplitIndex(dir string) (*RecSplitIndex, time.Duration, error) {
	start := time.Now()

	// Open RecSplit index (uses mmap internally)
	idx, err := recsplit.OpenIndex(filepath.Join(dir, "txhash.idx"))
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open RecSplit index: %v", err)
	}

	// Create reader for thread-safe lookups
	reader := recsplit.NewIndexReader(idx)

	// Load values
	valuesPath := filepath.Join(dir, "values.bin")
	valuesFile, err := os.Open(valuesPath)
	if err != nil {
		idx.Close()
		return nil, 0, fmt.Errorf("failed to open values file: %v", err)
	}
	defer valuesFile.Close()

	var numEntries uint64
	if err := binary.Read(valuesFile, binary.LittleEndian, &numEntries); err != nil {
		idx.Close()
		return nil, 0, fmt.Errorf("failed to read values header: %v", err)
	}

	values := make([]uint32, numEntries)
	if err := binary.Read(valuesFile, binary.LittleEndian, values); err != nil && err != io.EOF {
		idx.Close()
		return nil, 0, fmt.Errorf("failed to read values: %v", err)
	}

	return &RecSplitIndex{
		Index:      idx,
		Reader:     reader,
		Values:     values,
		NumEntries: numEntries,
		useMmap:    false,
	}, time.Since(start), nil
}

// LoadRecSplitIndexMmap loads RecSplit index and mmap's the values
func LoadRecSplitIndexMmap(dir string) (*RecSplitIndex, time.Duration, error) {
	start := time.Now()

	// Open RecSplit index (uses mmap internally)
	idx, err := recsplit.OpenIndex(filepath.Join(dir, "txhash.idx"))
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open RecSplit index: %v", err)
	}

	// Create reader for thread-safe lookups
	reader := recsplit.NewIndexReader(idx)

	// Mmap values file
	valuesPath := filepath.Join(dir, "values.bin")
	file, err := os.Open(valuesPath)
	if err != nil {
		idx.Close()
		return nil, 0, fmt.Errorf("failed to open values file: %v", err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		idx.Close()
		return nil, 0, fmt.Errorf("failed to stat values file: %v", err)
	}

	// Mmap the entire file
	data, err := syscall.Mmap(
		int(file.Fd()),
		0,
		int(fileInfo.Size()),
		syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	if err != nil {
		file.Close()
		idx.Close()
		return nil, 0, fmt.Errorf("failed to mmap values file: %v", err)
	}

	// Read header
	numEntries := binary.LittleEndian.Uint64(data[:8])

	// Create slice pointing to values (skip 8-byte header)
	valuesPtr := (*uint32)(unsafe.Pointer(&data[8]))
	values := unsafe.Slice(valuesPtr, numEntries)

	return &RecSplitIndex{
		Index:      idx,
		Reader:     reader,
		NumEntries: numEntries,
		useMmap:    true,
		mmapData:   data,
		mmapValues: values,
		mmapFile:   file,
	}, time.Since(start), nil
}

// Close releases resources
func (r *RecSplitIndex) Close() {
	if r.Reader != nil {
		r.Reader.Close()
	}
	if r.Index != nil {
		r.Index.Close()
	}
	if r.useMmap {
		if r.mmapData != nil {
			syscall.Munmap(r.mmapData)
		}
		if r.mmapFile != nil {
			r.mmapFile.Close()
		}
	}
}

// Lookup returns the ledgerSeq for a txHash
func (r *RecSplitIndex) Lookup(txHash []byte) (uint32, bool) {
	// RecSplit lookup - returns array index
	idx := r.Reader.Lookup(txHash)

	if idx >= r.NumEntries {
		return 0, false
	}

	if r.useMmap {
		return r.mmapValues[idx], true
	}
	return r.Values[idx], true
}

// GetMemoryMode returns a string describing the memory mode
func (r *RecSplitIndex) GetMemoryMode() string {
	if r.useMmap {
		return "mmap (OS-managed)"
	}
	return "RAM (fully loaded)"
}

// =============================================================================
// Timing structs
// =============================================================================

type OpenTiming struct {
	RecSplitLoad  time.Duration
	DB1OptsCreate time.Duration
	DB1Open       time.Duration
	DB1ROCreate   time.Duration
	DB1Total      time.Duration
	DecoderCreate time.Duration
	GrandTotal    time.Duration
}

type QueryTiming struct {
	RecSplitLookup time.Duration
	DB1Query       time.Duration
	Decompress     time.Duration
	Unmarshal      time.Duration
	TxReaderCreate time.Duration
	TxSearch       time.Duration
	TxCount        int
	TxExtract      time.Duration
	Total          time.Duration
}

type QueryResult struct {
	TxHash              string
	LedgerSeq           uint32
	TxIndex             uint32
	ClosedAt            time.Time
	EnvelopeBytes       []byte
	ResultBytes         []byte
	MetaBytes           []byte
	CompressedLCMSize   int
	UncompressedLCMSize int
}

type RunningStats struct {
	QueryCount       int
	TotalRecSplit    time.Duration
	TotalDB1Query    time.Duration
	TotalDecompress  time.Duration
	TotalUnmarshal   time.Duration
	TotalTxSearch    time.Duration
	TotalTxExtract   time.Duration
	TotalTime        time.Duration
	TotalTxScanned   int
	NotFoundCount    int
	NotFoundRecSplit int
	NotFoundDB1      int
	NotFoundInLedger int
}

// =============================================================================
// Main
// =============================================================================

func main() {
	var db1Path, recsplitDir, txHashHex string
	var ongoing, quiet, useMmap bool
	var blockCacheSizeMB int

	flag.StringVar(&db1Path, "db1", "", "Path to RocksDB DB1 (ledgerSeq -> compressed LCM)")
	flag.StringVar(&recsplitDir, "recsplit-dir", "", "Directory containing txhash.idx and values.bin")
	flag.StringVar(&txHashHex, "tx", "", "Transaction hash in hex")
	flag.BoolVar(&ongoing, "ongoing", false, "Read transaction hashes from stdin continuously")
	flag.BoolVar(&quiet, "quiet", false, "Print only metrics")
	flag.BoolVar(&useMmap, "mmap", false, "Use mmap for values array")
	flag.IntVar(&blockCacheSizeMB, "block-cache-mb", 512, "Block cache size in MB for DB1")

	flag.Parse()

	if db1Path == "" || recsplitDir == "" {
		log.Fatalf("Both --db1 and --recsplit-dir are required")
	}
	if ongoing && txHashHex != "" {
		log.Fatalf("--tx cannot be used with --ongoing")
	}
	if !ongoing && txHashHex == "" {
		log.Fatalf("Provide --tx OR --ongoing")
	}

	// =========================================================================
	// Load RecSplit and open RocksDB
	// =========================================================================
	openTiming := OpenTiming{}
	grandStart := time.Now()

	// Load RecSplit
	var rsIdx *RecSplitIndex
	var rsLoadTime time.Duration
	var err error

	if useMmap {
		fmt.Println("Loading RecSplit index with mmap...")
		rsIdx, rsLoadTime, err = LoadRecSplitIndexMmap(recsplitDir)
	} else {
		fmt.Println("Loading RecSplit index into RAM...")
		rsIdx, rsLoadTime, err = LoadRecSplitIndex(recsplitDir)
	}
	if err != nil {
		log.Fatalf("Failed to load RecSplit: %v", err)
	}
	defer rsIdx.Close()
	openTiming.RecSplitLoad = rsLoadTime

	// Open DB1
	t2 := time.Now()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	if blockCacheSizeMB > 0 {
		cache := grocksdb.NewLRUCache(uint64(blockCacheSizeMB) * 1024 * 1024)
		bbto.SetBlockCache(cache)
	}
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10))
	bbto.SetCacheIndexAndFilterBlocks(true)
	opts.SetBlockBasedTableFactory(bbto)
	openTiming.DB1OptsCreate = time.Since(t2)

	t3 := time.Now()
	db1, err := grocksdb.OpenDbForReadOnly(opts, db1Path, false)
	if err != nil {
		log.Fatalf("Failed to open DB1: %v", err)
	}
	defer db1.Close()
	defer opts.Destroy()
	openTiming.DB1Open = time.Since(t3)

	t4 := time.Now()
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	openTiming.DB1ROCreate = time.Since(t4)
	openTiming.DB1Total = openTiming.DB1OptsCreate + openTiming.DB1Open + openTiming.DB1ROCreate

	// Create zstd decoder
	t5 := time.Now()
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		log.Fatalf("Failed to create zstd decoder: %v", err)
	}
	defer decoder.Close()
	openTiming.DecoderCreate = time.Since(t5)
	openTiming.GrandTotal = time.Since(grandStart)

	// Print initialization info
	fmt.Printf("\n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("RecSplit Two-Step Lookup - Initialization\n")
	fmt.Printf("================================================================================\n")
	fmt.Printf("\n")
	fmt.Printf("RecSplit Index: %s\n", recsplitDir)
	fmt.Printf("  Load time:          %s\n", helpers.FormatDuration(openTiming.RecSplitLoad))
	fmt.Printf("  Keys indexed:       %s\n", helpers.FormatNumber(int64(rsIdx.NumEntries)))
	fmt.Printf("  Memory mode:        %s\n", rsIdx.GetMemoryMode())
	fmt.Printf("\n")
	fmt.Printf("DB1: %s\n", db1Path)
	fmt.Printf("  Open time:          %s\n", helpers.FormatDuration(openTiming.DB1Total))
	fmt.Printf("\n")
	fmt.Printf("TOTAL OPEN TIME:      %s\n", helpers.FormatDuration(openTiming.GrandTotal))
	fmt.Printf("================================================================================\n\n")

	printMemStats()

	// =========================================================================
	// Query
	// =========================================================================
	if ongoing {
		runningStats := RunningStats{}
		reader := bufio.NewScanner(os.Stdin)

		fmt.Println("Ongoing mode: enter tx hashes (or STOP to quit, STATS for summary):")

		for reader.Scan() {
			line := strings.TrimSpace(reader.Text())
			if line == "" {
				continue
			}
			if strings.EqualFold(line, "STOP") {
				printFinalStats(runningStats)
				return
			}
			if strings.EqualFold(line, "STATS") {
				printFinalStats(runningStats)
				continue
			}

			timing, result, err := runQuery(rsIdx, db1, ro, decoder, line)

			runningStats.QueryCount++
			runningStats.TotalRecSplit += timing.RecSplitLookup
			runningStats.TotalDB1Query += timing.DB1Query
			runningStats.TotalDecompress += timing.Decompress
			runningStats.TotalUnmarshal += timing.Unmarshal
			runningStats.TotalTxSearch += timing.TxSearch
			runningStats.TotalTxExtract += timing.TxExtract
			runningStats.TotalTime += timing.Total
			runningStats.TotalTxScanned += timing.TxCount

			if err != nil {
				runningStats.NotFoundCount++
				if strings.Contains(err.Error(), "not found in RecSplit") {
					runningStats.NotFoundRecSplit++
				} else if strings.Contains(err.Error(), "not found in DB1") {
					runningStats.NotFoundDB1++
				} else if strings.Contains(err.Error(), "not found in ledger") {
					runningStats.NotFoundInLedger++
				}
				if !quiet {
					fmt.Printf("Error: %v\n\n", err)
				} else {
					fmt.Printf("NOT FOUND: %s (RecSplit: %s)\n", line, helpers.FormatDuration(timing.RecSplitLookup))
				}
				continue
			}

			printQueryResult(timing, result, quiet)
		}

		printFinalStats(runningStats)
		return
	}

	// Single query
	timing, result, err := runQuery(rsIdx, db1, ro, decoder, txHashHex)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	printQueryResult(timing, result, quiet)
}

func runQuery(
	rsIdx *RecSplitIndex,
	db1 *grocksdb.DB,
	ro *grocksdb.ReadOptions,
	decoder *zstd.Decoder,
	txHashHex string,
) (QueryTiming, *QueryResult, error) {
	timing := QueryTiming{}
	totalStart := time.Now()

	txHashBytes, err := hex.DecodeString(txHashHex)
	if err != nil {
		return timing, nil, fmt.Errorf("invalid hex: %v", err)
	}
	if len(txHashBytes) != 32 {
		return timing, nil, fmt.Errorf("invalid hash length: %d", len(txHashBytes))
	}

	// Step 1: RecSplit lookup
	t1 := time.Now()
	ledgerSeq, found := rsIdx.Lookup(txHashBytes)
	timing.RecSplitLookup = time.Since(t1)

	if !found {
		timing.Total = time.Since(totalStart)
		return timing, nil, fmt.Errorf("txHash not found in RecSplit: %s", txHashHex)
	}

	// Step 2: DB1 lookup
	ledgerKey := make([]byte, 4)
	binary.BigEndian.PutUint32(ledgerKey, ledgerSeq)

	t2 := time.Now()
	slice1, err := db1.Get(ro, ledgerKey)
	timing.DB1Query = time.Since(t2)

	if err != nil {
		return timing, nil, fmt.Errorf("DB1 query error: %v", err)
	}
	defer slice1.Free()

	if !slice1.Exists() {
		timing.Total = time.Since(totalStart)
		return timing, nil, fmt.Errorf("ledger %d not found in DB1", ledgerSeq)
	}

	compressedLCM := make([]byte, len(slice1.Data()))
	copy(compressedLCM, slice1.Data())

	// Step 3: Decompress
	t3 := time.Now()
	uncompressedLCM, err := decoder.DecodeAll(compressedLCM, nil)
	timing.Decompress = time.Since(t3)
	if err != nil {
		return timing, nil, fmt.Errorf("decompress failed: %v", err)
	}

	// Step 4: Unmarshal
	t4 := time.Now()
	var lcm xdr.LedgerCloseMeta
	err = lcm.UnmarshalBinary(uncompressedLCM)
	timing.Unmarshal = time.Since(t4)
	if err != nil {
		return timing, nil, fmt.Errorf("unmarshal failed: %v", err)
	}

	// Step 5: Find transaction
	t5 := time.Now()
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		network.PublicNetworkPassphrase, lcm)
	timing.TxReaderCreate = time.Since(t5)
	if err != nil {
		return timing, nil, fmt.Errorf("tx reader failed: %v", err)
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
			return timing, nil, fmt.Errorf("tx read error: %v", err)
		}
		txCount++

		if tx.Hash.HexString() == txHashHex {
			foundTx = &tx
			break
		}
	}
	timing.TxSearch = time.Since(t6)
	timing.TxCount = txCount

	if foundTx == nil {
		timing.Total = time.Since(totalStart)
		return timing, nil, fmt.Errorf("txHash %s not found in ledger %d", txHashHex, ledgerSeq)
	}

	// Step 6: Extract
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

func printQueryResult(timing QueryTiming, result *QueryResult, quiet bool) {
	if quiet {
		fmt.Printf("TX: %s | Ledger: %d | RecSplit: %s | Total: %s\n",
			result.TxHash[:16]+"...", result.LedgerSeq,
			helpers.FormatDuration(timing.RecSplitLookup),
			helpers.FormatDuration(timing.Total))
		return
	}

	fmt.Printf("\nTransaction: %s\n", result.TxHash)
	fmt.Printf("Ledger: %d | Index: %d | Closed: %v\n", result.LedgerSeq, result.TxIndex, result.ClosedAt)
	fmt.Printf("Timing: RecSplit=%s DB1=%s Decomp=%s Unmarshal=%s TxSearch=%s Total=%s\n",
		helpers.FormatDuration(timing.RecSplitLookup),
		helpers.FormatDuration(timing.DB1Query),
		helpers.FormatDuration(timing.Decompress),
		helpers.FormatDuration(timing.Unmarshal),
		helpers.FormatDuration(timing.TxSearch),
		helpers.FormatDuration(timing.Total))
	fmt.Println()
}

func printFinalStats(stats RunningStats) {
	if stats.QueryCount == 0 {
		return
	}
	n := time.Duration(stats.QueryCount - stats.NotFoundCount)
	if n == 0 {
		n = 1
	}
	fmt.Printf("\nStats: %d queries, %d not found\n", stats.QueryCount, stats.NotFoundCount)
	fmt.Printf("Avg: RecSplit=%s DB1=%s Decomp=%s Total=%s\n",
		helpers.FormatDuration(stats.TotalRecSplit/n),
		helpers.FormatDuration(stats.TotalDB1Query/n),
		helpers.FormatDuration(stats.TotalDecompress/n),
		helpers.FormatDuration(stats.TotalTime/n))
}

func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Memory: Alloc=%s HeapInuse=%s\n",
		helpers.FormatBytes(int64(m.Alloc)),
		helpers.FormatBytes(int64(m.HeapInuse)))
}
