// tx-hash-store-benchmark benchmarks read performance of a tx_hash -> ledger_seq
// RocksDB store with 16 column families.
//
// It reads transaction hashes from a file (one 64-character hex hash per line)
// and looks them up in the store, measuring latency and throughput.
//
// Usage:
//
//	tx-hash-store-benchmark \
//	  --store /path/to/rocksdb/store \
//	  --hashes /path/to/hashes.txt \
//	  --count 10000
//
// The hashes file should contain one 64-character hex transaction hash per line:
//
//	0a1b2c3d4e5f...  (64 chars)
//	1f2e3d4c5b6a...  (64 chars)
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/linxGnu/grocksdb"
)

const (
	MB = 1024 * 1024
)

// ColumnFamilyNames contains the names of all 16 column families.
var ColumnFamilyNames = []string{
	"0", "1", "2", "3", "4", "5", "6", "7",
	"8", "9", "a", "b", "c", "d", "e", "f",
}

func main() {
	var (
		storePath       = flag.String("store", "", "Path to RocksDB store (required)")
		hashesFile      = flag.String("hashes", "", "Path to file with hex tx hashes (required)")
		count           = flag.Int("count", 10000, "Number of lookups to perform")
		randomize       = flag.Bool("randomize", true, "Randomize hash order for lookups")
		blockCacheMB    = flag.Int("block-cache", 512, "Block cache size in MB")
		warmup          = flag.Int("warmup", 100, "Number of warmup lookups (not counted)")
		showPercentiles = flag.Bool("percentiles", true, "Show latency percentiles")
		showHelp        = flag.Bool("help", false, "Show help message")
	)

	flag.Parse()

	if *showHelp {
		printUsage()
		os.Exit(0)
	}

	if *storePath == "" || *hashesFile == "" {
		fmt.Fprintf(os.Stderr, "Error: --store and --hashes are required\n\n")
		printUsage()
		os.Exit(1)
	}

	// Load hashes from file
	fmt.Printf("Loading hashes from %s...\n", *hashesFile)
	hashes, err := loadHashes(*hashesFile, *count+*warmup)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading hashes: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d hashes\n", len(hashes))

	if len(hashes) < *warmup+*count {
		fmt.Fprintf(os.Stderr, "Warning: only %d hashes available, need %d for warmup + count\n",
			len(hashes), *warmup+*count)
		if len(hashes) <= *warmup {
			fmt.Fprintf(os.Stderr, "Error: not enough hashes for even warmup\n")
			os.Exit(1)
		}
		*count = len(hashes) - *warmup
	}

	// Randomize if requested
	if *randomize {
		fmt.Printf("Randomizing hash order...\n")
		rand.Shuffle(len(hashes), func(i, j int) {
			hashes[i], hashes[j] = hashes[j], hashes[i]
		})
	}

	// Open store
	fmt.Printf("Opening store at %s...\n", *storePath)
	store, err := openStore(*storePath, *blockCacheMB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()
	fmt.Printf("Store opened with %d MB block cache\n", *blockCacheMB)

	// Warmup
	if *warmup > 0 {
		fmt.Printf("Warming up with %d lookups...\n", *warmup)
		for i := 0; i < *warmup && i < len(hashes); i++ {
			store.Get(hashes[i])
		}
	}

	// Run benchmark
	fmt.Printf("\nRunning benchmark with %d lookups...\n", *count)
	fmt.Println()

	latencies := make([]time.Duration, 0, *count)
	found := 0
	notFound := 0

	startTime := time.Now()

	for i := 0; i < *count; i++ {
		hashIdx := *warmup + i
		if hashIdx >= len(hashes) {
			hashIdx = hashIdx % len(hashes) // Wrap around if needed
		}
		hash := hashes[hashIdx]

		start := time.Now()
		value, err := store.Get(hash)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error looking up hash: %v\n", err)
			continue
		}

		latencies = append(latencies, elapsed)

		if value != nil {
			found++
		} else {
			notFound++
		}
	}

	totalTime := time.Since(startTime)

	// Calculate statistics
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var totalLatency time.Duration
	for _, l := range latencies {
		totalLatency += l
	}

	avgLatency := totalLatency / time.Duration(len(latencies))
	minLatency := latencies[0]
	maxLatency := latencies[len(latencies)-1]
	medianLatency := latencies[len(latencies)/2]

	throughput := float64(*count) / totalTime.Seconds()

	// Print results
	fmt.Println("================================================================================")
	fmt.Println("                         BENCHMARK RESULTS")
	fmt.Println("================================================================================")
	fmt.Println()
	fmt.Printf("Store:           %s\n", *storePath)
	fmt.Printf("Lookups:         %d\n", *count)
	fmt.Printf("Found:           %d (%.2f%%)\n", found, float64(found)/float64(*count)*100)
	fmt.Printf("Not Found:       %d (%.2f%%)\n", notFound, float64(notFound)/float64(*count)*100)
	fmt.Println()
	fmt.Println("LATENCY:")
	fmt.Printf("  Min:           %s\n", helpers.FormatDuration(minLatency))
	fmt.Printf("  Max:           %s\n", helpers.FormatDuration(maxLatency))
	fmt.Printf("  Avg:           %s\n", helpers.FormatDuration(avgLatency))
	fmt.Printf("  Median:        %s\n", helpers.FormatDuration(medianLatency))

	if *showPercentiles && len(latencies) >= 100 {
		fmt.Println()
		fmt.Println("PERCENTILES:")
		p50 := latencies[len(latencies)*50/100]
		p75 := latencies[len(latencies)*75/100]
		p90 := latencies[len(latencies)*90/100]
		p95 := latencies[len(latencies)*95/100]
		p99 := latencies[len(latencies)*99/100]
		fmt.Printf("  p50:           %s\n", helpers.FormatDuration(p50))
		fmt.Printf("  p75:           %s\n", helpers.FormatDuration(p75))
		fmt.Printf("  p90:           %s\n", helpers.FormatDuration(p90))
		fmt.Printf("  p95:           %s\n", helpers.FormatDuration(p95))
		fmt.Printf("  p99:           %s\n", helpers.FormatDuration(p99))
	}

	fmt.Println()
	fmt.Println("THROUGHPUT:")
	fmt.Printf("  Total Time:    %s\n", helpers.FormatDuration(totalTime))
	fmt.Printf("  Queries/sec:   %.2f\n", throughput)
	fmt.Println()
	fmt.Println("================================================================================")
}

// Store wraps the RocksDB store for benchmarking.
type Store struct {
	db         *grocksdb.DB
	opts       *grocksdb.Options
	cfHandles  []*grocksdb.ColumnFamilyHandle
	cfOpts     []*grocksdb.Options
	blockCache *grocksdb.Cache
	ro         *grocksdb.ReadOptions
	cfIndexMap map[string]int
}

func openStore(path string, blockCacheMB int) (*Store, error) {
	// Create block cache
	var blockCache *grocksdb.Cache
	if blockCacheMB > 0 {
		blockCache = grocksdb.NewLRUCache(uint64(blockCacheMB * MB))
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)

	// Prepare CF names
	cfNames := []string{"default"}
	cfNames = append(cfNames, ColumnFamilyNames...)

	// Create options for each CF with block cache
	cfOptsList := make([]*grocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts := grocksdb.NewDefaultOptions()

		bbto := grocksdb.NewDefaultBlockBasedTableOptions()
		if blockCache != nil {
			bbto.SetBlockCache(blockCache)
		}
		cfOpts.SetBlockBasedTableFactory(bbto)

		cfOptsList[i] = cfOpts
	}

	// Open for read-only
	db, cfHandles, err := grocksdb.OpenDbForReadOnlyColumnFamilies(opts, path, cfNames, cfOptsList, false)
	if err != nil {
		opts.Destroy()
		for _, cfOpt := range cfOptsList {
			if cfOpt != nil {
				cfOpt.Destroy()
			}
		}
		if blockCache != nil {
			blockCache.Destroy()
		}
		return nil, fmt.Errorf("failed to open store: %w", err)
	}

	// Build CF index map
	cfIndexMap := make(map[string]int)
	for i, name := range cfNames {
		cfIndexMap[name] = i
	}

	ro := grocksdb.NewDefaultReadOptions()

	return &Store{
		db:         db,
		opts:       opts,
		cfHandles:  cfHandles,
		cfOpts:     cfOptsList,
		blockCache: blockCache,
		ro:         ro,
		cfIndexMap: cfIndexMap,
	}, nil
}

func (s *Store) Close() {
	if s.ro != nil {
		s.ro.Destroy()
	}
	for _, cfHandle := range s.cfHandles {
		if cfHandle != nil {
			cfHandle.Destroy()
		}
	}
	if s.db != nil {
		s.db.Close()
	}
	if s.opts != nil {
		s.opts.Destroy()
	}
	for _, cfOpt := range s.cfOpts {
		if cfOpt != nil {
			cfOpt.Destroy()
		}
	}
	if s.blockCache != nil {
		s.blockCache.Destroy()
	}
}

func (s *Store) Get(txHash []byte) ([]byte, error) {
	// Determine column family
	cfName := getCFName(txHash)
	idx := s.cfIndexMap[cfName]
	cfHandle := s.cfHandles[idx]

	slice, err := s.db.GetCF(s.ro, cfHandle, txHash)
	if err != nil {
		return nil, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return nil, nil
	}

	// Copy the data
	result := make([]byte, slice.Size())
	copy(result, slice.Data())
	return result, nil
}

func getCFName(txHash []byte) string {
	if len(txHash) < 1 {
		return "0"
	}
	idx := int(txHash[0] >> 4) // High nibble
	if idx < 0 || idx > 15 {
		return "0"
	}
	return ColumnFamilyNames[idx]
}

func loadHashes(path string, maxHashes int) ([][]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var hashes [][]byte
	scanner := bufio.NewScanner(file)

	for scanner.Scan() && len(hashes) < maxHashes {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse hex hash (should be 64 chars = 32 bytes)
		if len(line) != 64 {
			continue // Skip invalid lines
		}

		hash, err := hex.DecodeString(line)
		if err != nil {
			continue // Skip invalid hex
		}

		hashes = append(hashes, hash)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return hashes, nil
}

func decodeLedgerSeq(value []byte) uint32 {
	if len(value) != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(value)
}

func printUsage() {
	fmt.Println("tx-hash-store-benchmark - Benchmark tx_hash RocksDB store read performance")
	fmt.Println()
	fmt.Println("USAGE:")
	fmt.Println("  tx-hash-store-benchmark --store PATH --hashes FILE [OPTIONS]")
	fmt.Println()
	fmt.Println("OPTIONS:")
	fmt.Println("  --store PATH        Path to RocksDB store (required)")
	fmt.Println("  --hashes FILE       Path to file with hex tx hashes (required)")
	fmt.Println("  --count N           Number of lookups to perform (default: 10000)")
	fmt.Println("  --warmup N          Number of warmup lookups (default: 100)")
	fmt.Println("  --block-cache N     Block cache size in MB (default: 512)")
	fmt.Println("  --randomize         Randomize hash order (default: true)")
	fmt.Println("  --percentiles       Show latency percentiles (default: true)")
	fmt.Println("  --help              Show this help message")
	fmt.Println()
	fmt.Println("HASHES FILE FORMAT:")
	fmt.Println("  One 64-character hex transaction hash per line:")
	fmt.Println()
	fmt.Println("    0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b")
	fmt.Println("    1f2e3d4c5b6a7f8e9d0c1b2a3f4e5d6c7b8a9f0e1d2c3b4a5f6e7d8c9b0a1f2e")
	fmt.Println("    # Lines starting with # are ignored")
	fmt.Println()
	fmt.Println("EXAMPLES:")
	fmt.Println("  # Basic benchmark")
	fmt.Println("  tx-hash-store-benchmark \\")
	fmt.Println("    --store /data/tx-hash-store \\")
	fmt.Println("    --hashes /data/sample-hashes.txt")
	fmt.Println()
	fmt.Println("  # Benchmark with larger cache and more lookups")
	fmt.Println("  tx-hash-store-benchmark \\")
	fmt.Println("    --store /data/tx-hash-store \\")
	fmt.Println("    --hashes /data/sample-hashes.txt \\")
	fmt.Println("    --count 100000 \\")
	fmt.Println("    --block-cache 2048")
}
