// compact-store triggers manual compaction on a RocksDB store with 16 column
// families used for tx_hash -> ledger_seq mappings.
//
// This utility is designed to be run after bulk ingestion when automatic
// compaction is disabled. It compacts all column families sequentially,
// optimizing the store for read performance.
//
// Usage:
//
//	compact-store --store /path/to/rocksdb/store
//
// Options:
//
//	--store PATH      Path to RocksDB store (required)
//	--cf NAME         Compact only this column family (can be repeated)
//	--parallel        Compact column families in parallel (uses more resources)
//	--log-file PATH   Log to file instead of stdout
//	--help            Show help message
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
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

// stringSliceFlag allows multiple --cf flags
type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	return strings.Join(*s, ", ")
}

func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	var cfNames stringSliceFlag

	var (
		storePath   = flag.String("store", "", "Path to RocksDB store (required)")
		parallel    = flag.Bool("parallel", false, "Compact column families in parallel")
		logFile     = flag.String("log-file", "", "Path to log file (stdout if empty)")
		showHelp    = flag.Bool("help", false, "Show help message")
		showVersion = flag.Bool("version", false, "Show version information")
	)

	flag.Var(&cfNames, "cf", "Compact only this column family (can be repeated)")
	flag.Parse()

	if *showHelp {
		printUsage()
		os.Exit(0)
	}

	if *showVersion {
		fmt.Println("compact-store v1.0.0")
		fmt.Println("Part of stellar-full-history-ingestion tools")
		os.Exit(0)
	}

	if *storePath == "" {
		fmt.Fprintf(os.Stderr, "Error: --store is required\n\n")
		printUsage()
		os.Exit(1)
	}

	// Validate store exists
	if _, err := os.Stat(*storePath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: store does not exist: %s\n", *storePath)
		os.Exit(1)
	}

	// Setup logging
	var logWriter = os.Stdout
	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening log file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		logWriter = f
	}

	log := func(format string, args ...interface{}) {
		fmt.Fprintf(logWriter, time.Now().Format("2006/01/02 15:04:05 ")+format+"\n", args...)
	}

	// Determine which CFs to compact
	cfsToCompact := ColumnFamilyNames
	if len(cfNames) > 0 {
		cfsToCompact = cfNames
		// Validate CF names
		validCFs := make(map[string]bool)
		for _, cf := range ColumnFamilyNames {
			validCFs[cf] = true
		}
		for _, cf := range cfNames {
			if !validCFs[cf] {
				fmt.Fprintf(os.Stderr, "Error: invalid column family name: %s\n", cf)
				fmt.Fprintf(os.Stderr, "Valid names: %s\n", strings.Join(ColumnFamilyNames, ", "))
				os.Exit(1)
			}
		}
	}

	// Setup signal handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log("Received signal %v, compaction will complete current CF then exit", sig)
	}()

	// Open database
	log("================================================================================")
	log("                         COMPACT STORE UTILITY")
	log("================================================================================")
	log("")
	log("Store:            %s", *storePath)
	log("Column Families:  %s", strings.Join(cfsToCompact, ", "))
	log("Mode:             %s", map[bool]string{true: "PARALLEL", false: "SEQUENTIAL"}[*parallel])
	log("")

	log("Opening store...")

	// Create options
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)
	defer opts.Destroy()

	// Prepare all CF names (including default)
	allCFNames := []string{"default"}
	allCFNames = append(allCFNames, ColumnFamilyNames...)

	// Create options for each CF
	cfOptsList := make([]*grocksdb.Options, len(allCFNames))
	for i := range allCFNames {
		cfOptsList[i] = grocksdb.NewDefaultOptions()
	}
	defer func() {
		for _, cfOpt := range cfOptsList {
			if cfOpt != nil {
				cfOpt.Destroy()
			}
		}
	}()

	// Open database with all column families
	db, cfHandles, err := grocksdb.OpenDbColumnFamilies(opts, *storePath, allCFNames, cfOptsList)
	if err != nil {
		log("ERROR: Failed to open store: %v", err)
		os.Exit(1)
	}
	defer func() {
		for _, cfHandle := range cfHandles {
			if cfHandle != nil {
				cfHandle.Destroy()
			}
		}
		db.Close()
	}()

	// Build CF name to handle map
	cfHandleMap := make(map[string]*grocksdb.ColumnFamilyHandle)
	for i, name := range allCFNames {
		cfHandleMap[name] = cfHandles[i]
	}

	log("Store opened successfully")
	log("")
	log("================================================================================")
	log("                         STARTING COMPACTION")
	log("================================================================================")
	log("")

	totalStart := time.Now()

	if *parallel {
		// Parallel compaction
		var wg sync.WaitGroup
		results := make(chan string, len(cfsToCompact))

		for _, cfName := range cfsToCompact {
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				cfHandle := cfHandleMap[name]

				start := time.Now()
				db.CompactRangeCF(cfHandle, grocksdb.Range{Start: nil, Limit: nil})
				elapsed := time.Since(start)

				results <- fmt.Sprintf("CF[%s]: completed in %s", name, helpers.FormatDuration(elapsed))
			}(cfName)
		}

		// Wait for all to complete
		go func() {
			wg.Wait()
			close(results)
		}()

		for result := range results {
			log("%s", result)
		}
	} else {
		// Sequential compaction
		for i, cfName := range cfsToCompact {
			cfHandle := cfHandleMap[cfName]

			log("Compacting column family [%s] (%d/%d)...", cfName, i+1, len(cfsToCompact))
			start := time.Now()

			db.CompactRangeCF(cfHandle, grocksdb.Range{Start: nil, Limit: nil})

			elapsed := time.Since(start)
			log("  Completed in %s", helpers.FormatDuration(elapsed))
		}
	}

	totalTime := time.Since(totalStart)

	log("")
	log("================================================================================")
	log("                         COMPACTION COMPLETE")
	log("================================================================================")
	log("")
	log("Total time:       %s", helpers.FormatDuration(totalTime))
	log("CFs compacted:    %d", len(cfsToCompact))
	log("")
	log("The store is now optimized for read operations.")
	log("")
	log("================================================================================")
}

func printUsage() {
	fmt.Println("compact-store - Trigger manual compaction on a RocksDB store")
	fmt.Println()
	fmt.Println("USAGE:")
	fmt.Println("  compact-store --store PATH [OPTIONS]")
	fmt.Println()
	fmt.Println("OPTIONS:")
	fmt.Println("  --store PATH      Path to RocksDB store (required)")
	fmt.Println("  --cf NAME         Compact only this column family (can be repeated)")
	fmt.Println("  --parallel        Compact column families in parallel")
	fmt.Println("  --log-file PATH   Log to file instead of stdout")
	fmt.Println("  --help            Show this help message")
	fmt.Println("  --version         Show version information")
	fmt.Println()
	fmt.Println("EXAMPLES:")
	fmt.Println("  # Compact all column families sequentially")
	fmt.Println("  compact-store --store /data/tx-hash-store")
	fmt.Println()
	fmt.Println("  # Compact specific column families")
	fmt.Println("  compact-store --store /data/tx-hash-store --cf 0 --cf 1 --cf 2")
	fmt.Println()
	fmt.Println("  # Compact in parallel (uses more CPU/IO)")
	fmt.Println("  compact-store --store /data/tx-hash-store --parallel")
	fmt.Println()
	fmt.Println("COLUMN FAMILIES:")
	fmt.Println("  0, 1, 2, 3, 4, 5, 6, 7, 8, 9, a, b, c, d, e, f")
	fmt.Println()
	fmt.Println("NOTES:")
	fmt.Println("  - Compaction can take a long time for large stores")
	fmt.Println("  - Parallel mode uses more resources but completes faster")
	fmt.Println("  - The store remains usable during compaction")
}
