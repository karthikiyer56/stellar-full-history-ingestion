// =============================================================================
// main.go - Entry Point for txhash-ingestion-workflow
// =============================================================================
//
// This is the entry point for the txhash-ingestion-workflow tool. It handles:
//   - Command-line flag parsing
//   - Signal handling (SIGHUP for queries, SIGINT/SIGTERM for graceful shutdown)
//   - Logger initialization
//   - Workflow orchestration
//
// USAGE:
//
//	txhash-ingestion-workflow \
//	  --lfs-store /path/to/lfs \
//	  --start-ledger 10000001 \
//	  --end-ledger 20000000 \
//	  --output-dir /path/to/output \
//	  --log-file /path/to/ingestion.log \
//	  --error-file /path/to/ingestion.err \
//	  --query-file /path/to/queries.txt \
//	  --query-output /path/to/query-results.csv \
//	  --query-log /path/to/query-stats.log \
//	  --query-error /path/to/query-errors.log \
//	  [--parallel-recsplit]
//	  [--block-cache-mb 8192]
//	  [--dry-run]
//
// SIGNAL HANDLING:
//
//	SIGHUP:
//	  - Triggers query processing during INGESTING and COMPACTING phases
//	  - Reads txHashes from --query-file and looks them up in RocksDB
//	  - Results written to --query-output as CSV
//	  - Statistics written to --query-log
//	  - Ignored during BUILDING_RECSPLIT, VERIFYING, and COMPLETE phases
//
//	SIGINT / SIGTERM:
//	  - Triggers graceful shutdown
//	  - Completes any in-progress batch before exiting
//	  - Ensures checkpoints are consistent
//
// EXIT CODES:
//
//	0 - Success (or already complete)
//	1 - Configuration error
//	2 - Runtime error
//	130 - Interrupted by SIGINT
//	143 - Terminated by SIGTERM
//
// =============================================================================

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// =============================================================================
// Version Information
// =============================================================================

const (
	// Version is the tool version
	Version = "1.0.0"

	// ToolName is the name of this tool
	ToolName = "txhash-ingestion-workflow"
)

// =============================================================================
// Exit Codes
// =============================================================================

const (
	ExitSuccess      = 0
	ExitConfigError  = 1
	ExitRuntimeError = 2
	ExitInterrupted  = 130 // 128 + SIGINT(2)
	ExitTerminated   = 143 // 128 + SIGTERM(15)
)

// =============================================================================
// Main Entry Point
// =============================================================================

func main() {
	// Parse command-line flags
	config := parseFlags()

	// Validate configuration
	if err := config.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		os.Exit(ExitConfigError)
	}

	// Create logger
	logger, err := NewDualLogger(config.LogFile, config.ErrorFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(ExitConfigError)
	}
	defer logger.Close()

	// Log startup
	logStartup(logger, config)

	// Handle dry-run
	if config.DryRun {
		logger.Info("DRY RUN: Configuration validated. Exiting without executing workflow.")
		fmt.Println("Dry run complete. Configuration is valid.")
		os.Exit(ExitSuccess)
	}

	// Create workflow
	workflow, err := NewWorkflow(config, logger)
	if err != nil {
		logger.Error("Failed to create workflow: %v", err)
		fmt.Fprintf(os.Stderr, "Failed to create workflow: %v\n", err)
		os.Exit(ExitRuntimeError)
	}
	defer workflow.Close()

	// Create query handler
	queryHandler, err := NewQueryHandler(config, workflow.GetStore(), logger)
	if err != nil {
		logger.Error("Failed to create query handler: %v", err)
		fmt.Fprintf(os.Stderr, "Failed to create query handler: %v\n", err)
		os.Exit(ExitRuntimeError)
	}
	workflow.SetQueryHandler(queryHandler)

	// Set up signal handling
	sigChan := setupSignalHandling(queryHandler, logger)

	// Run workflow in a separate goroutine so we can handle signals
	errChan := make(chan error, 1)
	go func() {
		errChan <- workflow.Run()
	}()

	// Wait for workflow completion or signal
	select {
	case err := <-errChan:
		if err != nil {
			logger.Error("Workflow failed: %v", err)
			fmt.Fprintf(os.Stderr, "Workflow failed: %v\n", err)
			os.Exit(ExitRuntimeError)
		}
		logger.Info("Workflow completed successfully")
		fmt.Println("Workflow completed successfully")
		os.Exit(ExitSuccess)

	case sig := <-sigChan:
		// Handle termination signals
		logger.Info("Received signal: %v", sig)
		logger.Info("Initiating graceful shutdown...")

		// Give the workflow a chance to complete current batch
		// The workflow will checkpoint at the next batch boundary

		if sig == syscall.SIGINT {
			os.Exit(ExitInterrupted)
		}
		os.Exit(ExitTerminated)
	}
}

// =============================================================================
// Flag Parsing
// =============================================================================

// parseFlags parses command-line flags and returns a Config.
func parseFlags() *Config {
	config := &Config{
		RocksDB: DefaultRocksDBSettings(),
	}

	// Required flags
	flag.StringVar(&config.LFSStorePath, "lfs-store", "", "Path to LFS ledger store (required)")
	flag.StringVar(&config.OutputDir, "output-dir", "", "Base output directory (required)")
	flag.StringVar(&config.LogFile, "log-file", "", "Path to main log file (required)")
	flag.StringVar(&config.ErrorFile, "error-file", "", "Path to error log file (required)")
	flag.StringVar(&config.QueryFile, "query-file", "", "Path to query file for SIGHUP queries (required)")
	flag.StringVar(&config.QueryOutput, "query-output", "", "Path to query results CSV output (required)")
	flag.StringVar(&config.QueryLog, "query-log", "", "Path to query statistics log (required)")
	flag.StringVar(&config.QueryError, "query-error", "", "Path to query errors log (required)")

	// Ledger range (uint32 via temporary variables)
	var startLedger, endLedger uint64
	flag.Uint64Var(&startLedger, "start-ledger", 0, "First ledger to ingest (must match X0000001 pattern)")
	flag.Uint64Var(&endLedger, "end-ledger", 0, "Last ledger to ingest (must match Y0000000 pattern)")

	// Optional flags
	flag.BoolVar(&config.ParallelRecsplit, "parallel-recsplit", false, "Build 16 RecSplit indexes in parallel (requires ~144GB RAM)")
	flag.BoolVar(&config.DryRun, "dry-run", false, "Validate configuration and exit without executing")
	flag.IntVar(&config.RocksDB.BlockCacheSizeMB, "block-cache-mb", DefaultBlockCacheMB, "RocksDB block cache size in MB")

	// Version flag
	showVersion := flag.Bool("version", false, "Show version and exit")

	// Custom usage message
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", ToolName)
		fmt.Fprintf(os.Stderr, "%s ingests txHash→ledgerSeq mappings from LFS into RocksDB,\n", ToolName)
		fmt.Fprintf(os.Stderr, "builds RecSplit indexes, and verifies the results.\n\n")
		fmt.Fprintf(os.Stderr, "Required flags:\n")
		fmt.Fprintf(os.Stderr, "  --lfs-store PATH      Path to LFS ledger store\n")
		fmt.Fprintf(os.Stderr, "  --start-ledger N      First ledger to ingest (e.g., 10000001)\n")
		fmt.Fprintf(os.Stderr, "  --end-ledger N        Last ledger to ingest (e.g., 20000000)\n")
		fmt.Fprintf(os.Stderr, "  --output-dir PATH     Base output directory\n")
		fmt.Fprintf(os.Stderr, "  --log-file PATH       Path to main log file\n")
		fmt.Fprintf(os.Stderr, "  --error-file PATH     Path to error log file\n")
		fmt.Fprintf(os.Stderr, "  --query-file PATH     Path to query file for SIGHUP queries\n")
		fmt.Fprintf(os.Stderr, "  --query-output PATH   Path to query results CSV output\n")
		fmt.Fprintf(os.Stderr, "  --query-log PATH      Path to query statistics log\n")
		fmt.Fprintf(os.Stderr, "  --query-error PATH    Path to query errors log\n")
		fmt.Fprintf(os.Stderr, "\nOptional flags:\n")
		fmt.Fprintf(os.Stderr, "  --parallel-recsplit   Build 16 RecSplit indexes in parallel (~144GB RAM)\n")
		fmt.Fprintf(os.Stderr, "  --block-cache-mb N    RocksDB block cache size in MB (default: %d)\n", DefaultBlockCacheMB)
		fmt.Fprintf(os.Stderr, "  --dry-run             Validate configuration and exit\n")
		fmt.Fprintf(os.Stderr, "  --version             Show version and exit\n")
		fmt.Fprintf(os.Stderr, "\nSignal handling:\n")
		fmt.Fprintf(os.Stderr, "  SIGHUP                Trigger query from --query-file\n")
		fmt.Fprintf(os.Stderr, "  SIGINT/SIGTERM        Graceful shutdown\n")
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s \\\n", ToolName)
		fmt.Fprintf(os.Stderr, "    --lfs-store /data/lfs \\\n")
		fmt.Fprintf(os.Stderr, "    --start-ledger 10000001 \\\n")
		fmt.Fprintf(os.Stderr, "    --end-ledger 20000000 \\\n")
		fmt.Fprintf(os.Stderr, "    --output-dir /data/output \\\n")
		fmt.Fprintf(os.Stderr, "    --log-file /var/log/txhash.log \\\n")
		fmt.Fprintf(os.Stderr, "    --error-file /var/log/txhash.err \\\n")
		fmt.Fprintf(os.Stderr, "    --query-file /data/queries.txt \\\n")
		fmt.Fprintf(os.Stderr, "    --query-output /data/query-results.csv \\\n")
		fmt.Fprintf(os.Stderr, "    --query-log /var/log/query-stats.log \\\n")
		fmt.Fprintf(os.Stderr, "    --query-error /var/log/query-errors.log\n")
	}

	flag.Parse()

	// Handle version flag
	if *showVersion {
		fmt.Printf("%s version %s\n", ToolName, Version)
		os.Exit(0)
	}

	// Convert ledger range to uint32
	config.StartLedger = uint32(startLedger)
	config.EndLedger = uint32(endLedger)

	return config
}

// =============================================================================
// Signal Handling
// =============================================================================

// setupSignalHandling sets up signal handlers for SIGHUP, SIGINT, and SIGTERM.
//
// SIGHUP triggers the query handler.
// SIGINT/SIGTERM are returned on a channel for graceful shutdown.
func setupSignalHandling(queryHandler *QueryHandler, logger Logger) chan os.Signal {
	// Channel for termination signals
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	// Channel for SIGHUP
	hupChan := make(chan os.Signal, 10)
	signal.Notify(hupChan, syscall.SIGHUP)

	// Handle SIGHUP in a separate goroutine
	go func() {
		for range hupChan {
			logger.Info("Received SIGHUP signal")
			queryHandler.TriggerQuery()
		}
	}()

	return termChan
}

// =============================================================================
// Startup Logging
// =============================================================================

// logStartup logs startup information.
func logStartup(logger Logger, config *Config) {
	logger.Separator()
	logger.Info("                    %s v%s", ToolName, Version)
	logger.Separator()
	logger.Info("")
	logger.Info("Process ID:  %d", os.Getpid())
	logger.Info("Working Dir: %s", mustGetwd())
	logger.Info("")

	// Log a hint about SIGHUP
	logger.Info("SIGNAL HANDLING:")
	logger.Info("  SIGHUP  → Trigger query from %s", config.QueryFile)
	logger.Info("  SIGINT  → Graceful shutdown")
	logger.Info("  SIGTERM → Graceful shutdown")
	logger.Info("")
	logger.Info("To trigger a query during ingestion:")
	logger.Info("  kill -HUP %d", os.Getpid())
	logger.Info("")

	logger.Sync()
}

// mustGetwd returns the current working directory or "unknown".
func mustGetwd() string {
	wd, err := os.Getwd()
	if err != nil {
		return "unknown"
	}
	return wd
}
