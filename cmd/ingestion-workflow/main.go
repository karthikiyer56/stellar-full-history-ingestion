// =============================================================================
// ingestion-workflow - Stellar Blockchain Full History Ingestion Pipeline
// =============================================================================
//
// This is the main entry point for the Stellar full history ingestion workflow.
// It orchestrates the complete lifecycle of ledger data from GCS to immutable storage.
//
// WHAT IT DOES:
//   1. Reads Stellar ledger data from Google Cloud Storage (GCS)
//   2. Ingests into active RocksDB stores (LCM for ledgers, TxHash for transactions)
//   3. Transitions to immutable storage (LFS chunks + RecSplit indexes)
//   4. Optionally deletes active RocksDB stores after transition completes
//
// THREE-PHASE LIFECYCLE:
//   - INGESTING: Fetch ledgers → Write to RocksDB (crash-recoverable every N ledgers)
//   - TRANSITIONING: Compact RocksDB → Export to LFS + RecSplit (parallel paths)
//   - COMPLETE: Query from immutable storage only
//
// CONFIGURATION:
//   - Uses TOML config file (--config flag, required)
//   - Supports dry-run mode (--dry-run) for config validation
//   - Configures RocksDB settings, GCS backend, parallel ranges, etc.
//   - See config.toml.example or cmd/ingestion-workflow/README.md for details
//
// SUBCOMMANDS/MODES:
//   - No subcommands - single binary with flag-based configuration
//   - Backfill mode: Ingest historical ledger ranges (configurable start/end)
//   - Supports parallel range processing (each range = 10M ledgers)
//
// EXAMPLE USAGE:
//   # Validate config
//   ./ingestion-workflow --config config.toml --dry-run
//
//   # Full ingestion with logging
//   ./ingestion-workflow \
//     --config config.toml \
//     --log-file /data/stellar/logs/ingestion.log \
//     --error-file /data/stellar/logs/errors.log \
//     --verbose
//
// CRASH RECOVERY:
//   - Automatically resumes from last checkpoint (stored in meta store)
//   - Ledger-level recovery during INGESTING (every 1000 ledgers by default)
//   - Chunk-level recovery during LFS writing (every chunk)
//   - No manual intervention needed - just restart the process
//
// =============================================================================

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/config"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/logging"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/orchestrator"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/stores/meta"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/types"
)

const (
	defaultLogPath   = "/dev/stdout"
	defaultErrorPath = "/dev/stderr"
)

func main() {
	configPath := flag.String("config", "", "Path to config file (required)")
	dryRun := flag.Bool("dry-run", false, "Validate config and exit without processing")
	logFile := flag.String("log-file", defaultLogPath, "Path to log file (default: stdout)")
	errorFile := flag.String("error-file", defaultErrorPath, "Path to error file (default: stderr)")
	verbose := flag.Bool("verbose", false, "Enable DEBUG-level logging")
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --config is required")
		flag.Usage()
		os.Exit(1)
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Config validation failed: %v", err)
	}

	if *dryRun {
		totalRanges := cfg.CalculateRangeCount()
		fmt.Println("Config valid.")
		fmt.Printf("Ledger range: %d - %d\n", cfg.Backfill.StartLedger, cfg.Backfill.EndLedger)
		fmt.Printf("Total ranges: %d\n", totalRanges)
		fmt.Printf("Parallel ranges: %d\n", cfg.Backfill.ParallelRanges)
		fmt.Printf("Ledgers per range: %d\n", config.LedgersPerRange)
		os.Exit(0)
	}

	logger, err := logging.NewDualLogger(*logFile, *errorFile, *verbose)
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Close()

	logger.Info("Starting ingestion workflow")
	logger.Info("Config: %s", *configPath)
	logger.Info("Data directory: %s", cfg.Service.DataDir)

	metaStorePath := filepath.Join(cfg.Service.DataDir, "meta")
	metaSettings := &types.MetaRocksDBSettings{
		WriteBufferMB:        cfg.RocksDB.Meta.WriteBufferMB,
		MaxWriteBufferNumber: cfg.RocksDB.Meta.MaxWriteBufferNumber,
		TargetFileSizeMB:     cfg.RocksDB.Meta.TargetFileSizeMB,
		BlockCacheMB:         cfg.RocksDB.Meta.BlockCacheMB,
	}
	metaStore, err := meta.NewMetaStore(metaStorePath, metaSettings)
	if err != nil {
		logger.Error("Failed to create meta store: %v", err)
		os.Exit(1)
	}
	defer metaStore.Close()

	coordinator := orchestrator.NewBackfillCoordinator(cfg, metaStore, logger)

	ctx := context.Background()
	if err := coordinator.Run(ctx); err != nil {
		logger.Error("Backfill coordinator failed: %v", err)
		os.Exit(1)
	}

	logger.Info("Ingestion workflow completed successfully")
}
