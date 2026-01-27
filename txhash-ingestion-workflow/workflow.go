// =============================================================================
// workflow.go - Phase Orchestration and Workflow Management
// =============================================================================
//
// This file implements the main workflow orchestrator that coordinates
// all phases of the txHash ingestion pipeline:
//
//   Phase 1: INGESTING           - Read LFS, extract txHashes, write to RocksDB
//   Phase 2: COMPACTING          - Full compaction of all 16 CFs
//   Phase 3: BUILDING_RECSPLIT   - Build RecSplit indexes
//   Phase 4: VERIFYING_RECSPLIT  - Verify RecSplit against RocksDB
//   Phase 5: COMPLETE            - Done
//
// WORKFLOW DESIGN:
//
//   The workflow is designed to be crash-recoverable at any point.
//   The MetaStore tracks progress, and each phase can be resumed:
//
//     INGESTING:
//       - Resume from last_committed_ledger + 1
//       - Up to 999 ledgers may be re-ingested (duplicates handled by compaction)
//
//     COMPACTING:
//       - Restart compaction for ALL CFs
//       - Compaction is idempotent
//
//     BUILDING_RECSPLIT:
//       - Delete temp/index files and rebuild from scratch
//       - RecSplit building is not resumable mid-CF
//
//     VERIFYING_RECSPLIT:
//       - Re-run verification for all 16 CFs (parallel, idempotent)
//
//     COMPLETE:
//       - Log "already complete" and exit 0
//
// SIGNAL HANDLING:
//
//   The workflow integrates with the QueryHandler for SIGHUP queries:
//     - SIGHUP during INGESTING/COMPACTING → triggers query
//     - SIGHUP during other phases → ignored
//
//   Graceful shutdown (SIGINT/SIGTERM) is handled in main.go.
//
// =============================================================================

package main

import (
	"fmt"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/compact"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/memory"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/recsplit"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/store"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/types"
	"github.com/karthikiyer56/stellar-full-history-ingestion/txhash-ingestion-workflow/pkg/verify"
)

// =============================================================================
// Workflow - Main Orchestrator
// =============================================================================

// Workflow orchestrates the entire txHash ingestion pipeline.
//
// LIFECYCLE:
//
//	workflow, err := NewWorkflow(config, logger)
//	if err != nil {
//	    // handle error
//	}
//	defer workflow.Close()
//
//	// Set query handler for SIGHUP support
//	workflow.SetQueryHandler(queryHandler)
//
//	// Run the workflow
//	if err := workflow.Run(); err != nil {
//	    // handle error
//	}
type Workflow struct {
	// config is the main configuration
	config *Config

	// logger is the main workflow logger
	logger interfaces.Logger

	// store is the RocksDB data store
	store interfaces.TxHashStore

	// meta is the meta store for checkpointing
	meta interfaces.MetaStore

	// memory is the memory monitor
	memory *memory.MemoryMonitor

	// queryHandler is the SIGHUP query handler (optional)
	queryHandler *QueryHandler

	// stats tracks overall workflow statistics
	stats *WorkflowStats

	// startTime is when the workflow started
	startTime time.Time
}

// WorkflowStats tracks overall workflow statistics.
type WorkflowStats struct {
	// StartTime when workflow began
	StartTime time.Time

	// EndTime when workflow completed
	EndTime time.Time

	// TotalTime is the total workflow duration
	TotalTime time.Duration

	// IsResume indicates if this was a resumed workflow
	IsResume bool

	// ResumedFromPhase is the phase we resumed from
	ResumedFromPhase types.Phase

	// IngestionTime is the time spent in ingestion phase
	IngestionTime time.Duration

	// CompactionTime is the time spent in compaction phase (RocksDB compaction only)
	CompactionTime time.Duration

	// CountVerifyTime is the time spent verifying counts after compaction
	CountVerifyTime time.Duration

	// RecSplitTime is the time spent building RecSplit indexes
	RecSplitTime time.Duration

	// VerificationTime is the time spent in verification phase
	VerificationTime time.Duration

	// TotalKeysIngested is the total number of txHash entries
	TotalKeysIngested uint64

	// TotalKeysVerified is the number of verified keys
	TotalKeysVerified uint64

	// VerificationFailures is the number of verification failures
	VerificationFailures uint64
}

// NewWorkflow creates a new Workflow orchestrator.
//
// This opens the RocksDB stores and initializes all components.
//
// PARAMETERS:
//   - config: Validated configuration
//   - logger: Logger instance
//
// RETURNS:
//   - A new Workflow instance
//   - An error if initialization fails
func NewWorkflow(config *Config, logger interfaces.Logger) (*Workflow, error) {
	// Open RocksDB data store
	logger.Info("Opening RocksDB data store...")
	txStore, err := store.OpenRocksDBTxHashStore(config.RocksDBPath, &config.RocksDB, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to open data store: %w", err)
	}

	// Open meta store
	logger.Info("Opening meta store...")
	meta, err := OpenRocksDBMetaStore(config.MetaStorePath)
	if err != nil {
		txStore.Close()
		return nil, fmt.Errorf("failed to open meta store: %w", err)
	}

	// Create memory monitor
	memMon := memory.NewMemoryMonitor(logger, RAMWarningThresholdGB)

	return &Workflow{
		config:    config,
		logger:    logger,
		store:     txStore,
		meta:      meta,
		memory:    memMon,
		stats:     &WorkflowStats{},
		startTime: time.Now(),
	}, nil
}

// SetQueryHandler sets the query handler for SIGHUP support.
//
// The query handler must be created separately (after the workflow is created)
// because it needs a reference to the store.
func (w *Workflow) SetQueryHandler(qh *QueryHandler) {
	w.queryHandler = qh
}

// Run executes the workflow from the current state.
//
// This is the main entry point that runs all phases in sequence,
// handling resume logic and phase transitions.
func (w *Workflow) Run() error {
	w.stats.StartTime = time.Now()

	w.logger.Separator()
	w.logger.Info("                    TXHASH INGESTION WORKFLOW")
	w.logger.Separator()
	w.logger.Info("")
	w.logger.Info("Start Time: %s", w.stats.StartTime.Format("2006-01-02 15:04:05"))
	w.logger.Info("")

	// Check resumability and determine starting point
	canResume, startFrom, phase, err := CheckResumability(w.meta, w.config.StartLedger, w.config.EndLedger)
	if err != nil {
		return fmt.Errorf("failed to check resumability: %w", err)
	}

	w.stats.IsResume = canResume
	w.stats.ResumedFromPhase = phase

	// Handle resume or fresh start
	if canResume {
		LogResumeState(w.meta, w.logger, startFrom, phase)

		// Check if already complete
		if phase == types.PhaseComplete {
			w.logger.Info("Workflow already complete. Nothing to do.")
			w.logger.Info("")
			return nil
		}
	} else {
		// Fresh start - set config in meta store
		w.logger.Info("Starting fresh workflow...")
		if err := w.meta.SetConfig(w.config.StartLedger, w.config.EndLedger); err != nil {
			return fmt.Errorf("failed to set config: %w", err)
		}
		phase = types.PhaseIngesting
		startFrom = w.config.StartLedger
	}

	// Take initial memory snapshot
	snapshot := memory.TakeMemorySnapshot()
	snapshot.Log(w.logger, "Initial")

	// Start query handler if available
	if w.queryHandler != nil {
		w.queryHandler.SetPhase(phase)
		w.queryHandler.Start()
	}

	// Execute phases based on current state
	if err := w.runFromPhase(phase, startFrom); err != nil {
		return err
	}

	// Workflow complete
	w.stats.EndTime = time.Now()
	w.stats.TotalTime = time.Since(w.stats.StartTime)

	// Log final summary
	w.logFinalSummary()

	return nil
}

// runFromPhase executes the workflow from a specific phase.
func (w *Workflow) runFromPhase(startPhase types.Phase, startFromLedger uint32) error {
	// Execute phases in order, starting from startPhase
	switch startPhase {
	case types.PhaseIngesting:
		if err := w.runIngestion(startFromLedger); err != nil {
			return err
		}
		fallthrough

	case types.PhaseCompacting:
		if err := w.runCompaction(); err != nil {
			return err
		}
		fallthrough

	case types.PhaseBuildingRecsplit:
		if err := w.runRecSplitBuild(); err != nil {
			return err
		}
		fallthrough

	case types.PhaseVerifyingRecsplit:
		if err := w.runVerification(); err != nil {
			return err
		}
		fallthrough

	case types.PhaseComplete:
		// Already handled above
	}

	return nil
}

// =============================================================================
// Phase Execution Methods
// =============================================================================

// runIngestion executes the ingestion phase.
func (w *Workflow) runIngestion(startFromLedger uint32) error {
	w.logger.Separator()
	w.logger.Info("                    PHASE 1: INGESTION")
	w.logger.Separator()
	w.logger.Info("")

	// Update query handler phase
	if w.queryHandler != nil {
		w.queryHandler.SetPhase(types.PhaseIngesting)
	}

	// Log start state
	isResume := startFromLedger > w.config.StartLedger
	LogIngestionStart(w.logger, w.config, isResume, startFromLedger)

	phaseStart := time.Now()

	// Choose between parallel and sequential ingestion
	var err error
	if w.config.SequentialIngestion {
		w.logger.Info("Using SEQUENTIAL ingestion mode")
		w.logger.Info("")
		err = RunIngestion(w.config, w.store, w.meta, w.logger, w.memory, startFromLedger)
	} else {
		w.logger.Info("Using PARALLEL ingestion mode")
		w.logger.Info("")
		err = RunParallelIngestion(w.config, w.store, w.meta, w.logger, w.memory, startFromLedger)
	}
	if err != nil {
		return fmt.Errorf("ingestion failed: %w", err)
	}

	w.stats.IngestionTime = time.Since(phaseStart)

	// Get total keys from CF counts
	cfCounts, _ := w.meta.GetCFCounts()
	var totalKeys uint64
	for _, count := range cfCounts {
		totalKeys += count
	}
	w.stats.TotalKeysIngested = totalKeys

	// Transition to next phase
	if err := w.meta.SetPhase(types.PhaseCompacting); err != nil {
		return fmt.Errorf("failed to set phase to COMPACTING: %w", err)
	}

	w.logger.Info("")
	w.logger.Info("Ingestion phase completed in %s", helpers.FormatDuration(w.stats.IngestionTime))
	w.logger.Info("Total keys ingested: %s", helpers.FormatNumber(int64(totalKeys)))
	w.logger.Info("")
	w.logger.Sync()

	return nil
}

// runCompaction executes the compaction phase.
func (w *Workflow) runCompaction() error {
	w.logger.Separator()
	w.logger.Info("                    PHASE 2: COMPACTION")
	w.logger.Separator()
	w.logger.Info("")

	// Update query handler phase
	if w.queryHandler != nil {
		w.queryHandler.SetPhase(types.PhaseCompacting)
	}

	// Run compaction (track time separately)
	compactionStart := time.Now()
	_, err := compact.RunCompaction(w.store, w.logger, w.memory)
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}
	w.stats.CompactionTime = time.Since(compactionStart)

	// Verify counts match after compaction (track time separately)
	// This catches count mismatches early before RecSplit build
	verifyStats, err := compact.VerifyCountsAfterCompaction(w.store, w.meta, w.logger)
	if err != nil {
		return fmt.Errorf("post-compaction count verification failed: %w", err)
	}
	w.stats.CountVerifyTime = verifyStats.TotalTime

	// Log warning if mismatches found (but don't abort - RecSplit will fail definitively)
	if !verifyStats.AllMatched {
		w.logger.Error("")
		w.logger.Error("WARNING: Count verification found %d mismatches!", verifyStats.Mismatches)
		w.logger.Error("Continuing to RecSplit build, which will fail if counts don't match.")
		w.logger.Error("")
	}

	// Transition to next phase
	if err := w.meta.SetPhase(types.PhaseBuildingRecsplit); err != nil {
		return fmt.Errorf("failed to set phase to BUILDING_RECSPLIT: %w", err)
	}

	w.logger.Info("")
	w.logger.Info("Compaction phase completed:")
	w.logger.Info("  RocksDB compaction: %s", helpers.FormatDuration(w.stats.CompactionTime))
	w.logger.Info("  Count verification: %s", helpers.FormatDuration(w.stats.CountVerifyTime))
	w.logger.Info("")
	w.logger.Sync()

	return nil
}

// runRecSplitBuild executes the RecSplit building phase.
func (w *Workflow) runRecSplitBuild() error {
	w.logger.Separator()
	w.logger.Info("                    PHASE 3: RECSPLIT BUILDING")
	w.logger.Separator()
	w.logger.Info("")

	// Update query handler phase (SIGHUP ignored during this phase)
	if w.queryHandler != nil {
		w.queryHandler.SetPhase(types.PhaseBuildingRecsplit)
	}

	phaseStart := time.Now()

	// Get CF counts from meta store
	cfCounts, err := w.meta.GetCFCounts()
	if err != nil {
		return fmt.Errorf("failed to get CF counts: %w", err)
	}

	// Build RecSplit indexes
	builder := recsplit.NewBuilder(
		w.store,
		cfCounts,
		w.config.RecsplitIndexPath,
		w.config.RecsplitTmpPath,
		w.config.MultiIndexEnabled,
		w.logger,
		w.memory,
	)
	stats, err := builder.Run()
	if err != nil {
		return fmt.Errorf("RecSplit build failed: %w", err)
	}

	w.stats.RecSplitTime = time.Since(phaseStart)

	// Transition to next phase
	if err := w.meta.SetPhase(types.PhaseVerifyingRecsplit); err != nil {
		return fmt.Errorf("failed to set phase to VERIFYING_RECSPLIT: %w", err)
	}

	w.logger.Info("")
	w.logger.Info("RecSplit build completed in %s", helpers.FormatDuration(stats.TotalTime))
	w.logger.Info("Total keys indexed: %s", helpers.FormatNumber(int64(stats.TotalKeys)))
	w.logger.Info("")
	w.logger.Sync()

	return nil
}

// runVerification executes the verification phase.
func (w *Workflow) runVerification() error {
	w.logger.Separator()
	w.logger.Info("                    PHASE 4: VERIFICATION")
	w.logger.Separator()
	w.logger.Info("")

	// Update query handler phase (SIGHUP ignored during this phase)
	if w.queryHandler != nil {
		w.queryHandler.SetPhase(types.PhaseVerifyingRecsplit)
	}

	// Check if resuming from a specific CF
	resumeFromCF, _ := w.meta.GetVerifyCF()

	phaseStart := time.Now()

	// Create verifier and run verification
	verifier := verify.NewVerifier(
		w.store,
		w.meta,
		w.config.RecsplitIndexPath,
		resumeFromCF,
		w.config.MultiIndexEnabled,
		w.logger,
		w.memory,
	)
	stats, err := verifier.Run()
	if err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	w.stats.VerificationTime = time.Since(phaseStart)
	w.stats.TotalKeysVerified = stats.TotalKeysVerified
	w.stats.VerificationFailures = stats.TotalFailures

	// Transition to complete phase
	if err := w.meta.SetPhase(types.PhaseComplete); err != nil {
		return fmt.Errorf("failed to set phase to COMPLETE: %w", err)
	}

	// Update query handler phase
	if w.queryHandler != nil {
		w.queryHandler.SetPhase(types.PhaseComplete)
	}

	w.logger.Info("")
	w.logger.Info("Verification completed in %s", helpers.FormatDuration(stats.TotalTime))
	w.logger.Info("Keys verified: %s", helpers.FormatNumber(int64(stats.TotalKeysVerified)))
	if stats.TotalFailures > 0 {
		w.logger.Error("Verification failures: %d", stats.TotalFailures)
	} else {
		w.logger.Info("Verification failures: 0")
	}
	w.logger.Info("")
	w.logger.Sync()

	return nil
}

// =============================================================================
// Summary and Cleanup
// =============================================================================

// logFinalSummary logs the final workflow summary.
func (w *Workflow) logFinalSummary() {
	w.logger.Separator()
	w.logger.Info("                    WORKFLOW COMPLETE")
	w.logger.Separator()
	w.logger.Info("")

	w.logger.Info("OVERALL STATISTICS:")
	w.logger.Info("  Start Time:        %s", w.stats.StartTime.Format("2006-01-02 15:04:05"))
	w.logger.Info("  End Time:          %s", w.stats.EndTime.Format("2006-01-02 15:04:05"))
	w.logger.Info("  Total Duration:    %s", helpers.FormatDuration(w.stats.TotalTime))
	w.logger.Info("")

	if w.stats.IsResume {
		w.logger.Info("  Resumed From:      %s", w.stats.ResumedFromPhase)
	}

	w.logger.Info("")
	w.logger.Info("PHASE DURATIONS:")
	w.logger.Info("  Ingestion:         %s", helpers.FormatDuration(w.stats.IngestionTime))
	w.logger.Info("  Compaction:        %s", helpers.FormatDuration(w.stats.CompactionTime))
	w.logger.Info("  Count Verify:      %s", helpers.FormatDuration(w.stats.CountVerifyTime))
	w.logger.Info("  RecSplit Build:    %s", helpers.FormatDuration(w.stats.RecSplitTime))
	w.logger.Info("  Verification:      %s", helpers.FormatDuration(w.stats.VerificationTime))
	w.logger.Info("")

	w.logger.Info("DATA STATISTICS:")
	w.logger.Info("  Keys Ingested:     %s", helpers.FormatNumber(int64(w.stats.TotalKeysIngested)))
	w.logger.Info("  Keys Verified:     %s", helpers.FormatNumber(int64(w.stats.TotalKeysVerified)))
	if w.stats.VerificationFailures > 0 {
		w.logger.Error("  Verify Failures:   %d", w.stats.VerificationFailures)
	} else {
		w.logger.Info("  Verify Failures:   0")
	}
	w.logger.Info("")

	// Final memory snapshot
	w.memory.LogSummary(w.logger)

	// Query handler stats
	if w.queryHandler != nil {
		batches, total, found, notFound := w.queryHandler.GetStats()
		if batches > 0 {
			w.logger.Info("SIGHUP QUERIES:")
			w.logger.Info("  Query Batches:     %d", batches)
			w.logger.Info("  Total Queries:     %d", total)
			w.logger.Info("  Found:             %d", found)
			w.logger.Info("  Not Found:         %d", notFound)
			w.logger.Info("")
		}
	}

	w.logger.Info("OUTPUT LOCATIONS:")
	w.logger.Info("  RocksDB:           %s", w.config.RocksDBPath)
	w.logger.Info("  RecSplit Indexes:  %s", w.config.RecsplitIndexPath)
	w.logger.Info("  Meta Store:        %s", w.config.MetaStorePath)
	w.logger.Info("")

	w.logger.Separator()
	w.logger.Info("                    SUCCESS")
	w.logger.Separator()
	w.logger.Info("")

	w.logger.Sync()
}

// Close releases all resources held by the workflow.
//
// This must be called when done with the workflow (use defer).
func (w *Workflow) Close() {
	w.logger.Info("Shutting down workflow...")

	// Stop query handler
	if w.queryHandler != nil {
		w.queryHandler.Stop()
	}

	// Stop memory monitor
	if w.memory != nil {
		w.memory.Stop()
	}

	// Close stores
	if w.store != nil {
		w.store.Close()
	}
	if w.meta != nil {
		w.meta.Close()
	}

	w.logger.Info("Workflow shutdown complete")
	w.logger.Sync()
}

// =============================================================================
// Accessor Methods
// =============================================================================

// GetStore returns the data store.
func (w *Workflow) GetStore() interfaces.TxHashStore {
	return w.store
}

// GetMeta returns the meta store.
func (w *Workflow) GetMeta() interfaces.MetaStore {
	return w.meta
}

// GetConfig returns the configuration.
func (w *Workflow) GetConfig() *Config {
	return w.config
}

// GetLogger returns the logger.
func (w *Workflow) GetLogger() interfaces.Logger {
	return w.logger
}

// GetStats returns the workflow statistics.
func (w *Workflow) GetStats() *WorkflowStats {
	return w.stats
}
