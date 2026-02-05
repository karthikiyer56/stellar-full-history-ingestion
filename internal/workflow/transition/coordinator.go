// =============================================================================
// coordinator.go - Transition Coordinator (Parallel Compaction + Indexing)
// =============================================================================
//
// Orchestrates the TRANSITIONING phase for a single range by running:
//   - Ledger path: compact LCM store → write LFS chunks → IMMUTABLE
//   - TxHash path: compact TxHash store → build RecSplit → verify → COMPLETE
//
// Both paths run in parallel. After both complete successfully, the coordinator:
//   - Calls MaybeTransitionRangeState() exactly once
//   - Optionally deletes active RocksDB stores (deletion gate)
// =============================================================================

package transition

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/config"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/interfaces"
)

// TransitionCoordinator orchestrates ledger + txhash transition phases.
type TransitionCoordinator struct {
	metaStore       interfaces.MetaStore
	lcmStore        interfaces.LedgerStore
	txStore         interfaces.TxHashStore
	lfsWriter       interfaces.LFSWriter
	recsplitBuilder interfaces.RecSplitBuilder
	dataDir         string
	config          *config.TransitionConfig
	log             interfaces.Logger
}

// NewTransitionCoordinator creates a new transition coordinator.
func NewTransitionCoordinator(
	metaStore interfaces.MetaStore,
	lcmStore interfaces.LedgerStore,
	txStore interfaces.TxHashStore,
	lfsWriter interfaces.LFSWriter,
	recsplitBuilder interfaces.RecSplitBuilder,
	dataDir string,
	cfg *config.TransitionConfig,
	log interfaces.Logger,
) *TransitionCoordinator {
	if metaStore == nil {
		panic("NewTransitionCoordinator: metaStore is required")
	}
	if lcmStore == nil {
		panic("NewTransitionCoordinator: lcmStore is required")
	}
	if txStore == nil {
		panic("NewTransitionCoordinator: txStore is required")
	}
	if lfsWriter == nil {
		panic("NewTransitionCoordinator: lfsWriter is required")
	}
	if recsplitBuilder == nil {
		panic("NewTransitionCoordinator: recsplitBuilder is required")
	}
	if cfg == nil {
		panic("NewTransitionCoordinator: config is required")
	}
	if log == nil {
		panic("NewTransitionCoordinator: log is required")
	}

	return &TransitionCoordinator{
		metaStore:       metaStore,
		lcmStore:        lcmStore,
		txStore:         txStore,
		lfsWriter:       lfsWriter,
		recsplitBuilder: recsplitBuilder,
		dataDir:         dataDir,
		config:          cfg,
		log:             log.WithScope("TRANSITION"),
	}
}

// Run executes the transition phase for the specified range.
func (tc *TransitionCoordinator) Run(rangeID uint32) error {
	if tc == nil {
		return fmt.Errorf("transition coordinator is nil")
	}

	tc.log.Info("Starting transition for range %04d", rangeID)

	rangeState, err := tc.metaStore.GetRangeState(rangeID)
	if err != nil {
		return fmt.Errorf("failed to get range state: %w", err)
	}
	if rangeState != interfaces.RangeStateTransitioning {
		return fmt.Errorf("range %d not in TRANSITIONING state (state=%s)", rangeID, rangeState)
	}

	var wg sync.WaitGroup
	var ledgerErr, txhashErr error

	wg.Add(2)

	// -------------------------------------------------------------------------
	// Goroutine 1: Ledger Path (Compact → LFS → IMMUTABLE)
	// -------------------------------------------------------------------------
	go func() {
		defer wg.Done()

		if err := tc.metaStore.SetLedgerPhase(rangeID, interfaces.LedgerPhaseCompacting); err != nil {
			ledgerErr = fmt.Errorf("failed to set ledger phase to COMPACTING: %w", err)
			return
		}
		tc.log.Info("Range %d: ledger phase → COMPACTING", rangeID)

		compactDuration, err := tc.lcmStore.Compact()
		if err != nil {
			ledgerErr = fmt.Errorf("ledger compaction failed: %w", err)
			return
		}
		tc.log.Info("Range %d: ledger compaction completed in %s", rangeID, helpers.FormatDuration(compactDuration))

		if err := tc.metaStore.SetLedgerPhase(rangeID, interfaces.LedgerPhaseWritingLFS); err != nil {
			ledgerErr = fmt.Errorf("failed to set ledger phase to WRITING_LFS: %w", err)
			return
		}
		tc.log.Info("Range %d: ledger phase → WRITING_LFS", rangeID)

		lfsStart := time.Now()
		if err := tc.lfsWriter.WriteRange(rangeID, tc.lcmStore, tc.metaStore); err != nil {
			ledgerErr = fmt.Errorf("LFS write failed: %w", err)
			return
		}
		tc.log.Info("Range %d: ledger transition completed in %s", rangeID, helpers.FormatDuration(time.Since(lfsStart)))
	}()

	// -------------------------------------------------------------------------
	// Goroutine 2: TxHash Path (Compact → RecSplit → Verify → COMPLETE)
	// -------------------------------------------------------------------------
	go func() {
		defer wg.Done()

		if err := tc.metaStore.SetTxHashPhase(rangeID, interfaces.TxHashPhaseCompacting); err != nil {
			txhashErr = fmt.Errorf("failed to set txhash phase to COMPACTING: %w", err)
			return
		}
		tc.log.Info("Range %d: txhash phase → COMPACTING", rangeID)

		compactStart := time.Now()
		compactDurations, err := tc.txStore.CompactAll()
		if err != nil {
			txhashErr = fmt.Errorf("txhash compaction failed: %w", err)
			return
		}
		tc.log.Info("Range %d: txhash compaction completed in %s", rangeID, helpers.FormatDuration(time.Since(compactStart)))
		for cfName, duration := range compactDurations {
			tc.log.Debug("Range %d: CF %s compaction duration %s", rangeID, cfName, helpers.FormatDuration(duration))
		}

		if err := tc.metaStore.SetTxHashPhase(rangeID, interfaces.TxHashPhaseBuildingRecSplit); err != nil {
			txhashErr = fmt.Errorf("failed to set txhash phase to BUILDING_RECSPLIT: %w", err)
			return
		}
		tc.log.Info("Range %d: txhash phase → BUILDING_RECSPLIT", rangeID)

		cfCounts, err := tc.metaStore.GetTxHashCounts(rangeID)
		if err != nil {
			txhashErr = fmt.Errorf("failed to get txhash counts: %w", err)
			return
		}

		recsplitStart := time.Now()
		if _, err := tc.recsplitBuilder.BuildAll(rangeID, cfCounts, tc.txStore); err != nil {
			txhashErr = fmt.Errorf("recsplit build failed: %w", err)
			return
		}
		tc.log.Info("Range %d: RecSplit build completed in %s", rangeID, helpers.FormatDuration(time.Since(recsplitStart)))

		if err := tc.metaStore.SetTxHashPhase(rangeID, interfaces.TxHashPhaseVerifying); err != nil {
			txhashErr = fmt.Errorf("failed to set txhash phase to VERIFYING_RECSPLIT: %w", err)
			return
		}
		tc.log.Info("Range %d: txhash phase → VERIFYING_RECSPLIT", rangeID)

		indexDir := filepath.Join(tc.dataDir, "immutable", "txhash", fmt.Sprintf("%04d", rangeID), "index")
		for cfName, count := range cfCounts {
			if count == 0 {
				continue
			}

			indexPath := filepath.Join(indexDir, fmt.Sprintf("cf-%s.idx", cfName))
			if !fileExists(indexPath) {
				txhashErr = fmt.Errorf("RecSplit index missing for CF %s: %s", cfName, indexPath)
				return
			}

			tc.log.Info("Verified RecSplit index for CF %s (%d keys)", cfName, count)
		}

		tc.log.Info("All RecSplit indexes verified successfully")

		if err := tc.metaStore.SetTxHashPhase(rangeID, interfaces.TxHashPhaseComplete); err != nil {
			txhashErr = fmt.Errorf("failed to set txhash phase to COMPLETE: %w", err)
			return
		}
		tc.log.Info("Range %d: txhash phase → COMPLETE", rangeID)
	}()

	// Wait for both goroutines to complete
	wg.Wait()

	// Error aggregation: Collect errors from BOTH paths before failing.
	// Why not fail-fast? We want to log complete failure context (both goroutine results)
	// so operators can see the full scope of issues (e.g., both compactions failed).
	if ledgerErr != nil || txhashErr != nil {
		if ledgerErr != nil && txhashErr != nil {
			return fmt.Errorf("transition errors: ledger=%v; txhash=%v", ledgerErr, txhashErr)
		}
		if ledgerErr != nil {
			return ledgerErr
		}
		return txhashErr
	}

	newState, transitioned, err := tc.metaStore.MaybeTransitionRangeState(rangeID)
	if err != nil {
		return fmt.Errorf("failed to transition range state: %w", err)
	}
	if transitioned {
		tc.log.Info("Range %d: state transitioned to %s", rangeID, newState)
	}

	// Deletion gate: Only delete RocksDB stores when range is COMPLETE.
	// Why check newState? MaybeTransitionRangeState() checks both ledger + txhash phases.
	// COMPLETE means: ledger=IMMUTABLE AND txhash=COMPLETE (both compactions + transitions done).
	// Safe to delete active stores only when both immutable replacements exist.
	if newState == interfaces.RangeStateComplete {
		if err := tc.maybeDeleteRocksDB(rangeID); err != nil {
			return err
		}
	}

	tc.log.Info("Transition completed for range %04d", rangeID)
	return nil
}

// maybeDeleteRocksDB deletes active RocksDB stores once transition is complete.
func (tc *TransitionCoordinator) maybeDeleteRocksDB(rangeID uint32) error {
	ledgerPhase, err := tc.metaStore.GetLedgerPhase(rangeID)
	if err != nil {
		return fmt.Errorf("failed to get ledger phase: %w", err)
	}
	txhashPhase, err := tc.metaStore.GetTxHashPhase(rangeID)
	if err != nil {
		return fmt.Errorf("failed to get txhash phase: %w", err)
	}

	// Double-check both phases before deletion (defense in depth).
	// Why check again? Caller already verified via MaybeTransitionRangeState, but
	// this method can be called independently. Ensures we never delete active stores
	// while immutable replacements (LFS chunks + RecSplit indexes) don't exist yet.
	if ledgerPhase == interfaces.LedgerPhaseImmutable && txhashPhase == interfaces.TxHashPhaseComplete {
		if tc.config.PreserveRocksDBAfterTransition {
			tc.log.Info("Preserving RocksDB stores (PreserveRocksDBAfterTransition=true)")
			return nil
		}

		tc.log.Info("Deleting RocksDB stores for range %04d", rangeID)
		if err := os.RemoveAll(activeLedgerStorePath(tc.dataDir, rangeID)); err != nil {
			return fmt.Errorf("failed to delete ledger RocksDB store: %w", err)
		}
		if err := os.RemoveAll(activeTxHashStorePath(tc.dataDir, rangeID)); err != nil {
			return fmt.Errorf("failed to delete txhash RocksDB store: %w", err)
		}
	}

	return nil
}

func activeLedgerStorePath(dataDir string, rangeID uint32) string {
	return filepath.Join(dataDir, "active", "rocksdb", fmt.Sprintf("%04d-ledger-store", rangeID))
}

func activeTxHashStorePath(dataDir string, rangeID uint32) string {
	return filepath.Join(dataDir, "active", "rocksdb", fmt.Sprintf("%04d-txhash-store", rangeID))
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
