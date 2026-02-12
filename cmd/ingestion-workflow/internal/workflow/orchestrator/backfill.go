// =============================================================================
// backfill.go - Backfill Coordinator (Parallel Range Management)
// =============================================================================
//
// Manages parallel ingestion across multiple 10M ledger ranges. Acts as the
// top-level orchestrator for the entire backfill operation.
//
// PARALLELISM:
//   - Runs up to N ranges concurrently (configured via ParallelRanges)
//   - Uses channel-based semaphore (activeRanges) to limit concurrency
//   - Each range is independent with its own backend and stores
//
// LIFECYCLE PER RANGE:
//   1. Create ledger backend (captive-core or buffered-storage)
//   2. Create RocksDB stores (ledger + txhash)
//   3. Create transition components (LFS writer, RecSplit builder)
//   4. Run range orchestrator (ingestion → transition)
//   5. Close all resources (defer cleanup)
//
// ERROR HANDLING:
//   - Collects errors from all ranges (doesn't fail-fast)
//   - Reports total failure count at end
//   - Allows partial success (some ranges complete, others fail)
//
// =============================================================================

package orchestrator

import (
	"context"
	"fmt"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/backend"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/config"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/stores/lcm"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/stores/txhash"
	transition2 "github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/transition"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/types"
	"path/filepath"
	"sync"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
)

// =============================================================================
// Backend Factory
// =============================================================================

// BackendFactory creates a LedgerBackend for a given range.
// Each range gets its own backend for isolation and parallel processing.
// Includes context for GCS initialization.
type BackendFactory func(ctx context.Context, rangeID uint32) (ledgerbackend.LedgerBackend, error)

// =============================================================================
// Backfill Coordinator Type
// =============================================================================

type backfillCoordinator struct {
	config         *config.Config
	metaStore      interfaces.MetaStore
	logger         interfaces.Logger
	backendFactory BackendFactory
}

// =============================================================================
// Constructor
// =============================================================================

func NewBackfillCoordinator(
	cfg *config.Config,
	metaStore interfaces.MetaStore,
	logger interfaces.Logger,
) *backfillCoordinator {
	if cfg == nil {
		panic("NewBackfillCoordinator: config is required")
	}
	if metaStore == nil {
		panic("NewBackfillCoordinator: metaStore is required")
	}
	if logger == nil {
		panic("NewBackfillCoordinator: logger is required")
	}

	return &backfillCoordinator{
		config:    cfg,
		metaStore: metaStore,
		logger:    logger.WithScope("BACKFILL"),
	}
}

// SetBackendFactory allows tests to inject mock backends.
func (bc *backfillCoordinator) SetBackendFactory(f BackendFactory) {
	bc.backendFactory = f
}

// =============================================================================
// Parallel Range Execution (Run)
// =============================================================================

// Run executes the backfill workflow for all ranges in parallel.
// Uses channel-based concurrency limiting (activeRanges semaphore pattern).
func (bc *backfillCoordinator) Run(ctx context.Context) error {
	totalRanges := bc.config.CalculateRangeCount()
	parallelRanges := bc.config.Backfill.ParallelRanges
	if parallelRanges < 1 {
		parallelRanges = 1
	}

	bc.logger.Info("Starting backfill for %d ranges (parallel=%d)", totalRanges, parallelRanges)

	// Concurrency limiting: Channel acts as semaphore with capacity = parallelRanges.
	// Each goroutine takes a slot (send), processes range, then releases slot (receive).
	activeRanges := make(chan struct{}, parallelRanges)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	errCount := 0

	for rangeID := uint32(0); rangeID < totalRanges; rangeID++ {
		activeRanges <- struct{}{} // Block if parallelRanges limit reached
		wg.Add(1)

		go func(id uint32) {
			defer func() {
				<-activeRanges // Release semaphore slot
				wg.Done()
			}()

			// Error aggregation: Don't fail-fast, collect all errors.
			// Allows partial success (some ranges complete even if others fail).
			if err := bc.processRange(ctx, id); err != nil {
				bc.logger.Error("Range %d: failed: %v", id, err)
				errMu.Lock()
				errCount++
				errMu.Unlock()
			}
		}(rangeID)
	}

	wg.Wait()

	if errCount > 0 {
		return fmt.Errorf("backfill completed with %d failed ranges", errCount)
	}

	bc.logger.Info("Backfill completed for all %d ranges", totalRanges)
	return nil
}

// =============================================================================
// Per-Range Lifecycle (processRange)
// =============================================================================

// processRange handles the complete lifecycle for a single range:
//  1. Create backend (why not shared: each range needs independent ledger access)
//  2. Create stores (LedgerStore, TxHashStore)
//  3. Create transition components (LFS writer, RecSplit builder)
//  4. Run range orchestrator
//  5. Clean up resources (deferred)
func (bc *backfillCoordinator) processRange(ctx context.Context, rangeID uint32) error {
	bc.logger.Info("Range %d: starting", rangeID)

	rangeBackend, err := bc.createBackendForRange(ctx, rangeID)
	if err != nil {
		return fmt.Errorf("failed to create backend: %w", err)
	}
	defer rangeBackend.Close()

	settings := ledgerRocksDBSettings(bc.config)
	lcmStore, err := lcm.NewRocksDbLedgerStore(bc.config.Service.DataDir, rangeID, settings)
	if err != nil {
		return fmt.Errorf("failed to create LCM store: %w", err)
	}
	defer lcmStore.Close()

	if _, err := lcmStore.Open(); err != nil {
		return fmt.Errorf("failed to open LCM store: %w", err)
	}

	txStore, err := txhash.NewRocksDbTxHashStore(bc.config.Service.DataDir, rangeID, txHashRocksDBSettings(bc.config))
	if err != nil {
		return fmt.Errorf("failed to create TxHash store: %w", err)
	}
	defer txStore.Close()

	if _, err := txStore.Open(); err != nil {
		return fmt.Errorf("failed to open TxHash store: %w", err)
	}

	rangeLogger := bc.logger.WithScope(fmt.Sprintf("RANGE-%04d", rangeID))
	lfsBasePath := filepath.Join(bc.config.Service.DataDir, bc.config.ImmutableStores.LedgersBase)
	lfsWriter, err := transition2.NewLFSWriter(lfsBasePath, rangeLogger)
	if err != nil {
		return fmt.Errorf("failed to create LFS writer: %w", err)
	}
	defer lfsWriter.Close()

	recsplitBuilder := transition2.NewRecSplitBuilder(bc.config.Service.DataDir, rangeLogger)

	transitionCoordinator := transition2.NewTransitionCoordinator(
		bc.metaStore,
		lcmStore,
		txStore,
		lfsWriter,
		recsplitBuilder,
		bc.config.Service.DataDir,
		&bc.config.Transition,
		rangeLogger,
	)

	rangeOrch := NewRangeOrchestrator(
		rangeID,
		rangeBackend,
		bc.metaStore,
		lcmStore,
		txStore,
		bc.config,
		bc.logger,
	)

	if err := rangeOrch.Run(ctx); err != nil {
		return fmt.Errorf("range orchestrator failed: %w", err)
	}

	state, err := bc.metaStore.GetRangeState(rangeID)
	if err != nil {
		return fmt.Errorf("failed to get range state: %w", err)
	}
	if state == interfaces.RangeStateTransitioning {
		if err := transitionCoordinator.Run(rangeID); err != nil {
			return fmt.Errorf("transition coordinator failed: %w", err)
		}
	}

	bc.logger.Info("Range %d: completed successfully", rangeID)
	return nil
}

// =============================================================================
// Helper Functions
// =============================================================================

// createBackendForRange creates a new backend for a specific range.
// Why not shared: Each range needs independent ledger access for parallel processing.
func (bc *backfillCoordinator) createBackendForRange(ctx context.Context, rangeID uint32) (ledgerbackend.LedgerBackend, error) {
	if bc.backendFactory != nil {
		return bc.backendFactory(ctx, rangeID)
	}

	return backend.NewGCSBackend(ctx, &bc.config.Backfill.BufferedStorage)
}

// Settings converters: Transform generic config settings into store-specific types.

func ledgerRocksDBSettings(cfg *config.Config) *types.LedgerRocksDBSettings {
	return &types.LedgerRocksDBSettings{
		WriteBufferMB:        cfg.RocksDB.Ledger.WriteBufferMB,
		MaxWriteBufferNumber: cfg.RocksDB.Ledger.MaxWriteBufferNumber,
		TargetFileSizeMB:     cfg.RocksDB.Ledger.TargetFileSizeMB,
		BlockCacheMB:         cfg.RocksDB.Ledger.BlockCacheMB,
	}
}

func txHashRocksDBSettings(cfg *config.Config) *types.TxHashRocksDBSettings {
	return &types.TxHashRocksDBSettings{
		WriteBufferMB:        cfg.RocksDB.TxHash.WriteBufferMB,
		MaxWriteBufferNumber: cfg.RocksDB.TxHash.MaxWriteBufferNumber,
		TargetFileSizeMB:     cfg.RocksDB.TxHash.TargetFileSizeMB,
		BlockCacheMB:         cfg.RocksDB.TxHash.BlockCacheMB,
	}
}

func metaRocksDBSettings(cfg *config.Config) *types.MetaRocksDBSettings {
	return &types.MetaRocksDBSettings{
		WriteBufferMB:        cfg.RocksDB.Meta.WriteBufferMB,
		MaxWriteBufferNumber: cfg.RocksDB.Meta.MaxWriteBufferNumber,
		TargetFileSizeMB:     cfg.RocksDB.Meta.TargetFileSizeMB,
		BlockCacheMB:         cfg.RocksDB.Meta.BlockCacheMB,
	}
}

var _ interfaces.BackfillCoordinator = (*backfillCoordinator)(nil)
