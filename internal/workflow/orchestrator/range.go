// =============================================================================
// range.go - Range Orchestrator (Single 10M Ledger Range)
// =============================================================================
//
// Manages ingestion and transition for a single ledger range (typically 10M ledgers).
//
// STATE MACHINE:
//   PENDING → INGESTING → TRANSITIONING → COMPLETE
//
// KEY RESPONSIBILITIES:
//   - Ingestion: Fetch ledgers from backend, extract transactions, store in RocksDB
//   - Checkpointing: Track progress every N ledgers for crash recovery
//   - Transition: Trigger compaction and conversion to immutable formats (LFS, RecSplit)
//
// CRASH RECOVERY:
//   - Reads last committed ledger from meta store on restart
//   - Resumes from resumeFrom = max(lastLedgerLCM, lastLedgerTxHash) + 1
//   - Uses fallthrough in state machine to continue where it left off
//
// =============================================================================

package orchestrator

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/config"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/stores/txhash/cf"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// =============================================================================
// Range Orchestrator Type
// =============================================================================

type rangeOrchestrator struct {
	rangeID           uint32
	backend           ledgerbackend.LedgerBackend
	metaStore         interfaces.MetaStore
	lcmStore          interfaces.LedgerStore
	txStore           interfaces.TxHashStore
	config            *config.Config
	log               interfaces.Logger
	startLedger       uint32
	endLedger         uint32
	networkPassphrase string
	state             string
}

func NewRangeOrchestrator(
	rangeID uint32,
	backend ledgerbackend.LedgerBackend,
	metaStore interfaces.MetaStore,
	lcmStore interfaces.LedgerStore,
	txStore interfaces.TxHashStore,
	config *config.Config,
	log interfaces.Logger,
) *rangeOrchestrator {
	if backend == nil {
		panic("NewRangeOrchestrator: backend is required")
	}
	if metaStore == nil {
		panic("NewRangeOrchestrator: metaStore is required")
	}
	if lcmStore == nil {
		panic("NewRangeOrchestrator: lcmStore is required")
	}
	if txStore == nil {
		panic("NewRangeOrchestrator: txStore is required")
	}
	if config == nil {
		panic("NewRangeOrchestrator: config is required")
	}
	if log == nil {
		panic("NewRangeOrchestrator: log is required")
	}

	startLedger, endLedger, err := calculateRangeBounds(config, rangeID)
	if err != nil {
		panic(fmt.Sprintf("NewRangeOrchestrator: %v", err))
	}

	scope := fmt.Sprintf("RANGE-%04d", rangeID)
	return &rangeOrchestrator{
		rangeID:           rangeID,
		backend:           backend,
		metaStore:         metaStore,
		lcmStore:          lcmStore,
		txStore:           txStore,
		config:            config,
		log:               log.WithScope(scope),
		startLedger:       startLedger,
		endLedger:         endLedger,
		networkPassphrase: networkPassphraseForConfig(config),
	}
}

// =============================================================================
// State Machine Execution (Run)
// =============================================================================

// Run executes the state machine for this range. Uses fallthrough to continue
// execution across states after transitions, enabling crash recovery to resume
// from the exact state where it left off.
func (ro *rangeOrchestrator) Run(ctx context.Context) error {
	rangeState, err := ro.metaStore.GetRangeState(ro.rangeID)
	if err != nil {
		return fmt.Errorf("failed to get range state: %w", err)
	}
	if rangeState == "" {
		rangeState = interfaces.RangeStatePending
	}
	ro.state = rangeState

	switch rangeState {
	case interfaces.RangeStatePending:
		ro.log.Info("Range %d starting (PENDING → INGESTING)", ro.rangeID)
		if err := ro.metaStore.SetRangeState(ro.rangeID, interfaces.RangeStateIngesting); err != nil {
			return fmt.Errorf("failed to set range state: %w", err)
		}
		if err := ro.ensureIngestingSubPhases(); err != nil {
			return fmt.Errorf("failed to initialize sub-phases: %w", err)
		}
		ro.state = interfaces.RangeStateIngesting
		// fallthrough: Continue immediately to ingestion without returning.
		// This enables a fresh range to start ingestion in the same Run() call.
		fallthrough

	case interfaces.RangeStateIngesting:
		if err := ro.ensureIngestingSubPhases(); err != nil {
			return fmt.Errorf("failed to initialize sub-phases: %w", err)
		}
		if err := ro.runIngestion(ctx); err != nil {
			return fmt.Errorf("ingestion failed: %w", err)
		}
		if _, _, err := ro.metaStore.MaybeTransitionRangeState(ro.rangeID); err != nil {
			return fmt.Errorf("failed to transition range state: %w", err)
		}
		ro.refreshState()
		// fallthrough: Continue to transitioning phase immediately after ingestion completes.
		fallthrough

	case interfaces.RangeStateTransitioning:
		if err := ro.runTransition(ctx); err != nil {
			return fmt.Errorf("transition failed: %w", err)
		}
		if _, _, err := ro.metaStore.MaybeTransitionRangeState(ro.rangeID); err != nil {
			return fmt.Errorf("failed to transition range state: %w", err)
		}
		ro.refreshState()

	case interfaces.RangeStateComplete:
		ro.log.Info("Range %d already complete", ro.rangeID)
	}

	return nil
}

// =============================================================================
// Public Getters
// =============================================================================

func (ro *rangeOrchestrator) GetRangeID() uint32 {
	return ro.rangeID
}

func (ro *rangeOrchestrator) GetState() string {
	if ro.state == "" {
		return interfaces.RangeStatePending
	}
	return ro.state
}

// =============================================================================
// Ingestion Phase
// =============================================================================

// runIngestion processes all ledgers in this range, extracting transaction hashes
// and storing both ledger data and tx mappings. Implements crash recovery by:
//  1. Reading last committed ledger from meta store
//  2. Calculating resumeFrom = min(lastLedgerLCM, lastLedgerTxHash) + 1
//  3. Checkpointing progress every N ledgers (default 1000)
func (ro *rangeOrchestrator) runIngestion(ctx context.Context) error {
	lastLedgerLCM, err := ro.metaStore.GetLastCommittedLedger(ro.rangeID, "ledger")
	if err != nil {
		return fmt.Errorf("failed to get last committed ledger (ledger): %w", err)
	}
	lastLedgerTxHash, err := ro.metaStore.GetLastCommittedLedger(ro.rangeID, "txhash")
	if err != nil {
		return fmt.Errorf("failed to get last committed ledger (txhash): %w", err)
	}

	// Crash recovery: Resume from the minimum of both stores + 1.
	// This ensures both stores stay in sync even if one crashes during a checkpoint.
	resumeFrom := helpers.MinUint32(lastLedgerLCM, lastLedgerTxHash) + 1
	if resumeFrom < ro.startLedger {
		resumeFrom = ro.startLedger
	}

	ledgerRange := ledgerbackend.BoundedRange(ro.startLedger, ro.endLedger)
	if err := ro.backend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("PrepareRange failed: %w", err)
	}

	checkpointInterval := uint32(ro.config.Backfill.CheckpointInterval)
	if checkpointInterval == 0 {
		checkpointInterval = 1000
	}

	ledgerCount := uint64(0)
	if lastLedgerLCM >= ro.startLedger {
		ledgerCount = uint64(lastLedgerLCM-ro.startLedger) + 1
	}

	txHashCounts, err := ro.metaStore.GetTxHashCounts(ro.rangeID)
	if err != nil {
		return fmt.Errorf("failed to get txhash counts: %w", err)
	}
	txHashCounts = ensureCFCounts(txHashCounts)

	lcmBatch := make(map[uint32][]byte)
	txHashBatch := make(map[string][]interfaces.Entry)

	for seq := resumeFrom; seq <= ro.endLedger; seq++ {
		lcm, err := ro.backend.GetLedger(ctx, seq)
		if err != nil {
			return fmt.Errorf("GetLedger(%d) failed: %w", seq, err)
		}

		lcmBytes, err := lcm.MarshalBinary()
		if err != nil {
			return fmt.Errorf("marshal ledger %d failed: %w", seq, err)
		}
		lcmBatch[seq] = lcmBytes
		if seq > lastLedgerLCM {
			ledgerCount++
		}

		txHashes, err := ro.extractTxHashes(lcm)
		if err != nil {
			return fmt.Errorf("extract tx hashes for ledger %d failed: %w", seq, err)
		}

		ledgerSeqBytes := helpers.Uint32ToBytes(seq)
		for _, txHash := range txHashes {
			cfName := cf.GetName(txHash)
			entry := interfaces.Entry{
				Key:   txHash,
				Value: ledgerSeqBytes,
			}
			txHashBatch[cfName] = append(txHashBatch[cfName], entry)
			if seq > lastLedgerTxHash {
				txHashCounts[cfName]++
			}
		}

		if seq%checkpointInterval == 0 {
			if err := ro.flushAndCheckpoint(seq, lcmBatch, txHashBatch, ledgerCount, txHashCounts); err != nil {
				return err
			}
			ro.log.Info("Range %d: checkpointed at ledger %d", ro.rangeID, seq)
			lcmBatch = make(map[uint32][]byte)
			txHashBatch = make(map[string][]interfaces.Entry)
		}
	}

	if err := ro.flushAndCheckpoint(ro.endLedger, lcmBatch, txHashBatch, ledgerCount, txHashCounts); err != nil {
		return err
	}

	if err := ro.metaStore.SetLedgerPhase(ro.rangeID, interfaces.LedgerPhaseCompacting); err != nil {
		return fmt.Errorf("failed to set ledger phase: %w", err)
	}
	if err := ro.metaStore.SetTxHashPhase(ro.rangeID, interfaces.TxHashPhaseCompacting); err != nil {
		return fmt.Errorf("failed to set txhash phase: %w", err)
	}

	ro.log.Info(
		"Range %d: ingestion complete, ledgers=%d, txhashes=%d",
		ro.rangeID,
		ledgerCount,
		sumCounts(txHashCounts),
	)

	return nil
}

// =============================================================================
// Transition Phase
// =============================================================================

func (ro *rangeOrchestrator) runTransition(ctx context.Context) error {
	ro.log.Info("Range %d: transition phase (compaction, LFS, RecSplit)", ro.rangeID)

	ledgerPhase, err := ro.metaStore.GetLedgerPhase(ro.rangeID)
	if err != nil {
		return fmt.Errorf("failed to get ledger phase: %w", err)
	}
	txhashPhase, err := ro.metaStore.GetTxHashPhase(ro.rangeID)
	if err != nil {
		return fmt.Errorf("failed to get txhash phase: %w", err)
	}

	ro.log.Info(
		"Range %d: ledger phase=%s, txhash phase=%s",
		ro.rangeID,
		ledgerPhase,
		txhashPhase,
	)

	return nil
}

// =============================================================================
// Helper Functions
// =============================================================================

// ensureIngestingSubPhases initializes ledger and txhash sub-phase tracking
// if not already set. Called when entering INGESTING state to ensure phase
// metadata exists for progress tracking.
func (ro *rangeOrchestrator) ensureIngestingSubPhases() error {
	ledgerPhase, err := ro.metaStore.GetLedgerPhase(ro.rangeID)
	if err != nil {
		return fmt.Errorf("failed to get ledger phase: %w", err)
	}
	if ledgerPhase == "" {
		if err := ro.metaStore.SetLedgerPhase(ro.rangeID, interfaces.LedgerPhaseIngesting); err != nil {
			return fmt.Errorf("failed to set ledger phase: %w", err)
		}
	}

	txhashPhase, err := ro.metaStore.GetTxHashPhase(ro.rangeID)
	if err != nil {
		return fmt.Errorf("failed to get txhash phase: %w", err)
	}
	if txhashPhase == "" {
		if err := ro.metaStore.SetTxHashPhase(ro.rangeID, interfaces.TxHashPhaseIngesting); err != nil {
			return fmt.Errorf("failed to set txhash phase: %w", err)
		}
	}

	return nil
}

func (ro *rangeOrchestrator) refreshState() {
	state, err := ro.metaStore.GetRangeState(ro.rangeID)
	if err != nil {
		return
	}
	if state != "" {
		ro.state = state
	}
}

// flushAndCheckpoint atomically writes batches to both stores and commits
// the checkpoint to meta store. This ensures crash recovery can resume from
// the exact ledger where the checkpoint occurred.
func (ro *rangeOrchestrator) flushAndCheckpoint(
	ledgerSeq uint32,
	lcmBatch map[uint32][]byte,
	txHashBatch map[string][]interfaces.Entry,
	ledgerCount uint64,
	txHashCounts map[string]uint64,
) error {
	if err := ro.lcmStore.WriteBatch(lcmBatch); err != nil {
		return fmt.Errorf("LCM WriteBatch failed: %w", err)
	}
	if _, err := ro.txStore.WriteBatchParallel(txHashBatch); err != nil {
		return fmt.Errorf("TxHash WriteBatch failed: %w", err)
	}

	if err := ro.metaStore.CommitCheckpoint(ro.rangeID, ledgerSeq, ledgerCount, txHashCounts); err != nil {
		return fmt.Errorf("CommitCheckpoint failed: %w", err)
	}

	return nil
}

func (ro *rangeOrchestrator) extractTxHashes(lcm xdr.LedgerCloseMeta) ([][]byte, error) {
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(ro.networkPassphrase, lcm)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var txHashes [][]byte
	for {
		tx, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		hash := copyBytes(tx.Hash[:])
		txHashes = append(txHashes, hash)
	}

	return txHashes, nil
}

func calculateRangeBounds(cfg *config.Config, rangeID uint32) (uint32, uint32, error) {
	start := uint64(cfg.Backfill.StartLedger) + uint64(rangeID)*uint64(config.LedgersPerRange)
	end := start + uint64(config.LedgersPerRange) - 1
	maxEnd := uint64(cfg.Backfill.EndLedger)
	if start > maxEnd {
		return 0, 0, fmt.Errorf("range %d start ledger %d exceeds end ledger %d", rangeID, start, maxEnd)
	}
	if end > maxEnd {
		end = maxEnd
	}
	return uint32(start), uint32(end), nil
}

func networkPassphraseForConfig(cfg *config.Config) string {
	if cfg == nil {
		return network.PublicNetworkPassphrase
	}

	mode := strings.ToLower(cfg.Service.Mode)
	if strings.Contains(mode, "testnet") {
		return network.TestNetworkPassphrase
	}

	bucketPath := strings.ToLower(cfg.Backfill.BufferedStorage.BucketPath)
	if strings.Contains(bucketPath, "testnet") {
		return network.TestNetworkPassphrase
	}

	return network.PublicNetworkPassphrase
}

func ensureCFCounts(counts map[string]uint64) map[string]uint64 {
	if counts == nil {
		counts = make(map[string]uint64)
	}
	for _, name := range cf.Names {
		if _, ok := counts[name]; !ok {
			counts[name] = 0
		}
	}
	return counts
}

func sumCounts(counts map[string]uint64) uint64 {
	var total uint64
	for _, count := range counts {
		total += count
	}
	return total
}

func copyBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	copyBuf := make([]byte, len(src))
	copy(copyBuf, src)
	return copyBuf
}

var _ interfaces.RangeOrchestrator = (*rangeOrchestrator)(nil)
