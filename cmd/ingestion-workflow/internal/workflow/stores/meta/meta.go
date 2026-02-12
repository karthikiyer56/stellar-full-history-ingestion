// =============================================================================
// meta.go - MetaStore Implementation with Two-Level Phase Tracking
// =============================================================================
//
// This file implements the MetaStore interface for the ingestion workflow.
// It uses RocksDB for persistence and supports:
//   - Two-level phase tracking (range state + sub-phases)
//   - LFS checkpoint progress with int32 chunk IDs
//   - Atomic checkpointing using WriteBatch
//   - Compact CF counts serialization (NOT JSON)
//
// STORAGE FORMAT:
//   - uint32: 4-byte big-endian (using helpers.Uint32ToBytes)
//   - uint64: 8-byte big-endian (using helpers.Uint64ToBytes)
//   - int32: 4-byte big-endian (for LFS chunk IDs with -1 sentinel)
//   - strings: UTF-8 bytes
//   - cf_counts: "0:N,1:M,...,f:K" (compact string, NOT JSON)
//
// PHASE TRANSITIONS:
//   PENDING → INGESTING (manual, when ingestion starts)
//   INGESTING → TRANSITIONING (when both sub-phases finish ingestion)
//   TRANSITIONING → COMPLETE (when ledger=IMMUTABLE AND txhash=COMPLETE)
//
// =============================================================================

package meta

import (
	"encoding/binary"
	"fmt"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/stores/rocksdb"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/types"
	"strconv"
	"strings"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/linxGnu/grocksdb"
)

// =============================================================================
// CF Names (for compact string serialization)
// =============================================================================

// cfNames defines the 16 column family names (0-f)
var cfNames = []string{
	"0", "1", "2", "3", "4", "5", "6", "7",
	"8", "9", "a", "b", "c", "d", "e", "f",
}

// =============================================================================
// metaStore Implementation
// =============================================================================

// metaStore implements the MetaStore interface using RocksDB.
type metaStore struct {
	rocksdb.BaseStore
}

// NewMetaStore creates a new meta store at the given path.
// Path: typically {data_dir}/meta
// settings: optional per-store RocksDB tuning parameters. If nil, uses sensible defaults.
func NewMetaStore(path string, settings *types.MetaRocksDBSettings) (*metaStore, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	// Apply per-store settings if provided
	var blockCache *grocksdb.Cache
	if settings != nil {
		if settings.WriteBufferMB > 0 {
			opts.SetWriteBufferSize(uint64(settings.WriteBufferMB * 1024 * 1024))
		}
		if settings.MaxWriteBufferNumber > 0 {
			opts.SetMaxWriteBufferNumber(settings.MaxWriteBufferNumber)
		}
		if settings.TargetFileSizeMB > 0 {
			opts.SetTargetFileSizeBase(uint64(settings.TargetFileSizeMB * 1024 * 1024))
		}
		if settings.BlockCacheMB > 0 {
			blockCache = grocksdb.NewLRUCache(uint64(settings.BlockCacheMB * 1024 * 1024))
			bbto := grocksdb.NewDefaultBlockBasedTableOptions()
			bbto.SetBlockCache(blockCache)
			opts.SetBlockBasedTableFactory(bbto)
		}
	}

	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		opts.Destroy()
		if blockCache != nil {
			blockCache.Destroy()
		}
		return nil, fmt.Errorf("failed to open meta store at %s: %w", path, err)
	}

	readOpts := grocksdb.NewDefaultReadOptions()
	writeOpts := grocksdb.NewDefaultWriteOptions()
	writeOpts.SetSync(true) // Ensure durability

	return &metaStore{
		BaseStore: rocksdb.BaseStore{
			DB:         db,
			Opts:       opts,
			ReadOpts:   readOpts,
			WriteOpts:  writeOpts,
			BlockCache: blockCache,
			Path:       path,
		},
	}, nil
}

// =============================================================================
// Range-Level State Management
// =============================================================================

// GetRangeState returns the range-level state.
// Returns empty string if not set.
func (ms *metaStore) GetRangeState(rangeID uint32) (string, error) {
	key := fmt.Sprintf("range:%d:state", rangeID)
	return ms.getString(key)
}

// SetRangeState updates the range-level state.
func (ms *metaStore) SetRangeState(rangeID uint32, state string) error {
	key := fmt.Sprintf("range:%d:state", rangeID)
	return ms.DB.Put(ms.WriteOpts, []byte(key), []byte(state))
}

// =============================================================================
// Ledger Sub-Phase Management
// =============================================================================

// GetLedgerPhase returns the ledger sub-phase.
// Returns empty string if not set.
func (ms *metaStore) GetLedgerPhase(rangeID uint32) (string, error) {
	key := fmt.Sprintf("range:%d:ledger:phase", rangeID)
	return ms.getString(key)
}

// SetLedgerPhase updates the ledger sub-phase.
func (ms *metaStore) SetLedgerPhase(rangeID uint32, phase string) error {
	key := fmt.Sprintf("range:%d:ledger:phase", rangeID)
	return ms.DB.Put(ms.WriteOpts, []byte(key), []byte(phase))
}

// =============================================================================
// TxHash Sub-Phase Management
// =============================================================================

// GetTxHashPhase returns the txhash sub-phase.
// Returns empty string if not set.
func (ms *metaStore) GetTxHashPhase(rangeID uint32) (string, error) {
	key := fmt.Sprintf("range:%d:txhash:phase", rangeID)
	return ms.getString(key)
}

// SetTxHashPhase updates the txhash sub-phase.
func (ms *metaStore) SetTxHashPhase(rangeID uint32, phase string) error {
	key := fmt.Sprintf("range:%d:txhash:phase", rangeID)
	return ms.DB.Put(ms.WriteOpts, []byte(key), []byte(phase))
}

// =============================================================================
// Ingestion Checkpointing
// =============================================================================

// GetLastCommittedLedger returns the last committed ledger for a sub-phase.
// subPhase: "ledger" or "txhash"
// Returns 0 if not set.
func (ms *metaStore) GetLastCommittedLedger(rangeID uint32, subPhase string) (uint32, error) {
	key := fmt.Sprintf("range:%d:%s:last_committed_ledger", rangeID, subPhase)
	return ms.getUint32(key)
}

// CommitCheckpoint atomically updates progress after a successful batch.
// All updates are written in a single RocksDB WriteBatch.
func (ms *metaStore) CommitCheckpoint(rangeID, ledgerSeq uint32, ledgerCount uint64, txHashCounts map[string]uint64) error {
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Update ledger sub-phase checkpoint
	ledgerKey := fmt.Sprintf("range:%d:ledger:last_committed_ledger", rangeID)
	batch.Put([]byte(ledgerKey), helpers.Uint32ToBytes(ledgerSeq))

	ledgerCountKey := fmt.Sprintf("range:%d:ledger:count", rangeID)
	batch.Put([]byte(ledgerCountKey), helpers.Uint64ToBytes(ledgerCount))

	// Update txhash sub-phase checkpoint
	txhashKey := fmt.Sprintf("range:%d:txhash:last_committed_ledger", rangeID)
	batch.Put([]byte(txhashKey), helpers.Uint32ToBytes(ledgerSeq))

	txhashCountsKey := fmt.Sprintf("range:%d:txhash:cf_counts", rangeID)
	batch.Put([]byte(txhashCountsKey), []byte(serializeCFCounts(txHashCounts)))

	return ms.DB.Write(ms.WriteOpts, batch)
}

// =============================================================================
// LFS Progress Tracking
// =============================================================================

// GetLFSLastChunkWritten returns the last fully committed LFS chunk ID.
// Returns -1 if no chunks have been written yet (fresh start).
// Returns 0 if chunk 0 was written (resume from chunk 1).
// Returns N if chunk N was written (resume from chunk N+1).
func (ms *metaStore) GetLFSLastChunkWritten(rangeID uint32) (int32, error) {
	key := fmt.Sprintf("range:%d:ledger:lfs_last_chunk_written", rangeID)

	// Check if key exists explicitly to distinguish "unset" from "chunk 0 written"
	slice, err := ms.DB.Get(ms.ReadOpts, []byte(key))
	if err != nil {
		return -1, err
	}
	defer slice.Free()

	if !slice.Exists() {
		return -1, nil // Unset - no chunks written
	}

	if slice.Size() != 4 {
		return -1, fmt.Errorf("invalid int32 size: %d bytes", slice.Size())
	}

	return bytesToInt32(slice.Data()), nil
}

// SetLFSLastChunkWritten updates the last fully committed LFS chunk ID.
// chunkID: int32 with -1 as sentinel for "no chunks written yet"
func (ms *metaStore) SetLFSLastChunkWritten(rangeID uint32, chunkID int32) error {
	key := fmt.Sprintf("range:%d:ledger:lfs_last_chunk_written", rangeID)
	return ms.DB.Put(ms.WriteOpts, []byte(key), int32ToBytes(chunkID))
}

// =============================================================================
// TxHash Counts
// =============================================================================

// GetTxHashCounts returns the per-CF txhash counts.
// Returns empty map if not set.
func (ms *metaStore) GetTxHashCounts(rangeID uint32) (map[string]uint64, error) {
	key := fmt.Sprintf("range:%d:txhash:cf_counts", rangeID)
	value, err := ms.getString(key)
	if err != nil {
		return nil, err
	}

	if value == "" {
		// Return empty counts
		counts := make(map[string]uint64)
		for _, cfName := range cfNames {
			counts[cfName] = 0
		}
		return counts, nil
	}

	return parseCFCounts(value), nil
}

// =============================================================================
// State Transition Checks
// =============================================================================

// BothSubPhasesFinishedIngesting returns true when BOTH ledger and txhash
// sub-phases have left the INGESTING phase.
func (ms *metaStore) BothSubPhasesFinishedIngesting(rangeID uint32) (bool, error) {
	ledgerPhase, err := ms.GetLedgerPhase(rangeID)
	if err != nil {
		return false, err
	}

	txhashPhase, err := ms.GetTxHashPhase(rangeID)
	if err != nil {
		return false, err
	}

	return ledgerPhase != interfaces.LedgerPhaseIngesting &&
		txhashPhase != interfaces.TxHashPhaseIngesting, nil
}

// BothSubPhasesComplete returns true when ledger is IMMUTABLE and txhash is COMPLETE.
func (ms *metaStore) BothSubPhasesComplete(rangeID uint32) (bool, error) {
	ledgerPhase, err := ms.GetLedgerPhase(rangeID)
	if err != nil {
		return false, err
	}

	txhashPhase, err := ms.GetTxHashPhase(rangeID)
	if err != nil {
		return false, err
	}

	return ledgerPhase == interfaces.LedgerPhaseImmutable &&
		txhashPhase == interfaces.TxHashPhaseComplete, nil
}

// =============================================================================
// Range State Transitions
// =============================================================================

// MaybeTransitionRangeState checks sub-phase conditions and transitions range state if appropriate.
// This implements the MetaStore interface method.
//
// Transitions:
//   - INGESTING → TRANSITIONING: when BothSubPhasesFinishedIngesting() is true
//   - TRANSITIONING → COMPLETE: when BothSubPhasesComplete() is true
//
// Returns the new state (or current state if no transition occurred).
func (ms *metaStore) MaybeTransitionRangeState(rangeID uint32) (string, bool, error) {
	rangeState, err := ms.GetRangeState(rangeID)
	if err != nil {
		return "", false, err
	}

	switch rangeState {
	case interfaces.RangeStateIngesting:
		// Transition to TRANSITIONING when BOTH sub-phases finish ingestion
		finished, err := ms.BothSubPhasesFinishedIngesting(rangeID)
		if err != nil {
			return rangeState, false, err
		}
		if finished {
			if err := ms.SetRangeState(rangeID, interfaces.RangeStateTransitioning); err != nil {
				return rangeState, false, err
			}
			return interfaces.RangeStateTransitioning, true, nil
		}

	case interfaces.RangeStateTransitioning:
		// Transition to COMPLETE when BOTH sub-phases are terminal
		complete, err := ms.BothSubPhasesComplete(rangeID)
		if err != nil {
			return rangeState, false, err
		}
		if complete {
			if err := ms.SetRangeState(rangeID, interfaces.RangeStateComplete); err != nil {
				return rangeState, false, err
			}
			return interfaces.RangeStateComplete, true, nil
		}
	}

	return rangeState, false, nil
}

// =============================================================================
// Close
// =============================================================================

// Close releases all resources.
func (ms *metaStore) Close() error {
	return ms.CloseBase()
}

// =============================================================================
// Internal Helper Methods
// =============================================================================

// getString reads a string value from RocksDB.
func (ms *metaStore) getString(key string) (string, error) {
	slice, err := ms.DB.Get(ms.ReadOpts, []byte(key))
	if err != nil {
		return "", err
	}
	defer slice.Free()

	if !slice.Exists() {
		return "", nil
	}

	return string(slice.Data()), nil
}

// getUint32 reads a uint32 value from RocksDB.
func (ms *metaStore) getUint32(key string) (uint32, error) {
	slice, err := ms.DB.Get(ms.ReadOpts, []byte(key))
	if err != nil {
		return 0, err
	}
	defer slice.Free()

	if !slice.Exists() || slice.Size() != 4 {
		return 0, nil
	}

	return helpers.BytesToUint32(slice.Data()), nil
}

// int32ToBytes converts an int32 to 4-byte big-endian slice.
func int32ToBytes(n int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(n))
	return b
}

// bytesToInt32 converts a 4-byte big-endian slice to int32.
func bytesToInt32(b []byte) int32 {
	if len(b) != 4 {
		return 0
	}
	return int32(binary.BigEndian.Uint32(b))
}

// =============================================================================
// CF Counts Serialization
// =============================================================================

// serializeCFCounts serializes CF counts to a compact string.
// Format: "cf:count,cf:count,..."
// Example: "0:125000,1:123500,2:124000,...,f:126000"
//
// NOTE: This is NOT JSON. It's a compact string format matching
// txhash-ingestion-workflow/meta_store.go:478-515
func serializeCFCounts(counts map[string]uint64) string {
	var parts []string
	for _, cfName := range cfNames {
		count := counts[cfName]
		parts = append(parts, fmt.Sprintf("%s:%d", cfName, count))
	}
	return strings.Join(parts, ",")
}

// parseCFCounts parses CF counts from a compact string.
func parseCFCounts(s string) map[string]uint64 {
	counts := make(map[string]uint64)

	// Initialize with zeros
	for _, cfName := range cfNames {
		counts[cfName] = 0
	}

	if s == "" {
		return counts
	}

	parts := strings.Split(s, ",")
	for _, part := range parts {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			continue
		}
		cf := kv[0]
		count, err := strconv.ParseUint(kv[1], 10, 64)
		if err != nil {
			continue
		}
		counts[cf] = count
	}

	return counts
}

// =============================================================================
// Compile-Time Interface Check
// =============================================================================

var _ interfaces.MetaStore = (*metaStore)(nil)
