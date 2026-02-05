package interfaces

import (
	"context"
	"time"
)

// Phase constants - two-level tracking
const (
	// Range-level states
	RangeStatePending       = "PENDING"
	RangeStateIngesting     = "INGESTING"
	RangeStateTransitioning = "TRANSITIONING"
	RangeStateComplete      = "COMPLETE"

	// Ledger sub-phases (INCLUDES COMPACTING)
	LedgerPhaseIngesting  = "INGESTING"
	LedgerPhaseCompacting = "COMPACTING" // Compact RocksDB before LFS export
	LedgerPhaseWritingLFS = "WRITING_LFS"
	LedgerPhaseImmutable  = "IMMUTABLE"

	// TxHash sub-phases
	TxHashPhaseIngesting        = "INGESTING"
	TxHashPhaseCompacting       = "COMPACTING"
	TxHashPhaseBuildingRecSplit = "BUILDING_RECSPLIT"
	TxHashPhaseVerifying        = "VERIFYING_RECSPLIT"
	TxHashPhaseComplete         = "COMPLETE"
)

/*
	Each of the 3 rocksdb stores has its own interface here.
	Implementations are in internal/workflow/stores/...

	This design choice of having separate interfaces for the 3 stores, as opposed to a single "type Rocksdb interface"
	is deliberate since each store has different methods and responsibilities, but more importantly, the key value types are different,
	and so are the access patterns. Trying to unify them into a single interface would lead to awkward method signatures
	We want to avoid a "one size fits all" interface that ends up being a leaky abstraction.
*/

// MetaStore tracks workflow state and checkpoints (two-level phase tracking)
type MetaStore interface {
	// Range-level state management
	GetRangeState(rangeID uint32) (string, error)
	SetRangeState(rangeID uint32, state string) error

	// Ledger sub-phase management
	GetLedgerPhase(rangeID uint32) (string, error)
	SetLedgerPhase(rangeID uint32, phase string) error

	// TxHash sub-phase management
	GetTxHashPhase(rangeID uint32) (string, error)
	SetTxHashPhase(rangeID uint32, phase string) error

	// Ingestion checkpointing (during INGESTING phase)
	GetLastCommittedLedger(rangeID uint32, subPhase string) (uint32, error) // subPhase: "ledger" or "txhash"
	CommitCheckpoint(rangeID, ledgerSeq uint32, ledgerCount uint64, txHashCounts map[string]uint64) error

	// LFS progress (during WRITING_LFS phase)
	// Returns last FULLY COMMITTED chunk ID as int32:
	//   - Returns -1 if no chunks written yet (fresh start)
	//   - Returns 0 if chunk 0 was written (resume from chunk 1)
	//   - Returns N if chunk N was written (resume from chunk N+1)
	// This avoids uint32 sentinel ambiguity where 0 could mean "unset" or "chunk 0 written"
	GetLFSLastChunkWritten(rangeID uint32) (int32, error)
	SetLFSLastChunkWritten(rangeID uint32, chunkID int32) error

	// Counts for RecSplit
	GetTxHashCounts(rangeID uint32) (map[string]uint64, error)

	// Transition state checks
	// Returns true when BOTH ledger and txhash sub-phases finished ingestion
	BothSubPhasesFinishedIngesting(rangeID uint32) (bool, error)
	// Returns true when ledger:phase=IMMUTABLE AND txhash:phase=COMPLETE
	BothSubPhasesComplete(rangeID uint32) (bool, error)

	// MaybeTransitionRangeState checks sub-phase conditions and transitions range state if appropriate.
	// Call this after updating sub-phases to trigger range-level state transitions:
	//   - INGESTING → TRANSITIONING: when BothSubPhasesFinishedIngesting() is true
	//   - TRANSITIONING → COMPLETE: when BothSubPhasesComplete() is true
	// Returns the new state (or current state if no transition occurred).
	MaybeTransitionRangeState(rangeID uint32) (newState string, transitioned bool, err error)

	Close() error
}

// LedgerStore stores ledger data in RocksDB
type LedgerStore interface {
	Open() (openDuration time.Duration, err error)
	WriteBatch(entries map[uint32][]byte) error
	Get(ledgerSeq uint32) ([]byte, error)
	NewIterator() Iterator
	Compact() (time.Duration, error)
	GetPath() string
	GetSize() (int64, error)
	Close() error
}

// TxHashStore stores txhash→ledgerSeq in RocksDB with 16 CFs
type TxHashStore interface {
	Open() (openDuration time.Duration, err error)
	WriteBatch(entriesByCF map[string][]Entry) error
	Get(txHash []byte) (uint32, bool, error)
	NewScanIteratorCF(cfName string) Iterator
	CompactAll() (map[string]time.Duration, error)
	GetPath() string
	GetSize() (int64, error)
	Close() error
}

// LFSWriter writes LFS chunks from LedgerStore
type LFSWriter interface {
	WriteRange(rangeID uint32, lcmStore LedgerStore, metaStore MetaStore) error
	Close() error
}

// RecSplitBuilder builds RecSplit indexes from TxHashStore
type RecSplitBuilder interface {
	BuildAll(rangeID uint32, cfCounts map[string]uint64, store TxHashStore) (map[string]time.Duration, error)
}

// RangeOrchestrator handles a single 10M range
type RangeOrchestrator interface {
	Run(ctx context.Context) error
	GetRangeID() uint32
	GetState() string
}

// BackfillCoordinator manages multiple RangeOrchestrators
type BackfillCoordinator interface {
	Run(ctx context.Context) error
}

// Iterator for RocksDB iteration
type Iterator interface {
	SeekToFirst()
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Error() error
	Close()
}

// Entry for batch writes
type Entry struct {
	Key   []byte
	Value []byte
}

// Logger is the logging interface used throughout the workflow.
// Implementations should support dual output (log file + error file) per CLAUDE.md requirements.
// See Task 5 for implementation details.
type Logger interface {
	Info(format string, args ...interface{})
	Error(format string, args ...interface{})
	Debug(format string, args ...interface{})
	Warn(format string, args ...interface{})
	WithScope(scope string) Logger // Returns scoped logger with [SCOPE] prefix
	Separator()                    // Log visual separator line
	Sync()                         // Flush buffers to disk
}
