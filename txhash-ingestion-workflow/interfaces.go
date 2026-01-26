// =============================================================================
// interfaces.go - Store Interfaces for DRY Principle
// =============================================================================
//
// This file defines the core interfaces used throughout the txhash-ingestion-workflow.
// By using interfaces, we achieve:
//
//   1. TESTABILITY: Mock implementations can be injected for unit testing
//   2. DRY (Don't Repeat Yourself): Common patterns are abstracted once
//   3. SEPARATION OF CONCERNS: Data store vs meta store have distinct interfaces
//   4. FLEXIBILITY: Easy to swap implementations (e.g., different storage backends)
//
// ARCHITECTURE OVERVIEW:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                         txhash-ingestion-workflow                        │
//   ├─────────────────────────────────────────────────────────────────────────┤
//   │                                                                         │
//   │   ┌─────────────────┐                    ┌─────────────────┐           │
//   │   │  TxHashStore    │                    │   MetaStore     │           │
//   │   │  (Interface)    │                    │   (Interface)   │           │
//   │   └────────┬────────┘                    └────────┬────────┘           │
//   │            │                                      │                     │
//   │            ▼                                      ▼                     │
//   │   ┌─────────────────┐                    ┌─────────────────┐           │
//   │   │ RocksDBTxHash   │                    │ RocksDBMeta     │           │
//   │   │ Store (Impl)    │                    │ Store (Impl)    │           │
//   │   └─────────────────┘                    └─────────────────┘           │
//   │                                                                         │
//   │   Data: txHash → ledgerSeq              Progress tracking:              │
//   │   16 Column Families (0-f)              - last_committed_ledger        │
//   │                                         - cf_counts                     │
//   │                                         - phase                         │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package main

import (
	"time"
)

// =============================================================================
// Phase Enum
// =============================================================================

// Phase represents the current workflow phase.
// Phases progress linearly: INGESTING → COMPACTING → BUILDING_RECSPLIT → VERIFYING → COMPLETE
//
// CRASH RECOVERY BEHAVIOR BY PHASE:
//
//	INGESTING:
//	  - Resume from last_committed_ledger + 1
//	  - Re-ingest up to 999 ledgers (duplicates are fine, compaction dedupes)
//	  - Counts from meta store are accurate for all committed batches
//
//	COMPACTING:
//	  - Restart compaction for ALL 16 CFs
//	  - Compaction is idempotent (re-compacting is safe)
//
//	BUILDING_RECSPLIT:
//	  - Delete all temp and index files
//	  - Rebuild all RecSplit indexes from scratch
//
//	VERIFYING:
//	  - Resume from the CF stored in verify_cf
//	  - Restart that CF from the beginning
//
//	COMPLETE:
//	  - Log "Already complete" and exit 0
type Phase string

const (
	PhaseIngesting        Phase = "INGESTING"
	PhaseCompacting       Phase = "COMPACTING"
	PhaseBuildingRecsplit Phase = "BUILDING_RECSPLIT"
	PhaseVerifying        Phase = "VERIFYING"
	PhaseComplete         Phase = "COMPLETE"
)

// String returns the string representation of the phase.
func (p Phase) String() string {
	return string(p)
}

// =============================================================================
// Entry - Key-Value Pair for Batch Writes
// =============================================================================

// Entry represents a txHash → ledgerSeq mapping.
//
// KEY FORMAT:
//   - 32 bytes: Transaction hash (raw bytes, not hex encoded)
//   - First byte's high nibble determines column family (0-f)
//
// VALUE FORMAT:
//   - 4 bytes: Ledger sequence number (big-endian uint32)
//
// EXAMPLE:
//
//	txHash  = 0x0a1b2c3d... (32 bytes, first nibble = 0 → CF "0")
//	ledgerSeq = 12345678 → [0x00, 0xbc, 0x61, 0x4e] (big-endian)
type Entry struct {
	Key   []byte // 32-byte transaction hash
	Value []byte // 4-byte ledger sequence (big-endian)
}

// =============================================================================
// CFStats - Per-Column Family Statistics
// =============================================================================

// CFLevelStats holds per-level statistics for a column family.
// RocksDB uses levels L0-L6 for its LSM tree structure.
//
// LEVEL OVERVIEW:
//
//	L0: Recently flushed MemTables (unsorted, overlapping)
//	L1-L6: Sorted, non-overlapping files (progressively larger)
//
// During ingestion (no compaction), all files land in L0.
// After compaction, files are organized into L6 (fully compacted).
type CFLevelStats struct {
	Level     int   // Level number (0-6)
	FileCount int64 // Number of SST files at this level
	Size      int64 // Total size in bytes at this level
}

// CFStats holds comprehensive statistics for a single column family.
type CFStats struct {
	Name          string         // Column family name ("0" through "f")
	EstimatedKeys int64          // RocksDB's estimate of key count
	TotalSize     int64          // Total size in bytes
	TotalFiles    int64          // Total SST file count
	LevelStats    []CFLevelStats // Per-level breakdown (L0-L6)
}

// =============================================================================
// Iterator Interface
// =============================================================================

// Iterator abstracts RocksDB iteration for traversing key-value pairs.
//
// USAGE PATTERN:
//
//	iter := store.NewIteratorCF("0")
//	defer iter.Close()
//
//	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
//	    key := iter.Key()
//	    value := iter.Value()
//	    // process key-value
//	}
//
//	if err := iter.Error(); err != nil {
//	    // handle error
//	}
//
// IMPORTANT:
//   - Always call Close() when done (use defer)
//   - Check Error() after iteration completes
//   - Key() and Value() return slices that may be invalidated by Next()
type Iterator interface {
	// SeekToFirst positions the iterator at the first key.
	SeekToFirst()

	// Valid returns true if the iterator is positioned at a valid key-value pair.
	Valid() bool

	// Next advances the iterator to the next key.
	// Must only be called when Valid() returns true.
	Next()

	// Key returns the key at the current position.
	// The returned slice is only valid until the next call to Next() or Close().
	Key() []byte

	// Value returns the value at the current position.
	// The returned slice is only valid until the next call to Next() or Close().
	Value() []byte

	// Error returns any error encountered during iteration.
	// Should be checked after iteration completes.
	Error() error

	// Close releases resources associated with the iterator.
	// Must be called when done iterating.
	Close()
}

// =============================================================================
// TxHashStore Interface
// =============================================================================

// TxHashStore defines the interface for txHash→ledgerSeq storage operations.
//
// This interface abstracts the RocksDB store that holds the main data:
//   - 16 column families (0-f) partitioned by first hex char of txHash
//   - Each entry: 32-byte txHash → 4-byte ledgerSeq
//
// THREAD SAFETY:
//   - All methods are safe for concurrent use from multiple goroutines
//   - RocksDB handles internal locking
//   - A single ReadOptions instance is reused for all reads (not per-query)
//
// COLUMN FAMILY ROUTING:
//   - txHash[0] >> 4 gives the CF index (0-15)
//   - CF names: "0", "1", ..., "9", "a", "b", ..., "f"
type TxHashStore interface {
	// =========================================================================
	// Write Operations
	// =========================================================================

	// WriteBatch writes a batch of entries to the appropriate column families.
	// The map key is the CF name ("0" through "f"), value is the list of entries.
	//
	// This is the primary write method during ingestion. Entries are routed
	// to their respective CFs based on txHash[0] >> 4.
	//
	// ATOMICITY:
	//   The entire batch is written atomically. Either all entries are written
	//   or none are (in case of error).
	//
	// IDEMPOTENCY:
	//   Writing the same key-value pair multiple times is safe. This is critical
	//   for crash recovery where we may re-ingest some ledgers.
	//
	WriteBatch(entriesByCF map[string][]Entry) error

	// =========================================================================
	// Read Operations
	// =========================================================================

	// Get retrieves the ledger sequence for a transaction hash.
	//
	// Returns:
	//   - value: 4-byte ledger sequence (big-endian), or nil if not found
	//   - found: true if the key exists, false otherwise
	//   - err: non-nil if a RocksDB error occurred
	//
	// The correct column family is determined automatically from txHash[0].
	//
	Get(txHash []byte) (value []byte, found bool, err error)

	// NewIteratorCF creates a new iterator for a specific column family.
	//
	// The caller is responsible for calling Close() on the returned iterator.
	// Use defer to ensure cleanup:
	//
	//   iter := store.NewIteratorCF("a")
	//   defer iter.Close()
	//
	NewIteratorCF(cfName string) Iterator

	// =========================================================================
	// Maintenance Operations
	// =========================================================================

	// FlushAll flushes all column family MemTables to SST files on disk.
	//
	// This is called:
	//   - At the end of ingestion (before compaction)
	//   - Ensures all data is persisted to SST files
	//   - Reduces WAL size
	//
	// BLOCKING:
	//   This method blocks until all flushes complete.
	//
	FlushAll() error

	// CompactAll performs full compaction on all 16 column families.
	//
	// WHAT COMPACTION DOES:
	//   1. Merges all L0 files into sorted, non-overlapping files
	//   2. Removes duplicate keys (keeps latest value)
	//   3. Organizes data into levels (typically L6 after full compaction)
	//
	// WHY COMPACTION IS IMPORTANT:
	//   - Removes duplicates from crash recovery re-ingestion
	//   - Reduces file count for faster reads
	//   - Prepares data for RecSplit building
	//
	// Returns the total time spent compacting all CFs.
	//
	CompactAll() time.Duration

	// CompactCF compacts a single column family by name.
	// Returns the time spent compacting this CF.
	CompactCF(cfName string) time.Duration

	// =========================================================================
	// Statistics
	// =========================================================================

	// GetAllCFStats returns statistics for all 16 column families.
	// Includes file counts, sizes, and per-level breakdown.
	GetAllCFStats() []CFStats

	// GetCFStats returns statistics for a specific column family.
	GetCFStats(cfName string) CFStats

	// =========================================================================
	// Lifecycle
	// =========================================================================

	// Close releases all resources associated with the store.
	// Must be called when done using the store.
	Close()

	// Path returns the filesystem path to the RocksDB store.
	Path() string
}

// =============================================================================
// MetaStore Interface
// =============================================================================

// MetaStore defines the interface for checkpoint and progress tracking.
//
// The meta store persists workflow state to enable crash recovery:
//   - Configuration (start/end ledger)
//   - Current phase
//   - Last committed ledger
//   - Per-CF entry counts
//   - Verification progress
//
// ATOMICITY:
//
//	CommitBatchProgress() updates all progress fields atomically.
//	This ensures consistency between last_committed_ledger and cf_counts.
//
// CRASH RECOVERY:
//
//	On restart, the workflow reads the meta store to determine:
//	1. Was this a fresh start or a resume?
//	2. If resume, what phase and progress?
//	3. Do the config params (start/end ledger) match?
//
// STORAGE:
//
//	Uses a separate RocksDB instance at <output-dir>/txHash-ledgerSeq/meta/
type MetaStore interface {
	// =========================================================================
	// Configuration (Set Once at Start)
	// =========================================================================

	// GetStartLedger returns the configured start ledger.
	// Returns 0 if not set (fresh start).
	GetStartLedger() (uint32, error)

	// GetEndLedger returns the configured end ledger.
	// Returns 0 if not set (fresh start).
	GetEndLedger() (uint32, error)

	// SetConfig stores the start and end ledger configuration.
	// Called once at the beginning of a fresh ingestion.
	//
	// ERROR ON MISMATCH:
	//   If config already exists and differs from the provided values,
	//   this indicates the user is trying to resume with different params.
	//   The caller should abort with an error in this case.
	//
	SetConfig(startLedger, endLedger uint32) error

	// =========================================================================
	// Phase Tracking
	// =========================================================================

	// GetPhase returns the current workflow phase.
	// Returns PhaseIngesting if not set (fresh start).
	GetPhase() (Phase, error)

	// SetPhase updates the current workflow phase.
	// Called when transitioning between phases.
	SetPhase(phase Phase) error

	// =========================================================================
	// Ingestion Progress
	// =========================================================================

	// GetLastCommittedLedger returns the last fully committed ledger sequence.
	//
	// "Fully committed" means:
	//   - The batch ending at this ledger was successfully written to RocksDB
	//   - The cf_counts were updated atomically with this value
	//
	// Returns 0 if no batches have been committed yet.
	//
	GetLastCommittedLedger() (uint32, error)

	// GetCFCounts returns the entry count for each column family.
	//
	// These counts reflect the state at last_committed_ledger:
	//   - Accurate count of entries for ledgers [start_ledger, last_committed_ledger]
	//   - Used to initialize RecSplit with exact key count
	//
	// Returns empty map if no batches have been committed yet.
	//
	GetCFCounts() (map[string]uint64, error)

	// CommitBatchProgress atomically updates progress after a successful batch.
	//
	// This is the critical method for crash recovery correctness:
	//
	//   1. Updates last_committed_ledger to the batch's ending ledger
	//   2. Updates all cf_counts with the new cumulative totals
	//   3. Flushes to disk to ensure durability
	//
	// ATOMICITY:
	//   All updates are written in a single RocksDB WriteBatch.
	//   Either all updates succeed or none do.
	//
	// CRASH RECOVERY GUARANTEE:
	//   After a crash, last_committed_ledger and cf_counts are always consistent.
	//   They represent the state after the last fully-committed batch.
	//
	// EXAMPLE:
	//   After processing ledgers 10,888,001 - 10,889,000:
	//   CommitBatchProgress(10889000, {0: 1525000, 1: 1498000, ..., f: 1512000})
	//
	CommitBatchProgress(lastLedger uint32, cfCounts map[string]uint64) error

	// =========================================================================
	// Verification Progress
	// =========================================================================

	// GetVerifyCF returns the column family currently being verified.
	// Returns empty string if verification hasn't started.
	GetVerifyCF() (string, error)

	// SetVerifyCF updates the column family currently being verified.
	// Called when starting verification of a new CF.
	SetVerifyCF(cf string) error

	// =========================================================================
	// Utility
	// =========================================================================

	// Exists returns true if the meta store has been initialized.
	// Used to detect fresh start vs resume.
	Exists() bool

	// LogState logs the current meta store state for debugging/monitoring.
	// Called periodically during ingestion to show checkpoint status.
	LogState(logger Logger)

	// =========================================================================
	// Lifecycle
	// =========================================================================

	// Close releases all resources associated with the meta store.
	Close()
}

// =============================================================================
// Logger Interface
// =============================================================================

// Logger defines the interface for logging operations.
// Implementations write to log file and error file separately.
type Logger interface {
	// Info logs an informational message to the log file.
	Info(format string, args ...interface{})

	// Error logs an error message to the error file.
	Error(format string, args ...interface{})

	// Separator logs a visual separator line to the log file.
	Separator()

	// Sync forces a flush of all log buffers to disk.
	Sync()

	// Close closes all log files.
	Close()
}

// =============================================================================
// Column Family Constants
// =============================================================================

// ColumnFamilyNames contains the names of all 16 column families.
// Each column family is named by the hex character it handles (0-9, a-f).
//
// PARTITIONING SCHEME:
//
//	txHash[0] >> 4 gives the CF index (0-15)
//	This distributes data roughly evenly (assuming uniform hash distribution)
var ColumnFamilyNames = []string{
	"0", "1", "2", "3", "4", "5", "6", "7",
	"8", "9", "a", "b", "c", "d", "e", "f",
}

// GetColumnFamilyIndex returns the column family index (0-15) for a transaction hash.
func GetColumnFamilyIndex(txHash []byte) int {
	if len(txHash) < 1 {
		return 0
	}
	return int(txHash[0] >> 4)
}

// GetColumnFamilyName returns the column family name for a transaction hash.
func GetColumnFamilyName(txHash []byte) string {
	idx := GetColumnFamilyIndex(txHash)
	if idx < 0 || idx > 15 {
		return "0"
	}
	return ColumnFamilyNames[idx]
}
