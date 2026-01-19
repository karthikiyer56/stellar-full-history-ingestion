package txhashrework

import (
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
)

// =============================================================================
// Merge Statistics
// =============================================================================

// Stats holds statistics about the ingestion/merge operation.
type Stats struct {
	TotalEntriesRead    int64
	TotalEntriesWritten int64
	DuplicateEntries    int64
	EntriesPerCF        map[string]int64

	LedgersProcessed  int64
	TransactionsFound int64
	WorkerErrors      int64

	ReadTime       time.Duration
	WriteTime      time.Duration
	CompactionTime time.Duration

	mu sync.Mutex
}

// NewStats creates a new Stats instance with initialized maps.
func NewStats() *Stats {
	stats := &Stats{
		EntriesPerCF: make(map[string]int64),
	}
	for _, cfName := range ColumnFamilyNames {
		stats.EntriesPerCF[cfName] = 0
	}
	return stats
}

// AddEntries adds entry counts to the stats (thread-safe).
func (s *Stats) AddEntries(read, written int64, perCF map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TotalEntriesRead += read
	s.TotalEntriesWritten += written
	for cfName, count := range perCF {
		s.EntriesPerCF[cfName] += count
	}
}

// AddCFEntries adds column family entry counts (thread-safe).
func (s *Stats) AddCFEntries(perCF map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for cfName, count := range perCF {
		s.EntriesPerCF[cfName] += count
	}
}

// AddLedgersAndTransactions atomically increments ledger and transaction counts.
func (s *Stats) AddLedgersAndTransactions(ledgers, transactions int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LedgersProcessed += ledgers
	s.TransactionsFound += transactions
}

// AddWrittenEntries atomically increments written entry count.
func (s *Stats) AddWrittenEntries(count int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TotalEntriesWritten += count
}

// AddWriteTime atomically adds to write time.
func (s *Stats) AddWriteTime(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.WriteTime += d
}

// IncrementErrors atomically increments error count.
func (s *Stats) IncrementErrors() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.WorkerErrors++
}

// StatsSnapshot is a point-in-time copy of Stats without the mutex.
type StatsSnapshot struct {
	TotalEntriesRead    int64
	TotalEntriesWritten int64
	DuplicateEntries    int64
	LedgersProcessed    int64
	TransactionsFound   int64
	WorkerErrors        int64
	ReadTime            time.Duration
	WriteTime           time.Duration
	CompactionTime      time.Duration
	EntriesPerCF        map[string]int64
}

// GetSnapshot returns a snapshot of current stats (thread-safe).
func (s *Stats) GetSnapshot() StatsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := StatsSnapshot{
		TotalEntriesRead:    s.TotalEntriesRead,
		TotalEntriesWritten: s.TotalEntriesWritten,
		DuplicateEntries:    s.DuplicateEntries,
		LedgersProcessed:    s.LedgersProcessed,
		TransactionsFound:   s.TransactionsFound,
		WorkerErrors:        s.WorkerErrors,
		ReadTime:            s.ReadTime,
		WriteTime:           s.WriteTime,
		CompactionTime:      s.CompactionTime,
		EntriesPerCF:        make(map[string]int64),
	}
	for k, v := range s.EntriesPerCF {
		snapshot.EntriesPerCF[k] = v
	}
	return snapshot
}

// LogSummary logs a summary of the stats using the provided logger.
func (s *Stats) LogSummary(logger *Logger, totalTime time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logger.Info("")
	logger.Info("================================================================================")
	logger.Info("                              OPERATION COMPLETE")
	logger.Info("================================================================================")
	logger.Info("")
	logger.Info("STATISTICS:")
	logger.Info("  Total Entries Read:    %s", helpers.FormatNumber(s.TotalEntriesRead))
	logger.Info("  Total Entries Written: %s", helpers.FormatNumber(s.TotalEntriesWritten))
	if s.LedgersProcessed > 0 {
		logger.Info("  Ledgers Processed:     %s", helpers.FormatNumber(s.LedgersProcessed))
		logger.Info("  Transactions Found:    %s", helpers.FormatNumber(s.TransactionsFound))
	}
	if s.DuplicateEntries > 0 {
		logger.Info("  Duplicate Entries:     %s", helpers.FormatNumber(s.DuplicateEntries))
	}
	if s.WorkerErrors > 0 {
		logger.Info("  Worker Errors:         %s", helpers.FormatNumber(s.WorkerErrors))
	}
	logger.Info("")
	logger.Info("COLUMN FAMILY DISTRIBUTION:")

	for i := 0; i < 16; i++ {
		cfName := ColumnFamilyNames[i]
		count := s.EntriesPerCF[cfName]
		pct := float64(0)
		if s.TotalEntriesWritten > 0 {
			pct = float64(count) / float64(s.TotalEntriesWritten) * 100
		}
		logger.Info("  CF[%s]: %s (%.2f%%)", cfName, helpers.FormatNumber(count), pct)
	}
	logger.Info("")
	logger.Info("TIMING:")
	logger.Info("  Total Time:            %s", helpers.FormatDuration(totalTime))
	logger.Info("  Processing Time:       %s", helpers.FormatDuration(s.ReadTime))
	logger.Info("  Write Time:            %s", helpers.FormatDuration(s.WriteTime))
	if s.CompactionTime > 0 {
		logger.Info("  Compaction Time:       %s", helpers.FormatDuration(s.CompactionTime))
	}
	logger.Info("")

	if s.ReadTime.Seconds() > 0 && s.WriteTime.Seconds() > 0 {
		logger.Info("THROUGHPUT:")
		logger.Info("  Read Rate:             %s entries/sec",
			helpers.FormatNumber(int64(float64(s.TotalEntriesRead)/s.ReadTime.Seconds())))
		logger.Info("  Write Rate:            %s entries/sec",
			helpers.FormatNumber(int64(float64(s.TotalEntriesWritten)/s.WriteTime.Seconds())))
		logger.Info("")
	}

	logger.Info("================================================================================")
}
