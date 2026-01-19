// merge.go
// =============================================================================
// Core Merge Logic for TX Hash Column Family Merger
// =============================================================================
//
// This module handles:
// - Iterating through source stores
// - Partitioning entries by first hex character
// - Writing batches to column families
// - Tracking statistics
//
// =============================================================================

package main

import (
	"log"
	"sync"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
)

// =============================================================================
// Merge Statistics
// =============================================================================

// MergeStats holds statistics about the merge operation.
type MergeStats struct {
	TotalEntriesRead    int64
	TotalEntriesWritten int64
	DuplicateEntries    int64
	EntriesPerCF        map[string]int64

	ReadTime       time.Duration
	WriteTime      time.Duration
	CompactionTime time.Duration

	mu sync.Mutex
}

// NewMergeStats creates a new MergeStats instance.
func NewMergeStats() *MergeStats {
	stats := &MergeStats{
		EntriesPerCF: make(map[string]int64),
	}
	for _, cfName := range ColumnFamilyNames {
		stats.EntriesPerCF[cfName] = 0
	}
	return stats
}

// AddEntries adds entry counts to the stats (thread-safe).
func (s *MergeStats) AddEntries(read, written int64, perCF map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TotalEntriesRead += read
	s.TotalEntriesWritten += written
	for cfName, count := range perCF {
		s.EntriesPerCF[cfName] += count
	}
}

// =============================================================================
// Merge Execution
// =============================================================================

// ExecuteMerge performs the merge operation.
func ExecuteMerge(config *Config, dryRun bool) (*MergeStats, error) {
	stats := NewMergeStats()

	// =========================================================================
	// Open Output Store (unless dry run)
	// =========================================================================
	var outputStore *OutputStore
	var err error

	if !dryRun {
		log.Printf("Opening output store with column families...")
		outputStore, err = OpenOutputStore(config)
		if err != nil {
			return nil, err
		}
		defer outputStore.Close()
		log.Printf("Output store opened successfully")
		log.Printf("")
	}

	// =========================================================================
	// Process Each Input Store
	// =========================================================================
	var totalReadTime time.Duration
	var totalWriteTime time.Duration

	for i, storePath := range config.InputStores {
		log.Printf("================================================================================")
		log.Printf("Processing input store %d/%d: %s", i+1, len(config.InputStores), storePath)
		log.Printf("================================================================================")

		readTime, writeTime, err := processSourceStore(
			storePath,
			outputStore,
			config.BatchSize,
			stats,
			dryRun,
		)
		if err != nil {
			return nil, err
		}

		totalReadTime += readTime
		totalWriteTime += writeTime

		log.Printf("")
		log.Printf("Completed store %d/%d: %s entries read",
			i+1, len(config.InputStores), helpers.FormatNumber(stats.TotalEntriesRead))
		log.Printf("")
	}

	stats.ReadTime = totalReadTime
	stats.WriteTime = totalWriteTime

	// =========================================================================
	// Final Compaction (unless dry run)
	// =========================================================================
	if !dryRun && outputStore != nil {
		stats.CompactionTime = outputStore.CompactAll()

		// Generate active store configuration for Phase 2
		if err := GenerateActiveStoreConfig(config); err != nil {
			log.Printf("Warning: failed to generate active store config: %v", err)
		}

		// Print instructions for using the store in active mode
		PrintActiveStoreInstructions(config)
	}

	return stats, nil
}

// processSourceStore processes a single source store.
func processSourceStore(
	storePath string,
	outputStore *OutputStore,
	batchSize int,
	stats *MergeStats,
	dryRun bool,
) (readTime, writeTime time.Duration, err error) {
	// Open source store
	log.Printf("Opening source store...")
	sourceStore, err := OpenSourceStore(storePath)
	if err != nil {
		return 0, 0, err
	}
	defer sourceStore.Close()
	log.Printf("Source store opened")

	// Create iterator
	iter := sourceStore.NewIterator()
	defer iter.Close()

	// Track entries by column family for current batch
	entriesByCF := make(map[string][]Entry)
	for _, cfName := range ColumnFamilyNames {
		entriesByCF[cfName] = make([]Entry, 0, batchSize/16)
	}

	// Per-batch stats
	batchEntriesPerCF := make(map[string]int64)
	for _, cfName := range ColumnFamilyNames {
		batchEntriesPerCF[cfName] = 0
	}

	var entriesInBatch int
	var entriesProcessed int64
	var batchNum int

	readStart := time.Now()

	// Iterate through all entries
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		// Get key and value
		keySlice := iter.Key()
		valueSlice := iter.Value()

		// Copy key and value (slices are only valid until next iteration)
		key := make([]byte, keySlice.Size())
		copy(key, keySlice.Data())
		keySlice.Free()

		value := make([]byte, valueSlice.Size())
		copy(value, valueSlice.Data())
		valueSlice.Free()

		// Determine column family
		cfName := GetColumnFamilyName(key)

		// Add to batch
		entriesByCF[cfName] = append(entriesByCF[cfName], Entry{
			Key:   key,
			Value: value,
		})
		batchEntriesPerCF[cfName]++
		entriesInBatch++
		entriesProcessed++

		// Write batch when full
		if entriesInBatch >= batchSize {
			batchReadTime := time.Since(readStart)
			readTime += batchReadTime

			batchNum++
			log.Printf("Batch %d: %s entries read (%s)", batchNum,
				helpers.FormatNumber(int64(entriesInBatch)),
				helpers.FormatDuration(batchReadTime))

			if !dryRun && outputStore != nil {
				writeStart := time.Now()
				if err := outputStore.WriteBatch(entriesByCF); err != nil {
					return readTime, writeTime, err
				}
				batchWriteTime := time.Since(writeStart)
				writeTime += batchWriteTime
				log.Printf("         Written in %s", helpers.FormatDuration(batchWriteTime))
			}

			// Update stats
			stats.AddEntries(int64(entriesInBatch), int64(entriesInBatch), batchEntriesPerCF)

			// Reset batch
			for cfName := range entriesByCF {
				entriesByCF[cfName] = entriesByCF[cfName][:0]
				batchEntriesPerCF[cfName] = 0
			}
			entriesInBatch = 0

			readStart = time.Now()
		}
	}

	// Check for iterator error
	if err := iter.Err(); err != nil {
		return readTime, writeTime, err
	}

	// Write remaining entries
	if entriesInBatch > 0 {
		batchReadTime := time.Since(readStart)
		readTime += batchReadTime

		batchNum++
		log.Printf("Batch %d (final): %s entries read (%s)", batchNum,
			helpers.FormatNumber(int64(entriesInBatch)),
			helpers.FormatDuration(batchReadTime))

		if !dryRun && outputStore != nil {
			writeStart := time.Now()
			if err := outputStore.WriteBatch(entriesByCF); err != nil {
				return readTime, writeTime, err
			}
			batchWriteTime := time.Since(writeStart)
			writeTime += batchWriteTime
			log.Printf("               Written in %s", helpers.FormatDuration(batchWriteTime))
		}

		// Update stats
		stats.AddEntries(int64(entriesInBatch), int64(entriesInBatch), batchEntriesPerCF)
	}

	log.Printf("")
	log.Printf("Store complete: %s total entries processed",
		helpers.FormatNumber(entriesProcessed))

	return readTime, writeTime, nil
}
