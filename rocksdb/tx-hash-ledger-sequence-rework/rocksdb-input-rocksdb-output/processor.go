// Package main provides the RocksDB input to RocksDB output processor.
//
// This tool merges multiple existing RocksDB stores (with or without column
// families) into a single output store with 16 column families partitioned
// by the first hex character of the transaction hash.
//
// This is useful for:
//   - Merging stores from different time periods
//   - Converting single-CF stores to multi-CF format
//   - Consolidating distributed stores
package main

import (
	"fmt"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	txhashrework "github.com/karthikiyer56/stellar-full-history-ingestion/rocksdb/tx-hash-ledger-sequence-rework"
)

// =============================================================================
// RocksDB Merge Processor
// =============================================================================

// MergeProcessor handles reading from input RocksDB stores and writing to output.
type MergeProcessor struct {
	config      *txhashrework.Config
	outputStore *txhashrework.OutputStore
	logger      *txhashrework.Logger
	stats       *txhashrework.Stats
}

// NewMergeProcessor creates a new merge processor.
func NewMergeProcessor(config *txhashrework.Config, outputStore *txhashrework.OutputStore, logger *txhashrework.Logger) *MergeProcessor {
	return &MergeProcessor{
		config:      config,
		outputStore: outputStore,
		logger:      logger,
		stats:       txhashrework.NewStats(),
	}
}

// Process merges all input stores into the output store.
func (p *MergeProcessor) Process() (*txhashrework.Stats, error) {
	startTime := time.Now()

	p.logger.Info("Starting merge of %d input store(s)", len(p.config.InputStores))

	for i, storePath := range p.config.InputStores {
		p.logger.Info("")
		p.logger.Info("================================================================================")
		p.logger.Info("Processing input store %d/%d: %s", i+1, len(p.config.InputStores), storePath)
		p.logger.Info("================================================================================")

		if err := p.processInputStore(storePath); err != nil {
			return nil, fmt.Errorf("failed to process store %s: %w", storePath, err)
		}

		p.logger.Info("")
		p.logger.Info("Completed store %d/%d: %s entries read so far",
			i+1, len(p.config.InputStores), helpers.FormatNumber(p.stats.TotalEntriesRead))
	}

	p.stats.ReadTime = time.Since(startTime) - p.stats.WriteTime

	return p.stats, nil
}

// processInputStore processes a single input RocksDB store.
func (p *MergeProcessor) processInputStore(storePath string) error {
	// Open input store
	p.logger.Info("Opening input store...")
	inputStore, err := txhashrework.OpenInputStore(storePath, 512) // 512MB block cache for reading
	if err != nil {
		return err
	}
	defer inputStore.Close()
	p.logger.Info("Input store opened")

	// Process each column family
	for _, cfName := range txhashrework.ColumnFamilyNames {
		if err := p.processColumnFamily(inputStore, cfName); err != nil {
			return fmt.Errorf("failed to process CF %s: %w", cfName, err)
		}
	}

	return nil
}

// processColumnFamily processes a single column family from input store.
func (p *MergeProcessor) processColumnFamily(inputStore *txhashrework.InputStore, cfName string) error {
	iter := inputStore.NewIteratorCF(cfName)
	defer iter.Close()

	// Track entries for current batch
	entriesByCF := make(map[string][]txhashrework.Entry)
	for _, cf := range txhashrework.ColumnFamilyNames {
		entriesByCF[cf] = make([]txhashrework.Entry, 0, p.config.BatchSize/16)
	}

	// Per-batch stats
	batchEntriesPerCF := make(map[string]int64)
	for _, cf := range txhashrework.ColumnFamilyNames {
		batchEntriesPerCF[cf] = 0
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

		// Determine output column family (may differ from input if re-partitioning)
		outputCF := txhashrework.GetColumnFamilyName(key)

		// Add to batch
		entriesByCF[outputCF] = append(entriesByCF[outputCF], txhashrework.Entry{
			Key:   key,
			Value: value,
		})
		batchEntriesPerCF[outputCF]++
		entriesInBatch++
		entriesProcessed++

		// Write batch when full
		if entriesInBatch >= p.config.BatchSize {
			batchReadTime := time.Since(readStart)
			p.stats.ReadTime += batchReadTime

			batchNum++
			p.logger.Info("  CF[%s] Batch %d: %s entries read (%s)", cfName, batchNum,
				helpers.FormatNumber(int64(entriesInBatch)),
				helpers.FormatDuration(batchReadTime))

			writeStart := time.Now()
			if err := p.outputStore.WriteBatch(entriesByCF); err != nil {
				return err
			}
			batchWriteTime := time.Since(writeStart)
			p.stats.WriteTime += batchWriteTime
			p.logger.Info("             Written in %s", helpers.FormatDuration(batchWriteTime))

			// Update stats
			p.stats.AddEntries(int64(entriesInBatch), int64(entriesInBatch), batchEntriesPerCF)

			// Reset batch
			for cf := range entriesByCF {
				entriesByCF[cf] = entriesByCF[cf][:0]
				batchEntriesPerCF[cf] = 0
			}
			entriesInBatch = 0

			readStart = time.Now()
		}
	}

	// Check for iterator error
	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	// Write remaining entries
	if entriesInBatch > 0 {
		batchReadTime := time.Since(readStart)
		p.stats.ReadTime += batchReadTime

		batchNum++
		p.logger.Info("  CF[%s] Batch %d (final): %s entries read (%s)", cfName, batchNum,
			helpers.FormatNumber(int64(entriesInBatch)),
			helpers.FormatDuration(batchReadTime))

		writeStart := time.Now()
		if err := p.outputStore.WriteBatch(entriesByCF); err != nil {
			return err
		}
		batchWriteTime := time.Since(writeStart)
		p.stats.WriteTime += batchWriteTime
		p.logger.Info("             Written in %s", helpers.FormatDuration(batchWriteTime))

		// Update stats
		p.stats.AddEntries(int64(entriesInBatch), int64(entriesInBatch), batchEntriesPerCF)
	}

	if entriesProcessed > 0 {
		p.logger.Info("  CF[%s] complete: %s entries", cfName,
			helpers.FormatNumber(entriesProcessed))
	}

	return nil
}
