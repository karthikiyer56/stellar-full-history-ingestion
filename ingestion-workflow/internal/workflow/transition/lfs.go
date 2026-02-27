// =============================================================================
// lfs.go - LFS Chunk Writer with Crash Recovery
// =============================================================================
//
// This file implements the LFSWriter interface for transitioning compacted
// LCM data from RocksDB to immutable LFS (Local Filesystem) chunk format.
//
// KEY FEATURES:
//   - Writes 1000 chunks per range (10M ledgers / 10K per chunk)
//   - Chunk-level checkpointing for crash recovery
//   - Phase transitions: COMPACTING → WRITING_LFS → IMMUTABLE
//   - Resume from last committed chunk using int32 sentinel (-1 = no chunks)
//
// STORAGE FORMAT:
//   - Data file: <dataDir>/chunks/XXXX/YYYYYY.data (zstd-compressed LCM)
//   - Index file: <dataDir>/chunks/XXXX/YYYYYY.index (byte offsets)
//   - XXXX = chunk_id / 1000, YYYYYY = chunk_id (6-digit zero-padded)
//
// CRASH RECOVERY:
//   - Checkpoint after EVERY chunk write via SetLFSLastChunkWritten
//   - If lastWritten == -1: fresh start from first chunk
//   - If lastWritten == N: resume from chunk N+1
//
// =============================================================================

package transition

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/karthikiyer56/stellar-full-history-ingestion/ingestion-workflow/internal/workflow/interfaces"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// LedgersPerChunk is the fixed number of ledgers per LFS chunk
	LedgersPerChunk = 10_000

	// ChunksPerRange is the number of chunks in a 10M ledger range
	ChunksPerRange = 1000 // 10M / 10K

	// IndexVersion is the LFS index file format version
	IndexVersion = 1

	// IndexHeaderSize is the size of the index file header in bytes
	IndexHeaderSize = 8

	// WriteBufferSize is the size of the buffered writer for data files
	WriteBufferSize = 4 * 1024 * 1024 // 4 MB

	// FirstLedgerSequence is the first ledger in the Stellar blockchain
	FirstLedgerSequence = 2
)

// =============================================================================
// LFS Writer Implementation
// =============================================================================

// lfsWriter implements the LFSWriter interface.
type lfsWriter struct {
	dataDir string
	log     interfaces.Logger
	encoder *zstd.Encoder
}

// NewLFSWriter creates a new LFS chunk writer.
// dataDir: base directory for LFS storage (typically <data_dir>/immutable/lfs)
func NewLFSWriter(dataDir string, log interfaces.Logger) (interfaces.LFSWriter, error) {
	// Create zstd encoder (reused across all chunks).
	// Why zstd? Best balance between compression ratio (~3-4x) and speed (~200-300 MB/s).
	// Alternatives: gzip (slower), snappy (lower ratio), lz4 (lower ratio).
	// Zstd level 3 (default) provides excellent compression without excessive CPU usage.
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}

	return &lfsWriter{
		dataDir: dataDir,
		log:     log,
		encoder: encoder,
	}, nil
}

// WriteRange writes all LFS chunks for a given range with crash recovery.
//
// Implementation follows plan's "LFS Completion Logic" section:
//  1. Calculate chunk range: firstChunk = rangeID * 1000, lastChunk = firstChunk + 999
//  2. Resume from last committed chunk (using int32 sentinel: -1 = no chunks)
//  3. Transition phase: COMPACTING → WRITING_LFS (at start)
//  4. Write each chunk + checkpoint after EVERY chunk
//  5. Transition phase: WRITING_LFS → IMMUTABLE (at end)
//
// CRASH RECOVERY:
//   - Scenario A: Fresh start (lastWritten == -1) → startChunk = firstChunk
//   - Scenario B: Crashed after chunk 0 (lastWritten == 0) → startChunk = 1
//   - Scenario C: Crashed after chunk N (lastWritten == N) → startChunk = N+1
func (lfs *lfsWriter) WriteRange(rangeID uint32, lcmStore interfaces.LedgerStore, metaStore interfaces.MetaStore) error {
	// Calculate chunk range for this 10M ledger range
	firstChunk := rangeID * ChunksPerRange       // e.g., Range 0 → chunk 0
	lastChunk := firstChunk + ChunksPerRange - 1 // e.g., Range 0 → chunk 999

	// Resume from crash - check last committed chunk (int32, -1 = no chunks)
	lastWritten, err := metaStore.GetLFSLastChunkWritten(rangeID)
	if err != nil {
		return fmt.Errorf("failed to get last chunk written: %w", err)
	}

	// Determine start chunk using int32 sentinel
	var startChunk uint32
	if lastWritten == -1 {
		// No chunks written yet - begin from first chunk
		startChunk = firstChunk
		lfs.log.Info("Range %d: starting LFS from chunk %d (fresh start)", rangeID, firstChunk)
	} else {
		// Resume after last committed chunk
		startChunk = uint32(lastWritten) + 1
		lfs.log.Info("Range %d: resuming LFS from chunk %d (last written: %d)",
			rangeID, startChunk, lastWritten)
	}

	// Transition to WRITING_LFS if coming from COMPACTING
	phase, err := metaStore.GetLedgerPhase(rangeID)
	if err != nil {
		return fmt.Errorf("failed to get ledger phase: %w", err)
	}

	if phase == interfaces.LedgerPhaseCompacting {
		if err := metaStore.SetLedgerPhase(rangeID, interfaces.LedgerPhaseWritingLFS); err != nil {
			return fmt.Errorf("failed to set phase to WRITING_LFS: %w", err)
		}
		lfs.log.Info("Range %d: ledger phase → WRITING_LFS", rangeID)
	}

	// Write remaining chunks
	totalChunks := ChunksPerRange
	chunksToWrite := lastChunk - startChunk + 1
	lfs.log.Info("Range %d: writing %d chunks (%d to %d)", rangeID, chunksToWrite, startChunk, lastChunk)

	for chunkID := startChunk; chunkID <= lastChunk; chunkID++ {
		// Write chunk data + index files
		if err := lfs.writeChunk(chunkID, lcmStore); err != nil {
			return fmt.Errorf("failed to write chunk %d: %w", chunkID, err)
		}

		// Commit progress to meta store (crash recovery point)
		if err := metaStore.SetLFSLastChunkWritten(rangeID, int32(chunkID)); err != nil {
			return fmt.Errorf("failed to checkpoint chunk %d: %w", chunkID, err)
		}

		// Log progress every 10 chunks or at completion
		progress := chunkID - firstChunk + 1
		if progress%10 == 0 || chunkID == lastChunk {
			lfs.log.Debug("Range %d: wrote chunk %d (%d/%d, %.1f%%)",
				rangeID, chunkID, progress, totalChunks, float64(progress*100)/float64(totalChunks))
		}
	}

	// ALL chunks written - transition to IMMUTABLE
	if err := metaStore.SetLedgerPhase(rangeID, interfaces.LedgerPhaseImmutable); err != nil {
		return fmt.Errorf("failed to set phase to IMMUTABLE: %w", err)
	}

	lfs.log.Info("Range %d: LFS complete, phase=IMMUTABLE (%d chunks written)", rangeID, chunksToWrite)
	return nil
}

// writeChunk writes a single LFS chunk (data + index files).
//
// Implementation follows local-fs/ingestion/lfs-ledger-ingestion.go:558-661
//  1. Create chunk directory (if needed)
//  2. Open data file with buffered writer
//  3. For each ledger: read from LCM store, compress, write, track offset
//  4. Flush data file
//  5. Write index file in single write
func (lfs *lfsWriter) writeChunk(chunkID uint32, lcmStore interfaces.LedgerStore) error {
	// Calculate ledger range for this chunk
	firstLedger := lfs.chunkFirstLedger(chunkID)
	lastLedger := lfs.chunkLastLedger(chunkID)

	// Paths for chunk files
	chunkDir := lfs.getChunkDir(lfs.dataDir, chunkID)
	dataPath := lfs.getDataPath(lfs.dataDir, chunkID)
	indexPath := lfs.getIndexPath(lfs.dataDir, chunkID)

	// Ensure parent directory exists
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		return fmt.Errorf("failed to create chunk directory: %w", err)
	}

	// Create data file with buffered writer
	dataFile, err := os.Create(dataPath)
	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}
	defer dataFile.Close()

	writer := bufio.NewWriterSize(dataFile, WriteBufferSize)

	// Track offsets in memory (~80 KB for 10K offsets)
	offsets := make([]uint64, 0, LedgersPerChunk+1)
	currentOffset := uint64(0)
	offsets = append(offsets, currentOffset)

	// Process each ledger: read, compress, write, track offset
	for seq := firstLedger; seq <= lastLedger; seq++ {
		// Read LCM bytes from store
		lcmBytes, err := lcmStore.Get(seq)
		if err != nil {
			// Ledger may not exist if range is partial (last chunk in range)
			lfs.log.Warn("Chunk %d: ledger %d not found, skipping", chunkID, seq)
			continue
		}

		// Compress with zstd
		compressed := lfs.encoder.EncodeAll(lcmBytes, nil)

		// Write to buffered writer
		if _, err := writer.Write(compressed); err != nil {
			return fmt.Errorf("failed to write ledger %d data: %w", seq, err)
		}

		// Track offset for index
		currentOffset += uint64(len(compressed))
		offsets = append(offsets, currentOffset)
	}

	// Flush and close data file
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush data file: %w", err)
	}

	if err := dataFile.Close(); err != nil {
		return fmt.Errorf("failed to close data file: %w", err)
	}

	// Write index file in a single write
	if err := lfs.writeIndexFile(indexPath, offsets); err != nil {
		// Clean up data file on index write failure
		os.Remove(dataPath)
		return fmt.Errorf("failed to write index file: %w", err)
	}

	return nil
}

// writeIndexFile writes the index file with header and offsets.
//
// FORMAT (matches local-fs/ingestion/lfs-ledger-ingestion.go:667-701):
//
//	Header (8 bytes):
//	  - byte 0: version (1)
//	  - byte 1: offset_size (4 or 8)
//	  - bytes 2-7: reserved (zeros)
//	Offsets (variable):
//	  - offset_size bytes per offset (little-endian)
//	  - Number of offsets = ledgers_in_chunk + 1
func (lfs *lfsWriter) writeIndexFile(path string, offsets []uint64) error {
	// Determine offset size based on final data file size
	finalOffset := offsets[len(offsets)-1]
	offsetSize := 4
	if finalOffset > 0xFFFFFFFF {
		offsetSize = 8
	}

	// Pre-allocate buffer: header (8 bytes) + all offsets
	bufSize := IndexHeaderSize + len(offsets)*offsetSize
	buf := make([]byte, bufSize)

	// Write header
	buf[0] = IndexVersion      // version
	buf[1] = uint8(offsetSize) // offset_size
	// bytes 2-7 are reserved (already zero)

	// Write offsets (little-endian)
	pos := IndexHeaderSize
	if offsetSize == 4 {
		for _, offset := range offsets {
			binary.LittleEndian.PutUint32(buf[pos:], uint32(offset))
			pos += 4
		}
	} else {
		for _, offset := range offsets {
			binary.LittleEndian.PutUint64(buf[pos:], offset)
			pos += 8
		}
	}

	// Single write
	return os.WriteFile(path, buf, 0644)
}

// Close releases all resources.
func (lfs *lfsWriter) Close() error {
	if lfs.encoder != nil {
		lfs.encoder.Close()
		lfs.encoder = nil
	}
	return nil
}

// =============================================================================
// Internal Helper Functions (matching helpers/lfs/chunk.go)
// =============================================================================

// getChunkDir returns the directory path for a chunk.
func (lfs *lfsWriter) getChunkDir(dataDir string, chunkID uint32) string {
	parentDir := chunkID / 1000
	return filepath.Join(dataDir, "chunks", fmt.Sprintf("%04d", parentDir))
}

// getDataPath returns the data file path for a chunk.
func (lfs *lfsWriter) getDataPath(dataDir string, chunkID uint32) string {
	return filepath.Join(lfs.getChunkDir(dataDir, chunkID), fmt.Sprintf("%06d.data", chunkID))
}

// getIndexPath returns the index file path for a chunk.
func (lfs *lfsWriter) getIndexPath(dataDir string, chunkID uint32) string {
	return filepath.Join(lfs.getChunkDir(dataDir, chunkID), fmt.Sprintf("%06d.index", chunkID))
}

// chunkFirstLedger returns the first ledger sequence in a chunk.
func (lfs *lfsWriter) chunkFirstLedger(chunkID uint32) uint32 {
	return (chunkID * LedgersPerChunk) + FirstLedgerSequence
}

// chunkLastLedger returns the last ledger sequence in a chunk.
func (lfs *lfsWriter) chunkLastLedger(chunkID uint32) uint32 {
	return ((chunkID + 1) * LedgersPerChunk) + FirstLedgerSequence - 1
}

// =============================================================================
// Compile-Time Interface Check
// =============================================================================

var _ interfaces.LFSWriter = (*lfsWriter)(nil)
