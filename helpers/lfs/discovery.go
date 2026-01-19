package lfs

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// =============================================================================
// Range Discovery
// =============================================================================

// LedgerRange represents a contiguous range of ledgers.
type LedgerRange struct {
	StartLedger uint32
	EndLedger   uint32
	StartChunk  uint32
	EndChunk    uint32
	TotalChunks uint32
}

// TotalLedgers returns the total number of ledgers in the range.
func (r LedgerRange) TotalLedgers() uint32 {
	return r.EndLedger - r.StartLedger + 1
}

// DiscoverLedgerRange scans the chunks directory to find available ledger range.
// It finds the first and last complete chunks and returns the corresponding ledger range.
func DiscoverLedgerRange(dataDir string) (LedgerRange, error) {
	var result LedgerRange

	chunksDir := filepath.Join(dataDir, "chunks")
	if _, err := os.Stat(chunksDir); os.IsNotExist(err) {
		return result, fmt.Errorf("chunks directory not found: %s", chunksDir)
	}

	// Find all chunk IDs by scanning the directory structure
	chunkIDs, err := findAllChunkIDs(chunksDir)
	if err != nil {
		return result, err
	}

	if len(chunkIDs) == 0 {
		return result, fmt.Errorf("no chunks found in: %s", chunksDir)
	}

	// Sort to find min and max
	sort.Slice(chunkIDs, func(i, j int) bool { return chunkIDs[i] < chunkIDs[j] })

	// Validate that first and last chunks are complete
	firstChunk := chunkIDs[0]
	lastChunk := chunkIDs[len(chunkIDs)-1]

	if !ChunkExists(dataDir, firstChunk) {
		return result, fmt.Errorf("first chunk %d is incomplete", firstChunk)
	}
	if !ChunkExists(dataDir, lastChunk) {
		return result, fmt.Errorf("last chunk %d is incomplete", lastChunk)
	}

	result.StartChunk = firstChunk
	result.EndChunk = lastChunk
	result.StartLedger = ChunkFirstLedger(firstChunk)
	result.EndLedger = ChunkLastLedger(lastChunk)
	result.TotalChunks = lastChunk - firstChunk + 1

	return result, nil
}

// findAllChunkIDs scans the chunks directory structure and returns all chunk IDs.
func findAllChunkIDs(chunksDir string) ([]uint32, error) {
	var chunkIDs []uint32

	// List parent directories (0000, 0001, etc.)
	parentDirs, err := os.ReadDir(chunksDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunks directory: %w", err)
	}

	for _, parentDir := range parentDirs {
		if !parentDir.IsDir() {
			continue
		}

		// Check if directory name is a 4-digit number
		if len(parentDir.Name()) != 4 {
			continue
		}
		if _, err := strconv.Atoi(parentDir.Name()); err != nil {
			continue
		}

		// List chunk files in this parent directory
		parentPath := filepath.Join(chunksDir, parentDir.Name())
		chunkFiles, err := os.ReadDir(parentPath)
		if err != nil {
			continue // Skip unreadable directories
		}

		for _, chunkFile := range chunkFiles {
			if chunkFile.IsDir() {
				continue
			}

			// Look for .data files (each chunk has .data and .index)
			if !strings.HasSuffix(chunkFile.Name(), ".data") {
				continue
			}

			// Parse chunk ID from filename (NNNNNN.data)
			name := strings.TrimSuffix(chunkFile.Name(), ".data")
			if len(name) != 6 {
				continue
			}

			chunkID, err := strconv.ParseUint(name, 10, 32)
			if err != nil {
				continue
			}

			chunkIDs = append(chunkIDs, uint32(chunkID))
		}
	}

	return chunkIDs, nil
}

// ValidateLfsStore checks if the LFS store path is valid.
func ValidateLfsStore(dataDir string) error {
	// Check if directory exists
	info, err := os.Stat(dataDir)
	if os.IsNotExist(err) {
		return fmt.Errorf("LFS store directory does not exist: %s", dataDir)
	}
	if err != nil {
		return fmt.Errorf("failed to access LFS store directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("LFS store path is not a directory: %s", dataDir)
	}

	// Check if chunks subdirectory exists
	chunksDir := filepath.Join(dataDir, "chunks")
	if _, err := os.Stat(chunksDir); os.IsNotExist(err) {
		return fmt.Errorf("chunks subdirectory not found: %s", chunksDir)
	}

	return nil
}

// CountAvailableChunks returns the number of complete chunks in the store.
func CountAvailableChunks(dataDir string) (int, error) {
	chunksDir := filepath.Join(dataDir, "chunks")
	chunkIDs, err := findAllChunkIDs(chunksDir)
	if err != nil {
		return 0, err
	}

	// Count only complete chunks (both .data and .index exist)
	count := 0
	for _, chunkID := range chunkIDs {
		if ChunkExists(dataDir, chunkID) {
			count++
		}
	}

	return count, nil
}

// FindContiguousRange finds the largest contiguous range of chunks.
// Returns the range that should be used for processing.
func FindContiguousRange(dataDir string) (LedgerRange, int, error) {
	var result LedgerRange

	chunksDir := filepath.Join(dataDir, "chunks")
	chunkIDs, err := findAllChunkIDs(chunksDir)
	if err != nil {
		return result, 0, err
	}

	if len(chunkIDs) == 0 {
		return result, 0, fmt.Errorf("no chunks found")
	}

	// Sort chunk IDs
	sort.Slice(chunkIDs, func(i, j int) bool { return chunkIDs[i] < chunkIDs[j] })

	// Find gaps
	gaps := 0
	for i := 1; i < len(chunkIDs); i++ {
		if chunkIDs[i] != chunkIDs[i-1]+1 {
			gaps++
		}
	}

	// For now, just use first to last (warn about gaps)
	result.StartChunk = chunkIDs[0]
	result.EndChunk = chunkIDs[len(chunkIDs)-1]
	result.StartLedger = ChunkFirstLedger(result.StartChunk)
	result.EndLedger = ChunkLastLedger(result.EndChunk)
	result.TotalChunks = uint32(len(chunkIDs))

	return result, gaps, nil
}
