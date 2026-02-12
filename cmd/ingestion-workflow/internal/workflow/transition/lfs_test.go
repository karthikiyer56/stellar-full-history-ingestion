package transition

import (
	"fmt"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/logging"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/stores/meta"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/types"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// =============================================================================
// Mock LCM Store for Testing
// =============================================================================

type mockLCMStore struct {
	data map[uint32][]byte
}

func newMockLCMStore() *mockLCMStore {
	return &mockLCMStore{
		data: make(map[uint32][]byte),
	}
}

func (m *mockLCMStore) Open() (time.Duration, error) {
	return 0, nil
}

func (m *mockLCMStore) WriteBatch(entries map[uint32][]byte) error {
	for k, v := range entries {
		m.data[k] = v
	}
	return nil
}

func (m *mockLCMStore) Get(ledgerSeq uint32) ([]byte, error) {
	data, ok := m.data[ledgerSeq]
	if !ok {
		return nil, fmt.Errorf("ledger %d not found", ledgerSeq)
	}
	return data, nil
}

func (m *mockLCMStore) NewIterator() interfaces.Iterator {
	return nil
}

func (m *mockLCMStore) Compact() (time.Duration, error) {
	return 0, nil
}

func (m *mockLCMStore) GetPath() string {
	return ""
}

func (m *mockLCMStore) GetSize() (int64, error) {
	return 0, nil
}

func (m *mockLCMStore) Close() error {
	return nil
}

// =============================================================================
// Test: Fresh Start (no chunks written)
// =============================================================================

func TestLFSWriter_FreshStart(t *testing.T) {
	tmpDir := t.TempDir()
	lfsDir := filepath.Join(tmpDir, "lfs")
	metaDir := filepath.Join(tmpDir, "meta")

	log := logging.NewTestLogger()
	defer log.Sync()

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	metaStore, err := meta.NewMetaStore(metaDir, settings)
	if err != nil {
		t.Fatalf("failed to create meta store: %v", err)
	}
	defer metaStore.Close()

	rangeID := uint32(0)

	if err := metaStore.SetRangeState(rangeID, interfaces.RangeStateIngesting); err != nil {
		t.Fatalf("failed to set range state: %v", err)
	}
	if err := metaStore.SetLedgerPhase(rangeID, interfaces.LedgerPhaseCompacting); err != nil {
		t.Fatalf("failed to set ledger phase: %v", err)
	}

	lcmStore := newMockLCMStore()
	for seq := uint32(2); seq <= uint32(10001); seq++ {
		lcmStore.data[seq] = []byte{byte(seq), byte(seq >> 8)}
	}

	writer, err := NewLFSWriter(lfsDir, log)
	if err != nil {
		t.Fatalf("failed to create LFS writer: %v", err)
	}
	defer writer.Close()

	if err := writer.WriteRange(rangeID, lcmStore, metaStore); err != nil {
		t.Fatalf("WriteRange failed: %v", err)
	}

	lastChunk, err := metaStore.GetLFSLastChunkWritten(rangeID)
	if err != nil {
		t.Fatalf("failed to get last chunk: %v", err)
	}
	if lastChunk != 0 {
		t.Errorf("expected last chunk 0, got %d", lastChunk)
	}

	phase, err := metaStore.GetLedgerPhase(rangeID)
	if err != nil {
		t.Fatalf("failed to get ledger phase: %v", err)
	}
	if phase != interfaces.LedgerPhaseImmutable {
		t.Errorf("expected phase IMMUTABLE, got %s", phase)
	}

	dataPath := filepath.Join(lfsDir, "chunks", "0000", "000000.data")
	if _, err := os.Stat(dataPath); os.IsNotExist(err) {
		t.Errorf("expected data file to exist: %s", dataPath)
	}

	indexPath := filepath.Join(lfsDir, "chunks", "0000", "000000.index")
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Errorf("expected index file to exist: %s", indexPath)
	}
}

// =============================================================================
// Test: Resume from Crash
// =============================================================================

func TestLFSWriter_ResumeFromCrash(t *testing.T) {
	tmpDir := t.TempDir()
	lfsDir := filepath.Join(tmpDir, "lfs")
	metaDir := filepath.Join(tmpDir, "meta")

	log := logging.NewTestLogger()
	defer log.Sync()

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	metaStore, err := meta.NewMetaStore(metaDir, settings)
	if err != nil {
		t.Fatalf("failed to create meta store: %v", err)
	}
	defer metaStore.Close()

	rangeID := uint32(0)

	if err := metaStore.SetRangeState(rangeID, interfaces.RangeStateIngesting); err != nil {
		t.Fatalf("failed to set range state: %v", err)
	}
	if err := metaStore.SetLedgerPhase(rangeID, interfaces.LedgerPhaseWritingLFS); err != nil {
		t.Fatalf("failed to set ledger phase: %v", err)
	}
	if err := metaStore.SetLFSLastChunkWritten(rangeID, 0); err != nil {
		t.Fatalf("failed to set last chunk: %v", err)
	}

	lcmStore := newMockLCMStore()
	for seq := uint32(2); seq <= uint32(20001); seq++ {
		lcmStore.data[seq] = []byte{byte(seq), byte(seq >> 8)}
	}

	writer, err := NewLFSWriter(lfsDir, log)
	if err != nil {
		t.Fatalf("failed to create LFS writer: %v", err)
	}
	defer writer.Close()

	if err := writer.WriteRange(rangeID, lcmStore, metaStore); err != nil {
		t.Fatalf("WriteRange failed: %v", err)
	}

	lastChunk, err := metaStore.GetLFSLastChunkWritten(rangeID)
	if err != nil {
		t.Fatalf("failed to get last chunk: %v", err)
	}
	if lastChunk != 1 {
		t.Errorf("expected last chunk 1, got %d", lastChunk)
	}

	dataPath0 := filepath.Join(lfsDir, "chunks", "0000", "000000.data")
	if _, err := os.Stat(dataPath0); err == nil {
		t.Errorf("chunk 0 should not be written (resume started at chunk 1)")
	}

	dataPath1 := filepath.Join(lfsDir, "chunks", "0000", "000001.data")
	if _, err := os.Stat(dataPath1); os.IsNotExist(err) {
		t.Errorf("expected data file to exist: %s", dataPath1)
	}
}

// =============================================================================
// Test: Sentinel Value (-1 means fresh start)
// =============================================================================

func TestLFSWriter_SentinelValue(t *testing.T) {
	tmpDir := t.TempDir()
	metaDir := filepath.Join(tmpDir, "meta")

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	metaStore, err := meta.NewMetaStore(metaDir, settings)
	if err != nil {
		t.Fatalf("failed to create meta store: %v", err)
	}
	defer metaStore.Close()

	rangeID := uint32(0)

	lastChunk, err := metaStore.GetLFSLastChunkWritten(rangeID)
	if err != nil {
		t.Fatalf("failed to get last chunk: %v", err)
	}
	if lastChunk != -1 {
		t.Errorf("expected sentinel -1 for fresh start, got %d", lastChunk)
	}

	if err := metaStore.SetLFSLastChunkWritten(rangeID, 0); err != nil {
		t.Fatalf("failed to set last chunk: %v", err)
	}

	lastChunk, err = metaStore.GetLFSLastChunkWritten(rangeID)
	if err != nil {
		t.Fatalf("failed to get last chunk: %v", err)
	}
	if lastChunk != 0 {
		t.Errorf("expected 0 after writing chunk 0, got %d", lastChunk)
	}

	if err := metaStore.SetLFSLastChunkWritten(rangeID, 42); err != nil {
		t.Fatalf("failed to set last chunk: %v", err)
	}

	lastChunk, err = metaStore.GetLFSLastChunkWritten(rangeID)
	if err != nil {
		t.Fatalf("failed to get last chunk: %v", err)
	}
	if lastChunk != 42 {
		t.Errorf("expected 42 after writing chunk 42, got %d", lastChunk)
	}
}

// =============================================================================
// Test: Phase Transitions
// =============================================================================

func TestLFSWriter_PhaseTransitions(t *testing.T) {
	tmpDir := t.TempDir()
	lfsDir := filepath.Join(tmpDir, "lfs")
	metaDir := filepath.Join(tmpDir, "meta")

	log := logging.NewTestLogger()
	defer log.Sync()

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	metaStore, err := meta.NewMetaStore(metaDir, settings)
	if err != nil {
		t.Fatalf("failed to create meta store: %v", err)
	}
	defer metaStore.Close()

	rangeID := uint32(0)

	if err := metaStore.SetLedgerPhase(rangeID, interfaces.LedgerPhaseCompacting); err != nil {
		t.Fatalf("failed to set ledger phase: %v", err)
	}

	lcmStore := newMockLCMStore()
	for seq := uint32(2); seq <= uint32(10001); seq++ {
		lcmStore.data[seq] = []byte{byte(seq)}
	}

	writer, err := NewLFSWriter(lfsDir, log)
	if err != nil {
		t.Fatalf("failed to create LFS writer: %v", err)
	}
	defer writer.Close()

	if err := writer.WriteRange(rangeID, lcmStore, metaStore); err != nil {
		t.Fatalf("WriteRange failed: %v", err)
	}

	phase, err := metaStore.GetLedgerPhase(rangeID)
	if err != nil {
		t.Fatalf("failed to get ledger phase: %v", err)
	}
	if phase != interfaces.LedgerPhaseImmutable {
		t.Errorf("expected IMMUTABLE, got %s", phase)
	}
}

// =============================================================================
// Test: Chunk Calculation for Different Ranges
// =============================================================================

func TestLFSWriter_ChunkCalculation(t *testing.T) {
	tmpDir := t.TempDir()
	lfsDir := filepath.Join(tmpDir, "lfs")
	log := logging.NewTestLogger()
	defer log.Sync()

	writer, err := NewLFSWriter(lfsDir, log)
	if err != nil {
		t.Fatalf("failed to create LFS writer: %v", err)
	}
	defer writer.Close()

	lfs := writer.(*lfsWriter)

	tests := []struct {
		rangeID    uint32
		firstChunk uint32
		lastChunk  uint32
		chunkCount uint32
	}{
		{0, 0, 999, 1000},
		{1, 1000, 1999, 1000},
		{2, 2000, 2999, 1000},
		{10, 10000, 10999, 1000},
	}

	for _, tt := range tests {
		firstChunk := tt.rangeID * ChunksPerRange
		lastChunk := firstChunk + ChunksPerRange - 1

		if firstChunk != tt.firstChunk {
			t.Errorf("range %d: expected firstChunk %d, got %d", tt.rangeID, tt.firstChunk, firstChunk)
		}

		if lastChunk != tt.lastChunk {
			t.Errorf("range %d: expected lastChunk %d, got %d", tt.rangeID, tt.lastChunk, lastChunk)
		}

		firstLedger := lfs.chunkFirstLedger(firstChunk)
		expectedFirst := tt.rangeID*10_000_000 + 2
		if firstLedger != expectedFirst {
			t.Errorf("range %d chunk %d: expected firstLedger %d, got %d",
				tt.rangeID, firstChunk, expectedFirst, firstLedger)
		}
	}
}
