package meta

import (
	"github.com/karthikiyer56/stellar-full-history-ingestion/ingestion-workflow/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/ingestion-workflow/internal/workflow/types"
	"os"
	"path/filepath"
	"testing"
)

func TestMetaStore_RangeState(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "meta")

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	ms, err := NewMetaStore(storePath, settings)
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer ms.Close()

	rangeID := uint32(0)

	state, err := ms.GetRangeState(rangeID)
	if err != nil {
		t.Fatalf("GetRangeState failed: %v", err)
	}
	if state != "" {
		t.Errorf("Expected empty state, got %q", state)
	}

	err = ms.SetRangeState(rangeID, interfaces.RangeStateIngesting)
	if err != nil {
		t.Fatalf("SetRangeState failed: %v", err)
	}

	state, err = ms.GetRangeState(rangeID)
	if err != nil {
		t.Fatalf("GetRangeState failed: %v", err)
	}
	if state != interfaces.RangeStateIngesting {
		t.Errorf("Expected state %q, got %q", interfaces.RangeStateIngesting, state)
	}
}

func TestMetaStore_LedgerPhase(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "meta")

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	ms, err := NewMetaStore(storePath, settings)
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer ms.Close()

	rangeID := uint32(0)

	testPhases := []string{
		interfaces.LedgerPhaseIngesting,
		interfaces.LedgerPhaseCompacting,
		interfaces.LedgerPhaseWritingLFS,
		interfaces.LedgerPhaseImmutable,
	}

	for _, expectedPhase := range testPhases {
		err = ms.SetLedgerPhase(rangeID, expectedPhase)
		if err != nil {
			t.Fatalf("SetLedgerPhase(%q) failed: %v", expectedPhase, err)
		}

		phase, err := ms.GetLedgerPhase(rangeID)
		if err != nil {
			t.Fatalf("GetLedgerPhase failed: %v", err)
		}
		if phase != expectedPhase {
			t.Errorf("Expected phase %q, got %q", expectedPhase, phase)
		}
	}
}

func TestMetaStore_TxHashPhase(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "meta")

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	ms, err := NewMetaStore(storePath, settings)
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer ms.Close()

	rangeID := uint32(0)

	testPhases := []string{
		interfaces.TxHashPhaseIngesting,
		interfaces.TxHashPhaseCompacting,
		interfaces.TxHashPhaseBuildingRecSplit,
		interfaces.TxHashPhaseVerifying,
		interfaces.TxHashPhaseComplete,
	}

	for _, expectedPhase := range testPhases {
		err = ms.SetTxHashPhase(rangeID, expectedPhase)
		if err != nil {
			t.Fatalf("SetTxHashPhase(%q) failed: %v", expectedPhase, err)
		}

		phase, err := ms.GetTxHashPhase(rangeID)
		if err != nil {
			t.Fatalf("GetTxHashPhase failed: %v", err)
		}
		if phase != expectedPhase {
			t.Errorf("Expected phase %q, got %q", expectedPhase, phase)
		}
	}
}

func TestMetaStore_CommitCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "meta")

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	ms, err := NewMetaStore(storePath, settings)
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer ms.Close()

	rangeID := uint32(0)
	ledgerSeq := uint32(12345)
	ledgerCount := uint64(100)
	txHashCounts := map[string]uint64{
		"0": 10, "1": 20, "2": 30, "3": 40,
		"4": 50, "5": 60, "6": 70, "7": 80,
		"8": 90, "9": 100, "a": 110, "b": 120,
		"c": 130, "d": 140, "e": 150, "f": 160,
	}

	err = ms.CommitCheckpoint(rangeID, ledgerSeq, ledgerCount, txHashCounts)
	if err != nil {
		t.Fatalf("CommitCheckpoint failed: %v", err)
	}

	lastCommitted, err := ms.GetLastCommittedLedger(rangeID, "ledger")
	if err != nil {
		t.Fatalf("GetLastCommittedLedger(ledger) failed: %v", err)
	}
	if lastCommitted != ledgerSeq {
		t.Errorf("Expected ledger lastCommitted %d, got %d", ledgerSeq, lastCommitted)
	}

	lastCommitted, err = ms.GetLastCommittedLedger(rangeID, "txhash")
	if err != nil {
		t.Fatalf("GetLastCommittedLedger(txhash) failed: %v", err)
	}
	if lastCommitted != ledgerSeq {
		t.Errorf("Expected txhash lastCommitted %d, got %d", ledgerSeq, lastCommitted)
	}

	retrievedCounts, err := ms.GetTxHashCounts(rangeID)
	if err != nil {
		t.Fatalf("GetTxHashCounts failed: %v", err)
	}

	for cfName, expectedCount := range txHashCounts {
		actualCount := retrievedCounts[cfName]
		if actualCount != expectedCount {
			t.Errorf("CF %s: expected count %d, got %d", cfName, expectedCount, actualCount)
		}
	}
}

func TestMetaStore_LFSLastChunkWritten(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "meta")

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	ms, err := NewMetaStore(storePath, settings)
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer ms.Close()

	rangeID := uint32(0)

	chunkID, err := ms.GetLFSLastChunkWritten(rangeID)
	if err != nil {
		t.Fatalf("GetLFSLastChunkWritten failed: %v", err)
	}
	if chunkID != -1 {
		t.Errorf("Expected initial chunk ID -1, got %d", chunkID)
	}

	testValues := []int32{-1, 0, 1, 10, 100, 1000}
	for _, expected := range testValues {
		err = ms.SetLFSLastChunkWritten(rangeID, expected)
		if err != nil {
			t.Fatalf("SetLFSLastChunkWritten(%d) failed: %v", expected, err)
		}

		chunkID, err = ms.GetLFSLastChunkWritten(rangeID)
		if err != nil {
			t.Fatalf("GetLFSLastChunkWritten failed: %v", err)
		}
		if chunkID != expected {
			t.Errorf("Expected chunk ID %d, got %d", expected, chunkID)
		}
	}
}

func TestMetaStore_CFCountsSerialization(t *testing.T) {
	counts := map[string]uint64{
		"0": 125000, "1": 123500, "2": 124000, "3": 125500,
		"4": 126000, "5": 124500, "6": 125200, "7": 123800,
		"8": 124800, "9": 125300, "a": 126200, "b": 124200,
		"c": 125700, "d": 123900, "e": 124600, "f": 126000,
	}

	serialized := serializeCFCounts(counts)

	if serialized == "" {
		t.Fatal("serializeCFCounts returned empty string")
	}

	parsed := parseCFCounts(serialized)

	for cfName, expectedCount := range counts {
		actualCount := parsed[cfName]
		if actualCount != expectedCount {
			t.Errorf("CF %s: expected count %d, got %d", cfName, expectedCount, actualCount)
		}
	}
}

func TestMetaStore_CFCountsSerializationEmpty(t *testing.T) {
	counts := make(map[string]uint64)
	for _, cfName := range cfNames {
		counts[cfName] = 0
	}

	serialized := serializeCFCounts(counts)
	parsed := parseCFCounts(serialized)

	for cfName := range counts {
		if parsed[cfName] != 0 {
			t.Errorf("CF %s: expected count 0, got %d", cfName, parsed[cfName])
		}
	}

	parsedEmpty := parseCFCounts("")
	for cfName := range counts {
		if parsedEmpty[cfName] != 0 {
			t.Errorf("CF %s: expected count 0 for empty string, got %d", cfName, parsedEmpty[cfName])
		}
	}
}

func TestMetaStore_BothSubPhasesFinishedIngesting(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "meta")

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	ms, err := NewMetaStore(storePath, settings)
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer ms.Close()

	rangeID := uint32(0)

	ms.SetLedgerPhase(rangeID, interfaces.LedgerPhaseIngesting)
	ms.SetTxHashPhase(rangeID, interfaces.TxHashPhaseIngesting)
	finished, err := ms.BothSubPhasesFinishedIngesting(rangeID)
	if err != nil {
		t.Fatalf("BothSubPhasesFinishedIngesting failed: %v", err)
	}
	if finished {
		t.Error("Expected false when both phases are INGESTING")
	}

	ms.SetLedgerPhase(rangeID, interfaces.LedgerPhaseCompacting)
	finished, err = ms.BothSubPhasesFinishedIngesting(rangeID)
	if err != nil {
		t.Fatalf("BothSubPhasesFinishedIngesting failed: %v", err)
	}
	if finished {
		t.Error("Expected false when only ledger phase finished")
	}

	ms.SetTxHashPhase(rangeID, interfaces.TxHashPhaseCompacting)
	finished, err = ms.BothSubPhasesFinishedIngesting(rangeID)
	if err != nil {
		t.Fatalf("BothSubPhasesFinishedIngesting failed: %v", err)
	}
	if !finished {
		t.Error("Expected true when both phases finished ingesting")
	}
}

func TestMetaStore_BothSubPhasesComplete(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "meta")

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	ms, err := NewMetaStore(storePath, settings)
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer ms.Close()

	rangeID := uint32(0)

	ms.SetLedgerPhase(rangeID, interfaces.LedgerPhaseWritingLFS)
	ms.SetTxHashPhase(rangeID, interfaces.TxHashPhaseBuildingRecSplit)
	complete, err := ms.BothSubPhasesComplete(rangeID)
	if err != nil {
		t.Fatalf("BothSubPhasesComplete failed: %v", err)
	}
	if complete {
		t.Error("Expected false when phases not complete")
	}

	ms.SetLedgerPhase(rangeID, interfaces.LedgerPhaseImmutable)
	complete, err = ms.BothSubPhasesComplete(rangeID)
	if err != nil {
		t.Fatalf("BothSubPhasesComplete failed: %v", err)
	}
	if complete {
		t.Error("Expected false when only ledger phase complete")
	}

	ms.SetTxHashPhase(rangeID, interfaces.TxHashPhaseComplete)
	complete, err = ms.BothSubPhasesComplete(rangeID)
	if err != nil {
		t.Fatalf("BothSubPhasesComplete failed: %v", err)
	}
	if !complete {
		t.Error("Expected true when both phases complete")
	}
}

func TestMetaStore_MaybeTransitionRangeState(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "meta")

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	ms, err := NewMetaStore(storePath, settings)
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer ms.Close()

	rangeID := uint32(0)

	ms.SetRangeState(rangeID, interfaces.RangeStateIngesting)
	ms.SetLedgerPhase(rangeID, interfaces.LedgerPhaseIngesting)
	ms.SetTxHashPhase(rangeID, interfaces.TxHashPhaseIngesting)

	newState, transitioned, err := ms.MaybeTransitionRangeState(rangeID)
	if err != nil {
		t.Fatalf("MaybeTransitionRangeState failed: %v", err)
	}
	if transitioned {
		t.Error("Expected no transition when both sub-phases still ingesting")
	}
	if newState != interfaces.RangeStateIngesting {
		t.Errorf("Expected state %q, got %q", interfaces.RangeStateIngesting, newState)
	}

	ms.SetLedgerPhase(rangeID, interfaces.LedgerPhaseCompacting)
	ms.SetTxHashPhase(rangeID, interfaces.TxHashPhaseCompacting)

	newState, transitioned, err = ms.MaybeTransitionRangeState(rangeID)
	if err != nil {
		t.Fatalf("MaybeTransitionRangeState failed: %v", err)
	}
	if !transitioned {
		t.Error("Expected transition to TRANSITIONING")
	}
	if newState != interfaces.RangeStateTransitioning {
		t.Errorf("Expected state %q, got %q", interfaces.RangeStateTransitioning, newState)
	}

	storedState, _ := ms.GetRangeState(rangeID)
	if storedState != interfaces.RangeStateTransitioning {
		t.Errorf("Expected stored state %q, got %q", interfaces.RangeStateTransitioning, storedState)
	}

	newState, transitioned, err = ms.MaybeTransitionRangeState(rangeID)
	if err != nil {
		t.Fatalf("MaybeTransitionRangeState failed: %v", err)
	}
	if transitioned {
		t.Error("Expected no transition when sub-phases not complete")
	}

	ms.SetLedgerPhase(rangeID, interfaces.LedgerPhaseImmutable)
	ms.SetTxHashPhase(rangeID, interfaces.TxHashPhaseComplete)

	newState, transitioned, err = ms.MaybeTransitionRangeState(rangeID)
	if err != nil {
		t.Fatalf("MaybeTransitionRangeState failed: %v", err)
	}
	if !transitioned {
		t.Error("Expected transition to COMPLETE")
	}
	if newState != interfaces.RangeStateComplete {
		t.Errorf("Expected state %q, got %q", interfaces.RangeStateComplete, newState)
	}

	storedState, _ = ms.GetRangeState(rangeID)
	if storedState != interfaces.RangeStateComplete {
		t.Errorf("Expected stored state %q, got %q", interfaces.RangeStateComplete, storedState)
	}
}

func TestMetaStore_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "meta")

	rangeID := uint32(0)
	expectedLedgerSeq := uint32(54321)
	expectedChunkID := int32(42)
	expectedCounts := map[string]uint64{
		"0": 1000, "1": 2000, "2": 3000, "3": 4000,
		"4": 5000, "5": 6000, "6": 7000, "7": 8000,
		"8": 9000, "9": 10000, "a": 11000, "b": 12000,
		"c": 13000, "d": 14000, "e": 15000, "f": 16000,
	}

	{
		settings := &types.MetaRocksDBSettings{
			WriteBufferMB:        64,
			MaxWriteBufferNumber: 2,
			TargetFileSizeMB:     64,
			BlockCacheMB:         128,
		}
		ms, err := NewMetaStore(storePath, settings)
		if err != nil {
			t.Fatalf("Failed to create meta store: %v", err)
		}

		ms.SetRangeState(rangeID, interfaces.RangeStateIngesting)
		ms.SetLedgerPhase(rangeID, interfaces.LedgerPhaseCompacting)
		ms.SetTxHashPhase(rangeID, interfaces.TxHashPhaseBuildingRecSplit)
		ms.CommitCheckpoint(rangeID, expectedLedgerSeq, 100, expectedCounts)
		ms.SetLFSLastChunkWritten(rangeID, expectedChunkID)

		ms.Close()
	}

	{
		settings := &types.MetaRocksDBSettings{
			WriteBufferMB:        64,
			MaxWriteBufferNumber: 2,
			TargetFileSizeMB:     64,
			BlockCacheMB:         128,
		}
		ms, err := NewMetaStore(storePath, settings)
		if err != nil {
			t.Fatalf("Failed to reopen meta store: %v", err)
		}
		defer ms.Close()

		rangeState, _ := ms.GetRangeState(rangeID)
		if rangeState != interfaces.RangeStateIngesting {
			t.Errorf("Expected range state %q, got %q", interfaces.RangeStateIngesting, rangeState)
		}

		ledgerPhase, _ := ms.GetLedgerPhase(rangeID)
		if ledgerPhase != interfaces.LedgerPhaseCompacting {
			t.Errorf("Expected ledger phase %q, got %q", interfaces.LedgerPhaseCompacting, ledgerPhase)
		}

		txhashPhase, _ := ms.GetTxHashPhase(rangeID)
		if txhashPhase != interfaces.TxHashPhaseBuildingRecSplit {
			t.Errorf("Expected txhash phase %q, got %q", interfaces.TxHashPhaseBuildingRecSplit, txhashPhase)
		}

		lastCommitted, _ := ms.GetLastCommittedLedger(rangeID, "txhash")
		if lastCommitted != expectedLedgerSeq {
			t.Errorf("Expected last committed %d, got %d", expectedLedgerSeq, lastCommitted)
		}

		chunkID, _ := ms.GetLFSLastChunkWritten(rangeID)
		if chunkID != expectedChunkID {
			t.Errorf("Expected chunk ID %d, got %d", expectedChunkID, chunkID)
		}

		counts, _ := ms.GetTxHashCounts(rangeID)
		for cfName, expectedCount := range expectedCounts {
			if counts[cfName] != expectedCount {
				t.Errorf("CF %s: expected count %d, got %d", cfName, expectedCount, counts[cfName])
			}
		}
	}
}

func TestMetaStore_MultipleRanges(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "meta")

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	ms, err := NewMetaStore(storePath, settings)
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}
	defer ms.Close()

	range0 := uint32(0)
	range1 := uint32(1)

	ms.SetRangeState(range0, interfaces.RangeStateIngesting)
	ms.SetRangeState(range1, interfaces.RangeStateTransitioning)

	ms.SetLedgerPhase(range0, interfaces.LedgerPhaseIngesting)
	ms.SetLedgerPhase(range1, interfaces.LedgerPhaseWritingLFS)

	ms.SetLFSLastChunkWritten(range0, 10)
	ms.SetLFSLastChunkWritten(range1, 20)

	state0, _ := ms.GetRangeState(range0)
	state1, _ := ms.GetRangeState(range1)
	if state0 != interfaces.RangeStateIngesting || state1 != interfaces.RangeStateTransitioning {
		t.Errorf("Range states not isolated: range0=%q, range1=%q", state0, state1)
	}

	phase0, _ := ms.GetLedgerPhase(range0)
	phase1, _ := ms.GetLedgerPhase(range1)
	if phase0 != interfaces.LedgerPhaseIngesting || phase1 != interfaces.LedgerPhaseWritingLFS {
		t.Errorf("Ledger phases not isolated: range0=%q, range1=%q", phase0, phase1)
	}

	chunk0, _ := ms.GetLFSLastChunkWritten(range0)
	chunk1, _ := ms.GetLFSLastChunkWritten(range1)
	if chunk0 != 10 || chunk1 != 20 {
		t.Errorf("LFS chunks not isolated: range0=%d, range1=%d", chunk0, chunk1)
	}
}

func TestMetaStore_CleanupOnClose(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "meta")

	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	ms, err := NewMetaStore(storePath, settings)
	if err != nil {
		t.Fatalf("Failed to create meta store: %v", err)
	}

	err = ms.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if ms.DB != nil || ms.Opts != nil || ms.ReadOpts != nil || ms.WriteOpts != nil {
		t.Error("Resources not cleaned up after Close()")
	}

	if _, err := os.Stat(storePath); os.IsNotExist(err) {
		t.Error("Store directory should exist after Close()")
	}
}
