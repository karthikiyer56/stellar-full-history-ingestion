package orchestrator

import (
	"context"
	"fmt"
	"github.com/karthikiyer56/stellar-full-history-ingestion/ingestion-workflow/internal/workflow/config"
	"github.com/karthikiyer56/stellar-full-history-ingestion/ingestion-workflow/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/ingestion-workflow/internal/workflow/logging"
	"github.com/karthikiyer56/stellar-full-history-ingestion/ingestion-workflow/internal/workflow/stores/txhash/cf"
	"reflect"
	"testing"
	"time"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestRangeOrchestrator_RunFreshIngestion(t *testing.T) {
	cfg := testConfig(1, 3, 2, "sdf-ledger-close-meta/v1/ledgers/testnet", "backfill-testnet")

	backend := &mockBackend{ledgers: make(map[uint32]xdr.LedgerCloseMeta)}
	for seq := uint32(1); seq <= 3; seq++ {
		lcm, _ := makeTestLedgerCloseMeta(seq, network.TestNetworkPassphrase)
		backend.ledgers[seq] = lcm
	}

	meta := newMockMetaStore()
	lcmStore := newMockLCMStore()
	txStore := newMockTxHashStore()
	logger := logging.NewTestLogger()

	chestrator := NewRangeOrchestrator(0, backend, meta, lcmStore, txStore, cfg, logger)
	if err := chestrator.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if backend.prepared.From() != 1 || backend.prepared.To() != 3 || !backend.prepared.Bounded() {
		t.Fatalf("unexpected prepared range: %s", backend.prepared.String())
	}

	if len(lcmStore.entries) != 3 {
		t.Fatalf("expected 3 LCM entries, got %d", len(lcmStore.entries))
	}
	if len(txStore.entries) != 3 {
		t.Fatalf("expected 3 txhash entries, got %d", len(txStore.entries))
	}

	if !reflect.DeepEqual(meta.commitCalls, []uint32{2, 3}) {
		t.Fatalf("unexpected checkpoints: %v", meta.commitCalls)
	}

	state, _ := meta.GetRangeState(0)
	if state != interfaces.RangeStateTransitioning {
		t.Fatalf("expected range state %s, got %s", interfaces.RangeStateTransitioning, state)
	}

	ledgerPhase, _ := meta.GetLedgerPhase(0)
	if ledgerPhase != interfaces.LedgerPhaseCompacting {
		t.Fatalf("expected ledger phase %s, got %s", interfaces.LedgerPhaseCompacting, ledgerPhase)
	}

	txhashPhase, _ := meta.GetTxHashPhase(0)
	if txhashPhase != interfaces.TxHashPhaseCompacting {
		t.Fatalf("expected txhash phase %s, got %s", interfaces.TxHashPhaseCompacting, txhashPhase)
	}

	if len(meta.stateHistory[0]) < 2 {
		t.Fatalf("expected state transitions, got %v", meta.stateHistory[0])
	}
}

func TestRangeOrchestrator_RunResumeFromCheckpoint(t *testing.T) {
	cfg := testConfig(1, 4, 2, "sdf-ledger-close-meta/v1/ledgers/testnet", "backfill-testnet")

	backend := &mockBackend{ledgers: make(map[uint32]xdr.LedgerCloseMeta)}
	for seq := uint32(1); seq <= 4; seq++ {
		lcm, _ := makeTestLedgerCloseMeta(seq, network.TestNetworkPassphrase)
		backend.ledgers[seq] = lcm
	}

	meta := newMockMetaStore()
	meta.SetRangeState(0, interfaces.RangeStateIngesting)
	meta.SetLedgerPhase(0, interfaces.LedgerPhaseIngesting)
	meta.SetTxHashPhase(0, interfaces.TxHashPhaseIngesting)

	counts := make(map[string]uint64)
	for _, name := range cf.Names {
		counts[name] = 0
	}
	if err := meta.CommitCheckpoint(0, 2, 2, counts); err != nil {
		t.Fatalf("CommitCheckpoint failed: %v", err)
	}
	meta.commitCalls = nil

	lcmStore := newMockLCMStore()
	txStore := newMockTxHashStore()
	logger := logging.NewTestLogger()

	chestrator := NewRangeOrchestrator(0, backend, meta, lcmStore, txStore, cfg, logger)
	if err := chestrator.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	expectedSeqs := []uint32{3, 4}
	if !reflect.DeepEqual(backend.requested, expectedSeqs) {
		t.Fatalf("expected ledgers %v, got %v", expectedSeqs, backend.requested)
	}

	if len(lcmStore.entries) != 2 {
		t.Fatalf("expected 2 LCM entries, got %d", len(lcmStore.entries))
	}
	if len(txStore.entries) != 2 {
		t.Fatalf("expected 2 txhash entries, got %d", len(txStore.entries))
	}
}

func testConfig(start, end uint32, checkpointInterval int, bucketPath, mode string) *config.Config {
	return &config.Config{
		Service: config.ServiceConfig{
			Mode: mode,
		},
		Backfill: config.BackfillConfig{
			StartLedger:        start,
			EndLedger:          end,
			CheckpointInterval: checkpointInterval,
			BufferedStorage: config.BufferedStorageConfig{
				BucketPath: bucketPath,
			},
		},
	}
}

func makeTestLedgerCloseMeta(seq uint32, passphrase string) (xdr.LedgerCloseMeta, [32]byte) {
	txEnv := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				Ext:           xdr.TransactionExt{V: 0},
				SourceAccount: xdr.MustMuxedAddress("GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"),
				Operations:    []xdr.Operation{},
				Fee:           xdr.Uint32(seq),
				SeqNum:        xdr.SequenceNumber(seq),
			},
			Signatures: []xdr.DecoratedSignature{},
		},
	}

	txHash, _ := network.HashTransactionInEnvelope(txEnv, passphrase)
	txMeta := xdr.TransactionResultMeta{
		Result:            xdr.TransactionResultPair{TransactionHash: xdr.Hash(txHash)},
		TxApplyProcessing: xdr.TransactionMeta{V: 3, V3: &xdr.TransactionMetaV3{}},
	}

	ledgerHeader := xdr.LedgerHeaderHistoryEntry{
		Header: xdr.LedgerHeader{LedgerSeq: xdr.Uint32(seq)},
	}

	lcm := xdr.LedgerCloseMeta{V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: ledgerHeader,
			TxProcessing: []xdr.TransactionResultMeta{txMeta},
			TxSet: xdr.GeneralizedTransactionSet{V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					Phases: []xdr.TransactionPhase{{
						V: 0,
						V0Components: &[]xdr.TxSetComponent{{
							TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
								Txs: []xdr.TransactionEnvelope{txEnv},
							},
						}},
					}},
				},
			},
		},
	}

	return lcm, txHash
}

type mockBackend struct {
	ledgers   map[uint32]xdr.LedgerCloseMeta
	requested []uint32
	prepared  ledgerbackend.Range
}

func (m *mockBackend) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	var max uint32
	for seq := range m.ledgers {
		if seq > max {
			max = seq
		}
	}
	return max, nil
}

func (m *mockBackend) GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMeta, error) {
	m.requested = append(m.requested, seq)
	if lcm, ok := m.ledgers[seq]; ok {
		return lcm, nil
	}
	return xdr.LedgerCloseMeta{}, fmt.Errorf("ledger %d not found", seq)
}

func (m *mockBackend) PrepareRange(ctx context.Context, r ledgerbackend.Range) error {
	m.prepared = r
	return nil
}

func (m *mockBackend) IsPrepared(ctx context.Context, ledgerRange ledgerbackend.Range) (bool, error) {
	return true, nil
}

func (m *mockBackend) Close() error {
	return nil
}

type mockMetaStore struct {
	rangeState    map[uint32]string
	ledgerPhase   map[uint32]string
	txHashPhase   map[uint32]string
	lastCommitted map[uint32]map[string]uint32
	ledgerCounts  map[uint32]uint64
	txHashCounts  map[uint32]map[string]uint64
	lfsLastChunk  map[uint32]int32
	commitCalls   []uint32
	stateHistory  map[uint32][]string
}

func newMockMetaStore() *mockMetaStore {
	return &mockMetaStore{
		rangeState:    make(map[uint32]string),
		ledgerPhase:   make(map[uint32]string),
		txHashPhase:   make(map[uint32]string),
		lastCommitted: make(map[uint32]map[string]uint32),
		ledgerCounts:  make(map[uint32]uint64),
		txHashCounts:  make(map[uint32]map[string]uint64),
		lfsLastChunk:  make(map[uint32]int32),
		stateHistory:  make(map[uint32][]string),
	}
}

func (m *mockMetaStore) GetRangeState(rangeID uint32) (string, error) {
	return m.rangeState[rangeID], nil
}

func (m *mockMetaStore) SetRangeState(rangeID uint32, state string) error {
	m.rangeState[rangeID] = state
	m.stateHistory[rangeID] = append(m.stateHistory[rangeID], state)
	return nil
}

func (m *mockMetaStore) GetLedgerPhase(rangeID uint32) (string, error) {
	return m.ledgerPhase[rangeID], nil
}

func (m *mockMetaStore) SetLedgerPhase(rangeID uint32, phase string) error {
	m.ledgerPhase[rangeID] = phase
	return nil
}

func (m *mockMetaStore) GetTxHashPhase(rangeID uint32) (string, error) {
	return m.txHashPhase[rangeID], nil
}

func (m *mockMetaStore) SetTxHashPhase(rangeID uint32, phase string) error {
	m.txHashPhase[rangeID] = phase
	return nil
}

func (m *mockMetaStore) GetLastCommittedLedger(rangeID uint32, subPhase string) (uint32, error) {
	if m.lastCommitted[rangeID] == nil {
		return 0, nil
	}
	return m.lastCommitted[rangeID][subPhase], nil
}

func (m *mockMetaStore) CommitCheckpoint(rangeID, ledgerSeq uint32, ledgerCount uint64, txHashCounts map[string]uint64) error {
	if m.lastCommitted[rangeID] == nil {
		m.lastCommitted[rangeID] = make(map[string]uint32)
	}
	m.lastCommitted[rangeID]["ledger"] = ledgerSeq
	m.lastCommitted[rangeID]["txhash"] = ledgerSeq
	m.ledgerCounts[rangeID] = ledgerCount
	m.txHashCounts[rangeID] = copyCounts(txHashCounts)
	m.commitCalls = append(m.commitCalls, ledgerSeq)
	return nil
}

func (m *mockMetaStore) GetLFSLastChunkWritten(rangeID uint32) (int32, error) {
	chunk, ok := m.lfsLastChunk[rangeID]
	if !ok {
		return -1, nil
	}
	return chunk, nil
}

func (m *mockMetaStore) SetLFSLastChunkWritten(rangeID uint32, chunkID int32) error {
	m.lfsLastChunk[rangeID] = chunkID
	return nil
}

func (m *mockMetaStore) GetTxHashCounts(rangeID uint32) (map[string]uint64, error) {
	counts, ok := m.txHashCounts[rangeID]
	if !ok {
		counts = make(map[string]uint64)
		for _, name := range cf.Names {
			counts[name] = 0
		}
	}
	return copyCounts(counts), nil
}

func (m *mockMetaStore) BothSubPhasesFinishedIngesting(rangeID uint32) (bool, error) {
	ledgerPhase := m.ledgerPhase[rangeID]
	txhashPhase := m.txHashPhase[rangeID]
	return ledgerPhase != interfaces.LedgerPhaseIngesting && txhashPhase != interfaces.TxHashPhaseIngesting, nil
}

func (m *mockMetaStore) BothSubPhasesComplete(rangeID uint32) (bool, error) {
	ledgerPhase := m.ledgerPhase[rangeID]
	txhashPhase := m.txHashPhase[rangeID]
	return ledgerPhase == interfaces.LedgerPhaseImmutable && txhashPhase == interfaces.TxHashPhaseComplete, nil
}

func (m *mockMetaStore) MaybeTransitionRangeState(rangeID uint32) (string, bool, error) {
	state := m.rangeState[rangeID]
	if state == "" {
		return state, false, nil
	}

	switch state {
	case interfaces.RangeStateIngesting:
		finished, _ := m.BothSubPhasesFinishedIngesting(rangeID)
		if finished {
			_ = m.SetRangeState(rangeID, interfaces.RangeStateTransitioning)
			return interfaces.RangeStateTransitioning, true, nil
		}
	case interfaces.RangeStateTransitioning:
		complete, _ := m.BothSubPhasesComplete(rangeID)
		if complete {
			_ = m.SetRangeState(rangeID, interfaces.RangeStateComplete)
			return interfaces.RangeStateComplete, true, nil
		}
	}

	return state, false, nil
}

func (m *mockMetaStore) Close() error {
	return nil
}

type mockLCMStore struct {
	entries map[uint32][]byte
	batches int
}

func newMockLCMStore() *mockLCMStore {
	return &mockLCMStore{entries: make(map[uint32][]byte)}
}

func (m *mockLCMStore) Open() (time.Duration, error) { return 0, nil }

func (m *mockLCMStore) WriteBatch(entries map[uint32][]byte) error {
	for seq, data := range entries {
		m.entries[seq] = cloneBytes(data)
	}
	m.batches++
	return nil
}

func (m *mockLCMStore) Get(ledgerSeq uint32) ([]byte, error) {
	value, ok := m.entries[ledgerSeq]
	if !ok {
		return nil, fmt.Errorf("ledger %d not found", ledgerSeq)
	}
	return cloneBytes(value), nil
}

func (m *mockLCMStore) NewIterator() interfaces.Iterator { return nil }

func (m *mockLCMStore) Compact() (time.Duration, error) { return 0, nil }

func (m *mockLCMStore) GetPath() string { return "<mock>" }

func (m *mockLCMStore) GetSize() (int64, error) { return int64(len(m.entries)), nil }

func (m *mockLCMStore) Close() error { return nil }

type mockTxHashStore struct {
	entries map[string]uint32
	batches int
}

func newMockTxHashStore() *mockTxHashStore {
	return &mockTxHashStore{entries: make(map[string]uint32)}
}

func (m *mockTxHashStore) Open() (time.Duration, error) { return 0, nil }

func (m *mockTxHashStore) WriteBatch(entriesByCF map[string][]interfaces.Entry) error {
	for _, entries := range entriesByCF {
		for _, entry := range entries {
			m.entries[string(entry.Key)] = helpers.BytesToUint32(entry.Value)
		}
	}
	m.batches++
	return nil
}

func (m *mockTxHashStore) Get(txHash []byte) (uint32, bool, error) {
	ledgerSeq, ok := m.entries[string(txHash)]
	return ledgerSeq, ok, nil
}

func (m *mockTxHashStore) NewScanIteratorCF(cfName string) interfaces.Iterator { return nil }

func (m *mockTxHashStore) CompactAll() (map[string]time.Duration, error) {
	return map[string]time.Duration{}, nil
}

func (m *mockTxHashStore) GetPath() string { return "<mock>" }

func (m *mockTxHashStore) GetSize() (int64, error) { return int64(len(m.entries)), nil }

func (m *mockTxHashStore) Close() error { return nil }

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	buf := make([]byte, len(src))
	copy(buf, src)
	return buf
}

func copyCounts(counts map[string]uint64) map[string]uint64 {
	copyMap := make(map[string]uint64)
	for key, value := range counts {
		copyMap[key] = value
	}
	return copyMap
}
