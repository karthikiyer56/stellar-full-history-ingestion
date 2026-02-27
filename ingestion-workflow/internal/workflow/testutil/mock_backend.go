package testutil

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Compile-time assertion that MockLedgerBackend implements LedgerBackend
var _ ledgerbackend.LedgerBackend = (*MockLedgerBackend)(nil)

// MockLedgerBackend implements ledgerbackend.LedgerBackend for testing.
// Use NewMockLedgerBackend() to create with test data.
type MockLedgerBackend struct {
	ledgers  map[uint32]xdr.LedgerCloseMeta
	prepared bool
}

func NewMockLedgerBackend(ledgers map[uint32]xdr.LedgerCloseMeta) *MockLedgerBackend {
	return &MockLedgerBackend{ledgers: ledgers}
}

func (m *MockLedgerBackend) GetLedger(ctx context.Context, seq uint32) (xdr.LedgerCloseMeta, error) {
	if lcm, ok := m.ledgers[seq]; ok {
		return lcm, nil
	}
	return xdr.LedgerCloseMeta{}, fmt.Errorf("ledger %d not found", seq)
}

func (m *MockLedgerBackend) PrepareRange(ctx context.Context, r ledgerbackend.Range) error {
	m.prepared = true
	return nil
}

func (m *MockLedgerBackend) IsPrepared(ctx context.Context, r ledgerbackend.Range) (bool, error) {
	return m.prepared, nil
}

func (m *MockLedgerBackend) GetLatestLedgerSequence(ctx context.Context) (uint32, error) {
	var max uint32
	for seq := range m.ledgers {
		if seq > max {
			max = seq
		}
	}
	return max, nil
}

func (m *MockLedgerBackend) Close() error { return nil }

// IsPreparedFlag returns whether PrepareRange was called (for test assertions)
func (m *MockLedgerBackend) IsPreparedFlag() bool {
	return m.prepared
}
