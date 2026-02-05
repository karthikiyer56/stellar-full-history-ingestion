package orchestrator

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stretchr/testify/require"

	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/config"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/logging"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/stores/meta"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/testutil"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/types"
)

func TestBackfillCoordinator_OfflineIntegration(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := &config.Config{
		Service: config.ServiceConfig{
			DataDir: tmpDir,
			Mode:    "backfill",
		},
		Backfill: config.BackfillConfig{
			StartLedger:        2,
			EndLedger:          101,
			LedgerBackend:      "buffered_storage",
			ParallelRanges:     1,
			CheckpointInterval: 10,
		},
		ActiveStores: config.ActiveStoresConfig{
			BasePath: "active/rocksdb",
		},
		ImmutableStores: config.ImmutableStoresConfig{
			LedgersBase: "immutable/ledgers",
			TxHashBase:  "immutable/txhash",
		},
		RocksDB: config.RocksDBStoresConfig{
			Ledger: types.LedgerRocksDBSettings{
				BlockCacheMB:         64,
				WriteBufferMB:        32,
				MaxWriteBufferNumber: 2,
				TargetFileSizeMB:     256,
			},
			TxHash: types.TxHashRocksDBSettings{
				BlockCacheMB:         64,
				WriteBufferMB:        32,
				MaxWriteBufferNumber: 2,
				TargetFileSizeMB:     256,
			},
			Meta: types.MetaRocksDBSettings{
				BlockCacheMB:         64,
				WriteBufferMB:        32,
				MaxWriteBufferNumber: 2,
				TargetFileSizeMB:     256,
			},
		},
		Transition: config.TransitionConfig{
			PreserveRocksDBAfterTransition: true,
		},
	}

	ledgers := testutil.CreateTestLedgers(2, 101)
	mockBackend := testutil.NewMockLedgerBackend(ledgers)

	metaStorePath := filepath.Join(tmpDir, "meta")
	settings := &types.MetaRocksDBSettings{
		WriteBufferMB:        64,
		MaxWriteBufferNumber: 2,
		TargetFileSizeMB:     64,
		BlockCacheMB:         128,
	}
	metaStore, err := meta.NewMetaStore(metaStorePath, settings)
	require.NoError(t, err)
	defer metaStore.Close()

	logger := logging.NewTestLogger()

	coord := NewBackfillCoordinator(cfg, metaStore, logger)

	coord.SetBackendFactory(func(ctx context.Context, rangeID uint32) (ledgerbackend.LedgerBackend, error) {
		return mockBackend, nil
	})

	err = coord.Run(context.Background())
	require.NoError(t, err)

	require.True(t, mockBackend.IsPreparedFlag(), "PrepareRange() should have been called on mock backend")

	rangeState, err := metaStore.GetRangeState(0)
	require.NoError(t, err)
	require.Contains(t, []string{interfaces.RangeStateTransitioning, interfaces.RangeStateComplete}, rangeState)

	ledgerCheckpoint, err := metaStore.GetLastCommittedLedger(0, "ledger")
	require.NoError(t, err)
	require.Equal(t, uint32(101), ledgerCheckpoint)

	txhashCheckpoint, err := metaStore.GetLastCommittedLedger(0, "txhash")
	require.NoError(t, err)
	require.Equal(t, uint32(101), txhashCheckpoint)

	cfCounts, err := metaStore.GetTxHashCounts(0)
	require.NoError(t, err)
	require.NotEmpty(t, cfCounts)

	t.Logf("Integration test passed: Range 0 completed successfully")
	t.Logf("  - Range state: %s", rangeState)
	t.Logf("  - Ledger checkpoint: %d", ledgerCheckpoint)
	t.Logf("  - TxHash checkpoint: %d", txhashCheckpoint)
	t.Logf("  - CF counts: %v", cfCounts)
}
