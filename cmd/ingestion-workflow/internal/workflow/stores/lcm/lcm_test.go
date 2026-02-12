package lcm

import (
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/types"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLCMStorePath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lcm-store-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		rangeID      uint32
		expectedPath string
	}{
		{0, filepath.Join(tmpDir, "active", "rocksdb", "0000-ledger-store")},
		{1, filepath.Join(tmpDir, "active", "rocksdb", "0001-ledger-store")},
		{42, filepath.Join(tmpDir, "active", "rocksdb", "0042-ledger-store")},
		{9999, filepath.Join(tmpDir, "active", "rocksdb", "9999-ledger-store")},
	}

	for _, tt := range tests {
		t.Run(tt.expectedPath, func(t *testing.T) {
			settings := &types.LedgerRocksDBSettings{}
			store, err := NewRocksDbLedgerStore(tmpDir, tt.rangeID, settings)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedPath, store.GetPath())

			_, err = store.Open()
			require.NoError(t, err)
			defer store.Close()
		})
	}
}

func TestLCMStoreWriteBatchAndGet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lcm-store-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	settings := &types.LedgerRocksDBSettings{
		BlockCacheMB:         8,
		WriteBufferMB:        4,
		MaxWriteBufferNumber: 2,
	}
	store, err := NewRocksDbLedgerStore(tmpDir, 0, settings)
	require.NoError(t, err)

	openDuration, err := store.Open()
	require.NoError(t, err)
	assert.Greater(t, openDuration.Nanoseconds(), int64(0))
	defer store.Close()

	entries := map[uint32][]byte{
		100: []byte("ledger-100-data"),
		101: []byte("ledger-101-data"),
		200: []byte("ledger-200-data"),
	}
	err = store.WriteBatch(entries)
	require.NoError(t, err)

	data, err := store.Get(100)
	require.NoError(t, err)
	assert.Equal(t, []byte("ledger-100-data"), data)

	data, err = store.Get(101)
	require.NoError(t, err)
	assert.Equal(t, []byte("ledger-101-data"), data)

	data, err = store.Get(200)
	require.NoError(t, err)
	assert.Equal(t, []byte("ledger-200-data"), data)

	_, err = store.Get(999)
	assert.Error(t, err)
}

func TestLCMStoreIterator(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lcm-store-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	settings := &types.LedgerRocksDBSettings{}
	store, err := NewRocksDbLedgerStore(tmpDir, 0, settings)
	require.NoError(t, err)

	_, err = store.Open()
	require.NoError(t, err)
	defer store.Close()

	entries := map[uint32][]byte{
		10: []byte("data-10"),
		20: []byte("data-20"),
		30: []byte("data-30"),
	}
	err = store.WriteBatch(entries)
	require.NoError(t, err)

	iter := store.NewIterator()
	defer iter.Close()

	var count int
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}

	assert.NoError(t, iter.Error())
	assert.Equal(t, 3, count)
}

func TestLCMStoreCompact(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lcm-store-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	settings := &types.LedgerRocksDBSettings{}
	store, err := NewRocksDbLedgerStore(tmpDir, 0, settings)
	require.NoError(t, err)

	_, err = store.Open()
	require.NoError(t, err)
	defer store.Close()

	entries := map[uint32][]byte{
		1: []byte("data-1"),
		2: []byte("data-2"),
	}
	err = store.WriteBatch(entries)
	require.NoError(t, err)

	duration, err := store.Compact()
	require.NoError(t, err)
	assert.Greater(t, duration.Nanoseconds(), int64(0))

	data, err := store.Get(1)
	require.NoError(t, err)
	assert.Equal(t, []byte("data-1"), data)
}

func TestLCMStoreGetSize(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lcm-store-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	settings := &types.LedgerRocksDBSettings{}
	store, err := NewRocksDbLedgerStore(tmpDir, 0, settings)
	require.NoError(t, err)

	_, err = store.Open()
	require.NoError(t, err)
	defer store.Close()

	entries := map[uint32][]byte{
		1: []byte("test-data-1"),
		2: []byte("test-data-2"),
	}
	err = store.WriteBatch(entries)
	require.NoError(t, err)

	size, err := store.GetSize()
	require.NoError(t, err)
	assert.Greater(t, size, int64(0))
}

func TestLCMStoreClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lcm-store-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	settings := &types.LedgerRocksDBSettings{}
	store, err := NewRocksDbLedgerStore(tmpDir, 0, settings)
	require.NoError(t, err)

	_, err = store.Open()
	require.NoError(t, err)

	entries := map[uint32][]byte{
		1: []byte("data"),
	}
	err = store.WriteBatch(entries)
	require.NoError(t, err)

	err = store.Close()
	require.NoError(t, err)

	err = store.Close()
	require.NoError(t, err)
}
