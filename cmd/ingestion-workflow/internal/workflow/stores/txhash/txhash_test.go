package txhash

import (
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/stores/txhash/cf"
	"github.com/karthikiyer56/stellar-full-history-ingestion/cmd/ingestion-workflow/internal/workflow/types"
	"testing"

	"github.com/karthikiyer56/stellar-full-history-ingestion/helpers"
)

func TestTxHashStorePath(t *testing.T) {
	tests := []struct {
		rangeID      uint32
		expectedPath string
	}{
		{0, "testdata/active/rocksdb/0000-txhash-store"},
		{1, "testdata/active/rocksdb/0001-txhash-store"},
		{42, "testdata/active/rocksdb/0042-txhash-store"},
		{9999, "testdata/active/rocksdb/9999-txhash-store"},
	}

	settings := &types.TxHashRocksDBSettings{
		BlockCacheMB:         8,
		WriteBufferMB:        32,
		MaxWriteBufferNumber: 4,
	}

	for _, tt := range tests {
		store, err := NewRocksDbTxHashStore("testdata", tt.rangeID, settings)
		if err != nil {
			t.Fatalf("NewTxHashStore failed: %v", err)
		}

		if store.GetPath() != tt.expectedPath {
			t.Errorf("Expected path %s, got %s", tt.expectedPath, store.GetPath())
		}
	}
}

func TestCFRouting(t *testing.T) {
	tests := []struct {
		txHash      []byte
		expectedCF  string
		expectedIdx int
	}{
		{[]byte{0x00}, "0", 0},
		{[]byte{0x10}, "1", 1},
		{[]byte{0x20}, "2", 2},
		{[]byte{0x90}, "9", 9},
		{[]byte{0xa0}, "a", 10},
		{[]byte{0xb5}, "b", 11},
		{[]byte{0xf9}, "f", 15},
	}

	for _, tt := range tests {
		idx := cf.GetIndex(tt.txHash)
		if idx != tt.expectedIdx {
			t.Errorf("GetIndex(%x) = %d, expected %d", tt.txHash[0], idx, tt.expectedIdx)
		}

		name := cf.GetName(tt.txHash)
		if name != tt.expectedCF {
			t.Errorf("GetName(%x) = %s, expected %s", tt.txHash[0], name, tt.expectedCF)
		}
	}
}

func TestTxHashStoreWriteAndGet(t *testing.T) {
	tmpDir := t.TempDir()

	settings := &types.TxHashRocksDBSettings{
		BlockCacheMB:         8,
		WriteBufferMB:        32,
		MaxWriteBufferNumber: 4,
	}

	store, err := NewRocksDbTxHashStore(tmpDir, 1, settings)
	if err != nil {
		t.Fatalf("NewTxHashStore failed: %v", err)
	}

	_, err = store.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	entriesByCF := make(map[string][]interfaces.Entry)

	txHash1 := make([]byte, 32)
	txHash1[0] = 0x00
	ledgerSeq1 := uint32(12345)

	txHash2 := make([]byte, 32)
	txHash2[0] = 0xa0
	ledgerSeq2 := uint32(67890)

	txHash3 := make([]byte, 32)
	txHash3[0] = 0xf5
	ledgerSeq3 := uint32(99999)

	entriesByCF["0"] = []interfaces.Entry{
		{Key: txHash1, Value: helpers.Uint32ToBytes(ledgerSeq1)},
	}
	entriesByCF["a"] = []interfaces.Entry{
		{Key: txHash2, Value: helpers.Uint32ToBytes(ledgerSeq2)},
	}
	entriesByCF["f"] = []interfaces.Entry{
		{Key: txHash3, Value: helpers.Uint32ToBytes(ledgerSeq3)},
	}

	_, err = store.WriteBatch(entriesByCF)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	ledger, found, err := store.Get(txHash1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("txHash1 not found")
	}
	if ledger != ledgerSeq1 {
		t.Errorf("Expected ledger %d, got %d", ledgerSeq1, ledger)
	}

	ledger, found, err = store.Get(txHash2)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("txHash2 not found")
	}
	if ledger != ledgerSeq2 {
		t.Errorf("Expected ledger %d, got %d", ledgerSeq2, ledger)
	}

	ledger, found, err = store.Get(txHash3)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("txHash3 not found")
	}
	if ledger != ledgerSeq3 {
		t.Errorf("Expected ledger %d, got %d", ledgerSeq3, ledger)
	}

	missingHash := make([]byte, 32)
	missingHash[0] = 0x55
	_, found, err = store.Get(missingHash)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if found {
		t.Error("Expected not found for missing hash")
	}
}

func TestTxHashStoreIterator(t *testing.T) {
	tmpDir := t.TempDir()

	settings := &types.TxHashRocksDBSettings{
		BlockCacheMB:         8,
		WriteBufferMB:        32,
		MaxWriteBufferNumber: 4,
	}

	store, err := NewRocksDbTxHashStore(tmpDir, 1, settings)
	if err != nil {
		t.Fatalf("NewTxHashStore failed: %v", err)
	}

	_, err = store.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	entriesByCF := make(map[string][]interfaces.Entry)
	cf0Entries := []interfaces.Entry{}

	for i := 0; i < 100; i++ {
		txHash := make([]byte, 32)
		txHash[0] = 0x00
		txHash[1] = byte(i)
		ledgerSeq := uint32(10000 + i)

		cf0Entries = append(cf0Entries, interfaces.Entry{
			Key:   txHash,
			Value: helpers.Uint32ToBytes(ledgerSeq),
		})
	}

	entriesByCF["0"] = cf0Entries

	_, err = store.WriteBatch(entriesByCF)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	iter := store.NewScanIteratorCF("0")
	if iter == nil {
		t.Fatal("NewScanIteratorCF returned nil")
	}
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
		key := iter.Key()
		if len(key) != 32 {
			t.Errorf("Invalid key length: %d", len(key))
		}
		value := iter.Value()
		if len(value) != 4 {
			t.Errorf("Invalid value length: %d", len(value))
		}
	}

	if iter.Error() != nil {
		t.Fatalf("Iterator error: %v", iter.Error())
	}

	if count != 100 {
		t.Errorf("Expected 100 entries, got %d", count)
	}
}

func TestTxHashStoreCompactAll(t *testing.T) {
	tmpDir := t.TempDir()

	settings := &types.TxHashRocksDBSettings{
		BlockCacheMB:         8,
		WriteBufferMB:        32,
		MaxWriteBufferNumber: 4,
	}

	store, err := NewRocksDbTxHashStore(tmpDir, 1, settings)
	if err != nil {
		t.Fatalf("NewTxHashStore failed: %v", err)
	}

	_, err = store.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	entriesByCF := make(map[string][]interfaces.Entry)
	for _, cfName := range cf.Names {
		entries := []interfaces.Entry{}
		for i := 0; i < 10; i++ {
			txHash := make([]byte, 32)
			idx := cf.GetIndex([]byte{byte(i)})
			if cf.Names[idx] == cfName {
				txHash[0] = byte(i)
			} else {
				continue
			}
			ledgerSeq := uint32(20000 + i)
			entries = append(entries, interfaces.Entry{
				Key:   txHash,
				Value: helpers.Uint32ToBytes(ledgerSeq),
			})
		}
		if len(entries) > 0 {
			entriesByCF[cfName] = entries
		}
	}

	_, err = store.WriteBatch(entriesByCF)
	if err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	durations, err := store.CompactAll()
	if err != nil {
		t.Fatalf("CompactAll failed: %v", err)
	}

	if len(durations) != cf.Count {
		t.Errorf("Expected %d CF durations, got %d", cf.Count, len(durations))
	}

	for _, cfName := range cf.Names {
		if _, ok := durations[cfName]; !ok {
			t.Errorf("Missing duration for CF %s", cfName)
		}
	}
}

func TestTxHashStoreClose(t *testing.T) {
	tmpDir := t.TempDir()

	settings := &types.TxHashRocksDBSettings{
		BlockCacheMB:         8,
		WriteBufferMB:        32,
		MaxWriteBufferNumber: 4,
	}

	store, err := NewRocksDbTxHashStore(tmpDir, 1, settings)
	if err != nil {
		t.Fatalf("NewTxHashStore failed: %v", err)
	}

	_, err = store.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	err = store.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	err = store.Close()
	if err != nil {
		t.Errorf("Second Close failed: %v", err)
	}
}
