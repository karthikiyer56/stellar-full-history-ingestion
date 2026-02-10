package transition

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/interfaces"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/logging"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/stores/txhash"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/stores/txhash/cf"
	"github.com/karthikiyer56/stellar-full-history-ingestion/internal/workflow/types"
)

func TestRecSplitBuilder_BuildAll_BasicOperation(t *testing.T) {
	tmpDir := t.TempDir()

	logger := logging.NewTestLogger()
	defer logger.Close()

	rocksdbPath := filepath.Join(tmpDir, "rocksdb")
	settings := types.TxHashRocksDBSettings{
		BlockCacheMB:         64,
		WriteBufferMB:        32,
		MaxWriteBufferNumber: 2,
	}

	store, err := txhash.NewRocksDbTxHashStore(rocksdbPath, 0, &settings)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	if _, err := store.Open(); err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	testData := map[string][]interfaces.Entry{
		"0": {
			{Key: make32ByteKey(0x00), Value: encodeLedgerSeq(100)},
			{Key: make32ByteKey(0x01), Value: encodeLedgerSeq(101)},
		},
		"5": {
			{Key: make32ByteKey(0x50), Value: encodeLedgerSeq(200)},
			{Key: make32ByteKey(0x51), Value: encodeLedgerSeq(201)},
			{Key: make32ByteKey(0x52), Value: encodeLedgerSeq(202)},
		},
		"f": {
			{Key: make32ByteKey(0xf0), Value: encodeLedgerSeq(300)},
		},
	}

	if _, err := store.WriteBatch(testData); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	cfCounts := map[string]uint64{
		"0": 2,
		"5": 3,
		"f": 1,
	}

	for _, cfName := range cf.Names {
		if _, exists := cfCounts[cfName]; !exists {
			cfCounts[cfName] = 0
		}
	}

	builder := NewRecSplitBuilder(tmpDir, logger)

	durations, err := builder.BuildAll(0, cfCounts, store)
	if err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	if len(durations) != 16 {
		t.Errorf("Expected 16 duration entries, got %d", len(durations))
	}

	indexDir := filepath.Join(tmpDir, "immutable", "txhash", "0000", "index")
	for _, cfName := range cf.Names {
		indexPath := filepath.Join(indexDir, fmt.Sprintf("cf-%s.idx", cfName))

		count := cfCounts[cfName]
		if count == 0 {
			if _, err := os.Stat(indexPath); err == nil {
				t.Errorf("CF %s: index file should not exist for empty CF", cfName)
			}
		} else {
			if _, err := os.Stat(indexPath); os.IsNotExist(err) {
				t.Errorf("CF %s: index file missing at %s", cfName, indexPath)
			}
		}
	}
}

func TestRecSplitBuilder_BuildAll_PathFormat(t *testing.T) {
	tests := []struct {
		rangeID      uint32
		expectedPath string
	}{
		{0, "immutable/txhash/0000/index"},
		{1, "immutable/txhash/0001/index"},
		{42, "immutable/txhash/0042/index"},
		{999, "immutable/txhash/0999/index"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("rangeID=%d", tt.rangeID), func(t *testing.T) {
			tmpDir := t.TempDir()
			logger := logging.NewTestLogger()
			defer logger.Close()

			rocksdbPath := filepath.Join(tmpDir, "rocksdb")
			settings := types.TxHashRocksDBSettings{
				BlockCacheMB:         64,
				WriteBufferMB:        32,
				MaxWriteBufferNumber: 2,
			}

			store, err := txhash.NewRocksDbTxHashStore(rocksdbPath, tt.rangeID, &settings)
			if err != nil {
				t.Fatalf("Failed to create store: %v", err)
			}

			if _, err := store.Open(); err != nil {
				t.Fatalf("Failed to open store: %v", err)
			}
			defer store.Close()

			testData := map[string][]interfaces.Entry{
				"a": {
					{Key: make32ByteKey(0xa0), Value: encodeLedgerSeq(100)},
				},
			}

			if _, err := store.WriteBatch(testData); err != nil {
				t.Fatalf("Failed to write batch: %v", err)
			}

			cfCounts := make(map[string]uint64)
			for _, cfName := range cf.Names {
				if cfName == "a" {
					cfCounts[cfName] = 1
				} else {
					cfCounts[cfName] = 0
				}
			}

			builder := NewRecSplitBuilder(tmpDir, logger)

			_, err = builder.BuildAll(tt.rangeID, cfCounts, store)
			if err != nil {
				t.Fatalf("BuildAll failed: %v", err)
			}

			expectedDir := filepath.Join(tmpDir, tt.expectedPath)
			if _, err := os.Stat(expectedDir); os.IsNotExist(err) {
				t.Errorf("Expected directory %s does not exist", expectedDir)
			}

			indexPath := filepath.Join(expectedDir, "cf-a.idx")
			if _, err := os.Stat(indexPath); os.IsNotExist(err) {
				t.Errorf("Index file missing at %s", indexPath)
			}
		})
	}
}

func TestRecSplitBuilder_BuildAll_EmptyCFs(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewTestLogger()
	defer logger.Close()

	rocksdbPath := filepath.Join(tmpDir, "rocksdb")
	settings := types.TxHashRocksDBSettings{
		BlockCacheMB:         64,
		WriteBufferMB:        32,
		MaxWriteBufferNumber: 2,
	}

	store, err := txhash.NewRocksDbTxHashStore(rocksdbPath, 0, &settings)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	if _, err := store.Open(); err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	cfCounts := make(map[string]uint64)
	for _, cfName := range cf.Names {
		cfCounts[cfName] = 0
	}

	builder := NewRecSplitBuilder(tmpDir, logger)

	durations, err := builder.BuildAll(0, cfCounts, store)
	if err != nil {
		t.Fatalf("BuildAll failed for all-empty CFs: %v", err)
	}

	if len(durations) != 16 {
		t.Errorf("Expected 16 duration entries, got %d", len(durations))
	}

	for _, cfName := range cf.Names {
		if durations[cfName] != 0 {
			t.Errorf("CF %s: expected 0 duration for empty CF, got %v", cfName, durations[cfName])
		}
	}

	indexDir := filepath.Join(tmpDir, "immutable", "txhash", "0000", "index")
	entries, err := os.ReadDir(indexDir)
	if err != nil {
		t.Fatalf("Failed to read index directory: %v", err)
	}

	for _, entry := range entries {
		if entry.Name() != "." && entry.Name() != ".." {
			t.Errorf("Expected no index files for all-empty CFs, found: %s", entry.Name())
		}
	}
}

func TestRecSplitBuilder_BuildAll_ParallelExecution(t *testing.T) {
	tmpDir := t.TempDir()
	logger := logging.NewTestLogger()
	defer logger.Close()

	rocksdbPath := filepath.Join(tmpDir, "rocksdb")
	settings := types.TxHashRocksDBSettings{
		BlockCacheMB:         64,
		WriteBufferMB:        32,
		MaxWriteBufferNumber: 2,
	}

	store, err := txhash.NewRocksDbTxHashStore(rocksdbPath, 0, &settings)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	if _, err := store.Open(); err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	defer store.Close()

	testData := make(map[string][]interfaces.Entry)
	for i, cfName := range cf.Names {
		entries := make([]interfaces.Entry, 10)
		for j := 0; j < 10; j++ {
			key := make32ByteKey(byte(i*16 + j))
			value := encodeLedgerSeq(uint32(i*100 + j))
			entries[j] = interfaces.Entry{Key: key, Value: value}
		}
		testData[cfName] = entries
	}

	if _, err := store.WriteBatch(testData); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	cfCounts := make(map[string]uint64)
	for _, cfName := range cf.Names {
		cfCounts[cfName] = 10
	}

	builder := NewRecSplitBuilder(tmpDir, logger)

	durations, err := builder.BuildAll(0, cfCounts, store)
	if err != nil {
		t.Fatalf("BuildAll failed: %v", err)
	}

	if len(durations) != 16 {
		t.Errorf("Expected 16 duration entries, got %d", len(durations))
	}

	for _, cfName := range cf.Names {
		if durations[cfName] == 0 {
			t.Errorf("CF %s: expected non-zero duration, got 0", cfName)
		}
	}

	indexDir := filepath.Join(tmpDir, "immutable", "txhash", "0000", "index")
	for _, cfName := range cf.Names {
		indexPath := filepath.Join(indexDir, fmt.Sprintf("cf-%s.idx", cfName))
		if _, err := os.Stat(indexPath); os.IsNotExist(err) {
			t.Errorf("CF %s: index file missing at %s", cfName, indexPath)
		}
	}
}

func make32ByteKey(firstByte byte) []byte {
	key := make([]byte, 32)
	key[0] = firstByte
	return key
}

func encodeLedgerSeq(ledgerSeq uint32) []byte {
	return []byte{
		byte(ledgerSeq >> 24),
		byte(ledgerSeq >> 16),
		byte(ledgerSeq >> 8),
		byte(ledgerSeq),
	}
}
