package backend

import (
	"context"
	"fmt"
	"github.com/karthikiyer56/stellar-full-history-ingestion/ingestion-workflow/internal/workflow/config"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/datastore"
)

// GCSBackend wraps the SDK's BufferedStorageBackend and manages both the backend
// and dataStore lifecycle. The dataStore must be explicitly closed when done.
//
// This composite pattern ensures both resources are cleaned up together during shutdown.
type GCSBackend struct {
	// LedgerBackend is the SDK's interface for reading ledgers.
	// Embedding exposes all interface methods directly on GCSBackend.
	ledgerbackend.LedgerBackend

	// dataStore is the underlying GCS datastore owned by this backend.
	// Must be closed when the backend is closed.
	dataStore datastore.DataStore
}

// NewGCSBackend creates a GCS-backed ledger backend using the SDK's BufferedStorageBackend.
//
// This follows the pattern from rocksdb/ingestion-v2/main.go:139-167:
// 1. Configure datastore with bucket path
// 2. Create dataStoreSchema (LedgersPerFile: 1, FilesPerPartition: 64000)
// 3. Create dataStore
// 4. Configure BufferedStorageBackendConfig (buffer size, workers, retry)
// 5. Create backend with NewBufferedStorageBackend(config, dataStore, schema)
//
// Returns a GCSBackend that owns both the ledgerBackend and dataStore.
// Caller MUST call Close() to release both resources.
func NewGCSBackend(ctx context.Context, cfg *config.BufferedStorageConfig) (*GCSBackend, error) {
	// Step 1: Configure datastore with GCS bucket path
	datastoreConfig := datastore.DataStoreConfig{
		Type: "GCS",
		Params: map[string]string{
			"destination_bucket_path": cfg.BucketPath,
		},
	}

	// Step 2: Define schema (matches pubnet ledger structure)
	dataStoreSchema := datastore.DataStoreSchema{
		LedgersPerFile:    1,
		FilesPerPartition: 64000,
	}

	// Step 3: Create dataStore
	dataStore, err := datastore.NewDataStore(ctx, datastoreConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS datastore: %w", err)
	}

	// Step 4: Configure BufferedStorageBackend
	// Use config values (defaults applied by config.LoadConfig)
	backendConfig := ledgerbackend.BufferedStorageBackendConfig{
		BufferSize: uint32(cfg.BufferSize),
		NumWorkers: uint32(cfg.NumWorkers),
		RetryLimit: 3,
		RetryWait:  5 * time.Second,
	}

	// Step 5: Create backend (connects backend + dataStore + schema)
	backend, err := ledgerbackend.NewBufferedStorageBackend(backendConfig, dataStore, dataStoreSchema)
	if err != nil {
		// If backend creation fails, close dataStore to avoid leak
		dataStore.Close()
		return nil, fmt.Errorf("failed to create buffered storage backend: %w", err)
	}

	return &GCSBackend{
		LedgerBackend: backend,
		dataStore:     dataStore,
	}, nil
}

// Close releases both the backend and dataStore resources.
//
// This method ensures proper cleanup order:
// 1. Close backend first (may flush pending reads/writes)
// 2. Close dataStore second (releases GCS client resources)
//
// Returns the first error encountered, but always attempts both closes.
func (g *GCSBackend) Close() error {
	var firstErr error

	// Close backend first (flushes any pending operations)
	if err := g.LedgerBackend.Close(); err != nil {
		firstErr = fmt.Errorf("failed to close ledger backend: %w", err)
	}

	// Close dataStore second (releases GCS client)
	if err := g.dataStore.Close(); err != nil {
		if firstErr == nil {
			firstErr = fmt.Errorf("failed to close datastore: %w", err)
		}
		// If both fail, return backend error (first encountered)
	}

	return firstErr
}
