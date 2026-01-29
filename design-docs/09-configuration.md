# Configuration

## Overview

This document provides the complete TOML configuration reference for the Stellar Full History RPC Service.

## TOML Reference

### [service]

Service-level settings.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `data_dir` | string | **Required** | - | Base directory for all data stores |
| `http_port` | int | Optional | `8080` | HTTP server port |
| `mode` | string | Optional | `"streaming"` | Operating mode: "backfill" or "streaming" |

### [meta_store]

Meta store RocksDB settings.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `path` | string | Optional | `{data_dir}/meta/rocksdb` | Meta store RocksDB path |

### [active_stores]

Active store RocksDB settings.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `ledger_path` | string | Optional | `{data_dir}/active/ledger/rocksdb` | Active ledger store path |
| `txhash_path` | string | Optional | `{data_dir}/active/txhash/rocksdb` | Active txhash store path |

### [immutable_stores]

Immutable store paths.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `ledgers_base` | string | Optional | `{data_dir}/immutable/ledgers` | Base path for immutable LFS chunks |
| `txhash_base` | string | Optional | `{data_dir}/immutable/txhash` | Base path for immutable RecSplit indexes |

### [rocksdb]

Shared RocksDB tuning parameters.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `block_cache_mb` | int | Optional | `8192` | Block cache size in MB |
| `write_buffer_mb` | int | Optional | `512` | Write buffer size in MB per CF |
| `max_write_buffer_number` | int | Optional | `2` | Max write buffers per CF |

### [backfill]

Backfill mode settings.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `start_ledger` | uint32 | **Conditional** | - | Required in backfill mode. Must be rangeFirstLedger(N) |
| `end_ledger` | uint32 | **Conditional** | - | Required in backfill mode. Must be rangeLastLedger(N) |
| `ledger_backend` | string | **Conditional** | - | Required in backfill mode. "buffered_storage" or "captive_core" |
| `parallel_ranges` | int | Optional | `2` | Number of parallel 10M range orchestrators |
| `checkpoint_interval` | int | Optional | `1000` | Ledgers between checkpoints |

### [backfill.buffered_storage]

GCS/S3 backend configuration (when ledger_backend="buffered_storage").

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `bucket_path` | string | **Conditional** | - | Required if ledger_backend="buffered_storage" |
| `buffer_size` | int | Optional | `10000` | Ledger buffer size |
| `num_workers` | int | Optional | `200` | Download worker count |

### [backfill.captive_core]

CaptiveStellarCore backend configuration (when ledger_backend="captive_core").

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `binary_path` | string | **Conditional** | - | Required if ledger_backend="captive_core" |
| `config_path` | string | **Conditional** | - | Required if ledger_backend="captive_core" |

### [streaming]

Streaming mode settings.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `checkpoint_interval` | int | Optional | `1` | Ledgers between checkpoints (1 = every ledger) |

### [streaming.captive_core]

CaptiveStellarCore configuration for streaming mode.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `binary_path` | string | **Conditional** | - | Required in streaming mode |
| `config_path` | string | **Conditional** | - | Required in streaming mode |

---

## Validation Rules

### Range Boundary Validation

When using backfill mode, the service validates that `start_ledger` and `end_ledger` align with range boundaries:

```go
func rangeFirstLedger(rangeID uint32) uint32 {
    return (rangeID * 10_000_000) + 2
}

func rangeLastLedger(rangeID uint32) uint32 {
    return ((rangeID + 1) * 10_000_000) + 1
}
```

**Valid start_ledger values**: 2, 10000002, 20000002, 30000002, ...
**Valid end_ledger values**: 10000001, 20000001, 30000001, ...

### Mode-Specific Validation

**Backfill Mode**:
- `start_ledger` and `end_ledger` are **required**
- `ledger_backend` is **required** (must be "buffered_storage" or "captive_core")
- If `ledger_backend="buffered_storage"`, then `[backfill.buffered_storage].bucket_path` is **required**
- If `ledger_backend="captive_core"`, then `[backfill.captive_core].binary_path` and `config_path` are **required**

**Streaming Mode**:
- `[streaming.captive_core].binary_path` and `config_path` are **required**
- `start_ledger` and `end_ledger` are **not used** (streaming starts from last processed ledger)

### Path Validation

- All `*_path` and `*_base` keys accept absolute or relative paths
- Relative paths are resolved relative to `[service].data_dir`
- Parent directories must exist or be creatable

---

## Example Configurations

### Example 1: Backfill Mode with BufferedStorage (GCS)

```toml
[service]
data_dir = "/data/stellar-rpc"
mode = "backfill"

[backfill]
start_ledger = 2
end_ledger = 30000001
ledger_backend = "buffered_storage"
parallel_ranges = 2
checkpoint_interval = 1000

[backfill.buffered_storage]
bucket_path = "gs://stellar-ledgers/mainnet"
buffer_size = 10000
num_workers = 200

[rocksdb]
block_cache_mb = 8192
write_buffer_mb = 512
max_write_buffer_number = 2
```

**Description**: Ingests ledgers 2 to 30,000,001 (ranges 0, 1, 2) from GCS using 2 parallel orchestrators. Creates immutable stores directly, then exits.

---

### Example 2: Backfill Mode with CaptiveStellarCore

```toml
[service]
data_dir = "/data/stellar-rpc"
mode = "backfill"

[backfill]
start_ledger = 30000002
end_ledger = 50000001
ledger_backend = "captive_core"
parallel_ranges = 2

[backfill.captive_core]
binary_path = "/usr/local/bin/stellar-core"
config_path = "/etc/stellar/captive-core.cfg"

[rocksdb]
block_cache_mb = 8192
```

**Description**: Ingests ledgers 30,000,002 to 50,000,001 (ranges 3, 4) using CaptiveStellarCore as the data source.

---

### Example 3: Streaming Mode

```toml
[service]
data_dir = "/data/stellar-rpc"
http_port = 8080
mode = "streaming"

[streaming]
checkpoint_interval = 1

[streaming.captive_core]
binary_path = "/usr/local/bin/stellar-core"
config_path = "/etc/stellar/captive-core.cfg"

[rocksdb]
block_cache_mb = 8192
```

**Description**: Runs in streaming mode, ingesting real-time ledgers and serving queries. Automatically transitions data to immutable stores at 10M boundaries.

---

### Example 4: Multi-Disk Configuration

```toml
[service]
data_dir = "/data/stellar-rpc"  # Default location for meta store
mode = "streaming"

[meta_store]
path = "/ssd1/stellar-rpc/meta/rocksdb"  # Dedicated disk for meta store

[active_stores]
ledger_path = "/nvme/stellar-rpc/active/ledger/rocksdb"  # Fast NVMe for active stores
txhash_path = "/nvme/stellar-rpc/active/txhash/rocksdb"

[immutable_stores]
ledgers_base = "/hdd/stellar-rpc/immutable/ledgers"  # Large HDD for immutable data
txhash_base = "/hdd/stellar-rpc/immutable/txhash"

[streaming.captive_core]
binary_path = "/usr/local/bin/stellar-core"
config_path = "/etc/stellar/captive-core.cfg"
```

**Description**: Optimizes performance by placing active stores on fast NVMe, immutable stores on cheaper HDD, and meta store on dedicated SSD.

---

## Configuration Tips

### Memory Tuning

Total RocksDB memory usage can be estimated as:

```
Total Memory = (write_buffer_mb × max_write_buffer_number × num_column_families) + block_cache_mb
```

For the txhash store with 16 column families:
```
TxHash Memory = (512 MB × 2 × 16) + 8192 MB = 24,576 MB (~24 GB)
```

### Backfill Performance

- **BufferedStorage** (GCS): Faster for bulk ingestion, requires network bandwidth
- **CaptiveStellarCore**: Slower but self-contained, no external dependencies
- **Parallel ranges**: 2 is recommended; higher values increase memory usage proportionally

### Streaming Checkpoint Interval

- Default `checkpoint_interval = 1` provides best crash recovery (resume from last ledger)
- Higher values reduce meta store write frequency but increase recovery time
- Not recommended to change from default

### Path Override Strategy

**Relative paths** (resolved relative to `data_dir`):
```toml
[service]
data_dir = "/data/stellar-rpc"

[active_stores]
ledger_path = "active/ledger/rocksdb"  # Resolves to /data/stellar-rpc/active/ledger/rocksdb
```

**Absolute paths** (used as-is):
```toml
[active_stores]
ledger_path = "/nvme/stellar-rpc/active/ledger/rocksdb"  # Used exactly as specified
```

---

## Related Documents

- [Architecture Overview](./01-architecture-overview.md) - System components and hardware requirements
- [Directory Structure](./08-directory-structure.md) - File system layout and multi-disk scenarios
- [Backfill Workflow](./03-backfill-workflow.md) - Backfill mode operation
- [Streaming Workflow](./04-streaming-workflow.md) - Streaming mode operation

---

## References

- BufferedStorage backend: `rocksdb/ingestion-v2/main.go`
- CaptiveStellarCore usage: `rocksdb/ingestion-v2/` examples
