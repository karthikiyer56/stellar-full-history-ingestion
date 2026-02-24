# Configuration

## Overview

All configuration is provided via a TOML file. The service is started with `--config path/to/config.toml --mode (backfill|streaming)`. Mode is a required startup flag, not a config file value.

---

## TOML Reference

### [service]

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `data_dir` | string | **Required** | — | Base directory for all data stores. Relative sub-paths are resolved from here. |
| `http_port` | int | Optional | `8080` | HTTP server port (health/status in backfill; all endpoints in streaming) |

---

### [meta_store]

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `path` | string | Optional | `{data_dir}/meta/rocksdb` | Absolute or relative path to meta store RocksDB directory. WAL is always enabled. |

---

### [active_stores]

Used in streaming mode only. Ignored during backfill.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `base_path` | string | Optional | `{data_dir}/active/rocksdb` | Base directory for active range stores. Individual stores created as `{base_path}/{rangeID:04d}-ledger-store/` |

---

### [immutable_stores]

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `ledgers_base` | string | Optional | `{data_dir}/immutable/ledgers` | Base path for LFS chunk files |
| `txhash_base` | string | Optional | `{data_dir}/immutable/txhash` | Base path for raw txhash flat files and RecSplit index files |

---

### [backfill]

Required when `--mode backfill` is passed at startup. Ignored in streaming mode.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `start_ledger` | uint32 | **Required** | — | Must equal `rangeFirstLedger(N)` for some N. Valid values: 2, 10000002, 20000002, … |
| `end_ledger` | uint32 | **Required** | — | Must equal `rangeLastLedger(M)` for some M ≥ N. Valid values: 10000001, 20000001, 30000001, … |
| `parallel_ranges` | int | Optional | `2` | Concurrent range orchestrators. Recommended: 2. Higher values scale memory linearly. |
| `flush_interval` | int | Optional | `100` | Max ledgers buffered in RAM before flushing to disk per chunk write. |

**Ledger backend (mutually exclusive)**: exactly one of `[backfill.bsb]` or `[backfill.captive_core]` must be present. Both present → startup error. Neither present → startup error.

---

### [backfill.bsb]

Use when fetching historical ledgers from GCS/S3 (recommended for backfill).

Cannot be combined with `[backfill.captive_core]`.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `bucket_path` | string | **Required** | — | GCS or S3 path, e.g. `"gs://stellar-ledgers/mainnet"` |
| `num_instances` | int | Optional | `20` | BSB instances per range orchestrator. Valid values: `10` or `20`. All instances run in parallel within a range. |
| `buffer_size` | int | Optional | `1000` | BSB internal ledger prefetch depth per BSB instance. |
| `num_workers` | int | Optional | `20` | BSB internal download worker count per BSB instance. |

**`num_instances` constraints**:
- `20` (default) → each BSB instance spans 500K ledgers (50 chunks per instance)
- `10` → each BSB instance spans 1M ledgers (100 chunks per instance)
- Both values are exact multiples of the 10K chunk size
- All instances within a range run concurrently — expect non-contiguous chunk completion on crash

---

### [backfill.captive_core]

Use when fetching historical ledgers by replaying via a local `stellar-core` binary.

Cannot be combined with `[backfill.bsb]`.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `binary_path` | string | **Required** | — | Path to `stellar-core` binary |
| `config_path` | string | **Required** | — | Path to `captive-core.cfg` |

**Important constraints when using captive_core for backfill**:
- There is no BSB parallelism — `[backfill.bsb]`'s `num_instances` does not apply
- Each range orchestrator runs a single CaptiveStellarCore instance sequentially through its ledger range
- Running multiple CaptiveStellarCore instances (`parallel_ranges > 1`) requires ~8 GB RAM per instance
- Prefer `[backfill.bsb]` for large-scale backfill; `captive_core` is for environments without GCS access

---

### [streaming]

Required when `--mode streaming` is passed at startup.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `start_ledger` | uint32 | Optional | auto | Override the ledger to start streaming from. Normally derived from meta store (`streaming:last_committed_ledger + 1`). |

---

### [streaming.captive_core]

Required in streaming mode.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `binary_path` | string | **Required** | — | Path to `stellar-core` binary |
| `config_path` | string | **Required** | — | Path to `captive-core.cfg` |

---

### [rocksdb]

Shared RocksDB tuning. Applied to meta store and active store (streaming).

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `block_cache_mb` | int | Optional | `8192` | Shared block cache in MB |
| `write_buffer_mb` | int | Optional | `64` | Write buffer per column family in MB |
| `max_write_buffer_number` | int | Optional | `2` | Max memtables per CF before stall |

---

## Validation Rules

### Range Boundary Validation (Backfill)

```go
func rangeFirstLedger(rangeID uint32) uint32 {
    return (rangeID * 10_000_000) + 2
}
func rangeLastLedger(rangeID uint32) uint32 {
    return ((rangeID + 1) * 10_000_000) + 1
}
```

- `start_ledger` must satisfy `(start_ledger - 2) % 10_000_000 == 0`
- `end_ledger` must satisfy `(end_ledger - 1) % 10_000_000 == 0`
- `end_ledger >= start_ledger`

**Valid `start_ledger` values**: 2, 10000002, 20000002, 30000002, …  
**Valid `end_ledger` values**: 10000001, 20000001, 30000001, …

### Path Resolution

- Relative paths are resolved relative to `data_dir`
- Absolute paths are used as-is
- Parent directories must exist or be creatable by the process

### Streaming Gap Validation

Before streaming starts, the service validates that all ranges prior to the start range are in `COMPLETE` state in the meta store. Any gap results in a fatal startup error.

---

## Example Configurations

### Example 1: Backfill with GCS (Recommended)

Ingest ledgers 2–30,000,001 (ranges 0, 1, 2) from GCS:

```toml
[service]
data_dir = "/data/stellar-rpc"   # required
# http_port = 8080               # optional — defaults to 8080

[backfill]
start_ledger    = 2              # required — must be a valid range start (2, 10000002, …)
end_ledger      = 30000001       # required — must be a valid range end (10000001, 20000001, …)
# parallel_ranges = 2            # optional — defaults to 2
# flush_interval  = 100          # optional — defaults to 100

[backfill.bsb]
bucket_path   = "gs://stellar-ledgers/mainnet"  # required
# num_instances = 20             # optional — defaults to 20; valid values: 10 or 20
# buffer_size   = 1000           # optional — defaults to 1000
# num_workers   = 20             # optional — defaults to 20

[rocksdb]
# All fields optional; shown here with non-default values for backfill (lower cache is fine)
block_cache_mb = 4096            # optional — defaults to 8192; 4096 is sufficient for backfill
# write_buffer_mb         = 64   # optional — defaults to 64
# max_write_buffer_number = 2    # optional — defaults to 2
```

**Run**: `ingestion-workflow --config config.toml --mode backfill`

---

### Example 2: Backfill with CaptiveStellarCore

```toml
[service]
data_dir = "/data/stellar-rpc"   # required
# http_port = 8080               # optional — defaults to 8080

[backfill]
start_ledger    = 30000002       # required
end_ledger      = 50000001       # required
parallel_ranges = 1              # optional — defaults to 2; use 1 here: each captive_core instance needs ~8GB RAM
# flush_interval = 100           # optional — defaults to 100

[backfill.captive_core]
binary_path = "/usr/local/bin/stellar-core"   # required
config_path = "/etc/stellar/captive-core.cfg" # required
```

**Note**: `parallel_ranges = 1` recommended to avoid running two CaptiveStellarCore instances.
BSB parallelism (`num_instances`) does not apply when using `captive_core`.

---

### Example 3: Streaming Mode

```toml
[service]
data_dir  = "/data/stellar-rpc"  # required
# http_port = 8080               # optional — defaults to 8080

# [streaming]
# start_ledger = <auto>          # optional — defaults to streaming:last_committed_ledger + 1
#                                #   (or first ledger after last COMPLETE range if no checkpoint exists)
#                                #   Override only if you need to force a specific resume point.

[streaming.captive_core]
binary_path = "/usr/local/bin/stellar-core"   # required
config_path = "/etc/stellar/captive-core.cfg" # required

[rocksdb]
# All fields optional; shown here with streaming-appropriate values
block_cache_mb          = 8192   # optional — defaults to 8192; keep high for streaming query performance
# write_buffer_mb         = 64   # optional — defaults to 64
# max_write_buffer_number = 2    # optional — defaults to 2
```

**Run**: `ingestion-workflow --config config.toml --mode streaming`

---

### Example 4: Multi-Disk Layout

Spread stores across separate SSD volumes for maximum I/O parallelism. All stores benefit from SSD — meta store requires fast random I/O, active stores require fast write throughput, and immutable stores see large sequential writes during backfill and frequent reads during query serving.

```toml
[service]
data_dir = "/data/stellar-rpc"   # required; used as base for any unset sub-paths below

[meta_store]
# optional — defaults to {data_dir}/meta/rocksdb
path = "/ssd0/stellar-rpc/meta/rocksdb"

[active_stores]
# optional — defaults to {data_dir}/active/rocksdb
# used in streaming mode only; ignored during backfill
base_path = "/ssd1/stellar-rpc/active/rocksdb"

[immutable_stores]
# both optional — default to {data_dir}/immutable/ledgers and {data_dir}/immutable/txhash
ledgers_base = "/ssd2/stellar-rpc/immutable/ledgers"
txhash_base  = "/ssd3/stellar-rpc/immutable/txhash"

[streaming.captive_core]
binary_path = "/usr/local/bin/stellar-core"   # required
config_path = "/etc/stellar/captive-core.cfg" # required
```

---

## Configuration Tips

### Memory Budget, `num_instances` Trade-off, and `flush_interval` Rule

See [12-metrics-and-sizing.md](./12-metrics-and-sizing.md) for the full memory budget breakdown across all modes, the `num_instances` trade-off table, and the `flush_interval` rule.

### `[backfill.bsb]` vs `[backfill.captive_core]`

| Dimension | `[backfill.bsb]` | `[backfill.captive_core]` |
|-----------|-----------------|--------------------------|
| Data source | GCS / S3 bucket | Local stellar-core binary |
| BSB parallelism | 20 instances in parallel per range | Not applicable — sequential |
| RAM per orchestrator | ~205MB (20 BSB instances) | ~8GB (one stellar-core process) |
| Recommended for | Large-scale backfill, cloud environments | Air-gapped / no GCS access |
| `parallel_ranges` safe value | 2 (default) | 1 recommended |

---

## Related Documents

- [09-directory-structure.md](./09-directory-structure.md) — how config paths map to on-disk layout
- [03-backfill-workflow.md](./03-backfill-workflow.md) — how backfill config drives the workflow
- [04-streaming-workflow.md](./04-streaming-workflow.md) — how streaming config is applied
- [01-architecture-overview.md](./01-architecture-overview.md) — hardware requirements
- [12-metrics-and-sizing.md](./12-metrics-and-sizing.md) — memory budgets, num_instances trade-off, flush_interval rule, storage estimates
