# tx-hash-store-benchmark

Benchmark tool for measuring read performance of `tx_hash -> ledger_seq` RocksDB stores.

## Overview

This tool reads transaction hashes from a file and looks them up in a RocksDB store,
measuring latency and throughput. Use it to:

- Verify store performance after compaction
- Compare different RocksDB configurations
- Measure cache effectiveness
- Identify performance bottlenecks

## Features

- **Store Statistics Display**: Shows comprehensive store stats before benchmark (SST files, WAL files, per-CF level breakdown)
- **Progress Logging**: Updates every 1% with running statistics
- **Separate Tracking**: Found, NotFound, and Error cases tracked separately
- **Detailed Statistics**: Min, max, avg, stddev, and percentiles (p50, p75, p90, p95, p99)
- **Latency Histogram**: Visual breakdown of latency distribution
- **Log File Support**: Separate log and error file outputs

## Usage

### Basic Benchmark

```bash
tx-hash-store-benchmark \
  --store /data/tx-hash-store \
  --hashes /data/sample-hashes.txt
```

### With Custom Settings

```bash
tx-hash-store-benchmark \
  --store /data/tx-hash-store \
  --hashes /data/sample-hashes.txt \
  --block-cache 2048 \
  --log-file benchmark.log \
  --error-file benchmark.err
```

## Options

| Flag | Description | Default |
|------|-------------|---------|
| `--store` | Path to RocksDB store | (required) |
| `--hashes` | Path to file with hex tx hashes | (required) |
| `--block-cache` | Block cache size in MB | 512 |
| `--log-file` | Output file for logs | stdout |
| `--error-file` | Output file for errors | stdout |
| `--help` | Show help message | |

## Hash File Format

The hashes file should contain one 64-character hex transaction hash per line:

```
0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b
1f2e3d4c5b6a7f8e9d0c1b2a3f4e5d6c7b8a9f0e1d2c3b4a5f6e7d8c9b0a1f2e
# Lines starting with # are ignored
```

To generate a sample file, you can extract hashes from the store:

```bash
# Extract 10,000 random hashes from CF 0
rocksdb_ldb --db=/data/tx-hash-store scan --column_family=0 | \
  head -10000 | \
  awk '{print $1}' > sample-hashes.txt
```

## Output

The benchmark outputs store statistics followed by benchmark results:

### Store Statistics

```
================================================================================
                          STORE STATISTICS
================================================================================

STORAGE OVERVIEW:
  Total SST Files Size:  45.23 GB
  Live SST Files Size:   44.89 GB
  Est. Live Data Size:   44.50 GB
  Memtable Size:         128 MB

FILE COUNTS (Filesystem):
  SST Files:             1,234
  WAL Files:             2 (256 MB)

COLUMN FAMILY SUMMARY:
  Total CFs:             16
  Total Estimated Keys:  2,500,000,000
  Total CF Size:         45.23 GB
  Total CF Files:        1,234

COLUMN FAMILY DETAILS (Per-CF Level Breakdown):

  CF   Est. Keys       Size        L0    L1    L2    L3    L4    L5    L6
  ─────────────────────────────────────────────────────────────────────────
  0    156,250,000     2.83 GB      2     -     4    12    45     -     -
  1    156,250,000     2.83 GB      1     -     3    11    42     -     -
  ...
  ─────────────────────────────────────────────────────────────────────────
  TOT  2,500,000,000  45.23 GB     32     -    64   192   720     -     -

================================================================================
```

### Benchmark Results

```
================================================================================
                           BENCHMARK COMPLETE
================================================================================

SUMMARY:
  Total Time:      1.23s
  Total Lookups:   10,000
  Found:           9,987 (99.87%)
  Not Found:       13 (0.13%)
  Errors:          0 (0.00%)
  Throughput:      8,130.08 lookups/sec

================================================================================
                        FOUND LATENCY STATISTICS
================================================================================
  Count:     9,987
  Min:       1.23us
  Max:       15.67ms
  Avg:       45.23us
  Std Dev:   12.34us

  Percentiles:
    p50:     32.11us
    p75:     48.67us
    p90:     78.34us
    p95:     125.89us
    p99:     456.78us

================================================================================
                        COMBINED LATENCY HISTOGRAM
================================================================================
  0-10us:      1,234 (12.3%)
  10-25us:     2,345 (23.5%)
  25-50us:     3,456 (34.6%)
  50-100us:    2,345 (23.5%)
  100-250us:   456 (4.6%)
  250-500us:   123 (1.2%)
  500us-1ms:   28 (0.3%)
  1ms+:        0 (0.0%)
```

## Metrics Explained

### Latency

- **Min/Max**: Extremes (max often indicates cache misses)
- **Avg**: Mean latency across all lookups
- **Std Dev**: Standard deviation (measure of variability)

### Percentiles

- **p50**: Half of lookups are faster than this
- **p90**: 90% of lookups are faster than this
- **p99**: 99% of lookups are faster than this (tail latency)

### Throughput

- **Queries/sec**: Lookups per second (single-threaded)

## Interpreting Results

### Good Performance

- p50 < 50us (with warm cache)
- p99 < 1ms
- Queries/sec > 10,000

### Signs of Problems

| Symptom | Possible Cause |
|---------|----------------|
| High p99, low p50 | Cache misses, need larger block cache |
| Low throughput | Store not compacted, too many levels |
| High "Not Found" | Wrong hash format, or hashes from different store |
| Increasing latency | Block cache thrashing |
| Many files at L0 | Need compaction |

## Block Cache Tuning

The block cache significantly affects read performance. Test different sizes:

```bash
# Small cache (512 MB - default)
tx-hash-store-benchmark --store /data/store --hashes hashes.txt --block-cache 512

# Large cache (4 GB)
tx-hash-store-benchmark --store /data/store --hashes hashes.txt --block-cache 4096
```

## Example Workflow

```bash
# 1. Generate sample hashes from store
rocksdb_ldb --db=/data/tx-hash-store scan --column_family=0 | \
  head -10000 | awk '{print $1}' > sample-hashes.txt

# 2. Run benchmark before compaction
tx-hash-store-benchmark --store /data/tx-hash-store --hashes sample-hashes.txt

# 3. Run compaction
compact-store --store /data/tx-hash-store --parallel

# 4. Run benchmark after compaction (compare results)
tx-hash-store-benchmark --store /data/tx-hash-store --hashes sample-hashes.txt
```

## Building

```bash
go build ./benchmarking/txHash-ledger-seq-rocksdb-store/
```
