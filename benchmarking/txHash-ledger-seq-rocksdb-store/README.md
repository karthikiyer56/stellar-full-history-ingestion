# tx-hash-store-benchmark

Benchmark tool for measuring read performance of `tx_hash -> ledger_seq` RocksDB stores.

## Overview

This tool reads transaction hashes from a file and looks them up in a RocksDB store,
measuring latency and throughput. Use it to:

- Verify store performance after compaction
- Compare different RocksDB configurations
- Measure cache effectiveness
- Identify performance bottlenecks

## Usage

### Basic Benchmark

```bash
tx-hash-store-benchmark \
  --store /data/tx-hash-store \
  --hashes /data/sample-hashes.txt \
  --count 10000
```

### With Custom Settings

```bash
tx-hash-store-benchmark \
  --store /data/tx-hash-store \
  --hashes /data/sample-hashes.txt \
  --count 100000 \
  --warmup 1000 \
  --block-cache 2048 \
  --randomize=true
```

## Options

| Flag | Description | Default |
|------|-------------|---------|
| `--store` | Path to RocksDB store | (required) |
| `--hashes` | Path to file with hex tx hashes | (required) |
| `--count` | Number of lookups to perform | 10000 |
| `--warmup` | Number of warmup lookups (not counted) | 100 |
| `--block-cache` | Block cache size in MB | 512 |
| `--randomize` | Randomize hash order for lookups | true |
| `--percentiles` | Show latency percentiles | true |
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

```
================================================================================
                         BENCHMARK RESULTS
================================================================================

Store:           /data/tx-hash-store
Lookups:         10000
Found:           9987 (99.87%)
Not Found:       13 (0.13%)

LATENCY:
  Min:           1.23us
  Max:           15.67ms
  Avg:           45.23us
  Median:        32.11us

PERCENTILES:
  p50:           32.11us
  p75:           48.67us
  p90:           78.34us
  p95:           125.89us
  p99:           456.78us

THROUGHPUT:
  Total Time:    1.23s
  Queries/sec:   8130.08

================================================================================
```

## Metrics Explained

### Latency

- **Min/Max**: Extremes (max often indicates cache misses)
- **Avg**: Mean latency across all lookups
- **Median**: 50th percentile (better for skewed distributions)

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

## Cache Warming

The `--warmup` option performs initial lookups that are not counted in statistics.
This simulates a "warm" cache scenario.

```bash
# Test cold cache performance
tx-hash-store-benchmark --store /data/store --hashes hashes.txt --warmup 0

# Test warm cache performance
tx-hash-store-benchmark --store /data/store --hashes hashes.txt --warmup 10000
```

## Randomization

By default, hashes are randomized before lookup. This simulates real-world access
patterns where lookups are not sequential.

```bash
# Sequential access (best-case for caching)
tx-hash-store-benchmark --store /data/store --hashes hashes.txt --randomize=false

# Random access (realistic)
tx-hash-store-benchmark --store /data/store --hashes hashes.txt --randomize=true
```

## Block Cache Tuning

The block cache significantly affects read performance. Test different sizes:

```bash
# Small cache (512 MB)
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
