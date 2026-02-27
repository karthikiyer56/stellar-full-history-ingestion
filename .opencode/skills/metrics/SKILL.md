---
name: metrics
description: Mandatory metrics instrumentation pattern, Logger interface, and all operations that must be tracked for I/O-bound and compute-heavy code
---

# Metrics & Logging

## Mandatory Rule

**Every I/O-bound or compute-heavy operation MUST be instrumented.** No exceptions.

## OpMetrics Pattern

```go
type OpMetrics struct {
    Count     int64
    Bytes     int64
    StartTime time.Time
    Latencies []time.Duration  // for percentile calculation
}
```

### Required Metrics Per Operation

| Metric | Format | Example |
|--------|--------|---------|
| Count | total operations | `1,234,567` |
| Bytes | total data volume | `12.34 GB` |
| Duration | wall-clock time | `1h23m45s` |
| Throughput | rate over time | `1.23K/s, 45.6 MB/s` |
| Latency percentiles | p50, p90, p95, p99 | `p50=1.2ms p90=5.4ms p95=12ms p99=45ms` |

## Progress Logging

Log progress **every 1 minute**. ALWAYS include elapsed from **process start** (not from last log).

```go
if time.Since(lastLog) >= 1*time.Minute {
    elapsed := time.Since(processStart)  // ALWAYS from process start
    logger.Info("progress: %s/%s (%s), elapsed=%s, rate=%s, eta=%s",
        helpers.FormatNumber(count),
        helpers.FormatNumber(total),
        helpers.FormatPercent(float64(count)/float64(total), 1),
        helpers.FormatDuration(elapsed),
        helpers.FormatRate(count-lastCount, time.Since(lastLog)),
        estimateETA(count, total, elapsed))
    lastLog = time.Now()
    lastCount = count
}
```

## Final Summary Format

Every operation must print a final summary:

```
================================================================================
OPERATION COMPLETE: [operation-name]
================================================================================
  Ledgers processed:  1,234,567
  Bytes written:      123.45 GB
  Total duration:     1h23m45s
  Average rate:       1.23K ledgers/s (28.5 MB/s)

  Latency (ledger processing):
    p50=1.2ms  p90=5.4ms  p95=12.3ms  p99=45.6ms
================================================================================
```

## Logger Interface

```go
type Logger interface {
    Info(format string, args ...interface{})
    Error(format string, args ...interface{})
    Debug(format string, args ...interface{})
    Warn(format string, args ...interface{})
    Separator()
    Sync()
}
```

**Log format:** `[2006-01-02T15:04:05Z] [LEVEL] message`

**DualLogger:** Use `--log-file` for INFO/DEBUG output, `--error-file` for WARN/ERROR output.

### Supporting Packages
- `stats` package: throughput calculation, percentile computation, progress with ETA
- `memory` package: RSS monitoring

## Operations to Instrument

| Category | Operations |
|----------|------------|
| **GCS/Remote** | Bucket list, object get, object put, metadata fetch |
| **Filesystem** | File read, file write, directory scan, file delete |
| **RocksDB** | Put, Get, Delete, Iterate, Compact, Flush, Checkpoint |
| **Network** | HTTP requests, gRPC calls, retries |
| **Processing** | Ledger parse, transaction decode, hash compute, verification |
| **Build/Index** | RecSplit build, index write, compaction |
