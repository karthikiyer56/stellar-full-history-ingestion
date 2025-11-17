# RocksDB Stellar Blockchain Ingestion Performance Analysis

## Executive Summary

This report analyzes the performance of a RocksDB-based ingestion system for Stellar blockchain data across two EC2 instance types. The findings reveal **critical scalability issues with RocksDB for random-key workloads** (DB2 and DB3), while demonstrating acceptable performance for sequential-key workloads (DB1).

**Key Finding:** RocksDB is **not recommended** for the transaction hash-based datastores (DB2, DB3) due to exponentially increasing compaction times that make long-term ingestion untenable.

---

## Ingestion Configuration (sample set of 1 month)

### Dataset Run Specifications
- **Date Range:** October 1-31, 2025
- **Ledger Range:** 59,173,759 → 59,641,797
- **Total Ledgers:** 468,038 ledgers
- **Ledger Frequency:** 1 ledger ~5 seconds
- **Batch Size:** 2,000 ledgers per batch
- **Total Batches Required:** ~234 batches
- **Average Transactions per Batch:** ~500,000-700,000 transactions
- **Average Transactions per ledger:** ~250
### Datastore Architecture

#### DB1: Sequential Key Store
- **Mapping:** `ledgerSequence (uint32)` → `compressed ledger data`
- **Key Pattern:** Strictly sequential, monotonically increasing
- **Value Size:** ~120 KB (after zstd compression)
- **Compaction Strategy:** Range-based (leverages sequential nature)

#### DB2: Random Key Store (Large Values)
- **Mapping:** `transactionHash (32 bytes)` → `compressed transaction data`
- **Key Pattern:** Completely random, cryptographic hash
- **Value Size:** ~1.2 KB (after zstd compression)
- **Compaction Strategy:** Full-range (cannot optimize)
- **Transaction Count:** ~250 transactions per ledger → ~500K per batch

#### DB3: Random Key Store (Small Values)
- **Mapping:** `transactionHash (32 bytes)` → `ledgerSequence (uint32)`
- **Key Pattern:** Completely random (same as DB2)
- **Value Size:** 4 bytes (uncompressed uint32)
- **Compaction Strategy:** Full-range (cannot optimize)

### Configuration Settings
```
Application-level Compression: ENABLED (zstd)
RocksDB Native Compression: DISABLED
Write Buffer Size: 512 MB × 6 = 3 GB total
L0 Compaction Trigger: 50 files
Background Jobs (for compaction and flushing): 20
Max Open Files: 5000
```

---

## Test Infrastructure

### Machine 1: c5.4xlarge (EBS-backed)
- **CPU:** 16 cores, Intel Xeon Platinum 8275CL @ 3.00GHz
- **RAM:** 32 GB
- **Storage:** 4 TB EBS (network-attached)
- **Storage Type:** gp3 EBS volume (network I/O bottleneck)

### Machine 2: i4i.4xlarge (NVMe-backed)
- **CPU:** 16 cores, Intel Xeon Platinum 8275CL @ 3.00GHz
- **RAM:** 128 GB (4x more than c5.4xlarge)
- **Storage:** 3.75 TB local NVMe
- **Storage Type:** High-performance local SSD

---

## Sample Logs

### c5.4xlarge (EBS)
```
===== Processing batch #1, [Ledger 59173759-59175758] to DB (write + flush + compact) at ledger: 59175758,  (2000 ledgers, 4717
65 transactions) =====
2025/11/15 21:54:44 Batch Total Time: 1m 57s...
2025/11/15 21:54:44     Batch GetLedger Time: 29.565s...
2025/11/15 21:54:44     Batch Total Compression Time: 45.469s...
2025/11/15 21:54:44     Batch IO Time: 10.040s...
2025/11/15 21:54:44     Batch Compute Time (?): 32.255s...
2025/11/15 21:54:44
2025/11/15 21:54:44     DB1: /mnt/xvdf/karthik/rocksdb-dir/db1/oct-2025
2025/11/15 21:54:44             CompressionTime (app level zstd): 11.556s...
2025/11/15 21:54:44             I/O time: 2.036s (write: 231ms, flush: 1.800s, compact: 3ms)
2025/11/15 21:54:44
2025/11/15 21:54:44     DB2: /mnt/xvdf/karthik/rocksdb-dir/db2/oct-2025
2025/11/15 21:54:44             CompressionTime (app level zstd): 33.912s...
2025/11/15 21:54:44             I/O time: 6.845s (write: 1.733s, flush: 5.108s, compact: 3ms)
2025/11/15 21:54:44
2025/11/15 21:54:44     DB3: /mnt/xvdf/karthik/rocksdb-dir/db3/oct-2025
2025/11/15 21:54:44             I/O time: 1.160s (write: 968ms, flush: 187ms, compact: 3ms)
2025/11/15 21:54:44
2025/11/15 21:54:44 ========== Batch processing complete ==========

.....
.....

===== Processing batch #2, [Ledger 59175759-59177758] to DB (write + flush + compact) at ledger: 59177758,  (2000 ledgers, 5230
47 transactions) =====
2025/11/15 21:57:00 Batch Total Time: 2m 16s...
2025/11/15 21:57:00     Batch GetLedger Time: 30.879s...
2025/11/15 21:57:00     Batch Total Compression Time: 48.894s...
2025/11/15 21:57:00     Batch IO Time: 22.179s...
2025/11/15 21:57:00     Batch Compute Time (?): 33.754s...
2025/11/15 21:57:00
2025/11/15 21:57:00     DB1: /mnt/xvdf/karthik/rocksdb-dir/db1/oct-2025
2025/11/15 21:57:00             CompressionTime (app level zstd): 12.530s...
2025/11/15 21:57:00             I/O time: 2.349s (write: 261ms, flush: 2.084s, compact: 3ms)
2025/11/15 21:57:00
2025/11/15 21:57:00     DB2: /mnt/xvdf/karthik/rocksdb-dir/db2/oct-2025
2025/11/15 21:57:00             CompressionTime (app level zstd): 36.364s...
2025/11/15 21:57:00             I/O time: 18.055s (write: 1.946s, flush: 5.451s, compact: 10.658s)
2025/11/15 21:57:00
2025/11/15 21:57:00     DB3: /mnt/xvdf/karthik/rocksdb-dir/db3/oct-2025
2025/11/15 21:57:00             I/O time: 1.775s (write: 1.103s, flush: 225ms, compact: 446ms)
2025/11/15 21:57:00
2025/11/15 21:57:00 ========== Batch processing complete ==========

.....
.....

===== Processing batch #3, [Ledger 59177759-59179758] to DB (write + flush + compact) at ledger: 59179758,  (2000 ledgers, 554739 transactions) =====
2025/11/15 21:59:05 Batch Total Time: 2m 5s...
2025/11/15 21:59:05     Batch GetLedger Time: 25.110s...
2025/11/15 21:59:05     Batch Total Compression Time: 45.172s...
2025/11/15 21:59:05     Batch IO Time: 26.755s...
2025/11/15 21:59:05     Batch Compute Time (?): 28.313s...
2025/11/15 21:59:05
2025/11/15 21:59:05     DB1: /mnt/xvdf/karthik/rocksdb-dir/db1/oct-2025
2025/11/15 21:59:05             CompressionTime (app level zstd): 10.839s...
2025/11/15 21:59:05             I/O time: 1.800s (write: 190ms, flush: 1.605s, compact: 3ms)
2025/11/15 21:59:05
2025/11/15 21:59:05     DB2: /mnt/xvdf/karthik/rocksdb-dir/db2/oct-2025
2025/11/15 21:59:05             CompressionTime (app level zstd): 34.333s...
2025/11/15 21:59:05             I/O time: 22.841s (write: 1.910s, flush: 5.085s, compact: 15.846s)
2025/11/15 21:59:05
2025/11/15 21:59:05     DB3: /mnt/xvdf/karthik/rocksdb-dir/db3/oct-2025
2025/11/15 21:59:05             I/O time: 2.114s (write: 1.203s, flush: 222ms, compact: 687ms)
2025/11/15 21:59:05
2025/11/15 21:59:05 ========== Batch processing complete ==========

.....
.....

===== Processing batch #39, [Ledger 59249759-59251758] to DB (write + flush + compact) at ledger: 59251758,  (2000 ledgers, 568939 transactions) =====
2025/11/16 01:25:11 Batch Total Time: 9m 23s...
2025/11/16 01:25:11     Batch GetLedger Time: 29.193s...
2025/11/16 01:25:11     Batch Total Compression Time: 49.284s...
2025/11/16 01:25:11     Batch IO Time: 7m 32s...
2025/11/16 01:25:11     Batch Compute Time (?): 32.847s...
2025/11/16 01:25:11
2025/11/16 01:25:11     DB1: /mnt/xvdf/karthik/rocksdb-dir/db1/oct-2025
2025/11/16 01:25:11             CompressionTime (app level zstd): 12.332s...
2025/11/16 01:25:11             I/O time: 2.171s (write: 244ms, flush: 1.915s, compact: 11ms)
2025/11/16 01:25:11
2025/11/16 01:25:11     DB2: /mnt/xvdf/karthik/rocksdb-dir/db2/oct-2025
2025/11/16 01:25:11             CompressionTime (app level zstd): 36.953s...
2025/11/16 01:25:11             I/O time: 7m 9s (write: 1.878s, flush: 5.396s, compact: 7m 2s)
2025/11/16 01:25:11
2025/11/16 01:25:11     DB3: /mnt/xvdf/karthik/rocksdb-dir/db3/oct-2025
2025/11/16 01:25:11             I/O time: 20.665s (write: 1.237s, flush: 252ms, compact: 19.176s)
2025/11/16 01:25:11
2025/11/16 01:25:11 ========== Batch processing complete ==========
2025/11/16 01:26:27
========================================

.....
.....

===== Processing batch #40, [Ledger 59251759-59253758] to DB (write + flush + compact) at ledger: 59253758,  (2000 ledgers, 520200 transactions) =====
2025/11/16 01:34:33 Batch Total Time: 9m 22s...
2025/11/16 01:34:33     Batch GetLedger Time: 25.402s...
2025/11/16 01:34:33     Batch Total Compression Time: 42.655s...
2025/11/16 01:34:33     Batch IO Time: 7m 45s...
2025/11/16 01:34:33     Batch Compute Time (?): 28.229s...
2025/11/16 01:34:33
2025/11/16 01:34:33     DB1: /mnt/xvdf/karthik/rocksdb-dir/db1/oct-2025
2025/11/16 01:34:33             CompressionTime (app level zstd): 10.186s...
2025/11/16 01:34:33             I/O time: 1.463s (write: 140ms, flush: 1.311s, compact: 11ms)
2025/11/16 01:34:33
2025/11/16 01:34:33     DB2: /mnt/xvdf/karthik/rocksdb-dir/db2/oct-2025
2025/11/16 01:34:33             CompressionTime (app level zstd): 32.469s...
2025/11/16 01:34:33             I/O time: 7m 23s (write: 1.721s, flush: 4.802s, compact: 7m 16s)
2025/11/16 01:34:33
2025/11/16 01:34:33     DB3: /mnt/xvdf/karthik/rocksdb-dir/db3/oct-2025
2025/11/16 01:34:33             I/O time: 20.937s (write: 1.104s, flush: 221ms, compact: 19.611s)
2025/11/16 01:34:33
2025/11/16 01:34:33 ========== Batch processing complete ==========

.....
.....

========= ROCKSDB STATS as of ledger: 59253758 =====
2025/11/16 01:34:33 [DB1] RocksDB Stats:
2025/11/16 01:34:33   L0 Files: 0
2025/11/16 01:34:33   L1 Files: 0
2025/11/16 01:34:33   L2 Files: 0
2025/11/16 01:34:33   L3 Files: 0
2025/11/16 01:34:33   L4 Files: 0
2025/11/16 01:34:33   L5 Files: 0
2025/11/16 01:34:33   L6 Files: 40
2025/11/16 01:34:33   Estimated Keys: 80,000
2025/11/16 01:34:33   Avg Key+Value Size: 160534.22 bytes
2025/11/16 01:34:33   Total SST Size: 11.96 GB
2025/11/16 01:34:33   Memtable Usage: 2.00 KB
2025/11/16 01:34:33   Compaction Pending: 0
2025/11/16 01:34:33 [DB2] RocksDB Stats:
2025/11/16 01:34:33   L0 Files: 0
2025/11/16 01:34:33   L1 Files: 0
2025/11/16 01:34:33   L2 Files: 0
2025/11/16 01:34:33   L3 Files: 0
2025/11/16 01:34:33   L4 Files: 0
2025/11/16 01:34:33   L5 Files: 0
2025/11/16 01:34:33   L6 Files: 52
2025/11/16 01:34:33   Estimated Keys: 20,673,788
2025/11/16 01:34:33   Avg Key+Value Size: 1282.26 bytes
2025/11/16 01:34:33   Total SST Size: 24.69 GB
2025/11/16 01:34:33   Memtable Usage: 2.00 KB
2025/11/16 01:34:33   Compaction Pending: 0
2025/11/16 01:34:33 [DB3] RocksDB Stats:
2025/11/16 01:34:33   L0 Files: 0
2025/11/16 01:34:33   L1 Files: 0
2025/11/16 01:34:33   L2 Files: 0
2025/11/16 01:34:33   L3 Files: 0
2025/11/16 01:34:33   L4 Files: 0
2025/11/16 01:34:33   L5 Files: 0
2025/11/16 01:34:33   L6 Files: 4
2025/11/16 01:34:33   Estimated Keys: 20,673,788
2025/11/16 01:34:33   Avg Key+Value Size: 45.28 bytes
2025/11/16 01:34:33   Total SST Size: 892.65 MB
2025/11/16 01:34:33   Memtable Usage: 2.00 KB
2025/11/16 01:34:33   Compaction Pending: 0
2025/11/16 01:34:33
2025/11/16 01:34:33
Compression Stats
2025/11/16 01:34:33 LCM Compression:
2025/11/16 01:34:33   Original size:          113.64 GB
2025/11/16 01:34:33   Compressed size:        11.96 GB
2025/11/16 01:34:33   Compression ratio:      89.48% reduction
2025/11/16 01:34:33   Space saved:            101.68 GB
2025/11/16 01:34:33
2025/11/16 01:34:33 TxData Compression:
2025/11/16 01:34:33   Original size:          105.30 GB
2025/11/16 01:34:33   Compressed size:        23.72 GB
2025/11/16 01:34:33   Compression ratio:      77.47% reduction
2025/11/16 01:34:33   Space saved:            81.58 GB
2025/11/16 01:34:33
========================================

.....
.....

===== Processing batch #124, [Ledger 59419759-59421758] to DB (write + flush + compact) at ledger: 59421758,  (2000 ledgers, 713279 transactions) =====
2025/11/17 02:46:57 Batch Total Time: 26m 44s...
2025/11/17 02:46:57     Batch GetLedger Time: 27.499s...
2025/11/17 02:46:57     Batch Total Compression Time: 54.848s...
2025/11/17 02:46:57     Batch IO Time: 24m 49s...
2025/11/17 02:46:57     Batch Compute Time (?): 32.984s...
2025/11/17 02:46:57
2025/11/17 02:46:57     DB1: /mnt/xvdf/karthik/rocksdb-dir/db1/oct-2025
2025/11/17 02:46:57             CompressionTime (app level zstd): 12.197s...
2025/11/17 02:46:57             I/O time: 1.977s (write: 94ms, flush: 1.871s, compact: 11ms)
2025/11/17 02:46:57
2025/11/17 02:46:57     DB2: /mnt/xvdf/karthik/rocksdb-dir/db2/oct-2025
2025/11/17 02:46:57             CompressionTime (app level zstd): 42.651s...
2025/11/17 02:46:57             I/O time: 23m 36s (write: 2.361s, flush: 6.056s, compact: 23m 28s)
2025/11/17 02:46:57
2025/11/17 02:46:57     DB3: /mnt/xvdf/karthik/rocksdb-dir/db3/oct-2025
2025/11/17 02:46:57             I/O time: 1m 11s (write: 1.684s, flush: 298ms, compact: 1m 9s)
2025/11/17 02:46:57
2025/11/17 02:46:57 ========== Batch processing complete ==========
```

### i4i.4xlarge (NVMe)
```
===== Processing batch #1, [Ledger 59173759-59175758] to DB (write + flush + compact) at ledger: 59175758,  (2000 ledgers, 471765 transactions) =====
2025/11/17 01:47:25 Batch Total Time: 1m 30s...
2025/11/17 01:47:25     Batch GetLedger Time: 23.722s...
2025/11/17 01:47:25     Batch Total Compression Time: 34.584s...
2025/11/17 01:47:25     Batch IO Time: 7.412s...
2025/11/17 01:47:25     Batch Compute Time (?): 24.515s...
2025/11/17 01:47:25
2025/11/17 01:47:25     DB1: /mnt/xvdf/karthik/rocksdb-dir/db1/oct-2025
2025/11/17 01:47:25             CompressionTime (app level zstd): 9.980s...
2025/11/17 01:47:25             I/O time: 414ms (write: 159ms, flush: 253ms, compact: 657µs)
2025/11/17 01:47:25
2025/11/17 01:47:25     DB2: /mnt/xvdf/karthik/rocksdb-dir/db2/oct-2025
2025/11/17 01:47:25             CompressionTime (app level zstd): 24.604s...
2025/11/17 01:47:25             I/O time: 2.453s (write: 1.622s, flush: 830ms, compact: 304µs)
2025/11/17 01:47:25
2025/11/17 01:47:25     DB3: /mnt/xvdf/karthik/rocksdb-dir/db3/oct-2025
2025/11/17 01:47:25             I/O time: 4.545s (write: 991ms, flush: 149ms, compact: 3.404s)
2025/11/17 01:47:25
2025/11/17 01:47:25 ========== Batch processing complete ==========

.....
.....

===== Processing batch #2, [Ledger 59175759-59177758] to DB (write + flush + compact) at ledger: 59177758,  (2000 ledgers, 5230
47 transactions) =====
2025/11/17 01:49:02 Batch Total Time: 1m 37s...
2025/11/17 01:49:02     Batch GetLedger Time: 24.336s...
2025/11/17 01:49:02     Batch Total Compression Time: 37.144s...
2025/11/17 01:49:02     Batch IO Time: 9.841s...
2025/11/17 01:49:02     Batch Compute Time (?): 25.516s...
2025/11/17 01:49:02
2025/11/17 01:49:02     DB1: /mnt/xvdf/karthik/rocksdb-dir/db1/oct-2025
2025/11/17 01:49:02             CompressionTime (app level zstd): 10.798s...
2025/11/17 01:49:02             I/O time: 459ms (write: 179ms, flush: 280ms, compact: 398µs)
2025/11/17 01:49:02
2025/11/17 01:49:02     DB2: /mnt/xvdf/karthik/rocksdb-dir/db2/oct-2025
2025/11/17 01:49:02             CompressionTime (app level zstd): 26.346s...
2025/11/17 01:49:02             I/O time: 4.744s (write: 1.794s, flush: 853ms, compact: 2.097s)
2025/11/17 01:49:02
2025/11/17 01:49:02     DB3: /mnt/xvdf/karthik/rocksdb-dir/db3/oct-2025
2025/11/17 01:49:02             I/O time: 4.637s (write: 1.125s, flush: 170ms, compact: 3.342s)
2025/11/17 01:49:02
2025/11/17 01:49:02 ========== Batch processing complete ==========

.....
.....

===== Processing batch #3, [Ledger 59177759-59179758] to DB (write + flush + compact) at ledger: 59179758,  (2000 ledgers, 5547
39 transactions) =====
2025/11/17 01:50:28 Batch Total Time: 1m 26s...
2025/11/17 01:50:28     Batch GetLedger Time: 19.799s...
2025/11/17 01:50:28     Batch Total Compression Time: 34.082s...
2025/11/17 01:50:28     Batch IO Time: 10.896s...
2025/11/17 01:50:28     Batch Compute Time (?): 21.382s...
2025/11/17 01:50:28
2025/11/17 01:50:28     DB1: /mnt/xvdf/karthik/rocksdb-dir/db1/oct-2025
2025/11/17 01:50:28             CompressionTime (app level zstd): 9.215s...
2025/11/17 01:50:28             I/O time: 379ms (write: 150ms, flush: 227ms, compact: 402µs)
2025/11/17 01:50:28
2025/11/17 01:50:28     DB2: /mnt/xvdf/karthik/rocksdb-dir/db2/oct-2025
2025/11/17 01:50:28             CompressionTime (app level zstd): 24.867s...
2025/11/17 01:50:28             I/O time: 5.800s (write: 1.832s, flush: 820ms, compact: 3.148s)
2025/11/17 01:50:28
2025/11/17 01:50:28     DB3: /mnt/xvdf/karthik/rocksdb-dir/db3/oct-2025
2025/11/17 01:50:28             I/O time: 4.717s (write: 1.180s, flush: 182ms, compact: 3.355s)
2025/11/17 01:50:28
2025/11/17 01:50:28 ========== Batch processing complete ==========

.....
.....

===== Processing batch #39, [Ledger 59249759-59251758] to DB (write + flush + compact) at ledger: 59251758,  (2000 ledgers, 568939 transactions) =====
2025/11/17 02:56:35 Batch Total Time: 2m 16s...
2025/11/17 02:56:35     Batch GetLedger Time: 22.725s...
2025/11/17 02:56:35     Batch Total Compression Time: 37.082s...
2025/11/17 02:56:35     Batch IO Time: 52.102s...
2025/11/17 02:56:35     Batch Compute Time (?): 24.287s...
2025/11/17 02:56:35
2025/11/17 02:56:35     DB1: /mnt/xvdf/karthik/rocksdb-dir/db1/oct-2025
2025/11/17 02:56:35             CompressionTime (app level zstd): 10.390s...
2025/11/17 02:56:35             I/O time: 317ms (write: 63ms, flush: 252ms, compact: 1ms)
2025/11/17 02:56:35
2025/11/17 02:56:35     DB2: /mnt/xvdf/karthik/rocksdb-dir/db2/oct-2025
2025/11/17 02:56:35             CompressionTime (app level zstd): 26.693s...
2025/11/17 02:56:35             I/O time: 43.568s (write: 1.745s, flush: 861ms, compact: 40.961s)
2025/11/17 02:56:35
2025/11/17 02:56:35     DB3: /mnt/xvdf/karthik/rocksdb-dir/db3/oct-2025
2025/11/17 02:56:35             I/O time: 8.216s (write: 1.220s, flush: 192ms, compact: 6.803s)
2025/11/17 02:56:35
2025/11/17 02:56:35 ========== Batch processing complete ==========

.....
.....

===== Processing batch #40, [Ledger 59251759-59253758] to DB (write + flush + compact) at ledger: 59253758,  (2000 ledgers, 520200 transactions) =====
2025/11/17 02:58:40 Batch Total Time: 2m 4s...
2025/11/17 02:58:40     Batch GetLedger Time: 19.152s...
2025/11/17 02:58:40     Batch Total Compression Time: 31.883s...
2025/11/17 02:58:40     Batch IO Time: 52.748s...
2025/11/17 02:58:40     Batch Compute Time (?): 20.688s...
2025/11/17 02:58:40
2025/11/17 02:58:40     DB1: /mnt/xvdf/karthik/rocksdb-dir/db1/oct-2025
2025/11/17 02:58:40             CompressionTime (app level zstd): 8.470s...
2025/11/17 02:58:40             I/O time: 255ms (write: 54ms, flush: 199ms, compact: 1ms)
2025/11/17 02:58:40
2025/11/17 02:58:40     DB2: /mnt/xvdf/karthik/rocksdb-dir/db2/oct-2025
2025/11/17 02:58:40             CompressionTime (app level zstd): 23.413s...
2025/11/17 02:58:40             I/O time: 44.185s (write: 1.533s, flush: 801ms, compact: 41.850s)
2025/11/17 02:58:40
2025/11/17 02:58:40     DB3: /mnt/xvdf/karthik/rocksdb-dir/db3/oct-2025
2025/11/17 02:58:40             I/O time: 8.308s (write: 1.104s, flush: 167ms, compact: 7.037s)
2025/11/17 02:58:40
2025/11/17 02:58:40 ========== Batch processing complete ==========
2025/11/17 02:58:40
```

---

## Performance Analysis

_**NOTE: DB I/O times mentioned below includes writes(db.put for each key in batch) + flush (per batch) + compact (per batch)**_

### c5.4xlarge (EBS) - Performance Degradation

#### Early Batches (Batch #1-3)
| Batch | Total Time | DB1 I/O | DB2 I/O | DB3 I/O | DB2 Compact | DB3 Compact |
|-------|-----------|---------|---------|---------|-------------|-------------|
| #1    | 1m 57s    | 2.0s    | 6.8s    | 1.2s    | 3ms         | 3ms         |
| #2    | 2m 16s    | 2.3s    | 18.1s   | 1.8s    | 10.7s       | 446ms       |
| #3    | 2m 5s     | 1.8s    | 22.8s   | 2.1s    | 15.8s       | 687ms       |

#### Mid-Range Batches (Batch #39-40)
| Batch | Total Time | DB1 I/O | DB2 I/O | DB3 I/O | DB2 Compact | DB3 Compact |
|-------|-----------|---------|---------|---------|-------------|-------------|
| #39   | 9m 23s    | 2.2s    | 7m 9s   | 20.7s   | 7m 2s       | 19.2s       |
| #40   | 9m 22s    | 1.5s    | 7m 23s  | 20.9s   | 7m 16s      | 19.6s       |

#### Late Stage (Batch #124)
| Batch | Total Time | DB1 I/O | DB2 I/O | DB3 I/O | DB2 Compact | DB3 Compact |
|-------|-----------|---------|---------|---------|-------------|-------------|
| #124  | 26m 44s   | 2.0s    | 23m 36s | 1m 11s  | 23m 28s     | 1m 9s       |

**Critical Observation:** DB2 compaction time grew from 3ms (batch #1) to **23m 28s** (batch #124) - a **~47 million-fold increase**. This represents exponential degradation.

### i4i.4xlarge (NVMe) - Performance Comparison

_NOTE: Batches #1, #2, #3 are not included for i4i.4x large performance comparison to keep it short. Please refer logs for more information_

#### Early Batches (Batch #39-40)
| Batch | Total Time | DB1 I/O | DB2 I/O | DB3 I/O | DB2 Compact | DB3 Compact |
|-------|-----------|---------|---------|---------|-------------|-------------|
| #39   | 2m 16s    | 317ms   | 43.6s   | 8.2s    | 41.0s       | 6.8s        |
| #40   | 2m 4s     | 255ms   | 44.2s   | 8.3s    | 41.9s       | 7.0s        |

**Performance Improvement over c5.4xlarge:**
- **Total batch time:** 4.1x faster (9m 23s → 2m 16s)
- **DB1 I/O:** 7.0x faster (2.2s → 317ms)
- **DB2 I/O:** 9.8x faster (7m 9s → 43.6s)
- **DB3 I/O:** 2.5x faster (20.7s → 8.2s)
- **DB2 Compact:** 10.3x faster (7m 2s → 41.0s)

### Throughput Analysis

#### c5.4xlarge Throughput
- **Batch #1-3 average:** ~16-17 ledgers/sec
- **Batch #39-40 average:** ~6.2 ledgers/sec (63% degradation)
- **Batch #124:** ~1.25 ledgers/sec (92% degradation from start)
- **ETA at batch #39:** 17h 23m remaining

#### i4i.4xlarge Throughput
- **Batch #39-40 average:** ~18.5 ledgers/sec
- **ETA at batch #39:** 5h 49m remaining
- **Performance:** ~3x faster than c5.4xlarge at same stage

---

## Root Cause Analysis

### 1. Random Key Distribution Problem

**The Core Issue:** RocksDB's LSM-tree architecture is optimized for sequential or prefix-based keys. Transaction hashes are cryptographically random, causing:

- **Write Amplification:** Every new batch spreads writes across the entire key space
- **No Locality Benefits:** Cannot leverage range-based optimizations
- **Full Compaction Requirement:** Unable to use targeted range compaction
- **Exponential Growth:** Each batch compounds the compaction burden

### 2. Compaction Time Growth Pattern

#### DB2 Compaction Time Growth (c5.4xlarge)
```
Batch #1:   3ms      (baseline)
Batch #2:   10.7s    (3,567x increase)
Batch #3:   15.8s    (1.5x increase over batch #2)
Batch #39:  7m 2s    (26.7x increase over batch #3)
Batch #40:  7m 16s   (stable)
Batch #124: 23m 28s  (3.2x increase over batch #40)
```

**Pattern:** Compaction time grows with data size, but the rate accelerates as the database grows. This is characteristic of random-key LSM-tree limitations.

#### DB3 Compaction Time Growth
Despite much smaller values (4 bytes vs 1.2 KB), DB3 exhibits similar degradation:
```
Batch #1:   3ms
Batch #2:   446ms
Batch #3:   687ms
Batch #39:  19.2s
Batch #124: 1m 9s
```

**Implication:** The issue is driven by key randomness, not value size.

### 3. Why DB1 Performs Well

DB1 maintains consistent performance because:
- **Sequential keys** allow range-based compaction
- **Predictable access patterns** enable prefetching
- **No fragmentation** across the key space
- **Compaction targets only new ranges**

DB1 I/O time remains stable at ~1.5-2.5s across all batches, regardless of database size.

---

## Extrapolated Completion Time

### c5.4xlarge (EBS)

#### Current Progress (24+ hours runtime)
- **Completed:** 124 batches (53% of 234 total)
- **Remaining:** 110 batches
- **Current speed:** ~1.25 ledgers/sec
- **Average batch time (recent):** ~25-27 minutes

#### Pessimistic Extrapolation
If compaction time continues to increase:
- **Remaining batches:** 110 batches × 30 minutes = **55 hours**
- **Total time:** 24h (current) + 55h = **~79 hours (3.3 days)**

#### Realistic Extrapolation
Assuming continued degradation (1.5x slower every 40 batches):
- **Estimated total time:** **4-5 days** for October 2025 data

### i4i.4xlarge (NVMe)

#### Based on Batch #39-40 Performance
- **Current speed:** 18.5 ledgers/sec
- **Average batch time:** ~2.2 minutes
- **Total batches:** 234 batches

#### Optimistic Extrapolation
If performance remains stable:
- **Total time:** 234 batches × 2.2 min = **8.6 hours**

#### Realistic Extrapolation
The data shows DB2 compaction time is already 41s at batch #39. Based on c5.4xlarge growth patterns (scaled by 10x performance):
- **Batch #124 estimate:** ~2.3 minutes (vs 23m on c5.4xlarge)
- **Late-stage batches:** ~5-8 minutes each
- **Total time:** **18-24 hours** for October 2025 data

---

## Storage Efficiency

### Compression Statistics (from logs) (as of batch #40)

#### DB1 (LCM Compression )
```
Original size:    113.64 GB
Compressed size:  11.96 GB
Compression ratio: 89.48% reduction
Space saved:      101.68 GB
```

#### DB2 (TxData Compression)
```
Original size:    105.30 GB
Compressed size:  23.72 GB
Compression ratio: 77.47% reduction
Space saved:      81.58 GB
```

#### DB3 (No Compression)
```
Total SST Size:   892.65 MB
Estimated Keys:   20,673,788
Avg Key+Value Size: 45.28 bytes
```

**Total on-disk footprint after 40 batches:**
- DB1: 11.96 GB
- DB2: 24.69 GB
- DB3: 892.65 MB
- **Total: ~37.5 GB** for ~17% of October data

**Projected final size for October 2025:** ~220-240 GB

---

## Is RocksDB Viable for This Use Case?

### DB1 (ledgerSeq → ledger data): ✅ YES

**Verdict:** RocksDB is an **excellent choice** for DB1.

**Reasons:**
- Consistent, predictable performance
- Efficient range compaction
- Sequential key access aligns with LSM-tree strengths
- Sub-2-second I/O time per batch regardless of database size
- Excellent compression ratios

**Recommendation:** Continue using RocksDB for ledger sequence-based storage.

### DB2 (txHash → transaction data): ❌ NO

**Verdict:** RocksDB is **not viable** for production at scale.

**Critical Issues:**
1. **Exponential compaction time growth:** 3ms → 23m 28s over 124 batches
2. **Unpredictable write stalls:** Background compaction would block writes
3. **Poor scalability:** Performance degrades as data accumulates
4. **One year projection:** At current degradation rates, late-stage batches could take **hours each**

**Why forced compaction is necessary:** Without manual compaction after each batch, L0 files would accumulate, triggering write stalls at unpredictable times. This would halt ingestion completely. The current approach trades predictable slowdowns for unpredictable failures.

### DB3 (txHash → ledgerSeq): ❌ NO

**Verdict:** RocksDB is **not viable** for production at scale.

**Critical Issues:**
1. **Same random-key problems as DB2**
2. **Compaction time growth:** 3ms → 1m 9s over 124 batches
3. **Despite small values (4 bytes), key randomness dominates performance**
4. **Will exhibit same exponential degradation as DB2**

---

## Background Compaction vs. Manual Compaction Strategy

### Current Setup: Forced Compaction Every Batch

The current implementation manually calls `CompactRange()` after every 2,000 ledgers:
- DB1: Range-based compaction (efficient)
- DB2/DB3: Full-range compaction (expensive)

This approach trades **predictable slowdowns** for **avoiding unpredictable write stalls**.

### Alternative: Relying on Automatic Background Compaction

#### Expected Behavior During Bulk Ingestion

**Phase 1: Early batches (1-50)**
- L0 files accumulate rapidly (500K+ transactions per batch)
- Background compaction threads attempt to keep up
- **Likely outcome:** Frequent hits to `L0SlowdownWritesTrigger` (100 files)
- **Performance:** Writes slow down but do not stop
- **Batch time estimate:** 3-5 minutes (vs 2-9 minutes with manual compaction)

**Phase 2: Mid-range batches (51-150)**
- L0→L1 compactions accumulate
- L1→L2 compactions become expensive as data spreads across key space
- **Likely outcome:** Frequent write stalls at `L0StopWritesTrigger` (150 files)
- **Performance:** Unpredictable, ranging from 2 minutes to 20 minutes with stalls
- **Batch time estimate:** 5-15 minutes with high variance

**Phase 3: Late batches (151+)**
- Background compaction cannot keep pace with write rate
- L0 files consistently at 100-150
- **Likely outcome:** Near-continuous write stalls
- **Performance:** Severely degraded, potentially worse than manual compaction
- **Batch time estimate:** 15-30 minutes with frequent multi-minute stalls

#### Performance Comparison: Manual vs Background

Based on observed logs and RocksDB behavior patterns:

| Batch Range | Manual Compact (current) | Background Compact (estimated) | Variance |
|-------------|-------------------------|-------------------------------|----------|
| 1-40        | 2-9 min                | 3-6 min                       | Low      |
| 41-100      | 9-15 min               | 8-20 min                      | High     |
| 101-200     | 15-27 min              | 15-40 min                     | Very High|
| 201-234     | 25-35 min (projected)  | 30-60 min (projected)         | Extreme  |

**Key observation:** Background compaction may show marginal improvements initially but becomes **highly unpredictable** as database size increases. Performance variance can be extreme:
- Batch #150: 8 minutes (no write stalls)
- Batch #151: 35 minutes (write stall during critical L1→L2 compaction)
- Batch #152: 12 minutes (stall resolved)

### Post-Ingestion Streaming Performance

#### Scenario 1: Database State After Manual Compaction

**Database state upon completion:**
- All L0 files compacted to L6
- Clean, well-organized LSM tree structure
- No pending compactions
- **Example from batch #40 logs:**
    - DB2: L6 Files: 52, L0-L5: 0
    - DB3: L6 Files: 4, L0-L5: 0

**Streaming behavior (1 ledger every 5 seconds):**
1. New writes populate memtable
2. Memtable flushes create L0 files (every ~512MB)
3. With 250 tx/ledger × 1.2KB = 300KB/ledger
4. **L0 flush frequency:** Every ~1,700 ledgers ≈ every 2.4 hours
5. Background compaction handles L0→L1 gradually
6. **Write latency:** Predictable, ~10-50ms per ledger

**Compaction impact:** Every L0 flush triggers compaction touching **all ~240GB** of existing October data:
- Every 2.4 hours: spike in disk I/O
- Compaction I/O competes with read queries
- **Impact:** 30-60 second periods of degraded read performance every few hours

#### Scenario 2: Database State After Background Compaction

**Database state upon completion:**
- L0: 80-120 files (backlogged)
- L1-L5: Partially compacted
- Significant compaction debt
- Uneven distribution across levels

**Streaming behavior:**
1. Background compaction already behind from bulk ingestion
2. New writes add to L0 faster than compaction processing
3. **Within hours:** `L0SlowdownWritesTrigger` reached
4. **Within days:** Frequent write stalls
5. **Write latency:** Unpredictable, 10-500ms per ledger

**Compaction debt cannot be recovered** during streaming:
- 17,280 ledgers/day added continuously
- Background threads already at capacity
- Each compaction operation becomes more expensive

### Fundamental LSM-Tree Limitation with Random Keys

**Sequential keys (DB1):**
- New data compacts into **new, separate files** at each level
- Old data rarely requires re-compaction
- Compaction I/O proportional to **new data volume**

**Random keys (DB2/DB3):**
- New data merges with **existing files** at each level
- Every compaction touches existing data
- Compaction I/O proportional to **total database size**
- As database grows, **every write becomes more expensive**

### Compaction Overhead Analysis

For a completed October bulk ingestion (240GB in DB2):

**Daily streaming workload:**
- 17,280 ledgers/day × 250 tx/ledger = 4.32M transactions/day
- 4.32M tx × 1.2KB = 5.2GB/day of compressed transaction data

**Daily compaction overhead:**
- L0→L1: 5.2GB written, ~10GB read (overlapping L1 data)
- L1→L2: 5.2GB written, ~25GB read (overlapping L2 data)
- L2→L3: 5.2GB written, ~50GB read
- L3→L4: 5.2GB written, ~100GB read
- **Total I/O:** ~185GB/day for compaction alone

**Infrastructure impact:**
- i4i.4xlarge (NVMe): Manageable but significant overhead
- c5.4xlarge (EBS): **Saturates I/O capacity**

### Compaction-Free Operation: Not Viable

Disabling compaction or scheduling it only during off-hours fails because:

1. **L0 files accumulate indefinitely** → Severe read amplification
2. **Hard stop limit eventually reached** → Database becomes read-only
3. **Query performance degrades** → 10ms reads become 500ms+ reads
4. **Production deployment impossible** → Unacceptable user experience

### Comparative Analysis

**Manual compaction strategy:**
- ✅ Predictable performance during bulk ingestion
- ✅ Clean database state upon completion
- ❌ Performance degradation persists
- ❌ Streaming experiences periodic performance spikes
- **Verdict:** Superior to background compaction, but inadequate for production

**Background compaction strategy:**
- ✅ Marginal speed improvement in early batches
- ❌ Unpredictable write stalls
- ❌ Unclean database state upon completion
- ❌ Streaming experiences constant performance issues
- **Verdict:** Inferior to manual compaction in all meaningful metrics

### Root Cause: Architectural Mismatch

The compaction strategy is not the limiting factor. The fundamental issue is the **data structure choice**. LSM-trees are optimized for sequential writes to sequential keys. Random keys negate the core architectural advantages.

---

## One-Year Ingestion Projection

**_NOTE: The following projections are at current TPS of 50 tx/s or 250 tx/ledger, and not for 500 tx/s_**

### Dataset Scale 
- **Ledgers per month:** ~468,000
- **Ledgers per year:** ~5.6 million
- **Total batches (year):** ~2,800 batches
- **Estimated transactions:** ~250 million

### RocksDB Projection (i4i.4xlarge)

#### Optimistic Scenario (performance remains constant)
- **Time:** ~102 hours = **4.25 days**
- **Reality:** This won't happen due to degradation

#### Realistic Scenario (compaction time grows linearly with data size)
Based on observed 3.2x growth over ~85 batches (batch #40 → #124):
- **Batch #234 (1 month):** ~8-10 minutes
- **Batch #2800 (1 year):** ~50-90 minutes per batch
- **Total time:** **1,000-2,000 hours = 40-80 days**

#### Pessimistic Scenario (compaction time grows super-linearly)
If growth accelerates (observed in early batches):
- **Late-stage batches:** Could take several hours each
- **Total time:** **Months to complete**
- **Risk:** System becomes unusable before completion

### Continuous Streaming After Bulk Ingestion

**The Real Problem:** Even if bulk ingestion completes, you'll need to:
- Ingest 1 ledger every 5 seconds (17,280 ledgers/day)
- This adds ~8.6 batches worth of data per day
- **Daily overhead:** 70-760 minutes of compaction time
- **Conclusion:** Continuous streaming would be severely impacted by compaction backlog

---

## Alternative Solutions

### Recommended Alternatives (need to run more simulations and PoC)

The following recommendations are derived from [Erigon's github page](https://github.com/erigontech/erigon/wiki/Choice-of-storage-engine)

#### 1. **LMDB (Lightning Memory-Mapped Database)** ⭐ RECOMMENDED for DB3

**Strengths:**
- B+-tree architecture (not LSM-tree)
- No compaction needed
- Excellent for small, fixed-size values
- Very fast random reads
- Copy-on-write MVCC
- Single-writer, multiple-reader model

**Perfect for DB3 because:**
- Transaction hash → ledger sequence (4 bytes)
- No compaction overhead
- Random key distribution is not a problem
- Memory-mapped access is extremely fast

**Trade-offs:**
- Requires careful memory management
- Database size should fit in addressable space (space here is disk space and not RAM)
- Single writer may be limiting (but acceptable for your use case)

**Expected performance:** Sub-second writes for 500K transactions per batch, regardless of database size.

#### 2. **libmdbx (Modern LMDB fork)** ⭐ RECOMMENDED for DB2 & DB3

**Strengths:**
- All LMDB benefits plus modern improvements
- Better performance than LMDB
- More configurable
- Active development
- Better documentation and tooling

**Perfect for DB2 & DB3 because:**
- No compaction overhead
- Predictable performance
- Handles random keys efficiently
- Good compression support

**Trade-offs:**
- Slightly more complex API than LMDB
- Newer codebase (less battle-tested than LMDB)

**Expected performance:**
- DB2: 5-20 seconds per batch (stable)
- DB3: <1 second per batch (stable)

#### 3. **BadgerDB (Go-native LSM)** ⚠️ ALTERNATIVE

**Strengths:**
- Pure Go implementation
- Better LSM tuning than RocksDB for some workloads
- Built-in compression
- Active development

**Concerns:**
- Still LSM-based (may have similar issues)
- Would need testing to verify random-key performance

**Verdict:** Worth testing, but LMDB/libmdbx are safer bets.


#### 4. **Hybrid Approach** ⭐ RECOMMENDED ARCHITECTURE

**Design:**
- **DB1 (ledgerSeq → ledger):** Keep RocksDB (it works great)
- **DB2 (txHash → txData):** Use libmdbx or consider object storage
- **DB3 (txHash → ledgerSeq):** Use LMDB or libmdbx

**Rationale:**
- Play to each database's strengths
- RocksDB excels at sequential keys
- LMDB/libmdbx excels at random keys
- No single solution is optimal for all three use cases

---

## Recommendations & Next Steps

### Immediate Actions

#### 1. **Abort Current c5.4xlarge Run** ⚠️
- It will take 4-5 days to complete October
- Not sustainable for production
- Use remaining runtime to test alternatives

#### 2. **Complete i4i.4xlarge Run** ✅
- Finish October ingestion (~18-24 hours remaining)
- Collect complete performance profile
- Use as baseline for comparison

#### 3. **Prototype libmdbx Implementation** 🔧

---

## Conclusion

### Key Findings

1. **RocksDB is excellent for DB1** (sequential keys) but fundamentally unsuited for DB2/DB3 (random keys).

2. **Performance degradation is exponential**, not linear, making long-term operation untenable.

3. **The i4i.4xlarge instance is 3-10x faster** than c5.4xlarge, but this only delays the inevitable degradation.

4. **One-year ingestion with current approach is impractical**, potentially requiring months to complete.

5. **Continuous streaming after bulk ingestion would be severely impacted** by ongoing compaction overhead.
