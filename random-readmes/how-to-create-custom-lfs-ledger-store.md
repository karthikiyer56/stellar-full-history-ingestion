We want to implement a file based ledger storage in https://github.com/karthikiyer56/stellar-full-history-ingestion and see how it compares to the rocksdb ledger storage implementation.

Here is a specification for how the file based ledger storage should be implemented:

## Data Model

| Term | Definition |
|------|------------|
| Ledger | Opaque xdr binary blob, variable size, immutable once written |
| Sequence | Monotonically increasing 32-bit unsigned integer starting at 2 |
| Chunk | Fixed-capacity container holding up to N consecutive ledgers |

## Directory Layout
```
<data_dir>/
└── chunks/
    ├── 0000/
    │   ├── 000000.data
    │   ├── 000000.index
    │   ├── 000001.data
    │   ├── 000001.index
    │   └── ...
    ├── 0001/
    │   ├── 001000.data
    │   ├── 001000.index
    │   └── ...
    └── ...
```

Path pattern:
- `chunks/XXXX/YYYYYY.data`
- `chunks/XXXX/YYYYYY.index`

Where:
- XXXX = chunk_id / 1000 (4 digits, zero-padded)
- YYYYYY = chunk_id (6 digits, zero-padded)

## Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| data_dir | string | "./ledger-store" | Root directory for all data |
| chunk_size | u32 | fixed to 10000 for the POC | Maximum ledgers per chunk |

## File Formats

### YYYYYY.index

Variable-width offset index with minimal header.
```
┌─────────────────────────────────────────┐
│ Header (8 bytes)                        │
├─────────────────────────────────────────┤
│ version: u8                             │
│ offset_size: u8 (4 or 8)                │
│ reserved: [u8; 6]                       │
├─────────────────────────────────────────┤
│ Offsets                                 │
├─────────────────────────────────────────┤
│ offset[0]: u32 or u64                   │
│ offset[1]: u32 or u64                   │
│ ...                                     │
│ offset[count]: u32 or u64               │
└─────────────────────────────────────────┘
```

#### Index Header

| Field | Offset | Size | Description |
|-------|--------|------|-------------|
| version | 0 | 1 | Format version (current: 1) |
| offset_size | 1 | 1 | Bytes per offset entry (4 or 8) |
| reserved | 2 | 6 | Reserved for future use, zero-filled |

Total header size: 8 bytes

#### Offset Entries

- If offset_size = 4: each offset is a little-endian u32
- If offset_size = 8: each offset is a little-endian u64
- count + 1 offsets stored to enable size calculation for the last record

Deriving count from file size:
```
count = (file_size(index) - 8) / offset_size - 1
```

Record size calculation:
```
record_size[i] = offset[i + 1] - offset[i]
```

### YYYYYY.data

Append-only file containing zstd-compressed ledger records.
```
┌─────────────────────────────────────────┐
│ Record 0: zstd(ledger_0)                │
├─────────────────────────────────────────┤
│ Record 1: zstd(ledger_1)                │
├─────────────────────────────────────────┤
│ ...                                     │
└─────────────────────────────────────────┘
```

Each record is a raw zstd-compressed ledger blob with no framing. Record boundaries are determined entirely by the index.

## Operations

### Sequence to Location Mapping
```
chunk_id = (sequence - 2) / chunk_size
local_index = (sequence - 2) % chunk_size
parent_dir = chunk_id / 1000
path = sprintf("chunks/%04d/%06d", parent_dir, chunk_id)
```

Example with chunk_size=10000:

| Sequence | chunk_id | local_index | Path |
|----------|----------|-------------|------|
| 2 | 0 | 0 | chunks/0000/000000 |
| 10001 | 0 | 9999 | chunks/0000/000000 |
| 10002 | 1 | 0 | chunks/0000/000001 |
| 1234567 | 123 | 4565 | chunks/0000/000123 |
| 10010002 | 1001 | 0 | chunks/0001/001001 |

### Read: Point Lookup
```
GET(sequence: u32) -> bytes | NOT_FOUND

1. Compute chunk_id and local_index from sequence

2. Open index file, read header (version, offset_size)

3. Read two adjacent offsets from index using pread:
   entry_pos = 8 + local_index * offset_size
   pread(index_file, offset_size * 2, entry_pos)
   Parse: start_offset, end_offset

4. Close index file

5. Compute record size:
   record_size = end_offset - start_offset

6. Open data file, read compressed record using pread:
   compressed = pread(data_file, record_size, start_offset)

7. Close data file

8. Decompress:
   ledger = zstd_decompress(compressed)

9. Return ledger
```

### Write: Append

When appending a ledger:
1. Compute the chunk_id and local_index from the next sequence number
2. If starting a new chunk, create the parent directory if needed and create an empty data file
3. Compress the ledger with zstd and append to the data file
4. Track the offset in memory
5. When the chunk is full, determine offset_size based on final data file size, then write the index file

## Limits

| Limit | Value | Rationale |
|-------|-------|-----------|
| Min sequence | 2 | Stellar ledger numbering |
| Max sequence | 2^32 - 1 (~4.29 billion) | u32 sequence |
| Max chunk data (4-byte offset) | 4 GB | u32 offset |
| Max chunk data (8-byte offset) | 2^64 bytes | u64 offset |
| Max chunks | ~429,497 | Derived from max sequence with chunk_size=10000 |
| Max files per directory | 2000 | 1000 chunks × 2 files |
| Max subdirectories | 430 | At u32 max with chunk_size=10000 |

## Benchmark Metrics

The following metrics should be collected when comparing against alternative implementations:

**Latency:**
- Point lookup p50, p99, p999

**Throughput:**
- Writes per second
- Ingestion time

**Disk space:**
- Total storage size (data + index files)

**Memory usage:**
- Peak memory usage
- Steady-state memory usage

## Recommended Go Libraries

| Purpose | Library |
|---------|---------|
| Zstd | `github.com/klauspost/compress/zstd` |
| pread | `os.File.ReadAt()` (standard library) |
| Binary encoding | `encoding/binary` (standard library) |


## Versioning

Format changes require incrementing version in index header. Readers must reject unknown versions.

| Version | Description |
|---------|-------------|
| 1 | Initial format (this spec) |

## Future Enhancements

The following optimizations could be added in future iterations but are not necessary for this POC:

### Index Caching

Cache index file contents in memory using an LRU cache to avoid repeated file I/O for frequently accessed chunks.

- **Use case**: Workloads with temporal locality (e.g., recent ledgers accessed more frequently)
- **Trade-off**: Memory usage vs read latency
- **Implementation**: LRU cache keyed by chunk_id, storing parsed index offsets (~40 KB per chunk with 4-byte offsets)
- **Recommended library**: `github.com/hashicorp/golang-lru/v2`

### Ledger Range Queries

Support efficient retrieval of consecutive ledgers.

- **Use case**: Batch processing, historical analysis, replication
- **Implementation**: 
  - Read start and end offsets from index
  - Single pread for contiguous compressed data block
  - Sequential decompression of records
- **API**: `GET_RANGE(start_seq, end_seq) -> iterator<bytes>`

### File Descriptor Caching

Keep file descriptors open for recently accessed chunks to avoid open/close overhead.

- **Use case**: High-throughput random access patterns
- **Trade-off**: File descriptor usage vs syscall overhead (~2-4 μs per open/close)
- **Implementation**: LRU cache of open file descriptors with eviction callback to close files
