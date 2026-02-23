---
name: helpers-ref
description: Complete function reference for helpers/helpers.go and helpers/lfs/ — check here before writing any formatting, encoding, or filesystem utility code
---

# Helpers Reference

## NEVER Rules

| Rule |
|------|
| NEVER duplicate helper functions — check `helpers/` first; it is the single source of truth |
| NEVER inline formatting or utility code in app code — add to `helpers/` package instead |

## helpers/helpers.go

### Formatting

| Function | Signature | Output Example |
|----------|-----------|---------------|
| `FormatBytes` | `(int64) string` | `"1.23 GB"` |
| `FormatBytesWithPrecision` | `(int64, int) string` | `"1.234 GB"` |
| `FormatDuration` | `(time.Duration) string` | `"1h2m3s"` |
| `FormatFloat` | `(float64, int) string` | `"3.14"` |
| `FormatNumber` | `(int64) string` | `"1,234,567"` |
| `FormatPercent` | `(float64, int) string` | `"12.34%"` |
| `FormatRate` | `(int64, time.Duration) string` | `"1.23K/s"` |
| `WrapText` | `(string, int) string` | wrapped text |

### Encoding

| Function | Signature | Description |
|----------|-----------|-------------|
| `Uint32ToBytes` | `(uint32) []byte` | big-endian encode |
| `BytesToUint32` | `([]byte) uint32` | big-endian decode |
| `Uint64ToBytes` | `(uint64) []byte` | big-endian encode |
| `BytesToUint64` | `([]byte) uint64` | big-endian decode |
| `HexStringToBytes` | `(string) ([]byte, error)` | hex decode |
| `BytesToHexString` | `([]byte) string` | hex encode |

### Calculations

| Function | Signature | Description |
|----------|-----------|-------------|
| `BytesToGB` | `(int64) float64` | bytes to GB float |
| `CalculateCompressionRatio` | `(int64, int64) float64` | compression ratio |
| `CalculateOverhead` | `(int64, int64) float64` | overhead fraction |
| `Min` | `(int64, int64) int64` | minimum int64 |
| `Max` | `(int64, int64) int64` | maximum int64 |
| `MinUint32` | `(uint32, uint32) uint32` | minimum uint32 |
| `MaxUint32` | `(uint32, uint32) uint32` | maximum uint32 |

### Filesystem

| Function | Signature | Description |
|----------|-----------|-------------|
| `GetDirSize` | `(string) (int64, error)` | total size of directory |
| `GetFileCount` | `(string) (int, error)` | number of files in dir |
| `EnsureDir` | `(string) error` | create directory if not exists |
| `FileExists` | `(string) bool` | check if file/dir exists |
| `IsDir` | `(string) bool` | check if path is a directory |

## helpers/lfs/

LFS (Local File System) path helpers and ledger iteration.

| Function | Signature | Description |
|----------|-----------|-------------|
| `LedgerToChunkID` | `(uint32) uint32` | convert ledger sequence to chunk ID |
| `ChunkFirstLedger` | `(uint32) uint32` | first ledger in a chunk |
| `ChunkLastLedger` | `(uint32) uint32` | last ledger in a chunk |
| `LedgerIterator` | struct | iterate over ledgers in an LFS store |
| `DiscoverLedgerRange` | `(string) (uint32, uint32, error)` | find available ledger range in directory |
| `ValidateLfsStore` | `(string) error` | validate LFS store path and structure |
