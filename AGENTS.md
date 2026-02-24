# AGENTS.md - stellar-full-history-ingestion

**Module**: `github.com/karthikiyer56/stellar-full-history-ingestion` | **Go**: 1.24.3

## Critical Rules

### NEVER Do

| Rule | Reason |
|------|--------|
| `DisableWAL(true)` | WAL required for crash recovery; checkpoint system depends on it |
| Import `github.com/stellar/go` | ARCHIVED - use `github.com/stellar/go-stellar-sdk` |
| Duplicate helper functions | Check `helpers/` first; single source of truth |
| Inline formatting/utility code | Add to `helpers/` package instead of app code |
| Leave binaries in repo | Delete after `go build` verification |
| Empty catch blocks | Always handle errors explicitly |

## Log Format

```
[2006-01-02T15:04:05Z] [LEVEL] message
```

## Error Handling

| Type | Action |
|------|--------|
| Parse/data errors | ABORT - data integrity issue |
| Storage errors | ABORT - RocksDB/disk failure |
| Config errors | ABORT - invalid configuration |
| Verification mismatch | LOG and CONTINUE |
| Missing optional files | LOG and CONTINUE |

## Skills Index

Load these on-demand skills when the task matches the trigger keywords:

| Skill | Description | Load When |
|-------|-------------|-----------|
| `architecture` | Package layout, two-pipeline design (backfill/streaming), two separate RocksDB instances per range (ledger store + txhash store), meta store key hierarchy, range/chunk math, query routing, crash recovery, design doc conventions | Architecture questions, store design, state machines, crash scenarios, query routing, design docs, chunk/range math |
| `go-build` | CGO setup, Makefile targets, build commands, RocksDB dependencies | Building, compiling, CGO errors, `make` targets |
| `testing` | NEVER run `go test` directly; CGO flags for tests; Makefile test targets | Writing tests, running tests, linker errors |
| `metrics` | OpMetrics struct, progress logging, final summary format, Logger interface | Instrumenting I/O, adding metrics, progress logs |
| `helpers-ref` | `helpers/helpers.go` and `helpers/lfs/` function reference | Formatting output, LFS iteration, byte encoding |