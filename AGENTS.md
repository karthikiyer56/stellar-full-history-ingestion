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
| `codebase-overview` | Package layout, entry points, binaries, CLI flags, architecture phases | New to codebase, package locations, binary names |
| `go-build` | CGO setup, Makefile targets, build commands, RocksDB/MDBX dependencies | Building, compiling, CGO errors, `make` targets |
| `testing` | NEVER run `go test` directly; CGO flags for tests; Makefile test targets | Writing tests, running tests, linker errors |
| `metrics` | OpMetrics struct, progress logging, final summary format, Logger interface | Instrumenting I/O, adding metrics, progress logs |
| `helpers-ref` | `helpers/helpers.go` and `helpers/lfs/` function reference | Formatting output, LFS iteration, byte encoding |
| `crash-recovery` | 6 crash scenarios, resume rule (last_committed_ledger+1), WriteBatch pattern | Crash handling, checkpoint logic, resume from failure |
| `range-chunk-math` | Transition triggers, chunk math formulas, LedgerToChunkID, boundary constants | Ledger range ops, chunk IDs, transition conditions |
| `query-routing` | QueryRouter struct, store lifecycle, false positive handling, bloom filters | Query routing, multi-store reads, BloomFilter usage |
| `meta-store-design` | MetaStore key hierarchy, state machine enums, checkpoint schema | MetaStore design, state transitions, checkpoint keys |
| `design-doc-conventions` | Mermaid-first diagrams, doc structure, agent workflow sections | Writing design docs, architecture diagrams, ADRs |