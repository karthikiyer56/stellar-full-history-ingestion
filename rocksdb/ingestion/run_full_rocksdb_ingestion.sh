#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# RocksDB Full Ingestion Script
# =============================================================================
# Optimized for: 1 month of Stellar transaction data per RocksDB store
# Expected data: ~150M transactions/month, ~800 bytes each compressed = ~120 GB
# =============================================================================

# ===================================================================================
# Default values with rationale
# ===================================================================================

START_TIME=""
END_TIME=""
DB1_PATH=""
DB2_PATH=""
DB3_PATH=""
ROCKSDB_LCM_STORE=""

# 10K ledgers ≈ 400K tx ≈ 320 MB per batch (should roughly match write buffer)
BATCH_SIZE=10000

# App-level zstd: ~60-70% compression, better control than RocksDB native
ENABLE_APP_COMPRESSION=true
ENABLE_ROCKSDB_COMPRESSION=false

# 512 MB per buffer, sized to match batch size
WRITE_BUFFER_SIZE_MB=512
# 4 buffers × 512 MB = 2 GB total memtable memory
MAX_WRITE_BUFFER_NUMBER=4

# 999 = effectively disable L0 compaction triggers during ingestion
# Random hash keys = compaction during ingestion is wasteful
L0_COMPACTION_TRIGGER=999
L0_SLOWDOWN_TRIGGER=999
L0_STOP_TRIGGER=999

# 8 threads for final compaction
MAX_BACKGROUND_JOBS=8

# 1 GB files: 120 GB/month ÷ 1 GB = ~120 files/month, ~14K files for 10 years
TARGET_FILE_SIZE_MB=1024
# 2 GB L1 base: fewer levels = fewer bloom checks on read
MAX_BYTES_LEVEL_BASE_MB=2048

# 2000 handles for ~120 files/month (increase for serving 10 years)
MAX_OPEN_FILES=2000
# 512 MB cache: minimal for write-heavy ingestion (increase for read serving)
BLOCK_CACHE_SIZE_MB=512
# 10 bits/key ≈ 1% false positive, ~187 MB bloom memory per month
BLOOM_FILTER_BITS=10

# Disable auto-compaction: random keys = defer to final compaction
DISABLE_AUTO_COMPACTIONS=true
COMPACT_EVERY_N_BATCHES=0
COMPACT_AFTER_GB_WRITTEN=0

# WAL disabled: bulk ingestion, crash = restart anyway
DISABLE_WAL=true
# Disabled: we set custom parameters, PrepareForBulkLoad would override them
PREPARE_BULK_LOAD=false

# ===================================================================================
# Usage/help function
# ===================================================================================
usage() {
  cat <<EOF
Usage: $0 --start-time <RFC3339> --end-time <RFC3339> [options]

Optimized for monthly Stellar transaction ingestion into RocksDB.
Expected: ~150M tx/month × 800 bytes = ~120 GB per month.

REQUIRED:
  --start-time              Start time in RFC3339 (e.g., 2024-01-01T00:00:00+00:00)
  --end-time                End time in RFC3339 (e.g., 2024-01-31T23:59:59+00:00)

DATABASE PATHS (at least one required):
  --db1                     Path for DB1 (ledgerSeq -> compressed LCM)
  --db2                     Path for DB2 (txHash -> compressed TxData)
  --db3                     Path for DB3 (txHash -> ledgerSeq)
  --rocksdb-lcm-store       Read LCM from existing RocksDB store (disables DB1 writes)

BATCH SETTINGS:
  --ledger-batch-size       Ledgers per batch (default: $BATCH_SIZE)
                            10K ≈ 400K tx ≈ 320 MB, should match write buffer

COMPRESSION:
  --app-compression         zstd before RocksDB (default: $ENABLE_APP_COMPRESSION)
  --rocksdb-compression     RocksDB native compression (default: $ENABLE_ROCKSDB_COMPRESSION)

WRITE BUFFER (RAM during ingestion):
  --write-buffer-size-mb    Per-buffer size (default: $WRITE_BUFFER_SIZE_MB)
  --max-write-buffer-number Number of buffers (default: $MAX_WRITE_BUFFER_NUMBER)
                            Total memtable RAM = size × number

L0 MANAGEMENT (999 = disabled for random keys):
  --l0-compaction-trigger   Trigger compaction (default: $L0_COMPACTION_TRIGGER)
  --l0-slowdown-trigger     Slow writes (default: $L0_SLOWDOWN_TRIGGER)
  --l0-stop-trigger         Stop writes (default: $L0_STOP_TRIGGER)

FILE ORGANIZATION:
  --target-file-size-mb     SST file size (default: $TARGET_FILE_SIZE_MB)
                            120 GB ÷ 1024 MB = ~120 files/month
  --max-bytes-level-base-mb L1 max size (default: $MAX_BYTES_LEVEL_BASE_MB)
                            Larger = fewer levels = faster reads

BACKGROUND JOBS:
  --max-background-jobs     Compaction threads (default: $MAX_BACKGROUND_JOBS)

RESOURCES:
  --max-open-files          File handle limit (default: $MAX_OPEN_FILES)
  --block-cache-size-mb     Read cache in MB (default: $BLOCK_CACHE_SIZE_MB)
  --bloom-filter-bits       Bits per key, 0=off (default: $BLOOM_FILTER_BITS)
                            10 bits ≈ 1% false positive, ~187 MB/month

COMPACTION CONTROL:
  --disable-auto-compactions  Disable during ingestion (default: $DISABLE_AUTO_COMPACTIONS)
  --compact-every-n-batches   Periodic compaction (default: $COMPACT_EVERY_N_BATCHES, 0=final only)
  --compact-after-gb-written  Compact after N GB (default: $COMPACT_AFTER_GB_WRITTEN, 0=disabled)

BULK LOAD:
  --disable-wal             Disable write-ahead log (default: $DISABLE_WAL)
  --prepare-bulk-load       RocksDB bulk mode (default: $PREPARE_BULK_LOAD)
                            Set false when using custom tuning

OTHER:
  --help                    Show this help

EXAMPLES:

  # Basic monthly ingestion (reads LCM from existing store):
  $0 \\
    --start-time "2024-01-01T00:00:00+00:00" \\
    --end-time "2024-01-31T23:59:59+00:00" \\
    --rocksdb-lcm-store "/data/lcm_store" \\
    --db2 "/data/tx/2024_01"

  # Parallel ingestion (reduce memory per instance):
  $0 \\
    --start-time "2024-01-01T00:00:00+00:00" \\
    --end-time "2024-01-31T23:59:59+00:00" \\
    --rocksdb-lcm-store "/data/lcm_store" \\
    --db2 "/data/tx/2024_01" \\
    --write-buffer-size-mb 256 \\
    --max-write-buffer-number 4 \\
    --block-cache-size-mb 256

  # Full ingestion including LCM and hash->seq mapping:
  $0 \\
    --start-time "2024-01-01T00:00:00+00:00" \\
    --end-time "2024-01-31T23:59:59+00:00" \\
    --db1 "/data/lcm/2024_01" \\
    --db2 "/data/tx/2024_01" \\
    --db3 "/data/hash_seq/2024_01"
EOF
}

# ===================================================================================
# Parse CLI args
# ===================================================================================
while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-time) START_TIME="$2"; shift 2 ;;
    --end-time) END_TIME="$2"; shift 2 ;;
    --db1) DB1_PATH="$2"; shift 2 ;;
    --db2) DB2_PATH="$2"; shift 2 ;;
    --db3) DB3_PATH="$2"; shift 2 ;;
    --rocksdb-lcm-store) ROCKSDB_LCM_STORE="$2"; shift 2 ;;
    --ledger-batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --app-compression) ENABLE_APP_COMPRESSION="$2"; shift 2 ;;
    --rocksdb-compression) ENABLE_ROCKSDB_COMPRESSION="$2"; shift 2 ;;
    --write-buffer-size-mb) WRITE_BUFFER_SIZE_MB="$2"; shift 2 ;;
    --max-write-buffer-number) MAX_WRITE_BUFFER_NUMBER="$2"; shift 2 ;;
    --l0-compaction-trigger) L0_COMPACTION_TRIGGER="$2"; shift 2 ;;
    --l0-slowdown-trigger) L0_SLOWDOWN_TRIGGER="$2"; shift 2 ;;
    --l0-stop-trigger) L0_STOP_TRIGGER="$2"; shift 2 ;;
    --max-background-jobs) MAX_BACKGROUND_JOBS="$2"; shift 2 ;;
    --target-file-size-mb) TARGET_FILE_SIZE_MB="$2"; shift 2 ;;
    --max-bytes-level-base-mb) MAX_BYTES_LEVEL_BASE_MB="$2"; shift 2 ;;
    --max-open-files) MAX_OPEN_FILES="$2"; shift 2 ;;
    --block-cache-size-mb) BLOCK_CACHE_SIZE_MB="$2"; shift 2 ;;
    --bloom-filter-bits) BLOOM_FILTER_BITS="$2"; shift 2 ;;
    --disable-auto-compactions) DISABLE_AUTO_COMPACTIONS="$2"; shift 2 ;;
    --compact-every-n-batches) COMPACT_EVERY_N_BATCHES="$2"; shift 2 ;;
    --compact-after-gb-written) COMPACT_AFTER_GB_WRITTEN="$2"; shift 2 ;;
    --disable-wal) DISABLE_WAL="$2"; shift 2 ;;
    --prepare-bulk-load) PREPARE_BULK_LOAD="$2"; shift 2 ;;
    --help) usage; exit 0 ;;
    *)
      echo "❌ Unknown argument: $1" >&2
      echo "Use --help for usage information" >&2
      exit 1
      ;;
  esac
done

# ===================================================================================
# Validate arguments
# ===================================================================================
if [[ -z "$START_TIME" || -z "$END_TIME" ]]; then
  echo "❌ Both --start-time and --end-time are required" >&2
  usage
  exit 1
fi

if [[ -z "$DB1_PATH" && -z "$DB2_PATH" && -z "$DB3_PATH" ]]; then
  echo "❌ At least one database (--db1, --db2, or --db3) must be specified" >&2
  usage
  exit 1
fi

if [[ -n "$ROCKSDB_LCM_STORE" && -n "$DB1_PATH" ]]; then
  echo "⚠️  --rocksdb-lcm-store specified, --db1 will be ignored"
  DB1_PATH=""
fi

# ===================================================================================
# Calculate memory usage
# ===================================================================================
MEMTABLE_TOTAL=$((WRITE_BUFFER_SIZE_MB * MAX_WRITE_BUFFER_NUMBER))
ESTIMATED_RAM=$((MEMTABLE_TOTAL + BLOCK_CACHE_SIZE_MB + 200))  # +200 for bloom/overhead

# ===================================================================================
# Print configuration
# ===================================================================================
echo ""
echo "=============================================================================="
echo "🚀 RocksDB Ingestion - Monthly Stellar Transaction Data"
echo "=============================================================================="
echo ""
echo "TIME RANGE:"
echo "  Start: $START_TIME"
echo "  End:   $END_TIME"
echo ""
echo "DATABASES:"
echo "  DB1 (LCM):       ${DB1_PATH:-'(disabled)'}"
echo "  DB2 (TxData):    ${DB2_PATH:-'(disabled)'}"
echo "  DB3 (Hash→Seq):  ${DB3_PATH:-'(disabled)'}"
echo "  LCM Source:      ${ROCKSDB_LCM_STORE:-'GCS (remote)'}"
echo ""
echo "BATCH & COMPRESSION:"
echo "  Batch Size:           $BATCH_SIZE ledgers"
echo "  App Compression:      $ENABLE_APP_COMPRESSION"
echo "  RocksDB Compression:  $ENABLE_ROCKSDB_COMPRESSION"
echo ""
echo "MEMORY (estimated ~${ESTIMATED_RAM} MB per instance):"
echo "  Write Buffers:        ${WRITE_BUFFER_SIZE_MB} MB × ${MAX_WRITE_BUFFER_NUMBER} = ${MEMTABLE_TOTAL} MB"
echo "  Block Cache:          ${BLOCK_CACHE_SIZE_MB} MB"
echo "  Bloom Filter:         ~187 MB per month (at ${BLOOM_FILTER_BITS} bits/key)"
echo ""
echo "FILE ORGANIZATION:"
echo "  Target File Size:     ${TARGET_FILE_SIZE_MB} MB → ~$((120 * 1024 / TARGET_FILE_SIZE_MB)) files/month"
echo "  L1 Base Size:         ${MAX_BYTES_LEVEL_BASE_MB} MB"
echo "  Max Open Files:       ${MAX_OPEN_FILES}"
echo ""
echo "COMPACTION:"
echo "  Auto Compaction:      $([ "$DISABLE_AUTO_COMPACTIONS" = "true" ] && echo "DISABLED" || echo "enabled")"
echo "  L0 Triggers:          ${L0_COMPACTION_TRIGGER}/${L0_SLOWDOWN_TRIGGER}/${L0_STOP_TRIGGER}"
echo "  Background Jobs:      ${MAX_BACKGROUND_JOBS}"
echo ""
echo "BULK LOAD:"
echo "  WAL:                  $([ "$DISABLE_WAL" = "true" ] && echo "DISABLED" || echo "enabled")"
echo "  PrepareForBulkLoad:   $PREPARE_BULK_LOAD"
echo "=============================================================================="
echo ""

# ===================================================================================
# Get ledger range from time range
# ===================================================================================
OUTFILE="/tmp/.ledger_range_rocksdb_$$.json"
trap "rm -f $OUTFILE" EXIT

echo "🔍 Determining ledger range..."
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  -v /tmp:/tmp stellar/stellar-etl:latest \
  stellar-etl get_ledger_range_from_times \
  --start-time "$START_TIME" \
  --end-time "$END_TIME" \
  -o "$OUTFILE"

START_LEDGER=$(jq -r '.start' "$OUTFILE")
END_LEDGER=$(jq -r '.end' "$OUTFILE")
TOTAL_LEDGERS=$((END_LEDGER - START_LEDGER + 1))
TOTAL_BATCHES=$(((TOTAL_LEDGERS + BATCH_SIZE - 1) / BATCH_SIZE))

echo ""
echo "📊 Ledger Range:"
echo "   Start:  $START_LEDGER"
echo "   End:    $END_LEDGER"
echo "   Total:  $TOTAL_LEDGERS ledgers"
echo "   Batches: $TOTAL_BATCHES"
echo ""

# ===================================================================================
# Locate or build binary
# ===================================================================================
BINARY="${ROCKSDB_INGESTION_BINARY:-$HOME/bin/full_rocksdb_ingestion}"

if [[ ! -f "$BINARY" ]]; then
  echo "❌ Binary not found at $BINARY"
  echo "   Set ROCKSDB_INGESTION_BINARY or build with: make build-rocksdb"
  exit 1
fi

# ===================================================================================
# Set library paths
# ===================================================================================
case "$(uname -s)" in
  Darwin)
    export DYLD_LIBRARY_PATH="${ROCKSDB_HOME:-$HOME/local/rocksdb}/lib:${DYLD_LIBRARY_PATH:-}"
    ;;
  Linux)
    export LD_LIBRARY_PATH="/usr/local/lib:${LD_LIBRARY_PATH:-}"
    ;;
esac

# ===================================================================================
# Build command
# ===================================================================================
CMD=(
  "$BINARY"
  --start-ledger "$START_LEDGER"
  --end-ledger "$END_LEDGER"
  --ledger-batch-size "$BATCH_SIZE"
  --app-compression="$ENABLE_APP_COMPRESSION"
  --rocksdb-compression="$ENABLE_ROCKSDB_COMPRESSION"
  --write-buffer-size-mb "$WRITE_BUFFER_SIZE_MB"
  --max-write-buffer-number "$MAX_WRITE_BUFFER_NUMBER"
  --l0-compaction-trigger "$L0_COMPACTION_TRIGGER"
  --l0-slowdown-trigger "$L0_SLOWDOWN_TRIGGER"
  --l0-stop-trigger "$L0_STOP_TRIGGER"
  --max-background-jobs "$MAX_BACKGROUND_JOBS"
  --target-file-size-mb "$TARGET_FILE_SIZE_MB"
  --max-bytes-level-base-mb "$MAX_BYTES_LEVEL_BASE_MB"
  --max-open-files "$MAX_OPEN_FILES"
  --block-cache-size-mb "$BLOCK_CACHE_SIZE_MB"
  --bloom-filter-bits "$BLOOM_FILTER_BITS"
  --disable-auto-compactions="$DISABLE_AUTO_COMPACTIONS"
  --compact-every-n-batches "$COMPACT_EVERY_N_BATCHES"
  --compact-after-gb-written "$COMPACT_AFTER_GB_WRITTEN"
  --disable-wal="$DISABLE_WAL"
  --prepare-bulk-load="$PREPARE_BULK_LOAD"
)

[[ -n "$DB1_PATH" ]] && CMD+=(--db1 "$DB1_PATH")
[[ -n "$DB2_PATH" ]] && CMD+=(--db2 "$DB2_PATH")
[[ -n "$DB3_PATH" ]] && CMD+=(--db3 "$DB3_PATH")
[[ -n "$ROCKSDB_LCM_STORE" ]] && CMD+=(--rocksdb-lcm-store "$ROCKSDB_LCM_STORE")

# ===================================================================================
# Run ingestion
# ===================================================================================
echo "🚀 Starting ingestion..."
echo ""
echo "Command: ${CMD[*]}"
echo ""

"${CMD[@]}"
EXIT_CODE=$?

echo ""
if [[ $EXIT_CODE -eq 0 ]]; then
  echo "✅ Ingestion completed successfully!"
else
  echo "❌ Ingestion failed with exit code: $EXIT_CODE"
fi

exit $EXIT_CODE