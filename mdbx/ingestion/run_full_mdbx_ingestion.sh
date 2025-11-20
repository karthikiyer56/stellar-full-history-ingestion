#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Default values
# -----------------------------
START_TIME="2025-10-01T00:00:00+00:00"
END_TIME="2025-10-02T00:00:00+00:00"
DB2_PATH=""
DB3_PATH=""
BATCH_SIZE=200
ENABLE_APP_COMPRESSION=true
ROCKSDB_LCM_STORE=""
DB2_PAGESIZE=16384
DB3_PAGESIZE=4096
SYNC_EVERY_N_BATCHES=0
DB_SYNC_TYPE="default"

# -----------------------------
# Usage/help function
# -----------------------------
usage() {
  cat <<EOF
Usage: $0 --start-time <RFC3339> --end-time <RFC3339> [options]

Mandatory:
  --start-time              Start time in RFC3339 format (e.g. 2025-10-01T00:00:00+00:00)
  --end-time                End time in RFC3339 format (e.g. 2025-10-02T00:00:00+00:00)

Optional:
  --db2                     Path for DB2 (txHash -> compressed TxData)
  --db3                     Path for DB3 (txHash -> ledgerSeq)
  --ledger-batch-size       Ledger batch size for commit (default: 5000)
  --app-compression         true/false (default: true)
  --rocksdb-lcm-store       Path to RocksDB store containing compressed LedgerCloseMeta
  --db2-pagesize            Page size for DB2 in bytes (default: 16384)
  --db3-pagesize            Page size for DB3 in bytes (default: 4096)
  --sync-every-n-batches    Sync to disk every N batches (required if db-sync is safe/utterly, default: 0)
  --db-sync                 Sync type: default|safe|utterly (default: default)
  --help                    Show this help message and exit

Example with all settings (default sync mode):
  $0 \\
    --start-time "2024-10-01T00:00:00+00:00" \\
    --end-time "2024-12-31T23:59:59+00:00" \\
    --db2 "/mnt/data/mdbx/txhash_to_txdata.mdbx" \\
    --db3 "/mnt/data/mdbx/txhash_to_ledgerseq.mdbx" \\
    --rocksdb-lcm-store "/mnt/data/rocksdb/ledger_close_meta" \\
    --ledger-batch-size 5000 \\
    --db2-pagesize 16384 \\
    --db3-pagesize 4096 \\
    --app-compression true \\
    --db-sync default

Example with SafeNoSync mode:
  $0 \\
    --start-time "2024-10-01T00:00:00+00:00" \\
    --end-time "2024-12-31T23:59:59+00:00" \\
    --db2 "/mnt/data/mdbx/db2.mdbx" \\
    --rocksdb-lcm-store "/mnt/data/rocksdb/lcm" \\
    --db-sync safe \\
    --sync-every-n-batches 20
EOF
}

# -----------------------------
# Parse optional CLI args
# -----------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-time) START_TIME="$2"; shift 2 ;;
    --end-time) END_TIME="$2"; shift 2 ;;
    --db2) DB2_PATH="$2"; shift 2 ;;
    --db3) DB3_PATH="$2"; shift 2 ;;
    --ledger-batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --app-compression) ENABLE_APP_COMPRESSION="$2"; shift 2 ;;
    --rocksdb-lcm-store) ROCKSDB_LCM_STORE="$2"; shift 2 ;;
    --db2-pagesize) DB2_PAGESIZE="$2"; shift 2 ;;
    --db3-pagesize) DB3_PAGESIZE="$2"; shift 2 ;;
    --sync-every-n-batches) SYNC_EVERY_N_BATCHES="$2"; shift 2 ;;
    --db-sync) DB_SYNC_TYPE="$2"; shift 2 ;;
    --help) usage; exit 0 ;;
    *)
      echo "❌ Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

# -----------------------------
# Check mandatory args
# -----------------------------
if [[ -z "$START_TIME" || -z "$END_TIME" ]]; then
  echo "❌ Both --start-time and --end-time are required" >&2
  usage
  exit 1
fi

# At least one DB must be specified
if [[ -z "$DB2_PATH" && -z "$DB3_PATH" ]]; then
  echo "❌ At least one database (--db2 or --db3) must be specified" >&2
  usage
  exit 1
fi

# Validate db-sync value
if [[ "$DB_SYNC_TYPE" != "default" && "$DB_SYNC_TYPE" != "safe" && "$DB_SYNC_TYPE" != "utterly" ]]; then
  echo "❌ Invalid --db-sync value: $DB_SYNC_TYPE (must be: default|safe|utterly)" >&2
  exit 1
fi

# Validate sync-every-n-batches is set when using safe/utterly
if [[ ("$DB_SYNC_TYPE" == "safe" || "$DB_SYNC_TYPE" == "utterly") && "$SYNC_EVERY_N_BATCHES" -eq 0 ]]; then
  echo "❌ --sync-every-n-batches must be specified when using --db-sync safe or utterly" >&2
  exit 1
fi

# -----------------------------
# Temporary output file
# -----------------------------
OUTFILE="/tmp/.ledger_range.json"

echo "----------------------------------"
echo "🔧 Parsed arguments:"
echo "  START_TIME=$START_TIME"
echo "  END_TIME=$END_TIME"
echo "  DB2_PATH=$DB2_PATH"
echo "  DB3_PATH=$DB3_PATH"
echo "  BATCH_SIZE=$BATCH_SIZE"
echo "  ENABLE_APP_COMPRESSION=$ENABLE_APP_COMPRESSION"
echo "  ROCKSDB_LCM_STORE=$ROCKSDB_LCM_STORE"
echo "  DB2_PAGESIZE=$DB2_PAGESIZE"
echo "  DB3_PAGESIZE=$DB3_PAGESIZE"
echo "  SYNC_EVERY_N_BATCHES=$SYNC_EVERY_N_BATCHES"
echo "  DB_SYNC_TYPE=$DB_SYNC_TYPE"
echo "----------------------------------"

cleanup() {
  rm -f "$OUTFILE"
}
trap cleanup EXIT

# -----------------------------
# Run stellar-etl to get range
# -----------------------------
docker run --rm --platform linux/amd64 \
  --user "$(id -u):$(id -g)" \
  -v /tmp:/tmp stellar/stellar-etl:latest \
  stellar-etl get_ledger_range_from_times \
  --start-time "$START_TIME" \
  --end-time "$END_TIME" \
  -o "$OUTFILE"

START_LEDGER=$(jq -r '.start' "$OUTFILE")
END_LEDGER=$(jq -r '.end' "$OUTFILE")

echo "✅ Ledger range determined: $START_LEDGER → $END_LEDGER"

# -----------------------------
# Build MDBX binary if needed
# -----------------------------
MDBX_BINARY="$HOME/bin/full_mdbx_ingestion"

if [[ ! -f "$MDBX_BINARY" ]]; then
  echo "🔨 Building MDBX binary..."
  make build-mdbx
fi

# -----------------------------
# Set library path for Mac
# -----------------------------
export MDBX_HOME="$HOME/local/libmdbx"
export DYLD_LIBRARY_PATH="$MDBX_HOME/lib:${DYLD_LIBRARY_PATH:-}"

echo "DYLD_LIBRARY_PATH is: $DYLD_LIBRARY_PATH"

# -----------------------------
# Run MDBX ingestion
# -----------------------------
echo ""
echo "🚀 Starting MDBX ingestion..."
echo ""

set -x

"$MDBX_BINARY" \
  --start-ledger "$START_LEDGER" \
  --end-ledger "$END_LEDGER" \
  --ledger-batch-size "$BATCH_SIZE" \
  ${DB2_PATH:+--db2 "$DB2_PATH"} \
  ${DB3_PATH:+--db3 "$DB3_PATH"} \
  ${ROCKSDB_LCM_STORE:+--rocksdb-lcm-store "$ROCKSDB_LCM_STORE"} \
  --db2-pagesize "$DB2_PAGESIZE" \
  --db3-pagesize "$DB3_PAGESIZE" \
  --db-sync "$DB_SYNC_TYPE" \
  ${SYNC_EVERY_N_BATCHES:+--sync-every-n-batches "$SYNC_EVERY_N_BATCHES"} \
  --app-compression="$ENABLE_APP_COMPRESSION"

set +x

echo ""
echo "✅ MDBX ingestion complete!"