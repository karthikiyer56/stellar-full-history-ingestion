#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Default values
# -----------------------------
START_TIME="2025-10-01T00:00:00+00:00"
END_TIME="2025-10-02T00:00:00+00:00"
DB2_PATH=""
DB3_PATH=""
BATCH_SIZE=2000
ENABLE_APP_COMPRESSION=true
ROCKSDB_LCM_STORE=""
DB_PAGESIZE=8192
# -----------------------------
# Usage/help function
# -----------------------------
usage() {
  cat <<EOF
Usage: $0 --start-time <RFC3339> --end-time <RFC3339> [options]

Mandatory:
  --start-time         Start time in RFC3339 format (e.g. 2025-10-01T00:00:00+00:00)
  --end-time           End time in RFC3339 format (e.g. 2025-10-02T00:00:00+00:00)

Optional:
  --db2                Path for DB2 (txHash -> compressed TxData)
  --db3                Path for DB3 (txHash -> ledgerSeq)
  --ledger-batch-size  Ledger batch size for commit (default: 2000)
  --app-compression    true/false (default: true)
  --rocksdb-lcm-store  Path to RocksDB store containing compressed LedgerCloseMeta
  --db-pagesize        Pagesize for new DB.
  --help               Show this help message and exit
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
    --db-pagesize) DB_PAGESIZE="$2"; shift 2 ;;
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
echo "  DB_PAGESIZE=$DB_PAGESIZE"
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
  ${DB_PAGESIZE:+--db-pagesize "$DB_PAGESIZE"} \
  --app-compression="$ENABLE_APP_COMPRESSION"

set +x

echo ""
echo "✅ MDBX ingestion complete!"