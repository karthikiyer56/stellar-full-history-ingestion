#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Default values
# -----------------------------
START_TIME="2025-02-01T00:00:00+00:00"
END_TIME="2025-05-01T00:00:00+00:00"
DB1_PATH=""
DB2_PATH=""
DB3_PATH=""
BATCH_SIZE=2000
ENABLE_APP_COMPRESSION=true
ENABLE_ROCKSDB_COMPRESSION=false


# -----------------------------
# Usage/help function
# -----------------------------
usage() {
  cat <<EOF
Usage: $0 --start-time <RFC3339> --end-time <RFC3339> [options]

Mandatory:
  --start-time         Start time in RFC3339 format (e.g. 2025-02-01T00:00:00+00:00)
  --end-time           End time in RFC3339 format (e.g. 2025-05-01T00:00:00+00:00)

Optional:
  --db1                Path for DataStore 1 (ledgerSeq -> compressed LCM)
  --db2                Path for DataStore 2 (txHash -> compressed TxData)
  --db3                Path for DataStore 3 (txHash -> ledgerSeq)
  --ledger-batch-size  Ledger batch size for commit (default: 2000)
  --app-compression    true/false (default: true)
  --rocksdb-compression true/false (default: false)
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
    --db1) DB1_PATH="$2"; shift 2 ;;
    --db2) DB2_PATH="$2"; shift 2 ;;
    --db3) DB3_PATH="$2"; shift 2 ;;
    --ledger-batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --app-compression) ENABLE_APP_COMPRESSION="$2"; shift 2 ;;
    --rocksdb-compression) ENABLE_ROCKSDB_COMPRESSION="$2"; shift 2 ;;
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

# -----------------------------
# Temporary output file
# -----------------------------
OUTFILE="/tmp/.ledger_range.json"

echo "----------------------------------"
echo "🔧 Parsed arguments:"
echo "  START_TIME=$START_TIME"
echo "  END_TIME=$END_TIME"
echo "  DB1_PATH=$DB1_PATH"
echo "  DB2_PATH=$DB2_PATH"
echo "  DB3_PATH=$DB3_PATH"
echo "  BATCH_SIZE=$BATCH_SIZE"
echo "  ENABLE_APP_COMPRESSION=$ENABLE_APP_COMPRESSION"
echo "  ENABLE_ROCKSDB_COMPRESSION=$ENABLE_ROCKSDB_COMPRESSION"
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
# Run your Go binary
# -----------------------------
# Make sure this full_ingestion binary exists in your path. if not, modify the code to run it where you want
full_ingestion \
  --start-ledger "$START_LEDGER" \
  --end-ledger "$END_LEDGER" \
  --ledger-batch-size "$BATCH_SIZE" \
  --db1 "$DB1_PATH" \
  --db2 "$DB2_PATH" \
  --db3 "$DB3_PATH" \
  --app-compression="$ENABLE_APP_COMPRESSION" \
  --rocksdb-compression="$ENABLE_ROCKSDB_COMPRESSION"
