#!/usr/bin/env bash
# =============================================================================
# Stellar RocksDB Ingestion Runner Script
# =============================================================================
#
# This script wraps the stellar_rocksdb_ingestion binary and handles:
# - Time-to-ledger range conversion using stellar-etl
# - Year-based date range calculation
# - Command validation
# - Configuration logging
#
# USAGE:
# ======
# Using --year (shorthand for full year):
#   ./run_ingestion.sh \
#     --config /path/to/config.toml \
#     --year 2022 \
#     --ledger-batch-size 2000 \
#     --enable-tx-hash-to-ledger-seq
#
# Using explicit time range:
#   ./run_ingestion.sh \
#     --config /path/to/config.toml \
#     --start-time "2022-01-01T00:00:00+00:00" \
#     --end-time "2022-12-31T23:59:59+00:00" \
#     --ledger-batch-size 2000 \
#     --enable-tx-hash-to-ledger-seq \
#     --enable-tx-hash-to-tx-data
#
# =============================================================================

set -euo pipefail

# =============================================================================
# Default Values
# =============================================================================
CONFIG_PATH=""
START_TIME=""
END_TIME=""
YEAR=""
LEDGER_BATCH_SIZE=""
ENABLE_LEDGER_SEQ_TO_LCM=false
ENABLE_TX_HASH_TO_LEDGER_SEQ=false

# =============================================================================
# Usage Function
# =============================================================================
usage() {
    cat <<EOF
================================================================================
                     Stellar RocksDB Ingestion Runner
================================================================================

USAGE:
    $0 [OPTIONS]

REQUIRED OPTIONS:
    --config <path>           Path to TOML configuration file
    --ledger-batch-size <n>   Number of ledgers per batch (e.g., 2000)

TIME RANGE (one of the following is required):
    --year <YYYY>             Year to ingest (e.g., 2022)
                              Automatically sets:
                              - start: YYYY-01-01T00:00:00+00:00
                              - end:   YYYY-12-31T23:59:59+00:00

    --start-time <timestamp>  Start time in RFC3339 format
    --end-time <timestamp>    End time in RFC3339 format
                              Example: "2022-01-01T00:00:00+00:00"

STORE FLAGS (at least one required):
    --enable-ledger-seq-to-lcm      Enable ledger_seq_to_lcm store
    --enable-tx-hash-to-ledger-seq  Enable tx_hash_to_ledger_seq store

OTHER OPTIONS:
    --help                    Show this help message

EXAMPLES:

    # Ingest full year 2022 with tx_hash_to_ledger_seq store:
    $0 \\
        --config ./config.toml \\
        --year 2022 \\
        --ledger-batch-size 2000 \\
        --enable-tx-hash-to-ledger-seq

    # Full ingestion with both stores:
    $0 \\
        --config ./config.toml \\
        --year 2022 \\
        --ledger-batch-size 2000 \\
        --enable-ledger-seq-to-lcm \\
        --enable-tx-hash-to-ledger-seq

================================================================================
EOF
}

# =============================================================================
# Parse Command Line Arguments
# =============================================================================
while [[ $# -gt 0 ]]; do
    case "$1" in
        --config)
            CONFIG_PATH="$2"
            shift 2
            ;;
        --start-time)
            START_TIME="$2"
            shift 2
            ;;
        --end-time)
            END_TIME="$2"
            shift 2
            ;;
        --year)
            YEAR="$2"
            shift 2
            ;;
        --ledger-batch-size)
            LEDGER_BATCH_SIZE="$2"
            shift 2
            ;;
        --enable-ledger-seq-to-lcm)
            ENABLE_LEDGER_SEQ_TO_LCM=true
            shift
            ;;
        --enable-tx-hash-to-ledger-seq)
            ENABLE_TX_HASH_TO_LEDGER_SEQ=true
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "ERROR: Unknown argument: $1" >&2
            echo "Use --help for usage information." >&2
            exit 1
            ;;
    esac
done

# =============================================================================
# Validate Required Arguments
# =============================================================================
echo ""
echo "================================================================================"
echo "                     Stellar RocksDB Ingestion Runner"
echo "================================================================================"
echo ""

# Check config path
if [[ -z "$CONFIG_PATH" ]]; then
    echo "ERROR: --config is required" >&2
    usage
    exit 1
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
    echo "ERROR: Config file not found: $CONFIG_PATH" >&2
    exit 1
fi

# Check batch size
if [[ -z "$LEDGER_BATCH_SIZE" ]]; then
    echo "ERROR: --ledger-batch-size is required" >&2
    usage
    exit 1
fi

if ! [[ "$LEDGER_BATCH_SIZE" =~ ^[0-9]+$ ]] || [[ "$LEDGER_BATCH_SIZE" -le 0 ]]; then
    echo "ERROR: --ledger-batch-size must be a positive integer" >&2
    exit 1
fi

# Check time range (either --year or --start-time/--end-time)
if [[ -n "$YEAR" ]]; then
    # Validate year format
    if ! [[ "$YEAR" =~ ^[0-9]{4}$ ]]; then
        echo "ERROR: --year must be a 4-digit year (e.g., 2022)" >&2
        exit 1
    fi

    # Calculate start and end times from year
    START_TIME="${YEAR}-01-01T00:00:00+00:00"
    END_TIME="${YEAR}-12-31T23:59:59+00:00"

    echo "Using year: $YEAR"
    echo "  Calculated start: $START_TIME"
    echo "  Calculated end:   $END_TIME"
    echo ""
elif [[ -n "$START_TIME" && -n "$END_TIME" ]]; then
    echo "Using explicit time range:"
    echo "  Start: $START_TIME"
    echo "  End:   $END_TIME"
    echo ""
else
    echo "ERROR: Either --year or both --start-time and --end-time are required" >&2
    usage
    exit 1
fi

# Check at least one store is enabled
if [[ "$ENABLE_LEDGER_SEQ_TO_LCM" != "true" && \
      "$ENABLE_TX_HASH_TO_LEDGER_SEQ" != "true" ]]; then
    echo "ERROR: At least one store must be enabled" >&2
    echo "Use one or more of:" >&2
    echo "  --enable-ledger-seq-to-lcm" >&2
    echo "  --enable-tx-hash-to-ledger-seq" >&2
    exit 1
fi

# =============================================================================
# Display Configuration
# =============================================================================
echo "Configuration:"
echo "  Config File:         $CONFIG_PATH"
echo "  Batch Size:          $LEDGER_BATCH_SIZE ledgers"
echo ""
echo "Enabled Stores:"
echo "  ledger_seq_to_lcm:     $ENABLE_LEDGER_SEQ_TO_LCM"
echo "  tx_hash_to_ledger_seq: $ENABLE_TX_HASH_TO_LEDGER_SEQ"
echo ""

# =============================================================================
# Convert Time Range to Ledger Range using stellar-etl
# =============================================================================
echo "--------------------------------------------------------------------------------"
echo "Converting time range to ledger range using stellar-etl..."
echo "--------------------------------------------------------------------------------"
echo ""

# Create temporary file for output
LEDGER_RANGE_FILE=$(mktemp /tmp/ledger_range_XXXXXX.json)
trap "rm -f $LEDGER_RANGE_FILE" EXIT

# Run stellar-etl to get ledger range
echo "Running stellar-etl get_ledger_range_from_times..."
echo ""

docker run --rm --platform linux/amd64 \
    --user "$(id -u):$(id -g)" \
    -v /tmp:/tmp \
    stellar/stellar-etl:latest \
    stellar-etl get_ledger_range_from_times \
    --start-time "$START_TIME" \
    --end-time "$END_TIME" \
    -o "$LEDGER_RANGE_FILE"

# Parse ledger range from output
if [[ ! -f "$LEDGER_RANGE_FILE" ]]; then
    echo "ERROR: Failed to get ledger range from stellar-etl" >&2
    exit 1
fi

START_LEDGER=$(jq -r '.start' "$LEDGER_RANGE_FILE")
END_LEDGER=$(jq -r '.end' "$LEDGER_RANGE_FILE")

if [[ -z "$START_LEDGER" || -z "$END_LEDGER" || "$START_LEDGER" == "null" || "$END_LEDGER" == "null" ]]; then
    echo "ERROR: Failed to parse ledger range from stellar-etl output" >&2
    echo "Output was:" >&2
    cat "$LEDGER_RANGE_FILE" >&2
    exit 1
fi

TOTAL_LEDGERS=$((END_LEDGER - START_LEDGER + 1))
TOTAL_BATCHES=$(( (TOTAL_LEDGERS + LEDGER_BATCH_SIZE - 1) / LEDGER_BATCH_SIZE ))

echo "Ledger Range:"
echo "  Start Ledger:        $START_LEDGER"
echo "  End Ledger:          $END_LEDGER"
echo "  Total Ledgers:       $TOTAL_LEDGERS"
echo "  Total Batches:       $TOTAL_BATCHES"
echo ""

# =============================================================================
# Locate Binary
# =============================================================================
BINARY="${STELLAR_ROCKSDB_INGESTION_BINARY:-rocksdb_ingestion_v2}"

## If not set via environment variable, look for it in common locations
#if [[ -z "$BINARY" ]]; then
#    # Check in same directory as script
#    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#    if [[ -f "$SCRIPT_DIR/stellar_rocksdb_ingestion" ]]; then
#        BINARY="$SCRIPT_DIR/stellar_rocksdb_ingestion"
#    # Check in $HOME/bin
#    elif [[ -f "$HOME/bin/stellar_rocksdb_ingestion" ]]; then
#        BINARY="$HOME/bin/stellar_rocksdb_ingestion"
#    # Check in current directory
#    elif [[ -f "./stellar_rocksdb_ingestion" ]]; then
#        BINARY="./stellar_rocksdb_ingestion"
#    fi
#fi
#
#if [[ -z "$BINARY" || ! -f "$BINARY" ]]; then
#    echo "ERROR: Cannot find stellar_rocksdb_ingestion binary" >&2
#    echo "" >&2
#    echo "Please either:" >&2
#    echo "  1. Set STELLAR_ROCKSDB_INGESTION_BINARY environment variable" >&2
#    echo "  2. Place the binary in the same directory as this script" >&2
#    echo "  3. Place the binary in \$HOME/bin/" >&2
#    echo "" >&2
#    echo "Build with: go build -o stellar_rocksdb_ingestion ." >&2
#    exit 1
#fi

echo "Binary: $BINARY"
echo ""

# =============================================================================
# Set Library Paths (for RocksDB)
# =============================================================================
case "$(uname -s)" in
    Darwin)
        export DYLD_LIBRARY_PATH="/Users/karthik/local/libmdbx/lib:${DYLD_LIBRARY_PATH:-}"
        echo "DYLD_LIBRARY_PATH ==> $DYLD_LIBRARY_PATH"
        ;;
    Linux)
        export LD_LIBRARY_PATH="/usr/local/lib:${LD_LIBRARY_PATH:-}"
        echo "LD_LIBRARY_PATH ==> $LD_LIBRARY_PATH"
        ;;
esac

# =============================================================================
# Build Command
# =============================================================================
CMD=(
    "$BINARY"
    --config "$CONFIG_PATH"
    --ledger-batch-size "$LEDGER_BATCH_SIZE"
    --start-ledger "$START_LEDGER"
    --end-ledger "$END_LEDGER"
    --start-time "$START_TIME"
    --end-time "$END_TIME"
)

# Add store enable flags
if [[ "$ENABLE_LEDGER_SEQ_TO_LCM" == "true" ]]; then
    CMD+=(--enable-ledger-seq-to-lcm)
fi
if [[ "$ENABLE_TX_HASH_TO_LEDGER_SEQ" == "true" ]]; then
    CMD+=(--enable-tx-hash-to-ledger-seq)
fi

# =============================================================================
# Run Ingestion
# =============================================================================
echo "================================================================================"
echo "                          STARTING INGESTION"
echo "================================================================================"
echo ""
echo "Command:"
echo "  ${CMD[*]}"
echo ""
echo "================================================================================"
echo ""

# Run the command
"${CMD[@]}"
EXIT_CODE=$?

# =============================================================================
# Report Result
# =============================================================================
echo ""
echo "================================================================================"
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "✅ Ingestion completed successfully!"
else
    echo "❌ Ingestion failed with exit code: $EXIT_CODE"
fi
echo "================================================================================"
echo ""

exit $EXIT_CODE