#!/usr/bin/env bash
# =============================================================================
# TX Hash to Ledger Sequence - Column Family Merger Runner Script
# =============================================================================
#
# This script wraps the tx-hash-cf-merger binary and handles library paths.
#
# USAGE:
# ======
#   # From existing RocksDB stores:
#   ./run_merger.sh --config config.toml
#   ./run_merger.sh --config config.toml --dry-run
#   ./run_merger.sh --config config.toml --batch-size 500000
#
#   # From LFS ledger store with logging:
#   ./run_merger.sh --config config.toml \
#       --lfs-workers 16 \
#       --log-file /var/log/merger.log \
#       --error-file /var/log/merger.err
#
# =============================================================================

set -euo pipefail

# =============================================================================
# Set Library Paths
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
# Locate Binary
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY="${SCRIPT_DIR}/tx-hash-cf-merger"

if [[ ! -f "$BINARY" ]]; then
    echo "Binary not found at: $BINARY"
    echo "Building..."
    cd "$SCRIPT_DIR"
    go build -o tx-hash-cf-merger .
    echo "Build complete."
fi

# =============================================================================
# Run Merger
# =============================================================================
echo ""
echo "================================================================================"
echo "        TX Hash to Ledger Sequence - Column Family Merger"
echo "================================================================================"
echo ""
echo "Running: $BINARY $*"
echo ""

"$BINARY" "$@"
EXIT_CODE=$?

echo ""
echo "================================================================================"
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "Merger completed successfully!"
else
    echo "Merger failed with exit code: $EXIT_CODE"
fi
echo "================================================================================"
echo ""

exit $EXIT_CODE
