#!/usr/bin/env bash

# =============================================================================
# Generate Random Ledger Sequence Numbers
# =============================================================================
#
# This script generates random ledger sequence numbers within a specified range
# and writes them to a file (one per line).
#
# Usage:
#   ./generate_random_ledgers.sh <start> <end> <count> <output_file>
#
# Example:
#   ./generate_random_ledgers.sh 2 5000000 100000 ledgers.txt
#
# Arguments:
#   start       - Minimum ledger sequence (inclusive)
#   end         - Maximum ledger sequence (inclusive)
#   count       - Number of random ledger sequences to generate
#   output_file - Output file path
#
# Notes:
#   - Numbers may repeat (not guaranteed unique)
#   - Numbers are not sorted
#   - Uses $RANDOM which has limited range, so we combine multiple calls
#     for better distribution across large ranges
#
# =============================================================================

set -e

# Check arguments
if [ $# -ne 4 ]; then
    echo "Usage: $0 <start> <end> <count> <output_file>"
    echo ""
    echo "Example: $0 2 5000000 100000 ledgers.txt"
    echo ""
    echo "Arguments:"
    echo "  start       - Minimum ledger sequence (inclusive)"
    echo "  end         - Maximum ledger sequence (inclusive)"
    echo "  count       - Number of random sequences to generate"
    echo "  output_file - Output file path"
    exit 1
fi

START=$1
END=$2
COUNT=$3
OUTPUT_FILE=$4

# Validate inputs
if [ "$START" -lt 2 ]; then
    echo "ERROR: start must be >= 2 (first ledger sequence)"
    exit 1
fi

if [ "$END" -lt "$START" ]; then
    echo "ERROR: end must be >= start"
    exit 1
fi

if [ "$COUNT" -lt 1 ]; then
    echo "ERROR: count must be >= 1"
    exit 1
fi

RANGE=$((END - START + 1))

echo "Generating $COUNT random ledger sequences..."
echo "  Range: $START to $END (span: $RANGE)"
echo "  Output: $OUTPUT_FILE"
echo ""

# Generate random numbers
# Using /dev/urandom for better randomness with large ranges
# shuf is efficient but may not be available everywhere
# awk with srand() is portable

if command -v shuf &> /dev/null; then
    # shuf is available - use it for efficiency
    # Generate numbers in range and take random sample
    seq "$START" "$END" | shuf -n "$COUNT" > "$OUTPUT_FILE"
else
    # Fallback to awk
    awk -v start="$START" -v end="$END" -v count="$COUNT" '
    BEGIN {
        srand()
        range = end - start + 1
        for (i = 0; i < count; i++) {
            print int(rand() * range) + start
        }
    }' > "$OUTPUT_FILE"
fi

# Count actual lines generated
ACTUAL_COUNT=$(wc -l < "$OUTPUT_FILE")

echo "Done! Generated $ACTUAL_COUNT ledger sequences."
echo ""

# Show sample
echo "Sample (first 5 lines):"
head -5 "$OUTPUT_FILE" | while read line; do
    echo "  $line"
done
echo ""