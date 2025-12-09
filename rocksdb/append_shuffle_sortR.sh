#!/usr/bin/env bash

# =============================================================================
# append_shuffle_sortR.sh
#
# Description:
#   Appends a specified number of random 32-byte hex hashes to an existing file
#   of 64-character hex strings and shuffles the entire file in a memory-efficient
#   way using sort -R (disk-backed random shuffle). Works for very large files.
#
# Usage:
#   ./append_shuffle_sortR.sh <K> <file>
#
# Arguments:
#   <K>    Number of random 32-byte hex hashes to append.
#   <file> Path to the file containing existing hex strings.
#
# Sample Tx File
#❯ tail -f ~/sample-txs
#   ceb5c785ea8673db90a55efff74bd6330a94c6ceffea1f87f8aa564bf5bed5a3
#   950282e2628c80d56600b60d54646f43b5796d2bcad85f6cf58036814ff1c5d5
#   7920c705ff1edc7af5878996014ea3d82542935ee8924773d5da2b509b619c03
#   8326ed3ffb24945cbd0b3dd297efea4e61e8341cd1c71a7c870e407299f238cb
#   ba55c010607892d26eff6e8c87ad72885ff58e194862afec185949e474d1496d
#   29334fca2ca2332323df0291f621f87bf016f3b8f7f9bcad68c41b42ce31d8f1
#
#
# Example:
#   ./append_shuffle_sortR.sh 1000000 ~/sample-txs
#   This will append 1,000,000 random hashes to ~/sample-txs and shuffle all lines.
#
# Requirements:
#   - Bash
#   - OpenSSL (for random hash generation)
#   - sort (GNU coreutils, supporting -R)
#
# Notes:
#   - The script safely overwrites the original file.
#   - For extremely large files, you can set TMPDIR for sort temporary files:
#       TMPDIR=/fast/tmp ./append_shuffle_sortR.sh 1000000 ~/sample-txs
# =============================================================================

set -euo pipefail

# --- Parse arguments ---
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <number_of_random_hashes> <file>"
    exit 1
fi

K="$1"
file="$2"

if [ ! -f "$file" ]; then
    echo "Error: File '$file' does not exist."
    exit 1
fi

tmpfile="$(mktemp)"
trap 'rm -f "$tmpfile"' EXIT

# --- Stream new random hashes and original file directly into sort -R ---
# This avoids writing millions of random lines to disk first
{
    for i in $(seq 1 "$K"); do
        openssl rand -hex 32
    done
    cat "$file"
} | sort -R -o "$tmpfile"

# --- Overwrite original file safely ---
mv "$tmpfile" "$file"

echo "Added $K random hashes and shuffled '$file' (disk-backed, memory-efficient)."
