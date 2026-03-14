#!/usr/bin/env bash
set -euo pipefail

# Configuration
DUMP_URL="${DUMP_URL:-https://dumps.wikimedia.org/enwiki/latest/}"
FILENAME="${FILENAME:-enwiki-latest-pages-articles.xml.bz2}"
OUTPUT_DIR="${OUTPUT_DIR:-.}"

CHECKSUM_FILE="enwiki-latest-sha1sums.txt"
OUTPUT_PATH="${OUTPUT_DIR}/${FILENAME}"

echo "Downloading Wikipedia dump: ${FILENAME}"
echo "From: ${DUMP_URL}"
echo "To:   ${OUTPUT_PATH}"
echo

# Create output directory if needed
mkdir -p "${OUTPUT_DIR}"

# Download the dump (resumable)
echo "==> Downloading dump (this is ~22GB, may take a while)..."
curl -C - -L -o "${OUTPUT_PATH}" "${DUMP_URL}${FILENAME}"
echo

# Download checksums
echo "==> Downloading checksums..."
curl -sL -o "${OUTPUT_DIR}/${CHECKSUM_FILE}" "${DUMP_URL}${CHECKSUM_FILE}"

# Extract expected checksum for our file
EXPECTED=$(grep "${FILENAME}" "${OUTPUT_DIR}/${CHECKSUM_FILE}" | awk '{print $1}')
if [ -z "${EXPECTED}" ]; then
    echo "WARNING: Could not find checksum for ${FILENAME} in ${CHECKSUM_FILE}"
    echo "Skipping verification."
    exit 0
fi

# Verify checksum
echo "==> Verifying SHA1 checksum..."
if command -v shasum &>/dev/null; then
    ACTUAL=$(shasum "${OUTPUT_PATH}" | awk '{print $1}')
elif command -v sha1sum &>/dev/null; then
    ACTUAL=$(sha1sum "${OUTPUT_PATH}" | awk '{print $1}')
else
    echo "WARNING: Neither shasum nor sha1sum found. Skipping verification."
    exit 0
fi

if [ "${ACTUAL}" = "${EXPECTED}" ]; then
    echo "Checksum OK: ${ACTUAL}"
else
    echo "ERROR: Checksum mismatch!"
    echo "  Expected: ${EXPECTED}"
    echo "  Actual:   ${ACTUAL}"
    exit 1
fi

echo
echo "Done. Wikipedia dump saved to: ${OUTPUT_PATH}"
