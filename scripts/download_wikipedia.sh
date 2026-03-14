#!/usr/bin/env bash
set -euo pipefail

# Configuration
DUMP_URL="${DUMP_URL:-https://dumps.wikimedia.org/enwiki/latest/}"
DUMP_FILENAME="${DUMP_FILENAME:-enwiki-latest-pages-articles-multistream.xml.bz2}"
INDEX_FILENAME="${INDEX_FILENAME:-enwiki-latest-pages-articles-multistream-index.txt.bz2}"
OUTPUT_DIR="${OUTPUT_DIR:-.}"

CHECKSUM_FILE="enwiki-latest-sha1sums.txt"
DUMP_PATH="${OUTPUT_DIR}/${DUMP_FILENAME}"
INDEX_PATH="${OUTPUT_DIR}/${INDEX_FILENAME}"

echo "Downloading Wikipedia multistream dump + index"
echo "From: ${DUMP_URL}"
echo "Dump:  ${DUMP_PATH}"
echo "Index: ${INDEX_PATH}"
echo

# Create output directory if needed
mkdir -p "${OUTPUT_DIR}"

# Download the multistream index (small, ~250MB compressed)
echo "==> Downloading multistream index..."
curl -C - -L -o "${INDEX_PATH}" "${DUMP_URL}${INDEX_FILENAME}"
echo

# Download the dump (resumable)
echo "==> Downloading multistream dump (this is ~22GB, may take a while)..."
curl -C - -L -o "${DUMP_PATH}" "${DUMP_URL}${DUMP_FILENAME}"
echo

# Download checksums
echo "==> Downloading checksums..."
curl -sL -o "${OUTPUT_DIR}/${CHECKSUM_FILE}" "${DUMP_URL}${CHECKSUM_FILE}"

# Extract expected checksum for our file
EXPECTED=$(grep "${DUMP_FILENAME}" "${OUTPUT_DIR}/${CHECKSUM_FILE}" | awk '{print $1}')
if [ -z "${EXPECTED}" ]; then
    echo "WARNING: Could not find checksum for ${FILENAME} in ${CHECKSUM_FILE}"
    echo "Skipping verification."
    exit 0
fi

# Verify checksum
echo "==> Verifying SHA1 checksum for dump..."
if command -v shasum &>/dev/null; then
    ACTUAL=$(shasum "${DUMP_PATH}" | awk '{print $1}')
elif command -v sha1sum &>/dev/null; then
    ACTUAL=$(sha1sum "${DUMP_PATH}" | awk '{print $1}')
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
echo "Done."
echo "  Dump:  ${DUMP_PATH}"
echo "  Index: ${INDEX_PATH}"
echo
echo "Run dedalus with:"
echo "  dedalus pipeline -i ${DUMP_PATH} -o out/ -v"
