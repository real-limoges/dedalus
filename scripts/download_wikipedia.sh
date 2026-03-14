#!/usr/bin/env bash
set -euo pipefail

# Configuration
DUMP_FILENAME="${DUMP_FILENAME:-enwiki-latest-pages-articles-multistream.xml.bz2}"
INDEX_FILENAME="${INDEX_FILENAME:-enwiki-latest-pages-articles-multistream-index.txt.bz2}"
OUTPUT_DIR="${OUTPUT_DIR:-.}"
CHECKSUM_FILE="enwiki-latest-sha1sums.txt"

# Mirrors to try, fastest first. Override with DUMP_URL to skip auto-detection.
MIRRORS=(
    "https://dumps.wikimedia.your.org/enwiki/latest/"
    "https://ftp.acc.umu.se/mirror/wikimedia.org/dumps/enwiki/latest/"
    "https://dumps.wikimedia.org/enwiki/latest/"
)

DUMP_PATH="${OUTPUT_DIR}/${DUMP_FILENAME}"
INDEX_PATH="${OUTPUT_DIR}/${INDEX_FILENAME}"

# Create output directory if needed
mkdir -p "${OUTPUT_DIR}"

# Pick the fastest mirror by timing a small HEAD request
pick_mirror() {
    if [ -n "${DUMP_URL:-}" ]; then
        echo "${DUMP_URL}"
        return
    fi

    echo "==> Testing mirrors..." >&2
    local best_url=""
    local best_time="999"

    for url in "${MIRRORS[@]}"; do
        # Time a HEAD request, timeout after 5s
        local t
        t=$(curl -sI -o /dev/null -w "%{time_total}" --max-time 5 "${url}${CHECKSUM_FILE}" 2>/dev/null || echo "999")
        echo "    ${url} -> ${t}s" >&2
        if awk "BEGIN{exit !(${t} < ${best_time})}"; then
            best_time="${t}"
            best_url="${url}"
        fi
    done

    if [ -z "${best_url}" ]; then
        echo "    All mirrors unreachable, using default" >&2
        best_url="https://dumps.wikimedia.org/enwiki/latest/"
    else
        echo "    Selected: ${best_url}" >&2
    fi
    echo "${best_url}"
}

BASE_URL=$(pick_mirror)
echo
echo "Downloading Wikipedia multistream dump + index"
echo "Mirror: ${BASE_URL}"
echo "Dump:   ${DUMP_PATH}"
echo "Index:  ${INDEX_PATH}"
echo

# Download a file with resume support, falling back through mirrors on failure
download() {
    local filename="$1"
    local dest="$2"

    if [ -f "${dest}" ]; then
        local existing_size
        existing_size=$(wc -c < "${dest}" 2>/dev/null || echo 0)
        if [ "${existing_size}" -gt 1000 ]; then
            echo "    Resuming from $(numfmt --to=iec "${existing_size}" 2>/dev/null || echo "${existing_size} bytes")"
        fi
    fi

    # Try the selected mirror first, then fall back to others
    local urls=("${BASE_URL}")
    for url in "${MIRRORS[@]}"; do
        if [ "${url}" != "${BASE_URL}" ]; then
            urls+=("${url}")
        fi
    done

    for url in "${urls[@]}"; do
        echo "    Trying ${url}..."
        if curl -C - -L --progress-bar --fail -o "${dest}" "${url}${filename}"; then
            return 0
        fi
        echo "    Failed, trying next mirror..."
    done

    echo "ERROR: All mirrors failed for ${filename}"
    return 1
}

# Download the multistream index (small, ~250MB compressed)
echo "==> Downloading multistream index..."
download "${INDEX_FILENAME}" "${INDEX_PATH}"
echo

# Download the dump (resumable, ~22GB)
echo "==> Downloading multistream dump (~22GB, may take a while)..."
download "${DUMP_FILENAME}" "${DUMP_PATH}"
echo

# Download checksums
echo "==> Downloading checksums..."
curl -sL --fail -o "${OUTPUT_DIR}/${CHECKSUM_FILE}" "${BASE_URL}${CHECKSUM_FILE}" || true

# Extract expected checksum for our file
if [ ! -f "${OUTPUT_DIR}/${CHECKSUM_FILE}" ]; then
    echo "WARNING: Could not download checksum file. Skipping verification."
    exit 0
fi

EXPECTED=$(grep "${DUMP_FILENAME}" "${OUTPUT_DIR}/${CHECKSUM_FILE}" | awk '{print $1}')
if [ -z "${EXPECTED}" ]; then
    echo "WARNING: Could not find checksum for ${DUMP_FILENAME} in ${CHECKSUM_FILE}"
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
