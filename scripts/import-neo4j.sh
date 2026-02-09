#!/usr/bin/env bash
set -euo pipefail
trap 'echo "ERROR: Script failed at line $LINENO (exit code $?)" >&2' ERR

# Import Dedalus CSV output into Neo4j using neo4j-admin database import,
# then start the database and load all data including images and external links.
#
# Usage:
#   ./scripts/import-neo4j.sh <output-dir>           # full import
#   ./scripts/import-neo4j.sh <output-dir> --resume   # resume after Ctrl+C pause
#   ./scripts/import-neo4j.sh <output-dir> --clean     # discard progress, start fresh
#
# Press Ctrl+C during LOAD CSV to pause. The current chunk will finish,
# progress is saved to <output-dir>/.import_progress, and you can resume later.
#
# Prerequisites:
#   - Neo4j 5.x+ installed (neo4j-admin, neo4j, cypher-shell on PATH)
#   - Dedalus output directory with nodes.csv, edges.csv, categories.csv,
#     article_categories.csv, images.csv, external_links.csv
#
# Example:
#   ./target/release/dedalus -i enwiki-latest-pages-articles.xml.bz2 -o output/
#   ./scripts/import-neo4j.sh output/
#
# The script will:
#   1. Bulk import nodes/edges/categories via neo4j-admin
#   2. Start Neo4j and wait for it to become available
#   3. Create constraints and indexes
#   4. Load images and external links via LOAD CSV (batched, pausable)

OUTPUT_DIR="${1:?Usage: $0 <output-dir> [--resume|--clean]}"
shift
RESUME=false
CLEAN=false
for arg in "$@"; do
    case "$arg" in
        --resume) RESUME=true ;;
        --clean)  CLEAN=true ;;
        *) echo "Unknown flag: $arg" >&2; exit 1 ;;
    esac
done

DATABASE="neo4j"
PROGRESS_FILE="$OUTPUT_DIR/.import_progress"
PAUSED=0

# Ctrl+C sets the pause flag; the current chunk finishes, then we save and exit.
trap 'echo ""; echo ">>> Ctrl+C received — will pause after current chunk ..."; PAUSED=1' INT

# Resolve to absolute path for LOAD CSV file:/// URIs
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
PROGRESS_FILE="$OUTPUT_DIR/.import_progress"

# Helper: run cypher-shell against a target database
run_cypher() {
    local db="$1"
    shift
    cypher-shell -d "$db" "$@"
}

# Helper: save progress state
save_progress() {
    echo "PHASE=$1" > "$PROGRESS_FILE"
    echo "LAST_CHUNK=$2" >> "$PROGRESS_FILE"
    echo ">>> Progress saved to $PROGRESS_FILE"
    echo ">>> Run with --resume to continue."
}

# Helper: clean up leftover chunk files
cleanup_chunks() {
    local import_dir="${NEO4J_IMPORT:-}"
    if [[ -n "$import_dir" ]]; then
        rm -f "$import_dir"/images_chunk_*.csv "$import_dir"/external_links_chunk_*.csv
    fi
}

# Handle --clean: remove progress file
if $CLEAN; then
    rm -f "$PROGRESS_FILE"
    echo "Cleared import progress."
fi

# Detect resume state
RESUME_PHASE=""
RESUME_CHUNK=0
if [[ -f "$PROGRESS_FILE" ]]; then
    if $RESUME; then
        # shellcheck source=/dev/null
        source "$PROGRESS_FILE"
        RESUME_PHASE="$PHASE"
        RESUME_CHUNK="$LAST_CHUNK"
        echo "Resuming from phase=$RESUME_PHASE, after chunk=$RESUME_CHUNK"
    else
        echo "Found existing progress file: $PROGRESS_FILE"
        echo "Use --resume to continue, or --clean to start fresh."
        exit 1
    fi
fi

# Helper: run cypher-shell against a target database
run_cypher() {
    local db="$1"
    shift
    cypher-shell -d "$db" "$@"
}

# Verify output files exist
for f in nodes.csv edges.csv categories.csv article_categories.csv images.csv external_links.csv; do
    if [[ ! -f "$OUTPUT_DIR/$f" ]]; then
        echo "Error: $OUTPUT_DIR/$f not found. Run dedalus first." >&2
        exit 1
    fi
done

# --------------------------------------------------------------------------
# Detect Neo4j home and import directory
# --------------------------------------------------------------------------
NEO4J_HOME="${NEO4J_HOME:-}"
if [[ -z "$NEO4J_HOME" ]]; then
    # Homebrew on macOS
    if command -v brew >/dev/null 2>&1; then
        BREW_PREFIX="$(brew --prefix neo4j 2>/dev/null || true)"
        if [[ -n "$BREW_PREFIX" && -d "$BREW_PREFIX/libexec" ]]; then
            NEO4J_HOME="$BREW_PREFIX/libexec"
        fi
    fi
    if [[ -z "$NEO4J_HOME" ]]; then
        # Common default locations
        for candidate in /var/lib/neo4j /usr/local/neo4j "$HOME/neo4j"; do
            if [[ -d "$candidate/data/databases" ]]; then
                NEO4J_HOME="$candidate"
                break
            fi
        done
    fi
fi

if [[ -n "$NEO4J_HOME" ]]; then
    echo "Detected NEO4J_HOME: $NEO4J_HOME"
else
    echo "Warning: Could not detect NEO4J_HOME. Set it manually if import fails." >&2
fi

NEO4J_IMPORT="${NEO4J_HOME:+$NEO4J_HOME/import}"

# --------------------------------------------------------------------------
# Steps 1-3: Bulk import, start Neo4j, create indexes (skip on resume)
# --------------------------------------------------------------------------
if [[ -z "$RESUME_PHASE" ]]; then

    # Step 1: Bulk import via neo4j-admin
    echo ""
    echo "==> Step 1: Bulk importing into Neo4j database '$DATABASE' from $OUTPUT_DIR ..."

    echo "    Stopping Neo4j (required for bulk import) ..."
    neo4j stop 2>/dev/null || true

    neo4j-admin database import full \
        --overwrite-destination=true \
        --max-off-heap-memory=16G \
        --nodes "Page=$OUTPUT_DIR/nodes.csv" \
        --nodes "Category=$OUTPUT_DIR/categories.csv" \
        --relationships "$OUTPUT_DIR/edges.csv" \
        --relationships "$OUTPUT_DIR/article_categories.csv" \
        -- "$DATABASE"

    echo "    Bulk import complete."

    # Step 2: Start Neo4j
    echo ""
    echo "==> Step 2: Starting Neo4j ..."
    neo4j start

    echo "    Waiting for Neo4j to accept connections ..."
    RETRIES=30
    for i in $(seq 1 $RETRIES); do
        if cypher-shell "RETURN 1;" >/dev/null 2>&1; then
            echo "    Neo4j is ready."
            break
        fi
        if [[ "$i" -eq "$RETRIES" ]]; then
            echo "Error: Neo4j did not become available after ${RETRIES} attempts." >&2
            echo "Check 'neo4j status' and logs, then run the remaining steps manually." >&2
            echo "Check that authentication is disabled in neo4j.conf or configure credentials." >&2
            exit 1
        fi
        sleep 3
    done

    # Step 3: Create constraints and indexes
    echo ""
    echo "==> Step 3: Creating constraints and indexes ..."

    run_cypher "$DATABASE" <<'CYPHER'
CREATE CONSTRAINT page_id IF NOT EXISTS FOR (p:Page) REQUIRE p.id IS UNIQUE;
CREATE INDEX page_title IF NOT EXISTS FOR (p:Page) ON (p.title);
CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE;
CYPHER

    echo "    Constraints and indexes created."

else
    # On resume, just make sure Neo4j is up
    echo ""
    echo "==> Ensuring Neo4j is running ..."
    if ! cypher-shell "RETURN 1;" >/dev/null 2>&1; then
        neo4j start
        RETRIES=30
        for i in $(seq 1 $RETRIES); do
            if cypher-shell "RETURN 1;" >/dev/null 2>&1; then
                echo "    Neo4j is ready."
                break
            fi
            if [[ "$i" -eq "$RETRIES" ]]; then
                echo "Error: Neo4j did not become available." >&2
                exit 1
            fi
            sleep 3
        done
    else
        echo "    Neo4j is ready."
    fi
fi

# --------------------------------------------------------------------------
# Step 4: Copy CSVs to Neo4j import directory
# --------------------------------------------------------------------------
echo ""
echo "==> Step 4: Loading images and external links via LOAD CSV ..."

if [[ -n "${NEO4J_IMPORT:-}" && -d "$NEO4J_IMPORT" ]]; then
    echo "    Copying CSVs to Neo4j import directory: $NEO4J_IMPORT"
    cp "$OUTPUT_DIR/images.csv" "$NEO4J_IMPORT/images.csv"
    cp "$OUTPUT_DIR/external_links.csv" "$NEO4J_IMPORT/external_links.csv"
    IMAGES_URI="file:///images.csv"
    EXTLINKS_URI="file:///external_links.csv"
else
    echo "    Warning: Neo4j import directory not found." >&2
    echo "    Attempting to copy CSVs into default import path." >&2
    if [[ -n "$NEO4J_HOME" ]]; then
        mkdir -p "$NEO4J_HOME/import"
        cp "$OUTPUT_DIR/images.csv" "$NEO4J_HOME/import/images.csv"
        cp "$OUTPUT_DIR/external_links.csv" "$NEO4J_HOME/import/external_links.csv"
        IMAGES_URI="file:///images.csv"
        EXTLINKS_URI="file:///external_links.csv"
        echo "    Created $NEO4J_HOME/import and copied CSVs."
    else
        echo "Error: Cannot determine Neo4j import directory." >&2
        echo "Set NEO4J_HOME or copy images.csv and external_links.csv to your Neo4j import/ directory," >&2
        echo "then run the LOAD CSV commands from Step 4 manually." >&2
        exit 1
    fi
fi

# --------------------------------------------------------------------------
# load_csv_chunked: split a CSV into chunks and load with progress
#   $1 = source CSV path
#   $2 = label (for display / phase name)
#   $3 = cypher template (use CHUNK_URI as placeholder)
#
# Respects RESUME_PHASE/RESUME_CHUNK to skip already-loaded chunks.
# Checks PAUSED flag between chunks for graceful Ctrl+C pause.
# Returns 1 if paused (caller should exit), 0 if completed.
# --------------------------------------------------------------------------
CHUNK_SIZE=100000

load_csv_chunked() {
    local csv_path="$1"
    local label="$2"
    local cypher_template="$3"

    local skip_to=0
    if [[ "$RESUME_PHASE" == "$label" ]]; then
        skip_to="$RESUME_CHUNK"
    elif [[ -n "$RESUME_PHASE" && "$RESUME_PHASE" != "$label" ]]; then
        # We haven't reached the resume phase yet — check ordering
        # phases are: images, external_links
        if [[ "$RESUME_PHASE" == "external_links" && "$label" == "images" ]]; then
            echo "    Skipping $label (already completed)."
            return 0
        fi
    fi

    local total
    total=$(( $(wc -l < "$csv_path") - 1 ))  # subtract header
    local chunks=$(( (total + CHUNK_SIZE - 1) / CHUNK_SIZE ))
    local loaded=0

    echo "    Loading $label ($total rows in $chunks chunks) ..."

    # Extract header
    local header
    header=$(head -1 "$csv_path")

    # Clean up any leftover chunks from a previous run, then re-split
    rm -f "$NEO4J_IMPORT/${label}_chunk_"*.csv

    local chunk_prefix="$NEO4J_IMPORT/${label}_chunk_"
    awk -v prefix="$chunk_prefix" -v size="$CHUNK_SIZE" -v header="$header" '
        NR == 1 { next }
        (NR - 2) % size == 0 {
            if (out) close(out)
            file_num++
            out = prefix file_num ".csv"
            print header > out
        }
        { print >> out }
    ' "$csv_path"

    for chunk_num in $(seq 1 "$chunks"); do
        local chunk_file="${chunk_prefix}${chunk_num}.csv"

        # Skip already-completed chunks on resume
        if [[ "$chunk_num" -le "$skip_to" ]]; then
            local chunk_rows
            chunk_rows=$(( $(wc -l < "$chunk_file") - 1 ))
            loaded=$(( loaded + chunk_rows ))
            rm -f "$chunk_file"
            continue
        fi

        if [[ "$skip_to" -gt 0 && "$chunk_num" -eq $((skip_to + 1)) ]]; then
            printf "    [skip] Resuming from chunk %d/%d (%d rows already loaded)\n" "$chunk_num" "$chunks" "$loaded"
        fi

        # Check pause flag before starting next chunk
        if [[ "$PAUSED" -eq 1 ]]; then
            local prev=$(( chunk_num - 1 ))
            save_progress "$label" "$prev"
            # Clean up remaining chunk files
            for f in "$chunk_prefix"*.csv; do rm -f "$f"; done
            return 1
        fi

        local chunk_rows
        chunk_rows=$(( $(wc -l < "$chunk_file") - 1 ))

        local chunk_uri="file:///${label}_chunk_${chunk_num}.csv"
        local cypher="${cypher_template//CHUNK_URI/$chunk_uri}"

        run_cypher "$DATABASE" <<< "$cypher"

        loaded=$(( loaded + chunk_rows ))
        local pct=$(( loaded * 100 / total ))
        printf "    [%3d%%] %d / %d rows loaded\n" "$pct" "$loaded" "$total"

        rm -f "$chunk_file"
    done

    return 0
}

# --------------------------------------------------------------------------
# Step 4a: Load images
# --------------------------------------------------------------------------
if ! load_csv_chunked "$OUTPUT_DIR/images.csv" "images" \
"LOAD CSV WITH HEADERS FROM 'CHUNK_URI' AS row
CALL {
    WITH row
    MATCH (p:Page) WHERE p.id = row.article_id
    MERGE (i:Image {filename: row.filename})
    MERGE (p)-[:HAS_IMAGE]->(i)
} IN TRANSACTIONS OF 10000 ROWS;"; then
    echo ""
    echo ">>> Paused during images. Resume with:"
    echo ">>>   $0 $OUTPUT_DIR --resume"
    # Restore default signal handling for clean exit
    trap - INT
    exit 0
fi

# --------------------------------------------------------------------------
# Step 4b: Load external links
# --------------------------------------------------------------------------
if ! load_csv_chunked "$OUTPUT_DIR/external_links.csv" "external_links" \
"LOAD CSV WITH HEADERS FROM 'CHUNK_URI' AS row
CALL {
    WITH row
    MATCH (p:Page) WHERE p.id = row.article_id
    MERGE (e:ExternalLink {url: row.url})
    MERGE (p)-[:HAS_LINK]->(e)
} IN TRANSACTIONS OF 10000 ROWS;"; then
    echo ""
    echo ">>> Paused during external_links. Resume with:"
    echo ">>>   $0 $OUTPUT_DIR --resume"
    trap - INT
    exit 0
fi

echo "    Images and external links loaded."

# Clean up progress file on successful completion
rm -f "$PROGRESS_FILE"

# Restore default signal handling
trap - INT

echo "    Indexes created."

# --------------------------------------------------------------------------
# Done
# --------------------------------------------------------------------------
echo ""
echo "============================================"
echo "  SUCCESS: Import complete!"
echo "============================================"
echo ""
echo "Database '$DATABASE' now contains:"
echo "  - Page nodes (from nodes.csv)"
echo "  - Category nodes and HAS_CATEGORY edges (from categories.csv, article_categories.csv)"
echo "  - LINKS_TO and SEE_ALSO edges (from edges.csv)"
echo "  - Image nodes and HAS_IMAGE edges (from images.csv)"
echo "  - ExternalLink nodes and HAS_LINK edges (from external_links.csv)"
echo ""
echo "Available at: http://localhost:7474"
echo ""
echo "Verify with:"
echo "  cypher-shell -d $DATABASE 'MATCH (n) RETURN labels(n)[0] AS label, count(n) ORDER BY label;'"
