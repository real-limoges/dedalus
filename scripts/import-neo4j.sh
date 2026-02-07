#!/usr/bin/env bash
set -euo pipefail
trap 'echo "ERROR: Script failed at line $LINENO (exit code $?)" >&2' ERR

# Import Dedalus CSV output into Neo4j using neo4j-admin database import,
# then start the database and load all data including images and external links.
#
# Usage:
#   ./scripts/import-neo4j.sh <output-dir>
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
#   3. Create constraints and indexes for bulk-imported data
#   4. Load images and external links via LOAD CSV (batched)
#   5. Create indexes for Image and ExternalLink nodes

OUTPUT_DIR="${1:?Usage: $0 <output-dir>}"
DATABASE="neo4j"

# Resolve to absolute path for LOAD CSV file:/// URIs
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"

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
# Step 1: Bulk import via neo4j-admin
# --------------------------------------------------------------------------
echo ""
echo "==> Step 1: Bulk importing into Neo4j database '$DATABASE' from $OUTPUT_DIR ..."

# neo4j-admin import requires the server to be stopped
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

# --------------------------------------------------------------------------
# Step 2: (Re)start Neo4j so it picks up the newly imported database
# --------------------------------------------------------------------------
echo ""
echo "==> Step 2: Starting Neo4j ..."
neo4j start

# Wait for Neo4j to become available (up to 90 seconds)
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

# --------------------------------------------------------------------------
# Step 3: Create constraints and indexes for bulk-imported data
# --------------------------------------------------------------------------
echo ""
echo "==> Step 3: Creating constraints and indexes ..."

run_cypher "$DATABASE" <<'CYPHER'
CREATE CONSTRAINT page_id IF NOT EXISTS FOR (p:Page) REQUIRE p.id IS UNIQUE;
CREATE INDEX page_title IF NOT EXISTS FOR (p:Page) ON (p.title);
CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE;
CYPHER

echo "    Constraints and indexes created."

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
    # Try to create the import directory if NEO4J_HOME is known
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
#   $2 = label (for display)
#   $3 = cypher template (use CHUNK_URI as placeholder)
# --------------------------------------------------------------------------
CHUNK_SIZE=100000

load_csv_chunked() {
    local csv_path="$1"
    local label="$2"
    local cypher_template="$3"

    local total
    total=$(( $(wc -l < "$csv_path") - 1 ))  # subtract header
    local chunks=$(( (total + CHUNK_SIZE - 1) / CHUNK_SIZE ))
    local loaded=0

    echo "    Loading $label ($total rows in $chunks chunks) ..."

    # Extract header
    local header
    header=$(head -1 "$csv_path")

    # Split CSV into chunk files upfront using awk (avoids pipefail issues)
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
}

# --------------------------------------------------------------------------
# Step 4a: Load images
# --------------------------------------------------------------------------
load_csv_chunked "$OUTPUT_DIR/images.csv" "images" \
"LOAD CSV WITH HEADERS FROM 'CHUNK_URI' AS row
CALL {
    WITH row
    MATCH (p:Page) WHERE p.id = row.article_id
    MERGE (i:Image {filename: row.filename})
    MERGE (p)-[:HAS_IMAGE]->(i)
} IN TRANSACTIONS OF 10000 ROWS;"

# --------------------------------------------------------------------------
# Step 4b: Load external links
# --------------------------------------------------------------------------
load_csv_chunked "$OUTPUT_DIR/external_links.csv" "external_links" \
"LOAD CSV WITH HEADERS FROM 'CHUNK_URI' AS row
CALL {
    WITH row
    MATCH (p:Page) WHERE p.id = row.article_id
    MERGE (e:ExternalLink {url: row.url})
    MERGE (p)-[:HAS_LINK]->(e)
} IN TRANSACTIONS OF 10000 ROWS;"

echo "    Images and external links loaded."

# --------------------------------------------------------------------------
# Step 5: Create indexes for Image and ExternalLink nodes (after loading)
# --------------------------------------------------------------------------
echo ""
echo "==> Step 5: Creating indexes for Image and ExternalLink nodes ..."

run_cypher "$DATABASE" <<'CYPHER'
CREATE INDEX image_filename IF NOT EXISTS FOR (i:Image) ON (i.filename);
CREATE INDEX extlink_url IF NOT EXISTS FOR (e:ExternalLink) ON (e.url);
CYPHER

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
