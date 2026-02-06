#!/usr/bin/env bash
set -euo pipefail

# Import Dedalus CSV output into Neo4j using neo4j-admin database import,
# then start the database and load all data including images and external links.
#
# Usage:
#   ./scripts/import-neo4j.sh <output-dir> [database-name]
#
# Prerequisites:
#   - Neo4j 5.x+ installed (neo4j-admin, neo4j, cypher-shell on PATH)
#   - Neo4j server stopped before running this script
#   - Dedalus output directory with nodes.csv, edges.csv, categories.csv,
#     article_categories.csv, images.csv, external_links.csv
#
# Example:
#   ./target/release/dedalus -i enwiki-latest-pages-articles.xml.bz2 -o output/
#   neo4j stop
#   ./scripts/import-neo4j.sh output/ neo4j
#   # Or for a custom database name (created automatically after import):
#   ./scripts/import-neo4j.sh output/ wikipedia
#
# The script will:
#   1. Validate all required CSV files
#   2. Bulk import nodes/edges/categories via neo4j-admin
#   3. Start Neo4j
#   4. Wait for the database to become available
#   5. Create the database if it doesn't already exist (non-default names)
#   6. Create constraints and indexes
#   7. Copy images.csv and external_links.csv to the Neo4j import directory
#   8. Load images and external links via LOAD CSV

OUTPUT_DIR="${1:?Usage: $0 <output-dir> [database-name]}"
DATABASE="${2:-neo4j}"

# Resolve to absolute path for LOAD CSV file:/// URIs
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"

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

neo4j-admin database import full \
    --overwrite-destination=true \
    --nodes=Page="$OUTPUT_DIR/nodes.csv" \
    --nodes=Category="$OUTPUT_DIR/categories.csv" \
    --relationships="$OUTPUT_DIR/edges.csv" \
    --relationships="$OUTPUT_DIR/article_categories.csv" \
    "$DATABASE"

echo "    Bulk import complete."

# --------------------------------------------------------------------------
# Step 2: Start Neo4j
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
        exit 1
    fi
    sleep 3
done

# --------------------------------------------------------------------------
# Step 3: Create database if not the default
# --------------------------------------------------------------------------
if [[ "$DATABASE" != "neo4j" ]]; then
    echo ""
    echo "==> Step 3: Creating database '$DATABASE' ..."
    cypher-shell -d system "CREATE DATABASE \`$DATABASE\` IF NOT EXISTS;" 2>/dev/null || true
    # Wait for the new database to come online
    sleep 3
    for i in $(seq 1 10); do
        if cypher-shell -d "$DATABASE" "RETURN 1;" >/dev/null 2>&1; then
            echo "    Database '$DATABASE' is online."
            break
        fi
        sleep 2
    done
else
    echo ""
    echo "==> Step 3: Using default database 'neo4j' (skipping CREATE DATABASE)."
fi

# --------------------------------------------------------------------------
# Step 4: Create constraints and indexes
# --------------------------------------------------------------------------
echo ""
echo "==> Step 4: Creating constraints and indexes ..."

cypher-shell -d "$DATABASE" <<'CYPHER'
CREATE CONSTRAINT page_id IF NOT EXISTS FOR (p:Page) REQUIRE p.id IS UNIQUE;
CREATE INDEX page_title IF NOT EXISTS FOR (p:Page) ON (p.title);
CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE;
CREATE INDEX image_filename IF NOT EXISTS FOR (i:Image) ON (i.filename);
CREATE INDEX extlink_url IF NOT EXISTS FOR (e:ExternalLink) ON (e.url);
CYPHER

echo "    Constraints and indexes created."

# --------------------------------------------------------------------------
# Step 5: Load images and external links via LOAD CSV
# --------------------------------------------------------------------------
echo ""
echo "==> Step 5: Loading images and external links via LOAD CSV ..."

# Copy CSVs to Neo4j import directory if we found one, otherwise use file:/// with absolute path
if [[ -n "${NEO4J_IMPORT:-}" && -d "$NEO4J_IMPORT" ]]; then
    echo "    Copying CSVs to Neo4j import directory: $NEO4J_IMPORT"
    cp "$OUTPUT_DIR/images.csv" "$NEO4J_IMPORT/images.csv"
    cp "$OUTPUT_DIR/external_links.csv" "$NEO4J_IMPORT/external_links.csv"
    IMAGES_URI="file:///images.csv"
    EXTLINKS_URI="file:///external_links.csv"
else
    echo "    Warning: Neo4j import directory not found. Using absolute file paths."
    echo "    If LOAD CSV fails, set NEO4J_HOME or copy CSVs to the Neo4j import directory."
    IMAGES_URI="file:///$OUTPUT_DIR/images.csv"
    EXTLINKS_URI="file:///$OUTPUT_DIR/external_links.csv"
fi

echo "    Loading images ..."
cypher-shell -d "$DATABASE" <<CYPHER
LOAD CSV WITH HEADERS FROM '$IMAGES_URI' AS row
MATCH (p:Page) WHERE p.id = toInteger(row.article_id)
MERGE (i:Image {filename: row.filename})
CREATE (p)-[:HAS_IMAGE]->(i);
CYPHER

echo "    Loading external links ..."
cypher-shell -d "$DATABASE" <<CYPHER
LOAD CSV WITH HEADERS FROM '$EXTLINKS_URI' AS row
MATCH (p:Page) WHERE p.id = toInteger(row.article_id)
MERGE (e:ExternalLink {url: row.url})
CREATE (p)-[:HAS_LINK]->(e);
CYPHER

# --------------------------------------------------------------------------
# Done
# --------------------------------------------------------------------------
echo ""
echo "==> Import complete!"
echo ""
echo "Database '$DATABASE' now contains:"
echo "  - Page nodes (from nodes.csv)"
echo "  - Category nodes and HAS_CATEGORY edges (from categories.csv, article_categories.csv)"
echo "  - LINKS_TO and SEE_ALSO edges (from edges.csv)"
echo "  - Image nodes and HAS_IMAGE edges (from images.csv)"
echo "  - ExternalLink nodes and HAS_LINK edges (from external_links.csv)"
echo ""
echo "Verify with:"
echo "  cypher-shell -d $DATABASE 'MATCH (n) RETURN labels(n)[0] AS label, count(n) ORDER BY label;'"
