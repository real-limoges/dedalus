#!/usr/bin/env bash
set -euo pipefail

# Import Dedalus CSV output into Neo4j using neo4j-admin database import.
#
# Usage:
#   ./scripts/import-neo4j.sh <output-dir> [database-name]
#
# Prerequisites:
#   - Neo4j 5.x installed (neo4j-admin on PATH)
#   - Neo4j server stopped before import
#   - Dedalus output directory with nodes.csv, edges.csv, categories.csv, article_categories.csv
#
# Example:
#   ./target/release/dedalus -i enwiki-latest-pages-articles.xml.bz2 -o output/
#   neo4j stop
#   ./scripts/import-neo4j.sh output/ wikipedia
#   neo4j start

OUTPUT_DIR="${1:?Usage: $0 <output-dir> [database-name]}"
DATABASE="${2:-neo4j}"

# Verify output files exist
for f in nodes.csv edges.csv categories.csv article_categories.csv; do
    if [[ ! -f "$OUTPUT_DIR/$f" ]]; then
        echo "Error: $OUTPUT_DIR/$f not found. Run dedalus first." >&2
        exit 1
    fi
done

echo "Importing into Neo4j database '$DATABASE' from $OUTPUT_DIR ..."

neo4j-admin database import full \
    --overwrite-destination \
    --nodes=Page="$OUTPUT_DIR/nodes.csv" \
    --nodes=Category="$OUTPUT_DIR/categories.csv" \
    --relationships="$OUTPUT_DIR/edges.csv" \
    --relationships="$OUTPUT_DIR/article_categories.csv" \
    "$DATABASE"

echo ""
echo "Import complete. Start Neo4j and create indexes:"
echo ""
echo "  neo4j start"
echo "  cypher-shell -d $DATABASE"
echo ""
echo "Then run:"
echo "  CREATE CONSTRAINT page_id IF NOT EXISTS FOR (p:Page) REQUIRE p.id IS UNIQUE;"
echo "  CREATE INDEX page_title IF NOT EXISTS FOR (p:Page) ON (p.title);"
echo "  CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE;"
