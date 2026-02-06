# Dedalus

A Rust data processing pipeline that extracts and transforms Wikipedia XML dumps into structured data for Neo4J graph database ingestion.

Dedalus reads compressed Wikipedia dumps (`.xml.bz2`), resolves redirects, extracts article link graphs, and outputs Neo4J-compatible CSV files alongside sharded JSON article content.

## Features

- **Two-pass streaming pipeline** -- indexing pass builds a title-to-ID map, extraction pass produces output in parallel
- **Memory-efficient** -- event-based XML parsing with `quick-xml`; never loads the full dump into memory
- **Parallel extraction** -- uses `rayon` for multi-core article processing
- **Parallel decompression** -- automatically uses `lbzip2` or `pbzip2` when available on PATH; falls back to in-process `MultiBzDecoder`
- **Redirect resolution** -- follows redirect chains (up to 5 hops) to resolve target article IDs
- **Neo4J-compatible output** -- `nodes.csv`, `edges.csv`, `categories.csv`, `article_categories.csv` formatted for `neo4j-admin import`; `images.csv` and `external_links.csv` loadable via `LOAD CSV`
- **Rich content extraction** -- categories, infoboxes, abstracts, see-also links, images, external links, section headings, disambiguation detection, revision timestamps
- **Namespace-aware** -- parses `<ns>` XML tag for page classification; filters namespace-prefixed links from article edges
- **Sharded JSON blobs** -- enriched article content stored as `blobs/{shard}/{id}.json` (1000 shards by default)
- **Resumable processing** -- index caching and checkpoint-based resume to skip redundant work
- **Dry-run mode** -- validate pipeline without writing files
- **Progress reporting and structured logging** via `indicatif` and `tracing`

## Building

Requires Rust 1.70+ (stable).

```bash
cargo build --release
```

## Usage

```bash
./target/release/dedalus --input <path-to-dump.xml.bz2> --output <output-dir>
```

### CLI Options

| Flag | Description | Default |
|------|-------------|---------|
| `-i, --input <PATH>` | Path to Wikipedia dump file (`.xml.bz2`) | required |
| `-o, --output <DIR>` | Output directory for generated files | required |
| `--shard-count <N>` | Number of shards for blob storage | `1000` |
| `--limit <N>` | Limit number of pages to process (useful for testing) | none |
| `--dry-run` | Run pipeline without writing output files | `false` |
| `-v, --verbose` | Increase verbosity (`-v` INFO, `-vv` DEBUG, `-vvv` TRACE) | WARN |
| `--resume` | Resume from last checkpoint if available | `false` |
| `--no-cache` | Force rebuild of index cache | `false` |
| `--checkpoint-interval <N>` | Save checkpoint every N articles | `10000` |
| `--clean` | Clear existing checkpoint and outputs before starting | `false` |

### Example

```bash
# Process the full English Wikipedia dump
./target/release/dedalus -i enwiki-latest-pages-articles.xml.bz2 -o output/ -v

# Quick test with 10,000 pages
./target/release/dedalus -i enwiki-latest-pages-articles.xml.bz2 -o output/ --limit 10000 -vv

# Resume interrupted processing (uses cached index and checkpoint)
./target/release/dedalus -i enwiki-latest-pages-articles.xml.bz2 -o output/ --resume -v

# Force fresh start (rebuild index, clear checkpoint)
./target/release/dedalus -i enwiki-latest-pages-articles.xml.bz2 -o output/ --clean -v

# Force index rebuild while keeping checkpoint
./target/release/dedalus -i enwiki-latest-pages-articles.xml.bz2 -o output/ --no-cache -v
```

## Output Format

```
output/
├── nodes.csv              # id:ID | title | :LABEL
├── edges.csv              # :START_ID | :END_ID | :TYPE (LINKS_TO, SEE_ALSO)
├── categories.csv         # id:ID(Category) | name | :LABEL
├── article_categories.csv # :START_ID | :END_ID(Category) | :TYPE (HAS_CATEGORY)
├── images.csv             # article_id | filename
├── external_links.csv     # article_id | url
├── index.cache            # Cached index for fast restarts (bincode)
├── checkpoint.bin         # Extraction progress checkpoint (bincode)
└── blobs/
    ├── 000/
    │   └── {id}.json      # Enriched article blob (see below)
    ├── 001/
    │   └── ...
    └── 999/
```

- **nodes.csv** -- one row per article with columns `id:ID`, `title`, `:LABEL` (compatible with `neo4j-admin import`)
- **edges.csv** -- one row per wikilink with columns `:START_ID`, `:END_ID`, `:TYPE` (`LINKS_TO` for regular links, `SEE_ALSO` for links in "See also" sections). Namespace-prefixed links (Category:, File:, Template:, etc.) are excluded.
- **categories.csv** -- deduplicated category nodes with columns `id:ID(Category)`, `name`, `:LABEL`
- **article_categories.csv** -- article-to-category edges with columns `:START_ID`, `:END_ID(Category)`, `:TYPE` (`HAS_CATEGORY`)
- **images.csv** -- image references extracted from `[[File:...]]` / `[[Image:...]]` wikilinks
- **external_links.csv** -- external URLs extracted from `[http://...]` markup
- **blobs/** -- enriched article JSON, sharded by `id % shard_count`. Each blob contains:
  - `id`, `title`, `abstract_text` (first paragraph, templates stripped)
  - `categories` (list), `infoboxes` (structured key-value), `sections` (heading list)
  - `timestamp` (revision ISO 8601), `is_disambiguation` (boolean)
- **index.cache** -- serialized index for skipping the indexing pass on subsequent runs (auto-invalidated if input file changes)
- **checkpoint.bin** -- extraction progress checkpoint for resumable processing (cleared on successful completion)

## Architecture

### Two-Pass Pipeline

1. **Indexing pass** (`index.rs`) -- streams through the dump with `skip_text` enabled to build an in-memory `FxHashMap<String, u32>` (from `rustc-hash`) of title-to-ID mappings and a redirect resolution table, pre-sized for ~8M articles and ~10M redirects. No article text is read.

2. **Extraction pass** (`extract.rs`) -- streams through the dump a second time, this time reading article text. Uses `rayon::par_bridge()` to process pages in parallel: extracts wikilinks, categories, infoboxes, images, external links, section headings, and abstracts. Resolves link targets through the index and writes all output files concurrently.

### Modules

| Module | Purpose |
|--------|---------|
| `main.rs` | CLI parsing (`clap`), orchestrates two-pass pipeline, summary output |
| `parser.rs` | `WikiReader` -- streaming XML parser implementing `Iterator<Item = WikiPage>`; auto-detects `lbzip2`/`pbzip2` for parallel decompression |
| `index.rs` | `WikiIndex` -- `FxHashMap`-based title-to-ID mapping with redirect chain resolution |
| `extract.rs` | Parallel extraction of nodes, edges, categories, images, external links, and article blobs |
| `models.rs` | Core types: `WikiPage`, `PageType`, `ArticleBlob` |
| `content.rs` | Text extraction helpers: abstract, sections, see-also links, categories, images, external links, disambiguation detection; CSV field sanitization |
| `infobox.rs` | Brace-matching `{{Infobox ...}}` parser producing structured key-value data |
| `stats.rs` | `ExtractionStats` -- atomic counters for thread-safe metrics |
| `config.rs` | Constants: redirect depth, shard count, progress interval, cache/checkpoint versions |
| `cache.rs` | Index persistence -- zero-copy serialization (`IndexCacheSer` borrows data) and single-pass `try_load_index` |
| `checkpoint.rs` | Extraction checkpointing -- save/load progress for resumable processing |

## Loading into Neo4j

After running Dedalus, import the CSVs into Neo4j using `neo4j-admin`:

```bash
# Stop Neo4j first
neo4j stop

# Import (or use scripts/import-neo4j.sh)
neo4j-admin database import full \
    --overwrite-destination \
    --nodes=Page=output/nodes.csv \
    --nodes=Category=output/categories.csv \
    --relationships=output/edges.csv \
    --relationships=output/article_categories.csv \
    neo4j

# Start Neo4j and create indexes
neo4j start
cypher-shell <<'EOF'
CREATE CONSTRAINT page_id IF NOT EXISTS FOR (p:Page) REQUIRE p.id IS UNIQUE;
CREATE INDEX page_title IF NOT EXISTS FOR (p:Page) ON (p.title);
CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE;
EOF
```

See `scripts/import-neo4j.sh` for a ready-to-use bulk import script. See `docs/IMPLEMENTATION_GUIDE_PHASES_6-9.md` for verification queries and sample Cypher.

### Loading Images and External Links

`images.csv` and `external_links.csv` use plain headers and are not compatible with `neo4j-admin import`. After Neo4j is running, load them with `LOAD CSV`:

```cypher
// Create Image nodes and HAS_IMAGE relationships
LOAD CSV WITH HEADERS FROM 'file:///images.csv' AS row
MATCH (p:Page) WHERE p.id = toInteger(row.article_id)
MERGE (i:Image {filename: row.filename})
CREATE (p)-[:HAS_IMAGE]->(i);

// Create ExternalLink nodes and HAS_LINK relationships
LOAD CSV WITH HEADERS FROM 'file:///external_links.csv' AS row
MATCH (p:Page) WHERE p.id = toInteger(row.article_id)
MERGE (e:ExternalLink {url: row.url})
CREATE (p)-[:HAS_LINK]->(e);

// Create indexes for the new node types
CREATE INDEX image_filename IF NOT EXISTS FOR (i:Image) ON (i.filename);
CREATE INDEX extlink_url IF NOT EXISTS FOR (e:ExternalLink) ON (e.url);
```

**Note:** Copy `images.csv` and `external_links.csv` to the Neo4j import directory (typically `<NEO4J_HOME>/import/`) or adjust the file paths accordingly.

## Development

```bash
cargo build --release          # Build optimized binary
cargo test --verbose           # Run tests
cargo fmt -- --check           # Check formatting
cargo clippy -- -D warnings    # Lint with strict warnings
```

CI runs formatting, linting, build, and test checks on every push and PR via GitHub Actions.

## License

BSD 3-Clause License. See [LICENSE](LICENSE).
