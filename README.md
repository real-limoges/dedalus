# Dedalus

A Rust pipeline that extracts Wikipedia XML dumps into structured graph data and imports it into [Neo4j](https://neo4j.com).

Dedalus reads compressed Wikipedia dumps (`.xml.bz2`), resolves redirects, extracts article link graphs, and loads everything into Neo4j as a queryable knowledge graph. It can also output raw CSV/JSON files for use with other tools.

## Features

- **Two-pass streaming pipeline** -- indexing pass builds a title-to-ID map, extraction pass produces output in parallel (1.62x faster with 8 shards)
- **Memory-efficient** -- event-based XML parsing with `quick-xml`; never loads the full dump into memory
- **Parallel extraction** -- uses `rayon` for multi-core article processing
- **Parallel decompression** -- automatically uses `lbzip2` or `pbzip2` when available on PATH; falls back to in-process `MultiBzDecoder`
- **Redirect resolution** -- follows redirect chains (up to 5 hops) to resolve target article IDs
- **CSV sharding with merge** -- `--csv-shards N` splits output for parallel extraction; `merge-csvs` combines with deduplication (<5 min overhead)
- **Fast Neo4j import** -- neo4j-admin bulk import (10-100x faster than Bolt) or incremental Bolt-based loading
- **Rich content extraction** -- categories, infoboxes, abstracts, see-also links, images, external links, section headings, disambiguation detection, revision timestamps
- **Namespace-aware** -- parses `<ns>` XML tag for page classification; filters namespace-prefixed links from article edges
- **Sharded JSON blobs** -- enriched article content stored as `blobs/{shard}/{id}.json` (1000 shards by default)
- **Resumable processing** -- index caching and checkpoint-based resume to skip redundant work
- **Dry-run mode** -- validate pipeline without writing files
- **Progress reporting and structured logging** via `indicatif` and `tracing`
- **M1/Apple Silicon optimizations** -- Native SIMD targeting for ~1.6x faster extraction on ARM64

## Building

Requires Rust 1.70+ (stable).

```bash
cargo build --release
```

**Performance Note**: The project uses `.cargo/config.toml` to enable native CPU targeting (`target-cpu=native`) for SIMD optimizations. On Apple Silicon (M1/M2/M3), this provides ~1.6x faster extraction via NEON SIMD instructions. Always use `--release` for production workloads.

## Performance

**Full English Wikipedia** (~22M pages, 87GB compressed):

| Stage | Configuration | Time | Speedup |
|-------|--------------|------|---------|
| Extraction | Single shard (`--csv-shards 1`) | ~4.5 hours | 1x baseline |
| Extraction | 8 shards (`--csv-shards 8`) | ~2.8 hours | **1.62x faster** |
| Merge | After 8-shard extraction | <5 minutes | minimal overhead |
| Import (neo4j-admin) | Bulk import (merged CSVs) | ~15-20 minutes | **10-100x vs Bolt** |
| Import (Bolt) | LOAD CSV via Bolt protocol | 4-6 hours | 1x baseline |

**Recommended workflow**: Extract with 8 shards → Merge → neo4j-admin import for best overall performance.

**Hardware recommendations**:
- **CPU**: 8+ cores for parallel extraction (rayon scales well)
- **RAM**: 16GB minimum (32GB+ recommended for full Wikipedia)
- **Storage**: SSD strongly recommended (CSV writes are I/O intensive)
- **Platform**: Apple Silicon benefits from NEON SIMD optimizations

## Quick Start

### Using Makefile (Recommended)

The easiest way to run the full pipeline:

```bash
# Full hybrid pipeline (extract 8 shards → merge → admin import)
make pipeline WIKI_DUMP=enwiki-latest-pages-articles.xml.bz2

# Test with limited pages
make test-pipeline WIKI_DUMP=small-dump.xml.bz2 LIMIT=10000

# Standard pipeline (single shard, no merge step)
make standard-pipeline WIKI_DUMP=enwiki-latest-pages-articles.xml.bz2

# See all options
make help
```

**Makefile Configuration Variables**:
- `WIKI_DUMP` -- Path to Wikipedia dump (default: `enwiki-latest-pages-articles.xml.bz2`)
- `OUTPUT_DIR` -- Output directory (default: `output`)
- `CSV_SHARDS` -- Number of CSV shards for parallel extraction (default: `8`)
- `SHARD_COUNT` -- Number of JSON blob shards (default: `1000`)
- `LIMIT` -- Cap pages processed for testing (default: none)
- `VERBOSE` -- Verbosity level (default: `-v` for INFO)

**Common Makefile Targets**:
- `make extract` -- Run extraction only
- `make merge` -- Merge sharded CSVs (auto-skips if single shard)
- `make import` -- Run import only (uses --admin-import)
- `make resume` -- Resume interrupted extraction
- `make clean-extract` -- Clear extraction output and start fresh
- `make clean-import` -- Import with clean Neo4j slate
- `make bolt-import` -- Import via Bolt instead of admin tool
- `make stats` -- Show output directory statistics

### Manual Commands

```bash
# Standard workflow (single shard)
dedalus extract -i enwiki-latest-pages-articles.xml.bz2 -o output/ --csv-shards 1 -v
dedalus import -o output/ --admin-import

# Hybrid workflow for optimal performance (fast extraction + fast import)
# Step 1: Fast extraction with 8 shards (1.62x speedup)
dedalus extract -i enwiki-latest-pages-articles.xml.bz2 -o output/ --csv-shards 8 -v

# Step 2: Merge CSVs (deduplicates categories/images/external links, <5 min overhead)
dedalus merge-csvs -o output/

# Step 3: Fast bulk import using neo4j-admin (10-100x faster than Bolt)
dedalus import -o output/ --admin-import
```

After import completes, Neo4j is available at:
- **Bolt**: `bolt://localhost:7687`
- **Browser**: `http://localhost:7474`

## Usage

Dedalus uses subcommands: `extract`, `import`, and `merge-csvs`.

### `dedalus extract`

Processes a Wikipedia dump into CSV/JSON output files.

```bash
dedalus extract -i <dump.xml.bz2> -o <output-dir> [OPTIONS]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-i, --input <PATH>` | Path to Wikipedia dump file (`.xml.bz2`) | required |
| `-o, --output <DIR>` | Output directory for generated files | required |
| `--shard-count <N>` | Number of shards for blob storage | `1000` |
| `--csv-shards <N>` | Number of CSV output shards for parallel extraction (8 recommended for performance) | `8` |
| `--limit <N>` | Limit pages processed (useful for testing) | none |
| `--dry-run` | Run pipeline without writing output files | `false` |
| `--resume` | Resume from last checkpoint if available | `false` |
| `--no-cache` | Force rebuild of index cache | `false` |
| `--checkpoint-interval <N>` | Save checkpoint every N articles | `10000` |
| `--clean` | Clear existing checkpoint and outputs before starting | `false` |

### `dedalus import`

Loads extracted CSV files into Neo4j via the Bolt protocol or neo4j-admin bulk import. Manages Docker lifecycle automatically.

```bash
dedalus import -o <output-dir> [OPTIONS]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output <DIR>` | Directory containing Dedalus CSV output | required |
| `--bolt-uri <URI>` | Neo4j Bolt URI | `bolt://localhost:7687` |
| `--import-prefix <PREFIX>` | Import file URI prefix for Neo4j LOAD CSV | `file://` |
| `--max-parallel-edges <N>` | Max concurrent edge LOAD CSV jobs (conservative for memory) | `4` |
| `--max-parallel-light <N>` | Max concurrent light relationship LOAD CSV jobs | `8` |
| `--compose-file <PATH>` | Docker compose file path (auto-detected if omitted) | auto |
| `--no-docker` | Skip Docker management, connect to already-running Neo4j | `false` |
| `--clean` | Tear down existing Neo4j volumes before importing | `false` |
| `--admin-import` | Use neo4j-admin bulk import (10-100x faster, requires non-sharded CSVs) | `false` |

**Import modes:**
- `--admin-import`: Uses neo4j-admin bulk import tool (10-100x faster). Best for full Wikipedia dumps. Requires empty database and non-sharded CSVs (use `merge-csvs` first if you extracted with `--csv-shards > 1`).
- Default (Bolt): Uses LOAD CSV via Bolt protocol. Slower but works with existing data and sharded CSVs.

### `dedalus merge-csvs`

Merges sharded CSV files into single files suitable for neo4j-admin import. Performs deduplication of categories, images, and external links.

```bash
dedalus merge-csvs -o <output-dir>
```

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output <DIR>` | Directory containing sharded CSVs (e.g., `nodes_000.csv`) | required |

**Note**: When using the Makefile (`make merge` or `make pipeline`), sharded CSV files are automatically archived to a `shards/` subdirectory after merging to prevent import confusion. This preserves the original sharded files while keeping only merged files in the main output directory.

### Global flags

| Flag | Description | Default |
|------|-------------|---------|
| `-v, --verbose` | Increase verbosity (`-v` INFO, `-vv` DEBUG, `-vvv` TRACE) | WARN |

### Examples

```bash
# Process the full English Wikipedia dump
dedalus extract -i enwiki-latest-pages-articles.xml.bz2 -o output/ -v

# Quick test with 10,000 pages
dedalus extract -i enwiki-latest-pages-articles.xml.bz2 -o output/ --limit 10000 -vv

# Extract with CSV sharding for parallel extraction
dedalus extract -i enwiki-latest-pages-articles.xml.bz2 -o output/ --csv-shards 8 -v

# Merge sharded CSVs for neo4j-admin import
dedalus merge-csvs -o output/

# Resume interrupted extraction
dedalus extract -i enwiki-latest-pages-articles.xml.bz2 -o output/ --resume -v

# Import into Neo4j using neo4j-admin bulk import (fastest)
dedalus import -o output/ --admin-import

# Import into Neo4j via Bolt (works with sharded CSVs)
dedalus import -o output/

# Clean import (tears down volumes, starts fresh)
dedalus import -o output/ --clean

# Import into an already-running Neo4j instance
dedalus import -o output/ --no-docker --bolt-uri bolt://my-neo4j:7687
```

## Output Format

With `--csv-shards 1` (default), extraction produces single files. With `--csv-shards N` (N > 1), each CSV is split into numbered shards (e.g. `edges_000.csv` through `edges_015.csv`).

```
output/
├── nodes.csv              # id:ID | title | :LABEL
├── edges.csv                   # :START_ID | :END_ID | :TYPE (LINKS_TO, SEE_ALSO)
├── categories.csv              # id:ID(Category) | name | :LABEL (deduplicated)
├── article_categories.csv      # :START_ID | :END_ID(Category) | :TYPE (HAS_CATEGORY)
├── image_nodes.csv             # id:ID(Image) | filename | :LABEL (deduplicated)
├── article_images.csv          # :START_ID | :END_ID(Image) | :TYPE (HAS_IMAGE)
├── external_link_nodes.csv     # id:ID(ExternalLink) | url | :LABEL (deduplicated)
├── article_external_links.csv  # :START_ID | :END_ID(ExternalLink) | :TYPE (HAS_LINK)
├── index.cache            # Cached index for fast restarts (bincode)
├── checkpoint.bin         # Extraction progress checkpoint (bincode, cleared on completion)
├── shards/                # Archived sharded CSVs (after merge-csvs, optional)
│   ├── nodes_000.csv
│   ├── edges_000.csv
│   └── ...
└── blobs/
    ├── 000/
    │   └── {id}.json      # Enriched article blob
    ├── 001/
    │   └── ...
    └── 999/
```

### CSV files

- **nodes** -- one row per article: `id:ID`, `title`, `:LABEL`
- **edges** -- one row per wikilink: `:START_ID`, `:END_ID`, `:TYPE` (`LINKS_TO` or `SEE_ALSO`). Namespace-prefixed links (Category:, File:, Template:, etc.) are excluded.
- **categories** -- deduplicated category nodes: `id:ID(Category)`, `name`, `:LABEL`
- **article_categories** -- article-to-category edges: `:START_ID`, `:END_ID(Category)`, `:TYPE` (`HAS_CATEGORY`)
- **image_nodes** -- deduplicated image nodes extracted from `[[File:...]]` / `[[Image:...]]` wikilinks: `id:ID(Image)`, `filename`, `:LABEL`
- **article_images** -- article-to-image edges: `:START_ID`, `:END_ID(Image)`, `:TYPE` (`HAS_IMAGE`)
- **external_link_nodes** -- deduplicated external link nodes from `[http://...]` markup: `id:ID(ExternalLink)`, `url`, `:LABEL`
- **article_external_links** -- article-to-external-link edges: `:START_ID`, `:END_ID(ExternalLink)`, `:TYPE` (`HAS_LINK`)

### JSON blobs

Enriched article content, sharded by `id % shard_count`:

- `id`, `title`, `abstract_text` (first paragraph, templates stripped)
- `categories` (list), `infoboxes` (structured key-value), `sections` (heading list)
- `timestamp` (revision ISO 8601), `is_disambiguation` (boolean)

Empty fields are omitted from the JSON for compactness.

## Architecture

### Two-Pass Pipeline

1. **Indexing pass** (`index.rs`) -- streams through the dump with `skip_text` enabled to build an in-memory `FxHashMap<String, u32>` of title-to-ID mappings and a redirect resolution table, pre-sized for ~8M articles and ~10M redirects.

2. **Extraction pass** (`extract.rs`) -- streams through the dump a second time, reading article text. Uses `rayon::par_bridge()` to process pages in parallel: extracts wikilinks, categories, infoboxes, images, external links, section headings, and abstracts. `DashSet` deduplicates categories, images, and external links concurrently. `ShardedCsvWriter` distributes rows across N files by `page_id % csv_shards`.

3. **Merge pass** (`merge.rs`, optional) -- if using `--csv-shards > 1`, combines numbered CSV files into single merged files with cross-shard deduplication of categories, images, and external links. Uses streaming I/O (256KB buffers) and `FxHashSet` for deduplication. Overhead: <5 minutes for full Wikipedia.

4. **Import pass** (`import.rs`) -- two modes: (1) `--admin-import` uses `neo4j-admin database import` for 10-100x faster bulk loading of all node and relationship types; (2) default Bolt mode connects via `neo4rs`, creates indexes, loads CSVs with throttled parallelism via `FuturesUnordered` using `CALL { ... } IN TRANSACTIONS` for memory-bounded bulk loading, then creates constraints.

### Modules

| Module | Purpose |
|--------|---------|
| `main.rs` | CLI subcommands (`clap`), orchestrates extract/import/merge-csvs |
| `parser.rs` | `WikiReader` -- streaming XML parser implementing `Iterator<Item = WikiPage>`; auto-detects parallel decompressor |
| `index.rs` | `WikiIndex` -- `FxHashMap`-based title-to-ID mapping with redirect chain resolution |
| `extract.rs` | Parallel extraction with `ShardedCsvWriter` for split CSV output |
| `import.rs` | Neo4j import -- Docker management, Bolt connection with retry, throttled LOAD CSV, neo4j-admin bulk import |
| `merge.rs` | CSV shard merger -- streaming concatenation with deduplication of categories, images, and external links for neo4j-admin compatibility |
| `models.rs` | Core types: `WikiPage`, `PageType`, `ArticleBlob` |
| `content.rs` | Text extraction: abstract, sections, see-also links, categories, images, external links, disambiguation |
| `infobox.rs` | Brace-matching `{{Infobox ...}}` parser producing structured key-value data |
| `stats.rs` | `ExtractionStats` -- atomic counters for thread-safe metrics |
| `config.rs` | Constants for extraction and import |
| `cache.rs` | Index persistence -- zero-copy serialization via `IndexCacheSer` |
| `checkpoint.rs` | Extraction checkpointing with double-checked locking for resumable processing |

## Docker / Neo4j Setup

Dedalus includes a Docker Compose configuration in `neo4j-platform/docker-compose.yml` that runs:

- **Neo4j Community 5.x** -- graph database with Bolt protocol (port 7687) and browser UI (port 7474)

The `dedalus import` command manages this container automatically. To run it manually:

```bash
IMPORT_DIR=./output docker compose -f neo4j-platform/docker-compose.yml up -d
```

The `IMPORT_DIR` environment variable controls which host directory is mounted at `/import` inside the container.

## Development

### Using Makefile

```bash
make build                     # Build release binary
make test                      # Run tests + clippy + format check
make clean                     # Clean build artifacts
make clean-output              # Clean output directory
make clean-all                 # Clean everything
```

### Manual Commands

```bash
cargo build --release          # Build optimized binary
cargo test --verbose           # Run all tests (161 unit + integration)
cargo fmt -- --check           # Check formatting
cargo clippy -- -D warnings    # Lint with strict warnings
```

## Troubleshooting

### Extraction Issues

**Slow extraction performance**:
- Ensure you're using `cargo build --release` (debug builds are 10-50x slower)
- Use `--csv-shards 8` for 1.62x speedup on multi-core systems
- Check if `lbzip2` or `pbzip2` is available for parallel decompression (`which lbzip2`)
- On Apple Silicon, verify `.cargo/config.toml` has `target-cpu=native` for SIMD optimizations

**Index cache invalidation**:
- Use `--no-cache` to force rebuild if Wikipedia dump changes
- Cache is validated against input file modification time and size
- Cache location: `<output-dir>/index.cache`

**Checkpointing issues**:
- Use `--clean` to clear stale checkpoints and start fresh
- Use `--resume` to continue from last checkpoint (saves every 10,000 articles by default)
- Checkpoint location: `<output-dir>/checkpoint.bin`

**Out of memory during extraction**:
- Reduce `--csv-shards` to lower memory usage (more shards = more file handles)
- Ensure you have 16GB+ RAM for full Wikipedia dumps
- Check system resource usage with `htop` or Activity Monitor

### Import Issues

**Neo4j connection timeout**:
- Wait 30-60 seconds for Neo4j to start (import retries 30 times with 2s delay)
- Check Docker logs: `docker compose -f neo4j-platform/docker-compose.yml logs neo4j`
- Verify Neo4j is running: `docker compose -f neo4j-platform/docker-compose.yml ps`
- Ensure port 7687 (Bolt) and 7474 (Browser) are not in use

**Out of memory during import**:
- Use `--admin-import` for memory-efficient bulk loading (10-100x faster)
- Reduce `--max-parallel-edges` (default: 4 for Bolt)
- Reduce `--max-parallel-light` (default: 8 for Bolt)
- Increase Docker memory limit in Docker Desktop settings

**Import fails with "CSV file not found"**:
- Verify CSV files exist in output directory
- For `--admin-import`, merge sharded CSVs first: `dedalus merge-csvs -o output/`
- Check that `IMPORT_DIR` matches output directory if using manual Docker setup

**Slow Bolt import**:
- Use `--admin-import` for 10-100x speedup (requires empty database and merged CSVs)
- Bolt mode is designed for incremental updates, not bulk loading
- Ensure indexes exist (import creates them automatically before `LOAD CSV`)

### Neo4j Browser Access

After successful import, access Neo4j:
- **Browser UI**: http://localhost:7474
- **Bolt connection**: bolt://localhost:7687
- **Default credentials**: neo4j / password (set in docker-compose.yml)

Query examples:
```cypher
// Count articles
MATCH (a:Article) RETURN count(a);

// Find article by title
MATCH (a:Article {title: "Rust (programming language)"}) RETURN a;

// Find articles with most outgoing links
MATCH (a:Article)-[r:LINKS_TO]->()
RETURN a.title, count(r) as links
ORDER BY links DESC LIMIT 10;

// Find articles in a category
MATCH (a:Article)-[:HAS_CATEGORY]->(c:Category {name: "Programming languages"})
RETURN a.title;
```

### Performance Tips

1. **Fastest workflow**: `--csv-shards 8` → `merge-csvs` → `--admin-import`
2. **Use SSD storage**: CSV writes are I/O intensive
3. **Parallel decompression**: Install `lbzip2` for faster XML parsing
4. **Apple Silicon**: Automatic NEON SIMD optimization (1.6x faster)
5. **Resume interrupted runs**: Use `--resume` instead of restarting from scratch
6. **Test first**: Use `--limit 10000` to validate pipeline before processing full dump

## License

BSD 3-Clause License. See [LICENSE](LICENSE).
