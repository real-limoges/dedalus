# Dedalus

A Rust pipeline that extracts Wikipedia XML dumps into structured graph data and imports it into [Neo4j](https://neo4j.com).

Dedalus reads compressed Wikipedia dumps (`.xml.bz2`), resolves redirects, extracts article link graphs, and loads everything into Neo4j as a queryable knowledge graph. It can also output raw CSV/JSON files for use with other tools.

## Features

- **Two-pass streaming pipeline** -- indexing pass builds a title-to-ID map, extraction pass produces output in parallel
- **Memory-efficient** -- event-based XML parsing with `quick-xml`; never loads the full dump into memory
- **Parallel extraction** -- uses `rayon` for multi-core article processing
- **Parallel decompression** -- automatically uses `lbzip2` or `pbzip2` when available on PATH; falls back to in-process `MultiBzDecoder`
- **Redirect resolution** -- follows redirect chains (up to 5 hops) to resolve target article IDs
- **CSV sharding** -- `--csv-shards N` splits each CSV into N files for parallel database loading
- **Native Neo4j import** -- `dedalus import` manages Docker, connects via Bolt, and loads all CSVs with throttled parallelism
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

## Quick Start

```bash
# Extract Wikipedia dump with 16 CSV shards
dedalus extract -i enwiki-latest-pages-articles.xml.bz2 -o output/ --csv-shards 16 -v

# Import into Neo4j (starts Docker automatically)
dedalus import -o output/
```

After import completes, Neo4j is available at:
- **Bolt**: `bolt://localhost:7687`
- **Browser**: `http://localhost:7474`

## Usage

Dedalus uses subcommands: `extract` and `import`.

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
| `--csv-shards <N>` | Number of CSV output shards (1 = single file) | `1` |
| `--limit <N>` | Limit pages processed (useful for testing) | none |
| `--dry-run` | Run pipeline without writing output files | `false` |
| `--resume` | Resume from last checkpoint if available | `false` |
| `--no-cache` | Force rebuild of index cache | `false` |
| `--checkpoint-interval <N>` | Save checkpoint every N articles | `10000` |
| `--clean` | Clear existing checkpoint and outputs before starting | `false` |

### `dedalus import`

Loads extracted CSV files into Neo4j via the Bolt protocol. Manages Docker lifecycle automatically.

```bash
dedalus import -o <output-dir> [OPTIONS]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output <DIR>` | Directory containing Dedalus CSV output | required |
| `--bolt-uri <URI>` | Neo4j Bolt URI | `bolt://localhost:7687` |
| `--import-prefix <PREFIX>` | Import file URI prefix for Neo4j LOAD CSV | `file://` |
| `--max-parallel-edges <N>` | Max concurrent edge LOAD CSV jobs | `1` |
| `--max-parallel-light <N>` | Max concurrent light relationship LOAD CSV jobs | `4` |
| `--compose-file <PATH>` | Docker compose file path (auto-detected if omitted) | auto |
| `--no-docker` | Skip Docker management, connect to already-running Neo4j | `false` |
| `--clean` | Tear down existing Neo4j volumes before importing | `false` |

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

# Extract with CSV sharding for parallel import
dedalus extract -i enwiki-latest-pages-articles.xml.bz2 -o output/ --csv-shards 16 -v

# Resume interrupted extraction
dedalus extract -i enwiki-latest-pages-articles.xml.bz2 -o output/ --resume -v

# Import into Neo4j (Docker managed automatically)
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
├── edges.csv              # :START_ID | :END_ID | :TYPE (LINKS_TO, SEE_ALSO)
├── categories.csv         # id:ID(Category) | name | :LABEL
├── article_categories.csv # :START_ID | :END_ID(Category) | :TYPE (HAS_CATEGORY)
├── images.csv             # article_id | filename
├── external_links.csv     # article_id | url
├── index.cache            # Cached index for fast restarts (bincode)
├── checkpoint.bin         # Extraction progress checkpoint (bincode, cleared on completion)
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
- **images** -- image references from `[[File:...]]` / `[[Image:...]]` wikilinks
- **external_links** -- URLs from `[http://...]` markup

### JSON blobs

Enriched article content, sharded by `id % shard_count`:

- `id`, `title`, `abstract_text` (first paragraph, templates stripped)
- `categories` (list), `infoboxes` (structured key-value), `sections` (heading list)
- `timestamp` (revision ISO 8601), `is_disambiguation` (boolean)

Empty fields are omitted from the JSON for compactness.

## Architecture

### Two-Pass Pipeline

1. **Indexing pass** (`index.rs`) -- streams through the dump with `skip_text` enabled to build an in-memory `FxHashMap<String, u32>` of title-to-ID mappings and a redirect resolution table, pre-sized for ~8M articles and ~10M redirects.

2. **Extraction pass** (`extract.rs`) -- streams through the dump a second time, reading article text. Uses `rayon::par_bridge()` to process pages in parallel: extracts wikilinks, categories, infoboxes, images, external links, section headings, and abstracts. `ShardedCsvWriter` distributes rows across N files by `page_id % csv_shards`.

3. **Import** (`import.rs`) -- connects to Neo4j over Bolt (`neo4rs`), creates indexes, loads CSVs with throttled parallelism via `FuturesUnordered` using `CALL { ... } IN TRANSACTIONS` for memory-bounded bulk loading, then creates constraints.

### Modules

| Module | Purpose |
|--------|---------|
| `main.rs` | CLI subcommands (`clap`), orchestrates extract/import |
| `parser.rs` | `WikiReader` -- streaming XML parser implementing `Iterator<Item = WikiPage>`; auto-detects parallel decompressor |
| `index.rs` | `WikiIndex` -- `FxHashMap`-based title-to-ID mapping with redirect chain resolution |
| `extract.rs` | Parallel extraction with `ShardedCsvWriter` for split CSV output |
| `import.rs` | Neo4j import -- Docker management, Bolt connection with retry, throttled LOAD CSV |
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

```bash
cargo build --release          # Build optimized binary
cargo test --verbose           # Run tests (155 tests)
cargo fmt -- --check           # Check formatting
cargo clippy -- -D warnings    # Lint with strict warnings
```

## License

BSD 3-Clause License. See [LICENSE](LICENSE).
