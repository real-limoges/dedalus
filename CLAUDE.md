# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Dedalus is a Rust pipeline that extracts Wikipedia dumps into structured graph data and imports it into Neo4j. It processes compressed Wikipedia XML dumps (.bz2), outputs nodes/edges as CSV files along with article content as sharded JSON blobs, and loads everything into Neo4j via the Bolt protocol.

## Build Commands

```bash
cargo build --release          # Build optimized binary
cargo test --verbose           # Run tests
cargo fmt -- --check           # Check formatting
cargo clippy -- -D warnings    # Lint with strict warnings
```

## Running

The binary uses subcommands: `extract` and `import`.

### Extract

Processes a Wikipedia dump into CSV/JSON output files.

```bash
dedalus extract -i <path-to-wiki-dump.xml.bz2> -o <output-directory>
```

#### Extract flags

- `-i` / `--input` -- path to `.xml.bz2` Wikipedia dump (required)
- `-o` / `--output` -- output directory (required)
- `--shard-count` -- blob shard count (default: 1000)
- `--csv-shards` -- number of CSV output shards for parallel import (default: 1; >1 produces `edges_000.csv`, `edges_001.csv`, etc.)
- `--limit` -- cap pages processed (useful for testing)
- `--dry-run` -- skip file writes
- `--resume` -- resume from last checkpoint if available
- `--no-cache` -- force rebuild of index cache
- `--checkpoint-interval` -- save checkpoint every N articles (default: 10000)
- `--clean` -- clear existing checkpoint and outputs before starting

### Import

Loads extracted CSV files into Neo4j. Manages Docker lifecycle automatically.

```bash
dedalus import -o <output-directory>
```

#### Import flags

- `-o` / `--output` -- directory containing Dedalus CSV output files (required)
- `--bolt-uri` -- Neo4j Bolt URI (default: `bolt://localhost:7687`)
- `--import-prefix` -- import file URI prefix for Neo4j LOAD CSV (default: `file://`)
- `--max-parallel-edges` -- max concurrent LOAD CSV jobs for edges (default: 1, serialized due to memory pressure)
- `--max-parallel-light` -- max concurrent LOAD CSV jobs for lighter relationships (default: 4)
- `--compose-file` -- Docker compose file path (auto-detected if not specified)
- `--no-docker` -- skip Docker management, connect to an already-running Neo4j
- `--clean` -- tear down existing Neo4j volumes before importing

### Global flags

- `-v` / `--verbose` -- verbosity (`-v` INFO, `-vv` DEBUG, `-vvv` TRACE; default WARN)

### Typical workflow

```bash
# Extract with 16 CSV shards for parallel import
dedalus extract -i enwiki-latest-pages-articles.xml.bz2 -o out/ --csv-shards 16

# Import into Neo4j (starts Docker automatically)
dedalus import -o out/

# Import with a clean slate
dedalus import -o out/ --clean
```

## Architecture

### Two-Pass Extraction Pipeline

1. **Indexing Pass** (`index.rs`): Builds title-to-ID mapping and redirect resolution table without reading article text.
2. **Extraction Pass** (`extract.rs`): Uses the index to extract nodes, edges, categories, images, external links, and enriched article content in parallel.

### Core Modules

- **`main.rs`**: CLI entry point using `clap` subcommands (`Commands::Extract`, `Commands::Import`). Initializes `tracing` logging. The import path creates a `tokio` runtime manually; extraction stays sync/rayon.

- **`parser.rs`**: Streaming XML parser implementing `Iterator<Item = WikiPage>`. State machine over `quick-xml` events for memory-efficient BZ2 parsing. Probes PATH for `lbzip2`/`pbzip2` for parallel decompression; falls back to in-process `MultiBzDecoder`. `Drop` cleans up child processes. `skip_text` flag enables a lightweight indexing mode.

- **`index.rs`**: `FxHashMap`-based title-to-ID index. Follows redirect chains (max depth from `config::REDIRECT_MAX_DEPTH`). Uses `indicatif` progress spinner during building.

- **`extract.rs`**: Parallel extraction via `rayon::par_bridge()`. `ShardedCsvWriter` distributes CSV rows by `page_id % csv_shards` across N files. Uses `DashSet` for concurrent category deduplication. Outputs:
  - `nodes[_NNN].csv` -- `id:ID`, `title`, `:LABEL`
  - `edges[_NNN].csv` -- `:START_ID`, `:END_ID`, `:TYPE` (LINKS_TO or SEE_ALSO)
  - `categories[_NNN].csv` -- `id:ID(Category)`, `name`, `:LABEL`
  - `article_categories[_NNN].csv` -- `:START_ID`, `:END_ID(Category)`, `:TYPE` (HAS_CATEGORY)
  - `images[_NNN].csv` -- `article_id`, `filename`
  - `external_links[_NNN].csv` -- `article_id`, `url`
  - `blobs/{shard:03}/{id}.json` -- enriched article content

- **`import.rs`**: Neo4j import pipeline over Bolt (`neo4rs`). Detects CSV layout (single vs sharded). Manages Docker via `tokio::process::Command`. Connects with retry. Creates indexes, loads CSVs with throttled parallelism via `FuturesUnordered` using `CALL { ... } IN TRANSACTIONS OF 10000 ROWS` for memory-bounded bulk loading, then creates constraints.

- **`models.rs`**: Core types -- `WikiPage`, `PageType` (Article/Redirect/Special), `ArticleBlob`.

- **`content.rs`**: Regex-based text extraction -- `extract_abstract()`, `extract_sections()`, `extract_see_also_links()`, `extract_categories()`, `extract_images()`, `extract_external_links()`, `is_disambiguation()`. Brace-matching `strip_templates()` for clean abstract extraction.

- **`infobox.rs`**: Brace-matching `{{Infobox ...}}` parser that handles nested `{{...}}` templates.

- **`stats.rs`**: Thread-safe atomic counters for extraction statistics. Supports checkpoint serialization.

- **`config.rs`**: Constants for both extraction (`REDIRECT_MAX_DEPTH`, `SHARD_COUNT`, `PROGRESS_INTERVAL`, `CACHE_VERSION`, `CHECKPOINT_VERSION`, `CHECKPOINT_INTERVAL`) and import (`DEFAULT_BOLT_URI`, `IMPORT_MAX_RETRIES`, `IMPORT_RETRY_DELAY_SECS`, `IMPORT_MAX_PARALLEL_EDGES`, `IMPORT_MAX_PARALLEL_LIGHT`, `DEFAULT_IMPORT_PREFIX`).

- **`cache.rs`**: Saves/loads `WikiIndex` to disk as `index.cache` using `bincode`. Validates against input file mtime and size. Zero-copy serialization via `IndexCacheSer` (borrows index data).

- **`checkpoint.rs`**: `CheckpointManager` with double-checked locking for periodic saves. Atomic write via `.tmp` + rename. Cleared on successful completion.

### Key Patterns

- **Iterator trait** on `WikiReader` for lazy streaming from XML
- **ShardedCsvWriter** distributes rows by `page_id % csv_shards` across N files with a single `shard_for()` call
- **Namespace filtering**: `<ns>` tag for page type; namespace-prefixed link targets excluded from article edges
- **Brace-matching parser** for infoboxes (not regex, due to nested templates)
- **Concurrent category dedup**: `DashSet<String>` for thread-safe first-seen tracking
- **Batch edge writing**: local collection before mutex-protected writes reduces lock contention
- **Atomic counters** in `ExtractionStats` avoid locking for statistics
- **Atomic file writes**: `.tmp` + rename for crash safety (cache, checkpoint)
- **Cache validation**: input file mtime + size compared against stored metadata
- **Resume filtering**: `reader.filter(|p| p.id > last_processed_id)` skips already-processed pages
- **Conditional serialization**: `#[serde(skip_serializing_if = "...", default)]` for compact JSON
- **Parallel decompression**: external `lbzip2`/`pbzip2` with `Drop`-based cleanup
- **Throttled parallel import**: `FuturesUnordered` with bounded concurrency; edges serialized (1 at a time) due to memory pressure, lighter operations at 4 concurrent
- **Neo4j transactional batching**: `CALL { ... } IN TRANSACTIONS OF N ROWS` for memory-bounded bulk loading
- **Tokio runtime isolation**: manually created only for the import path; extraction uses sync rayon

## Dependencies

Key crates: `quick-xml` (XML parsing), `bzip2` (decompression), `rayon` (parallelism), `clap` (CLI), `csv` (output), `serde`/`serde_json` (serialization), `regex` (link/content extraction), `once_cell` (lazy regex), `indicatif` (progress), `tracing`/`tracing-subscriber` (logging), `anyhow` (errors), `bincode` (cache/checkpoint serialization), `dashmap` (concurrent category deduplication), `neo4rs` (Bolt protocol driver for Neo4j), `tokio` (async runtime for import), `futures` (throttled parallel loading).
