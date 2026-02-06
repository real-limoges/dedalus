# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Dedalus is a Rust data processing pipeline that extracts and transforms Wikipedia dumps into structured data for Neo4J graph database ingestion. It processes compressed Wikipedia XML dumps (.bz2) and outputs nodes/edges as CSV files along with article content as sharded JSON blobs.

## Build Commands

```bash
cargo build --release          # Build optimized binary
cargo test --verbose           # Run tests
cargo fmt -- --check           # Check formatting
cargo clippy -- -D warnings    # Lint with strict warnings
```

## Running

```bash
./target/release/dedalus --input <path-to-wiki-dump.xml.bz2> --output <output-directory>
```

### CLI Flags

- `--input` / `-i` -- path to `.xml.bz2` Wikipedia dump (required)
- `--output` / `-o` -- output directory (required)
- `--shard-count` -- blob shard count (default: 1000)
- `--limit` -- cap pages processed (useful for testing)
- `--dry-run` -- skip file writes
- `--verbose` / `-v` -- verbosity (`-v` INFO, `-vv` DEBUG, `-vvv` TRACE; default WARN)
- `--resume` -- resume from last checkpoint if available
- `--no-cache` -- force rebuild of index cache
- `--checkpoint-interval` -- save checkpoint every N articles (default: 10000)
- `--clean` -- clear existing checkpoint and outputs before starting

## Architecture

### Two-Pass Processing Pipeline

The system processes Wikipedia dumps in two passes:

1. **Indexing Pass** (`index.rs`): Builds title-to-ID mapping and redirect resolution table without reading article text
2. **Extraction Pass** (`extract.rs`): Uses the index to extract nodes, edges, categories, images, external links, and enriched article content in parallel

### Core Modules

- **`main.rs`**: CLI entry point using `clap` derive macros. Orchestrates the two-pass pipeline, initializes `tracing` logging, and prints a summary of extraction statistics.

- **`parser.rs` - WikiReader**: Streaming XML parser implementing `Iterator<Item = WikiPage>`. Uses state machine pattern with `quick-xml` for memory-efficient event-based parsing of BZ2-compressed Wikipedia dumps. Supports two modes via `skip_text` flag (indexing pass skips article text). Parses `<ns>` (namespace number) and `<timestamp>` (revision timestamp) XML tags; uses `ns` for page type classification with title-prefix fallback.

- **`index.rs` - WikiIndex**: In-memory HashMap-based index for fast title-to-ID resolution. Follows redirect chains (max depth from `config::REDIRECT_MAX_DEPTH`). Uses `indicatif` progress bar during index building.

- **`extract.rs`**: Parallel extraction using `rayon::par_bridge()` with thread-safe CSV writers via `Arc<Mutex<Writer>>`. Uses `DashSet` for concurrent category deduplication. Filters namespace-prefixed links (Category:, File:, Template:, etc.) from article edges. Outputs:
  - `nodes.csv` - Neo4J format: `id:ID`, `title`, `:LABEL`
  - `edges.csv` - Neo4J format: `:START_ID`, `:END_ID`, `:TYPE` (LINKS_TO or SEE_ALSO)
  - `categories.csv` - Neo4J format: `id:ID(Category)`, `name`, `:LABEL`
  - `article_categories.csv` - Neo4J format: `:START_ID`, `:END_ID(Category)`, `:TYPE` (HAS_CATEGORY)
  - `images.csv` - `article_id`, `filename`
  - `external_links.csv` - `article_id`, `url`
  - `blobs/{shard:03}/{id}.json` - Enriched article content (shard = id % shard_count)

- **`models.rs`**: Core types - `WikiPage` (with `ns`, `timestamp` fields), `PageType` (Article/Redirect/Special), `ArticleBlob` (with `abstract_text`, `categories`, `infoboxes`, `sections`, `timestamp`, `is_disambiguation`)

- **`content.rs`**: Regex-based text extraction helpers - `extract_abstract()` (first paragraph with templates stripped), `extract_sections()`, `extract_see_also_links()`, `extract_categories()`, `extract_images()`, `extract_external_links()`, `is_disambiguation()`

- **`infobox.rs`**: Brace-matching `{{Infobox ...}}` parser. Handles nested `{{...}}` templates in values. Produces `Infobox` structs with `infobox_type` and `fields: Vec<(String, String)>`.

- **`stats.rs` - ExtractionStats**: Thread-safe atomic counters (`AtomicU64`) tracking articles processed, edges extracted, blobs written, invalid links, categories found, category edges, see-also edges, infoboxes extracted, images found, and external links found. Supports checkpoint serialization.

- **`config.rs`**: Constants - `REDIRECT_MAX_DEPTH` (5), `SHARD_COUNT` (1000), `PROGRESS_INTERVAL` (1000), `CACHE_VERSION` (1), `CHECKPOINT_VERSION` (2), `CHECKPOINT_INTERVAL` (10000)

- **`cache.rs` - Index Persistence**: Saves/loads the WikiIndex to disk as `index.cache` using `bincode` serialization. Validates cache against input file mtime and size to auto-invalidate when the dump changes.

- **`checkpoint.rs` - CheckpointManager**: Tracks extraction progress and saves checkpoints every N articles. Enables resumable processing via `--resume` flag. Checkpoints are cleared on successful completion.

### Key Patterns

- **Iterator trait** on WikiReader enables lazy streaming from XML
- **Regex pattern** for extracting wiki links: `\[\[([^|\]]+?)(?:\|[^\]]+)?\]\]`
- **Namespace filtering**: `<ns>` tag used for page type classification; namespace-prefixed link targets (Category:, File:, Template:, Wikipedia:, Help:, Portal:, Draft:, User:, Module:, MediaWiki:) excluded from article edges
- **Brace-matching parser** for infoboxes (not regex, due to nested `{{...}}` templates)
- **Concurrent category dedup**: `DashSet<String>` from `dashmap` for thread-safe first-seen tracking
- **Batch edge writing**: Local collection before mutex-protected writes reduces lock contention
- **Atomic counters** in `ExtractionStats` avoid locking for statistics
- **Atomic checkpoint writes**: Write to `.tmp` file then rename for crash safety
- **Cache validation**: Compare input file mtime + size against stored metadata to detect changes
- **Resume filtering**: `reader.filter(|p| p.id > last_processed_id)` skips already-processed pages
- **Conditional serialization**: `#[serde(skip_serializing_if = "...", default)]` on `ArticleBlob` fields for compact JSON and backward compatibility

## Project Status

Complete. Phases 1-6 implemented (core pipeline, data extraction, CLI/observability, testing, resumable processing, enriched extraction). Neo4j loading is done via `neo4j-admin database import` -- see `scripts/import-neo4j.sh`. See `docs/FUTURE_IMPROVEMENTS.md` for potential enhancements.

## Dependencies

Key crates: `quick-xml` (XML parsing), `bzip2` (decompression), `rayon` (parallelism), `clap` (CLI), `csv` (output), `serde`/`serde_json` (serialization), `regex` (link/content extraction), `once_cell` (lazy regex), `indicatif` (progress), `tracing`/`tracing-subscriber` (logging), `anyhow` (errors), `bincode` (cache/checkpoint serialization), `dashmap` (concurrent category deduplication).
