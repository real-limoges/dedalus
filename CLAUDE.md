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

## Architecture

### Two-Pass Processing Pipeline

The system processes Wikipedia dumps in two passes:

1. **Indexing Pass** (`index.rs`): Builds title-to-ID mapping and redirect resolution table without reading article text
2. **Extraction Pass** (`extract.rs`): Uses the index to extract nodes, edges, and article content in parallel

### Core Modules

- **`main.rs`**: CLI entry point using `clap` derive macros. Orchestrates the two-pass pipeline, initializes `tracing` logging, and prints a summary of extraction statistics.

- **`parser.rs` - WikiReader**: Streaming XML parser implementing `Iterator<Item = WikiPage>`. Uses state machine pattern with `quick-xml` for memory-efficient event-based parsing of BZ2-compressed Wikipedia dumps. Supports two modes via `skip_text` flag (indexing pass skips article text).

- **`index.rs` - WikiIndex**: In-memory HashMap-based index for fast title-to-ID resolution. Follows redirect chains (max depth from `config::REDIRECT_MAX_DEPTH`). Uses `indicatif` progress bar during index building.

- **`extract.rs`**: Parallel extraction using `rayon::par_bridge()` with thread-safe CSV writers via `Arc<Mutex<Writer>>`. Outputs:
  - `nodes.csv` - Neo4J format: `id:ID`, `title`, `:LABEL`
  - `edges.csv` - Neo4J format: `:START_ID`, `:END_ID`, `:TYPE`
  - `blobs/{shard:03}/{id}.json` - Sharded article content (shard = id % shard_count)

- **`models.rs`**: Core types - `WikiPage`, `PageType` (Article/Redirect/Special), `ArticleBlob`

- **`stats.rs` - ExtractionStats**: Thread-safe atomic counters (`AtomicU64`) tracking articles processed, edges extracted, blobs written, and invalid links.

- **`config.rs`**: Constants - `REDIRECT_MAX_DEPTH` (5), `SHARD_COUNT` (1000), `PROGRESS_INTERVAL` (1000)

### Key Patterns

- **Iterator trait** on WikiReader enables lazy streaming from XML
- **Regex pattern** for extracting wiki links: `\[\[([^|\]]+?)(?:\|[^\]]+)?\]\]`
- **Special page filtering**: File:, Category:, Template: prefixes marked as `PageType::Special`
- **Batch edge writing**: Local collection before mutex-protected writes reduces lock contention
- **Atomic counters** in `ExtractionStats` avoid locking for statistics

## Project Status

Phases 1-3 complete (core pipeline, data extraction, CLI/observability). No tests yet -- Phase 4 (testing) is the next priority. See `docs/FUTURE_IMPROVEMENTS.md` for the full roadmap.

## Dependencies

Key crates: `quick-xml` (XML parsing), `bzip2` (decompression), `rayon` (parallelism), `clap` (CLI), `csv` (output), `serde`/`serde_json` (serialization), `regex` (link extraction), `once_cell` (lazy regex), `indicatif` (progress), `tracing`/`tracing-subscriber` (logging), `anyhow` (errors).
