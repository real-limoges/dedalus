# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Dedalus is a Rust pipeline that extracts Wikipedia dumps into structured graph data and imports it into Neo4j. It processes compressed Wikipedia XML dumps (.bz2), outputs nodes/edges as CSV files along with article content as sharded JSON blobs, and loads everything into Neo4j. The project emphasizes performance optimizations including multistream parallel parsing, parallel extraction, CSV sharding, and bulk import via neo4j-admin. When using Wikipedia's multistream dump format with the accompanying index file, both indexing and extraction passes decompress and parse bz2 streams in parallel across all CPU cores.

## Build & Optimization

The project targets M1 CPU with native SIMD optimizations via `.cargo/config.toml`:

```toml
# .cargo/config.toml
[build]
rustflags = ["-C", "target-cpu=native"]

[profile.release]
opt-level = 3
codegen-units = 1
strip = true
```

Additional release profile settings in `Cargo.toml`:

```toml
[profile.release]
opt-level = 3
codegen-units = 1
lto = true
panic = 'abort'
overflow-checks = false
```

This yields ~1.6x faster extraction on ARM64 due to NEON SIMD and single-pass codegen. Always build with `cargo build --release` for production.

### Build Commands

```bash
cargo build --release          # Build optimized binary with M1 CPU targeting
cargo test --verbose           # Run all tests (161 unit + integration tests)
cargo fmt -- --check           # Check formatting
cargo clippy -- -D warnings    # Lint with strict warnings
```

The binary uses `mimalloc` as the global allocator for better multi-core performance on systems with many threads.

## Running

The binary uses subcommands: `extract`, `import`, `merge-csvs`, `pipeline`, `stats`, and `tui`.

### Pipeline (Recommended)

Runs the full workflow in one command: extract → merge (if shards > 1) → archive shards → import.

```bash
dedalus pipeline -i <path-to-dump.xml.bz2> -o <output-directory>
```

**Pipeline flags:**
- `-i` / `--input` -- path to `.xml.bz2` Wikipedia dump (required)
- `-o` / `--output` -- output directory (required)
- `--csv-shards` -- number of CSV output shards (default: 8)
- `--shard-count` -- JSON blob shard count (default: 1000)
- `--limit` -- cap pages processed (useful for testing)
- `--resume` -- resume from last checkpoint
- `--no-cache` -- force rebuild of index cache
- `--checkpoint-interval` -- save checkpoint every N articles (default: 10000)
- `--clean` -- clear existing outputs and Neo4j data before starting
- `--bolt-uri` -- Neo4j Bolt URI (default: `bolt://localhost:7687`)
- `--import-prefix` -- import file URI prefix (default: `file://`)
- `--max-parallel-edges` -- max concurrent edge import jobs (default: 4)
- `--max-parallel-light` -- max concurrent light import jobs (default: 8)
- `--compose-file` -- Docker compose file path (auto-detected)
- `--no-docker` -- connect to already-running Neo4j
- `--no-import` -- skip import step (extract + merge only)
- `--no-archive` -- don't archive sharded CSVs after merging
- `--multistream-index` -- path to multistream index file (`.txt.bz2`) for parallel parsing (auto-detected from dump filename)

Pipeline always uses `--admin-import` mode (10-100x faster). For Bolt-based import, use the individual `import` subcommand.

### Extract

Processes a Wikipedia dump into CSV/JSON output files. Supports optional CSV sharding for parallel extraction (1.62x speedup with 8 shards).

```bash
dedalus extract -i <path-to-wiki-dump.xml.bz2> -o <output-directory>
```

**Extract flags:**
- `-i` / `--input` -- path to `.xml.bz2` Wikipedia dump (required)
- `-o` / `--output` -- output directory (required)
- `--shard-count` -- JSON blob shard count (default: 1000)
- `--csv-shards` -- number of CSV output shards for parallelism (default: 8; set to 1 for single file output)
- `--limit` -- cap pages processed (useful for testing)
- `--dry-run` -- skip file writes, validate pipeline only
- `--resume` -- resume from last checkpoint if available
- `--no-cache` -- force rebuild of index cache (useful if dump changes)
- `--checkpoint-interval` -- save checkpoint every N articles (default: 10000)
- `--clean` -- clear existing checkpoint and outputs before starting
- `--multistream-index` -- path to multistream index file (`.txt.bz2`) for parallel parsing (auto-detected from dump filename)

### Import

Loads extracted CSV files into Neo4j. Supports two import modes: (1) `--admin-import` for bulk loading (10-100x faster), or (2) default Bolt mode for incremental loading. Manages Docker lifecycle automatically.

```bash
dedalus import -o <output-directory>
```

**Import flags:**
- `-o` / `--output` -- directory containing Dedalus CSV output files (required)
- `--bolt-uri` -- Neo4j Bolt URI (default: `bolt://localhost:7687`)
- `--import-prefix` -- import file URI prefix for Neo4j LOAD CSV (default: `file://`)
- `--max-parallel-edges` -- max concurrent LOAD CSV jobs for edges (default: 4)
- `--max-parallel-light` -- max concurrent LOAD CSV jobs for lighter relationships (default: 8)
- `--compose-file` -- Docker compose file path (auto-detected if not specified)
- `--no-docker` -- skip Docker management, connect to an already-running Neo4j
- `--clean` -- tear down existing Neo4j volumes before importing
- `--admin-import` -- use neo4j-admin bulk import (10-100x faster, requires empty DB)

**Import modes:**
- `--admin-import`: Uses neo4j-admin database import tool. Fastest option (10-100x faster than Bolt). Requires non-sharded CSVs (single files). Use `dedalus merge-csvs` first if you extracted with `--csv-shards > 1`. Requires empty Neo4j database.
- Default (Bolt): Uses LOAD CSV via Bolt protocol and `FuturesUnordered` throttled parallelism. Works with sharded CSVs and existing data. Slower but more flexible.

### Merge CSVs

Merges sharded CSV files (from `--csv-shards > 1` extraction) into single files for `--admin-import`. Performs deduplication of categories, images, and external links across shards using streaming I/O (256KB buffers).

```bash
dedalus merge-csvs -o <output-directory>
```

**Merge CSVs flags:**
- `-o` / `--output` -- directory containing sharded CSVs (e.g., `nodes_000.csv`, `nodes_001.csv`)
- `--archive` -- archive sharded CSVs to `output/shards/` after merging (preserves originals while keeping only merged files in the main output directory)

### Global flags

- `-v` / `--verbose` -- increase verbosity (`-v` INFO, `-vv` DEBUG, `-vvv` TRACE; default WARN)

### Stats

Shows output directory statistics: CSV file sizes, blob counts, and total disk usage.

```bash
dedalus stats -o <output-directory>
```

**Stats flags:**
- `-o` / `--output` -- output directory to inspect (default: `output`)

### Typical Workflows

```bash
# Recommended: Full pipeline (extract 8 shards → merge → admin import)
dedalus pipeline -i enwiki-latest-pages-articles.xml.bz2 -o out/ -v

---

# Test pipeline with limited pages
dedalus pipeline -i small-dump.xml.bz2 -o out/ --limit 10000 -vv

---

# Extract + merge only (no Neo4j)
dedalus pipeline -i enwiki-latest-pages-articles.xml.bz2 -o out/ --no-import -v

---

# Single shard pipeline (simpler, slower extraction)
dedalus pipeline -i enwiki-latest-pages-articles.xml.bz2 -o out/ --csv-shards 1 -v

---

# Clean slate (clear outputs + Neo4j data)
dedalus pipeline -i enwiki-latest-pages-articles.xml.bz2 -o out/ --clean -v

---

# Resume interrupted extraction
dedalus extract -i enwiki-latest-pages-articles.xml.bz2 -o out/ --resume -v

---

# Bolt-based import (slower, for incremental updates)
dedalus extract -i enwiki-latest-pages-articles.xml.bz2 -o out/ --csv-shards 8 -v
dedalus import -o out/  # omit --admin-import to use Bolt

---

# Import into already-running Neo4j (no Docker)
dedalus import -o out/ --no-docker --bolt-uri bolt://my-neo4j:7687

---

# Check output directory statistics
dedalus stats -o out/

---

# Multistream parallel parsing (auto-detects index from dump filename)
dedalus pipeline -i enwiki-latest-pages-articles-multistream.xml.bz2 -o out/ -v

---

# Multistream with explicit index path
dedalus extract -i dump-multistream.xml.bz2 -o out/ \
  --multistream-index dump-multistream-index.txt.bz2 -v
```

## Architecture

### Two-Pass Extraction Pipeline

1. **Indexing Pass** (`index.rs`): Builds title-to-ID mapping (FxHashMap, pre-sized for 8M articles) and redirect resolution table without reading article text. Uses `skip_text` parser mode for speed. With multistream dumps, `build_multistream()` decompresses and parses bz2 streams in parallel via `rayon`, then merges results into a single index.

2. **Extraction Pass** (`extract.rs`): Uses the index to extract nodes, edges, categories, images, external links, and enriched article content in parallel via `rayon::par_bridge()`. `ShardedCsvWriter` distributes rows across N files by `page_id % csv_shards`. `DashSet` deduplicates categories, images, and external links concurrently. With multistream dumps, uses `multistream::par_iter_pages()` to parallelize both decompression and XML parsing across bz2 streams.

3. **Merge Pass** (`merge.rs`, optional): If `--csv-shards > 1`, use `dedalus merge-csvs` to combine shards into single files with cross-shard deduplication for `--admin-import` compatibility.

4. **Import Pass** (`import.rs`): Two modes: (1) `--admin-import` uses `neo4j-admin database import` for bulk loading; (2) default Bolt mode uses `neo4rs` driver with `LOAD CSV` and throttled `FuturesUnordered` parallelism.

### Core Modules

- **`main.rs`**: CLI entry point using `clap` subcommands. Initializes `tracing` logging with configurable verbosity. Uses `mimalloc` global allocator for better performance. Manually creates `tokio` runtime only for import path; extraction uses sync/rayon.

- **`parser.rs`**: `PageParser<R>` -- generic streaming XML parser implementing `Iterator<Item = WikiPage>` over any `Read` source. State machine over `quick-xml` events for memory-efficient parsing. `WikiReader` wraps `PageParser` with BZ2 decompression, probing PATH for `lbzip2`/`pbzip2` for parallel decompression (256KB BufReader); falls back to in-process `MultiBzDecoder`. `Drop` cleans up child processes. `skip_text` flag enables lightweight indexing mode.

- **`multistream.rs`**: Multistream dump support. Parses the bz2-compressed index file (`*-multistream-index.txt.bz2`) to extract `StreamRange` byte offsets for each independent bz2 stream in the dump. `par_iter_pages()` creates a `rayon` parallel iterator where each worker independently seeks, decompresses (`BzDecoder`), and parses its stream. `detect_index_path()` auto-detects the index file from the dump filename using Wikipedia's naming convention.

- **`index.rs`**: `FxHashMap`-based title-to-ID index (faster than SipHash for trusted input). Follows redirect chains up to `REDIRECT_MAX_DEPTH` (5 hops). Uses `indicatif` progress spinner during building. `build_multistream()` builds the index in parallel using `multistream::par_iter_pages()` with `skip_text=true`.

- **`extract.rs`**: Parallel extraction via `rayon::par_bridge()`. `ShardedCsvWriter` distributes CSV rows by `page_id % csv_shards` across N files. Pre-creates shard directories once (not per-article). Uses `DashSet` for concurrent deduplication of categories, images, and external links. Batches category writes (collect locally, lock once) to reduce contention. Outputs:
  - `nodes[_NNN].csv` -- `id:ID`, `title`, `:LABEL`
  - `edges[_NNN].csv` -- `:START_ID`, `:END_ID`, `:TYPE` (LINKS_TO or SEE_ALSO)
  - `categories[_NNN].csv` -- `id:ID(Category)`, `name`, `:LABEL` (deduplicated)
  - `article_categories[_NNN].csv` -- `:START_ID`, `:END_ID(Category)`, `:TYPE` (HAS_CATEGORY)
  - `image_nodes[_NNN].csv` -- `id:ID(Image)`, `filename`, `:LABEL` (deduplicated)
  - `article_images[_NNN].csv` -- `:START_ID`, `:END_ID(Image)`, `:TYPE` (HAS_IMAGE)
  - `external_link_nodes[_NNN].csv` -- `id:ID(ExternalLink)`, `url`, `:LABEL` (deduplicated)
  - `article_external_links[_NNN].csv` -- `:START_ID`, `:END_ID(ExternalLink)`, `:TYPE` (HAS_LINK)
  - `blobs/{shard:03}/{id}.json` -- enriched article content

- **`import.rs`**: Neo4j import pipeline with two modes. (1) `--admin-import` uses `neo4j-admin database import` for 10-100x faster bulk loading. (2) Default Bolt mode uses `neo4rs` driver with `LOAD CSV` and throttled parallelism via `FuturesUnordered`. Both modes load all CSV types. Detects CSV layout (single vs sharded). Manages Docker via `tokio::process::Command`. Connects with retry (30 attempts, 2s delay). **Critical**: Creates indexes BEFORE `LOAD CSV` with `MERGE` to prevent O(n²) full label scans.

- **`merge.rs`**: CSV shard merger for neo4j-admin compatibility. Detects shard count from `nodes_*.csv` files. Concatenates all CSV types with streaming I/O (256KB buffers). Deduplicates categories, images, and external links using `FxHashSet` to handle cross-shard duplicates. Outputs single merged files ready for `--admin-import`. `archive_shards()` moves `*_NNN.csv` files to `output/shards/` after merging to keep the output directory clean.

- **`models.rs`**: Core types -- `WikiPage`, `PageType` (Article/Redirect/Special), `ArticleBlob` with conditional serialization for compact JSON.

- **`content.rs`**: Regex-based text extraction -- `extract_abstract()` (direct string building, not collect+join), `extract_sections()`, `extract_see_also_links()`, `extract_categories()`, `extract_images()`, `extract_external_links()`, `is_disambiguation()`. Brace-matching `strip_templates()` for clean abstract extraction. Single-pass regex via `captures_iter()` (not `find_iter()` + `captures()`).

- **`infobox.rs`**: Brace-matching `{{Infobox ...}}` parser (not regex) that correctly handles nested `{{...}}` templates and extracts structured key-value data.

- **`stats.rs`**: `ExtractionStats` -- thread-safe atomic counters for extraction statistics. Avoids locking for performance. Supports checkpoint serialization.

- **`config.rs`**: Constants for both extraction and import:
  - Extraction: `REDIRECT_MAX_DEPTH` (5), `SHARD_COUNT` (1000), `PROGRESS_INTERVAL` (1000), `CACHE_VERSION` (2), `CHECKPOINT_VERSION` (3), `CHECKPOINT_INTERVAL` (10000)
  - Import: `DEFAULT_BOLT_URI`, `IMPORT_MAX_RETRIES` (30), `IMPORT_RETRY_DELAY_SECS` (2), `IMPORT_MAX_PARALLEL_EDGES` (4), `IMPORT_MAX_PARALLEL_LIGHT` (8), `DEFAULT_IMPORT_PREFIX` (file://)

- **`cache.rs`**: Index persistence using `bincode`. Saves/loads `WikiIndex` as `index.cache`. Validates against input file mtime and size. Zero-copy serialization via `IndexCacheSer` (borrows FxHashMaps instead of cloning). Single-pass deserialization.

- **`checkpoint.rs`**: `CheckpointManager` with double-checked locking for periodic checkpoint saves. Atomic write via `.tmp` + rename for crash safety. Cleared on successful completion.

- **`tui/`**: Interactive terminal UI (`ratatui` + `crossterm`). `mod.rs` sets up tracing capture and the alternate-screen event loop. `app.rs` defines `App` state, per-operation config structs, field enums, and validation. `event.rs` polls `crossterm` on a background thread via `mpsc`. `logging.rs` implements a `tracing::Layer` that pushes formatted lines into a shared `VecDeque`. `runner.rs` spawns worker threads for extract/import/merge with shared `Arc<AtomicBool>` completion signals. `ui.rs` renders config forms, real-time stats panels, scrollable logs, and done summaries.

### Key Performance Patterns

- **Iterator trait** on `WikiReader` for lazy streaming (never loads full dump in memory)
- **ShardedCsvWriter** distributes rows by `page_id % csv_shards` across N files with single `shard_for()` call
- **Pre-sized FxHashMap** (8M articles, 10M redirects) vs dynamic growth
- **Namespace filtering**: `<ns>` tag for page type; namespace-prefixed link targets excluded from article edges
- **Brace-matching parser** for infoboxes (not regex, due to nested templates)
- **Concurrent category dedup**: `DashSet<String>` with `contains()` check before clone+insert
- **Batch edge writing**: local edge collection before single mutex-protected write (reduces lock contention)
- **Atomic counters** in `ExtractionStats` (avoids locking for high-frequency stats)
- **Atomic file writes**: `.tmp` + rename for crash safety (cache, checkpoint)
- **Cache validation**: input file mtime + size compared against stored metadata
- **Resume filtering**: `reader.filter(|p| p.id > last_processed_id)` skips already-processed pages
- **Conditional serialization**: `#[serde(skip_serializing_if = "...", default)]` for compact JSON blobs
- **Parallel decompression**: external `lbzip2`/`pbzip2` (with `pbzip2` fallback) and `Drop`-based cleanup
- **Multistream parallel parsing**: with multistream dumps, each rayon worker independently seeks to a bz2 stream offset, decompresses with `BzDecoder`, wraps in synthetic `<mediawiki>` tags, and parses XML -- true parallelism in both decompression and parsing
- **Throttled parallel import**: `FuturesUnordered` with bounded concurrency; edges at 4 concurrent, lighter operations at 8 concurrent
- **Neo4j transactional batching**: `CALL { ... } IN TRANSACTIONS OF N ROWS` for memory-bounded bulk loading
- **Tokio runtime isolation**: manually created only for import path; extraction uses sync rayon
- **M1 CPU targeting**: `target-cpu=native` for NEON SIMD, `codegen-units=1` for better optimization
- **String allocations**: single-pass building in `sanitize_field()` and `extract_abstract()` (vs replace→split→collect→join chains)
- **BufWriter**: 128KB buffers (increased from 64KB) for CSV writers, 256KB for merge operations
- **JSON output**: `to_writer()` not `to_writer_pretty()`, with `BufWriter` for efficiency

### CSV Sharding & Merge Trade-off

The hybrid workflow solves a performance trade-off:
- **Pure extraction with `--csv-shards 8`**: 1.62x speedup but produces 8 files per CSV type
- **neo4j-admin import**: Requires single files (10-100x faster than Bolt but needs merged CSVs)
- **Solution**: `dedalus merge-csvs` merges shards with cross-shard deduplication in <5 minutes

Recommended for full Wikipedia dumps:
```
Extract (8 shards, 1.62x faster) → Merge (<5 min, dedup) → Admin Import (10-100x faster)
```

## Dependencies

Key crates and their roles:
- `quick-xml` -- streaming XML parsing (state machine, never full dump in memory)
- `bzip2` -- BZ2 decompression fallback (uses external `lbzip2`/`pbzip2` when available)
- `rayon` -- data parallelism for extraction (par_bridge)
- `clap` -- CLI with subcommands
- `csv` -- CSV writing with multiple files per type
- `serde` / `serde_json` -- serialization, conditional field skipping for compact blobs
- `regex` / `once_cell` -- lazy regex compilation for link/content extraction
- `indicatif` -- progress spinners during indexing
- `tracing` / `tracing-subscriber` -- structured logging with configurable verbosity
- `anyhow` -- error handling with context
- `bincode` -- cache/checkpoint serialization (zero-copy via IndexCacheSer)
- `dashmap` -- concurrent category deduplication (DashSet)
- `rustc-hash` -- FxHashMap/FxHashSet for faster hashing (trusted input, no DoS risk)
- `neo4rs` -- Neo4j Bolt protocol driver
- `tokio` -- async runtime for import operations (manually created, not for extraction)
- `futures` -- FuturesUnordered for throttled parallel operations
- `mimalloc` -- global allocator for better performance on multi-core systems

## Testing

All tests must pass before committing:

```bash
cargo test --verbose           # Run all tests (161 unit + integration)
cargo clippy -- -D warnings    # Lint with strict warnings
cargo fmt -- --check           # Check formatting
```

Test suites:
- **Unit tests**: Inline in modules (parser, index, extract, merge, content, infobox)
- **Integration tests**: `tests/integration_test.rs` (end-to-end extract + merge)
- **Merge tests**: `tests/test_merge_csvs.rs` (CSV merging with deduplication)

## Recent Optimizations (2026-02-09)

- M1 CPU targeting via `.cargo/config.toml` (`target-cpu=native`, `codegen-units=1`, `opt-level=3`)
- String allocation optimizations in `sanitize_field()` (single-pass build vs replace→split→collect→join)
- Direct string building in `extract_abstract()` (vs collect+join)
- Increased CSV writer buffer size to 128KB (was 64KB)
- Pre-create shard directories once (not per-article)
- Cache: single-pass deserialization (was deserializing twice)
- Cache: zero-copy serialization via `IndexCacheSer` (borrows FxHashMaps, no clone)
- Single-pass regex via `captures_iter()` (not `find_iter()` + `captures()`)
- Batched category writes (collect locally, lock once)
- `contains()` check before clone+insert in DashSet
- Page title/timestamp moved into blob instead of cloned
- Edge tuples store only (target_id, edge_type), not source_id per edge
- Consolidated duplicate LINK_REGEX into content.rs
- CSV merging with 256KB streaming buffers and `FxHashSet` deduplication
- MiMalloc global allocator for better multi-core performance

## Architecture Decisions

### Why two-pass pipeline?

Single-pass requires holding the full title-to-ID mapping in memory while parsing text. Two-pass allows building the index first (faster, less memory), then processing with redirect resolution in the extraction pass.

### Why FxHashMap vs SipHash?

FxHash is faster than SipHash for trusted input (Wikipedia dump) and we don't care about DoS resistance for internal data structures.

### Why csv-shards with merge?

CSV sharding provides 1.62x extraction speedup on multi-core systems, but `neo4j-admin import` requires single files. Merge with deduplication solves this in <5 minutes and enables the fastest overall pipeline.

### Neo4j: indexes BEFORE LOAD CSV

**Critical lesson**: Always create indexes BEFORE `LOAD CSV` with `MERGE`. Without indexes, `MERGE` performs a full label scan per row, resulting in O(n²) performance. `neo4j-admin import` doesn't need pre-existing indexes (builds its own), but Bolt-based `LOAD CSV` absolutely does. The `import.rs` creates indexes before any loading.

### Why throttle parallel edge jobs?

Memory pressure: each concurrent edge job buffers all its rows in memory before committing. Edges are the largest CSV type. Default of 4 concurrent edge jobs balances throughput vs memory usage; lighter operations use 8 concurrent. Reduce `--max-parallel-edges` if OOM occurs.

### Why multistream parallel parsing?

Wikipedia distributes dumps in two formats: standard (single bz2 stream) and multistream (concatenated independent bz2 streams with an index file). Standard dumps require sequential decompression -- even with `lbzip2`, the single-stream nature limits parallelism. Multistream dumps contain ~200K independent bz2 streams, each covering ~100 pages. By parsing the index file to learn stream byte offsets, each rayon worker can independently `seek()` + `BzDecoder` its own stream, achieving true parallelism in both decompression and XML parsing. The index file is auto-detected from the dump filename (`*-multistream.xml.bz2` → `*-multistream-index.txt.bz2`) or specified explicitly via `--multistream-index`.

### Why atomic counters for stats?

Stats are updated at high frequency (per-article). Atomic operations avoid locking overhead.

## Troubleshooting

- **OOM during import**: Reduce `--max-parallel-edges` or `--max-parallel-light`. Use `--admin-import` for fastest memory-efficient loading.
- **Slow extraction**: Ensure `cargo build --release` with M1 targeting. Set `--csv-shards 8` for 1.62x speedup. Use multistream dumps (`*-multistream.xml.bz2` + index) for parallel decompression.
- **Index cache invalid**: Use `--no-cache` to rebuild. Cache validates against input file mtime and size.
- **Checkpoint conflicts**: Use `--clean` to start fresh or `--resume` to continue.
- **Neo4j connection timeout**: Increase `IMPORT_MAX_RETRIES` or check Docker logs with `docker compose -f neo4j-platform/docker-compose.yml logs neo4j`.
