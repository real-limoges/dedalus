# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Dedalus is a Rust pipeline that turns a compressed Wikipedia XML dump (`.xml.bz2`) into graph data.
It writes nodes, edges, categories, images and external links as CSV files and article content as sharded JSON blobs, computes PageRank, label-propagation communities and degree into `analytics.csv`, and loads all of it into a single DuckDB file.
With a multistream dump and its index file, both the indexing and extraction passes decompress and parse bz2 streams in parallel across every core.
`docs/reference/internal/architecture.md` is the one-page map of the passes and the files they write.

## Build & Optimization

The project targets Apple Silicon.
`.cargo/config.toml` sets `target-cpu=native` (NEON SIMD) and points the DuckDB build at Homebrew:

```toml
[build]
rustflags = ["-C", "target-cpu=native"]

[env]
DUCKDB_LIB_DIR = "/opt/homebrew/opt/duckdb/lib"
DUCKDB_INCLUDE_DIR = "/opt/homebrew/opt/duckdb/include"
```

The release profile lives only in `Cargo.toml`:

```toml
[profile.release]
opt-level = 3
codegen-units = 1
lto = true
panic = 'abort'
overflow-checks = false
strip = true
```

`panic = 'abort'` means a panic in any worker ends the whole run, not one page.
Always build with `cargo build --release` for real runs.

### DuckDB Linking

The `duckdb` crate links Homebrew's `libduckdb.dylib` (`brew install duckdb`), which keeps builds fast.
The crate version must match the library: crate `1.20000.x` wraps DuckDB 2.0.x, so a `brew upgrade` to a new DuckDB minor version needs a matching crate bump.
CI's Ubuntu runners have no Homebrew, so each job downloads DuckDB's prebuilt `duckdb-shared-libs-linux-amd64.tar.gz` from its GitHub release and sets `DUCKDB_LIB_DIR`, `DUCKDB_INCLUDE_DIR` and `LD_LIBRARY_PATH`, which override `[env]`.
`DUCKDB_VERSION` in `.github/workflows/ci.yml` must move with the crate.
Nothing compiles DuckDB from source.

### Build Commands

```bash
cargo build --release                       # Optimized binary
cargo test                                  # Unit and integration tests
cargo clippy --all-targets -- -D warnings   # Lint, tests included
cargo fmt -- --check                        # Formatting (rustfmt edition 2024)
git config core.hooksPath .githooks         # Pre-push hook runs all of the above
```

The binary uses `mimalloc` as the global allocator.

## Running

Subcommands: `extract`, `merge-csvs`, `analytics`, `load`, `pipeline`, `stats`, and `tui`.

### Pipeline (Recommended)

Runs extract, then merge (when `--csv-shards > 1`) and archive, then analytics, then the DuckDB load.

```bash
dedalus pipeline -i <path-to-dump.xml.bz2> -o <output-directory>
```

**Pipeline flags:**
- `-i` / `--input`: path to the `.xml.bz2` dump (required)
- `-o` / `--output`: output directory (required)
- `--csv-shards`: CSV output shards (default 8, at least 1)
- `--shard-count`: JSON blob shard directories (default 1000, at least 1)
- `--limit`: stop after this many pages (exact, and stops reading the dump)
- `--checkpoint-interval`: pages per extraction chunk; a checkpoint is saved after each (default 10000)
- `--resume`: resume extraction from the last checkpoint
- `--no-cache`: rebuild the index instead of loading `index.cache`
- `--clean`: clear existing checkpoint and outputs first
- `--no-archive`: leave shard CSVs in place after merging
- `--no-analytics`: skip analytics
- `--no-load`: skip the DuckDB load
- `--db-path`: DuckDB file, relative to the output directory (default `wikipedia.duckdb`); always replaced by `pipeline`
- `--multistream-index`: multistream index file (auto-detected from the dump's name)

### Extract

```bash
dedalus extract -i <path-to-dump.xml.bz2> -o <output-directory>
```

Takes the extraction flags above (`--csv-shards`, `--shard-count`, `--limit`, `--checkpoint-interval`, `--resume`, `--no-cache`, `--clean`, `--multistream-index`) plus `--dry-run`, which writes nothing.

### Merge CSVs

```bash
dedalus merge-csvs -o <output-directory> [--archive]
```

Merges `nodes_000.csv`, `nodes_001.csv`, ... into `nodes.csv` for every CSV type, deduplicating category, image and external link nodes across shards.
Shards must be numbered `0..N`; every shard and header is checked before anything is written; each merged file is written to `.tmp` and renamed.
`--archive` moves the shards into `output/shards/`, replacing shards archived earlier.

### Analytics

```bash
dedalus analytics -o <output-directory> [--pagerank-iterations 20] [--damping 0.85]
```

Needs merged CSVs.
Builds a CSR graph from LINKS_TO edges only, computes PageRank, label propagation and in plus out degree, and writes `analytics.csv` (`id:ID`, `pagerank:double`, `community:long`, `degree:int`).

### Load

```bash
dedalus load -o <output-directory> [--db-path wikipedia.duckdb] [--clean]
```

Needs merged CSVs.
Creates one DuckDB table per CSV (`articles`, `links`, `categories`, `article_categories`, `images`, `article_images`, `external_links`, `article_external_links`) and `analytics` when `analytics.csv` exists.
Refuses to replace an existing database without `--clean`.

### Stats and TUI

`dedalus stats -o <dir>` shows CSV sizes, blob counts, the DuckDB file size and total disk usage.
`dedalus tui` opens a form per operation (Extract, MergeCsvs, Analytics, Load) with live stats and a scrolling log.

### Global Flags

- `-v` / `--verbose`: `-v` INFO, `-vv` DEBUG, `-vvv` TRACE (default WARN)

### Typical Workflows

```bash
# Full pipeline
dedalus pipeline -i enwiki-latest-pages-articles-multistream.xml.bz2 -o out/ -v

# Small test run
dedalus pipeline -i enwiki-latest-pages-articles-multistream.xml.bz2 -o out/ --limit 10000 -v

# CSVs only
dedalus pipeline -i enwiki-latest-pages-articles-multistream.xml.bz2 -o out/ --no-load -v

# Step by step
dedalus extract -i enwiki-latest-pages-articles-multistream.xml.bz2 -o out/ --csv-shards 14 -v
dedalus merge-csvs -o out/ --archive
dedalus analytics -o out/
dedalus load -o out/ --clean

# Resume an interrupted extraction
dedalus extract -i enwiki-latest-pages-articles-multistream.xml.bz2 -o out/ --resume -v
```

## Architecture

### Multi-Pass Pipeline

1. **Index** (`index.rs`): builds the title-to-id map (`FxHashMap`, pre-sized for 8M articles) and the redirect table without reading article text (`skip_text`).
   With a multistream dump, `build_multistream()` parses streams in parallel and merges the results.
2. **Extract** (`extract.rs`): reads the dump again in ordered chunks of `--checkpoint-interval` pages and processes each chunk's pages in parallel with rayon; reading the next chunk overlaps with processing the current one.
   After each chunk every writer is flushed and a checkpoint is saved.
3. **Merge** (`merge.rs`): when sharded, combines shards with cross-shard deduplication.
4. **Analytics** (`analytics.rs`): CSR graph, PageRank, label propagation, degree, written to `analytics.csv`.
5. **Load** (`load.rs`): copies the merged CSVs and `analytics.csv` into DuckDB.

The CSVs are the hand-off between passes; the DuckDB file is a queryable copy of them.

### Core Modules

- **`main.rs`**: CLI entry point (`clap` subcommands), `tracing` setup, `mimalloc` allocator.
- **`parser.rs`**: `PageParser<R>`, a streaming `quick-xml` state machine implementing `Iterator<Item = WikiPage>`; per-page state is reset on every `<page>`, and a parse error is recorded rather than ending the stream silently.
  `WikiReader` wraps it with bz2 decompression, using `lbzip2` or `pbzip2` from `PATH` when present; `WikiReader::finish` fails on a parser error or a nonzero decompressor exit, and kills the child if reading stopped early.
- **`multistream.rs`**: parses the multistream index into `StreamRange`s (offsets must increase and lie inside the dump), parses one stream (`parse_stream`) or a run of streams in dump order (`parse_streams`), and finds the index from the dump's name (`detect_index_path`).
- **`title.rs`**: `normalize_title` (MediaWiki canonical form: drop the `#anchor` and a leading `:`, `_` to space, collapse whitespace, uppercase the first character) and `is_namespace_link` (case-insensitive).
  The index stores and looks up titles in this form.
- **`index.rs`**: `WikiIndex`; `resolve_id` normalizes the title and follows redirects up to `REDIRECT_MAX_DEPTH` (5) hops.
- **`extract.rs`**: `ShardedCsvWriter` (rows to `page_id % csv_shards`), `CsvOutputs`, the `Extractor` that turns one page into rows and a blob, `PageSource` (sequential or multistream chunks), `run_extraction_with_stats` (returns `ExtractionEnd::Completed` or `Cancelled`), and `run_extract_job`, the shared entry point for the CLI and the TUI.
  Outputs `nodes`, `edges` (LINKS_TO or SEE_ALSO), `categories`, `article_categories`, `image_nodes`, `article_images`, `external_link_nodes`, `article_external_links` (each `[_NNN].csv`) and `blobs/{shard:03}/{id}.json`.
  A fresh run removes CSVs from an earlier run that it will not overwrite.
- **`checkpoint.rs`**: `Checkpoint` holds the `RunIdentity` (input path, mtime, size, output dir, shard counts, reading mode), a `ResumePoint` (`AfterPages` for sequential dumps, `AfterStreams` for multistream), pages read, each CSV's byte length, and stats.
  On resume each CSV is truncated back to its recorded length.
- **`atomic_file.rs`**: `write_atomically` writes `<path>.tmp`, flushes, syncs and renames.
- **`csv_util.rs`**: `CsvType` (names, headers, `shard_index`, `is_shared_node`), `detect_csv_layout` (prefers merged `nodes.csv`), `count_shards` (requires `0..N`), `stale_csv_files`.
- **`merge.rs`**: `merge_csv_shards(dir, n)`, `merge_detected_shards(dir)`, `archive_shards(dir)`.
- **`analytics.rs`**: two-pass CSR build over LINKS_TO edges (duplicate node ids and edges to unknown pages are skipped and counted), PageRank in f64 with the rank of pages without outgoing links spread evenly, label propagation with smallest-label tie-breaking, degree, `analytics.csv`.
- **`load.rs`**: DuckDB `read_csv` per table with explicit column types; built in `<db>.tmp` and renamed.
- **`content.rs`**: regex and bracket-matching wikitext extraction: `extract_abstract` (lead section as plain text), `extract_sections`, `see_also_section_range` (ends at the next level-2 heading), `extract_categories`, `extract_images`, `extract_external_links`, `is_disambiguation`, `strip_templates`.
- **`infobox.rs`**: brace-matching `{{Infobox ...}}` parser; fields split only at template and link depth zero.
- **`models.rs`**: `WikiPage`, `PageType`, `EdgeType`, `ArticleBlob` (empty fields skipped in JSON).
- **`stats.rs`**: `ExtractionStats`, atomic counters with checkpoint round-tripping.
- **`cache.rs`**: `index.cache` via `bincode`, validated against the input's mtime and size and `CACHE_VERSION`.
- **`config.rs`**: constants: `REDIRECT_MAX_DEPTH` (5), `SHARD_COUNT` (1000), `DEFAULT_CSV_SHARDS` (8), `CHECKPOINT_INTERVAL` (10000), `PAGES_PER_STREAM` (100), `CACHE_VERSION` (3), `CHECKPOINT_VERSION` (4), buffer sizes, `PAGERANK_ITERATIONS` (20), `PAGERANK_DAMPING` (0.85), `PAGERANK_EPSILON` (1e-6), `LABEL_PROPAGATION_MAX_ITERATIONS` (50).
- **`tui/`**: `ratatui` + `crossterm`.
  `mod.rs` runs the event loop and restores the terminal on exit and on panic; `app.rs` holds state and validation; `runner.rs` runs each operation on a worker thread that always reports completion, even after a panic; `ui.rs` draws forms, stats, the log and the done screen; `logging.rs` captures `tracing` output; `event.rs` polls input.

### Key Performance Patterns

- Streaming XML parsing; the dump is never held in memory.
- Multistream parallelism: each rayon worker seeks to its own bz2 stream, decompresses and parses it.
- Chunked parallel extraction with rayon, overlapping the read of the next chunk.
- `ShardedCsvWriter` spreads rows across files to cut lock contention; edges and entity rows are collected per page and written under one lock.
- `DashSet` for concurrent first-seen deduplication of categories, images and external links.
- Pre-sized `FxHashMap`s for the index (trusted input, so no DoS-resistant hashing needed).
- Atomic counters for stats.
- CSR graph for analytics, about 8 bytes per link.
- 128KB `BufWriter`s for CSV shards, 256KB buffers for merge.

### CSV Sharding and Merge Trade-Off

Sharded extraction is faster (about 1.6x with 14 shards in earlier benchmarks), but analytics and load read single files, so `merge-csvs` combines the shards first; `pipeline` does this automatically.

## Dependencies

- `quick-xml`: streaming XML parsing
- `bzip2`: in-process bz2 decompression (external `lbzip2`/`pbzip2` preferred for standard dumps)
- `rayon`: data parallelism for extraction and PageRank
- `clap`: CLI
- `csv`: CSV reading and writing
- `serde` / `serde_json`: JSON blobs
- `regex` / `once_cell`: lazily compiled regexes
- `memchr`: fast template scanning
- `indicatif`: progress spinners
- `tracing` / `tracing-subscriber`: logging
- `anyhow`: errors with context
- `bincode`: index cache and checkpoints
- `dashmap`: concurrent deduplication sets
- `rustc-hash`: `FxHashMap` / `FxHashSet`
- `duckdb`: DuckDB client (links Homebrew's library locally, DuckDB's prebuilt release in CI)
- `ratatui` / `crossterm`: TUI
- `mimalloc`: global allocator

## Testing

All of these must pass before committing:

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt -- --check
```

- **Unit tests**: inline in most modules (parser, multistream, title, index, extract, checkpoint, cache, atomic_file, stats, csv_util, merge, analytics, load, content, infobox, models, tui/ui).
- **`tests/integration_test.rs`**: parsing, indexing, exact edges, blobs and abstracts, sharding, crash-and-resume equivalence, exact limits, and a round trip from extract through merge, analytics and load into DuckDB.
- **`tests/test_merge_csvs.rs`**: merging with cross-shard deduplication.

## Architecture Decisions

### Why a Two-Pass Pipeline?

Resolving a link needs the id of its target, which may appear later in the dump.
Building the index first (fast, no text) lets the extraction pass resolve every link and redirect.

### Why Chunked Extraction?

Parallel processing finishes pages out of order, so "the last page done" is not a safe resume point.
Chunks give a barrier: after each one, everything before it is fully on disk, and the checkpoint records exactly how long each CSV was at that moment.

### Why FxHashMap?

FxHash is faster than SipHash, and the input is a trusted dump, so hash-flooding resistance buys nothing.

### Why CSV Sharding With a Merge?

Sharding cuts lock contention during extraction; the merge restores single files for analytics and load.

### Why DuckDB?

It is an embedded, multithreaded analytical database: a single file, no server, hostable on a laptop or a small cloud machine.
It bulk-loads the CSVs directly with `read_csv`.
The CSVs stay the source of truth, so another store could load them later.

### Why CSR for Analytics?

Compressed sparse rows store a graph in about 8 bytes per link plus a pointer per node, far less than per-node adjacency lists, and they are cache-friendly for PageRank.

### Why Label Propagation Instead of Louvain?

It is simpler, fast, and gives reasonable communities on the Wikipedia graph; full Louvain modularity optimization may come later.

### Why Multistream Parallel Parsing?

A standard dump is one bz2 stream, so decompression is sequential even with `lbzip2`.
A multistream dump is hundreds of thousands of independent streams; with their offsets from the index file, each worker can seek, decompress and parse on its own.

## Troubleshooting

- **Build cannot find `libduckdb`**: `brew install duckdb`, or set `DUCKDB_LIB_DIR` and `DUCKDB_INCLUDE_DIR` to an unpacked prebuilt DuckDB release, as CI does.
- **"Multistream index does not match dump"**: the dump is incomplete (often still downloading) or does not belong to that index.
- **Slow extraction**: release build, a multistream dump, more `--csv-shards`; install `lbzip2` for standard dumps.
- **Index cache invalid**: `--no-cache` rebuilds it; it is also rebuilt automatically when the dump or `CACHE_VERSION` changes.
- **Checkpoint conflicts**: `--clean` to start over, `--resume` to continue; a checkpoint from a different dump, output directory, shard count or reading mode is ignored.
- **Analytics or load needs merged CSVs**: run `dedalus merge-csvs -o <dir>`, or extract with `--csv-shards 1`.
- **Load refuses an existing database**: pass `--clean`.
