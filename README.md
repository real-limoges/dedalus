# Dedalus

A Rust data processing pipeline that extracts and transforms Wikipedia XML dumps into structured data for Neo4J graph database ingestion.

Dedalus reads compressed Wikipedia dumps (`.xml.bz2`), resolves redirects, extracts article link graphs, and outputs Neo4J-compatible CSV files alongside sharded JSON article content.

## Features

- **Two-pass streaming pipeline** -- indexing pass builds a title-to-ID map, extraction pass produces output in parallel
- **Memory-efficient** -- event-based XML parsing with `quick-xml`; never loads the full dump into memory
- **Parallel extraction** -- uses `rayon` for multi-core article processing
- **Redirect resolution** -- follows redirect chains (up to 5 hops) to resolve target article IDs
- **Neo4J-compatible output** -- `nodes.csv` and `edges.csv` formatted for `neo4j-admin import`
- **Sharded JSON blobs** -- article content stored as `blobs/{shard}/{id}.json` (1000 shards by default)
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

### Example

```bash
# Process the full English Wikipedia dump
./target/release/dedalus -i enwiki-latest-pages-articles.xml.bz2 -o output/ -v

# Quick test with 10,000 pages
./target/release/dedalus -i enwiki-latest-pages-articles.xml.bz2 -o output/ --limit 10000 -vv
```

## Output Format

```
output/
├── nodes.csv              # id:ID | title | :LABEL
├── edges.csv              # :START_ID | :END_ID | :TYPE
└── blobs/
    ├── 000/
    │   └── {id}.json      # { "id": ..., "title": "...", "text": "..." }
    ├── 001/
    │   └── ...
    └── 999/
```

- **nodes.csv** -- one row per article with columns `id:ID`, `title`, `:LABEL` (compatible with `neo4j-admin import`)
- **edges.csv** -- one row per wikilink with columns `:START_ID`, `:END_ID`, `:TYPE` (LINKS_TO)
- **blobs/** -- article text stored as JSON, sharded by `id % shard_count`

## Architecture

### Two-Pass Pipeline

1. **Indexing pass** (`index.rs`) -- streams through the dump with `skip_text` enabled to build an in-memory `HashMap<String, u32>` of title-to-ID mappings and a redirect resolution table. No article text is read.

2. **Extraction pass** (`extract.rs`) -- streams through the dump a second time, this time reading article text. Uses `rayon::par_bridge()` to process pages in parallel: extracts wikilinks via regex, resolves link targets through the index, and writes nodes/edges/blobs concurrently.

### Modules

| Module | Purpose |
|--------|---------|
| `main.rs` | CLI parsing (`clap`), orchestrates two-pass pipeline, summary output |
| `parser.rs` | `WikiReader` -- streaming XML parser implementing `Iterator<Item = WikiPage>` |
| `index.rs` | `WikiIndex` -- title-to-ID mapping with redirect chain resolution |
| `extract.rs` | Parallel extraction of nodes, edges, and article blobs |
| `models.rs` | Core types: `WikiPage`, `PageType`, `ArticleBlob` |
| `stats.rs` | `ExtractionStats` -- atomic counters for thread-safe metrics |
| `config.rs` | Constants: redirect depth, shard count, progress interval |

## Current Status

Phases 1-3 of the [roadmap](FUTURE_IMPROVEMENTS.md) are complete:

- **Phase 1 (Core Architecture)** -- two-pass pipeline, streaming parser, index, parallel extraction
- **Phase 2 (Data Extraction)** -- wikilink extraction, CSV output, JSON blob storage
- **Phase 3 (CLI & Observability)** -- `clap` CLI, `indicatif` progress bars, `tracing` logging, statistics

Next up: **Phase 4 (Testing)** -- unit tests, integration tests, and test fixtures.

See [FUTURE_IMPROVEMENTS.md](docs/FUTURE_IMPROVEMENTS.md) for the full roadmap.

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
