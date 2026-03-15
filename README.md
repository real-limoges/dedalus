# Dedalus

A high-performance Rust pipeline that transforms Wikipedia XML dumps into a structured graph database. Dedalus extracts articles, links, categories, and metadata from compressed dumps, loads them into an embedded [SurrealDB](https://surrealdb.com) database, and computes graph analytics like PageRank and community detection.

No external services required -- everything runs as a single binary with a local `wikipedia.db/` directory as output.

## What It Does

```
Wikipedia dump (.xml.bz2)
    |
    v
[Extract] ---> CSV files + JSON blobs
    |
    v
[Merge]   ---> Deduplicated single CSVs
    |
    v
[Load]    ---> SurrealDB (RocksDB)
    |
    v
[Analytics] -> PageRank, communities, degree
```

**Output**: A `wikipedia.db/` directory containing the full article graph with computed analytics, plus CSV/JSON files for use with other tools.

## Quick Start

```bash
# Build
cargo build --release

# Run the full pipeline (extract -> merge -> load -> analytics)
dedalus pipeline -i enwiki-latest-pages-articles-multistream.xml.bz2 -o output/ -v

# Test with a small subset first
dedalus pipeline -i enwiki-latest-pages-articles-multistream.xml.bz2 -o output/ --limit 10000 -vv
```

That's it. No Docker, no database setup, no configuration files.

## Getting a Wikipedia Dump

Download from [Wikimedia Downloads](https://dumps.wikimedia.org/enwiki/latest/). Use the **multistream** format for best performance:

```bash
# ~22GB dump + ~250MB index
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream.xml.bz2
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream-index.txt.bz2
```

The multistream format contains ~200K independent bz2 streams, allowing Dedalus to parallelize both decompression and XML parsing across all CPU cores. The standard (non-multistream) format works too, but decompression is single-threaded.

## Prerequisites

- **Rust 1.87+** -- [install via rustup](https://rustup.rs/)
- **lbzip2** (optional) -- parallel bzip2 decompression for standard (non-multistream) dumps
  ```bash
  brew install lbzip2        # macOS
  apt install lbzip2         # Debian/Ubuntu
  ```

## Subcommands

### `pipeline` -- Full Workflow (Recommended)

Runs everything in sequence: extract, merge, load into SurrealDB, compute analytics.

```bash
dedalus pipeline -i <dump.xml.bz2> -o <output-dir> [OPTIONS]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-i, --input` | Path to Wikipedia dump (`.xml.bz2`) | required |
| `-o, --output` | Output directory | required |
| `--csv-shards <N>` | Parallel extraction shards | `8` |
| `--limit <N>` | Cap pages processed (for testing) | none |
| `--db-path` | SurrealDB database path | `wikipedia.db` |
| `--clean` | Clear existing outputs before starting | `false` |
| `--resume` | Resume from last checkpoint | `false` |
| `--no-load` | Skip SurrealDB load + analytics | `false` |
| `--no-analytics` | Skip analytics computation | `false` |
| `--no-archive` | Keep sharded CSVs after merging | `false` |
| `--multistream-index` | Path to multistream index file | auto-detected |

### `extract` -- CSV/JSON Extraction

Processes a dump into CSV files and JSON blobs without loading into a database.

```bash
dedalus extract -i <dump.xml.bz2> -o <output-dir> [OPTIONS]
```

Key flags: `--csv-shards`, `--limit`, `--dry-run`, `--resume`, `--clean`, `--no-cache`

### `load` -- SurrealDB Import

Loads merged CSVs (articles + edges) into an embedded SurrealDB database.

```bash
dedalus load -o <output-dir> [OPTIONS]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Directory containing CSV output | required |
| `--db-path` | SurrealDB database path | `wikipedia.db` |
| `--batch-size` | Records per insert batch | `10000` |
| `--clean` | Remove existing database first | `false` |

### `analytics` -- Graph Analytics

Computes PageRank, community detection (label propagation), and degree centrality from CSVs, writing results back to SurrealDB.

```bash
dedalus analytics -o <output-dir> [OPTIONS]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Directory containing CSV output | required |
| `--db-path` | SurrealDB database path | `wikipedia.db` |
| `--pagerank-iterations` | Max PageRank iterations | `20` |
| `--damping` | PageRank damping factor | `0.85` |

### `merge-csvs` -- Shard Merging

Combines sharded CSV files into single files with cross-shard deduplication. Required before `load` if you extracted with `--csv-shards > 1`.

```bash
dedalus merge-csvs -o <output-dir> [--archive]
```

### `stats` -- Output Statistics

Shows CSV file sizes, blob counts, SurrealDB size, and total disk usage.

```bash
dedalus stats -o <output-dir>
```

### `tui` -- Interactive Terminal UI

Form-based interface for configuring and monitoring all operations with real-time stats and log streaming.

```bash
dedalus tui
```

| Key | Action |
|-----|--------|
| `Tab` | Switch between operation tabs |
| `Up/Down` | Navigate form fields |
| `Enter` | Toggle checkbox or start operation |
| `c` | Cancel running operation |
| `r` | Return to config (from done screen) |
| `q` | Quit |

### Global Flags

| Flag | Description |
|------|-------------|
| `-v` | INFO logging |
| `-vv` | DEBUG logging |
| `-vvv` | TRACE logging |

## Example Workflows

```bash
# Full pipeline (recommended)
dedalus pipeline -i enwiki-multistream.xml.bz2 -o out/ -v

# Extract only, no database
dedalus pipeline -i enwiki-multistream.xml.bz2 -o out/ --no-load -v

# Step-by-step
dedalus extract -i enwiki-multistream.xml.bz2 -o out/ --csv-shards 14 -v
dedalus merge-csvs -o out/ --archive
dedalus load -o out/ --clean
dedalus analytics -o out/

# Resume interrupted extraction
dedalus extract -i enwiki-multistream.xml.bz2 -o out/ --resume -v

# Clean start
dedalus pipeline -i enwiki-multistream.xml.bz2 -o out/ --clean -v
```

## Output Format

```
output/
├── nodes.csv                   # Article nodes (id, title)
├── edges.csv                   # Article-to-article links
├── categories.csv              # Category nodes (deduplicated)
├── article_categories.csv      # Article-to-category edges
├── image_nodes.csv             # Image nodes (deduplicated)
├── article_images.csv          # Article-to-image edges
├── external_link_nodes.csv     # External link nodes (deduplicated)
├── article_external_links.csv  # Article-to-external-link edges
├── wikipedia.db/               # SurrealDB database (RocksDB)
├── index.cache                 # Cached title-to-ID index
├── blobs/
│   ├── 000/{id}.json           # Enriched article content
│   ├── 001/{id}.json
│   └── ...
└── shards/                     # Archived sharded CSVs (optional)
```

### SurrealDB Schema

```sql
-- Tables
article { id, title, pagerank, community, degree }
links_to (relation: article -> article)

-- Example queries
SELECT * FROM article ORDER BY pagerank DESC LIMIT 10;
SELECT count() FROM article;
SELECT * FROM article WHERE title = "Rust (programming language)";
SELECT ->links_to->article.title FROM article:12345;
```

### JSON Blobs

Each article gets an enriched JSON blob at `blobs/{id % 1000}/{id}.json`:

```json
{
  "id": 12345,
  "title": "Example Article",
  "abstract_text": "First paragraph with templates stripped...",
  "categories": ["Category A", "Category B"],
  "infoboxes": [{"template": "Infobox software", "fields": {...}}],
  "sections": ["History", "Design", "See also"],
  "timestamp": "2024-01-15T10:30:00Z",
  "is_disambiguation": false
}
```

## Performance

**Full English Wikipedia** (~22M pages, 87GB compressed):

| Stage | Time | Notes |
|-------|------|-------|
| Extract (14 shards) | ~2.8 hours | 1.62x faster than single shard |
| Extract (1 shard) | ~4.5 hours | Simpler but slower |
| Merge | <5 minutes | Cross-shard deduplication |
| SurrealDB Load | ~15-30 minutes | Batch inserts to RocksDB |
| Analytics | ~5-10 minutes | PageRank + communities + degree |

**Hardware recommendations**:
- 8+ CPU cores (rayon scales linearly)
- 16GB+ RAM (32GB recommended for full Wikipedia analytics)
- SSD storage (CSV writes are I/O intensive)
- Apple Silicon benefits from automatic NEON SIMD optimizations (~1.6x)

## Architecture

Dedalus uses a multi-pass streaming architecture:

1. **Indexing** -- Streams through the dump with text skipped, building a `FxHashMap` title-to-ID index (pre-sized for 8M articles) with redirect resolution (up to 5 hops). With multistream dumps, this is parallelized across bz2 streams.

2. **Extraction** -- Second pass reads article text. `rayon::par_bridge()` parallelizes processing. `ShardedCsvWriter` distributes output across N files. `DashSet` deduplicates categories/images/external links concurrently. Checkpointing every 10K articles enables resume.

3. **Merge** -- Streaming concatenation with `FxHashSet` deduplication across shards. 256KB I/O buffers.

4. **Load** -- Opens embedded SurrealDB (RocksDB backend), creates schema, batch-inserts articles and edges from merged CSVs. Record IDs map directly from Wikipedia page IDs (`article:12345`).

5. **Analytics** -- Builds a CSR (Compressed Sparse Row) graph from CSVs (~1GB for full Wikipedia). Computes PageRank via rayon-parallel power iteration, label propagation communities, and in+out degree. Batch-writes results to SurrealDB.

Key design choices:
- **Two-pass pipeline** -- Index first (fast, no text), then extract with redirect resolution
- **FxHashMap** -- Faster than SipHash for trusted input (no DoS risk)
- **CSV intermediates** -- Debuggable, reusable, decouples extraction from storage
- **Embedded SurrealDB** -- No Docker, no external services, single directory output
- **CSR for analytics** -- ~1GB for 7M nodes / 200M edges vs ~3GB+ for adjacency lists
- **Tokio isolation** -- Only created for async SurrealDB operations; extraction stays on sync rayon

## Development

```bash
cargo build --release          # Build optimized binary
cargo test --verbose           # Run all tests (177 unit + integration)
cargo clippy -- -D warnings    # Lint with strict warnings
cargo fmt -- --check           # Check formatting
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Slow extraction | Use `--release`, `--csv-shards 14`, multistream dumps, install `lbzip2` |
| Stale index cache | `--no-cache` to rebuild |
| Interrupted extraction | `--resume` to continue, or `--clean` to restart |
| Load fails (sharded CSVs) | Run `dedalus merge-csvs` first |
| OOM during analytics | Ensure 4GB+ free RAM for CSR graph |
| Existing database conflicts | Use `--clean` on load/pipeline |

## License

MIT License. See [LICENSE](LICENSE).
