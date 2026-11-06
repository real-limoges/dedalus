# Dedalus

A Rust pipeline that turns a Wikipedia XML dump into graph data.
Dedalus extracts articles, links, categories, images and external links from a compressed dump, computes PageRank, communities and degree over the link graph, and loads everything into a single [DuckDB](https://duckdb.org) file.

Every stage also leaves plain CSV files behind, so the output is usable without DuckDB.

## What It Does

```
Wikipedia dump (.xml.bz2)
    |
    v
[Extract]   ---> CSV files + JSON blobs
    |
    v
[Merge]     ---> Deduplicated single CSVs
    |
    v
[Analytics] ---> analytics.csv (PageRank, communities, degree)
    |
    v
[Load]      ---> wikipedia.duckdb
```

## Quick Start

```bash
brew install duckdb            # the build links Homebrew's libduckdb
cargo build --release

# Run the full pipeline (extract -> merge -> analytics -> load)
dedalus pipeline -i enwiki-latest-pages-articles-multistream.xml.bz2 -o output/ -v

# Try a small slice first
dedalus pipeline -i enwiki-latest-pages-articles-multistream.xml.bz2 -o output/ --limit 10000 -v

# Query the result
duckdb output/wikipedia.duckdb
```

## Getting a Wikipedia Dump

Download from [Wikimedia Downloads](https://dumps.wikimedia.org/enwiki/latest/), or run `scripts/download_wikipedia.sh`, which picks the fastest mirror.
Use the **multistream** format and fetch its index too:

```bash
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream.xml.bz2
wget https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream-index.txt.bz2
```

A multistream dump is a few hundred thousand independent bz2 streams of about 100 pages each, and the index file lists their byte offsets.
Dedalus uses the index to decompress and parse streams in parallel on every core.
The index is found automatically when it sits beside the dump with the matching name; otherwise pass `--multistream-index`.
A standard (single-stream) dump works too, but decompression is then sequential.

Dedalus checks that every offset in the index lies inside the dump, so a dump that is still downloading is refused rather than read partway.

## Prerequisites

- **Rust 1.94+**: [install via rustup](https://rustup.rs/).
- **DuckDB 2.0** from Homebrew: `brew install duckdb`.
  `.cargo/config.toml` points the build at `/opt/homebrew/opt/duckdb`.
  On a machine without Homebrew, download the prebuilt library from DuckDB's GitHub release (for example `duckdb-shared-libs-linux-amd64.tar.gz`) and set `DUCKDB_LIB_DIR` and `DUCKDB_INCLUDE_DIR` to where it was unpacked, as CI does; on Linux, `LD_LIBRARY_PATH` too.
  The `duckdb` crate version must match the library: crate `1.20000.x` wraps DuckDB 2.0.x.
- **lbzip2** (optional): parallel decompression for standard, non-multistream dumps (`brew install lbzip2`).

## Subcommands

### `pipeline`: Full Workflow (Recommended)

Runs extract, merge (when sharded), analytics and load in sequence.

```bash
dedalus pipeline -i <dump.xml.bz2> -o <output-dir> [OPTIONS]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-i, --input` | Path to the Wikipedia dump (`.xml.bz2`) | Required |
| `-o, --output` | Output directory | Required |
| `--csv-shards <N>` | CSV shards written in parallel during extraction | `8` |
| `--shard-count <N>` | Directories the JSON blobs are spread across | `1000` |
| `--limit <N>` | Stop after this many pages (for testing) | None |
| `--checkpoint-interval <N>` | Pages per extraction chunk; a checkpoint is saved after each | `10000` |
| `--resume` | Resume extraction from the last checkpoint | Off |
| `--no-cache` | Rebuild the title index instead of loading `index.cache` | Off |
| `--clean` | Clear existing checkpoint and outputs before starting | Off |
| `--no-archive` | Leave shard CSVs in place after merging | Off |
| `--no-analytics` | Skip analytics | Off |
| `--no-load` | Skip the DuckDB load | Off |
| `--db-path` | DuckDB file, relative to the output directory | `wikipedia.duckdb` |
| `--multistream-index` | Path to the multistream index file | Auto-detected |

The pipeline always replaces the DuckDB file, since it has just rewritten the CSVs the file mirrors.

### `extract`: CSV and JSON Extraction

```bash
dedalus extract -i <dump.xml.bz2> -o <output-dir> [OPTIONS]
```

Takes the same extraction flags as `pipeline` (`--csv-shards`, `--shard-count`, `--limit`, `--checkpoint-interval`, `--resume`, `--no-cache`, `--clean`, `--multistream-index`), plus `--dry-run`, which writes nothing.

Extraction reads the dump in chunks of `--checkpoint-interval` pages.
After each chunk every file is flushed and a checkpoint is saved, recording where reading stopped and how long each CSV was.
`--resume` cuts each CSV back to that length and continues from the next chunk, so an interrupted run ends with exactly the output of an uninterrupted one.

### `merge-csvs`: Shard Merging

Combines sharded CSVs (`nodes_000.csv`, ...) into single files, deduplicating category, image and external link nodes across shards.
Analytics and load need the merged files.

```bash
dedalus merge-csvs -o <output-dir> [--archive]
```

The shards must be numbered `0..N` with no gaps.
`--archive` moves them into `shards/` afterward, replacing any shards archived by an earlier run.

### `analytics`: Graph Analytics

Computes PageRank, communities (label propagation) and in plus out degree over the LINKS_TO edges, and writes them to `analytics.csv`.
SEE_ALSO edges, and edges to pages that are not in `nodes.csv`, are left out.

```bash
dedalus analytics -o <output-dir> [OPTIONS]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Directory holding the merged CSVs | Required |
| `--pagerank-iterations` | Maximum PageRank iterations | `20` |
| `--damping` | PageRank damping factor | `0.85` |

### `load`: DuckDB Load

Copies the merged CSVs, and `analytics.csv` when it exists, into one DuckDB file.

```bash
dedalus load -o <output-dir> [OPTIONS]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Directory holding the merged CSVs | Required |
| `--db-path` | DuckDB file, relative to the output directory | `wikipedia.duckdb` |
| `--clean` | Replace the database if it already exists | Off |

The database is built in a `.tmp` file and renamed into place when every table has loaded, so a failed load keeps the previous database.

### `stats`: Output Statistics

Shows CSV file sizes, blob counts, the DuckDB file size and total disk usage.

```bash
dedalus stats -o <output-dir>
```

### `tui`: Interactive Terminal UI

A form for each operation (Extract, MergeCsvs, Analytics, Load), with live extraction stats and a scrolling log.

```bash
dedalus tui
```

| Key | Screen | Action |
|-----|--------|--------|
| `Tab`, `Shift+Tab` | Config | Switch operation |
| `Up`, `Down` | Config | Move between fields |
| `Enter` | Config | Toggle a checkbox, or start the operation |
| `Space` | Config | Toggle a checkbox |
| `Esc` | Config | Quit |
| `q` | Config | Quit, unless a text field is selected |
| `c`, `Esc` | Progress | Cancel an extraction after the current chunk (resume later with Resume) |
| `Up`, `Down`, `End` | Progress | Scroll the log; `End` follows new lines again |
| `r` | Done | Back to the config screen |
| `q`, `Esc` | Done | Quit |
| `Ctrl+C` | Any | Quit |

### Global Flags

| Flag | Description |
|------|-------------|
| `-v` | INFO logging |
| `-vv` | DEBUG logging |
| `-vvv` | TRACE logging |

## Example Workflows

```bash
# Full pipeline
dedalus pipeline -i enwiki-multistream.xml.bz2 -o out/ -v

# CSVs only, no DuckDB
dedalus pipeline -i enwiki-multistream.xml.bz2 -o out/ --no-load -v

# Step by step
dedalus extract -i enwiki-multistream.xml.bz2 -o out/ --csv-shards 14 -v
dedalus merge-csvs -o out/ --archive
dedalus analytics -o out/
dedalus load -o out/ --clean

# Resume an interrupted extraction
dedalus extract -i enwiki-multistream.xml.bz2 -o out/ --resume -v
```

## Output Format

```
output/
├── nodes.csv                   # Articles: id, title
├── edges.csv                   # Article links: source, target, LINKS_TO or SEE_ALSO
├── categories.csv              # Category nodes (deduplicated)
├── article_categories.csv      # Article-to-category rows
├── image_nodes.csv             # Image nodes (deduplicated)
├── article_images.csv          # Article-to-image rows
├── external_link_nodes.csv     # External link nodes (deduplicated)
├── article_external_links.csv  # Article-to-external-link rows
├── analytics.csv               # id, pagerank, community, degree
├── wikipedia.duckdb            # All of the above as DuckDB tables
├── index.cache                 # Cached title-to-id index
├── checkpoint.bin              # Resume state, removed when extraction completes
├── blobs/
│   ├── 000/{id}.json           # Article content
│   └── ...
└── shards/                     # Archived shard CSVs (after --archive)
```

The CSV headers use a typed-column style (`id:ID`, `:START_ID`, `:TYPE`).
Titles are stored in MediaWiki's canonical form (first letter uppercase, underscores as spaces), so link targets and category names written either way resolve to the same entry.

### DuckDB Tables

| Table | Columns |
|-------|---------|
| `articles` | `id`, `title` |
| `links` | `src`, `dst`, `type` (`LINKS_TO` or `SEE_ALSO`) |
| `categories`, `article_categories` | `id`, `name`; `article_id`, `category_id` |
| `images`, `article_images` | `id`, `filename`; `article_id`, `image_id` |
| `external_links`, `article_external_links` | `id`, `url`; `article_id`, `link_id` |
| `analytics` | `id`, `pagerank`, `community`, `degree` |

```sql
-- The ten highest-ranked articles
SELECT a.title, x.pagerank
FROM analytics x JOIN articles a USING (id)
ORDER BY x.pagerank DESC LIMIT 10;

-- What an article links to
SELECT t.title
FROM links l JOIN articles t ON t.id = l.dst
WHERE l.src = (SELECT id FROM articles WHERE title = 'Rust (programming language)')
  AND l.type = 'LINKS_TO';

-- The largest communities, named by one member
SELECT a.title, count(*) AS size
FROM analytics x JOIN articles a ON a.id = x.community
GROUP BY a.title ORDER BY size DESC LIMIT 10;
```

A community is identified by the page id of one of its articles.

### JSON Blobs

Each article gets a JSON blob at `blobs/{id % shard_count}/{id}.json`:

```json
{
  "id": 12345,
  "title": "Example Article",
  "abstract_text": "The lead section as plain text, links shown as their visible text...",
  "categories": ["Category A", "Category B"],
  "infoboxes": [{"infobox_type": "Infobox software", "fields": [["name", "Example"]]}],
  "sections": ["History", "Design", "See also"],
  "timestamp": "2024-01-15T10:30:00Z",
  "is_disambiguation": false
}
```

Empty lists and a false `is_disambiguation` are left out of the file.

## Resources

Timings and peak memory on the full English dump have not been measured since the pipeline was reworked.
As estimates: the analytics graph takes about 8 bytes per link (around 2 GB for 200 million links), and extraction holds the title index plus the sets of category, image and external link names seen so far, which together can run to several GB.
Plan for 16 GB of RAM or more and an SSD.
In earlier benchmarks, 14 CSV shards extracted about 1.6 times faster than one.

## Architecture

1. **Index**: streams the dump without article text, building a title-to-id map (`FxHashMap`) and a redirect table; redirect chains are followed up to 5 hops.
   With a multistream dump, streams are parsed in parallel.
2. **Extract**: reads the dump again in ordered chunks, processing each chunk's pages in parallel with rayon.
   Rows go to `page_id % csv_shards`; category, image and external link names are deduplicated with `DashSet`s.
3. **Merge**: concatenates shards per CSV type, deduplicating shared nodes with `FxHashSet`; each merged file is written to a `.tmp` sibling and renamed.
4. **Analytics**: builds a compressed sparse row graph from LINKS_TO edges in two passes over `edges.csv`, then runs rayon-parallel PageRank (rank on pages with no outgoing links is spread evenly, so ranks sum to 1), rayon-parallel label propagation, and degree.
5. **Load**: one `CREATE TABLE ... AS SELECT ... FROM read_csv(...)` per CSV, with explicit column types.

`docs/reference/internal/architecture.md` has the longer version.

## Development

```bash
cargo build --release                  # Optimized binary
cargo test                             # All unit and integration tests
cargo clippy --all-targets -- -D warnings
cargo fmt -- --check
git config core.hooksPath .githooks    # Run fmt, clippy, build and tests before every push
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Build cannot find `libduckdb` | `brew install duckdb`, or point `DUCKDB_LIB_DIR` and `DUCKDB_INCLUDE_DIR` at DuckDB's prebuilt library |
| Slow extraction | Use `--release`, a multistream dump, and more `--csv-shards`; install `lbzip2` for standard dumps |
| "Multistream index does not match dump" | The dump is incomplete (often still downloading) or belongs to a different index |
| Stale index cache | `--no-cache` to rebuild |
| Interrupted extraction | `--resume` to continue, or `--clean` to restart |
| Analytics or load says it needs merged CSVs | Run `dedalus merge-csvs -o <dir>` first, or extract with `--csv-shards 1` |
| Load says the database already exists | Pass `--clean` to replace it |

## License

MIT License. See [LICENSE](LICENSE).
