# Changelog

## [Unreleased]

### Added
- DuckDB load pass (`dedalus load`, the last `pipeline` step, and a Load tab in the TUI): one table per merged CSV plus `analytics`, built in a `.tmp` file and renamed into place.
  The build links Homebrew's `libduckdb`; CI downloads DuckDB's prebuilt Linux library instead.
- `analytics.csv`: PageRank, community and degree per article, replacing the fields written into SurrealDB.
- Resumable chunked extraction: the dump is read in ordered chunks, every writer is flushed after each chunk, and the checkpoint records the read position and every CSV's length, so `--resume` produces exactly the output of an uninterrupted run.
- Canonical title form (`title::normalize_title`): underscores, leading colons, anchors and a lowercase first letter all resolve to the same article; namespace detection is case-insensitive.
- `atomic_file::write_atomically`, used for the index cache, checkpoints, merged CSVs and `analytics.csv`.
- `WikiReader::finish`, which reports XML errors and a failed `lbzip2`/`pbzip2` exit instead of treating a truncated dump as complete.
- Multistream index validation: offsets must increase and lie inside the dump.
- Integration tests for exact edges, namespace exclusion, plain-text abstracts, crash-and-resume, exact limits, and extract through load into DuckDB.

### Changed
- Abstracts are plain text: links show their visible text, and file and category embeds, footnotes, comments and bold or italic quotes are removed.
- SEE_ALSO edges are only links inside the article's "See also" section, which ends at the next level-2 heading.
- Analytics uses LINKS_TO edges only; PageRank spreads the rank of pages without outgoing links so ranks sum to 1, and runs in f64.
- Community detection is named for what it is (`compute_label_propagation`, `LABEL_PROPAGATION_MAX_ITERATIONS`) and breaks ties by smallest label.
- The analytics graph is built in two passes over `edges.csv`, without a per-node `Vec`.
- `merge-csvs` requires shards numbered `0..N`, checks every shard and header before writing, and `--archive` replaces previously archived shards.
- Layout detection prefers merged `nodes.csv` over leftover shards; a fresh extraction removes CSVs and `analytics.csv` from an earlier run.
- `--limit` stops reading the dump and is exact across chunks; cancelling in the TUI keeps the checkpoint.
- Shard counts must be at least 1, in the CLI and the TUI; CLI and TUI defaults come from `config.rs`.
- CI and the pre-push hook run clippy with `--all-targets`; `rustfmt.toml` uses edition 2024; the release profile is defined only in `Cargo.toml`.
- Index cache version 3 and checkpoint version 4; older files are rebuilt or ignored.

### Fixed
- `strip_templates` panicked (an abort in release builds) on an unclosed template ending in multi-byte text.
- Infobox fields split on `|` inside `[[links]]`, and keys were taken from `=` inside nested templates or links.
- Pipe-trick links and links inside image captions were missed; `{{Disambiguation needed}}` marked ordinary articles as disambiguation pages.
- Redirect chains stopped one hop short of `REDIRECT_MAX_DEPTH`.
- A malformed page could leak fields into the next page in the parser.
- CSV flush and write errors were dropped, and blobs were counted before they were written.
- Category and image names differing only in spelling were stored as separate nodes.
- The TUI quit when `q` was typed into a path, left the terminal in raw mode after a panic, and hung when a worker panicked.

### Removed
- SurrealDB, the `tokio` runtime and `futures`.
- `scripts/import-neo4j.sh`; the CSV headers keep their typed-column style.
- Dead code: `cache::load_index`, `cache::is_cache_valid`, `ExtractionStats::inc_categories`, `extract_see_also_links`, `csv_files_for`, `validate_csv_files`.

## Earlier

### Added
- `EdgeType` enum replacing stringly-typed `"LINKS_TO"` / `"SEE_ALSO"` constants.
- `CsvType` enum with an `ALL` constant and `base_name()`, replacing `CSV_TYPES: &[&str]`.
- `ExtractionConfig` struct, removing every `#[allow(clippy::too_many_arguments)]`.
- `Display` impls for `PageType`, `EdgeType` and `CsvLayout`.
- `#[must_use]` on pure public functions, and doc comments on public items and the TUI modules.
- Buffer and capacity constants in `config.rs` replacing magic numbers.
- `lib.rs` re-exports for the primary API types.
- `rustfmt.toml` (`max_width = 100`) and `clippy.toml` (`too-many-arguments-threshold = 8`).
- Cargo.toml metadata: authors, keywords, categories, rust-version, readme.

### Changed
- `run_extraction()` and `run_extraction_with_stats()` take `&ExtractionConfig` instead of positional parameters.
- `ProgressStyle::template().unwrap()` replaced with `.expect("valid progress template")`.
- Lock poison errors say a writer thread panicked.
