# Changelog

## [Unreleased]

### Added
- README: Prerequisites section with install commands for Rust, Docker, lbzip2
- README: "Obtaining Wikipedia Dumps" section with multistream vs standard comparison
- README: `dedalus pipeline` flag table (17 flags), restructured as first Usage subsection
- README: `dedalus stats` and `dedalus tui` sections with flag tables and keyboard controls
- README: `--archive` flag added to `merge-csvs` table
- README: Quick Start restructured around `pipeline` as the recommended one-command approach
- Inline `//!` module docs and `///` item docs for all 6 TUI source files
- Doc comment on `Infobox` struct in `infobox.rs`
- CLAUDE.md: `tui/` entry in Core Modules table
- `EdgeType` enum replacing stringly-typed `"LINKS_TO"` / `"SEE_ALSO"` constants
- `CsvType` enum with `ALL` constant and `base_name()` method replacing `CSV_TYPES: &[&str]`
- `ExtractionConfig` struct eliminating all `#[allow(clippy::too_many_arguments)]` suppressions
- `Display` impls for `PageType`, `EdgeType`, and `CsvLayout`
- `Default` impl for `ImportConfig`
- `Debug` impls for `WikiIndex`, `CheckpointManager`, `ImportConfig`
- `Clone` derive for `ArticleBlob`, `Checkpoint`, `CheckpointStats`, `ImportConfig`, `CsvLayout`
- `#[must_use]` annotations on all pure/query public functions
- Buffer/capacity constants in `config.rs` replacing magic numbers across the codebase
- Doc comments on all public structs, enums, and functions
- `lib.rs` re-exports for primary API types
- `rustfmt.toml` (max_width=100), `clippy.toml` (too-many-arguments-threshold=8)
- Cargo.toml metadata: authors, keywords, categories, rust-version, readme

### Changed
- `run_extraction()` and `run_extraction_with_stats()` now take `&ExtractionConfig` instead of 9-12 positional parameters
- Extraction closure decomposed into `process_article_edges()`, `write_dedup_entities()`, and `write_article_blob()` helpers
- `CsvLayout::description()` replaced by `Display` impl
- `detect_csv_layout()` and `detect_shard_count()` use iterator idioms instead of manual counting loops
- `ProgressStyle::template().unwrap()` replaced with `.expect("valid progress template")`
- Lock poison error message improved with context about writer thread panics

### Removed
- All `#[allow(clippy::too_many_arguments)]` suppressions
- `CSV_TYPES: &[&str]` constant (replaced by `CsvType::ALL`)
- `CsvLayout::description()` method (replaced by `Display`)
