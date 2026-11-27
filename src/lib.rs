//! Dedalus: Wikipedia dump extraction pipeline producing graph CSVs, analytics and DuckDB
//!
//! The pipeline reads a Wikipedia XML dump in several passes, writes graph data
//! as plain files, then loads those files into a DuckDB database file:
//!
//! 1. Index: stream the dump without reading article text, and build an in-memory
//!    title-to-ID map and redirect table.
//! 2. Extract: process articles in parallel into nodes, edges, categories, images,
//!    external links and article content, written as CSV files and JSON blobs.
//! 3. Merge (when sharded): combine the CSV shards into single files, deduplicating
//!    across shards.
//! 4. Analytics: compute PageRank, label-propagation communities and degree into
//!    `analytics.csv`.
//! 5. Load: copy the merged CSVs and `analytics.csv` into one DuckDB file.
//!
//! # Architecture
//!
//! - The XML parser is event-based and streams the dump, so the whole dump is
//!   never in memory.
//! - Extraction processes articles concurrently with rayon.
//! - CSV output is spread across N shard files during extraction.
//! - A `DashSet` records the first sighting of each category and image across threads.
//! - Statistics are lock-free atomic counters.
//! - Checkpoints and the index cache let a rerun skip work already done.
//!
//! # Modules
//!
//! - [`parser`]: streaming XML parser with bz2 decompression
//! - [`multistream`]: multistream index parsing and per-stream parallel parsing
//! - [`index`]: title-to-ID map with redirect resolution
//! - [`extract`]: parallel extraction with CSV sharding
//! - [`merge`]: CSV shard merging with deduplication
//! - [`analytics`]: PageRank, label propagation and degree
//! - [`load`]: DuckDB loader, one table per CSV
//! - [`csv_util`]: CSV file names and layout detection
//! - [`title`]: canonical title form (MediaWiki rules) and namespace detection
//! - [`content`]: text extraction (abstracts, sections, links, categories)
//! - [`infobox`]: infobox parsing, including nested templates
//! - [`models`]: core data types (`WikiPage`, `ArticleBlob`, `PageType`)
//! - [`cache`]: index persistence in `index.cache` (postcard)
//! - [`checkpoint`]: extraction progress checkpoints
//! - [`atomic_file`]: crash-safe file replacement
//! - [`stats`]: atomic counters for extraction metrics
//! - [`config`]: constants for extraction, merge and analytics

pub mod analytics;
pub mod atomic_file;
pub mod cache;
pub mod checkpoint;
pub mod config;
pub mod content;
pub mod csv_util;
pub mod extract;
pub mod index;
pub mod infobox;
pub mod load;
pub mod merge;
pub mod models;
pub mod multistream;
pub mod parser;
pub mod stats;
pub mod title;
pub mod tui;

// The main API types, re-exported at the crate root.
pub use checkpoint::{Checkpoint, CheckpointManager};
pub use csv_util::CsvType;
pub use extract::ExtractionConfig;
pub use index::WikiIndex;
pub use models::{ArticleBlob, EdgeType, PageType, WikiPage};
pub use parser::WikiReader;
pub use stats::ExtractionStats;
