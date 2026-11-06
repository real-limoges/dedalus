//! Dedalus: Wikipedia dump extraction pipeline producing graph CSVs, analytics and DuckDB
//!
//! This crate provides a multi-pass pipeline for extracting structured graph data from
//! Wikipedia XML dumps into plain files, then loading them into a DuckDB database file:
//!
//! 1. **Indexing Pass** -- Build an in-memory title-to-ID mapping and redirect resolution
//!    table by streaming through the dump without reading article text
//! 2. **Extraction Pass** -- Process articles in parallel to extract nodes, edges, categories,
//!    images, external links, and enriched content; output CSV files and JSON blobs
//! 3. **Merge Pass** (when sharded) -- Combine sharded CSV files into single files with
//!    deduplication
//! 4. **Analytics Pass** -- Compute PageRank, label-propagation communities, and degree,
//!    written to `analytics.csv`
//! 5. **Load Pass** -- Copy the merged CSVs and `analytics.csv` into one DuckDB file
//!
//! # Architecture
//!
//! The pipeline is designed for performance and memory efficiency:
//!
//! - **Streaming XML parsing** -- Never loads full dump into memory; uses event-based parsing
//! - **Parallel extraction** -- Uses rayon to process articles concurrently
//! - **CSV sharding** -- Distributes output across N files for parallel extraction
//! - **Concurrent deduplication** -- DashSet for thread-safe first-seen category/image tracking
//! - **Atomic operations** -- Lock-free counters for high-frequency statistics
//! - **Resumable processing** -- Checkpointing and index caching to skip redundant work
//!
//! # Key Modules
//!
//! - [`parser`] -- Streaming XML parser with BZ2 decompression
//! - [`index`] -- Title-to-ID mapping with redirect resolution
//! - [`extract`] -- Parallel extraction with CSV sharding
//! - [`merge`] -- CSV shard merging with deduplication
//! - [`analytics`] -- Graph analytics (PageRank, label propagation, degree)
//! - [`load`] -- DuckDB loader (one table per CSV)
//! - [`csv_util`] -- CSV file names and layout detection
//! - [`title`] -- Canonical title form (MediaWiki rules) and namespace detection
//! - [`content`] -- Text extraction (abstracts, sections, links, categories)
//! - [`infobox`] -- Structured infobox parsing with nested template support
//! - [`models`] -- Core data types (WikiPage, ArticleBlob, PageType)
//! - [`cache`] -- Index persistence with zero-copy serialization
//! - [`checkpoint`] -- Extraction progress checkpointing
//! - [`atomic_file`] -- Crash-safe file replacement
//! - [`stats`] -- Thread-safe atomic counters for extraction metrics
//! - [`config`] -- Constants for extraction, merge and analytics

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

// Re-export primary API types for convenient library use.
pub use checkpoint::{Checkpoint, CheckpointManager};
pub use csv_util::CsvType;
pub use extract::ExtractionConfig;
pub use index::WikiIndex;
pub use models::{ArticleBlob, EdgeType, PageType, WikiPage};
pub use parser::WikiReader;
pub use stats::ExtractionStats;
