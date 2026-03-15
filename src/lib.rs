//! Dedalus: Wikipedia dump extraction pipeline with SurrealDB embedded storage
//!
//! This crate provides a multi-pass pipeline for extracting structured graph data from
//! Wikipedia XML dumps and loading it into an embedded SurrealDB (RocksDB) database:
//!
//! 1. **Indexing Pass** -- Build an in-memory title-to-ID mapping and redirect resolution
//!    table by streaming through the dump without reading article text
//! 2. **Extraction Pass** -- Process articles in parallel to extract nodes, edges, categories,
//!    images, external links, and enriched content; output CSV files and JSON blobs
//! 3. **Merge Pass** (optional) -- Combine sharded CSV files into single files with
//!    deduplication for SurrealDB loading
//! 4. **Load Pass** -- Load article nodes and edges into embedded SurrealDB (RocksDB)
//! 5. **Analytics Pass** -- Compute PageRank, community detection, and degree metrics
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
//! - **Embedded database** -- SurrealDB with RocksDB backend, no external services needed
//!
//! # Key Modules
//!
//! - [`parser`] -- Streaming XML parser with BZ2 decompression
//! - [`index`] -- Title-to-ID mapping with redirect resolution
//! - [`extract`] -- Parallel extraction with CSV sharding
//! - [`merge`] -- CSV shard merging with deduplication
//! - [`surrealdb_writer`] -- SurrealDB embedded loader (reads CSVs, writes to RocksDB)
//! - [`analytics`] -- Graph analytics (PageRank, Louvain, degree)
//! - [`csv_util`] -- CSV layout detection and validation utilities
//! - [`content`] -- Text extraction (abstracts, sections, links, categories)
//! - [`infobox`] -- Structured infobox parsing with nested template support
//! - [`models`] -- Core data types (WikiPage, ArticleBlob, PageType)
//! - [`cache`] -- Index persistence with zero-copy serialization
//! - [`checkpoint`] -- Extraction progress checkpointing
//! - [`stats`] -- Thread-safe atomic counters for extraction metrics
//! - [`config`] -- Constants for extraction and loading

pub mod analytics;
pub mod cache;
pub mod checkpoint;
pub mod config;
pub mod content;
pub mod csv_util;
pub mod extract;
pub mod index;
pub mod infobox;
pub mod merge;
pub mod models;
pub mod multistream;
pub mod parser;
pub mod stats;
pub mod surrealdb_writer;
pub mod tui;

// Re-export primary API types for convenient library use.
pub use checkpoint::{Checkpoint, CheckpointManager};
pub use csv_util::CsvType;
pub use extract::ExtractionConfig;
pub use index::WikiIndex;
pub use models::{ArticleBlob, EdgeType, PageType, WikiPage};
pub use parser::WikiReader;
pub use stats::ExtractionStats;
pub use surrealdb_writer::SurrealWriterConfig;
