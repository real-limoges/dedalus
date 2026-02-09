//! Dedalus: Wikipedia dump extraction and Neo4j import pipeline
//!
//! This crate provides a two-pass pipeline for extracting structured graph data from
//! Wikipedia XML dumps and importing it into Neo4j:
//!
//! 1. **Indexing Pass** -- Build an in-memory title-to-ID mapping and redirect resolution
//!    table by streaming through the dump without reading article text
//! 2. **Extraction Pass** -- Process articles in parallel to extract nodes, edges, categories,
//!    images, external links, and enriched content; output CSV files and JSON blobs
//! 3. **Merge Pass** (optional) -- Combine sharded CSV files into single files with
//!    deduplication for neo4j-admin compatibility
//! 4. **Import Pass** -- Load CSV data into Neo4j using either neo4j-admin bulk import
//!    (10-100x faster) or Bolt protocol (slower but works with existing data)
//!
//! # Architecture
//!
//! The pipeline is designed for performance and memory efficiency:
//!
//! - **Streaming XML parsing** -- Never loads full dump into memory; uses event-based parsing
//! - **Parallel extraction** -- Uses rayon to process articles concurrently
//! - **CSV sharding** -- Distributes output across N files for parallel import
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
//! - [`import`] -- Neo4j import via admin tool or Bolt protocol
//! - [`content`] -- Text extraction (abstracts, sections, links, categories)
//! - [`infobox`] -- Structured infobox parsing with nested template support
//! - [`models`] -- Core data types (WikiPage, ArticleBlob, PageType)
//! - [`cache`] -- Index persistence with zero-copy serialization
//! - [`checkpoint`] -- Extraction progress checkpointing
//! - [`stats`] -- Thread-safe atomic counters for extraction metrics
//! - [`config`] -- Constants for extraction and import
//!
//! # Performance Optimizations
//!
//! - **FxHashMap** instead of SipHash for trusted input (faster, no DoS risk)
//! - **Pre-sized collections** -- 8M articles, 10M redirects to avoid reallocation
//! - **String allocation** -- Single-pass building in hot paths
//! - **Buffer sizes** -- 128KB for CSV writers, 256KB for merge operations
//! - **Batch writes** -- Collect locally, lock once to reduce contention
//! - **M1 CPU targeting** -- NEON SIMD via target-cpu=native
//!
//! # Example Usage
//!
//! ```bash
//! # Extract with 8 shards for parallel processing
//! dedalus extract -i enwiki-latest-pages-articles.xml.bz2 -o output/ --csv-shards 8
//!
//! # Merge sharded CSVs for neo4j-admin import
//! dedalus merge-csvs -o output/
//!
//! # Import using fast bulk import
//! dedalus import -o output/ --admin-import
//! ```

pub mod cache;
pub mod checkpoint;
pub mod config;
pub mod content;
pub mod extract;
pub mod import;
pub mod index;
pub mod infobox;
pub mod merge;
pub mod models;
pub mod parser;
pub mod stats;
