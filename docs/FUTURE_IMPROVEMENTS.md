# Dedalus Future Improvements

Phases 1-3 have been completed:

- **Phase 1 (Core Architecture)** -- two-pass pipeline, streaming XML parser, in-memory index, parallel extraction via rayon
- **Phase 2 (Data Extraction)** -- wikilink regex extraction, Neo4J-compatible CSV output (nodes/edges), sharded JSON blob storage
- **Phase 3 (CLI & Observability)** -- `clap` CLI with `--input`, `--output`, `--shard-count`, `--limit`, `--dry-run`, `--verbose`; `indicatif` progress bars; `tracing` structured logging; atomic extraction statistics with summary output

The remaining phases are listed below.

---

## Phase 4: Testing

### 4.1 Unit Tests
- **parser.rs**: Test XML parsing, redirect detection, page type classification
- **index.rs**: Test redirect resolution, depth limits, circular redirects
- **extract.rs**: Test regex patterns, shard calculation
- **models.rs**: Test serialization/deserialization

### 4.2 Integration Tests
- Create `tests/` directory with sample Wikipedia XML snippets
- Test end-to-end flow with small dataset
- Validate output file formats

---

## Phase 5: Resumable Processing

### 5.1 Index Persistence
- **File**: `src/index.rs`
- Serialize `WikiIndex` to disk after building
- Load from cache on subsequent runs (with timestamp validation)
- Use `bincode` for efficient serialization

### 5.2 Checkpoint System
- **File**: `src/extract.rs`
- Track last processed article ID
- Save checkpoint every N articles
- Add `--resume` CLI flag to continue from checkpoint

---

## Phase 6: Feature Enhancements

### 6.1 Category Extraction
- **Files**: `src/parser.rs`, `src/models.rs`, `src/extract.rs`
- Parse `[[Category:Name]]` patterns from article text
- Output `categories.csv` with category nodes
- Output `article_categories.csv` for HAS_CATEGORY edges

### 6.2 Additional Output Formats
- **JSONL**: Line-delimited JSON (streaming-friendly)
- **Compressed output**: gzip CSV files
- Add `--format` CLI option

### 6.3 More Namespace Support
- Currently only filters File:, Category:, Template:
- Add handling for: User:, Wikipedia:, Help:, MediaWiki:, Portal:, Draft:

---

## Phase 7: Neo4j Direct Import

### 7.1 Neo4j Driver Integration
- **File**: New `src/neo4j.rs`
- Add `neo4rs` crate for async Neo4j Bolt protocol support
- Connection configuration via CLI or environment variables:
  ```
  --neo4j-uri bolt://localhost:7687
  --neo4j-user neo4j
  --neo4j-password <password>
  ```
- Connection pooling for parallel writes

### 7.2 Batch Import
- **File**: `src/neo4j.rs`, `src/extract.rs`
- Batch nodes/edges into chunks (e.g., 1000 per transaction)
- Use `UNWIND` for efficient bulk inserts:
  ```cypher
  UNWIND $nodes AS node
  CREATE (p:Page {id: node.id, title: node.title})
  ```
- Configurable batch size via `--batch-size` CLI option

### 7.3 Output Mode Selection
- **File**: `src/main.rs`
- Add `--output-mode` flag: `csv` (default), `neo4j`, `both`
- Skip CSV writing when using neo4j-only mode

---

## Phase 8: Neo4j Schema Setup

### 8.1 Constraints and Indexes
- **File**: `src/neo4j.rs`
- Auto-create schema before import:
  ```cypher
  CREATE CONSTRAINT page_id IF NOT EXISTS FOR (p:Page) REQUIRE p.id IS UNIQUE;
  CREATE INDEX page_title IF NOT EXISTS FOR (p:Page) ON (p.title);
  ```
- Add `--skip-schema` flag to skip if already exists

### 8.2 Category Schema (if Phase 6 implemented)
- Additional constraints for Category nodes
- Index on category names for fast lookup

### 8.3 Schema Validation
- Verify constraints exist before bulk import
- Warn if indexes are missing (performance impact)

---

## Phase 9: Verification & Querying

### 9.1 Import Verification
- **File**: `src/neo4j.rs`
- Post-import validation queries:
  ```cypher
  MATCH (p:Page) RETURN count(p) AS node_count;
  MATCH ()-[r:LINKS_TO]->() RETURN count(r) AS edge_count;
  ```
- Compare counts against extraction stats
- Report discrepancies

### 9.2 Sample Queries
- **File**: New `src/queries.rs`
- Built-in query templates:
  - Find page by title
  - Get outgoing/incoming links
  - Find shortest path between pages
  - Most linked-to pages (PageRank-style)
- Add `--query` CLI mode for interactive exploration

### 9.3 Health Check
- `--ping` flag to test Neo4j connectivity
- Report database version and cluster status

---

## Phase 10: Parallelization Improvements

### 10.1 Multi-Stream BZ2 Decompression
- **File**: New `src/decompress.rs`, `src/parser.rs`
- Wikipedia dumps are BZ2 files with multiple independent streams
- Decompress multiple streams in parallel using `rayon`
- Feed decompressed chunks to XML parser via bounded channel

### 10.2 Producer-Consumer XML Parsing
- **File**: `src/parser.rs`, `src/extract.rs`
- Current: Sequential read -> parallel process via `par_bridge()`
- Improved: Dedicated reader thread + worker pool via `crossbeam-channel`
- Buffer size tunable via `--buffer-size` CLI option

### 10.3 Lock-Free Edge Collection
- **File**: `src/extract.rs`
- Current: `Arc<Mutex<Writer>>` with batch writes
- Options: per-thread edge buffers, `crossbeam-queue::SegQueue`, or sharded writers

### 10.4 Parallel Shard Writing
- **File**: `src/extract.rs`
- Pre-create shard directories, use async I/O via `tokio::fs`
- Batch writes per shard to reduce syscall overhead

### 10.5 Parallel Indexing Pass
- **File**: `src/index.rs`
- Options: thread-local HashMaps merged at end, or `DashMap` concurrent HashMap
- Note: May be I/O bound -- profile before optimizing

### 10.6 Benchmarking Infrastructure
- **File**: New `benches/throughput.rs`
- Use `criterion` for micro-benchmarks
- Metrics: pages/second, edges/second, memory high-water mark, per-phase timing

---

## Phase 11: Error Handling & Resilience

### 11.1 Graceful XML Error Recovery
- **File**: `src/parser.rs`
- Skip malformed pages, log error, continue processing
- Track error count and report summary at end
- Add `--strict` flag to fail-fast if preferred

### 11.2 Structured Error Types
- **File**: New `src/error.rs`
- Define `DedalusError` enum with variants: `IoError`, `XmlParseError`, `IndexError`, `SerializationError`
- Use `thiserror` for derive macros

### 11.3 Retry Logic
- **File**: `src/neo4j.rs` (Phase 7+)
- Exponential backoff for transient Neo4j failures
- Configurable max retries via `--max-retries`

---

## Phase 12: Observability Enhancements

Progress bars (`indicatif`), structured logging (`tracing`), and extraction statistics are already implemented. Remaining items:

### 12.1 Metrics Export
- **File**: New `src/metrics.rs`
- Prometheus-compatible metrics endpoint (optional feature)
- Metrics: throughput, memory usage, error rates, phase durations

### 12.2 JSON Log Format
- Optional `--log-format json` for structured log output
- Useful for log aggregation in production deployments

---

## Phase 13: Memory Optimization

### 13.1 String Interning
- Use `lasso` or `string-interner` crate
- Deduplicate repeated strings (titles appear in index + edges)

### 13.2 Memory-Mapped Reading
- Use `memmap2` for memory-mapped file access
- Let OS handle paging for files larger than RAM

### 13.3 Arena Allocation
- Use `bumpalo` for batch allocations during page processing
- Reset arena after each page for zero fragmentation

---

## Phase 14: Input Flexibility

### 14.1 Multiple Input Files
- Accept multiple `--input` arguments or glob patterns
- Process dumps in sequence or parallel

### 14.2 Compression Format Support
- Support: uncompressed XML, Zstandard (`.zst`), XZ/LZMA (`.xz`), gzip (`.gz`)
- Auto-detect from file extension or magic bytes

### 14.3 Streaming from URL
- Accept `--input https://dumps.wikimedia.org/...`
- Stream directly from Wikimedia without local download via `reqwest`

### 14.4 Incremental Updates
- Fetch recent changes from Wikipedia API
- Update index and edges incrementally

---

## Phase 15: Output Flexibility

### 15.1 Parquet Output
- Use `parquet` crate for columnar output
- Add `--format parquet` option

### 15.2 SQLite Output
- Direct SQLite database output via `rusqlite`
- Self-contained, no Neo4j required

### 15.3 Arrow IPC Format
- Use `arrow` crate for zero-copy interop with Python/Pandas/DuckDB

### 15.4 Configurable CSV Options
- Custom delimiter, quote style, header toggle, compression (`.csv.gz`, `.csv.zst`)

---

## Phase 16: Content Processing

### 16.1 Wikitext Parsing
- Use `parse_wiki_text` or similar crate
- Extract structured content beyond regex links
- Handle templates, references, formatting

### 16.2 Infobox Extraction
- Parse infobox templates into structured key-value pairs
- Output as `infoboxes.json` or additional CSV columns

### 16.3 Section Extraction
- Parse `== Section ==` headers for article structure/outline

### 16.4 Plain Text Extraction
- Strip all wiki markup for clean text output (NLP/ML pipelines)

---

## Phase 17: Graph Analysis

### 17.1 In-Memory Graph
- Build `petgraph` representation from edges

### 17.2 PageRank Computation
- Compute PageRank scores, output as additional column in nodes.csv

### 17.3 Connected Components
- Find strongly/weakly connected components, identify orphan articles

### 17.4 Graph Statistics
- Node/edge count, density, degree distribution, diameter estimation

---

## Phase 18: Deployment & Distribution

### 18.1 Docker Container
- Multi-stage build for small image
- Publish to GitHub Container Registry

### 18.2 Pre-built Binaries
- GitHub Actions release workflow for Linux x86_64, macOS x86_64/ARM, Windows

### 18.3 Package Manager Support
- Homebrew formula, AUR package, `cargo install` from crates.io

---

## Phase 19: Configuration & UX

### 19.1 Config File Support
- Support `dedalus.toml` config file; CLI args override config

### 19.2 Environment Variables
- `DEDALUS_INPUT`, `DEDALUS_OUTPUT`, etc. for containerized deployments

### 19.3 Interactive Mode
- Prompt for missing required arguments with defaults

---

## Phase 20: Documentation

### 20.1 API Documentation
- `cargo doc` with module-level documentation on all public types

### 20.2 Usage Examples
- `examples/` directory with common workflows and integration examples

### 20.3 Architecture Documentation
- `docs/architecture.md` with Mermaid data flow diagrams

### 20.4 Performance Tuning Guide
- `docs/performance.md` with hardware recommendations and optimal settings

---

## Dependencies to Add

Crates already in use are omitted. These are needed for future phases:

| Crate | Phase | Purpose |
|-------|-------|---------|
| `bincode` | 5 | Index serialization |
| `neo4rs` | 7-9 | Async Neo4j driver |
| `tokio` | 7-10 | Async runtime |
| `crossbeam-channel` | 10 | Bounded MPMC channels |
| `dashmap` | 10 | Concurrent HashMap |
| `criterion` | 10 | Benchmarking framework |
| `thiserror` | 11 | Error derive macros |
| `lasso` | 13 | String interning |
| `memmap2` | 13 | Memory-mapped files |
| `bumpalo` | 13 | Arena allocation |
| `zstd` | 14 | Zstandard compression |
| `xz2` | 14 | XZ/LZMA compression |
| `flate2` | 14 | Gzip compression |
| `reqwest` | 14 | HTTP streaming |
| `parquet` | 15 | Columnar output |
| `arrow` | 15 | Arrow IPC format |
| `rusqlite` | 15 | SQLite output |
| `parse_wiki_text` | 16 | Wikitext parsing |
| `petgraph` | 17 | Graph data structures |
| `toml` | 19 | Config file parsing |
