//! Compile-time constants for extraction and import configuration.
//!
//! Includes buffer sizes, retry parameters, hash map capacities, and default
//! values for CLI flags.

// -- Extraction constants --

/// Maximum depth for following redirect chains.
pub const REDIRECT_MAX_DEPTH: u32 = 5;

/// Number of shards for blob storage (shard = id % SHARD_COUNT).
pub const SHARD_COUNT: u32 = 1000;

/// Progress update interval (tick every N pages).
pub const PROGRESS_INTERVAL: u32 = 1000;

/// Index cache format version. Bump when the format changes.
pub const CACHE_VERSION: u32 = 2;

/// Checkpoint format version. Bump when the format changes.
pub const CHECKPOINT_VERSION: u32 = 3;

/// Save a checkpoint every N articles.
pub const CHECKPOINT_INTERVAL: u32 = 10_000;

// -- Buffer / capacity constants --

/// BufWriter capacity for CSV shard writers (128 KB).
pub const CSV_WRITER_BUF_SIZE: usize = 128 * 1024;

/// BufReader / BufWriter capacity for merge operations (256 KB).
pub const MERGE_BUF_SIZE: usize = 256 * 1024;

/// BufReader capacity for BZ2 decompression and cache/checkpoint I/O (256 KB).
pub const BUFREADER_CAPACITY: usize = 256 * 1024;

/// Pre-sized capacity for the title-to-ID hash map.
pub const INDEX_INITIAL_ARTICLES: usize = 8_000_000;

/// Pre-sized capacity for the redirect resolution hash map.
pub const INDEX_INITIAL_REDIRECTS: usize = 10_000_000;

// -- SurrealDB constants --

/// SurrealDB namespace.
pub const SURREAL_NAMESPACE: &str = "dedalus";

/// SurrealDB database name.
pub const SURREAL_DATABASE: &str = "wikipedia";

/// Default batch size for SurrealDB inserts.
pub const SURREAL_BATCH_SIZE: usize = 10_000;

/// Default database path (relative to output directory).
pub const DEFAULT_DB_PATH: &str = "wikipedia.db";

// -- Analytics constants --

/// Default number of PageRank power iterations.
pub const PAGERANK_ITERATIONS: u32 = 20;

/// PageRank damping factor.
pub const PAGERANK_DAMPING: f64 = 0.85;

/// PageRank convergence threshold.
pub const PAGERANK_EPSILON: f64 = 1e-6;

/// Maximum iterations for Louvain / label propagation.
pub const LOUVAIN_MAX_ITERATIONS: u32 = 50;
