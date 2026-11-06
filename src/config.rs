//! Compile-time constants for extraction, merge and analytics.
//!
//! Includes buffer sizes, hash map capacities, file format versions, and
//! default values for CLI flags.

// -- Extraction constants --

/// Maximum depth for following redirect chains.
pub const REDIRECT_MAX_DEPTH: u32 = 5;

/// Number of shards for blob storage (shard = id % SHARD_COUNT).
pub const SHARD_COUNT: u32 = 1000;

/// Progress update interval (tick every N pages).
pub const PROGRESS_INTERVAL: u32 = 1000;

/// Index cache format version. Bump when the format or the key form changes
/// (version 3: titles are stored in `title::normalize_title` form).
pub const CACHE_VERSION: u32 = 3;

/// Checkpoint format version. Bump when the format changes
/// (version 4: resume points, CSV lengths and run identity).
pub const CHECKPOINT_VERSION: u32 = 4;

/// Pages per extraction chunk. A checkpoint is saved after every chunk.
pub const CHECKPOINT_INTERVAL: u32 = 10_000;

/// Default number of CSV output shards.
pub const DEFAULT_CSV_SHARDS: u32 = 8;

/// Approximate pages per stream in a Wikipedia multistream dump, used to turn
/// a chunk size in pages into a number of streams.
pub const PAGES_PER_STREAM: usize = 100;

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

// -- Analytics constants --

/// Default number of PageRank power iterations.
pub const PAGERANK_ITERATIONS: u32 = 20;

/// PageRank damping factor.
pub const PAGERANK_DAMPING: f64 = 0.85;

/// PageRank convergence threshold.
pub const PAGERANK_EPSILON: f64 = 1e-6;

/// Maximum passes of label propagation community detection.
pub const LABEL_PROPAGATION_MAX_ITERATIONS: u32 = 50;
