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

/// Default Bolt URI for Neo4j connection.
pub const DEFAULT_BOLT_URI: &str = "bolt://localhost:7687";

/// Maximum connection retry attempts.
pub const IMPORT_MAX_RETRIES: u32 = 30;

/// Delay between connection retry attempts in seconds.
pub const IMPORT_RETRY_DELAY_SECS: u64 = 2;

/// Default max parallel edge loads (serialized due to memory pressure).
pub const IMPORT_MAX_PARALLEL_EDGES: usize = 1;

/// Default max parallel light relationship loads.
pub const IMPORT_MAX_PARALLEL_LIGHT: usize = 4;

/// Default import file URI prefix for Neo4j LOAD CSV.
pub const DEFAULT_IMPORT_PREFIX: &str = "file://";
