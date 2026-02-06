/// Maximum depth for following redirect chains
pub const REDIRECT_MAX_DEPTH: u32 = 5;

/// Number of shards for blob storage (shard = id % SHARD_COUNT)
pub const SHARD_COUNT: u32 = 1000;

/// Progress update interval (tick every N pages)
pub const PROGRESS_INTERVAL: u32 = 1000;

/// Version number for index cache format (bump when format changes)
pub const CACHE_VERSION: u32 = 1;

/// Version number for checkpoint format (bump when format changes)
pub const CHECKPOINT_VERSION: u32 = 2;

/// Default checkpoint interval (save checkpoint every N articles)
pub const CHECKPOINT_INTERVAL: u32 = 10_000;
