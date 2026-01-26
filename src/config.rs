/// Maximum depth for following redirect chains
pub const REDIRECT_MAX_DEPTH: u32 = 5;

/// Number of shards for blob storage (shard = id % SHARD_COUNT)
pub const SHARD_COUNT: u32 = 1000;

/// Progress update interval (tick every N pages)
pub const PROGRESS_INTERVAL: u32 = 1000;
