use crate::config::CHECKPOINT_VERSION;
use crate::stats::ExtractionStats;
use anyhow::{Context, Result};
use bincode::Options;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::time::SystemTime;
use tracing::{debug, info, warn};

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct CheckpointStats {
    pub articles_processed: u64,
    pub edges_extracted: u64,
    pub blobs_written: u64,
    pub invalid_links: u64,
    pub categories_found: u64,
    pub category_edges: u64,
    pub see_also_edges: u64,
    pub infoboxes_extracted: u64,
    pub images_found: u64,
    pub external_links_found: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Checkpoint {
    pub version: u32,
    pub input_path: String,
    pub input_mtime: u64,
    pub output_dir: String,
    pub shard_count: u32,
    pub last_processed_id: u32,
    pub stats: CheckpointStats,
}

pub fn checkpoint_path(output_dir: &str) -> PathBuf {
    Path::new(output_dir).join("checkpoint.bin")
}

fn get_input_mtime(input_path: &str) -> Result<u64> {
    let metadata = fs::metadata(input_path)
        .with_context(|| format!("Failed to get metadata for: {}", input_path))?;
    let mtime = metadata
        .modified()
        .context("Failed to get modification time")?
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("Invalid modification time")?
        .as_secs();
    Ok(mtime)
}

pub fn load_if_valid(
    input_path: &str,
    output_dir: &str,
    shard_count: u32,
) -> Result<Option<Checkpoint>> {
    let path = checkpoint_path(output_dir);

    if !path.exists() {
        return Ok(None);
    }

    let file_size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    let file = File::open(&path).context("Failed to open checkpoint file")?;
    let reader = BufReader::new(file);

    let options = bincode::options().with_limit(file_size.saturating_add(1024));

    let checkpoint: Checkpoint = match options.deserialize_from(reader) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Checkpoint file is corrupt or unreadable");
            return Ok(None);
        }
    };

    if checkpoint.version != CHECKPOINT_VERSION {
        info!(
            cached = checkpoint.version,
            current = CHECKPOINT_VERSION,
            "Checkpoint version mismatch"
        );
        return Ok(None);
    }

    if checkpoint.input_path != input_path {
        info!(
            cached = checkpoint.input_path,
            current = input_path,
            "Checkpoint input path mismatch"
        );
        return Ok(None);
    }

    let current_mtime = get_input_mtime(input_path)?;
    if checkpoint.input_mtime != current_mtime {
        info!(
            cached_mtime = checkpoint.input_mtime,
            current_mtime = current_mtime,
            "Input file has changed since checkpoint was created"
        );
        return Ok(None);
    }

    if checkpoint.output_dir != output_dir {
        info!(
            cached = checkpoint.output_dir,
            current = output_dir,
            "Checkpoint output directory mismatch"
        );
        return Ok(None);
    }

    if checkpoint.shard_count != shard_count {
        info!(
            cached = checkpoint.shard_count,
            current = shard_count,
            "Checkpoint shard count mismatch"
        );
        return Ok(None);
    }

    info!(
        last_id = checkpoint.last_processed_id,
        articles = checkpoint.stats.articles_processed,
        "Loaded valid checkpoint"
    );

    Ok(Some(checkpoint))
}

pub fn clear(output_dir: &str) -> Result<()> {
    let path = checkpoint_path(output_dir);
    if path.exists() {
        fs::remove_file(&path)
            .with_context(|| format!("Failed to remove checkpoint file: {:?}", path))?;
        info!("Checkpoint cleared");
    }
    Ok(())
}

pub struct CheckpointManager {
    checkpoint_path: PathBuf,
    input_path: String,
    input_mtime: u64,
    output_dir: String,
    shard_count: u32,
    interval: u32,
    last_saved_id: AtomicU32,
    pages_since_save: AtomicU32,
    save_lock: Mutex<()>,
}

impl CheckpointManager {
    pub fn new(
        input_path: &str,
        output_dir: &str,
        shard_count: u32,
        interval: u32,
    ) -> Result<Self> {
        let input_mtime = get_input_mtime(input_path)?;
        Ok(Self {
            checkpoint_path: checkpoint_path(output_dir),
            input_path: input_path.to_string(),
            input_mtime,
            output_dir: output_dir.to_string(),
            shard_count,
            interval,
            last_saved_id: AtomicU32::new(0),
            pages_since_save: AtomicU32::new(0),
            save_lock: Mutex::new(()),
        })
    }

    pub fn set_last_id(&self, id: u32) {
        self.last_saved_id.store(id, Ordering::Relaxed);
    }

    /// Double-checked locking: atomic counter for fast path, mutex for serialized saves.
    pub fn maybe_save(&self, page_id: u32, stats: &ExtractionStats) -> Result<bool> {
        let count = self.pages_since_save.fetch_add(1, Ordering::Relaxed) + 1;

        if count >= self.interval {
            let _guard = match self.save_lock.lock() {
                Ok(guard) => guard,
                Err(e) => {
                    warn!(error = %e, "Checkpoint save lock poisoned, skipping save");
                    return Ok(false);
                }
            };

            let current = self.pages_since_save.load(Ordering::Relaxed);
            if current < self.interval {
                return Ok(false);
            }

            self.save(page_id, stats)?;
            self.pages_since_save.store(0, Ordering::Relaxed);
            return Ok(true);
        }

        Ok(false)
    }

    pub fn save(&self, page_id: u32, stats: &ExtractionStats) -> Result<()> {
        let checkpoint = Checkpoint {
            version: CHECKPOINT_VERSION,
            input_path: self.input_path.clone(),
            input_mtime: self.input_mtime,
            output_dir: self.output_dir.clone(),
            shard_count: self.shard_count,
            last_processed_id: page_id,
            stats: stats.to_checkpoint(),
        };

        if let Some(parent) = self.checkpoint_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {:?}", parent))?;
        }

        let tmp_path = self.checkpoint_path.with_extension("bin.tmp");
        let file = File::create(&tmp_path)
            .with_context(|| format!("Failed to create temp checkpoint file: {:?}", tmp_path))?;
        let writer = BufWriter::new(file);

        bincode::DefaultOptions::new()
            .serialize_into(writer, &checkpoint)
            .context("Failed to serialize checkpoint")?;

        fs::rename(&tmp_path, &self.checkpoint_path).with_context(|| {
            format!(
                "Failed to rename temp checkpoint: {:?}",
                self.checkpoint_path
            )
        })?;

        self.last_saved_id.store(page_id, Ordering::Relaxed);

        debug!(
            page_id = page_id,
            articles = stats.articles(),
            "Checkpoint saved"
        );

        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        clear(&self.output_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_input(dir: &TempDir) -> PathBuf {
        let path = dir.path().join("test_input.txt");
        let mut file = File::create(&path).unwrap();
        writeln!(file, "test content").unwrap();
        path
    }

    #[test]
    fn checkpoint_path_returns_correct_path() {
        let path = checkpoint_path("/output/dir");
        assert_eq!(path, PathBuf::from("/output/dir/checkpoint.bin"));
    }

    #[test]
    fn load_if_valid_returns_none_when_no_checkpoint() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let result = load_if_valid(
            input_path.to_str().unwrap(),
            dir.path().to_str().unwrap(),
            1000,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn checkpoint_manager_save_and_load() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let input_str = input_path.to_str().unwrap();
        let output_dir = dir.path().to_str().unwrap();

        let manager = CheckpointManager::new(input_str, output_dir, 1000, 100).unwrap();

        let stats = ExtractionStats::new();
        stats.inc_articles();
        stats.inc_articles();
        stats.add_edges(10);

        manager.save(42, &stats).unwrap();

        let loaded = load_if_valid(input_str, output_dir, 1000).unwrap().unwrap();
        assert_eq!(loaded.last_processed_id, 42);
        assert_eq!(loaded.stats.articles_processed, 2);
        assert_eq!(loaded.stats.edges_extracted, 10);
    }

    #[test]
    fn checkpoint_invalidated_by_input_change() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let input_str = input_path.to_str().unwrap();
        let output_dir = dir.path().to_str().unwrap();

        let manager = CheckpointManager::new(input_str, output_dir, 1000, 100).unwrap();
        manager.save(42, &ExtractionStats::new()).unwrap();

        // mtime has second granularity
        std::thread::sleep(std::time::Duration::from_secs(1));
        let mut file = File::create(&input_path).unwrap();
        writeln!(file, "modified content").unwrap();

        let loaded = load_if_valid(input_str, output_dir, 1000).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn checkpoint_invalidated_by_shard_count_change() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let input_str = input_path.to_str().unwrap();
        let output_dir = dir.path().to_str().unwrap();

        let manager = CheckpointManager::new(input_str, output_dir, 1000, 100).unwrap();
        manager.save(42, &ExtractionStats::new()).unwrap();

        let loaded = load_if_valid(input_str, output_dir, 500).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn checkpoint_invalidated_by_output_dir_change() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let input_str = input_path.to_str().unwrap();
        let output_dir = dir.path().to_str().unwrap();

        let manager = CheckpointManager::new(input_str, output_dir, 1000, 100).unwrap();
        manager.save(42, &ExtractionStats::new()).unwrap();

        let loaded = load_if_valid(input_str, "/different/output", 1000).unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn clear_removes_checkpoint() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let input_str = input_path.to_str().unwrap();
        let output_dir = dir.path().to_str().unwrap();

        let manager = CheckpointManager::new(input_str, output_dir, 1000, 100).unwrap();
        manager.save(42, &ExtractionStats::new()).unwrap();

        let path = checkpoint_path(output_dir);
        assert!(path.exists());

        clear(output_dir).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn clear_ok_when_no_checkpoint() {
        let dir = TempDir::new().unwrap();
        let output_dir = dir.path().to_str().unwrap();
        assert!(clear(output_dir).is_ok());
    }

    #[test]
    fn maybe_save_respects_interval() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let input_str = input_path.to_str().unwrap();
        let output_dir = dir.path().to_str().unwrap();

        let manager = CheckpointManager::new(input_str, output_dir, 1000, 3).unwrap();
        let stats = ExtractionStats::new();

        assert!(!manager.maybe_save(1, &stats).unwrap());
        assert!(!manager.maybe_save(2, &stats).unwrap());
        assert!(manager.maybe_save(3, &stats).unwrap());

        // Counter resets
        assert!(!manager.maybe_save(4, &stats).unwrap());
        assert!(!manager.maybe_save(5, &stats).unwrap());
        assert!(manager.maybe_save(6, &stats).unwrap());
    }

    #[test]
    fn corrupt_checkpoint_returns_none() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let checkpoint_file = checkpoint_path(dir.path().to_str().unwrap());

        let mut file = File::create(&checkpoint_file).unwrap();
        file.write_all(b"not valid bincode").unwrap();

        let result = load_if_valid(
            input_path.to_str().unwrap(),
            dir.path().to_str().unwrap(),
            1000,
        )
        .unwrap();
        assert!(result.is_none());
    }
}
