//! Extraction checkpoints for resumable processing.
//!
//! Extraction reads the dump in chunks and saves a checkpoint after each chunk
//! has been fully written and flushed. A checkpoint records how far into the
//! dump the run got (`ResumePoint`) and the length of every CSV file at that
//! moment. Resuming cuts each CSV back to its recorded length, discarding any
//! rows written after the checkpoint, and continues from the resume point, so
//! no page is skipped and none is written twice.
//!
//! Checkpoints are written atomically and removed when a run completes.

use crate::atomic_file::write_atomically;
use crate::config::CHECKPOINT_VERSION;
use crate::stats::ExtractionStats;
use anyhow::{Context, Result};
use bincode::Options;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tracing::{debug, info, warn};

/// Serializable snapshot of extraction counters for checkpoint persistence.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

/// How far into the dump a run has fully written its output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResumePoint {
    /// Reading a standard dump front to back: this many pages are done.
    AfterPages(u64),
    /// Reading a multistream dump: the first this-many streams are done.
    AfterStreams(usize),
}

/// What a checkpoint belongs to. A checkpoint is only used by a run with an
/// identical identity: same dump (path, size and modification time), same
/// output directory, same sharding, and same reading mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunIdentity {
    pub input_path: String,
    pub input_mtime: u64,
    pub input_size: u64,
    pub output_dir: String,
    pub shard_count: u32,
    pub csv_shards: u32,
    pub multistream: bool,
}

impl RunIdentity {
    /// Describes a run over `input_path`, reading the dump's size and mtime.
    pub fn of(
        input_path: &str,
        output_dir: &str,
        shard_count: u32,
        csv_shards: u32,
        multistream: bool,
    ) -> Result<Self> {
        let metadata = fs::metadata(input_path)
            .with_context(|| format!("Failed to get metadata for: {}", input_path))?;
        let input_mtime = metadata
            .modified()
            .context("Failed to get modification time")?
            .duration_since(SystemTime::UNIX_EPOCH)
            .context("Invalid modification time")?
            .as_secs();
        Ok(Self {
            input_path: input_path.to_string(),
            input_mtime,
            input_size: metadata.len(),
            output_dir: output_dir.to_string(),
            shard_count,
            csv_shards,
            multistream,
        })
    }
}

/// A saved extraction checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub version: u32,
    pub identity: RunIdentity,
    pub resume_point: ResumePoint,
    /// Pages read from the start of the dump, for `--limit`.
    pub pages_read: u64,
    /// Every CSV file's name and byte length when the checkpoint was taken.
    pub csv_lengths: Vec<(String, u64)>,
    pub stats: CheckpointStats,
}

/// Returns the path to the checkpoint file for a given output directory.
#[must_use]
pub fn checkpoint_path(output_dir: &str) -> PathBuf {
    Path::new(output_dir).join("checkpoint.bin")
}

/// Loads the checkpoint in the output directory if it belongs to this run.
pub fn load_if_valid(identity: &RunIdentity) -> Result<Option<Checkpoint>> {
    let path = checkpoint_path(&identity.output_dir);
    if !path.exists() {
        return Ok(None);
    }

    let file_size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    let file = File::open(&path).context("Failed to open checkpoint file")?;
    let reader = BufReader::with_capacity(crate::config::BUFREADER_CAPACITY, file);
    let options = bincode::options().with_limit(file_size.saturating_add(1024));

    let checkpoint: Checkpoint = match options.deserialize_from(reader) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Checkpoint file is corrupt or from an older version");
            return Ok(None);
        }
    };

    if checkpoint.version != CHECKPOINT_VERSION {
        info!(
            saved = checkpoint.version,
            current = CHECKPOINT_VERSION,
            "Checkpoint version mismatch"
        );
        return Ok(None);
    }

    if checkpoint.identity != *identity {
        info!(
            saved = ?checkpoint.identity,
            current = ?identity,
            "Checkpoint belongs to a different run"
        );
        return Ok(None);
    }

    info!(
        resume_point = ?checkpoint.resume_point,
        articles = checkpoint.stats.articles_processed,
        "Loaded valid checkpoint"
    );
    Ok(Some(checkpoint))
}

/// Removes the checkpoint file for the given output directory.
pub fn clear(output_dir: &str) -> Result<()> {
    let path = checkpoint_path(output_dir);
    if path.exists() {
        fs::remove_file(&path)
            .with_context(|| format!("Failed to remove checkpoint file: {:?}", path))?;
        info!("Checkpoint cleared");
    }
    Ok(())
}

/// Saves checkpoints for one run.
#[derive(Debug)]
pub struct CheckpointManager {
    identity: RunIdentity,
}

impl CheckpointManager {
    #[must_use]
    pub fn new(identity: RunIdentity) -> Self {
        Self { identity }
    }

    /// Writes a checkpoint. Call only after every row and blob up to
    /// `resume_point` has been written and flushed.
    pub fn save(
        &self,
        resume_point: ResumePoint,
        pages_read: u64,
        csv_lengths: Vec<(String, u64)>,
        stats: &ExtractionStats,
    ) -> Result<()> {
        let checkpoint = Checkpoint {
            version: CHECKPOINT_VERSION,
            identity: self.identity.clone(),
            resume_point,
            pages_read,
            csv_lengths,
            stats: stats.to_checkpoint(),
        };

        write_atomically(&checkpoint_path(&self.identity.output_dir), |writer| {
            bincode::DefaultOptions::new()
                .serialize_into(writer, &checkpoint)
                .context("Failed to serialize checkpoint")
        })?;

        debug!(resume_point = ?resume_point, articles = stats.articles(), "Checkpoint saved");
        Ok(())
    }

    /// Removes the checkpoint file (called after successful extraction).
    pub fn clear(&self) -> Result<()> {
        clear(&self.identity.output_dir)
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

    fn identity(input: &Path, output: &str, shard_count: u32) -> RunIdentity {
        RunIdentity::of(input.to_str().unwrap(), output, shard_count, 1, false).unwrap()
    }

    fn save_sample(id: &RunIdentity, stats: &ExtractionStats) {
        CheckpointManager::new(id.clone())
            .save(
                ResumePoint::AfterPages(42),
                42,
                vec![("nodes.csv".to_string(), 100)],
                stats,
            )
            .unwrap();
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
        let id = identity(&input_path, dir.path().to_str().unwrap(), 1000);
        assert!(load_if_valid(&id).unwrap().is_none());
    }

    #[test]
    fn checkpoint_manager_save_and_load() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let id = identity(&input_path, dir.path().to_str().unwrap(), 1000);

        let stats = ExtractionStats::new();
        stats.inc_articles();
        stats.inc_articles();
        stats.add_edges(10);
        save_sample(&id, &stats);

        let loaded = load_if_valid(&id).unwrap().unwrap();
        assert_eq!(loaded.resume_point, ResumePoint::AfterPages(42));
        assert_eq!(loaded.pages_read, 42);
        assert_eq!(loaded.csv_lengths, vec![("nodes.csv".to_string(), 100)]);
        assert_eq!(loaded.stats.articles_processed, 2);
        assert_eq!(loaded.stats.edges_extracted, 10);
    }

    #[test]
    fn checkpoint_invalidated_by_input_change() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let output_dir = dir.path().to_str().unwrap();
        save_sample(
            &identity(&input_path, output_dir, 1000),
            &ExtractionStats::new(),
        );

        // A different size is caught even within the same mtime second.
        let mut file = File::create(&input_path).unwrap();
        writeln!(file, "modified content that is longer").unwrap();

        let id = identity(&input_path, output_dir, 1000);
        assert!(load_if_valid(&id).unwrap().is_none());
    }

    #[test]
    fn checkpoint_invalidated_by_shard_count_change() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let output_dir = dir.path().to_str().unwrap();
        save_sample(
            &identity(&input_path, output_dir, 1000),
            &ExtractionStats::new(),
        );

        let id = identity(&input_path, output_dir, 500);
        assert!(load_if_valid(&id).unwrap().is_none());
    }

    #[test]
    fn checkpoint_invalidated_by_reading_mode_change() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let output_dir = dir.path().to_str().unwrap();
        save_sample(
            &identity(&input_path, output_dir, 1000),
            &ExtractionStats::new(),
        );

        let mut id = identity(&input_path, output_dir, 1000);
        id.multistream = true;
        assert!(load_if_valid(&id).unwrap().is_none());
    }

    #[test]
    fn clear_removes_checkpoint() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let output_dir = dir.path().to_str().unwrap();
        save_sample(
            &identity(&input_path, output_dir, 1000),
            &ExtractionStats::new(),
        );

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
    fn corrupt_checkpoint_returns_none() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let output_dir = dir.path().to_str().unwrap();
        let checkpoint_file = checkpoint_path(output_dir);

        let mut file = File::create(&checkpoint_file).unwrap();
        file.write_all(b"not valid bincode").unwrap();

        let id = identity(&input_path, output_dir, 1000);
        assert!(load_if_valid(&id).unwrap().is_none());
    }
}
