//! Crash-safe file replacement.
//!
//! The file is written to a `.tmp` sibling, flushed, synced to disk, and only
//! then renamed over the target. A crash or a full disk at any point leaves
//! either the old file or the complete new one, never a truncated file under
//! the final name, and every write error is returned rather than lost in a
//! `BufWriter`'s drop.

use anyhow::{Context, Result};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

/// Replaces `path` with whatever `write` produces.
pub fn write_atomically(
    path: &Path,
    write: impl FnOnce(&mut BufWriter<File>) -> Result<()>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    let mut tmp_name = path.as_os_str().to_owned();
    tmp_name.push(".tmp");
    let tmp_path = Path::new(&tmp_name);

    let file = File::create(tmp_path)
        .with_context(|| format!("Failed to create {}", tmp_path.display()))?;
    let mut writer = BufWriter::new(file);
    write(&mut writer)?;
    writer
        .flush()
        .with_context(|| format!("Failed to write {}", tmp_path.display()))?;
    let file = writer
        .into_inner()
        .map_err(|e| e.into_error())
        .with_context(|| format!("Failed to write {}", tmp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("Failed to sync {}", tmp_path.display()))?;

    fs::rename(tmp_path, path)
        .with_context(|| format!("Failed to rename {} into place", tmp_path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn replaces_the_file_and_leaves_no_tmp() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        fs::write(&path, b"old").unwrap();

        write_atomically(&path, |w| Ok(w.write_all(b"new")?)).unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"new");
        assert!(!dir.path().join("state.bin.tmp").exists());
    }

    #[test]
    fn a_failed_write_keeps_the_old_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.bin");
        fs::write(&path, b"old").unwrap();

        let result = write_atomically(&path, |_| anyhow::bail!("serializer failed"));

        assert!(result.is_err());
        assert_eq!(fs::read(&path).unwrap(), b"old");
    }
}
