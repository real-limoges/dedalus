use crate::config::CACHE_VERSION;
use crate::index::WikiIndex;
use anyhow::{bail, Context, Result};
use bincode::Options;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tracing::{info, warn};

/// Metadata stored with the index cache for validation
#[derive(Serialize, Deserialize)]
pub struct CacheMetadata {
    pub version: u32,
    pub input_path: String,
    pub input_mtime: u64,
    pub input_size: u64,
    pub article_count: usize,
    pub redirect_count: usize,
}

/// Complete index cache including metadata and serialized index data
#[derive(Serialize, Deserialize)]
pub struct IndexCache {
    pub metadata: CacheMetadata,
    pub articles: Vec<(String, u32)>,
    pub redirects: Vec<(String, String)>,
}

/// Returns the path to the index cache file for a given output directory
pub fn cache_path(output_dir: &str) -> PathBuf {
    Path::new(output_dir).join("index.cache")
}

/// Get input file metadata (mtime as seconds since epoch, size in bytes)
fn get_input_metadata(input_path: &str) -> Result<(u64, u64)> {
    let metadata = fs::metadata(input_path)
        .with_context(|| format!("Failed to get metadata for: {}", input_path))?;
    let mtime = metadata
        .modified()
        .context("Failed to get modification time")?
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("Invalid modification time")?
        .as_secs();
    let size = metadata.len();
    Ok((mtime, size))
}

/// Check if an existing cache is valid for the given input file
pub fn is_cache_valid(cache_path: &Path, input_path: &str) -> Result<bool> {
    if !cache_path.exists() {
        return Ok(false);
    }

    // Get file size to use as a sanity check for bincode deserialization
    let file_size = fs::metadata(cache_path).map(|m| m.len()).unwrap_or(0);

    let file = File::open(cache_path).context("Failed to open cache file")?;
    let reader = BufReader::new(file);

    // Use bincode options with size limit to avoid memory allocation attacks from corrupt data
    let options = bincode::options().with_limit(file_size.saturating_add(1024)); // Allow a small buffer over file size

    let cache: IndexCache = match options.deserialize_from(reader) {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "Cache file is corrupt or unreadable");
            return Ok(false);
        }
    };

    if cache.metadata.version != CACHE_VERSION {
        info!(
            cached = cache.metadata.version,
            current = CACHE_VERSION,
            "Cache version mismatch"
        );
        return Ok(false);
    }

    if cache.metadata.input_path != input_path {
        info!(
            cached = cache.metadata.input_path,
            current = input_path,
            "Cache input path mismatch"
        );
        return Ok(false);
    }

    let (mtime, size) = get_input_metadata(input_path)?;
    if cache.metadata.input_mtime != mtime || cache.metadata.input_size != size {
        info!(
            cached_mtime = cache.metadata.input_mtime,
            current_mtime = mtime,
            cached_size = cache.metadata.input_size,
            current_size = size,
            "Input file has changed since cache was created"
        );
        return Ok(false);
    }

    Ok(true)
}

/// Save an index to the cache file
pub fn save_index(index: &WikiIndex, input_path: &str, output_dir: &str) -> Result<()> {
    let path = cache_path(output_dir);

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {:?}", parent))?;
    }

    let (mtime, size) = get_input_metadata(input_path)?;
    let (articles, redirects) = index.to_serializable();
    let (article_count, redirect_count) = index.stats();

    let cache = IndexCache {
        metadata: CacheMetadata {
            version: CACHE_VERSION,
            input_path: input_path.to_string(),
            input_mtime: mtime,
            input_size: size,
            article_count,
            redirect_count,
        },
        articles,
        redirects,
    };

    // Write to temporary file first, then rename atomically
    let tmp_path = path.with_extension("cache.tmp");
    let file = File::create(&tmp_path)
        .with_context(|| format!("Failed to create temp cache file: {:?}", tmp_path))?;
    let writer = BufWriter::new(file);

    bincode::DefaultOptions::new()
        .serialize_into(writer, &cache)
        .context("Failed to serialize index cache")?;

    fs::rename(&tmp_path, &path)
        .with_context(|| format!("Failed to rename temp cache file to: {:?}", path))?;

    info!(
        articles = article_count,
        redirects = redirect_count,
        path = ?path,
        "Index cache saved"
    );

    Ok(())
}

/// Load an index from the cache file
pub fn load_index(cache_path: &Path) -> Result<WikiIndex> {
    if !cache_path.exists() {
        bail!("Cache file does not exist: {:?}", cache_path);
    }

    // Get file size to use as a limit for bincode deserialization
    let file_size = fs::metadata(cache_path).map(|m| m.len()).unwrap_or(0);

    let file = File::open(cache_path)
        .with_context(|| format!("Failed to open cache file: {:?}", cache_path))?;
    let reader = BufReader::new(file);

    let options = bincode::options().with_limit(file_size.saturating_add(1024));

    let cache: IndexCache = options
        .deserialize_from(reader)
        .context("Failed to deserialize index cache")?;

    let index = WikiIndex::from_serializable(cache.articles, cache.redirects);

    info!(
        articles = cache.metadata.article_count,
        redirects = cache.metadata.redirect_count,
        "Index loaded from cache"
    );

    Ok(index)
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

    fn create_test_index() -> WikiIndex {
        WikiIndex::from_serializable(
            vec![("Article1".to_string(), 1), ("Article2".to_string(), 2)],
            vec![("Redirect1".to_string(), "Article1".to_string())],
        )
    }

    #[test]
    fn cache_path_returns_correct_path() {
        let path = cache_path("/output/dir");
        assert_eq!(path, PathBuf::from("/output/dir/index.cache"));
    }

    #[test]
    fn is_cache_valid_returns_false_when_no_cache() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nonexistent.cache");
        let result = is_cache_valid(&path, "/some/input").unwrap();
        assert!(!result);
    }

    #[test]
    fn save_and_load_index_roundtrip() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let input_str = input_path.to_str().unwrap();
        let output_dir = dir.path().to_str().unwrap();

        let original = create_test_index();
        save_index(&original, input_str, output_dir).unwrap();

        let cache_file = cache_path(output_dir);
        let loaded = load_index(&cache_file).unwrap();

        assert_eq!(loaded.resolve_id("Article1"), Some(1));
        assert_eq!(loaded.resolve_id("Article2"), Some(2));
        assert_eq!(loaded.resolve_id("Redirect1"), Some(1));
    }

    #[test]
    fn is_cache_valid_returns_true_for_valid_cache() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let input_str = input_path.to_str().unwrap();
        let output_dir = dir.path().to_str().unwrap();

        let index = create_test_index();
        save_index(&index, input_str, output_dir).unwrap();

        let cache_file = cache_path(output_dir);
        assert!(is_cache_valid(&cache_file, input_str).unwrap());
    }

    #[test]
    fn is_cache_valid_returns_false_when_input_modified() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let input_str = input_path.to_str().unwrap();
        let output_dir = dir.path().to_str().unwrap();

        let index = create_test_index();
        save_index(&index, input_str, output_dir).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));
        let mut file = File::create(&input_path).unwrap();
        writeln!(file, "modified content that is longer").unwrap();

        let cache_file = cache_path(output_dir);
        assert!(!is_cache_valid(&cache_file, input_str).unwrap());
    }

    #[test]
    fn is_cache_valid_returns_false_for_different_input_path() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let input_str = input_path.to_str().unwrap();
        let output_dir = dir.path().to_str().unwrap();

        let index = create_test_index();
        save_index(&index, input_str, output_dir).unwrap();

        let cache_file = cache_path(output_dir);
        assert!(!is_cache_valid(&cache_file, "/different/input/path").unwrap());
    }

    #[test]
    fn load_index_fails_for_nonexistent_file() {
        let path = PathBuf::from("/nonexistent/cache.bin");
        let result = load_index(&path);
        assert!(result.is_err());
    }

    #[test]
    fn is_cache_valid_returns_false_for_corrupt_cache() {
        let dir = TempDir::new().unwrap();
        let cache_file = dir.path().join("index.cache");

        let mut file = File::create(&cache_file).unwrap();
        file.write_all(b"not valid bincode data").unwrap();

        let result = is_cache_valid(&cache_file, "/some/input").unwrap();
        assert!(!result);
    }

    #[test]
    fn save_index_creates_parent_directories() {
        let dir = TempDir::new().unwrap();
        let input_path = create_test_input(&dir);
        let input_str = input_path.to_str().unwrap();
        let nested_output = dir.path().join("nested").join("deep").join("output");
        let output_dir = nested_output.to_str().unwrap();

        let index = create_test_index();
        save_index(&index, input_str, output_dir).unwrap();

        let cache_file = cache_path(output_dir);
        assert!(cache_file.exists());
    }
}
