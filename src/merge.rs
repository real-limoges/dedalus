use anyhow::{bail, Context, Result};
use csv::{Reader, Writer};
use rustc_hash::FxHashSet;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::Path;
use tracing::info;

/// Merges sharded CSV files into single files suitable for neo4j-admin import
pub fn merge_csv_shards(output_dir: &str) -> Result<()> {
    info!("Detecting CSV shards in: {}", output_dir);

    // Detect shard count from nodes_*.csv
    let shard_count = detect_shard_count(output_dir)?;
    info!("  Found {} shards", shard_count);

    // Merge each CSV type
    merge_simple(output_dir, "nodes", shard_count)?;
    merge_simple(output_dir, "edges", shard_count)?;
    merge_with_dedup(output_dir, "categories", shard_count)?; // Needs dedup
    merge_simple(output_dir, "article_categories", shard_count)?;
    merge_with_dedup(output_dir, "image_nodes", shard_count)?; // Needs dedup
    merge_simple(output_dir, "article_images", shard_count)?;
    merge_with_dedup(output_dir, "external_link_nodes", shard_count)?; // Needs dedup
    merge_simple(output_dir, "article_external_links", shard_count)?;

    info!("Merge complete. Single CSV files ready for --admin-import.");
    Ok(())
}

/// Detect shard count by counting nodes_*.csv files
fn detect_shard_count(output_dir: &str) -> Result<u32> {
    let mut count = 0u32;
    loop {
        let path = Path::new(output_dir).join(format!("nodes_{:03}.csv", count));
        if path.exists() {
            count += 1;
        } else {
            break;
        }
    }
    if count == 0 {
        bail!("No sharded CSV files found (expected nodes_000.csv, etc.)");
    }
    Ok(count)
}

/// Simple concatenation for CSV types without deduplication needs
fn merge_simple(output_dir: &str, base_name: &str, shard_count: u32) -> Result<()> {
    info!("  Merging {}...", base_name);

    let output_path = Path::new(output_dir).join(format!("{}.csv", base_name));
    let mut writer = Writer::from_writer(BufWriter::with_capacity(
        256 * 1024,
        File::create(&output_path)?,
    ));

    // Write header from first shard
    let first_shard = Path::new(output_dir).join(format!("{}_{:03}.csv", base_name, 0));
    let mut first_reader = Reader::from_reader(BufReader::with_capacity(
        256 * 1024,
        File::open(&first_shard)?,
    ));
    writer.write_record(first_reader.headers()?)?;

    // Copy data rows from all shards
    for shard in 0..shard_count {
        let shard_path = Path::new(output_dir).join(format!("{}_{:03}.csv", base_name, shard));
        let mut reader = Reader::from_reader(BufReader::with_capacity(
            256 * 1024,
            File::open(&shard_path)?,
        ));

        for result in reader.records() {
            let record = result?;
            writer.write_record(&record)?;
        }
    }

    writer.flush()?;
    Ok(())
}

/// Merge with deduplication for node files (first column is ID)
fn merge_with_dedup(output_dir: &str, base_name: &str, shard_count: u32) -> Result<()> {
    info!("  Merging {} (with deduplication)...", base_name);

    let output_path = Path::new(output_dir).join(format!("{}.csv", base_name));
    let mut writer = Writer::from_writer(BufWriter::with_capacity(
        256 * 1024,
        File::create(&output_path)?,
    ));

    // Track seen node IDs
    let mut seen_ids = FxHashSet::default();

    // Write header from first shard
    let first_shard = Path::new(output_dir).join(format!("{}_{:03}.csv", base_name, 0));
    let mut first_reader = Reader::from_reader(BufReader::with_capacity(
        256 * 1024,
        File::open(&first_shard)?,
    ));
    writer.write_record(first_reader.headers()?)?;

    // Read all shards, skip duplicates
    for shard in 0..shard_count {
        let shard_path = Path::new(output_dir).join(format!("{}_{:03}.csv", base_name, shard));
        let mut reader = Reader::from_reader(BufReader::with_capacity(
            256 * 1024,
            File::open(&shard_path)?,
        ));

        for result in reader.records() {
            let record = result?;
            let node_id = record.get(0).context("Missing node ID")?;

            // Only write if first occurrence
            if seen_ids.insert(node_id.to_string()) {
                writer.write_record(&record)?;
            }
        }
    }

    writer.flush()?;
    info!("    Unique nodes: {}", seen_ids.len());
    Ok(())
}

/// Check if a filename matches the shard pattern `*_NNN.csv`
fn is_shard_file(name: &str) -> bool {
    if !name.ends_with(".csv") {
        return false;
    }
    let stem = &name[..name.len() - 4]; // strip .csv
    if stem.len() < 5 {
        // Need at least 1 char base name + _NNN
        return false;
    }
    let suffix = &stem[stem.len() - 4..];
    suffix.starts_with('_') && suffix[1..].chars().all(|c| c.is_ascii_digit())
}

/// Archive sharded CSV files (e.g. `nodes_000.csv`) to `output_dir/shards/`
pub fn archive_shards(output_dir: &str) -> Result<()> {
    let shards_dir = Path::new(output_dir).join("shards");
    fs::create_dir_all(&shards_dir).context("Failed to create shards archive directory")?;

    let mut archived = 0u32;
    for entry in fs::read_dir(output_dir).context("Failed to read output directory")? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if entry.file_type()?.is_file() && is_shard_file(&name_str) {
            let dest = shards_dir.join(&*name_str);
            fs::rename(entry.path(), &dest)
                .with_context(|| format!("Failed to archive {}", name_str))?;
            archived += 1;
        }
    }

    info!(
        "Archived {} shard files to {}/shards/",
        archived, output_dir
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_shard(
        dir: &Path,
        base_name: &str,
        shard: u32,
        header: &str,
        rows: &[&str],
    ) -> Result<()> {
        let path = dir.join(format!("{}_{:03}.csv", base_name, shard));
        let mut file = File::create(path)?;
        writeln!(file, "{}", header)?;
        for row in rows {
            writeln!(file, "{}", row)?;
        }
        Ok(())
    }

    #[test]
    fn test_detect_shard_count() -> Result<()> {
        let temp_dir = TempDir::new()?;

        // Create 3 node shards
        for i in 0..3 {
            create_test_shard(temp_dir.path(), "nodes", i, "id:ID,title,:LABEL", &[])?;
        }

        let count = detect_shard_count(temp_dir.path().to_str().unwrap())?;
        assert_eq!(count, 3);
        Ok(())
    }

    #[test]
    fn test_detect_no_shards() {
        let temp_dir = TempDir::new().unwrap();
        let result = detect_shard_count(temp_dir.path().to_str().unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_merge_simple() -> Result<()> {
        let temp_dir = TempDir::new()?;

        // Create test shards
        create_test_shard(
            temp_dir.path(),
            "nodes",
            0,
            "id:ID,title,:LABEL",
            &["1,Article1,Article", "2,Article2,Article"],
        )?;
        create_test_shard(
            temp_dir.path(),
            "nodes",
            1,
            "id:ID,title,:LABEL",
            &["3,Article3,Article", "4,Article4,Article"],
        )?;

        merge_simple(temp_dir.path().to_str().unwrap(), "nodes", 2)?;

        // Verify merged file
        let merged_path = temp_dir.path().join("nodes.csv");
        let content = fs::read_to_string(merged_path)?;
        let lines: Vec<&str> = content.lines().collect();

        assert_eq!(lines.len(), 5); // 1 header + 4 data rows
        assert!(lines[0].contains("id:ID,title,:LABEL"));
        assert!(lines.iter().any(|l| l.contains("Article1")));
        assert!(lines.iter().any(|l| l.contains("Article4")));
        Ok(())
    }

    #[test]
    fn test_is_shard_file() {
        assert!(is_shard_file("nodes_000.csv"));
        assert!(is_shard_file("edges_001.csv"));
        assert!(is_shard_file("categories_099.csv"));
        assert!(is_shard_file("article_categories_123.csv"));
        assert!(!is_shard_file("nodes.csv"));
        assert!(!is_shard_file("edges.csv"));
        assert!(!is_shard_file("something.txt"));
        assert!(!is_shard_file("nodes_00.csv")); // only 2 digits
        assert!(!is_shard_file("_000.csv")); // no base name
    }

    #[test]
    fn test_archive_shards() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let dir = temp_dir.path().to_str().unwrap();

        // Create shard files and a merged file
        for i in 0..3 {
            create_test_shard(
                temp_dir.path(),
                "nodes",
                i,
                "id:ID,title,:LABEL",
                &["1,A,Page"],
            )?;
        }
        // Create a merged (non-shard) file
        let merged = temp_dir.path().join("nodes.csv");
        fs::write(&merged, "id:ID,title,:LABEL\n1,A,Page\n")?;

        archive_shards(dir)?;

        // Shard files should be moved
        assert!(!temp_dir.path().join("nodes_000.csv").exists());
        assert!(!temp_dir.path().join("nodes_001.csv").exists());
        assert!(!temp_dir.path().join("nodes_002.csv").exists());

        // Merged file should remain
        assert!(merged.exists());

        // Shard files should be in shards/
        let shards_dir = temp_dir.path().join("shards");
        assert!(shards_dir.join("nodes_000.csv").exists());
        assert!(shards_dir.join("nodes_001.csv").exists());
        assert!(shards_dir.join("nodes_002.csv").exists());

        Ok(())
    }

    #[test]
    fn test_merge_with_dedup() -> Result<()> {
        let temp_dir = TempDir::new()?;

        // Create shards with duplicate categories
        create_test_shard(
            temp_dir.path(),
            "categories",
            0,
            "id:ID(Category),name,:LABEL",
            &["Science,Science,Category", "Math,Math,Category"],
        )?;
        create_test_shard(
            temp_dir.path(),
            "categories",
            1,
            "id:ID(Category),name,:LABEL",
            &["Science,Science,Category", "History,History,Category"],
        )?;

        merge_with_dedup(temp_dir.path().to_str().unwrap(), "categories", 2)?;

        // Verify merged file has deduplication
        let merged_path = temp_dir.path().join("categories.csv");
        let content = fs::read_to_string(merged_path)?;
        let lines: Vec<&str> = content.lines().collect();

        // Should have 1 header + 3 unique categories (Science, Math, History)
        assert_eq!(lines.len(), 4);

        // Count occurrences of "Science" - should appear exactly once in data rows
        let science_count = lines
            .iter()
            .skip(1)
            .filter(|l| l.contains("Science"))
            .count();
        assert_eq!(science_count, 1);

        Ok(())
    }
}
