use anyhow::{bail, Context, Result};
use csv::{Reader, Writer};
use rustc_hash::FxHashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

/// Merges sharded CSV files into single files suitable for neo4j-admin import
pub fn merge_csv_shards(output_dir: &str) -> Result<()> {
    println!("Detecting CSV shards in: {}", output_dir);

    // Detect shard count from nodes_*.csv
    let shard_count = detect_shard_count(output_dir)?;
    println!("  Found {} shards", shard_count);

    // Merge each CSV type
    merge_simple(output_dir, "nodes", shard_count)?;
    merge_simple(output_dir, "edges", shard_count)?;
    merge_with_dedup(output_dir, "categories", shard_count)?; // Needs dedup
    merge_simple(output_dir, "article_categories", shard_count)?;
    merge_with_dedup(output_dir, "image_nodes", shard_count)?; // Needs dedup
    merge_simple(output_dir, "article_images", shard_count)?;
    merge_with_dedup(output_dir, "external_link_nodes", shard_count)?; // Needs dedup
    merge_simple(output_dir, "article_external_links", shard_count)?;

    println!("Merge complete. Single CSV files ready for --admin-import.");
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
    println!("  Merging {}...", base_name);

    let output_path = Path::new(output_dir).join(format!("{}.csv", base_name));
    let mut writer = Writer::from_writer(BufWriter::with_capacity(
        256 * 1024,
        File::create(&output_path)?,
    ));

    // Write header from first shard
    let first_shard = Path::new(output_dir).join(format!("{}_{:03}.csv", base_name, 0));
    let mut first_reader = Reader::from_reader(BufReader::new(File::open(&first_shard)?));
    writer.write_record(first_reader.headers()?)?;

    // Copy data rows from all shards
    for shard in 0..shard_count {
        let shard_path = Path::new(output_dir).join(format!("{}_{:03}.csv", base_name, shard));
        let mut reader = Reader::from_reader(BufReader::new(File::open(&shard_path)?));

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
    println!("  Merging {} (with deduplication)...", base_name);

    let output_path = Path::new(output_dir).join(format!("{}.csv", base_name));
    let mut writer = Writer::from_writer(BufWriter::with_capacity(
        256 * 1024,
        File::create(&output_path)?,
    ));

    // Track seen node IDs
    let mut seen_ids = FxHashSet::default();

    // Write header from first shard
    let first_shard = Path::new(output_dir).join(format!("{}_{:03}.csv", base_name, 0));
    let mut first_reader = Reader::from_reader(BufReader::new(File::open(&first_shard)?));
    writer.write_record(first_reader.headers()?)?;

    // Read all shards, skip duplicates
    for shard in 0..shard_count {
        let shard_path = Path::new(output_dir).join(format!("{}_{:03}.csv", base_name, shard));
        let mut reader = Reader::from_reader(BufReader::new(File::open(&shard_path)?));

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
    println!("    Unique nodes: {}", seen_ids.len());
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
