//! CSV shard merger.
//!
//! Concatenates the numbered shards of each CSV type (`nodes_000.csv`, ...)
//! into one merged file (`nodes.csv`), which is what load and analytics read.
//! Category, image and external link nodes can be written by pages in
//! different shards, so their merge keeps only the first row for each id.
//! Each merged file is written to a `.tmp` sibling and renamed into place, so
//! an interrupted merge never leaves a truncated `nodes.csv` behind.

use crate::atomic_file::write_atomically;
use crate::config::MERGE_BUF_SIZE;
use crate::csv_util::{self, CsvType};
use anyhow::{Context, Result, bail};
use rustc_hash::FxHashSet;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::Path;
use tracing::info;

/// Merges `shard_count` shards of every CSV type into single files.
///
/// Every shard file is checked to exist before anything is written, so a
/// missing shard fails the merge instead of producing a partial result.
pub fn merge_csv_shards(output_dir: &str, shard_count: u32) -> Result<()> {
    info!("Merging {shard_count} CSV shards in {output_dir}");
    for csv_type in CsvType::ALL {
        for name in csv_type.file_names(shard_count) {
            let path = Path::new(output_dir).join(&name);
            if !path.is_file() {
                bail!("Missing CSV shard: {}", path.display());
            }
        }
    }
    for &csv_type in CsvType::ALL {
        let rows = merge_type(output_dir, csv_type, shard_count)?;
        info!("  {}: {rows} rows", csv_type.merged_file_name());
    }
    info!("Merge complete");
    Ok(())
}

/// Counts the shards in `output_dir` and merges them. Returns the count.
///
/// For a standalone merge, where no extraction run says how many shards to
/// expect; fails unless the shards are numbered exactly `0..N`.
pub fn merge_detected_shards(output_dir: &str) -> Result<u32> {
    let shard_count = csv_util::count_shards(output_dir)?;
    merge_csv_shards(output_dir, shard_count)?;
    Ok(shard_count)
}

/// Merges the shards of one CSV type. Returns the number of rows written.
fn merge_type(output_dir: &str, csv_type: CsvType, shard_count: u32) -> Result<u64> {
    let merged_path = Path::new(output_dir).join(csv_type.merged_file_name());
    let mut seen_ids = FxHashSet::default();
    let mut rows = 0u64;

    write_atomically(&merged_path, |out| {
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .buffer_capacity(MERGE_BUF_SIZE)
            .from_writer(out);
        writer.write_record(csv_type.header())?;

        for name in csv_type.file_names(shard_count) {
            let shard_path = Path::new(output_dir).join(&name);
            let file = File::open(&shard_path)
                .with_context(|| format!("Failed to open {}", shard_path.display()))?;
            let mut reader =
                csv::Reader::from_reader(BufReader::with_capacity(MERGE_BUF_SIZE, file));

            let header = reader.headers()?;
            if !header.iter().eq(csv_type.header()) {
                bail!(
                    "{name} has header {:?}, expected {:?}",
                    header.iter().collect::<Vec<_>>(),
                    csv_type.header()
                );
            }

            for record in reader.records() {
                let record = record.with_context(|| format!("Failed to read {name}"))?;
                if csv_type.is_shared_node() {
                    let id = record
                        .get(0)
                        .with_context(|| format!("Row without an id in {name}"))?;
                    if !seen_ids.insert(id.to_string()) {
                        continue;
                    }
                }
                writer.write_record(&record)?;
                rows += 1;
            }
        }
        writer.flush()?;
        Ok(())
    })
    .with_context(|| format!("Failed to merge {}", csv_type.merged_file_name()))?;

    Ok(rows)
}

/// Moves shard files (`nodes_000.csv`, ...) into `output_dir/shards/`.
///
/// Shards archived by an earlier run are removed first, so the archive only
/// ever holds the shards of the latest merge.
pub fn archive_shards(output_dir: &str) -> Result<()> {
    let shards_dir = Path::new(output_dir).join("shards");
    fs::create_dir_all(&shards_dir).context("Failed to create shards archive directory")?;

    for entry in fs::read_dir(&shards_dir).context("Failed to read shards directory")? {
        let entry = entry?;
        if entry.file_type()?.is_file()
            && csv_util::is_shard_file(&entry.file_name().to_string_lossy())
        {
            fs::remove_file(entry.path())
                .with_context(|| format!("Failed to remove stale {}", entry.path().display()))?;
        }
    }

    let mut archived = 0u32;
    for entry in fs::read_dir(output_dir).context("Failed to read output directory")? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type()?.is_file() && csv_util::is_shard_file(&name) {
            fs::rename(entry.path(), shards_dir.join(&name))
                .with_context(|| format!("Failed to archive {name}"))?;
            archived += 1;
        }
    }

    info!("Archived {archived} shard files to {output_dir}/shards/");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_shard(dir: &Path, csv_type: CsvType, shard: u32, rows: &[&str]) {
        let mut file = File::create(dir.join(csv_type.shard_file_name(shard))).unwrap();
        writeln!(file, "{}", csv_type.header().join(",")).unwrap();
        for row in rows {
            writeln!(file, "{row}").unwrap();
        }
    }

    /// Writes empty shards of every type, so a full merge can run.
    fn write_empty_shards(dir: &Path, shard_count: u32) {
        for &csv_type in CsvType::ALL {
            for shard in 0..shard_count {
                write_shard(dir, csv_type, shard, &[]);
            }
        }
    }

    fn lines(path: &Path) -> Vec<String> {
        fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn merge_concatenates_shards_under_one_header() {
        let dir = TempDir::new().unwrap();
        write_empty_shards(dir.path(), 2);
        write_shard(
            dir.path(),
            CsvType::Nodes,
            0,
            &["1,A,Article", "2,B,Article"],
        );
        write_shard(dir.path(), CsvType::Nodes, 1, &["3,C,Article"]);

        merge_csv_shards(dir.path().to_str().unwrap(), 2).unwrap();

        assert_eq!(
            lines(&dir.path().join("nodes.csv")),
            vec![
                "id:ID,title,:LABEL",
                "1,A,Article",
                "2,B,Article",
                "3,C,Article"
            ]
        );
        assert!(!dir.path().join("nodes.csv.tmp").exists());
    }

    #[test]
    fn merge_dedups_shared_nodes_but_not_relations() {
        let dir = TempDir::new().unwrap();
        write_empty_shards(dir.path(), 2);
        write_shard(
            dir.path(),
            CsvType::Categories,
            0,
            &["Science,Science,Category", "Math,Math,Category"],
        );
        write_shard(
            dir.path(),
            CsvType::Categories,
            1,
            &["Science,Science,Category", "History,History,Category"],
        );
        write_shard(
            dir.path(),
            CsvType::ArticleCategories,
            0,
            &["1,Science,HAS_CATEGORY"],
        );
        write_shard(
            dir.path(),
            CsvType::ArticleCategories,
            1,
            &["1,Science,HAS_CATEGORY"],
        );

        merge_csv_shards(dir.path().to_str().unwrap(), 2).unwrap();

        assert_eq!(
            lines(&dir.path().join("categories.csv"))[1..],
            [
                "Science,Science,Category",
                "Math,Math,Category",
                "History,History,Category"
            ]
        );
        assert_eq!(lines(&dir.path().join("article_categories.csv")).len(), 3);
    }

    #[test]
    fn merge_fails_before_writing_when_a_shard_is_missing() {
        let dir = TempDir::new().unwrap();
        write_empty_shards(dir.path(), 2);
        fs::remove_file(dir.path().join("article_images_001.csv")).unwrap();

        let err = merge_csv_shards(dir.path().to_str().unwrap(), 2).unwrap_err();

        assert!(err.to_string().contains("article_images_001.csv"));
        assert!(!dir.path().join("nodes.csv").exists());
    }

    #[test]
    fn merge_rejects_a_shard_with_the_wrong_header() {
        let dir = TempDir::new().unwrap();
        write_empty_shards(dir.path(), 2);
        fs::write(dir.path().join("edges_001.csv"), "a,b,c\n").unwrap();

        let err = merge_csv_shards(dir.path().to_str().unwrap(), 2).unwrap_err();

        assert!(format!("{err:#}").contains("edges_001.csv has header"));
    }

    #[test]
    fn merge_detected_shards_counts_contiguous_shards() {
        let dir = TempDir::new().unwrap();
        write_empty_shards(dir.path(), 3);
        assert_eq!(
            merge_detected_shards(dir.path().to_str().unwrap()).unwrap(),
            3
        );
    }

    #[test]
    fn archive_moves_shards_and_replaces_stale_ones() {
        let dir = TempDir::new().unwrap();
        let shards_dir = dir.path().join("shards");
        fs::create_dir_all(&shards_dir).unwrap();
        fs::write(shards_dir.join("nodes_007.csv"), "stale\n").unwrap();
        write_shard(dir.path(), CsvType::Nodes, 0, &["1,A,Article"]);
        write_shard(dir.path(), CsvType::ImageNodes, 0, &[]);
        fs::write(dir.path().join("nodes.csv"), "merged\n").unwrap();

        archive_shards(dir.path().to_str().unwrap()).unwrap();

        assert!(!dir.path().join("nodes_000.csv").exists());
        assert!(!dir.path().join("image_nodes_000.csv").exists());
        assert!(dir.path().join("nodes.csv").exists());
        assert!(shards_dir.join("nodes_000.csv").exists());
        assert!(shards_dir.join("image_nodes_000.csv").exists());
        assert!(!shards_dir.join("nodes_007.csv").exists());
    }
}
