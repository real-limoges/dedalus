//! CSV file names and layout detection.
//!
//! Extraction writes each CSV type either as one file (`nodes.csv`) or as
//! numbered shards (`nodes_000.csv`, `nodes_001.csv`, ...). This module owns
//! those names, so merge, load and extraction agree on what a shard file is
//! and how many shards an output directory holds.

use anyhow::{Context, Result, bail};
use std::fs;
use std::path::{Path, PathBuf};

/// A type of CSV file produced by extraction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CsvType {
    Nodes,
    Edges,
    Categories,
    ArticleCategories,
    ImageNodes,
    ArticleImages,
    ExternalLinkNodes,
    ArticleExternalLinks,
}

impl CsvType {
    /// All CSV types in load order.
    pub const ALL: &[Self] = &[
        Self::Nodes,
        Self::Edges,
        Self::Categories,
        Self::ArticleCategories,
        Self::ImageNodes,
        Self::ArticleImages,
        Self::ExternalLinkNodes,
        Self::ArticleExternalLinks,
    ];

    /// The base filename (without shard suffix or `.csv` extension).
    pub fn base_name(self) -> &'static str {
        match self {
            Self::Nodes => "nodes",
            Self::Edges => "edges",
            Self::Categories => "categories",
            Self::ArticleCategories => "article_categories",
            Self::ImageNodes => "image_nodes",
            Self::ArticleImages => "article_images",
            Self::ExternalLinkNodes => "external_link_nodes",
            Self::ArticleExternalLinks => "article_external_links",
        }
    }

    /// The header row, in typed-column style (`id:ID`, `:START_ID`, `:TYPE`).
    pub fn header(self) -> [&'static str; 3] {
        match self {
            Self::Nodes => ["id:ID", "title", ":LABEL"],
            Self::Edges => [":START_ID", ":END_ID", ":TYPE"],
            Self::Categories => ["id:ID(Category)", "name", ":LABEL"],
            Self::ArticleCategories => [":START_ID", ":END_ID(Category)", ":TYPE"],
            Self::ImageNodes => ["id:ID(Image)", "filename", ":LABEL"],
            Self::ArticleImages => [":START_ID", ":END_ID(Image)", ":TYPE"],
            Self::ExternalLinkNodes => ["id:ID(ExternalLink)", "url", ":LABEL"],
            Self::ArticleExternalLinks => [":START_ID", ":END_ID(ExternalLink)", ":TYPE"],
        }
    }

    /// Whether the same row can come from several pages, so a merge must
    /// keep only the first row for each id.
    pub fn is_shared_node(self) -> bool {
        matches!(
            self,
            Self::Categories | Self::ImageNodes | Self::ExternalLinkNodes
        )
    }

    /// The merged (single) file name, such as `nodes.csv`.
    pub fn merged_file_name(self) -> String {
        format!("{}.csv", self.base_name())
    }

    /// The file name of one shard, such as `nodes_003.csv`.
    pub fn shard_file_name(self, shard: u32) -> String {
        format!("{}_{shard:03}.csv", self.base_name())
    }

    /// The shard number in `file_name`, if it is exactly a shard file name
    /// of this type (`nodes_003.csv` is shard 3; `nodes_3.csv` is not a shard).
    pub fn shard_index(self, file_name: &str) -> Option<u32> {
        let digits = file_name
            .strip_prefix(self.base_name())?
            .strip_prefix('_')?
            .strip_suffix(".csv")?;
        if !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let shard: u32 = digits.parse().ok()?;
        (self.shard_file_name(shard) == file_name).then_some(shard)
    }

    /// The files extraction writes for this type: the merged name when
    /// `csv_shards` is 1, otherwise one file per shard.
    pub fn file_names(self, csv_shards: u32) -> Vec<String> {
        if csv_shards == 1 {
            vec![self.merged_file_name()]
        } else {
            (0..csv_shards).map(|s| self.shard_file_name(s)).collect()
        }
    }
}

/// Whether `file_name` is a shard file of any CSV type.
pub fn is_shard_file(file_name: &str) -> bool {
    CsvType::ALL
        .iter()
        .any(|t| t.shard_index(file_name).is_some())
}

/// Whether CSV output is a single file per type or sharded across N files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CsvLayout {
    Single,
    Sharded { count: u32 },
}

impl std::fmt::Display for CsvLayout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CsvLayout::Single => f.write_str("single-file"),
            CsvLayout::Sharded { count } => write!(f, "sharded ({count} shards)"),
        }
    }
}

/// Detects whether the output directory holds merged or sharded CSV files.
///
/// A merged `nodes.csv` wins when both are present: that is the state after
/// a merge that did not archive its shards, and the merged files are the
/// ones load and analytics need.
pub fn detect_csv_layout(output_dir: &str) -> Result<CsvLayout> {
    if Path::new(output_dir)
        .join(CsvType::Nodes.merged_file_name())
        .exists()
    {
        return Ok(CsvLayout::Single);
    }
    if shard_indices(output_dir, CsvType::Nodes)?.is_empty() {
        bail!(
            "No CSV files found in {output_dir}. Expected nodes.csv or nodes_000.csv.\n\
             Run 'dedalus extract' first."
        );
    }
    Ok(CsvLayout::Sharded {
        count: count_shards(output_dir)?,
    })
}

/// The number of node shards in `output_dir`.
///
/// Fails unless the shards are numbered exactly `0..N`, since a gap means
/// files from different runs (or a lost file) and merging them would
/// silently drop or mix data.
pub fn count_shards(output_dir: &str) -> Result<u32> {
    let indices = shard_indices(output_dir, CsvType::Nodes)?;
    if indices.is_empty() {
        bail!("No sharded CSV files found in {output_dir} (expected nodes_000.csv, ...)");
    }
    let expected: Vec<u32> = (0..indices.len() as u32).collect();
    if indices != expected {
        bail!(
            "Node shards in {output_dir} are not numbered 0..{}: found {indices:?}",
            indices.len()
        );
    }
    Ok(indices.len() as u32)
}

/// The sorted shard numbers of one CSV type present in `output_dir`.
fn shard_indices(output_dir: &str, csv_type: CsvType) -> Result<Vec<u32>> {
    let mut indices: Vec<u32> = file_names_in(output_dir)?
        .iter()
        .filter_map(|name| csv_type.shard_index(name))
        .collect();
    indices.sort_unstable();
    Ok(indices)
}

/// CSV files in `output_dir` that a run with `csv_shards` shards would not
/// write: the merged files of a sharded run, or shards of another count.
///
/// Extraction removes these before a fresh run, so a later load or merge
/// cannot pick up rows from an earlier run.
pub fn stale_csv_files(output_dir: &str, csv_shards: u32) -> Result<Vec<PathBuf>> {
    let current: Vec<String> = CsvType::ALL
        .iter()
        .flat_map(|t| t.file_names(csv_shards))
        .collect();
    let is_csv_output = |name: &str| {
        is_shard_file(name) || CsvType::ALL.iter().any(|t| t.merged_file_name() == name)
    };
    Ok(file_names_in(output_dir)?
        .into_iter()
        .filter(|name| is_csv_output(name) && !current.contains(name))
        .map(|name| Path::new(output_dir).join(name))
        .collect())
}

/// The names of the regular files directly inside `dir`.
fn file_names_in(dir: &str) -> Result<Vec<String>> {
    if !Path::new(dir).exists() {
        return Ok(Vec::new());
    }
    let mut names = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("Failed to read directory {dir}"))? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn touch(dir: &TempDir, name: &str) {
        std::fs::write(dir.path().join(name), "header\n").unwrap();
    }

    fn dir_str(dir: &TempDir) -> &str {
        dir.path().to_str().unwrap()
    }

    #[test]
    fn detect_layout_single() {
        let dir = TempDir::new().unwrap();
        for csv_type in CsvType::ALL {
            touch(&dir, &csv_type.merged_file_name());
        }
        assert_eq!(detect_csv_layout(dir_str(&dir)).unwrap(), CsvLayout::Single);
    }

    #[test]
    fn detect_layout_sharded() {
        let dir = TempDir::new().unwrap();
        for csv_type in CsvType::ALL {
            for shard in 0..4u32 {
                touch(&dir, &csv_type.shard_file_name(shard));
            }
        }
        assert_eq!(
            detect_csv_layout(dir_str(&dir)).unwrap(),
            CsvLayout::Sharded { count: 4 }
        );
    }

    #[test]
    fn detect_layout_prefers_merged_files_over_unarchived_shards() {
        let dir = TempDir::new().unwrap();
        touch(&dir, "nodes_000.csv");
        touch(&dir, "nodes_001.csv");
        touch(&dir, "nodes.csv");
        assert_eq!(detect_csv_layout(dir_str(&dir)).unwrap(), CsvLayout::Single);
    }

    #[test]
    fn detect_layout_missing() {
        let dir = TempDir::new().unwrap();
        let err = detect_csv_layout(dir_str(&dir)).unwrap_err();
        assert!(err.to_string().contains("No CSV files"));
    }

    #[test]
    fn count_shards_rejects_a_gap() {
        let dir = TempDir::new().unwrap();
        touch(&dir, "nodes_000.csv");
        touch(&dir, "nodes_002.csv");
        let err = count_shards(dir_str(&dir)).unwrap_err();
        assert!(err.to_string().contains("not numbered"));
    }

    #[test]
    fn count_shards_ignores_other_types_with_a_shared_suffix() {
        let dir = TempDir::new().unwrap();
        touch(&dir, "nodes_000.csv");
        touch(&dir, "image_nodes_000.csv");
        touch(&dir, "image_nodes_001.csv");
        assert_eq!(count_shards(dir_str(&dir)).unwrap(), 1);
    }

    #[test]
    fn count_shards_reads_four_digit_shards() {
        let dir = TempDir::new().unwrap();
        for shard in 0..1001 {
            touch(&dir, &CsvType::Nodes.shard_file_name(shard));
        }
        assert_eq!(count_shards(dir_str(&dir)).unwrap(), 1001);
    }

    #[test]
    fn shard_index_matches_only_exact_shard_names() {
        assert_eq!(CsvType::Nodes.shard_index("nodes_000.csv"), Some(0));
        assert_eq!(CsvType::Nodes.shard_index("nodes_1000.csv"), Some(1000));
        assert_eq!(CsvType::Nodes.shard_index("nodes_00.csv"), None);
        assert_eq!(CsvType::Nodes.shard_index("nodes_0001.csv"), None);
        assert_eq!(CsvType::Nodes.shard_index("nodes.csv"), None);
        assert_eq!(CsvType::Nodes.shard_index("image_nodes_000.csv"), None);
        assert_eq!(CsvType::Nodes.shard_index("nodes_+01.csv"), None);
    }

    #[test]
    fn is_shard_file_covers_every_type_and_nothing_else() {
        for csv_type in CsvType::ALL {
            assert!(is_shard_file(&csv_type.shard_file_name(7)));
            assert!(!is_shard_file(&csv_type.merged_file_name()));
        }
        assert!(!is_shard_file("other_000.csv"));
        assert!(!is_shard_file("_000.csv"));
        assert!(!is_shard_file("nodes_000.txt"));
    }

    #[test]
    fn file_names_single_and_sharded() {
        assert_eq!(CsvType::Edges.file_names(1), vec!["edges.csv"]);
        assert_eq!(
            CsvType::Edges.file_names(3),
            vec!["edges_000.csv", "edges_001.csv", "edges_002.csv"]
        );
    }

    #[test]
    fn stale_csv_files_are_the_other_layout_and_extra_shards() {
        let dir = TempDir::new().unwrap();
        touch(&dir, "nodes.csv");
        touch(&dir, "nodes_000.csv");
        touch(&dir, "nodes_001.csv");
        touch(&dir, "nodes_002.csv");
        touch(&dir, "notes.txt");
        let mut stale: Vec<String> = stale_csv_files(dir_str(&dir), 2)
            .unwrap()
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        stale.sort();
        assert_eq!(stale, vec!["nodes.csv", "nodes_002.csv"]);
    }
}
