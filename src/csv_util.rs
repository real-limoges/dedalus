//! CSV file layout detection and validation utilities.
//!
//! Extracted from the former `import.rs` module. Provides shared helpers for
//! detecting whether CSV output is single-file or sharded, generating file lists,
//! and validating that all expected CSV files exist.

use anyhow::{Result, bail};
use std::path::Path;

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
    /// All CSV types in import order.
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
}

/// Whether CSV output is a single file per type or sharded across N files.
#[derive(Debug, Clone)]
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

/// Detects whether the output directory contains single or sharded CSV files.
pub fn detect_csv_layout(output_dir: &str) -> Result<CsvLayout> {
    let sharded_path = Path::new(output_dir).join("nodes_000.csv");
    let single_path = Path::new(output_dir).join("nodes.csv");

    if sharded_path.exists() {
        let count = (0u32..)
            .take_while(|&i| {
                Path::new(output_dir)
                    .join(format!("nodes_{i:03}.csv"))
                    .exists()
            })
            .count() as u32;
        if count == 0 {
            bail!("Found nodes_000.csv but could not count shards");
        }
        Ok(CsvLayout::Sharded { count })
    } else if single_path.exists() {
        Ok(CsvLayout::Single)
    } else {
        bail!(
            "No CSV files found in {output_dir}. Expected nodes.csv or nodes_000.csv.\n\
             Run 'dedalus extract' first."
        );
    }
}

/// Returns the list of filenames for a given base name and layout.
pub fn csv_files_for(base_name: &str, layout: &CsvLayout) -> Vec<String> {
    match layout {
        CsvLayout::Single => vec![format!("{base_name}.csv")],
        CsvLayout::Sharded { count } => (0..*count)
            .map(|s| format!("{base_name}_{s:03}.csv"))
            .collect(),
    }
}

/// Validates that all expected CSV files exist in the output directory.
pub fn validate_csv_files(output_dir: &str, layout: &CsvLayout) -> Result<()> {
    for csv_type in CsvType::ALL {
        let files = csv_files_for(csv_type.base_name(), layout);
        for file in &files {
            let path = Path::new(output_dir).join(file);
            if !path.exists() {
                bail!(
                    "Missing CSV file: {path:?}\n\
                     Run 'dedalus extract' first."
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn detect_layout_single() {
        let dir = TempDir::new().unwrap();
        for csv_type in CsvType::ALL {
            let name = format!("{}.csv", csv_type.base_name());
            std::fs::write(dir.path().join(name), "header\n").unwrap();
        }
        let layout = detect_csv_layout(dir.path().to_str().unwrap()).unwrap();
        assert!(matches!(layout, CsvLayout::Single));
    }

    #[test]
    fn detect_layout_sharded() {
        let dir = TempDir::new().unwrap();
        for csv_type in CsvType::ALL {
            for shard in 0..4u32 {
                let name = format!("{}_{shard:03}.csv", csv_type.base_name());
                std::fs::write(dir.path().join(name), "header\n").unwrap();
            }
        }
        let layout = detect_csv_layout(dir.path().to_str().unwrap()).unwrap();
        assert!(matches!(layout, CsvLayout::Sharded { count: 4 }));
    }

    #[test]
    fn detect_layout_missing() {
        let dir = TempDir::new().unwrap();
        let result = detect_csv_layout(dir.path().to_str().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No CSV files"));
    }

    #[test]
    fn csv_files_for_single() {
        let files = csv_files_for("edges", &CsvLayout::Single);
        assert_eq!(files, vec!["edges.csv"]);
    }

    #[test]
    fn csv_files_for_sharded() {
        let files = csv_files_for("edges", &CsvLayout::Sharded { count: 3 });
        assert_eq!(
            files,
            vec!["edges_000.csv", "edges_001.csv", "edges_002.csv"]
        );
    }

    #[test]
    fn validate_csv_files_ok() {
        let dir = TempDir::new().unwrap();
        for csv_type in CsvType::ALL {
            let name = format!("{}.csv", csv_type.base_name());
            std::fs::write(dir.path().join(name), "header\n").unwrap();
        }
        let layout = CsvLayout::Single;
        assert!(validate_csv_files(dir.path().to_str().unwrap(), &layout).is_ok());
    }

    #[test]
    fn validate_csv_files_missing() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("nodes.csv"), "header\n").unwrap();
        let layout = CsvLayout::Single;
        let result = validate_csv_files(dir.path().to_str().unwrap(), &layout);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Missing CSV file"));
    }
}
