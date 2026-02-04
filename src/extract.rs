use crate::config::SHARD_COUNT;
use crate::index::WikiIndex;
use crate::models::{ArticleBlob, PageType};
use crate::parser::WikiReader;
use crate::stats::ExtractionStats;
use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use rayon::prelude::*;
use regex::Regex;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

static LINK_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\[\[([^|\]]+?)(?:\|[^\]]+)?\]\]").unwrap());

pub fn run_extraction(
    path: &str,
    output_dir: &str,
    index: &WikiIndex,
    limit: Option<u64>,
    dry_run: bool,
) -> Result<ExtractionStats> {
    let stats = Arc::new(ExtractionStats::new());

    // Validate output directory
    let output_path = Path::new(output_dir);
    if !dry_run {
        fs::create_dir_all(output_path)
            .with_context(|| format!("Failed to create output directory: {}", output_dir))?;

        // Test write permissions
        let test_file = output_path.join(".write_test");
        fs::write(&test_file, "test")
            .with_context(|| format!("Output directory is not writable: {}", output_dir))?;
        fs::remove_file(&test_file).ok();
    }

    info!("Starting extraction from: {}", path);
    if dry_run {
        info!("Dry run mode - no files will be written");
    }

    let nodes_writer: Arc<Mutex<csv::Writer<Box<dyn Write + Send>>>> =
        Arc::new(Mutex::new(if dry_run {
            csv::Writer::from_writer(Box::new(std::io::sink()) as Box<dyn Write + Send>)
        } else {
            let file = File::create(format!("{}/nodes.csv", output_dir))
                .with_context(|| "Failed to create nodes.csv")?;
            csv::Writer::from_writer(Box::new(file) as Box<dyn Write + Send>)
        }));

    let edges_writer: Arc<Mutex<csv::Writer<Box<dyn Write + Send>>>> =
        Arc::new(Mutex::new(if dry_run {
            csv::Writer::from_writer(Box::new(std::io::sink()) as Box<dyn Write + Send>)
        } else {
            let file = File::create(format!("{}/edges.csv", output_dir))
                .with_context(|| "Failed to create edges.csv")?;
            csv::Writer::from_writer(Box::new(file) as Box<dyn Write + Send>)
        }));

    let reader = WikiReader::new(path, false)
        .with_context(|| format!("Failed to open wiki dump: {}", path))?;

    nodes_writer
        .lock()
        .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?
        .write_record(["id:ID", "title", ":LABEL"])
        .context("Failed to write nodes header")?;

    edges_writer
        .lock()
        .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?
        .write_record([":START_ID", ":END_ID", ":TYPE"])
        .context("Failed to write edges header")?;

    let stats_clone = Arc::clone(&stats);
    let limit_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));

    reader.par_bridge().for_each(|page| {
        // Check limit
        if let Some(max) = limit {
            let current = limit_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if current >= max {
                return;
            }
        }

        if let PageType::Article = page.page_type {
            let id_str = page.id.to_string();
            stats_clone.inc_articles();

            {
                let mut writer = nodes_writer.lock().expect("Lock poisoned");
                if let Err(e) = writer.write_record([&id_str, &page.title, "Page"]) {
                    warn!(error = %e, "Failed to write node record");
                }
            }

            if let Some(text) = &page.text {
                let mut local_edges = Vec::new();
                let mut invalid_count = 0u64;

                for caps in LINK_REGEX.captures_iter(text) {
                    let target_title = &caps[1];
                    if let Some(target_id) = index.resolve_id(target_title) {
                        local_edges.push((id_str.clone(), target_id.to_string()));
                    } else {
                        invalid_count += 1;
                    }
                }

                stats_clone.add_edges(local_edges.len() as u64);
                stats_clone.add_invalid_links(invalid_count);

                // batch write
                if !local_edges.is_empty() {
                    let mut writer = edges_writer.lock().expect("Lock poisoned");
                    for (start, end) in local_edges {
                        if let Err(e) = writer.write_record(&[start, end, "LINKS_TO".to_string()]) {
                            warn!(error = %e, "Failed to write edge record");
                        }
                    }
                }

                if !dry_run {
                    let shard = page.id % SHARD_COUNT;
                    let dir_path = format!("{}/blobs/{:03}", output_dir, shard);
                    if let Err(e) = fs::create_dir_all(&dir_path) {
                        warn!(error = %e, path = %dir_path, "Failed to create blob directory");
                        return;
                    }

                    let blob = ArticleBlob {
                        id: page.id,
                        title: page.title,
                        text: text.clone(),
                    };

                    let blob_path = format!("{}/{}.json", dir_path, page.id);
                    match File::create(&blob_path) {
                        Ok(mut f) => {
                            if let Err(e) = serde_json::to_writer_pretty(&mut f, &blob) {
                                warn!(error = %e, path = %blob_path, "Failed to write blob");
                            } else {
                                stats_clone.inc_blobs();
                                debug!(id = page.id, "Wrote blob");
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, path = %blob_path, "Failed to create blob file");
                        }
                    }
                }
            }
        }
    });

    info!(
        articles = stats.articles(),
        edges = stats.edges(),
        blobs = stats.blobs(),
        invalid_links = stats.invalid(),
        "Extraction complete"
    );

    Ok(
        Arc::try_unwrap(stats).unwrap_or_else(|arc| ExtractionStats {
            articles_processed: std::sync::atomic::AtomicU64::new(arc.articles()),
            edges_extracted: std::sync::atomic::AtomicU64::new(arc.edges()),
            blobs_written: std::sync::atomic::AtomicU64::new(arc.blobs()),
            invalid_links: std::sync::atomic::AtomicU64::new(arc.invalid()),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_regex_simple_link() {
        let caps: Vec<_> = LINK_REGEX.captures_iter("See [[Rust]]").collect();
        assert_eq!(caps.len(), 1);
        assert_eq!(&caps[0][1], "Rust");
    }

    #[test]
    fn link_regex_piped_link() {
        let caps: Vec<_> = LINK_REGEX
            .captures_iter("See [[Rust (programming language)|Rust]]")
            .collect();
        assert_eq!(caps.len(), 1);
        assert_eq!(&caps[0][1], "Rust (programming language)");
    }

    #[test]
    fn link_regex_multiple_links() {
        let text = "[[Rust]] and [[Python]] are languages.";
        let targets: Vec<&str> = LINK_REGEX
            .captures_iter(text)
            .map(|c| c.get(1).unwrap().as_str())
            .collect();
        assert_eq!(targets, vec!["Rust", "Python"]);
    }

    #[test]
    fn link_regex_no_links() {
        let caps: Vec<_> = LINK_REGEX.captures_iter("No links here").collect();
        assert!(caps.is_empty());
    }

    #[test]
    fn link_regex_link_with_spaces() {
        let caps: Vec<_> = LINK_REGEX
            .captures_iter("[[United States of America]]")
            .collect();
        assert_eq!(caps.len(), 1);
        assert_eq!(&caps[0][1], "United States of America");
    }

    #[test]
    fn link_regex_link_with_parentheses() {
        let caps: Vec<_> = LINK_REGEX.captures_iter("[[Mercury (planet)]]").collect();
        assert_eq!(caps.len(), 1);
        assert_eq!(&caps[0][1], "Mercury (planet)");
    }

    #[test]
    fn link_regex_adjacent_links() {
        let text = "[[A]][[B]][[C]]";
        let targets: Vec<&str> = LINK_REGEX
            .captures_iter(text)
            .map(|c| c.get(1).unwrap().as_str())
            .collect();
        assert_eq!(targets, vec!["A", "B", "C"]);
    }

    #[test]
    fn link_regex_ignores_single_brackets() {
        let caps: Vec<_> = LINK_REGEX.captures_iter("[not a link]").collect();
        assert!(caps.is_empty());
    }

    #[test]
    fn link_regex_empty_brackets() {
        // [[]] should not match because the regex requires at least one char
        let caps: Vec<_> = LINK_REGEX.captures_iter("[[]]").collect();
        assert!(caps.is_empty());
    }

    #[test]
    fn shard_calculation() {
        assert_eq!(0 % SHARD_COUNT, 0);
        assert_eq!(1 % SHARD_COUNT, 1);
        assert_eq!(999 % SHARD_COUNT, 999);
        assert_eq!(1000 % SHARD_COUNT, 0);
        assert_eq!(1001 % SHARD_COUNT, 1);
        assert_eq!(123456 % SHARD_COUNT, 456);
    }

    #[test]
    fn shard_stays_within_bounds() {
        for id in [0u32, 1, 500, 999, 1000, 99999, u32::MAX] {
            assert!(id % SHARD_COUNT < SHARD_COUNT);
        }
    }
}
