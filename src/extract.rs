//! Parallel extraction pass producing CSV and JSON output.
//!
//! Uses `rayon::par_bridge()` (or multistream parallel iteration) to process
//! articles concurrently. `ShardedCsvWriter` distributes rows across N files
//! by `page_id % csv_shards`. `DashSet` deduplicates categories, images, and
//! external links across threads.

use crate::checkpoint::{Checkpoint, CheckpointManager};
use crate::config::{CSV_WRITER_BUF_SIZE, PROGRESS_INTERVAL};
use crate::content;
use crate::content::LINK_REGEX;
use crate::index::WikiIndex;
use crate::infobox;
use crate::models::{ArticleBlob, EdgeType, PageType};
use crate::multistream::StreamRange;
use crate::parser::WikiReader;
use crate::stats::ExtractionStats;
use anyhow::{Context, Result};
use dashmap::DashSet;
use indicatif::ProgressBar;
use rayon::prelude::*;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

fn is_namespace_link(target: &str) -> bool {
    match target.as_bytes().first() {
        Some(b'C') => target.starts_with("Category:"),
        Some(b'F') => target.starts_with("File:"),
        Some(b'I') => target.starts_with("Image:"),
        Some(b'T') => target.starts_with("Template:"),
        Some(b'W') => target.starts_with("Wikipedia:"),
        Some(b'H') => target.starts_with("Help:"),
        Some(b'P') => target.starts_with("Portal:"),
        Some(b'D') => target.starts_with("Draft:"),
        Some(b'U') => target.starts_with("User:"),
        Some(b'M') => target.starts_with("Module:") || target.starts_with("MediaWiki:"),
        _ => false,
    }
}

fn strip_section_anchor(target: &str) -> &str {
    target.split('#').next().unwrap_or(target)
}

type CsvWriter = Arc<Mutex<csv::Writer<Box<dyn Write + Send>>>>;

fn create_csv_writer(
    output_dir: &str,
    filename: &str,
    dry_run: bool,
    resuming: bool,
) -> Result<CsvWriter> {
    Ok(Arc::new(Mutex::new(if dry_run {
        csv::Writer::from_writer(Box::new(std::io::sink()) as Box<dyn Write + Send>)
    } else if resuming {
        let path = format!("{}/{}", output_dir, filename);
        let file = if Path::new(&path).exists() {
            OpenOptions::new()
                .append(true)
                .open(&path)
                .with_context(|| format!("Failed to open {} for append", filename))?
        } else {
            File::create(&path)
                .with_context(|| format!("Failed to create {} during resume", filename))?
        };
        csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(
                Box::new(BufWriter::with_capacity(CSV_WRITER_BUF_SIZE, file))
                    as Box<dyn Write + Send>,
            )
    } else {
        let file = File::create(format!("{}/{}", output_dir, filename))
            .with_context(|| format!("Failed to create {}", filename))?;
        csv::Writer::from_writer(
            Box::new(BufWriter::with_capacity(CSV_WRITER_BUF_SIZE, file)) as Box<dyn Write + Send>,
        )
    })))
}

fn write_header(writer: &CsvWriter, fields: &[&str]) -> Result<()> {
    writer
        .lock()
        .map_err(|e| anyhow::anyhow!("CSV writer lock poisoned (a writer thread panicked): {}", e))?
        .write_record(fields)
        .context("Failed to write CSV header")
}

/// A set of CSV writers that shard rows by page ID.
///
/// When `csv_shards == 1`, produces a single file (e.g. `edges.csv`).
/// When `csv_shards > 1`, produces N files (e.g. `edges_000.csv`, `edges_001.csv`, ...).
struct ShardedCsvWriter {
    writers: Vec<CsvWriter>,
}

impl ShardedCsvWriter {
    fn new(
        output_dir: &str,
        base_name: &str,
        csv_shards: u32,
        dry_run: bool,
        resuming: bool,
    ) -> Result<Self> {
        let mut writers = Vec::with_capacity(csv_shards as usize);
        for shard in 0..csv_shards {
            let filename = if csv_shards == 1 {
                format!("{}.csv", base_name)
            } else {
                format!("{}_{:03}.csv", base_name, shard)
            };
            writers.push(create_csv_writer(output_dir, &filename, dry_run, resuming)?);
        }
        Ok(Self { writers })
    }

    fn write_headers(&self, fields: &[&str]) -> Result<()> {
        for writer in &self.writers {
            write_header(writer, fields)?;
        }
        Ok(())
    }

    fn shard_for(&self, page_id: u32) -> &CsvWriter {
        let idx = (page_id as usize) % self.writers.len();
        &self.writers[idx]
    }
}

/// Extracts edges from article text, classifying as LinksTo or SeeAlso.
/// Returns (deduplicated edges, invalid link count).
fn process_article_edges(
    text: &str,
    index: &WikiIndex,
    see_also_start: Option<usize>,
) -> (Vec<(u32, EdgeType)>, u64) {
    let mut local_edges: Vec<(u32, EdgeType)> = Vec::with_capacity(16);
    let mut invalid_count = 0u64;

    for caps in LINK_REGEX.captures_iter(text) {
        let target_title = strip_section_anchor(&caps[1]);
        if target_title.is_empty() || is_namespace_link(target_title) {
            continue;
        }
        if let Some(target_id) = index.resolve_id(target_title) {
            let edge_type = match see_also_start {
                Some(sa_start) if caps.get(0).unwrap().start() >= sa_start => EdgeType::SeeAlso,
                _ => EdgeType::LinksTo,
            };
            local_edges.push((target_id, edge_type));
        } else {
            invalid_count += 1;
        }
    }

    local_edges.sort_unstable();
    local_edges.dedup();
    (local_edges, invalid_count)
}

/// Deduplicates entity items against a global set, writes new nodes and all relationships.
/// Returns (new unique count, total relationship count).
fn write_dedup_entities(
    items: &[std::borrow::Cow<'_, str>],
    dedup_set: &DashSet<String>,
    node_writer: &ShardedCsvWriter,
    rel_writer: &ShardedCsvWriter,
    page_id: u32,
    id_str: &str,
    label: &str,
    rel_type: &str,
) -> (u64, u64) {
    let mut new_items: Vec<&str> = Vec::new();
    for item in items {
        if !dedup_set.contains(item.as_ref()) && dedup_set.insert(item.as_ref().to_owned()) {
            new_items.push(item.as_ref());
        }
    }

    if !new_items.is_empty() {
        if let Ok(mut writer) = node_writer.shard_for(page_id).lock() {
            for name in &new_items {
                if let Err(e) = writer.write_record([*name, *name, label]) {
                    warn!(error = %e, "Failed to write {} node record", label);
                }
            }
        }
    }

    if let Ok(mut writer) = rel_writer.shard_for(page_id).lock() {
        for item in items {
            if let Err(e) = writer.write_record([id_str, item.as_ref(), rel_type]) {
                warn!(error = %e, "Failed to write {} relationship record", rel_type);
            }
        }
    }

    (new_items.len() as u64, items.len() as u64)
}

/// Writes an article's JSON blob to the appropriate shard directory.
fn write_article_blob(
    output_dir: &str,
    shard_count: u32,
    page_id: u32,
    blob: &ArticleBlob,
    stats: &ExtractionStats,
) {
    let shard = page_id % shard_count;
    let blob_path = format!("{}/blobs/{:03}/{}.json", output_dir, shard, page_id);
    match File::create(&blob_path) {
        Ok(f) => {
            let mut w = BufWriter::new(f);
            if let Err(e) = serde_json::to_writer(&mut w, blob) {
                warn!(error = %e, path = %blob_path, "Failed to write blob");
            } else {
                stats.inc_blobs();
                debug!(id = page_id, "Wrote blob");
            }
        }
        Err(e) => {
            warn!(error = %e, path = %blob_path, "Failed to create blob file");
        }
    }
}

/// Configuration for the Wikipedia extraction pass.
#[derive(Debug, Clone)]
pub struct ExtractionConfig<'a> {
    pub input_path: &'a str,
    pub output_dir: &'a str,
    pub index: &'a WikiIndex,
    pub shard_count: u32,
    pub csv_shards: u32,
    pub limit: Option<u64>,
    pub dry_run: bool,
    pub resume_from: Option<&'a Checkpoint>,
    pub checkpoint_mgr: Option<&'a CheckpointManager>,
    pub multistream_ranges: Option<&'a [StreamRange]>,
}

/// Runs extraction with default stats/cancel state. Returns final statistics.
pub fn run_extraction(config: &ExtractionConfig) -> Result<ExtractionStats> {
    let stats = Arc::new(if let Some(cp) = config.resume_from {
        ExtractionStats::from_checkpoint(&cp.stats)
    } else {
        ExtractionStats::new()
    });
    let cancel = Arc::new(AtomicBool::new(false));

    run_extraction_with_stats(config, stats, cancel, false)
}

/// Runs extraction with caller-provided stats, cancel flag, and progress visibility.
pub fn run_extraction_with_stats(
    config: &ExtractionConfig,
    stats: Arc<ExtractionStats>,
    cancel: Arc<AtomicBool>,
    hide_progress: bool,
) -> Result<ExtractionStats> {
    let path = config.input_path;
    let output_dir = config.output_dir;
    let index = config.index;
    let shard_count = config.shard_count;
    let csv_shards = config.csv_shards;
    let limit = config.limit;
    let dry_run = config.dry_run;
    let resume_from = config.resume_from;
    let checkpoint_mgr = config.checkpoint_mgr;
    let multistream_ranges = config.multistream_ranges;
    let resuming = resume_from.is_some();
    let resume_after_id = resume_from.map(|cp| cp.last_processed_id).unwrap_or(0);

    let output_path = Path::new(output_dir);
    if !dry_run {
        fs::create_dir_all(output_path)
            .with_context(|| format!("Failed to create output directory: {}", output_dir))?;

        let test_file = output_path.join(".write_test");
        fs::write(&test_file, "test")
            .with_context(|| format!("Output directory is not writable: {}", output_dir))?;
        fs::remove_file(&test_file).ok();

        // Pre-create all blob shard directories once, avoiding millions of
        // redundant create_dir_all calls inside the parallel loop.
        for shard in 0..shard_count {
            let dir_path = format!("{}/blobs/{:03}", output_dir, shard);
            fs::create_dir_all(&dir_path)
                .with_context(|| format!("Failed to create blob directory: {}", dir_path))?;
        }
    }

    info!("Starting extraction from: {}", path);
    if resuming {
        info!(
            last_id = resume_after_id,
            articles = stats.articles(),
            "Resuming from checkpoint"
        );
    }
    if dry_run {
        info!("Dry run mode - no files will be written");
    }

    let nodes_writer = ShardedCsvWriter::new(output_dir, "nodes", csv_shards, dry_run, resuming)?;
    let edges_writer = ShardedCsvWriter::new(output_dir, "edges", csv_shards, dry_run, resuming)?;
    let categories_writer =
        ShardedCsvWriter::new(output_dir, "categories", csv_shards, dry_run, resuming)?;
    let article_categories_writer = ShardedCsvWriter::new(
        output_dir,
        "article_categories",
        csv_shards,
        dry_run,
        resuming,
    )?;
    let image_nodes_writer =
        ShardedCsvWriter::new(output_dir, "image_nodes", csv_shards, dry_run, resuming)?;
    let article_images_writer =
        ShardedCsvWriter::new(output_dir, "article_images", csv_shards, dry_run, resuming)?;
    let external_link_nodes_writer = ShardedCsvWriter::new(
        output_dir,
        "external_link_nodes",
        csv_shards,
        dry_run,
        resuming,
    )?;
    let article_external_links_writer = ShardedCsvWriter::new(
        output_dir,
        "article_external_links",
        csv_shards,
        dry_run,
        resuming,
    )?;

    if !resuming {
        nodes_writer.write_headers(&["id:ID", "title", ":LABEL"])?;
        edges_writer.write_headers(&[":START_ID", ":END_ID", ":TYPE"])?;
        categories_writer.write_headers(&["id:ID(Category)", "name", ":LABEL"])?;
        article_categories_writer.write_headers(&[":START_ID", ":END_ID(Category)", ":TYPE"])?;
        image_nodes_writer.write_headers(&["id:ID(Image)", "filename", ":LABEL"])?;
        article_images_writer.write_headers(&[":START_ID", ":END_ID(Image)", ":TYPE"])?;
        external_link_nodes_writer.write_headers(&["id:ID(ExternalLink)", "url", ":LABEL"])?;
        article_external_links_writer.write_headers(&[
            ":START_ID",
            ":END_ID(ExternalLink)",
            ":TYPE",
        ])?;
    }

    let stats_clone = Arc::clone(&stats);
    let limit_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let limit_reached = Arc::new(AtomicBool::new(false));
    let seen_categories: Arc<DashSet<String>> = Arc::new(DashSet::new());
    let seen_images: Arc<DashSet<String>> = Arc::new(DashSet::new());
    let seen_external_links: Arc<DashSet<String>> = Arc::new(DashSet::new());
    let cancel_clone = Arc::clone(&cancel);

    let pb = if hide_progress {
        ProgressBar::hidden()
    } else {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            indicatif::ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .expect("valid progress template"),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));
        pb
    };
    let pb = Arc::new(pb);
    let pb_clone = Arc::clone(&pb);

    let process_page = |page: crate::models::WikiPage| {
        if limit_reached.load(Ordering::Relaxed) || cancel_clone.load(Ordering::Relaxed) {
            return;
        }
        if let Some(max) = limit {
            let current = limit_counter.fetch_add(1, Ordering::Relaxed);
            if current >= max {
                limit_reached.store(true, Ordering::Relaxed);
                return;
            }
        }

        if let PageType::Article = page.page_type {
            let mut itoa_buf = itoa::Buffer::new();
            let id_str = itoa_buf.format(page.id);
            stats_clone.inc_articles();

            if let Ok(mut writer) = nodes_writer.shard_for(page.id).lock() {
                if let Err(e) = writer.write_record([id_str, &page.title, "Page"]) {
                    warn!(error = %e, "Failed to write node record");
                }
            }

            if let Some(text) = &page.text {
                // -- Edges --
                let see_also_start = content::see_also_section_start(text);
                let (local_edges, invalid_count) =
                    process_article_edges(text, index, see_also_start);
                let links_to_count = local_edges
                    .iter()
                    .filter(|(_, t)| *t == EdgeType::LinksTo)
                    .count() as u64;
                let see_also_count = local_edges
                    .iter()
                    .filter(|(_, t)| *t == EdgeType::SeeAlso)
                    .count() as u64;
                stats_clone.add_edges(links_to_count);
                stats_clone.add_see_also_edges(see_also_count);
                stats_clone.add_invalid_links(invalid_count);

                if !local_edges.is_empty() {
                    let mut edge_itoa = itoa::Buffer::new();
                    if let Ok(mut writer) = edges_writer.shard_for(page.id).lock() {
                        for (end_id, edge_type) in &local_edges {
                            let end_str = edge_itoa.format(*end_id);
                            let type_str = match edge_type {
                                EdgeType::LinksTo => "LINKS_TO",
                                EdgeType::SeeAlso => "SEE_ALSO",
                            };
                            if let Err(e) = writer.write_record([id_str, end_str, type_str]) {
                                warn!(error = %e, "Failed to write edge record");
                            }
                        }
                    }
                }

                // -- Categories --
                let categories = content::extract_categories(text);
                if !categories.is_empty() {
                    let (new_count, rel_count) = write_dedup_entities(
                        &categories,
                        &seen_categories,
                        &categories_writer,
                        &article_categories_writer,
                        page.id,
                        id_str,
                        "Category",
                        "HAS_CATEGORY",
                    );
                    stats_clone.add_categories(new_count);
                    stats_clone.add_category_edges(rel_count);
                }

                // -- Images --
                let images = content::extract_images(text);
                if !images.is_empty() {
                    let (new_count, _rel_count) = write_dedup_entities(
                        &images,
                        &seen_images,
                        &image_nodes_writer,
                        &article_images_writer,
                        page.id,
                        id_str,
                        "Image",
                        "HAS_IMAGE",
                    );
                    stats_clone.add_images(new_count);
                }

                // -- External links --
                let ext_links = content::extract_external_links(text);
                if !ext_links.is_empty() {
                    let (new_count, _rel_count) = write_dedup_entities(
                        &ext_links,
                        &seen_external_links,
                        &external_link_nodes_writer,
                        &article_external_links_writer,
                        page.id,
                        id_str,
                        "ExternalLink",
                        "HAS_LINK",
                    );
                    stats_clone.add_external_links(new_count);
                }

                // -- Infoboxes & blob --
                let infoboxes = infobox::extract_infoboxes(text);
                if !infoboxes.is_empty() {
                    stats_clone.add_infoboxes(infoboxes.len() as u64);
                }

                if !dry_run {
                    let blob = ArticleBlob {
                        id: page.id,
                        title: page.title,
                        abstract_text: content::extract_abstract(text),
                        categories: categories.into_iter().map(|c| c.into_owned()).collect(),
                        infoboxes,
                        sections: content::extract_sections(text),
                        timestamp: page.timestamp,
                        is_disambiguation: content::is_disambiguation(text),
                    };
                    write_article_blob(output_dir, shard_count, page.id, &blob, &stats_clone);
                }
            }

            if let Some(mgr) = checkpoint_mgr {
                if let Err(e) = mgr.maybe_save(page.id, &stats_clone) {
                    warn!(error = %e, "Failed to save checkpoint");
                }
            }

            let articles = stats_clone.articles();
            if articles.is_multiple_of(PROGRESS_INTERVAL as u64) {
                pb_clone.set_message(format!(
                    "Extracting: {} articles, {} edges, {} blobs",
                    articles,
                    stats_clone.edges(),
                    stats_clone.blobs()
                ));
            }
        }
    };

    #[allow(clippy::needless_borrows_for_generic_args)]
    if let Some(ranges) = multistream_ranges {
        info!(
            streams = ranges.len(),
            "Using multistream parallel extraction"
        );
        crate::multistream::par_iter_pages(path, ranges, false)
            .filter(|page| page.id > resume_after_id)
            .for_each(&process_page);
    } else {
        let reader = WikiReader::new(path, false)
            .with_context(|| format!("Failed to open wiki dump: {}", path))?;
        reader
            .filter(|page| page.id > resume_after_id)
            .par_bridge()
            .for_each(&process_page);
    }

    pb.finish_and_clear();

    info!(
        articles = stats.articles(),
        edges = stats.edges(),
        blobs = stats.blobs(),
        invalid_links = stats.invalid(),
        categories = stats.categories(),
        infoboxes = stats.infoboxes(),
        "Extraction complete"
    );

    Ok(
        Arc::try_unwrap(stats).unwrap_or_else(|arc| ExtractionStats {
            articles_processed: std::sync::atomic::AtomicU64::new(arc.articles()),
            edges_extracted: std::sync::atomic::AtomicU64::new(arc.edges()),
            blobs_written: std::sync::atomic::AtomicU64::new(arc.blobs()),
            invalid_links: std::sync::atomic::AtomicU64::new(arc.invalid()),
            categories_found: std::sync::atomic::AtomicU64::new(arc.categories()),
            category_edges: std::sync::atomic::AtomicU64::new(arc.category_edges()),
            see_also_edges: std::sync::atomic::AtomicU64::new(arc.see_also_edges()),
            infoboxes_extracted: std::sync::atomic::AtomicU64::new(arc.infoboxes()),
            images_found: std::sync::atomic::AtomicU64::new(arc.images()),
            external_links_found: std::sync::atomic::AtomicU64::new(arc.external_links()),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SHARD_COUNT;

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

    #[test]
    fn namespace_filter_works() {
        assert!(is_namespace_link("Category:Science"));
        assert!(is_namespace_link("File:Example.jpg"));
        assert!(is_namespace_link("Image:Logo.png"));
        assert!(is_namespace_link("Template:Infobox"));
        assert!(is_namespace_link("Wikipedia:About"));
        assert!(is_namespace_link("Help:Editing"));
        assert!(is_namespace_link("Portal:Science"));
        assert!(is_namespace_link("Draft:New article"));
        assert!(!is_namespace_link("Rust (programming language)"));
        assert!(!is_namespace_link("Python"));
    }

    #[test]
    fn strip_section_anchor_works() {
        assert_eq!(strip_section_anchor("Article#Section"), "Article");
        assert_eq!(strip_section_anchor("Article"), "Article");
        assert_eq!(
            strip_section_anchor("United States#History"),
            "United States"
        );
        assert_eq!(strip_section_anchor("#Section_only"), "");
    }
}
