use crate::checkpoint::{Checkpoint, CheckpointManager};
use crate::config::PROGRESS_INTERVAL;
use crate::content;
use crate::content::LINK_REGEX;
use crate::index::WikiIndex;
use crate::infobox;
use crate::models::{ArticleBlob, PageType};
use crate::parser::WikiReader;
use crate::stats::ExtractionStats;
use anyhow::{Context, Result};
use dashmap::DashSet;
use indicatif::ProgressBar;
use rayon::prelude::*;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

fn is_namespace_link(target: &str) -> bool {
    target.starts_with("Category:")
        || target.starts_with("File:")
        || target.starts_with("Image:")
        || target.starts_with("Template:")
        || target.starts_with("Wikipedia:")
        || target.starts_with("Help:")
        || target.starts_with("Portal:")
        || target.starts_with("Draft:")
        || target.starts_with("User:")
        || target.starts_with("Module:")
        || target.starts_with("MediaWiki:")
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
                Box::new(BufWriter::with_capacity(64 * 1024, file)) as Box<dyn Write + Send>
            )
    } else {
        let file = File::create(format!("{}/{}", output_dir, filename))
            .with_context(|| format!("Failed to create {}", filename))?;
        csv::Writer::from_writer(
            Box::new(BufWriter::with_capacity(64 * 1024, file)) as Box<dyn Write + Send>
        )
    })))
}

fn write_header(writer: &CsvWriter, fields: &[&str]) -> Result<()> {
    writer
        .lock()
        .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?
        .write_record(fields)
        .context("Failed to write CSV header")
}

#[allow(clippy::too_many_arguments)]
pub fn run_extraction(
    path: &str,
    output_dir: &str,
    index: &WikiIndex,
    shard_count: u32,
    limit: Option<u64>,
    dry_run: bool,
    resume_from: Option<&Checkpoint>,
    checkpoint_mgr: Option<&CheckpointManager>,
) -> Result<ExtractionStats> {
    let stats = Arc::new(if let Some(cp) = resume_from {
        ExtractionStats::from_checkpoint(&cp.stats)
    } else {
        ExtractionStats::new()
    });

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

    let nodes_writer = create_csv_writer(output_dir, "nodes.csv", dry_run, resuming)?;
    let edges_writer = create_csv_writer(output_dir, "edges.csv", dry_run, resuming)?;
    let categories_writer = create_csv_writer(output_dir, "categories.csv", dry_run, resuming)?;
    let article_categories_writer =
        create_csv_writer(output_dir, "article_categories.csv", dry_run, resuming)?;
    let images_writer = create_csv_writer(output_dir, "images.csv", dry_run, resuming)?;
    let external_links_writer =
        create_csv_writer(output_dir, "external_links.csv", dry_run, resuming)?;

    let reader = WikiReader::new(path, false)
        .with_context(|| format!("Failed to open wiki dump: {}", path))?;

    if !resuming {
        write_header(&nodes_writer, &["id:ID", "title", ":LABEL"])?;
        write_header(&edges_writer, &[":START_ID", ":END_ID", ":TYPE"])?;
        write_header(&categories_writer, &["id:ID(Category)", "name", ":LABEL"])?;
        write_header(
            &article_categories_writer,
            &[":START_ID", ":END_ID(Category)", ":TYPE"],
        )?;
        write_header(&images_writer, &["article_id", "filename"])?;
        write_header(&external_links_writer, &["article_id", "url"])?;
    }

    let stats_clone = Arc::clone(&stats);
    let limit_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let seen_categories: Arc<DashSet<String>> = Arc::new(DashSet::new());

    let max_completed_id = Arc::new(AtomicU32::new(resume_after_id));

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap(),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    let pb = Arc::new(pb);
    let pb_clone = Arc::clone(&pb);

    reader
        .filter(|page| page.id > resume_after_id)
        .par_bridge()
        .for_each(|page| {
            if let Some(max) = limit {
                let current = limit_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if current >= max {
                    return;
                }
            }

            if let PageType::Article = page.page_type {
                let id_str = page.id.to_string();
                stats_clone.inc_articles();

                if let Ok(mut writer) = nodes_writer.lock() {
                    if let Err(e) = writer.write_record([&id_str, &page.title, "Page"]) {
                        warn!(error = %e, "Failed to write node record");
                    }
                }

                if let Some(text) = &page.text {
                    let see_also_start = content::see_also_section_start(text);

                    let mut local_edges: Vec<(String, &str)> = Vec::new();
                    let mut invalid_count = 0u64;

                    for caps in LINK_REGEX.captures_iter(text) {
                        let raw_target = &caps[1];

                        let target_title = strip_section_anchor(raw_target);

                        if target_title.is_empty() || is_namespace_link(target_title) {
                            continue;
                        }

                        if let Some(target_id) = index.resolve_id(target_title) {
                            let edge_type = match see_also_start {
                                Some(sa_start) if caps.get(0).unwrap().start() >= sa_start => {
                                    "SEE_ALSO"
                                }
                                _ => "LINKS_TO",
                            };
                            local_edges.push((target_id.to_string(), edge_type));
                        } else {
                            invalid_count += 1;
                        }
                    }

                    let links_to_count =
                        local_edges.iter().filter(|(_, t)| *t == "LINKS_TO").count() as u64;
                    let see_also_count =
                        local_edges.iter().filter(|(_, t)| *t == "SEE_ALSO").count() as u64;
                    stats_clone.add_edges(links_to_count);
                    stats_clone.add_see_also_edges(see_also_count);
                    stats_clone.add_invalid_links(invalid_count);

                    if !local_edges.is_empty() {
                        if let Ok(mut writer) = edges_writer.lock() {
                            for (end, edge_type) in &local_edges {
                                if let Err(e) =
                                    writer.write_record([id_str.as_str(), end.as_str(), edge_type])
                                {
                                    warn!(error = %e, "Failed to write edge record");
                                }
                            }
                        }
                    }

                    let categories = content::extract_categories(text);
                    if !categories.is_empty() {
                        // Collect newly-seen categories locally, then lock once.
                        let mut new_cats: Vec<&str> = Vec::new();
                        for cat_name in &categories {
                            if !seen_categories.contains(cat_name.as_str())
                                && seen_categories.insert(cat_name.clone())
                            {
                                new_cats.push(cat_name);
                            }
                        }
                        if !new_cats.is_empty() {
                            stats_clone.add_categories(new_cats.len() as u64);
                            if let Ok(mut writer) = categories_writer.lock() {
                                for cat_name in &new_cats {
                                    if let Err(e) =
                                        writer.write_record([*cat_name, *cat_name, "Category"])
                                    {
                                        warn!(error = %e, "Failed to write category record");
                                    }
                                }
                            }
                        }

                        stats_clone.add_category_edges(categories.len() as u64);
                        if let Ok(mut writer) = article_categories_writer.lock() {
                            for cat_name in &categories {
                                if let Err(e) =
                                    writer.write_record([id_str.as_str(), cat_name, "HAS_CATEGORY"])
                                {
                                    warn!(error = %e, "Failed to write category edge record");
                                }
                            }
                        }
                    }

                    let images = content::extract_images(text);
                    if !images.is_empty() {
                        stats_clone.add_images(images.len() as u64);
                        if let Ok(mut writer) = images_writer.lock() {
                            for filename in &images {
                                if let Err(e) =
                                    writer.write_record([id_str.as_str(), filename.as_str()])
                                {
                                    warn!(error = %e, "Failed to write image record");
                                }
                            }
                        }
                    }

                    let ext_links = content::extract_external_links(text);
                    if !ext_links.is_empty() {
                        stats_clone.add_external_links(ext_links.len() as u64);
                        if let Ok(mut writer) = external_links_writer.lock() {
                            for url in &ext_links {
                                if let Err(e) = writer.write_record([id_str.as_str(), url.as_str()])
                                {
                                    warn!(error = %e, "Failed to write external link record");
                                }
                            }
                        }
                    }

                    let infoboxes = infobox::extract_infoboxes(text);
                    if !infoboxes.is_empty() {
                        stats_clone.add_infoboxes(infoboxes.len() as u64);
                    }

                    let abstract_text = content::extract_abstract(text);
                    let sections = content::extract_sections(text);
                    let is_disambig = content::is_disambiguation(text);

                    if !dry_run {
                        let shard = page.id % shard_count;
                        let dir_path = format!("{}/blobs/{:03}", output_dir, shard);

                        let blob = ArticleBlob {
                            id: page.id,
                            title: page.title,
                            abstract_text,
                            categories,
                            infoboxes,
                            sections,
                            timestamp: page.timestamp,
                            is_disambiguation: is_disambig,
                        };

                        let blob_path = format!("{}/{}.json", dir_path, page.id);
                        match File::create(&blob_path) {
                            Ok(f) => {
                                let mut w = BufWriter::new(f);
                                if let Err(e) = serde_json::to_writer(&mut w, &blob) {
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

                max_completed_id.fetch_max(page.id, Ordering::Relaxed);

                if let Some(mgr) = checkpoint_mgr {
                    let highest = max_completed_id.load(Ordering::Relaxed);
                    if let Err(e) = mgr.maybe_save(highest, &stats_clone) {
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
        });

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
