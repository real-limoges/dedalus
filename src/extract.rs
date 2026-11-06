//! Extraction pass: reads every article and writes nodes, edges, categories,
//! images and external links as CSV, and article content as JSON blobs.
//!
//! The dump is read in chunks of pages, in dump order. Each chunk is processed
//! in parallel with rayon; then every writer is flushed and a checkpoint is
//! saved, so a checkpoint only ever describes output that is completely on
//! disk. Reading the next chunk overlaps with processing the current one.
//!
//! CSV rows are distributed across `csv_shards` files by `page_id % csv_shards`.
//! Category, image and external link nodes are deduplicated across the whole
//! run with `DashSet`s; relationship rows are written once per page.

use crate::cache;
use crate::checkpoint::{self, Checkpoint, CheckpointManager, ResumePoint, RunIdentity};
use crate::config::{CSV_WRITER_BUF_SIZE, PAGES_PER_STREAM};
use crate::content;
use crate::content::LINK_REGEX;
use crate::csv_util::CsvType;
use crate::index::WikiIndex;
use crate::infobox;
use crate::models::{ArticleBlob, EdgeType, PageType, WikiPage};
use crate::multistream::{self, StreamRange};
use crate::parser::WikiReader;
use crate::stats::ExtractionStats;
use crate::title::{is_namespace_link, normalize_title};
use anyhow::{Context, Result, ensure};
use dashmap::DashSet;
use indicatif::ProgressBar;
use rayon::prelude::*;
use std::borrow::Cow;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;
use tracing::{debug, info, warn};

// -- CSV output --------------------------------------------------------------

type CsvRowWriter = csv::Writer<Box<dyn Write + Send>>;

/// How the CSV files of a run are opened.
#[derive(Clone, Copy, PartialEq)]
enum OpenMode {
    /// Write nothing (`--dry-run`).
    DryRun,
    /// Create the files and write their headers.
    Fresh,
    /// Append to files already cut back to the checkpoint.
    Resume,
}

/// The files of one CSV type, one per shard, chosen by `page_id % csv_shards`.
struct ShardedCsvWriter {
    file_names: Vec<String>,
    writers: Vec<Mutex<CsvRowWriter>>,
}

impl ShardedCsvWriter {
    fn open(output_dir: &str, csv_type: CsvType, csv_shards: u32, mode: OpenMode) -> Result<Self> {
        let file_names = csv_type.file_names(csv_shards);
        let writers = file_names
            .iter()
            .map(|name| open_csv(output_dir, name, csv_type.header(), mode).map(Mutex::new))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            file_names,
            writers,
        })
    }

    /// The writer for a page's shard.
    fn shard_for(&self, page_id: u32) -> Result<MutexGuard<'_, CsvRowWriter>> {
        let idx = (page_id as usize) % self.writers.len();
        self.writers[idx]
            .lock()
            .map_err(|_| anyhow::anyhow!("CSV writer lock poisoned (a writer thread panicked)"))
    }

    fn flush(&self) -> Result<()> {
        for (name, writer) in self.file_names.iter().zip(&self.writers) {
            writer
                .lock()
                .map_err(|_| anyhow::anyhow!("CSV writer lock poisoned"))?
                .flush()
                .with_context(|| format!("Failed to write {name}"))?;
        }
        Ok(())
    }
}

fn open_csv(
    output_dir: &str,
    file_name: &str,
    header: [&str; 3],
    mode: OpenMode,
) -> Result<CsvRowWriter> {
    let path = Path::new(output_dir).join(file_name);
    let sink: Box<dyn Write + Send> = match mode {
        OpenMode::DryRun => Box::new(std::io::sink()),
        OpenMode::Fresh => {
            let file = File::create(&path)
                .with_context(|| format!("Failed to create {}", path.display()))?;
            Box::new(BufWriter::with_capacity(CSV_WRITER_BUF_SIZE, file))
        }
        OpenMode::Resume => {
            let file = OpenOptions::new()
                .append(true)
                .open(&path)
                .with_context(|| format!("Failed to open {} for append", path.display()))?;
            Box::new(BufWriter::with_capacity(CSV_WRITER_BUF_SIZE, file))
        }
    };
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(sink);
    if mode != OpenMode::Resume {
        writer
            .write_record(header)
            .with_context(|| format!("Failed to write header of {file_name}"))?;
    }
    Ok(writer)
}

/// Every CSV output of the extraction pass.
struct CsvOutputs {
    nodes: ShardedCsvWriter,
    edges: ShardedCsvWriter,
    categories: ShardedCsvWriter,
    article_categories: ShardedCsvWriter,
    image_nodes: ShardedCsvWriter,
    article_images: ShardedCsvWriter,
    external_link_nodes: ShardedCsvWriter,
    article_external_links: ShardedCsvWriter,
}

impl CsvOutputs {
    fn open(output_dir: &str, csv_shards: u32, mode: OpenMode) -> Result<Self> {
        let open = |csv_type| ShardedCsvWriter::open(output_dir, csv_type, csv_shards, mode);
        Ok(Self {
            nodes: open(CsvType::Nodes)?,
            edges: open(CsvType::Edges)?,
            categories: open(CsvType::Categories)?,
            article_categories: open(CsvType::ArticleCategories)?,
            image_nodes: open(CsvType::ImageNodes)?,
            article_images: open(CsvType::ArticleImages)?,
            external_link_nodes: open(CsvType::ExternalLinkNodes)?,
            article_external_links: open(CsvType::ArticleExternalLinks)?,
        })
    }

    fn all(&self) -> [&ShardedCsvWriter; 8] {
        [
            &self.nodes,
            &self.edges,
            &self.categories,
            &self.article_categories,
            &self.image_nodes,
            &self.article_images,
            &self.external_link_nodes,
            &self.article_external_links,
        ]
    }

    /// Flushes every file. Any write error, including one held back by a
    /// buffer since an earlier row, surfaces here.
    fn flush(&self) -> Result<()> {
        self.all().iter().try_for_each(|w| w.flush())
    }

    /// Every file's name and current length. Call right after `flush`.
    fn lengths(&self, output_dir: &str) -> Result<Vec<(String, u64)>> {
        self.all()
            .iter()
            .flat_map(|w| w.file_names.iter())
            .map(|name| {
                let path = Path::new(output_dir).join(name);
                let len = fs::metadata(&path)
                    .with_context(|| format!("Failed to stat {}", path.display()))?
                    .len();
                Ok((name.clone(), len))
            })
            .collect()
    }
}

/// Cuts every CSV back to its length at the checkpoint, discarding rows that
/// were written after it (including a half-written last row).
fn truncate_to_checkpoint(output_dir: &str, csv_lengths: &[(String, u64)]) -> Result<()> {
    for (name, len) in csv_lengths {
        let path = Path::new(output_dir).join(name);
        let file = OpenOptions::new()
            .write(true)
            .open(&path)
            .with_context(|| format!("Cannot resume: {} is missing", path.display()))?;
        let actual = file.metadata()?.len();
        ensure!(
            actual >= *len,
            "Cannot resume: {} is shorter ({actual} bytes) than at the checkpoint ({len} bytes)",
            path.display()
        );
        file.set_len(*len)
            .with_context(|| format!("Failed to truncate {}", path.display()))?;
    }
    Ok(())
}

/// The names already written to a node CSV, so a resumed run does not write
/// a category, image or external link node a second time.
fn seen_names_in(output_dir: &str, csv_type: CsvType, csv_shards: u32) -> Result<DashSet<String>> {
    let seen = DashSet::new();
    for name in csv_type.file_names(csv_shards) {
        let path = Path::new(output_dir).join(&name);
        let mut reader = csv::Reader::from_path(&path)
            .with_context(|| format!("Failed to read {}", path.display()))?;
        for record in reader.records() {
            let record = record.with_context(|| format!("Malformed row in {name}"))?;
            if let Some(id) = record.get(0) {
                seen.insert(id.to_string());
            }
        }
    }
    Ok(seen)
}

// -- Per-page extraction -----------------------------------------------------

/// Extracts edges from article text: a link inside the "See also" section is
/// SeeAlso, any other link LinksTo. Returns (deduplicated edges, invalid link count).
fn process_article_edges(
    text: &str,
    index: &WikiIndex,
    see_also: Option<std::ops::Range<usize>>,
) -> (Vec<(u32, EdgeType)>, u64) {
    let mut local_edges: Vec<(u32, EdgeType)> = Vec::with_capacity(16);
    let mut invalid_count = 0u64;

    for caps in LINK_REGEX.captures_iter(text) {
        let target_title = normalize_title(&caps[1]);
        if target_title.is_empty() || is_namespace_link(&target_title) {
            continue;
        }
        if let Some(target_id) = index.resolve_id(&target_title) {
            let link_start = caps.get(0).map_or(0, |m| m.start());
            let edge_type = match &see_also {
                Some(section) if section.contains(&link_start) => EdgeType::SeeAlso,
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

/// Writes an article's JSON blob to its shard directory, flushing so that a
/// write error is returned rather than lost when the writer is dropped.
fn write_article_blob(output_dir: &str, shard_count: u32, blob: &ArticleBlob) -> Result<()> {
    let shard = blob.id % shard_count;
    let blob_path = format!("{}/blobs/{:03}/{}.json", output_dir, shard, blob.id);
    let file = File::create(&blob_path).with_context(|| format!("Failed to create {blob_path}"))?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, blob)
        .with_context(|| format!("Failed to serialize {blob_path}"))?;
    writer
        .flush()
        .with_context(|| format!("Failed to write {blob_path}"))?;
    debug!(id = blob.id, "Wrote blob");
    Ok(())
}

/// One kind of deduplicated entity: its node file, its relationship file, and
/// the names written so far.
struct EntityOutput<'a> {
    nodes: &'a ShardedCsvWriter,
    relationships: &'a ShardedCsvWriter,
    seen: &'a DashSet<String>,
    label: &'static str,
    rel_type: &'static str,
}

impl EntityOutput<'_> {
    /// Writes a node row for each name not seen before in the run and a
    /// relationship row for every name. Returns how many names were new.
    fn write(&self, page_id: u32, id_str: &str, names: &[Cow<'_, str>]) -> Result<u64> {
        // Only the thread whose insert succeeds writes the node row.
        let new_names: Vec<&str> = names
            .iter()
            .map(|n| n.as_ref())
            .filter(|n| !self.seen.contains(*n) && self.seen.insert((*n).to_owned()))
            .collect();

        if !new_names.is_empty() {
            let mut writer = self.nodes.shard_for(page_id)?;
            for name in &new_names {
                writer.write_record([*name, *name, self.label])?;
            }
        }

        let mut writer = self.relationships.shard_for(page_id)?;
        for name in names {
            writer.write_record([id_str, name.as_ref(), self.rel_type])?;
        }

        Ok(new_names.len() as u64)
    }
}

/// Everything a worker needs to turn one page into output rows.
struct Extractor<'a> {
    index: &'a WikiIndex,
    output_dir: &'a str,
    shard_count: u32,
    dry_run: bool,
    csv: CsvOutputs,
    seen_categories: DashSet<String>,
    seen_images: DashSet<String>,
    seen_external_links: DashSet<String>,
    stats: &'a ExtractionStats,
}

impl Extractor<'_> {
    fn categories(&self) -> EntityOutput<'_> {
        EntityOutput {
            nodes: &self.csv.categories,
            relationships: &self.csv.article_categories,
            seen: &self.seen_categories,
            label: "Category",
            rel_type: "HAS_CATEGORY",
        }
    }

    fn images(&self) -> EntityOutput<'_> {
        EntityOutput {
            nodes: &self.csv.image_nodes,
            relationships: &self.csv.article_images,
            seen: &self.seen_images,
            label: "Image",
            rel_type: "HAS_IMAGE",
        }
    }

    fn external_links(&self) -> EntityOutput<'_> {
        EntityOutput {
            nodes: &self.csv.external_link_nodes,
            relationships: &self.csv.article_external_links,
            seen: &self.seen_external_links,
            label: "ExternalLink",
            rel_type: "HAS_LINK",
        }
    }

    /// Writes every row and the blob for one page. Pages other than articles
    /// produce nothing.
    fn process_page(&self, page: WikiPage) -> Result<()> {
        if page.page_type != PageType::Article {
            return Ok(());
        }

        let mut itoa_buf = itoa::Buffer::new();
        let id_str = itoa_buf.format(page.id);
        self.stats.inc_articles();
        self.csv
            .nodes
            .shard_for(page.id)?
            .write_record([id_str, &page.title, "Page"])?;

        let Some(text) = &page.text else {
            return Ok(());
        };

        // -- Edges --
        let (edges, invalid_count) =
            process_article_edges(text, self.index, content::see_also_section_range(text));
        let see_also_count = edges
            .iter()
            .filter(|(_, t)| *t == EdgeType::SeeAlso)
            .count() as u64;
        self.stats.add_edges(edges.len() as u64 - see_also_count);
        self.stats.add_see_also_edges(see_also_count);
        self.stats.add_invalid_links(invalid_count);
        if !edges.is_empty() {
            let mut end_buf = itoa::Buffer::new();
            let mut writer = self.csv.edges.shard_for(page.id)?;
            for (end_id, edge_type) in &edges {
                let type_str = edge_type.to_string();
                writer.write_record([id_str, end_buf.format(*end_id), &type_str])?;
            }
        }

        // -- Categories, images, external links --
        let categories = content::extract_categories(text);
        let new_categories = self.categories().write(page.id, id_str, &categories)?;
        self.stats.add_categories(new_categories);
        self.stats.add_category_edges(categories.len() as u64);

        let images = content::extract_images(text);
        self.stats
            .add_images(self.images().write(page.id, id_str, &images)?);

        let links = content::extract_external_links(text);
        self.stats
            .add_external_links(self.external_links().write(page.id, id_str, &links)?);

        // -- Infoboxes and blob --
        let infoboxes = infobox::extract_infoboxes(text);
        self.stats.add_infoboxes(infoboxes.len() as u64);

        if !self.dry_run {
            let blob = ArticleBlob {
                id: page.id,
                abstract_text: content::extract_abstract(text),
                categories: categories.into_iter().map(Cow::into_owned).collect(),
                infoboxes,
                sections: content::extract_sections(text),
                is_disambiguation: content::is_disambiguation(text),
                title: page.title,
                timestamp: page.timestamp,
            };
            write_article_blob(self.output_dir, self.shard_count, &blob)?;
            self.stats.inc_blobs();
        }
        Ok(())
    }

    /// Processes one chunk of pages in parallel. Any write error stops the run.
    fn process_chunk(&self, pages: Vec<WikiPage>) -> Result<()> {
        pages
            .into_par_iter()
            .try_for_each(|page| self.process_page(page))
    }
}

// -- Reading the dump in chunks ----------------------------------------------

/// The dump as a sequence of page chunks, in dump order.
// One PageSource exists per run, so the size difference between variants costs nothing.
#[allow(clippy::large_enum_variant)]
enum PageSource<'a> {
    /// A standard dump, decompressed and parsed front to back.
    Sequential { reader: WikiReader, pages_read: u64 },
    /// A multistream dump, parsed several streams at a time in parallel.
    Multistream {
        dump_path: &'a str,
        ranges: &'a [StreamRange],
        next_stream: usize,
    },
}

impl<'a> PageSource<'a> {
    /// Opens the dump, positioned just after `resume` if given.
    fn open(
        dump_path: &'a str,
        ranges: Option<&'a [StreamRange]>,
        resume: Option<ResumePoint>,
    ) -> Result<Self> {
        match (ranges, resume) {
            (Some(ranges), None) => Ok(Self::Multistream {
                dump_path,
                ranges,
                next_stream: 0,
            }),
            (Some(ranges), Some(ResumePoint::AfterStreams(done))) => Ok(Self::Multistream {
                dump_path,
                ranges,
                next_stream: done,
            }),
            (None, resume) => {
                let skip = match resume {
                    None => 0,
                    Some(ResumePoint::AfterPages(done)) => done,
                    Some(other) => anyhow::bail!("Cannot resume a standard dump from {other:?}"),
                };
                let mut reader = WikiReader::new(dump_path, false)
                    .with_context(|| format!("Failed to open wiki dump: {dump_path}"))?;
                if skip > 0 {
                    info!(
                        pages = skip,
                        "Skipping pages finished before the checkpoint"
                    );
                    let skipped = reader.by_ref().take(skip as usize).count() as u64;
                    ensure!(
                        skipped == skip,
                        "The dump ended after {skipped} pages, before the checkpoint at {skip}"
                    );
                }
                Ok(Self::Sequential {
                    reader,
                    pages_read: skip,
                })
            }
            (Some(_), Some(other)) => {
                anyhow::bail!("Cannot resume a multistream dump from {other:?}")
            }
        }
    }

    /// The next chunk of about `chunk_pages` pages, or `None` at the end.
    fn next_chunk(&mut self, chunk_pages: usize) -> Result<Option<Vec<WikiPage>>> {
        match self {
            Self::Sequential { reader, pages_read } => {
                let chunk: Vec<WikiPage> = reader.by_ref().take(chunk_pages).collect();
                *pages_read += chunk.len() as u64;
                Ok((!chunk.is_empty()).then_some(chunk))
            }
            Self::Multistream {
                dump_path,
                ranges,
                next_stream,
            } => {
                if *next_stream >= ranges.len() {
                    return Ok(None);
                }
                let streams = (chunk_pages / PAGES_PER_STREAM).max(1);
                let end = (*next_stream + streams).min(ranges.len());
                let pages =
                    multistream::parse_streams(dump_path, &ranges[*next_stream..end], false)?;
                *next_stream = end;
                Ok(Some(pages))
            }
        }
    }

    /// The resume point just after the last chunk returned.
    fn position(&self) -> ResumePoint {
        match self {
            Self::Sequential { pages_read, .. } => ResumePoint::AfterPages(*pages_read),
            Self::Multistream { next_stream, .. } => ResumePoint::AfterStreams(*next_stream),
        }
    }

    /// Reports whether the dump was read cleanly (see `WikiReader::finish`).
    fn finish(self) -> Result<()> {
        match self {
            Self::Sequential { reader, .. } => reader.finish(),
            Self::Multistream { .. } => Ok(()),
        }
    }
}

/// Cuts a chunk down so the run reads at most `limit` pages in total.
/// Returns `true` if the limit has now been reached.
fn apply_limit(pages: &mut Vec<WikiPage>, limit: Option<u64>, pages_read: u64) -> bool {
    let Some(limit) = limit else {
        return false;
    };
    let remaining = limit.saturating_sub(pages_read) as usize;
    if pages.len() >= remaining {
        pages.truncate(remaining);
        true
    } else {
        false
    }
}

// -- Running the pass --------------------------------------------------------

/// Configuration for the Wikipedia extraction pass.
#[derive(Debug, Clone)]
pub struct ExtractionConfig<'a> {
    pub input_path: &'a str,
    pub output_dir: &'a str,
    pub index: &'a WikiIndex,
    pub shard_count: u32,
    pub csv_shards: u32,
    /// Maximum pages to read from the start of the dump.
    pub limit: Option<u64>,
    pub dry_run: bool,
    /// Pages per chunk; a checkpoint is saved after each chunk.
    pub chunk_pages: u32,
    pub resume_from: Option<&'a Checkpoint>,
    pub checkpoint_mgr: Option<&'a CheckpointManager>,
    pub multistream_ranges: Option<&'a [StreamRange]>,
}

/// How an extraction run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtractionEnd {
    /// The whole dump (or `--limit` pages) was processed.
    Completed,
    /// The cancel flag was set. Output and checkpoint describe the last
    /// finished chunk, so the run can be resumed.
    Cancelled,
}

/// Runs extraction with fresh stats and no cancel flag. Returns final statistics.
pub fn run_extraction(config: &ExtractionConfig) -> Result<ExtractionStats> {
    let stats = Arc::new(ExtractionStats::new());
    let cancel = Arc::new(AtomicBool::new(false));
    run_extraction_with_stats(config, Arc::clone(&stats), cancel, false)?;
    Ok(stats.snapshot())
}

/// Runs extraction, updating `stats` as it goes and stopping between chunks
/// once `cancel` is set.
pub fn run_extraction_with_stats(
    config: &ExtractionConfig,
    stats: Arc<ExtractionStats>,
    cancel: Arc<AtomicBool>,
    hide_progress: bool,
) -> Result<ExtractionEnd> {
    let output_dir = config.output_dir;
    ensure!(config.csv_shards >= 1, "--csv-shards must be at least 1");
    ensure!(config.shard_count >= 1, "--shard-count must be at least 1");

    let resume = config.resume_from;
    if let Some(cp) = resume {
        stats.restore_from(&cp.stats);
        info!(
            resume_point = ?cp.resume_point,
            articles = stats.articles(),
            "Resuming from checkpoint"
        );
    }

    let mode = match (config.dry_run, resume) {
        (true, _) => OpenMode::DryRun,
        (false, None) => OpenMode::Fresh,
        (false, Some(_)) => OpenMode::Resume,
    };
    if mode == OpenMode::DryRun {
        info!("Dry run mode - no files will be written");
    } else {
        prepare_output_dir(output_dir, config.shard_count)?;
    }
    if mode == OpenMode::Fresh {
        remove_stale_csvs(output_dir, config.csv_shards)?;
    }
    if let Some(cp) = resume {
        truncate_to_checkpoint(output_dir, &cp.csv_lengths)?;
    }

    let seen = |csv_type| match mode {
        OpenMode::Resume => seen_names_in(output_dir, csv_type, config.csv_shards),
        _ => Ok(DashSet::new()),
    };
    let extractor = Extractor {
        index: config.index,
        output_dir,
        shard_count: config.shard_count,
        dry_run: config.dry_run,
        csv: CsvOutputs::open(output_dir, config.csv_shards, mode)?,
        seen_categories: seen(CsvType::Categories)?,
        seen_images: seen(CsvType::ImageNodes)?,
        seen_external_links: seen(CsvType::ExternalLinkNodes)?,
        stats: &stats,
    };

    info!("Starting extraction from: {}", config.input_path);
    if let Some(ranges) = config.multistream_ranges {
        info!(
            streams = ranges.len(),
            "Using multistream parallel extraction"
        );
    }
    let mut source = PageSource::open(
        config.input_path,
        config.multistream_ranges,
        resume.map(|cp| cp.resume_point),
    )?;
    let chunk_pages = config.chunk_pages.max(1) as usize;
    let mut pages_read = resume.map_or(0, |cp| cp.pages_read);
    let progress = progress_bar(hide_progress);

    let mut chunk = source.next_chunk(chunk_pages)?;
    let end = loop {
        let Some(mut pages) = chunk else {
            break ExtractionEnd::Completed;
        };
        if cancel.load(Ordering::Acquire) {
            break ExtractionEnd::Cancelled;
        }
        let limit_reached = apply_limit(&mut pages, config.limit, pages_read);
        pages_read += pages.len() as u64;
        let done_point = source.position();

        // Read the next chunk while this one is processed.
        let (next, processed) = if limit_reached {
            (Ok(None), extractor.process_chunk(pages))
        } else {
            rayon::join(
                || source.next_chunk(chunk_pages),
                || extractor.process_chunk(pages),
            )
        };
        processed?;
        extractor.csv.flush()?;
        // A chunk cut short by --limit ends the run, and the source has read
        // past the pages that were dropped, so it gets no checkpoint.
        if let Some(mgr) = config.checkpoint_mgr
            && !limit_reached
        {
            let lengths = extractor.csv.lengths(output_dir)?;
            mgr.save(done_point, pages_read, lengths, &stats)?;
        }
        progress.set_message(format!(
            "Extracting: {} articles, {} edges, {} blobs",
            stats.articles(),
            stats.edges(),
            stats.blobs()
        ));
        chunk = next?;
    };

    progress.finish_and_clear();
    extractor.csv.flush()?;
    if end == ExtractionEnd::Completed {
        source.finish()?;
    }

    info!(
        articles = stats.articles(),
        edges = stats.edges(),
        blobs = stats.blobs(),
        invalid_links = stats.invalid(),
        categories = stats.categories(),
        infoboxes = stats.infoboxes(),
        ended = ?end,
        "Extraction finished"
    );
    Ok(end)
}

/// Checks the output directory is writable and creates every blob shard
/// directory once, rather than per article.
fn prepare_output_dir(output_dir: &str, shard_count: u32) -> Result<()> {
    let output_path = Path::new(output_dir);
    fs::create_dir_all(output_path)
        .with_context(|| format!("Failed to create output directory: {}", output_dir))?;

    let test_file = output_path.join(".write_test");
    fs::write(&test_file, "test")
        .with_context(|| format!("Output directory is not writable: {}", output_dir))?;
    fs::remove_file(&test_file).ok();

    for shard in 0..shard_count {
        let dir_path = format!("{}/blobs/{:03}", output_dir, shard);
        fs::create_dir_all(&dir_path)
            .with_context(|| format!("Failed to create blob directory: {}", dir_path))?;
    }
    Ok(())
}

/// Removes CSV files an earlier run left that this run will not overwrite:
/// merged files when this run shards, shards of another count, and the
/// earlier run's `analytics.csv`. Without this, a later merge or analytics
/// run could read an old `nodes.csv` next to new shards.
fn remove_stale_csvs(output_dir: &str, csv_shards: u32) -> Result<()> {
    let analytics = Path::new(output_dir).join(crate::analytics::ANALYTICS_FILE);
    let stale = crate::csv_util::stale_csv_files(output_dir, csv_shards)?
        .into_iter()
        .chain(analytics.exists().then_some(analytics));
    for path in stale {
        info!(path = %path.display(), "Removing CSV from an earlier run");
        fs::remove_file(&path)
            .with_context(|| format!("Failed to remove stale {}", path.display()))?;
    }
    Ok(())
}

fn progress_bar(hidden: bool) -> ProgressBar {
    if hidden {
        return ProgressBar::hidden();
    }
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .expect("valid progress template"),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}

// -- The whole `extract` command ----------------------------------------------

/// Everything the `extract` command needs, as given on the command line or in
/// the TUI. Shared by both so they cannot drift apart.
#[derive(Debug, Clone)]
pub struct ExtractJob {
    pub input: String,
    pub output: String,
    pub shard_count: u32,
    pub csv_shards: u32,
    pub limit: Option<u64>,
    pub dry_run: bool,
    pub resume: bool,
    pub no_cache: bool,
    pub clean: bool,
    pub checkpoint_interval: u32,
    /// Multistream index path; detected from the dump's name when `None`.
    pub multistream_index: Option<String>,
}

/// What an `extract` command did.
#[derive(Debug, Clone, Copy)]
pub struct ExtractReport {
    pub end: ExtractionEnd,
    pub indexing_secs: f64,
    pub extraction_secs: f64,
}

/// Runs the `extract` command: optional clean, multistream detection, index
/// (from cache when valid), checkpoint lookup, and the extraction pass. The
/// checkpoint is cleared only when the run completes.
pub fn run_extract_job(
    job: &ExtractJob,
    stats: Arc<ExtractionStats>,
    cancel: Arc<AtomicBool>,
    hide_progress: bool,
) -> Result<ExtractReport> {
    ensure!(job.csv_shards >= 1, "--csv-shards must be at least 1");
    ensure!(job.shard_count >= 1, "--shard-count must be at least 1");

    if job.clean && Path::new(&job.output).exists() {
        info!("Cleaning output directory: {}", job.output);
        fs::remove_dir_all(&job.output)
            .with_context(|| format!("Failed to clean output directory: {}", job.output))?;
    }
    fs::create_dir_all(&job.output)
        .with_context(|| format!("Failed to create output directory: {}", job.output))?;

    let index_path = job
        .multistream_index
        .clone()
        .or_else(|| multistream::detect_index_path(&job.input));
    let ranges = match &index_path {
        Some(path) => {
            info!(index = %path, "Using multistream parallel parsing");
            Some(multistream::parse_multistream_index(path, &job.input)?)
        }
        None => None,
    };

    let start_indexing = Instant::now();
    let index = load_or_build_index(job, ranges.as_deref())?;
    let indexing_secs = start_indexing.elapsed().as_secs_f64();
    info!(duration_secs = indexing_secs, "Indexing complete");

    let identity = RunIdentity::of(
        &job.input,
        &job.output,
        job.shard_count,
        job.csv_shards,
        ranges.is_some(),
    )?;
    let checkpoint_mgr = (!job.dry_run).then(|| CheckpointManager::new(identity.clone()));
    let checkpoint = if job.resume && !job.clean {
        let found = checkpoint::load_if_valid(&identity)?;
        if found.is_none() {
            info!("No valid checkpoint found, starting fresh");
        }
        found
    } else {
        None
    };

    if cancel.load(Ordering::Acquire) {
        return Ok(ExtractReport {
            end: ExtractionEnd::Cancelled,
            indexing_secs,
            extraction_secs: 0.0,
        });
    }

    let start_extracting = Instant::now();
    let config = ExtractionConfig {
        input_path: &job.input,
        output_dir: &job.output,
        index: &index,
        shard_count: job.shard_count,
        csv_shards: job.csv_shards,
        limit: job.limit,
        dry_run: job.dry_run,
        chunk_pages: job.checkpoint_interval,
        resume_from: checkpoint.as_ref(),
        checkpoint_mgr: checkpoint_mgr.as_ref(),
        multistream_ranges: ranges.as_deref(),
    };
    let end = run_extraction_with_stats(&config, stats, cancel, hide_progress)?;
    let extraction_secs = start_extracting.elapsed().as_secs_f64();

    if end == ExtractionEnd::Completed
        && let Some(mgr) = &checkpoint_mgr
    {
        mgr.clear()?;
    }

    Ok(ExtractReport {
        end,
        indexing_secs,
        extraction_secs,
    })
}

/// Loads the index from cache when valid, otherwise builds and caches it.
fn load_or_build_index(job: &ExtractJob, ranges: Option<&[StreamRange]>) -> Result<WikiIndex> {
    let cache_path = cache::cache_path(&job.output);
    if job.no_cache {
        info!("Cache disabled, building fresh index");
    } else if let Some(index) = cache::try_load_index(&cache_path, &job.input)? {
        return Ok(index);
    } else {
        info!("Building index (cache missing or stale)");
    }

    let index = match ranges {
        Some(ranges) => WikiIndex::build_multistream(&job.input, ranges)?,
        None => WikiIndex::build(&job.input)?,
    };
    if !job.dry_run
        && let Err(e) = cache::save_index(&index, &job.input, &job.output)
    {
        warn!(error = %e, "Failed to save index cache");
    }
    Ok(index)
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
        let caps: Vec<_> = LINK_REGEX.captures_iter("[[]]").collect();
        assert!(caps.is_empty());
    }

    fn edges_index() -> WikiIndex {
        let articles = [("Rust", 1u32), ("United States", 2), ("Python", 3)]
            .into_iter()
            .map(|(t, id)| (t.to_string(), id))
            .collect();
        WikiIndex::from_maps(articles, Default::default())
    }

    fn edges_of(text: &str) -> (Vec<(u32, EdgeType)>, u64) {
        process_article_edges(text, &edges_index(), content::see_also_section_range(text))
    }

    #[test]
    fn edges_resolve_mediawiki_spellings() {
        let (edges, invalid) = edges_of("[[rust]] [[United_States]] [[ Python ]] [[Rust#History]]");
        assert_eq!(
            edges,
            vec![
                (1, EdgeType::LinksTo),
                (2, EdgeType::LinksTo),
                (3, EdgeType::LinksTo)
            ]
        );
        assert_eq!(invalid, 0);
    }

    #[test]
    fn edges_skip_namespace_links_in_any_case() {
        let (edges, invalid) =
            edges_of("[[Category:Rust]] [[category:Rust]] [[:Category:Rust]] [[File:Rust.png]]");
        assert!(edges.is_empty());
        assert_eq!(invalid, 0);
    }

    #[test]
    fn edges_after_see_also_section_are_links_to() {
        let text = "[[Rust]]\n== See also ==\n* [[Python]]\n== References ==\n[[United States]]";
        let (edges, _) = edges_of(text);
        assert_eq!(
            edges,
            vec![
                (1, EdgeType::LinksTo),
                (2, EdgeType::LinksTo),
                (3, EdgeType::SeeAlso)
            ]
        );
    }

    fn page(id: u32) -> WikiPage {
        WikiPage {
            id,
            title: format!("P{id}"),
            page_type: PageType::Article,
            text: None,
            ns: Some(0),
            timestamp: None,
        }
    }

    #[test]
    fn apply_limit_cuts_the_chunk_that_crosses_it() {
        let mut pages: Vec<_> = (1..=10).map(page).collect();
        assert!(apply_limit(&mut pages, Some(25), 20));
        assert_eq!(pages.len(), 5);
    }

    #[test]
    fn apply_limit_leaves_chunks_below_it() {
        let mut pages: Vec<_> = (1..=10).map(page).collect();
        assert!(!apply_limit(&mut pages, Some(25), 0));
        assert_eq!(pages.len(), 10);
        assert!(!apply_limit(&mut pages, None, 1_000));
    }

    #[test]
    fn truncate_to_checkpoint_discards_rows_after_it() {
        let dir = tempfile::TempDir::new().unwrap();
        let out = dir.path().to_str().unwrap();
        fs::write(dir.path().join("nodes.csv"), "header\nrow1\nrow2 partial").unwrap();

        truncate_to_checkpoint(out, &[("nodes.csv".to_string(), 12)]).unwrap();

        assert_eq!(
            fs::read_to_string(dir.path().join("nodes.csv")).unwrap(),
            "header\nrow1\n"
        );
    }

    #[test]
    fn truncate_to_checkpoint_rejects_a_shorter_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let out = dir.path().to_str().unwrap();
        fs::write(dir.path().join("nodes.csv"), "head").unwrap();
        assert!(truncate_to_checkpoint(out, &[("nodes.csv".to_string(), 12)]).is_err());
    }
}
