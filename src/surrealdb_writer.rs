//! SurrealDB embedded writer -- loads extracted CSVs into a SurrealDB RocksDB database.
//!
//! Replaces the former Neo4j import module. Reads merged CSV files (nodes + edges)
//! and writes them to an embedded SurrealDB instance backed by RocksDB. Only articles
//! and links_to edges are loaded; categories, images, and external links remain as
//! CSV-only output. Uses concurrent batch inserts for throughput.

use crate::config;
use crate::csv_util::{self, CsvLayout};
use anyhow::{Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use surrealdb::Surreal;
use surrealdb::engine::local::RocksDb;
use tracing::info;

/// Maximum number of concurrent batch insert tasks.
const MAX_CONCURRENT_BATCHES: usize = 8;

/// Configuration for the SurrealDB load step.
#[derive(Debug, Clone)]
pub struct SurrealWriterConfig {
    pub output_dir: String,
    pub db_path: String,
    pub batch_size: usize,
    pub clean: bool,
}

impl Default for SurrealWriterConfig {
    fn default() -> Self {
        Self {
            output_dir: String::new(),
            db_path: config::DEFAULT_DB_PATH.to_string(),
            batch_size: config::SURREAL_BATCH_SIZE,
            clean: false,
        }
    }
}

/// Statistics returned after a successful load.
#[derive(Debug, Default)]
pub struct LoadStats {
    pub articles_loaded: u64,
    pub edges_loaded: u64,
    pub elapsed_secs: f64,
}

/// Loads extracted CSV data into an embedded SurrealDB instance.
///
/// Reads `nodes.csv` and `edges.csv` from `output_dir`, creates the schema,
/// and batch-inserts records using concurrent tasks. The database is stored at
/// `db_path` (relative to `output_dir` if not absolute).
pub async fn run_surreal_load(config: SurrealWriterConfig) -> Result<LoadStats> {
    let start = Instant::now();

    let db_path = if Path::new(&config.db_path).is_absolute() {
        config.db_path.clone()
    } else {
        Path::new(&config.output_dir)
            .join(&config.db_path)
            .to_string_lossy()
            .to_string()
    };

    if config.clean && Path::new(&db_path).exists() {
        info!("Cleaning existing database: {}", db_path);
        std::fs::remove_dir_all(&db_path)
            .with_context(|| format!("Failed to remove existing DB: {}", db_path))?;
    }

    // Detect CSV layout and validate nodes + edges exist
    let layout = csv_util::detect_csv_layout(&config.output_dir)?;
    if !matches!(layout, CsvLayout::Single) {
        anyhow::bail!(
            "SurrealDB load requires merged (non-sharded) CSVs.\n\
             Run 'dedalus merge-csvs -o {}' first, or use --csv-shards 1.",
            config.output_dir
        );
    }
    info!("Detected {} CSV layout", layout);

    // Connect to embedded SurrealDB with RocksDB backend
    info!("Opening SurrealDB at {}", db_path);
    let db = Surreal::new::<RocksDb>(&db_path)
        .await
        .with_context(|| format!("Failed to open SurrealDB at {}", db_path))?;

    db.use_ns(config::SURREAL_NAMESPACE)
        .use_db(config::SURREAL_DATABASE)
        .await
        .context("Failed to select namespace/database")?;

    // Create schema
    create_schema(&db).await?;

    // Load articles from nodes.csv
    let nodes_path = Path::new(&config.output_dir).join("nodes.csv");
    let articles_loaded = load_articles(&db, &nodes_path, config.batch_size).await?;

    // Load edges from edges.csv
    let edges_path = Path::new(&config.output_dir).join("edges.csv");
    let edges_loaded = load_edges(&db, &edges_path, config.batch_size).await?;

    let elapsed = start.elapsed();
    info!(
        articles = articles_loaded,
        edges = edges_loaded,
        elapsed_secs = elapsed.as_secs_f64(),
        "SurrealDB load complete"
    );

    Ok(LoadStats {
        articles_loaded,
        edges_loaded,
        elapsed_secs: elapsed.as_secs_f64(),
    })
}

async fn create_schema(db: &Surreal<surrealdb::engine::local::Db>) -> Result<()> {
    info!("Creating SurrealDB schema");

    db.query(
        "
        DEFINE TABLE article SCHEMAFULL;
        DEFINE FIELD title ON article TYPE string;
        DEFINE FIELD pagerank ON article TYPE option<float> DEFAULT NONE;
        DEFINE FIELD community ON article TYPE option<int> DEFAULT NONE;
        DEFINE FIELD degree ON article TYPE option<int> DEFAULT NONE;
        DEFINE INDEX idx_article_title ON article FIELDS title;

        DEFINE TABLE links_to SCHEMAFULL TYPE RELATION FROM article TO article;
        ",
    )
    .await
    .context("Failed to create schema")?
    .check()
    .context("Schema creation returned errors")?;

    info!("Schema created");
    Ok(())
}

/// Escapes a string for use in a SurQL single-quoted string literal.
fn escape_surql(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

async fn load_articles(
    db: &Surreal<surrealdb::engine::local::Db>,
    csv_path: &Path,
    batch_size: usize,
) -> Result<u64> {
    info!(
        concurrency = MAX_CONCURRENT_BATCHES,
        "Loading articles from {:?}", csv_path
    );

    let mut reader = csv::Reader::from_path(csv_path)
        .with_context(|| format!("Failed to open {:?}", csv_path))?;

    let counter = Arc::new(AtomicU64::new(0));
    let mut in_flight = FuturesUnordered::new();
    let mut batch = String::with_capacity(batch_size * 80);
    let mut batch_count = 0usize;

    for result in reader.records() {
        let record = result.context("Failed to read CSV record")?;
        let id = record.get(0).unwrap_or("");
        let title = record.get(1).unwrap_or("");

        let escaped_title = escape_surql(title);
        batch.push_str(&format!(
            "CREATE article:{id} SET title = '{escaped_title}';\n"
        ));
        batch_count += 1;

        if batch_count >= batch_size {
            // If at max concurrency, wait for one to finish
            #[allow(clippy::collapsible_if)]
            if in_flight.len() >= MAX_CONCURRENT_BATCHES {
                if let Some(result) = in_flight.next().await {
                    match result {
                        Ok(inner) => inner?,
                        Err(e) => anyhow::bail!("Task join error: {e}"),
                    }
                }
            }

            let query = std::mem::replace(&mut batch, String::with_capacity(batch_size * 80));
            let db = db.clone();
            let counter = Arc::clone(&counter);
            let n = batch_count as u64;

            in_flight.push(tokio::spawn(async move {
                db.query(&query)
                    .await
                    .context("Failed to insert article batch")?;
                let total = counter.fetch_add(n, Ordering::Relaxed) + n;
                if total.is_multiple_of(100_000) {
                    info!(count = total, "Articles loaded");
                }
                Ok::<(), anyhow::Error>(())
            }));

            batch_count = 0;
        }
    }

    // Flush remaining batch
    if batch_count > 0 {
        if in_flight.len() >= MAX_CONCURRENT_BATCHES
            && let Some(result) = in_flight.next().await
        {
            match result {
                Ok(inner) => inner?,
                Err(e) => anyhow::bail!("Task join error: {e}"),
            }
        }
        let query = batch;
        let db = db.clone();
        let counter = Arc::clone(&counter);
        let n = batch_count as u64;
        in_flight.push(tokio::spawn(async move {
            db.query(&query)
                .await
                .context("Failed to insert final article batch")?;
            counter.fetch_add(n, Ordering::Relaxed);
            Ok::<(), anyhow::Error>(())
        }));
    }

    // Wait for all in-flight batches
    while let Some(result) = in_flight.next().await {
        match result {
            Ok(inner) => inner?,
            Err(e) => anyhow::bail!("Task join error: {e}"),
        }
    }

    let total = counter.load(Ordering::Relaxed);
    info!(total, "Articles loaded");
    Ok(total)
}

async fn load_edges(
    db: &Surreal<surrealdb::engine::local::Db>,
    csv_path: &Path,
    batch_size: usize,
) -> Result<u64> {
    info!(
        concurrency = MAX_CONCURRENT_BATCHES,
        "Loading edges from {:?}", csv_path
    );

    let mut reader = csv::Reader::from_path(csv_path)
        .with_context(|| format!("Failed to open {:?}", csv_path))?;

    let counter = Arc::new(AtomicU64::new(0));
    let mut in_flight = FuturesUnordered::new();
    let mut batch = String::with_capacity(batch_size * 60);
    let mut batch_count = 0usize;

    for result in reader.records() {
        let record = result.context("Failed to read CSV record")?;
        let start_id = record.get(0).unwrap_or("");
        let end_id = record.get(1).unwrap_or("");

        batch.push_str(&format!(
            "RELATE article:{start_id}->links_to->article:{end_id};\n"
        ));
        batch_count += 1;

        if batch_count >= batch_size {
            #[allow(clippy::collapsible_if)]
            if in_flight.len() >= MAX_CONCURRENT_BATCHES {
                if let Some(result) = in_flight.next().await {
                    match result {
                        Ok(inner) => inner?,
                        Err(e) => anyhow::bail!("Task join error: {e}"),
                    }
                }
            }

            let query = std::mem::replace(&mut batch, String::with_capacity(batch_size * 60));
            let db = db.clone();
            let counter = Arc::clone(&counter);
            let n = batch_count as u64;

            in_flight.push(tokio::spawn(async move {
                db.query(&query)
                    .await
                    .context("Failed to insert edge batch")?;
                let total = counter.fetch_add(n, Ordering::Relaxed) + n;
                if total.is_multiple_of(100_000) {
                    info!(count = total, "Edges loaded");
                }
                Ok::<(), anyhow::Error>(())
            }));

            batch_count = 0;
        }
    }

    // Flush remaining batch
    if batch_count > 0 {
        if in_flight.len() >= MAX_CONCURRENT_BATCHES
            && let Some(result) = in_flight.next().await
        {
            match result {
                Ok(inner) => inner?,
                Err(e) => anyhow::bail!("Task join error: {e}"),
            }
        }
        let query = batch;
        let db = db.clone();
        let counter = Arc::clone(&counter);
        let n = batch_count as u64;
        in_flight.push(tokio::spawn(async move {
            db.query(&query)
                .await
                .context("Failed to insert final edge batch")?;
            counter.fetch_add(n, Ordering::Relaxed);
            Ok::<(), anyhow::Error>(())
        }));
    }

    // Wait for all in-flight batches
    while let Some(result) = in_flight.next().await {
        match result {
            Ok(inner) => inner?,
            Err(e) => anyhow::bail!("Task join error: {e}"),
        }
    }

    let total = counter.load(Ordering::Relaxed);
    info!(total, "Edges loaded");
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_test_csvs(dir: &Path) {
        // Write all required CSV files
        std::fs::write(
            dir.join("nodes.csv"),
            "id:ID,title,:LABEL\n1,Test Article,Page\n2,Another Article,Page\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("edges.csv"),
            ":START_ID,:END_ID,:TYPE\n1,2,LINKS_TO\n",
        )
        .unwrap();
        // Write remaining required CSVs (empty with headers)
        for name in &[
            "categories",
            "article_categories",
            "image_nodes",
            "article_images",
            "external_link_nodes",
            "article_external_links",
        ] {
            std::fs::write(dir.join(format!("{name}.csv")), "header\n").unwrap();
        }
    }

    #[tokio::test]
    async fn test_surreal_load_basic() {
        let dir = TempDir::new().unwrap();
        write_test_csvs(dir.path());

        let config = SurrealWriterConfig {
            output_dir: dir.path().to_str().unwrap().to_string(),
            db_path: "test.db".to_string(),
            batch_size: 100,
            clean: true,
        };

        let stats = run_surreal_load(config).await.unwrap();
        assert_eq!(stats.articles_loaded, 2);
        assert_eq!(stats.edges_loaded, 1);
    }

    #[tokio::test]
    async fn test_surreal_load_clean() {
        let dir = TempDir::new().unwrap();
        write_test_csvs(dir.path());
        let db_path = dir.path().join("test.db");

        // Create a dummy directory to be cleaned
        std::fs::create_dir_all(&db_path).unwrap();
        std::fs::write(db_path.join("dummy"), "data").unwrap();

        let config = SurrealWriterConfig {
            output_dir: dir.path().to_str().unwrap().to_string(),
            db_path: "test.db".to_string(),
            batch_size: 100,
            clean: true,
        };

        let stats = run_surreal_load(config).await.unwrap();
        assert_eq!(stats.articles_loaded, 2);
    }

    #[test]
    fn test_escape_surql() {
        assert_eq!(escape_surql("simple"), "simple");
        assert_eq!(escape_surql("it's"), "it\\'s");
        assert_eq!(escape_surql("TBWA\\Chiat\\Day"), "TBWA\\\\Chiat\\\\Day");
        assert_eq!(escape_surql("a'b\\c"), "a\\'b\\\\c");
    }
}
