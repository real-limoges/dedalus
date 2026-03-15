//! Background worker threads for TUI operations.
//!
//! Each operation (extract, load, analytics, merge) runs on a dedicated thread,
//! communicating completion and errors back to the UI via shared `Arc<AtomicBool>`
//! / `Arc<Mutex<_>>` state.

use crate::cache;
use crate::checkpoint::{self, CheckpointManager};
use crate::index::WikiIndex;
use crate::stats::ExtractionStats;
use anyhow::{Context, Result};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::{info, warn};

use super::app::{AnalyticsConfigTui, App, ExtractConfig, LoadConfigTui, MergeConfig};

/// Timing results from an extraction run, split into indexing and extraction phases.
pub struct ExtractTimings {
    pub indexing_secs: f64,
    pub extraction_secs: f64,
}

/// Spawns the extraction worker thread with shared stats, cancellation, and completion signals.
pub fn spawn_extract(
    config: ExtractConfig,
    stats: Arc<ExtractionStats>,
    cancel: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    error: Arc<Mutex<Option<String>>>,
    _logs: Arc<Mutex<VecDeque<String>>>,
    timings: Arc<Mutex<Option<ExtractTimings>>>,
) {
    std::thread::spawn(move || {
        let result = run_extract_inner(&config, &stats, &cancel);
        match result {
            Ok(t) => {
                if let Ok(mut lock) = timings.lock() {
                    *lock = Some(t);
                }
            }
            Err(e) => {
                if let Ok(mut lock) = error.lock() {
                    *lock = Some(format!("{:#}", e));
                }
            }
        }
        done.store(true, Ordering::Release);
    });
}

fn run_extract_inner(
    config: &ExtractConfig,
    stats: &Arc<ExtractionStats>,
    cancel: &Arc<AtomicBool>,
) -> Result<ExtractTimings> {
    let output_dir = &config.output;
    let input = &config.input;
    let csv_shards: u32 = config.csv_shards.parse().context("Invalid csv_shards")?;
    let shard_count: u32 = config.blob_shards.parse().context("Invalid blob_shards")?;
    let limit: Option<u64> = if config.limit.is_empty() {
        None
    } else {
        Some(config.limit.parse().context("Invalid limit")?)
    };
    let checkpoint_interval: u32 = config.checkpoint.parse().context("Invalid checkpoint")?;

    if config.clean {
        let output_path = std::path::Path::new(output_dir);
        if output_path.exists() {
            info!("Cleaning output directory: {}", output_dir);
            std::fs::remove_dir_all(output_path)
                .with_context(|| format!("Failed to clean output directory: {}", output_dir))?;
        }
    }

    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create output directory: {}", output_dir))?;

    // Indexing
    let start_indexing = Instant::now();
    let cache_path = cache::cache_path(output_dir);

    let index = if config.no_cache {
        info!("Cache disabled, building fresh index");
        let idx = WikiIndex::build(input)?;
        if !config.dry_run
            && let Err(e) = cache::save_index(&idx, input, output_dir) {
                warn!(error = %e, "Failed to save index cache");
            }
        idx
    } else if let Some(idx) = cache::try_load_index(&cache_path, input)? {
        info!("Loaded index from cache");
        idx
    } else {
        info!("Building index (cache miss or invalid)");
        let idx = WikiIndex::build(input)?;
        if !config.dry_run
            && let Err(e) = cache::save_index(&idx, input, output_dir) {
                warn!(error = %e, "Failed to save index cache");
            }
        idx
    };

    let indexing_secs = start_indexing.elapsed().as_secs_f64();
    info!(duration_secs = indexing_secs, "Indexing complete");

    if cancel.load(Ordering::Acquire) {
        return Ok(ExtractTimings {
            indexing_secs,
            extraction_secs: 0.0,
        });
    }

    let checkpoint_mgr = if !config.dry_run {
        Some(CheckpointManager::new(
            input,
            output_dir,
            shard_count,
            csv_shards,
            checkpoint_interval,
        )?)
    } else {
        None
    };

    let checkpoint = if config.resume && !config.clean {
        match checkpoint::load_if_valid(input, output_dir, shard_count, csv_shards)? {
            Some(cp) => {
                info!(
                    last_id = cp.last_processed_id,
                    articles = cp.stats.articles_processed,
                    "Resuming from checkpoint"
                );
                if let Some(ref mgr) = checkpoint_mgr {
                    mgr.set_last_id(cp.last_processed_id);
                }
                Some(cp)
            }
            None => {
                info!("No valid checkpoint found, starting fresh");
                None
            }
        }
    } else {
        None
    };

    info!("Starting extraction pass");
    let start_extracting = Instant::now();
    let extraction_config = crate::extract::ExtractionConfig {
        input_path: input,
        output_dir,
        index: &index,
        shard_count,
        csv_shards,
        limit,
        dry_run: config.dry_run,
        resume_from: checkpoint.as_ref(),
        checkpoint_mgr: checkpoint_mgr.as_ref(),
        multistream_ranges: None,
    };
    crate::extract::run_extraction_with_stats(
        &extraction_config,
        Arc::clone(stats),
        Arc::clone(cancel),
        true, // hide indicatif
    )?;
    let extraction_secs = start_extracting.elapsed().as_secs_f64();
    info!(duration_secs = extraction_secs, "Extraction complete");

    if let Some(ref mgr) = checkpoint_mgr
        && let Err(e) = mgr.clear() {
            warn!(error = %e, "Failed to clear checkpoint");
        }

    Ok(ExtractTimings {
        indexing_secs,
        extraction_secs,
    })
}

/// Spawns the SurrealDB load worker thread (creates its own tokio runtime).
pub fn spawn_surreal_load(
    config: LoadConfigTui,
    done: Arc<AtomicBool>,
    error: Arc<Mutex<Option<String>>>,
) {
    std::thread::spawn(move || {
        let load_config = crate::surrealdb_writer::SurrealWriterConfig {
            output_dir: config.output,
            db_path: config.db_path,
            batch_size: config
                .batch_size
                .parse()
                .unwrap_or(crate::config::SURREAL_BATCH_SIZE),
            clean: config.clean,
        };

        let rt = match tokio::runtime::Builder::new_multi_thread()
            .thread_name("dedalus-load-worker")
            .enable_io()
            .enable_time()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                if let Ok(mut lock) = error.lock() {
                    *lock = Some(format!("Failed to create tokio runtime: {}", e));
                }
                done.store(true, Ordering::Release);
                return;
            }
        };

        match rt.block_on(crate::surrealdb_writer::run_surreal_load(load_config)) {
            Ok(_stats) => {}
            Err(e) => {
                if let Ok(mut lock) = error.lock() {
                    *lock = Some(format!("{:#}", e));
                }
            }
        }
        done.store(true, Ordering::Release);
    });
}

/// Spawns the analytics worker thread (creates its own tokio runtime).
pub fn spawn_analytics(
    config: AnalyticsConfigTui,
    done: Arc<AtomicBool>,
    error: Arc<Mutex<Option<String>>>,
) {
    std::thread::spawn(move || {
        let analytics_config = crate::analytics::AnalyticsConfig {
            db_path: config.db_path,
            output_dir: config.output,
            pagerank_iterations: config
                .pagerank_iterations
                .parse()
                .unwrap_or(crate::config::PAGERANK_ITERATIONS),
            pagerank_damping: config
                .damping
                .parse()
                .unwrap_or(crate::config::PAGERANK_DAMPING),
            ..Default::default()
        };

        let rt = match tokio::runtime::Builder::new_multi_thread()
            .thread_name("dedalus-analytics-worker")
            .enable_io()
            .enable_time()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                if let Ok(mut lock) = error.lock() {
                    *lock = Some(format!("Failed to create tokio runtime: {}", e));
                }
                done.store(true, Ordering::Release);
                return;
            }
        };

        match rt.block_on(crate::analytics::run_analytics(analytics_config)) {
            Ok(_stats) => {}
            Err(e) => {
                if let Ok(mut lock) = error.lock() {
                    *lock = Some(format!("{:#}", e));
                }
            }
        }
        done.store(true, Ordering::Release);
    });
}

/// Spawns the CSV merge worker thread.
pub fn spawn_merge(config: MergeConfig, done: Arc<AtomicBool>, error: Arc<Mutex<Option<String>>>) {
    std::thread::spawn(move || {
        match crate::merge::merge_csv_shards(&config.output) {
            Ok(()) => {}
            Err(e) => {
                if let Ok(mut lock) = error.lock() {
                    *lock = Some(format!("{:#}", e));
                }
            }
        }
        done.store(true, Ordering::Release);
    });
}

/// Resets progress state and spawns the appropriate worker thread for the current operation.
pub fn start_operation(app: &mut App) {
    // Reset state
    app.cancel.store(false, Ordering::Release);
    app.worker_done.store(false, Ordering::Release);
    if let Ok(mut e) = app.worker_error.lock() {
        *e = None;
    }
    app.stats = Arc::new(ExtractionStats::new());
    app.start_time = Some(Instant::now());
    app.log_scroll = 0;
    app.done_message.clear();
    app.indexing_secs = 0.0;
    app.extraction_secs = 0.0;

    match app.operation {
        super::app::Operation::Extract => {
            app.phase = "Extraction".to_string();
            let timings = Arc::new(Mutex::new(None));
            let timings_clone = Arc::clone(&timings);

            let done = Arc::clone(&app.worker_done);
            let error = Arc::clone(&app.worker_error);
            let stats = Arc::clone(&app.stats);
            let cancel = Arc::clone(&app.cancel);
            let logs = Arc::clone(&app.logs);

            let config = ExtractConfig {
                input: app.extract_config.input.clone(),
                output: app.extract_config.output.clone(),
                csv_shards: app.extract_config.csv_shards.clone(),
                blob_shards: app.extract_config.blob_shards.clone(),
                limit: app.extract_config.limit.clone(),
                checkpoint: app.extract_config.checkpoint.clone(),
                dry_run: app.extract_config.dry_run,
                resume: app.extract_config.resume,
                no_cache: app.extract_config.no_cache,
                clean: app.extract_config.clean,
            };

            spawn_extract(config, stats, cancel, done, error, logs, timings_clone);

            let worker_done = Arc::clone(&app.worker_done);
            let ih: Arc<Mutex<Option<f64>>> = Arc::new(Mutex::new(None));
            let eh: Arc<Mutex<Option<f64>>> = Arc::new(Mutex::new(None));
            let ih_clone = Arc::clone(&ih);
            let eh_clone = Arc::clone(&eh);
            std::thread::spawn(move || {
                while !worker_done.load(Ordering::Acquire) {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                if let Ok(t) = timings.lock()
                    && let Some(ref timing) = *t {
                        if let Ok(mut lock) = ih_clone.lock() {
                            *lock = Some(timing.indexing_secs);
                        }
                        if let Ok(mut lock) = eh_clone.lock() {
                            *lock = Some(timing.extraction_secs);
                        }
                    }
            });
        }
        super::app::Operation::Load => {
            app.phase = "Load".to_string();
            let done = Arc::clone(&app.worker_done);
            let error = Arc::clone(&app.worker_error);
            let config = LoadConfigTui {
                output: app.load_config.output.clone(),
                db_path: app.load_config.db_path.clone(),
                batch_size: app.load_config.batch_size.clone(),
                clean: app.load_config.clean,
            };
            spawn_surreal_load(config, done, error);
        }
        super::app::Operation::Analytics => {
            app.phase = "Analytics".to_string();
            let done = Arc::clone(&app.worker_done);
            let error = Arc::clone(&app.worker_error);
            let config = AnalyticsConfigTui {
                output: app.analytics_config.output.clone(),
                db_path: app.analytics_config.db_path.clone(),
                pagerank_iterations: app.analytics_config.pagerank_iterations.clone(),
                damping: app.analytics_config.damping.clone(),
            };
            spawn_analytics(config, done, error);
        }
        super::app::Operation::MergeCsvs => {
            app.phase = "Merge".to_string();
            let done = Arc::clone(&app.worker_done);
            let error = Arc::clone(&app.worker_error);
            let config = MergeConfig {
                output: app.merge_config.output.clone(),
            };
            spawn_merge(config, done, error);
        }
    }
}
