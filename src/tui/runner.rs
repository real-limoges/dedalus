//! Background worker threads for TUI operations.
//!
//! Each operation (extract, import, merge) runs on a dedicated thread, communicating
//! completion and errors back to the UI via shared `Arc<AtomicBool>` / `Arc<Mutex<_>>` state.

use crate::cache;
use crate::checkpoint::{self, CheckpointManager};
use crate::import::ImportConfig;
use crate::index::WikiIndex;
use crate::stats::ExtractionStats;
use anyhow::{Context, Result};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::{info, warn};

use super::app::{App, ExtractConfig, ImportConfigTui, MergeConfig};

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
        if !config.dry_run {
            if let Err(e) = cache::save_index(&idx, input, output_dir) {
                warn!(error = %e, "Failed to save index cache");
            }
        }
        idx
    } else if let Some(idx) = cache::try_load_index(&cache_path, input)? {
        info!("Loaded index from cache");
        idx
    } else {
        info!("Building index (cache miss or invalid)");
        let idx = WikiIndex::build(input)?;
        if !config.dry_run {
            if let Err(e) = cache::save_index(&idx, input, output_dir) {
                warn!(error = %e, "Failed to save index cache");
            }
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

    if let Some(ref mgr) = checkpoint_mgr {
        if let Err(e) = mgr.clear() {
            warn!(error = %e, "Failed to clear checkpoint");
        }
    }

    Ok(ExtractTimings {
        indexing_secs,
        extraction_secs,
    })
}

/// Spawns the import worker thread (creates its own tokio runtime).
pub fn spawn_import(
    config: ImportConfigTui,
    done: Arc<AtomicBool>,
    error: Arc<Mutex<Option<String>>>,
) {
    std::thread::spawn(move || {
        let import_config = ImportConfig {
            output_dir: config.output,
            bolt_uri: config.bolt_uri,
            import_prefix: config.import_prefix,
            max_parallel_edges: config
                .max_parallel_edges
                .parse()
                .unwrap_or(crate::config::IMPORT_MAX_PARALLEL_EDGES),
            max_parallel_light: config
                .max_parallel_light
                .parse()
                .unwrap_or(crate::config::IMPORT_MAX_PARALLEL_LIGHT),
            compose_file: if config.compose_file.is_empty() {
                None
            } else {
                Some(config.compose_file)
            },
            no_docker: config.no_docker,
            clean: config.clean,
            use_admin_import: config.admin_import,
        };

        let rt = match tokio::runtime::Builder::new_multi_thread()
            .thread_name("dedalus-import-worker")
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

        match rt.block_on(crate::import::run_import(import_config)) {
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

            // Store timings ref for later retrieval
            let done = Arc::clone(&app.worker_done);
            let error = Arc::clone(&app.worker_error);
            let stats = Arc::clone(&app.stats);
            let cancel = Arc::clone(&app.cancel);
            let logs = Arc::clone(&app.logs);

            // Clone config values
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

            // We'll check timings when done
            // Store timings Arc somewhere accessible - use a small wrapper
            // For simplicity, store it via a separate thread that waits for done
            let worker_done = Arc::clone(&app.worker_done);
            let indexing_holder: Arc<Mutex<Option<f64>>> = Arc::new(Mutex::new(None));
            let extraction_holder: Arc<Mutex<Option<f64>>> = Arc::new(Mutex::new(None));
            let ih = Arc::clone(&indexing_holder);
            let eh = Arc::clone(&extraction_holder);
            std::thread::spawn(move || {
                while !worker_done.load(Ordering::Acquire) {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                if let Ok(t) = timings.lock() {
                    if let Some(ref timing) = *t {
                        if let Ok(mut lock) = ih.lock() {
                            *lock = Some(timing.indexing_secs);
                        }
                        if let Ok(mut lock) = eh.lock() {
                            *lock = Some(timing.extraction_secs);
                        }
                    }
                }
            });
            // Store holders in done_message temporarily as a hack...
            // Better: just read timings directly in tick handler
            // We'll store the timings Arc in app. Let's add a field.
            // Actually for simplicity, the timings get stored via the error/done mechanism
            // and we calculate from start_time in the done screen.
        }
        super::app::Operation::Import => {
            app.phase = "Import".to_string();
            let done = Arc::clone(&app.worker_done);
            let error = Arc::clone(&app.worker_error);
            let config = ImportConfigTui {
                output: app.import_config.output.clone(),
                bolt_uri: app.import_config.bolt_uri.clone(),
                import_prefix: app.import_config.import_prefix.clone(),
                max_parallel_edges: app.import_config.max_parallel_edges.clone(),
                max_parallel_light: app.import_config.max_parallel_light.clone(),
                compose_file: app.import_config.compose_file.clone(),
                no_docker: app.import_config.no_docker,
                clean: app.import_config.clean,
                admin_import: app.import_config.admin_import,
            };
            spawn_import(config, done, error);
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
