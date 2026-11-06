//! Background worker threads for TUI operations.
//!
//! Each operation (extract, merge, analytics, load) runs on its own thread
//! through `spawn_worker`, which always marks the worker done, even if the
//! operation panics, and records any error for the Done screen.

use crate::extract::{ExtractJob, ExtractReport, run_extract_job};
use crate::stats::ExtractionStats;
use anyhow::{Context, Result};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::app::{App, ExtractConfig, Operation};

/// Runs `work` on a new thread. When it ends, by returning or by panicking,
/// any error is stored in `error` and `done` is set, so the UI never waits on
/// a worker that died.
fn spawn_worker(
    done: Arc<AtomicBool>,
    error: Arc<Mutex<Option<String>>>,
    work: impl FnOnce() -> Result<()> + Send + 'static,
) {
    std::thread::spawn(move || {
        let message = match catch_unwind(AssertUnwindSafe(work)) {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(format!("{e:#}")),
            Err(_) => Some("The worker thread panicked; see the log for details".to_string()),
        };
        if let Ok(mut slot) = error.lock() {
            *slot = message;
        }
        done.store(true, Ordering::Release);
    });
}

/// Turns the extract form into the job the CLI would run.
fn extract_job(config: &ExtractConfig) -> Result<ExtractJob> {
    let limit = if config.limit.is_empty() {
        None
    } else {
        Some(config.limit.parse().context("Invalid limit")?)
    };
    Ok(ExtractJob {
        input: config.input.clone(),
        output: config.output.clone(),
        shard_count: config.blob_shards.parse().context("Invalid blob shards")?,
        csv_shards: config.csv_shards.parse().context("Invalid CSV shards")?,
        limit,
        dry_run: config.dry_run,
        resume: config.resume,
        no_cache: config.no_cache,
        clean: config.clean,
        checkpoint_interval: config.checkpoint.parse().context("Invalid checkpoint")?,
        multistream_index: None,
    })
}

/// Resets progress state and spawns the worker for the current operation.
pub fn start_operation(app: &mut App) {
    app.cancel.store(false, Ordering::Release);
    app.worker_done.store(false, Ordering::Release);
    if let Ok(mut e) = app.worker_error.lock() {
        *e = None;
    }
    if let Ok(mut r) = app.extract_report.lock() {
        *r = None;
    }
    app.stats = Arc::new(ExtractionStats::new());
    app.start_time = Some(Instant::now());
    app.log_offset_from_bottom = 0;
    app.done_message.clear();
    app.indexing_secs = 0.0;
    app.extraction_secs = 0.0;
    app.phase = app.operation.label().to_string();

    let done = Arc::clone(&app.worker_done);
    let error = Arc::clone(&app.worker_error);

    match app.operation {
        Operation::Extract => {
            let job = extract_job(&app.extract_config);
            let stats = Arc::clone(&app.stats);
            let cancel = Arc::clone(&app.cancel);
            let report_slot = Arc::clone(&app.extract_report);
            spawn_worker(done, error, move || {
                let report = run_extract_job(&job?, stats, cancel, true)?;
                if let Ok(mut slot) = report_slot.lock() {
                    *slot = Some(report);
                }
                Ok(())
            });
        }
        Operation::Analytics => {
            let config = crate::analytics::AnalyticsConfig {
                output_dir: app.analytics_config.output.clone(),
                pagerank_iterations: app
                    .analytics_config
                    .pagerank_iterations
                    .parse()
                    .unwrap_or(crate::config::PAGERANK_ITERATIONS),
                pagerank_damping: app
                    .analytics_config
                    .damping
                    .parse()
                    .unwrap_or(crate::config::PAGERANK_DAMPING),
                ..Default::default()
            };
            spawn_worker(done, error, move || {
                crate::analytics::run_analytics(&config).map(|_stats| ())
            });
        }
        Operation::Load => {
            let config = crate::load::LoadConfig {
                output_dir: app.load_config.output.clone(),
                db_path: app.load_config.db_path.clone(),
                replace: app.load_config.clean,
            };
            spawn_worker(done, error, move || {
                crate::load::run_load(&config).map(|_stats| ())
            });
        }
        Operation::MergeCsvs => {
            let output = app.merge_config.output.clone();
            spawn_worker(done, error, move || {
                crate::merge::merge_detected_shards(&output).map(|_shard_count| ())
            });
        }
    }
}

/// Copies a finished extraction's timings into the app.
pub fn take_extract_report(app: &mut App) -> Option<ExtractReport> {
    let report = app.extract_report.lock().ok().and_then(|slot| *slot)?;
    app.indexing_secs = report.indexing_secs;
    app.extraction_secs = report.extraction_secs;
    Some(report)
}
