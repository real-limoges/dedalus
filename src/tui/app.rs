//! TUI application state and configuration types.
//!
//! Defines the data model for all three TUI screens (config, progress, done),
//! per-operation form configurations, field enums for navigation, and validation logic.

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::stats::ExtractionStats;

/// Which screen the TUI is currently displaying.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Config,
    Progress,
    Done,
}

/// Which Dedalus operation the user has selected in the config screen.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Extract,
    Load,
    Analytics,
    MergeCsvs,
}

impl Operation {
    /// Returns the display label for this operation (used in tab headers).
    pub fn label(&self) -> &str {
        match self {
            Operation::Extract => "Extract",
            Operation::Load => "Load",
            Operation::Analytics => "Analytics",
            Operation::MergeCsvs => "MergeCsvs",
        }
    }

    /// Returns all available operations in tab order.
    pub fn all() -> &'static [Operation] {
        &[
            Operation::Extract,
            Operation::Load,
            Operation::Analytics,
            Operation::MergeCsvs,
        ]
    }
}

/// Navigable fields in the Extract configuration form.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ExtractField {
    Input,
    Output,
    CsvShards,
    BlobShards,
    Limit,
    Checkpoint,
    DryRun,
    Resume,
    NoCache,
    Clean,
}

/// Navigable fields in the Load configuration form.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LoadField {
    Output,
    DbPath,
    BatchSize,
    Clean,
}

/// Navigable fields in the Analytics configuration form.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum AnalyticsField {
    Output,
    DbPath,
    PageRankIterations,
    Damping,
}

/// Navigable fields in the MergeCsvs configuration form.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum MergeField {
    Output,
}

/// Ordered list of Extract form fields for UI navigation.
pub static EXTRACT_FIELDS: &[ExtractField] = &[
    ExtractField::Input,
    ExtractField::Output,
    ExtractField::CsvShards,
    ExtractField::BlobShards,
    ExtractField::Limit,
    ExtractField::Checkpoint,
    ExtractField::DryRun,
    ExtractField::Resume,
    ExtractField::NoCache,
    ExtractField::Clean,
];

/// Ordered list of Load form fields for UI navigation.
pub static LOAD_FIELDS: &[LoadField] = &[
    LoadField::Output,
    LoadField::DbPath,
    LoadField::BatchSize,
    LoadField::Clean,
];

/// Ordered list of Analytics form fields for UI navigation.
pub static ANALYTICS_FIELDS: &[AnalyticsField] = &[
    AnalyticsField::Output,
    AnalyticsField::DbPath,
    AnalyticsField::PageRankIterations,
    AnalyticsField::Damping,
];

/// Ordered list of MergeCsvs form fields for UI navigation.
pub static MERGE_FIELDS: &[MergeField] = &[MergeField::Output];

/// Form state for the Extract operation's configuration fields.
pub struct ExtractConfig {
    pub input: String,
    pub output: String,
    pub csv_shards: String,
    pub blob_shards: String,
    pub limit: String,
    pub checkpoint: String,
    pub dry_run: bool,
    pub resume: bool,
    pub no_cache: bool,
    pub clean: bool,
}

impl Default for ExtractConfig {
    fn default() -> Self {
        Self {
            input: String::new(),
            output: "output".to_string(),
            csv_shards: "8".to_string(),
            blob_shards: "1000".to_string(),
            limit: String::new(),
            checkpoint: "10000".to_string(),
            dry_run: false,
            resume: false,
            no_cache: false,
            clean: false,
        }
    }
}

/// Form state for the Load operation's configuration fields.
pub struct LoadConfigTui {
    pub output: String,
    pub db_path: String,
    pub batch_size: String,
    pub clean: bool,
}

impl Default for LoadConfigTui {
    fn default() -> Self {
        Self {
            output: "output".to_string(),
            db_path: crate::config::DEFAULT_DB_PATH.to_string(),
            batch_size: crate::config::SURREAL_BATCH_SIZE.to_string(),
            clean: false,
        }
    }
}

/// Form state for the Analytics operation's configuration fields.
pub struct AnalyticsConfigTui {
    pub output: String,
    pub db_path: String,
    pub pagerank_iterations: String,
    pub damping: String,
}

impl Default for AnalyticsConfigTui {
    fn default() -> Self {
        Self {
            output: "output".to_string(),
            db_path: crate::config::DEFAULT_DB_PATH.to_string(),
            pagerank_iterations: crate::config::PAGERANK_ITERATIONS.to_string(),
            damping: crate::config::PAGERANK_DAMPING.to_string(),
        }
    }
}

/// Form state for the MergeCsvs operation's configuration fields.
pub struct MergeConfig {
    pub output: String,
}

impl Default for MergeConfig {
    fn default() -> Self {
        Self {
            output: "output".to_string(),
        }
    }
}

/// Root application state shared across all TUI screens and operations.
pub struct App {
    pub screen: Screen,
    pub operation: Operation,
    pub field_index: usize,
    pub extract_config: ExtractConfig,
    pub load_config: LoadConfigTui,
    pub analytics_config: AnalyticsConfigTui,
    pub merge_config: MergeConfig,
    pub status_message: String,
    pub error_message: Option<String>,

    // Progress state
    pub stats: Arc<ExtractionStats>,
    pub cancel: Arc<AtomicBool>,
    pub start_time: Option<Instant>,
    pub worker_done: Arc<AtomicBool>,
    pub worker_error: Arc<Mutex<Option<String>>>,
    pub logs: Arc<Mutex<VecDeque<String>>>,
    pub log_scroll: usize,
    pub phase: String,

    // Done state
    pub done_message: String,
    pub indexing_secs: f64,
    pub extraction_secs: f64,
}

impl App {
    /// Creates a new `App` with default configuration and the given shared log buffer.
    pub fn new(logs: Arc<Mutex<VecDeque<String>>>) -> Self {
        Self {
            screen: Screen::Config,
            operation: Operation::Extract,
            field_index: 0,
            extract_config: ExtractConfig::default(),
            load_config: LoadConfigTui::default(),
            analytics_config: AnalyticsConfigTui::default(),
            merge_config: MergeConfig::default(),
            status_message: "Ready".to_string(),
            error_message: None,

            stats: Arc::new(ExtractionStats::new()),
            cancel: Arc::new(AtomicBool::new(false)),
            start_time: None,
            worker_done: Arc::new(AtomicBool::new(false)),
            worker_error: Arc::new(Mutex::new(None)),
            logs,
            log_scroll: 0,
            phase: String::new(),

            done_message: String::new(),
            indexing_secs: 0.0,
            extraction_secs: 0.0,
        }
    }

    /// Returns the number of navigable fields for the current operation.
    pub fn field_count(&self) -> usize {
        match self.operation {
            Operation::Extract => EXTRACT_FIELDS.len(),
            Operation::Load => LOAD_FIELDS.len(),
            Operation::Analytics => ANALYTICS_FIELDS.len(),
            Operation::MergeCsvs => MERGE_FIELDS.len(),
        }
    }

    /// Returns `true` if the currently selected field is a boolean checkbox.
    pub fn current_field_is_checkbox(&self) -> bool {
        match self.operation {
            Operation::Extract => matches!(
                EXTRACT_FIELDS[self.field_index],
                ExtractField::DryRun
                    | ExtractField::Resume
                    | ExtractField::NoCache
                    | ExtractField::Clean
            ),
            Operation::Load => matches!(LOAD_FIELDS[self.field_index], LoadField::Clean),
            Operation::Analytics => false,
            Operation::MergeCsvs => false,
        }
    }

    /// Toggles the boolean value of the currently selected checkbox field.
    pub fn toggle_checkbox(&mut self) {
        match self.operation {
            Operation::Extract => match EXTRACT_FIELDS[self.field_index] {
                ExtractField::DryRun => self.extract_config.dry_run = !self.extract_config.dry_run,
                ExtractField::Resume => self.extract_config.resume = !self.extract_config.resume,
                ExtractField::NoCache => {
                    self.extract_config.no_cache = !self.extract_config.no_cache
                }
                ExtractField::Clean => self.extract_config.clean = !self.extract_config.clean,
                _ => {}
            },
            Operation::Load => {
                if LOAD_FIELDS[self.field_index] == LoadField::Clean {
                    self.load_config.clean = !self.load_config.clean;
                }
            }
            Operation::Analytics => {}
            Operation::MergeCsvs => {}
        }
    }

    /// Returns a mutable reference to the currently selected text field, or `None` for checkboxes.
    pub fn current_text_field(&mut self) -> Option<&mut String> {
        match self.operation {
            Operation::Extract => match EXTRACT_FIELDS[self.field_index] {
                ExtractField::Input => Some(&mut self.extract_config.input),
                ExtractField::Output => Some(&mut self.extract_config.output),
                ExtractField::CsvShards => Some(&mut self.extract_config.csv_shards),
                ExtractField::BlobShards => Some(&mut self.extract_config.blob_shards),
                ExtractField::Limit => Some(&mut self.extract_config.limit),
                ExtractField::Checkpoint => Some(&mut self.extract_config.checkpoint),
                _ => None,
            },
            Operation::Load => match LOAD_FIELDS[self.field_index] {
                LoadField::Output => Some(&mut self.load_config.output),
                LoadField::DbPath => Some(&mut self.load_config.db_path),
                LoadField::BatchSize => Some(&mut self.load_config.batch_size),
                LoadField::Clean => None,
            },
            Operation::Analytics => match ANALYTICS_FIELDS[self.field_index] {
                AnalyticsField::Output => Some(&mut self.analytics_config.output),
                AnalyticsField::DbPath => Some(&mut self.analytics_config.db_path),
                AnalyticsField::PageRankIterations => {
                    Some(&mut self.analytics_config.pagerank_iterations)
                }
                AnalyticsField::Damping => Some(&mut self.analytics_config.damping),
            },
            Operation::MergeCsvs => match MERGE_FIELDS[self.field_index] {
                MergeField::Output => Some(&mut self.merge_config.output),
            },
        }
    }

    /// Validates all fields for the current operation before starting it.
    pub fn validate(&self) -> Result<(), String> {
        match self.operation {
            Operation::Extract => {
                if self.extract_config.input.is_empty() {
                    return Err("Input file is required".to_string());
                }
                if self.extract_config.output.is_empty() {
                    return Err("Output directory is required".to_string());
                }
                if self.extract_config.csv_shards.parse::<u32>().is_err() {
                    return Err("CSV shards must be a number".to_string());
                }
                if self.extract_config.blob_shards.parse::<u32>().is_err() {
                    return Err("Blob shards must be a number".to_string());
                }
                if !self.extract_config.limit.is_empty()
                    && self.extract_config.limit.parse::<u64>().is_err()
                {
                    return Err("Limit must be a number".to_string());
                }
                if self.extract_config.checkpoint.parse::<u32>().is_err() {
                    return Err("Checkpoint interval must be a number".to_string());
                }
                Ok(())
            }
            Operation::Load => {
                if self.load_config.output.is_empty() {
                    return Err("Output directory is required".to_string());
                }
                if self.load_config.batch_size.parse::<usize>().is_err() {
                    return Err("Batch size must be a number".to_string());
                }
                Ok(())
            }
            Operation::Analytics => {
                if self.analytics_config.output.is_empty() {
                    return Err("Output directory is required".to_string());
                }
                if self
                    .analytics_config
                    .pagerank_iterations
                    .parse::<u32>()
                    .is_err()
                {
                    return Err("PageRank iterations must be a number".to_string());
                }
                if self.analytics_config.damping.parse::<f64>().is_err() {
                    return Err("Damping factor must be a number".to_string());
                }
                Ok(())
            }
            Operation::MergeCsvs => {
                if self.merge_config.output.is_empty() {
                    return Err("Output directory is required".to_string());
                }
                Ok(())
            }
        }
    }
}
