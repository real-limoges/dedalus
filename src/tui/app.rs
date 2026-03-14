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
    Import,
    MergeCsvs,
}

impl Operation {
    /// Returns the display label for this operation (used in tab headers).
    pub fn label(&self) -> &str {
        match self {
            Operation::Extract => "Extract",
            Operation::Import => "Import",
            Operation::MergeCsvs => "MergeCsvs",
        }
    }

    /// Returns all available operations in tab order.
    pub fn all() -> &'static [Operation] {
        &[Operation::Extract, Operation::Import, Operation::MergeCsvs]
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

/// Navigable fields in the Import configuration form.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ImportField {
    Output,
    BoltUri,
    ImportPrefix,
    MaxParallelEdges,
    MaxParallelLight,
    ComposeFile,
    NoDocker,
    CleanImport,
    AdminImport,
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

/// Ordered list of Import form fields for UI navigation.
pub static IMPORT_FIELDS: &[ImportField] = &[
    ImportField::Output,
    ImportField::BoltUri,
    ImportField::ImportPrefix,
    ImportField::MaxParallelEdges,
    ImportField::MaxParallelLight,
    ImportField::ComposeFile,
    ImportField::NoDocker,
    ImportField::CleanImport,
    ImportField::AdminImport,
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

/// Form state for the Import operation's configuration fields.
pub struct ImportConfigTui {
    pub output: String,
    pub bolt_uri: String,
    pub import_prefix: String,
    pub max_parallel_edges: String,
    pub max_parallel_light: String,
    pub compose_file: String,
    pub no_docker: bool,
    pub clean: bool,
    pub admin_import: bool,
}

impl Default for ImportConfigTui {
    fn default() -> Self {
        Self {
            output: "output".to_string(),
            bolt_uri: crate::config::DEFAULT_BOLT_URI.to_string(),
            import_prefix: crate::config::DEFAULT_IMPORT_PREFIX.to_string(),
            max_parallel_edges: crate::config::IMPORT_MAX_PARALLEL_EDGES.to_string(),
            max_parallel_light: crate::config::IMPORT_MAX_PARALLEL_LIGHT.to_string(),
            compose_file: String::new(),
            no_docker: false,
            clean: false,
            admin_import: true,
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
    pub import_config: ImportConfigTui,
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
            import_config: ImportConfigTui::default(),
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
            Operation::Import => IMPORT_FIELDS.len(),
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
            Operation::Import => matches!(
                IMPORT_FIELDS[self.field_index],
                ImportField::NoDocker | ImportField::CleanImport | ImportField::AdminImport
            ),
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
            Operation::Import => match IMPORT_FIELDS[self.field_index] {
                ImportField::NoDocker => {
                    self.import_config.no_docker = !self.import_config.no_docker
                }
                ImportField::CleanImport => self.import_config.clean = !self.import_config.clean,
                ImportField::AdminImport => {
                    self.import_config.admin_import = !self.import_config.admin_import
                }
                _ => {}
            },
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
            Operation::Import => match IMPORT_FIELDS[self.field_index] {
                ImportField::Output => Some(&mut self.import_config.output),
                ImportField::BoltUri => Some(&mut self.import_config.bolt_uri),
                ImportField::ImportPrefix => Some(&mut self.import_config.import_prefix),
                ImportField::MaxParallelEdges => Some(&mut self.import_config.max_parallel_edges),
                ImportField::MaxParallelLight => Some(&mut self.import_config.max_parallel_light),
                ImportField::ComposeFile => Some(&mut self.import_config.compose_file),
                _ => None,
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
            Operation::Import => {
                if self.import_config.output.is_empty() {
                    return Err("Output directory is required".to_string());
                }
                if self
                    .import_config
                    .max_parallel_edges
                    .parse::<usize>()
                    .is_err()
                {
                    return Err("Max parallel edges must be a number".to_string());
                }
                if self
                    .import_config
                    .max_parallel_light
                    .parse::<usize>()
                    .is_err()
                {
                    return Err("Max parallel light must be a number".to_string());
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
