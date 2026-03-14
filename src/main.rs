//! CLI entry point for the Dedalus pipeline.
//!
//! Uses `clap` subcommands to orchestrate extract, import, merge-csvs, pipeline,
//! stats, and tui operations. Initializes `tracing` logging with configurable
//! verbosity and uses `mimalloc` as the global allocator.

use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use dedalus::cache;
use dedalus::checkpoint::{self, CheckpointManager};
use dedalus::import::ImportConfig;
use std::fs;
use std::path::Path;
use std::process::ExitCode;
use std::time::Instant;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(name = "dedalus")]
#[command(about = "Extract Wikipedia dumps and import into graph databases")]
struct Cli {
    /// Verbosity level (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Extract Wikipedia dumps into CSV/JSON format
    Extract(ExtractArgs),
    /// Import extracted CSV files into Neo4j
    Import(ImportArgs),
    /// Merge sharded CSV files into single files for neo4j-admin import
    MergeCsvs(MergeCsvsArgs),
    /// Run the full pipeline: extract -> merge -> import
    Pipeline(PipelineArgs),
    /// Show output directory statistics
    Stats(StatsArgs),
    /// Launch interactive TUI for configuration and monitoring
    Tui,
}

#[derive(Args)]
struct ExtractArgs {
    /// Path to the Wikipedia dump file (.xml.bz2)
    #[arg(short, long)]
    input: String,

    /// Output directory for generated files
    #[arg(short, long)]
    output: String,

    /// Number of shards for blob storage
    #[arg(long, default_value_t = 1000)]
    shard_count: u32,

    /// Number of CSV output shards for parallel import (1 = single file)
    #[arg(long, default_value_t = 8)]
    csv_shards: u32,

    /// Limit number of pages to process (for testing)
    #[arg(long)]
    limit: Option<u64>,

    /// Dry run - don't write output files
    #[arg(long)]
    dry_run: bool,

    /// Resume from last checkpoint if available
    #[arg(long)]
    resume: bool,

    /// Force rebuild of index cache
    #[arg(long)]
    no_cache: bool,

    /// Checkpoint interval in articles processed
    #[arg(long, default_value_t = dedalus::config::CHECKPOINT_INTERVAL)]
    checkpoint_interval: u32,

    /// Clear existing checkpoint and outputs before starting
    #[arg(long)]
    clean: bool,

    /// Path to multistream index file (.txt.bz2) for parallel parsing
    #[arg(long)]
    multistream_index: Option<String>,
}

#[derive(Args)]
struct ImportArgs {
    /// Directory containing Dedalus CSV output files
    #[arg(short, long)]
    output: String,

    /// Neo4j Bolt URI
    #[arg(long, default_value = dedalus::config::DEFAULT_BOLT_URI)]
    bolt_uri: String,

    /// Import file URI prefix for Neo4j LOAD CSV
    #[arg(long, default_value = dedalus::config::DEFAULT_IMPORT_PREFIX)]
    import_prefix: String,

    /// Max parallel LOAD CSV jobs for edge operations
    #[arg(long, default_value_t = dedalus::config::IMPORT_MAX_PARALLEL_EDGES)]
    max_parallel_edges: usize,

    /// Max parallel LOAD CSV jobs for lighter relationship operations
    #[arg(long, default_value_t = dedalus::config::IMPORT_MAX_PARALLEL_LIGHT)]
    max_parallel_light: usize,

    /// Docker compose file path (auto-detected if not specified)
    #[arg(long)]
    compose_file: Option<String>,

    /// Skip Docker management, just connect to an already-running Neo4j
    #[arg(long)]
    no_docker: bool,

    /// Clear existing Neo4j data before importing
    #[arg(long)]
    clean: bool,

    /// Use neo4j-admin import (10-100x faster, requires empty DB)
    #[arg(long)]
    admin_import: bool,
}

#[derive(Args)]
struct MergeCsvsArgs {
    /// Output directory containing sharded CSVs (e.g., nodes_000.csv, nodes_001.csv)
    #[arg(short, long)]
    output: String,

    /// Archive sharded CSVs to output/shards/ after merging
    #[arg(long)]
    archive: bool,
}

#[derive(Args)]
struct PipelineArgs {
    /// Path to the Wikipedia dump file (.xml.bz2)
    #[arg(short, long)]
    input: String,

    /// Output directory for generated files
    #[arg(short, long)]
    output: String,

    /// Number of shards for blob storage
    #[arg(long, default_value_t = 1000)]
    shard_count: u32,

    /// Number of CSV output shards for parallel extraction (1 = single file)
    #[arg(long, default_value_t = 8)]
    csv_shards: u32,

    /// Limit number of pages to process (for testing)
    #[arg(long)]
    limit: Option<u64>,

    /// Resume from last checkpoint if available
    #[arg(long)]
    resume: bool,

    /// Force rebuild of index cache
    #[arg(long)]
    no_cache: bool,

    /// Checkpoint interval in articles processed
    #[arg(long, default_value_t = dedalus::config::CHECKPOINT_INTERVAL)]
    checkpoint_interval: u32,

    /// Clear existing outputs and Neo4j data before starting
    #[arg(long)]
    clean: bool,

    /// Neo4j Bolt URI
    #[arg(long, default_value = dedalus::config::DEFAULT_BOLT_URI)]
    bolt_uri: String,

    /// Import file URI prefix for Neo4j LOAD CSV
    #[arg(long, default_value = dedalus::config::DEFAULT_IMPORT_PREFIX)]
    import_prefix: String,

    /// Max parallel LOAD CSV jobs for edge operations
    #[arg(long, default_value_t = dedalus::config::IMPORT_MAX_PARALLEL_EDGES)]
    max_parallel_edges: usize,

    /// Max parallel LOAD CSV jobs for lighter relationship operations
    #[arg(long, default_value_t = dedalus::config::IMPORT_MAX_PARALLEL_LIGHT)]
    max_parallel_light: usize,

    /// Docker compose file path (auto-detected if not specified)
    #[arg(long)]
    compose_file: Option<String>,

    /// Skip Docker management, just connect to an already-running Neo4j
    #[arg(long)]
    no_docker: bool,

    /// Skip the import step (extract + merge only)
    #[arg(long)]
    no_import: bool,

    /// Don't archive sharded CSVs after merging
    #[arg(long)]
    no_archive: bool,

    /// Path to multistream index file (.txt.bz2) for parallel parsing
    #[arg(long)]
    multistream_index: Option<String>,
}

#[derive(Args)]
struct StatsArgs {
    /// Output directory to inspect
    #[arg(short, long, default_value = "output")]
    output: String,
}

fn run_extract(args: ExtractArgs) -> Result<()> {
    if args.clean {
        let output_path = Path::new(&args.output);
        if output_path.exists() {
            info!("Cleaning output directory: {}", args.output);
            fs::remove_dir_all(output_path)
                .with_context(|| format!("Failed to clean output directory: {}", args.output))?;
        }
    }

    fs::create_dir_all(&args.output)
        .with_context(|| format!("Failed to create output directory: {}", args.output))?;

    // Resolve multistream index: explicit flag > auto-detect from filename
    let multistream_index_path = args
        .multistream_index
        .clone()
        .or_else(|| dedalus::multistream::detect_index_path(&args.input));

    let multistream_ranges = if let Some(ref idx_path) = multistream_index_path {
        info!(index = %idx_path, "Using multistream parallel parsing");
        let ranges = dedalus::multistream::parse_multistream_index(idx_path, &args.input)?;
        info!(streams = ranges.len(), "Multistream index parsed");
        Some(ranges)
    } else {
        None
    };

    let start_indexing = Instant::now();
    let cache_path = cache::cache_path(&args.output);

    let index = if args.no_cache {
        info!("Cache disabled, building fresh index");
        let idx = if let Some(ref ranges) = multistream_ranges {
            dedalus::index::WikiIndex::build_multistream(&args.input, ranges)?
        } else {
            dedalus::index::WikiIndex::build(&args.input)?
        };
        if !args.dry_run {
            if let Err(e) = cache::save_index(&idx, &args.input, &args.output) {
                warn!(error = %e, "Failed to save index cache");
            }
        }
        idx
    } else if let Some(idx) = cache::try_load_index(&cache_path, &args.input)? {
        info!("Loaded index from cache");
        idx
    } else {
        info!("Building index (cache miss or invalid)");
        let idx = if let Some(ref ranges) = multistream_ranges {
            dedalus::index::WikiIndex::build_multistream(&args.input, ranges)?
        } else {
            dedalus::index::WikiIndex::build(&args.input)?
        };
        if !args.dry_run {
            if let Err(e) = cache::save_index(&idx, &args.input, &args.output) {
                warn!(error = %e, "Failed to save index cache");
            }
        }
        idx
    };

    let indexing_duration = start_indexing.elapsed();
    info!(
        duration_secs = indexing_duration.as_secs_f64(),
        "Indexing complete"
    );

    let checkpoint_mgr = if !args.dry_run {
        Some(CheckpointManager::new(
            &args.input,
            &args.output,
            args.shard_count,
            args.csv_shards,
            args.checkpoint_interval,
        )?)
    } else {
        None
    };

    let checkpoint = if args.resume && !args.clean {
        match checkpoint::load_if_valid(
            &args.input,
            &args.output,
            args.shard_count,
            args.csv_shards,
        )? {
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
    let extraction_config = dedalus::extract::ExtractionConfig {
        input_path: &args.input,
        output_dir: &args.output,
        index: &index,
        shard_count: args.shard_count,
        csv_shards: args.csv_shards,
        limit: args.limit,
        dry_run: args.dry_run,
        resume_from: checkpoint.as_ref(),
        checkpoint_mgr: checkpoint_mgr.as_ref(),
        multistream_ranges: multistream_ranges.as_deref(),
    };
    let stats = dedalus::extract::run_extraction(&extraction_config)?;
    let extraction_duration = start_extracting.elapsed();
    info!(
        duration_secs = extraction_duration.as_secs_f64(),
        "Extraction complete"
    );

    if let Some(ref mgr) = checkpoint_mgr {
        if let Err(e) = mgr.clear() {
            warn!(error = %e, "Failed to clear checkpoint");
        }
    }

    println!();
    println!("=== Summary ===");
    println!(
        "Indexing time:      {:.2}s",
        indexing_duration.as_secs_f64()
    );
    println!(
        "Extraction time:    {:.2}s",
        extraction_duration.as_secs_f64()
    );
    println!(
        "Total time:         {:.2}s",
        (indexing_duration + extraction_duration).as_secs_f64()
    );
    println!();
    println!("Articles processed: {}", stats.articles());
    println!("Edges extracted:    {}", stats.edges());
    println!("See also edges:     {}", stats.see_also_edges());
    println!("Blobs written:      {}", stats.blobs());
    println!("Invalid links:      {}", stats.invalid());
    println!("Categories found:   {}", stats.categories());
    println!("Category edges:     {}", stats.category_edges());
    println!("Infoboxes found:    {}", stats.infoboxes());
    println!("Images found:       {}", stats.images());
    println!("External links:     {}", stats.external_links());

    Ok(())
}

fn run_import(args: ImportArgs) -> Result<()> {
    let config = ImportConfig {
        output_dir: args.output,
        bolt_uri: args.bolt_uri,
        import_prefix: args.import_prefix,
        max_parallel_edges: args.max_parallel_edges,
        max_parallel_light: args.max_parallel_light,
        compose_file: args.compose_file,
        no_docker: args.no_docker,
        clean: args.clean,
        use_admin_import: args.admin_import,
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("dedalus-import-worker")
        .enable_io()
        .enable_time()
        .build()?;
    rt.block_on(dedalus::import::run_import(config))
}

fn run_pipeline(args: PipelineArgs) -> Result<()> {
    let overall_start = Instant::now();

    // Pre-flight: validate dump file
    let input_path = Path::new(&args.input);
    if !input_path.exists() {
        bail!(
            "Wikipedia dump not found: {}\n\n\
             Please specify a valid dump file:\n  \
             dedalus pipeline -i path/to/dump.xml.bz2 -o output/\n\n\
             Download Wikipedia dumps from:\n  \
             https://dumps.wikimedia.org/enwiki/latest/",
            args.input
        );
    }
    let dump_size = fs::metadata(input_path)?.len();
    println!("==> Using Wikipedia dump: {}", args.input);
    println!("    Size: {}", format_size(dump_size));
    println!();

    // Step 1: Extract
    let step_count = if args.no_import { 2 } else { 3 };
    println!("==> Step 1/{}: Extracting Wikipedia dump...", step_count);
    println!("    Input:       {}", args.input);
    println!("    Output:      {}", args.output);
    println!("    CSV shards:  {}", args.csv_shards);
    println!("    Blob shards: {}", args.shard_count);
    if let Some(limit) = args.limit {
        println!("    Limit:       {} pages", limit);
    }
    println!();

    run_extract(ExtractArgs {
        input: args.input.clone(),
        output: args.output.clone(),
        shard_count: args.shard_count,
        csv_shards: args.csv_shards,
        limit: args.limit,
        dry_run: false,
        resume: args.resume,
        no_cache: args.no_cache,
        checkpoint_interval: args.checkpoint_interval,
        clean: args.clean,
        multistream_index: args.multistream_index.clone(),
    })
    .context("Extraction step failed")?;

    // Step 2: Merge (conditional)
    if args.csv_shards > 1 {
        println!();
        println!(
            "==> Step 2/{}: Merging {} CSV shards...",
            step_count, args.csv_shards
        );
        dedalus::merge::merge_csv_shards(&args.output).context("Merge step failed")?;

        if !args.no_archive {
            println!("==> Archiving sharded CSV files...");
            dedalus::merge::archive_shards(&args.output).context("Shard archiving failed")?;
        }
    } else {
        println!();
        println!("==> Step 2/{}: Skipping merge (csv-shards=1)", step_count);
    }

    // Step 3: Import
    if !args.no_import {
        println!();
        println!(
            "==> Step 3/{}: Importing into Neo4j (admin bulk import)...",
            step_count
        );

        let import_config = ImportConfig {
            output_dir: args.output.clone(),
            bolt_uri: args.bolt_uri.clone(),
            import_prefix: args.import_prefix,
            max_parallel_edges: args.max_parallel_edges,
            max_parallel_light: args.max_parallel_light,
            compose_file: args.compose_file,
            no_docker: args.no_docker,
            clean: args.clean,
            use_admin_import: true,
        };

        let rt = tokio::runtime::Builder::new_multi_thread()
            .thread_name("dedalus-import-worker")
            .enable_io()
            .enable_time()
            .build()?;
        rt.block_on(dedalus::import::run_import(import_config))
            .context("Import step failed")?;
    } else {
        println!();
        println!(
            "==> Step {0}/{0}: Skipping import (--no-import)",
            step_count
        );
    }

    let total_duration = overall_start.elapsed();
    println!();
    println!(
        "==> Pipeline complete! ({:.1}s)",
        total_duration.as_secs_f64()
    );
    println!("    Output:  {}", args.output);
    if !args.no_import {
        println!("    Neo4j:   {}", args.bolt_uri);
        println!("    Browser: http://localhost:7474");
    }
    println!();

    Ok(())
}

fn run_stats(args: StatsArgs) -> Result<()> {
    let output_dir = Path::new(&args.output);
    if !output_dir.exists() {
        bail!("Output directory does not exist: {}", args.output);
    }

    println!("==> Output Statistics");
    println!();
    println!("Directory: {}", args.output);
    println!();

    // CSV files
    println!("CSV Files:");
    let mut csv_files: Vec<_> = fs::read_dir(output_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().map(|t| t.is_file()).unwrap_or(false)
                && e.file_name().to_string_lossy().ends_with(".csv")
        })
        .collect();
    csv_files.sort_by_key(|e| e.file_name());

    if csv_files.is_empty() {
        println!("  None found");
    } else {
        for entry in &csv_files {
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            println!(
                "  {} ({})",
                entry.file_name().to_string_lossy(),
                format_size(size)
            );
        }
    }
    println!();

    // Blob directories
    println!("Blob Directories:");
    let blobs_dir = output_dir.join("blobs");
    if blobs_dir.exists() {
        let mut blob_count = 0u64;
        let mut blob_size = 0u64;
        if let Ok(shard_dirs) = fs::read_dir(&blobs_dir) {
            for shard_dir in shard_dirs.filter_map(|e| e.ok()) {
                if shard_dir.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    if let Ok(files) = fs::read_dir(shard_dir.path()) {
                        for file in files.filter_map(|e| e.ok()) {
                            if file.file_name().to_string_lossy().ends_with(".json") {
                                blob_count += 1;
                                blob_size += file.metadata().map(|m| m.len()).unwrap_or(0);
                            }
                        }
                    }
                }
            }
        }
        println!("  Total blobs: {}", blob_count);
        println!("  Disk usage:  {}", format_size(blob_size));
    } else {
        println!("  None found");
    }
    println!();

    // Archived shards
    let shards_dir = output_dir.join("shards");
    if shards_dir.exists() {
        let shard_count = fs::read_dir(&shards_dir)
            .map(|entries| entries.filter_map(|e| e.ok()).count())
            .unwrap_or(0);
        if shard_count > 0 {
            println!("Archived Shards:");
            println!("  {} files in {}/shards/", shard_count, args.output);
            println!();
        }
    }

    // Total size
    let total_size = dir_size(output_dir);
    println!("Total size: {}", format_size(total_size));
    println!();

    Ok(())
}

fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.filter_map(|e| e.ok()) {
            let ft = entry.file_type().unwrap_or_else(|_| {
                // Fallback: treat as file
                fs::metadata(entry.path())
                    .map(|m| m.file_type())
                    .unwrap_or_else(|_| entry.file_type().unwrap())
            });
            if ft.is_file() {
                total += entry.metadata().map(|m| m.len()).unwrap_or(0);
            } else if ft.is_dir() {
                total += dir_size(&entry.path());
            }
        }
    }
    total
}

fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * 1024;
    const GB: u64 = 1024 * 1024 * 1024;
    match bytes {
        b if b >= GB => format!("{:.1}G", b as f64 / GB as f64),
        b if b >= MB => format!("{:.1}M", b as f64 / MB as f64),
        b if b >= KB => format!("{:.1}K", b as f64 / KB as f64),
        b => format!("{}B", b),
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    // TUI sets up its own tracing subscriber, so handle it before the default one
    if matches!(cli.command, Commands::Tui) {
        return match dedalus::tui::run_tui() {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("TUI error: {:#}", e);
                ExitCode::FAILURE
            }
        };
    }

    let level = match cli.verbose {
        0 => Level::WARN,
        1 => Level::INFO,
        2 => Level::DEBUG,
        _ => Level::TRACE,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    let result = match cli.command {
        Commands::Extract(args) => run_extract(args),
        Commands::Import(args) => run_import(args),
        Commands::MergeCsvs(args) => {
            let output = args.output.clone();
            let archive = args.archive;
            dedalus::merge::merge_csv_shards(&output).and_then(|()| {
                if archive {
                    dedalus::merge::archive_shards(&output)
                } else {
                    Ok(())
                }
            })
        }
        Commands::Pipeline(args) => run_pipeline(args),
        Commands::Stats(args) => run_stats(args),
        Commands::Tui => unreachable!(),
    };

    match result {
        Ok(()) => {
            info!("Completed successfully");
            ExitCode::SUCCESS
        }
        Err(e) => {
            error!("Error: {:#}", e);
            ExitCode::FAILURE
        }
    }
}
