//! CLI entry point for the Dedalus pipeline.
//!
//! Uses `clap` subcommands to orchestrate extract, merge-csvs, analytics, load,
//! pipeline, stats, and tui operations. Initializes `tracing` logging with
//! configurable verbosity and uses `mimalloc` as the global allocator.

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use dedalus::analytics::{AnalyticsConfig, AnalyticsStats};
use dedalus::extract::{ExtractJob, run_extract_job};
use dedalus::load::{LoadConfig, LoadStats};
use dedalus::stats::ExtractionStats;
use std::fs;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(name = "dedalus")]
#[command(about = "Extract Wikipedia dumps into graph CSVs, JSON blobs, analytics and DuckDB")]
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
    /// Compute graph analytics (PageRank, communities, degree) into analytics.csv
    Analytics(AnalyticsArgs),
    /// Merge sharded CSV files into single files
    MergeCsvs(MergeCsvsArgs),
    /// Load merged CSVs and analytics.csv into a DuckDB database file
    Load(LoadArgs),
    /// Run the full pipeline: extract -> merge -> analytics -> load
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
    #[arg(long, default_value_t = dedalus::config::SHARD_COUNT, value_parser = at_least_one)]
    shard_count: u32,

    /// Number of CSV output shards for parallel extraction (1 = single file)
    #[arg(long, default_value_t = dedalus::config::DEFAULT_CSV_SHARDS, value_parser = at_least_one)]
    csv_shards: u32,

    /// Limit number of pages read from the start of the dump (for testing)
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

    /// Pages per extraction chunk; a checkpoint is saved after each chunk
    #[arg(long, default_value_t = dedalus::config::CHECKPOINT_INTERVAL, value_parser = at_least_one)]
    checkpoint_interval: u32,

    /// Clear existing checkpoint and outputs before starting
    #[arg(long)]
    clean: bool,

    /// Path to multistream index file (.txt.bz2) for parallel parsing
    #[arg(long)]
    multistream_index: Option<String>,
}

#[derive(Args)]
struct AnalyticsArgs {
    /// Directory containing merged Dedalus CSV output files
    #[arg(short, long)]
    output: String,

    /// Maximum number of PageRank iterations
    #[arg(long, default_value_t = dedalus::config::PAGERANK_ITERATIONS)]
    pagerank_iterations: u32,

    /// PageRank damping factor
    #[arg(long, default_value_t = dedalus::config::PAGERANK_DAMPING)]
    damping: f64,
}

#[derive(Args)]
struct LoadArgs {
    /// Directory containing merged Dedalus CSV output files
    #[arg(short, long)]
    output: String,

    /// DuckDB database file (relative paths are inside the output directory)
    #[arg(long, default_value = dedalus::load::DEFAULT_DB_FILE)]
    db_path: String,

    /// Replace the database if it already exists
    #[arg(long)]
    clean: bool,
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
    #[arg(long, default_value_t = dedalus::config::SHARD_COUNT, value_parser = at_least_one)]
    shard_count: u32,

    /// Number of CSV output shards for parallel extraction (1 = single file)
    #[arg(long, default_value_t = dedalus::config::DEFAULT_CSV_SHARDS, value_parser = at_least_one)]
    csv_shards: u32,

    /// Limit number of pages read from the start of the dump (for testing)
    #[arg(long)]
    limit: Option<u64>,

    /// Resume from last checkpoint if available
    #[arg(long)]
    resume: bool,

    /// Force rebuild of index cache
    #[arg(long)]
    no_cache: bool,

    /// Pages per extraction chunk; a checkpoint is saved after each chunk
    #[arg(long, default_value_t = dedalus::config::CHECKPOINT_INTERVAL, value_parser = at_least_one)]
    checkpoint_interval: u32,

    /// Clear existing outputs before starting
    #[arg(long)]
    clean: bool,

    /// Skip analytics computation
    #[arg(long)]
    no_analytics: bool,

    /// Skip loading into DuckDB
    #[arg(long)]
    no_load: bool,

    /// DuckDB database file, replaced on every run (relative paths are inside the output directory)
    #[arg(long, default_value = dedalus::load::DEFAULT_DB_FILE)]
    db_path: String,

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

/// Clap value parser for counts that must be at least 1.
fn at_least_one(s: &str) -> Result<u32, String> {
    match s.parse::<u32>() {
        Ok(n) if n >= 1 => Ok(n),
        Ok(_) => Err("must be at least 1".to_string()),
        Err(e) => Err(e.to_string()),
    }
}

fn run_extract(args: ExtractArgs) -> Result<()> {
    let job = ExtractJob {
        input: args.input,
        output: args.output,
        shard_count: args.shard_count,
        csv_shards: args.csv_shards,
        limit: args.limit,
        dry_run: args.dry_run,
        resume: args.resume,
        no_cache: args.no_cache,
        clean: args.clean,
        checkpoint_interval: args.checkpoint_interval,
        multistream_index: args.multistream_index,
    };
    let stats = Arc::new(ExtractionStats::new());
    let cancel = Arc::new(AtomicBool::new(false));
    let report = run_extract_job(&job, Arc::clone(&stats), cancel, false)?;

    println!();
    println!("=== Summary ===");
    println!("Indexing time:      {:.2}s", report.indexing_secs);
    println!("Extraction time:    {:.2}s", report.extraction_secs);
    println!(
        "Total time:         {:.2}s",
        report.indexing_secs + report.extraction_secs
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

fn run_analytics(args: AnalyticsArgs) -> Result<()> {
    let config = AnalyticsConfig {
        output_dir: args.output,
        pagerank_iterations: args.pagerank_iterations,
        pagerank_damping: args.damping,
        ..Default::default()
    };
    let stats = dedalus::analytics::run_analytics(&config)?;

    println!();
    println!("=== Analytics Summary ===");
    print_analytics_stats(&stats);
    Ok(())
}

fn print_analytics_stats(stats: &AnalyticsStats) {
    println!("Nodes:            {}", stats.node_count);
    println!("Edges:            {}", stats.edge_count);
    println!("Skipped edges:    {}", stats.skipped_edges);
    println!("Duplicate nodes:  {}", stats.duplicate_nodes);
    println!(
        "PageRank:         {} iterations, residual {:.2e}",
        stats.pagerank_iterations_run, stats.pagerank_residual
    );
    println!("Communities:      {}", stats.communities_found);
    println!("Elapsed:          {:.2}s", stats.elapsed_secs);
}

fn run_load(args: LoadArgs) -> Result<()> {
    let config = LoadConfig {
        output_dir: args.output,
        db_path: args.db_path,
        replace: args.clean,
    };
    let stats = dedalus::load::run_load(&config)?;

    println!();
    println!("=== Load Summary ===");
    print_load_stats(&stats);
    Ok(())
}

fn print_load_stats(stats: &LoadStats) {
    println!("Database:         {}", stats.db_path.display());
    for (table, rows) in &stats.tables {
        println!("  {table:<24}{rows} rows");
    }
    println!("Elapsed:          {:.2}s", stats.elapsed_secs);
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

    // Extract and merge always run; analytics and load unless skipped.
    let step_count = 2 + u32::from(!args.no_analytics) + u32::from(!args.no_load);

    // Step 1: Extract
    let mut step = 1;
    println!("==> Step {step}/{step_count}: Extracting Wikipedia dump...");
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
    step += 1;
    if args.csv_shards > 1 {
        println!();
        println!(
            "==> Step {step}/{step_count}: Merging {} CSV shards...",
            args.csv_shards
        );
        dedalus::merge::merge_csv_shards(&args.output, args.csv_shards)
            .context("Merge step failed")?;

        if !args.no_archive {
            println!("==> Archiving sharded CSV files...");
            dedalus::merge::archive_shards(&args.output).context("Shard archiving failed")?;
        }
    } else {
        println!();
        println!("==> Step {step}/{step_count}: Skipping merge (csv-shards=1)");
    }

    // Step 3: Analytics
    if args.no_analytics {
        println!();
        println!("==> Skipping analytics (--no-analytics)");
    } else {
        step += 1;
        println!();
        println!("==> Step {step}/{step_count}: Computing graph analytics...");
        let config = AnalyticsConfig {
            output_dir: args.output.clone(),
            ..Default::default()
        };
        let stats = dedalus::analytics::run_analytics(&config).context("Analytics step failed")?;
        println!(
            "    PageRank ({} iters), {} communities, {:.1}s",
            stats.pagerank_iterations_run, stats.communities_found, stats.elapsed_secs
        );
    }

    // Step 4: Load into DuckDB. The database mirrors the CSVs this run just
    // wrote, so it is always replaced.
    let load_config = LoadConfig {
        output_dir: args.output.clone(),
        db_path: args.db_path.clone(),
        replace: true,
    };
    if args.no_load {
        println!();
        println!("==> Skipping DuckDB load (--no-load)");
    } else {
        step += 1;
        println!();
        println!("==> Step {step}/{step_count}: Loading into DuckDB...");
        let stats = dedalus::load::run_load(&load_config).context("Load step failed")?;
        println!(
            "    {} tables in {:.1}s",
            stats.tables.len(),
            stats.elapsed_secs
        );
    }

    let total_duration = overall_start.elapsed();
    println!();
    println!(
        "==> Pipeline complete! ({:.1}s)",
        total_duration.as_secs_f64()
    );
    println!("    Output:   {}", args.output);
    if !args.no_load {
        println!("    Database: {}", load_config.resolved_db_path().display());
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
                if shard_dir.file_type().map(|t| t.is_dir()).unwrap_or(false)
                    && let Ok(files) = fs::read_dir(shard_dir.path())
                {
                    for file in files.filter_map(|e| e.ok()) {
                        if file.file_name().to_string_lossy().ends_with(".json") {
                            blob_count += 1;
                            blob_size += file.metadata().map(|m| m.len()).unwrap_or(0);
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

    // DuckDB database
    let db_file = output_dir.join(dedalus::load::DEFAULT_DB_FILE);
    if let Ok(meta) = fs::metadata(&db_file) {
        println!("DuckDB:");
        println!("  Path: {}", db_file.display());
        println!("  Size: {}", format_size(meta.len()));
        println!();
    }

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

/// Total size of the files under `path`. Entries that cannot be read count
/// as zero, and symlinks are not followed, so a link cycle cannot recurse.
fn dir_size(path: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(|entry| entry.ok())
        .map(|entry| match entry.metadata() {
            Ok(meta) if meta.is_dir() => dir_size(&entry.path()),
            Ok(meta) if meta.is_file() => meta.len(),
            _ => 0,
        })
        .sum()
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
        Commands::Analytics(args) => run_analytics(args),
        Commands::Load(args) => run_load(args),
        Commands::MergeCsvs(args) => {
            let output = args.output.clone();
            let archive = args.archive;
            dedalus::merge::merge_detected_shards(&output).and_then(|_shard_count| {
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
