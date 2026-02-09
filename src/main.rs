use anyhow::{Context, Result};
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

    let start_indexing = Instant::now();
    let cache_path = cache::cache_path(&args.output);

    let index = if args.no_cache {
        info!("Cache disabled, building fresh index");
        let idx = dedalus::index::WikiIndex::build(&args.input)?;
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
        let idx = dedalus::index::WikiIndex::build(&args.input)?;
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
    let stats = dedalus::extract::run_extraction(
        &args.input,
        &args.output,
        &index,
        args.shard_count,
        args.csv_shards,
        args.limit,
        args.dry_run,
        checkpoint.as_ref(),
        checkpoint_mgr.as_ref(),
    )?;
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
        .worker_threads(4)
        .thread_name("dedalus-import-worker")
        .enable_io()
        .enable_time()
        .build()?;
    rt.block_on(dedalus::import::run_import(config))
}

fn main() -> ExitCode {
    let cli = Cli::parse();

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
        Commands::MergeCsvs(args) => dedalus::merge::merge_csv_shards(&args.output),
    };

    match result {
        Ok(()) => {
            info!("Completed successfully");
            ExitCode::SUCCESS
        }
        Err(e) => {
            error!("Error: {:#}", e);
            eprintln!("Error: {:#}", e);
            ExitCode::FAILURE
        }
    }
}
