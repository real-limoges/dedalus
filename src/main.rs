use anyhow::{Context, Result};
use clap::Parser;
use dedalus::cache;
use dedalus::checkpoint::{self, CheckpointManager};
use std::fs;
use std::path::Path;
use std::process::ExitCode;
use std::time::Instant;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(name = "dedalus")]
#[command(about = "Extract Wikipedia dumps into Neo4J-compatible format")]
struct Cli {
    /// Path to the Wikipedia dump file (.xml.bz2)
    #[arg(short, long)]
    input: String,

    /// Output directory for generated files
    #[arg(short, long)]
    output: String,

    /// Number of shards for blob storage
    #[arg(long, default_value_t = 1000)]
    shard_count: u32,

    /// Limit number of pages to process (for testing)
    #[arg(long)]
    limit: Option<u64>,

    /// Dry run - don't write output files
    #[arg(long)]
    dry_run: bool,

    /// Verbosity level (-v, -vv, -vvv)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,

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

fn run(args: Cli) -> Result<()> {
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

    // Index loading: try cache first unless --no-cache
    let start_indexing = Instant::now();
    let cache_path = cache::cache_path(&args.output);

    let index = if args.no_cache {
        info!("Cache disabled, building fresh index");
        let idx = dedalus::index::WikiIndex::build(&args.input)?;
        // Still save the cache for future runs
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
            args.checkpoint_interval,
        )?)
    } else {
        None
    };

    let checkpoint = if args.resume && !args.clean {
        match checkpoint::load_if_valid(&args.input, &args.output, args.shard_count)? {
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

fn main() -> ExitCode {
    let args = Cli::parse();

    let level = match args.verbose {
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

    match run(args) {
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
