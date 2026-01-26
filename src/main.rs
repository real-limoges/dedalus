mod config;
mod extract;
mod index;
mod models;
mod parser;
mod stats;

use anyhow::Result;
use clap::Parser;
use std::process::ExitCode;
use std::time::Instant;
use tracing::{error, info, Level};
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
}

fn run(args: Cli) -> Result<()> {
    info!("Starting indexing pass");
    let start_indexing = Instant::now();
    let index = index::WikiIndex::build(&args.input)?;
    let indexing_duration = start_indexing.elapsed();
    info!(
        duration_secs = indexing_duration.as_secs_f64(),
        "Indexing complete"
    );

    info!("Starting extraction pass");
    let start_extracting = Instant::now();
    let stats =
        extract::run_extraction(&args.input, &args.output, &index, args.limit, args.dry_run)?;
    let extraction_duration = start_extracting.elapsed();
    info!(
        duration_secs = extraction_duration.as_secs_f64(),
        "Extraction complete"
    );

    // Print summary
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
    println!("Blobs written:      {}", stats.blobs());
    println!("Invalid links:      {}", stats.invalid());

    Ok(())
}

fn main() -> ExitCode {
    let args = Cli::parse();

    // Initialize tracing based on verbosity
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
