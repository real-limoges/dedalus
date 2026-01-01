mod extract;
mod index;
mod models;
mod parser;

use clap::Parser;
use std::time::Instant;

#[derive(Parser)]
struct Cli {
    #[arg(short, long)]
    input: String,
    #[arg(short, long)]
    output: String,
}
fn main() {
    let args = Cli::parse();

    println!("# ----- Mapping ----- #");
    let start_indexing = Instant::now();
    let index = index::WikiIndex::build(&args.input);
    println!("Mapping Took: {:?}", start_indexing.elapsed().as_secs_f64());

    println!("# ----- Extracting ----- #");
    let start_extracting = Instant::now();
    extract::run_extraction(&args.input, &args.output, &index);
    println!(
        "Extracting Took: {:?}",
        start_extracting.elapsed().as_secs_f64(),
    );
}
