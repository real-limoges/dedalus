mod extract;
mod index;
mod models;
mod parser;

use std::time::Instant;

fn main() {
    let start = Instant::now();
    println!("Timing is : {:?}", start.elapsed());
}
