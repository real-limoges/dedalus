mod extract;
mod index;
mod models;
mod parser;
mod index;
mod extract;

use std::time::Instant;

fn main() {
    let start = Instant::now();
    println!("Timing is : {:?}", start.elapsed());
}
