#![allow(dead_code)]

use crate::index::WikiIndex;
use crate::parser::WikiReader;
use regex::Regex;

pub fn run_extraction(path: &str, output_dir: &str, _index: &WikiIndex) {
    let _reader = WikiReader::new(path, true).expect("Failed to open wiki dump");
    // there's no way I didn't look this up haha
    let _link_regex = Regex::new(r"\[\[([^|\]]+?)(?:\|[^\]]+)?\]\]").unwrap();

    let _nodes_writer = csv::Writer::from_path(format!("{}/nodes.csv", output_dir)).unwrap();
    let _edges_writer = csv::Writer::from_path(format!("{}/edges.csv", output_dir)).unwrap();
}
