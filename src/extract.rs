#![allow(dead_code)]

use crate::index::WikiIndex;
use crate::models::{ArticleBlob, PageType};
use crate::parser::WikiReader;
use regex::Regex;
use std::fs::{self, File};

pub fn run_extraction(path: &str, output_dir: &str, index: &WikiIndex) {
    let reader = WikiReader::new(path, true).expect("Failed to open wiki dump");
    // there's no way I didn't look this up haha
    let link_regex = Regex::new(r"\[\[([^|\]]+?)(?:\|[^\]]+)?\]\]").unwrap();

    let mut nodes_writer = csv::Writer::from_path(format!("{}/nodes.csv", output_dir)).unwrap();
    let mut edges_writer = csv::Writer::from_path(format!("{}/edges.csv", output_dir)).unwrap();

    nodes_writer
        .write_record(["id:ID", "title", ":LABEL"])
        .unwrap();
    edges_writer
        .write_record([":START_ID", ":END_ID", ":TYPE"])
        .unwrap();

    for page in reader {
        if let PageType::Article = page.page_type {
            nodes_writer
                .write_record(&[page.id.to_string(), page.title.clone(), "Page".to_string()])
                .unwrap();

            if let Some(text) = &page.text {
                for caps in link_regex.captures_iter(text) {
                    let target_title = &caps[1];
                    if let Some(target_id) = index.resolve_id(target_title) {
                        edges_writer
                            .write_record(&[
                                page.id.to_string(),
                                target_id.to_string(),
                                "LINKS_TO".to_string(),
                            ])
                            .unwrap();
                    }
                }

                let shard = page.id % 1000;
                let dir_path = format!("{}/blobs/{:03}", output_dir, shard);
                fs::create_dir_all(&dir_path).ok();

                let blob = ArticleBlob {
                    id: page.id,
                    title: page.title,
                    text: text.clone(),
                };

                let blob_path = format!("{}/{}.json", dir_path, page.id);
                let mut f = File::create(blob_path).unwrap();
                serde_json::to_writer_pretty(&mut f, &blob).unwrap();
            }
        }
    }
}
