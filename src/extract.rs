#![allow(dead_code)]

use crate::index::WikiIndex;
use crate::models::{ArticleBlob, PageType};
use crate::parser::WikiReader;
use regex::Regex;
use std::fs::{self, File};
use std::sync::{Arc, Mutex};
use rayon::prelude::*;

pub fn run_extraction(path: &str, output_dir: &str, index: &WikiIndex) {
    let nodes_writer = Arc::new(Mutex::new(
        csv::Writer::from_path(format!("{}/nodes.csv", output_dir)).unwrap()
    ));
    let edges_writer = Arc::new(Mutex::new(
        csv::Writer::from_path(format!("{}/edges.csv", output_dir)).unwrap()
    ));


    let reader = WikiReader::new(path, true).expect("Failed to open wiki dump");
    // there's no way I didn't look this up haha
    let link_regex = Regex::new(r"\[\[([^|\]]+?)(?:\|[^\]]+)?\]\]").unwrap();

    nodes_writer
        .lock()
        .unwrap()
        .write_record(["id:ID", "title", ":LABEL"])
        .unwrap();
    edges_writer
        .lock()
        .unwrap()
        .write_record([":START_ID", ":END_ID", ":TYPE"])
        .unwrap();

    reader.par_bridge().for_each(|page| {
        if let PageType::Article = page.page_type {
            let id_str = page.id.to_string();
            {
                let mut writer = nodes_writer.lock().unwrap();
                writer.write_record([&id_str, &page.title, "Page"]).unwrap();
            }

            if let Some(text) = &page.text {
                let mut local_edges = Vec::new();

                for caps in link_regex.captures_iter(text) {
                    let target_title = &caps[1];
                    if let Some(target_id) = index.resolve_id(target_title) {
                        local_edges.push((id_str.clone(), target_id.to_string()));
                    }
                }
                // batch write
                if !local_edges.is_empty() {
                    let mut writer = edges_writer.lock().unwrap();
                    for (start, end) in local_edges {
                        writer.write_record(&[start, end, "LINKS_TO".to_string()]).unwrap();
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
    });
}