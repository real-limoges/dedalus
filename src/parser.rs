use crate::models::{PageType, WikiPage};
use anyhow::{Context, Result};
use bzip2::read::BzDecoder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::fs::File;
use std::io::BufReader;
use std::str;

pub struct WikiReader {
    reader: Reader<BufReader<BzDecoder<File>>>,
    buf: Vec<u8>,
    skip_text: bool,
}

impl WikiReader {
    pub fn new(path: &str, skip_text: bool) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("Could not open file: {}", path))?;
        let decoder = BzDecoder::new(file);
        let reader = BufReader::new(decoder);

        let xml_reader = Reader::from_reader(reader);

        Ok(Self {
            reader: xml_reader,
            buf: Vec::with_capacity(1024), // Pre-allocate a reasonable buffer
            skip_text,
        })
    }
}

impl Iterator for WikiReader {
    type Item = WikiPage;

    fn next(&mut self) -> Option<Self::Item> {
        // state
        let mut current_id = None;
        let mut current_title: Option<String> = None;
        let mut current_text: Option<String> = None;
        let mut redirect_target = None;

        // flags
        let mut in_title = false;
        let mut in_id = false;
        let mut in_text = false;

        loop {
            match self.reader.read_event_into(&mut self.buf) {

                // 1. START TAGS
                Ok(Event::Start(e)) => match e.name().as_ref() {
                    b"page" => {
                        // implicit reset
                    }
                    b"title" => in_title = true,
                    b"id" if current_id.is_none() => in_id = true,
                    b"text" => {
                        // skip text first time around
                        if !self.skip_text {
                            in_text = true;
                        }
                    }
                    b"redirect" => {
                        if let Ok(Some(attr)) = e.try_get_attribute("title") {
                            redirect_target = Some(String::from_utf8_lossy(&attr.value).to_string());
                        }
                    }
                    _ => (),
                },

                Ok(Event::Text(e)) => {
                    if in_title {
                        if let Ok(s) = e.unescape() {
                            current_title = Some(s.into_owned());
                        }
                    } else if in_id {
                        let s = String::from_utf8_lossy(&e).trim().to_string();
                        current_id = s.parse::<u32>().ok();
                    } else if in_text
                        && let Ok(s) = e.unescape() {
                            current_text = Some(s.into_owned());
                        }
                },

                Ok(Event::End(e)) => match e.name().as_ref() {
                    b"title" => in_title = false,
                    b"id" => in_id = false,
                    b"text" => in_text = false,
                    b"page" => {
                        if let (Some(id), Some(title)) = (current_id, current_title.take()) {

                            let page_type = if let Some(target) = redirect_target.take() {
                                PageType::Redirect(target)
                            } else if title.starts_with("File:") || title.starts_with("Category:") || title.starts_with("Template:") {
                                PageType::Special
                            } else {
                                PageType::Article
                            };

                            return Some(WikiPage {
                                id,
                                title,
                                page_type,
                                text: current_text.take(),
                            });
                        }
                    }
                    _ => (),
                },
                Ok(Event::Eof) => return None,
                Err(e) => {
                    eprintln!("XML Parse Error at position {}: {:?}", self.reader.buffer_position(), e);
                    return None;
                }
                _ => (),
            }
            // reuse memory
            self.buf.clear();
        }
    }
}