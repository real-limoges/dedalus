#![allow(dead_code, unused_variables)]
use crate::models::{WikiPage, PageType};
use anyhow::Result;
use bzip2::read::BzDecoder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::fs::File;
use std::io::BufReader;

pub struct WikiReader {
    reader: Reader<BufReader<BzDecoder<File>>>,
    buf: Vec<u8>,
    skip_text: bool,
}

impl WikiReader {
    pub fn new(path: &str, skip_text: bool) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = Reader::from_reader(BufReader::new(BzDecoder::new(file)));
        reader.config_mut().trim_text(true);
        Ok(Self {
            reader,
            buf: Vec::new(),
            skip_text,
        })
    }
}

// this is so disgusting. i couldn't figure out how to do this
// functionally because I have to abuse the buffer
impl Iterator for WikiReader {
    type Item = WikiPage;

    fn next(&mut self) -> Option<Self::Item> {
        let mut current_id = None;
        let mut current_title = None;
        let mut redirect_target = None;
        let mut current_text = None;

        // some state flags of where I am
        let mut in_title = false;
        let mut in_id = false;
        let mut in_text = false;

        // I'll build the loop at the end
        // there's a few ways to do this. I'm gonna reuse the buffer to speed
        // things up. I'm going to read into the buffer and match the event
        // to parse
        loop {
            let event = self.reader.read_event_into(&mut self.buf);

            match event {
                Ok(Event::Start(e)) => match e.name().as_ref() {
                    b"page" => {
                        todo!()
                    }
                    b"title" => in_title = true,
                    b"id" if current_id.is_none() => in_id = true,
                    b"text" => {
                        if !self.skip_text {
                            in_text = true
                        }
                    }
                    b"redirect" => {
                        if let Ok(attr) = e.try_get_attribute("title") {
                            redirect_target =
                                Some(String::from_utf8_lossy(&attr.unwrap().value).to_string());
                        }
                    },
                    _ => (),

                },

                Ok(Event::Text(e)) => {
                    if in_title {
                        let s = String::from_utf8_lossy(&e);
                        current_title = Some(s.into_owned());
                    }
                    if in_id {
                        let s = String::from_utf8_lossy(&e);
                        current_id = s.parse::<u32>().ok();
                    }
                    if in_text {
                        let s = String::from_utf8_lossy(&e);
                        current_text = Some(s.into_owned());
                    }
                }

                Ok(Event::End(e)) => match e.name().as_ref() {
                    b"title" => in_title = false,
                    b"id" => in_id = false,
                    b"text" => in_text = false,
                    b"page" => {
                        let title = current_title.take()?;
                        let id = current_id.take()?;

                        let page_type = if let Some(target) = redirect_target.take() {
                            PageType::Redirect(target)
                        } else if title.starts_with("File:") || title.starts_with("Category:") {
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
                    _ => (),
                },
                Ok(Event::Eof) => return None,
                Err(_) => return None,
                _ => (),
            }
            self.buf.clear();
        }
    }
}
