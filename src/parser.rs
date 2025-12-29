#![allow(dead_code, unused_variables)]
use crate::models::{WikiPage};
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
        let event = self.reader.read_event_into(&mut self.buf);

        match event {
            Ok(Event::Start(e)) => {todo!()},
            Ok(Event::Text(e)) => {todo!()},
            Ok(Event::End(e)) => {todo!()},
            Ok(Event::Eof) => {todo!()},
            Err(_) => return None,
            _ => (),
        }
        self.buf.clear();
    }
}
