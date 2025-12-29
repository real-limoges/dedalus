#![allow(dead_code)]
use bzip2::read::BzDecoder;
use quick_xml::reader::Reader;
use std::fs::File;
use std::io::BufReader;

pub struct WikiReader {
    reader: Reader<BufReader<BzDecoder<File>>>,
    buf: Vec<u8>,
}
