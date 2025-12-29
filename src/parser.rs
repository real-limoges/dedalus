#![allow(dead_code)]
use quick_xml::reader::Reader;
use std::io::BufReader;
use std::fs::File;
use bzip2::read::BzDecoder;

pub struct WikiReader {
    reader: Reader<BufReader<BzDecoder<File>>>,
    buf: Vec<u8>
}