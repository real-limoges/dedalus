#![allow(dead_code)]
use bzip2::read::BzDecoder;
use quick_xml::reader::Reader;
use std::io::BufReader;
use std::fs::File;

pub struct WikiReader {
    reader: Reader<BufReader<BzDecoder<File>>>,
    buf: Vec<u8>
}
