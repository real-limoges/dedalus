use crate::models::WikiPage;
use crate::parser::PageParser;
use anyhow::{Context, Result};
use bzip2::read::{BzDecoder, MultiBzDecoder};
use rayon::prelude::*;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom};
use tracing::{info, warn};

/// A contiguous bz2 stream within the multistream dump.
#[derive(Debug, Clone)]
pub struct StreamRange {
    /// Byte offset where this bz2 stream starts in the dump file.
    pub offset: u64,
    /// Number of bytes in this bz2 stream (until the next stream starts).
    pub length: u64,
}

/// Parse the multistream index file to extract bz2 stream byte offsets.
///
/// The index file is bz2-compressed. Each decompressed line has the format:
///   `byte_offset:page_id:page_title`
///
/// Multiple lines sharing the same byte_offset belong to the same bz2 stream.
/// We group by offset and compute stream lengths from consecutive offsets.
pub fn parse_multistream_index(index_path: &str, dump_path: &str) -> Result<Vec<StreamRange>> {
    info!("Parsing multistream index: {}", index_path);

    let file = File::open(index_path)
        .with_context(|| format!("Failed to open multistream index: {}", index_path))?;
    let decoder = MultiBzDecoder::new(file);
    let reader = BufReader::with_capacity(256 * 1024, decoder);

    // Collect unique offsets in order
    let mut offsets: Vec<u64> = Vec::with_capacity(250_000);
    let mut last_offset: Option<u64> = None;

    for line in reader.lines() {
        let line = line.context("Failed to read index line")?;
        let colon_pos = match line.find(':') {
            Some(pos) => pos,
            None => continue,
        };
        let offset: u64 = match line[..colon_pos].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        if last_offset != Some(offset) {
            offsets.push(offset);
            last_offset = Some(offset);
        }
    }

    if offsets.is_empty() {
        anyhow::bail!("No stream offsets found in multistream index");
    }

    // Get dump file size for the last stream's length
    let dump_size = fs::metadata(dump_path)
        .with_context(|| format!("Failed to stat dump file: {}", dump_path))?
        .len();

    // Build StreamRange vec with computed lengths
    let mut ranges = Vec::with_capacity(offsets.len());
    for i in 0..offsets.len() {
        let offset = offsets[i];
        let length = if i + 1 < offsets.len() {
            offsets[i + 1] - offset
        } else {
            dump_size - offset
        };
        ranges.push(StreamRange { offset, length });
    }

    info!(
        streams = ranges.len(),
        dump_size = dump_size,
        "Multistream index parsed"
    );

    Ok(ranges)
}

/// Parse all pages from a single bz2 stream within the dump file.
fn parse_stream(dump_path: &str, range: &StreamRange, skip_text: bool) -> Vec<WikiPage> {
    let result = parse_stream_inner(dump_path, range, skip_text);
    match result {
        Ok(pages) => pages,
        Err(e) => {
            warn!(
                offset = range.offset,
                length = range.length,
                error = %e,
                "Failed to parse stream, skipping"
            );
            Vec::new()
        }
    }
}

fn parse_stream_inner(
    dump_path: &str,
    range: &StreamRange,
    skip_text: bool,
) -> Result<Vec<WikiPage>> {
    let mut file = File::open(dump_path)?;
    file.seek(SeekFrom::Start(range.offset))?;
    let limited = file.take(range.length);
    let decoder = BzDecoder::new(limited);

    // Wrap decompressed XML in <mediawiki> tags since individual streams
    // contain bare <page> elements without a root wrapper.
    let prefix = Cursor::new(b"<mediawiki>" as &[u8]);
    let suffix = Cursor::new(b"</mediawiki>" as &[u8]);
    let wrapped = prefix.chain(decoder).chain(suffix);

    let parser = PageParser::new(wrapped, skip_text).skip_timestamp(skip_text);
    Ok(parser.collect())
}

/// Parse pages from a single stream for index building (skip_text=true).
pub fn parse_stream_for_index(dump_path: &str, range: &StreamRange) -> Vec<WikiPage> {
    parse_stream(dump_path, range, true)
}

/// Create a parallel iterator over all pages in the multistream dump.
///
/// Each rayon worker independently opens the dump file, seeks to a stream offset,
/// decompresses, and parses XML. This achieves true parallelism in both
/// decompression and XML parsing.
pub fn par_iter_pages<'a>(
    dump_path: &'a str,
    ranges: &'a [StreamRange],
    skip_text: bool,
) -> impl ParallelIterator<Item = WikiPage> + 'a {
    ranges
        .par_iter()
        .flat_map_iter(move |range| parse_stream(dump_path, range, skip_text))
}

/// Try to auto-detect the multistream index file from the dump path.
///
/// Wikipedia naming convention:
///   Dump:  `*-multistream.xml.bz2`
///   Index: `*-multistream-index.txt.bz2`
pub fn detect_index_path(dump_path: &str) -> Option<String> {
    // Check if the dump filename contains "multistream"
    if !dump_path.contains("multistream") {
        return None;
    }

    // Replace `.xml.bz2` with `-index.txt.bz2`
    let index_path = dump_path.replace(".xml.bz2", "-index.txt.bz2");
    if index_path == dump_path {
        return None;
    }

    if std::path::Path::new(&index_path).exists() {
        info!(path = %index_path, "Auto-detected multistream index file");
        Some(index_path)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bzip2::write::BzEncoder;
    use bzip2::Compression;
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    fn create_bz2_stream(data: &[u8]) -> Vec<u8> {
        let mut encoder = BzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn create_multistream_dump() -> (NamedTempFile, NamedTempFile) {
        // Create two independent bz2 streams with page XML
        let stream1_xml = b"<page>
            <title>Article One</title>
            <id>1</id>
            <ns>0</ns>
            <revision><id>100</id><text>Content one</text></revision>
        </page>
        <page>
            <title>Article Two</title>
            <id>2</id>
            <ns>0</ns>
            <revision><id>200</id><text>Content two</text></revision>
        </page>";

        let stream2_xml = b"<page>
            <title>Article Three</title>
            <id>3</id>
            <ns>0</ns>
            <revision><id>300</id><text>Content three</text></revision>
        </page>";

        let stream1_bz2 = create_bz2_stream(stream1_xml);
        let stream2_bz2 = create_bz2_stream(stream2_xml);

        let stream1_len = stream1_bz2.len() as u64;

        // Concatenate bz2 streams into dump file
        let mut dump = NamedTempFile::new().unwrap();
        dump.write_all(&stream1_bz2).unwrap();
        dump.write_all(&stream2_bz2).unwrap();
        dump.flush().unwrap();

        // Create index file (bz2-compressed)
        let index_content = format!(
            "0:1:Article One\n0:2:Article Two\n{}:3:Article Three\n",
            stream1_len
        );
        let index_bz2 = create_bz2_stream(index_content.as_bytes());
        let mut index = NamedTempFile::new().unwrap();
        index.write_all(&index_bz2).unwrap();
        index.flush().unwrap();

        (dump, index)
    }

    #[test]
    fn parse_index_extracts_stream_ranges() {
        let (dump, index) = create_multistream_dump();
        let ranges = parse_multistream_index(
            index.path().to_str().unwrap(),
            dump.path().to_str().unwrap(),
        )
        .unwrap();

        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].offset, 0);
        assert!(ranges[0].length > 0);
        assert!(ranges[1].offset > 0);
        assert!(ranges[1].length > 0);
        assert_eq!(ranges[0].length, ranges[1].offset);
    }

    #[test]
    fn parse_stream_extracts_pages() {
        let (dump, index) = create_multistream_dump();
        let ranges = parse_multistream_index(
            index.path().to_str().unwrap(),
            dump.path().to_str().unwrap(),
        )
        .unwrap();

        let pages1 = parse_stream(dump.path().to_str().unwrap(), &ranges[0], false);
        assert_eq!(pages1.len(), 2);
        assert_eq!(pages1[0].title, "Article One");
        assert_eq!(pages1[1].title, "Article Two");

        let pages2 = parse_stream(dump.path().to_str().unwrap(), &ranges[1], false);
        assert_eq!(pages2.len(), 1);
        assert_eq!(pages2[0].title, "Article Three");
    }

    #[test]
    fn par_iter_pages_collects_all() {
        let (dump, index) = create_multistream_dump();
        let ranges = parse_multistream_index(
            index.path().to_str().unwrap(),
            dump.path().to_str().unwrap(),
        )
        .unwrap();

        let dump_path = dump.path().to_str().unwrap();
        let mut pages: Vec<_> = par_iter_pages(dump_path, &ranges, false).collect();
        pages.sort_by_key(|p| p.id);

        assert_eq!(pages.len(), 3);
        assert_eq!(pages[0].title, "Article One");
        assert_eq!(pages[1].title, "Article Two");
        assert_eq!(pages[2].title, "Article Three");
    }

    #[test]
    fn par_iter_pages_skip_text() {
        let (dump, index) = create_multistream_dump();
        let ranges = parse_multistream_index(
            index.path().to_str().unwrap(),
            dump.path().to_str().unwrap(),
        )
        .unwrap();

        let dump_path = dump.path().to_str().unwrap();
        let pages: Vec<_> = par_iter_pages(dump_path, &ranges, true).collect();

        assert_eq!(pages.len(), 3);
        for page in &pages {
            assert!(page.text.is_none());
        }
    }

    #[test]
    fn detect_index_path_finds_sibling() {
        let dir = TempDir::new().unwrap();
        let dump_path = dir
            .path()
            .join("enwiki-latest-pages-articles-multistream.xml.bz2");
        let index_path = dir
            .path()
            .join("enwiki-latest-pages-articles-multistream-index.txt.bz2");

        // Create empty files
        File::create(&dump_path).unwrap();
        File::create(&index_path).unwrap();

        let detected = detect_index_path(dump_path.to_str().unwrap());
        assert_eq!(detected.as_deref(), Some(index_path.to_str().unwrap()));
    }

    #[test]
    fn detect_index_path_returns_none_for_regular_dump() {
        let detected = detect_index_path("/tmp/enwiki-latest-pages-articles.xml.bz2");
        assert!(detected.is_none());
    }

    #[test]
    fn detect_index_path_returns_none_when_missing() {
        let detected = detect_index_path("/tmp/nonexistent-multistream.xml.bz2");
        assert!(detected.is_none());
    }
}
