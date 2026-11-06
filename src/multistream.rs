//! Multistream dump support for parallel decompression and parsing.
//!
//! Parses the bz2-compressed index file to extract `StreamRange` byte offsets,
//! then provides `par_iter_pages()` where each rayon worker independently seeks,
//! decompresses, and parses its own bz2 stream.

use crate::models::WikiPage;
use crate::parser::PageParser;
use anyhow::{Context, Result};
use bzip2::read::{BzDecoder, MultiBzDecoder};
use rayon::prelude::*;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom};
use tracing::info;

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

    let ranges = ranges_from_offsets(&offsets, dump_size).with_context(|| {
        format!("Multistream index {index_path} does not match dump {dump_path}")
    })?;

    info!(
        streams = ranges.len(),
        dump_size = dump_size,
        "Multistream index parsed"
    );

    Ok(ranges)
}

/// Turns the sorted, distinct stream offsets into ranges. The last stream runs
/// to the end of the dump. Fails if the offsets do not increase or run past
/// the end of the dump, which means the index belongs to a different dump.
fn ranges_from_offsets(offsets: &[u64], dump_size: u64) -> Result<Vec<StreamRange>> {
    let ends = offsets
        .iter()
        .skip(1)
        .copied()
        .chain(std::iter::once(dump_size));
    offsets
        .iter()
        .zip(ends)
        .map(|(&offset, end)| {
            if end <= offset {
                anyhow::bail!("stream offset {offset} is not before {end}");
            }
            Ok(StreamRange {
                offset,
                length: end - offset,
            })
        })
        .collect()
}

/// Parses every page in one bz2 stream of the dump. A stream that fails to
/// decompress or parse is an error: skipping it would silently drop pages.
pub fn parse_stream(
    dump_path: &str,
    range: &StreamRange,
    skip_text: bool,
) -> Result<Vec<WikiPage>> {
    let mut file =
        File::open(dump_path).with_context(|| format!("Failed to open dump: {dump_path}"))?;
    file.seek(SeekFrom::Start(range.offset))?;
    let limited = file.take(range.length);
    let decoder = BzDecoder::new(limited);

    // Wrap decompressed XML in <mediawiki> tags since individual streams
    // contain bare <page> elements without a root wrapper.
    let prefix = Cursor::new(b"<mediawiki>" as &[u8]);
    let suffix = Cursor::new(b"</mediawiki>" as &[u8]);
    let wrapped = prefix.chain(decoder).chain(suffix);

    let mut parser = PageParser::new(wrapped, skip_text).skip_timestamp(skip_text);
    let pages: Vec<WikiPage> = parser.by_ref().collect();
    match parser.error() {
        Some(error) => anyhow::bail!(
            "Stream at byte {} ({} bytes) is unreadable: {error}",
            range.offset,
            range.length
        ),
        None => Ok(pages),
    }
}

/// Parses the given streams in parallel, one rayon task per stream, and
/// returns their pages in dump order.
pub fn parse_streams(
    dump_path: &str,
    ranges: &[StreamRange],
    skip_text: bool,
) -> Result<Vec<WikiPage>> {
    let per_stream: Vec<Vec<WikiPage>> = ranges
        .par_iter()
        .map(|range| parse_stream(dump_path, range, skip_text))
        .collect::<Result<_>>()?;
    Ok(per_stream.into_iter().flatten().collect())
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
    use bzip2::Compression;
    use bzip2::write::BzEncoder;
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

        let pages1 = parse_stream(dump.path().to_str().unwrap(), &ranges[0], false).unwrap();
        assert_eq!(pages1.len(), 2);
        assert_eq!(pages1[0].title, "Article One");
        assert_eq!(pages1[1].title, "Article Two");

        let pages2 = parse_stream(dump.path().to_str().unwrap(), &ranges[1], false).unwrap();
        assert_eq!(pages2.len(), 1);
        assert_eq!(pages2[0].title, "Article Three");
    }

    #[test]
    fn parse_streams_returns_pages_in_dump_order() {
        let (dump, index) = create_multistream_dump();
        let ranges = parse_multistream_index(
            index.path().to_str().unwrap(),
            dump.path().to_str().unwrap(),
        )
        .unwrap();

        let dump_path = dump.path().to_str().unwrap();
        let pages = parse_streams(dump_path, &ranges, false).unwrap();

        let titles: Vec<&str> = pages.iter().map(|p| p.title.as_str()).collect();
        assert_eq!(titles, vec!["Article One", "Article Two", "Article Three"]);
    }

    #[test]
    fn corrupt_stream_is_an_error() {
        let (dump, _index) = create_multistream_dump();
        let dump_path = dump.path().to_str().unwrap();
        // Start one byte into the first stream, so it is not valid bzip2.
        let bad = StreamRange {
            offset: 1,
            length: 20,
        };
        assert!(parse_stream(dump_path, &bad, false).is_err());
    }

    #[test]
    fn offsets_must_increase_and_stay_inside_the_dump() {
        assert!(ranges_from_offsets(&[0, 100], 200).is_ok());
        assert!(ranges_from_offsets(&[100, 50], 200).is_err());
        assert!(ranges_from_offsets(&[0, 300], 200).is_err());
        assert!(ranges_from_offsets(&[0, 100, 100], 200).is_err());
    }

    #[test]
    fn parse_streams_skip_text() {
        let (dump, index) = create_multistream_dump();
        let ranges = parse_multistream_index(
            index.path().to_str().unwrap(),
            dump.path().to_str().unwrap(),
        )
        .unwrap();

        let dump_path = dump.path().to_str().unwrap();
        let pages = parse_streams(dump_path, &ranges, true).unwrap();

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
