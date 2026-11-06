//! Streaming XML parser with BZ2 decompression.
//!
//! `PageParser<R>` implements `Iterator<Item = WikiPage>` over any `Read` source
//! using a state machine over `quick-xml` events. `WikiReader` wraps it with BZ2
//! decompression, probing PATH for `lbzip2`/`pbzip2` for parallel decompression.

use crate::models::{PageType, WikiPage};
use anyhow::{Context, Result};
use bzip2::read::MultiBzDecoder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::fs::File;
use std::io::{BufReader, Read};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::str;
use tracing::{info, warn};

#[cfg(test)]
use bzip2::Compression;
#[cfg(test)]
use bzip2::write::BzEncoder;
#[cfg(test)]
use std::io::Write;

enum DecompressSource {
    External(ChildStdout),
    InProcess(MultiBzDecoder<File>),
}

impl Read for DecompressSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::External(r) => r.read(buf),
            Self::InProcess(r) => r.read(buf),
        }
    }
}

/// Generic XML page parser that works with any `Read` source.
/// Extracts `WikiPage` items from a MediaWiki XML stream.
///
/// Iteration ends at end of input or at the first XML or read error. The two
/// look the same to a `for` loop, so callers that must not accept truncated
/// input check `error()` afterwards.
pub struct PageParser<R: Read> {
    reader: Reader<BufReader<R>>,
    buf: Vec<u8>,
    skip_text: bool,
    pub(crate) skip_timestamp: bool,
    reached_eof: bool,
    error: Option<String>,
}

/// The element whose text is currently being read.
#[derive(Clone, Copy, PartialEq)]
enum Field {
    Title,
    Id,
    Ns,
    Timestamp,
    Text,
}

/// Values collected for the `<page>` being parsed. Replaced with a fresh
/// value at every `<page>` boundary, so a malformed page cannot leak its id,
/// redirect target or text into the next one.
#[derive(Default)]
struct PageFields {
    id: Option<u32>,
    title: Option<String>,
    text: Option<String>,
    redirect_target: Option<String>,
    ns: Option<i32>,
    timestamp: Option<String>,
}

impl PageFields {
    /// Builds the page, or `None` if the id or title is missing.
    fn into_page(self) -> Option<WikiPage> {
        let id = self.id?;
        let title = self.title?;
        let page_type = match (self.redirect_target, self.ns) {
            (Some(target), _) => PageType::Redirect(target),
            (None, Some(0)) => PageType::Article,
            (None, Some(_)) => PageType::Special,
            (None, None)
                if title.starts_with("File:")
                    || title.starts_with("Category:")
                    || title.starts_with("Template:") =>
            {
                PageType::Special
            }
            (None, None) => PageType::Article,
        };
        Some(WikiPage {
            id,
            title,
            page_type,
            text: self.text,
            ns: self.ns,
            timestamp: self.timestamp,
        })
    }
}

impl<R: Read> PageParser<R> {
    pub fn new(source: R, skip_text: bool) -> Self {
        let buf_reader = BufReader::with_capacity(256 * 1024, source);
        let mut xml_reader = Reader::from_reader(buf_reader);
        xml_reader.check_end_names(false);
        xml_reader.trim_text(true);
        Self {
            reader: xml_reader,
            buf: Vec::with_capacity(crate::config::BUFREADER_CAPACITY),
            skip_text,
            skip_timestamp: false,
            reached_eof: false,
            error: None,
        }
    }

    pub fn skip_timestamp(mut self, val: bool) -> Self {
        self.skip_timestamp = val;
        self
    }

    /// The XML or read error that ended iteration early, if any.
    #[must_use]
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }

    /// True once the parser has read to the end of its input.
    #[must_use]
    pub fn reached_eof(&self) -> bool {
        self.reached_eof
    }
}

/// Which element's text to capture, given the parser's skip settings.
fn field_for(
    name: &[u8],
    fields: &PageFields,
    skip_text: bool,
    skip_timestamp: bool,
) -> Option<Field> {
    match name {
        b"title" => Some(Field::Title),
        // The first <id> in a page is the page id; later ones belong to
        // the revision and contributor.
        b"id" if fields.id.is_none() => Some(Field::Id),
        b"ns" => Some(Field::Ns),
        b"timestamp" if !skip_timestamp => Some(Field::Timestamp),
        b"text" if !skip_text => Some(Field::Text),
        _ => None,
    }
}

/// The `title` attribute of a `<redirect>` element, with XML entities decoded
/// (`AT&amp;T` becomes `AT&T`).
fn redirect_title(e: &quick_xml::events::BytesStart<'_>) -> Option<String> {
    let attr = e.try_get_attribute("title").ok()??;
    let title = match attr.unescape_value() {
        Ok(value) => value.into_owned(),
        Err(_) => String::from_utf8_lossy(&attr.value).into_owned(),
    };
    Some(title)
}

impl<R: Read> Iterator for PageParser<R> {
    type Item = WikiPage;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reached_eof || self.error.is_some() {
            return None;
        }

        let mut fields = PageFields::default();
        let mut reading: Option<Field> = None;

        loop {
            // quick-xml appends to the buffer, so clear it before every event,
            // including the one after a page was returned.
            self.buf.clear();
            match self.reader.read_event_into(&mut self.buf) {
                Ok(Event::Start(e)) => match e.name().as_ref() {
                    b"page" => {
                        fields = PageFields::default();
                        reading = None;
                    }
                    b"redirect" => fields.redirect_target = redirect_title(&e),
                    name => {
                        reading = field_for(name, &fields, self.skip_text, self.skip_timestamp);
                    }
                },

                Ok(Event::Empty(e)) => {
                    if e.name().as_ref() == b"redirect" {
                        fields.redirect_target = redirect_title(&e);
                    }
                }

                Ok(Event::Text(e)) => match reading {
                    Some(Field::Title) => fields.title = e.unescape().ok().map(|s| s.into_owned()),
                    Some(Field::Id) => {
                        fields.id = str::from_utf8(&e).ok().and_then(|s| s.trim().parse().ok());
                    }
                    Some(Field::Ns) => {
                        fields.ns = str::from_utf8(&e).ok().and_then(|s| s.trim().parse().ok());
                    }
                    Some(Field::Timestamp) => {
                        fields.timestamp = str::from_utf8(&e).ok().map(str::to_string);
                    }
                    Some(Field::Text) => fields.text = e.unescape().ok().map(|s| s.into_owned()),
                    None => {}
                },

                Ok(Event::End(e)) => {
                    if e.name().as_ref() == b"page" {
                        let page = std::mem::take(&mut fields).into_page();
                        reading = None;
                        if page.is_some() {
                            return page;
                        }
                    } else {
                        reading = None;
                    }
                }

                Ok(Event::Eof) => {
                    self.reached_eof = true;
                    return None;
                }

                Err(e) => {
                    let position = self.reader.buffer_position();
                    warn!(position, error = ?e, "XML parse error");
                    self.error = Some(format!("XML parse error at byte {position}: {e}"));
                    return None;
                }

                _ => (),
            }
        }
    }
}

/// High-level Wikipedia dump reader with automatic BZ2 decompression.
///
/// Probes PATH for `lbzip2`/`pbzip2` for parallel decompression; falls back
/// to in-process `MultiBzDecoder`. Implements `Iterator<Item = WikiPage>`.
pub struct WikiReader {
    parser: PageParser<DecompressSource>,
    _child: Option<Child>,
}

fn find_decompressor() -> Option<&'static str> {
    ["lbzip2", "pbzip2"].into_iter().find(|cmd| {
        Command::new(cmd)
            .arg("--help")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok()
    })
}

fn spawn_decompressor(cmd: &str, path: &str) -> Result<Child> {
    Command::new(cmd)
        .arg("-dc")
        .arg(path)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("Failed to spawn {}", cmd))
}

impl WikiReader {
    pub fn new(path: &str, skip_text: bool) -> Result<Self> {
        if !std::path::Path::new(path).exists() {
            return Err(anyhow::anyhow!("Could not open file: {}", path));
        }

        let (source, child): (DecompressSource, Option<Child>) = if let Some(cmd) =
            find_decompressor()
        {
            match spawn_decompressor(cmd, path) {
                Ok(mut child) => {
                    let stdout = child
                        .stdout
                        .take()
                        .ok_or_else(|| anyhow::anyhow!("Failed to capture stdout from {}", cmd))?;
                    info!(decompressor = cmd, "Using external parallel decompressor");
                    (DecompressSource::External(stdout), Some(child))
                }
                Err(e) => {
                    warn!(error = %e, "External decompressor failed, falling back to in-process");
                    let file = File::open(path)
                        .with_context(|| format!("Could not open file: {}", path))?;
                    (DecompressSource::InProcess(MultiBzDecoder::new(file)), None)
                }
            }
        } else {
            let file =
                File::open(path).with_context(|| format!("Could not open file: {}", path))?;
            (DecompressSource::InProcess(MultiBzDecoder::new(file)), None)
        };

        let parser = PageParser::new(source, skip_text);

        Ok(Self {
            parser,
            _child: child,
        })
    }

    pub fn skip_timestamp(mut self, val: bool) -> Self {
        self.parser.skip_timestamp = val;
        self
    }

    /// Constructor that forces in-process decompression, bypassing external tool detection.
    #[cfg(test)]
    fn new_inprocess(path: &str, skip_text: bool) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("Could not open file: {}", path))?;
        let source = DecompressSource::InProcess(MultiBzDecoder::new(file));
        let parser = PageParser::new(source, skip_text);

        Ok(Self {
            parser,
            _child: None,
        })
    }
}

impl WikiReader {
    /// Reports how iteration ended. Call it after the loop over pages.
    ///
    /// Fails if the XML was malformed or unreadable, or if the external
    /// decompressor exited with an error after the parser reached end of input
    /// (a truncated or corrupt dump looks like a clean end to the parser). If
    /// the caller stopped early, the decompressor is stopped and that is not
    /// an error.
    pub fn finish(mut self) -> Result<()> {
        if let Some(error) = self.parser.error() {
            anyhow::bail!("Dump could not be read to the end: {error}");
        }
        let Some(mut child) = self._child.take() else {
            return Ok(());
        };
        if !self.parser.reached_eof() {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(());
        }
        let status = child
            .wait()
            .context("Failed to wait for the decompressor")?;
        if !status.success() {
            anyhow::bail!(
                "Decompressor exited with {status}; the dump may be truncated or corrupt"
            );
        }
        Ok(())
    }
}

impl Drop for WikiReader {
    fn drop(&mut self) {
        if let Some(ref mut child) = self._child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Iterator for WikiReader {
    type Item = WikiPage;

    fn next(&mut self) -> Option<Self::Item> {
        self.parser.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn create_bz2_xml(xml: &str) -> NamedTempFile {
        let mut encoder = BzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(xml.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(&compressed).unwrap();
        tmp.flush().unwrap();
        tmp
    }

    #[test]
    fn parse_single_article() {
        let xml = r#"<mediawiki>
            <page>
                <title>Rust</title>
                <id>1</id>
                <revision>
                    <id>100</id>
                    <text>Rust is a systems programming language.</text>
                </revision>
            </page>
        </mediawiki>"#;

        let tmp = create_bz2_xml(xml);
        let reader = WikiReader::new(tmp.path().to_str().unwrap(), false).unwrap();
        let pages: Vec<_> = reader.collect();

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].id, 1);
        assert_eq!(pages[0].title, "Rust");
        assert!(matches!(pages[0].page_type, PageType::Article));
        assert_eq!(
            pages[0].text.as_deref(),
            Some("Rust is a systems programming language.")
        );
    }

    #[test]
    fn parse_skip_text_mode() {
        let xml = r#"<mediawiki>
            <page>
                <title>Rust</title>
                <id>1</id>
                <revision>
                    <id>100</id>
                    <text>This text should be skipped.</text>
                </revision>
            </page>
        </mediawiki>"#;

        let tmp = create_bz2_xml(xml);
        let reader = WikiReader::new(tmp.path().to_str().unwrap(), true).unwrap();
        let pages: Vec<_> = reader.collect();

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].id, 1);
        assert!(pages[0].text.is_none());
    }

    #[test]
    fn parse_redirect_page() {
        let xml = r#"<mediawiki>
            <page>
                <title>Rust lang</title>
                <id>2</id>
                <redirect title="Rust (programming language)" />
                <revision>
                    <id>200</id>
                    <text>#REDIRECT [[Rust (programming language)]]</text>
                </revision>
            </page>
        </mediawiki>"#;

        let tmp = create_bz2_xml(xml);
        let reader = WikiReader::new(tmp.path().to_str().unwrap(), true).unwrap();
        let pages: Vec<_> = reader.collect();

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].id, 2);
        assert_eq!(pages[0].title, "Rust lang");
        match &pages[0].page_type {
            PageType::Redirect(target) => {
                assert_eq!(target, "Rust (programming language)");
            }
            _ => panic!("Expected Redirect page type"),
        }
    }

    #[test]
    fn classify_special_pages() {
        let xml = r#"<mediawiki>
            <page>
                <title>File:Example.jpg</title>
                <id>10</id>
            </page>
            <page>
                <title>Category:Programming languages</title>
                <id>11</id>
            </page>
            <page>
                <title>Template:Infobox</title>
                <id>12</id>
            </page>
        </mediawiki>"#;

        let tmp = create_bz2_xml(xml);
        let reader = WikiReader::new(tmp.path().to_str().unwrap(), true).unwrap();
        let pages: Vec<_> = reader.collect();

        assert_eq!(pages.len(), 3);
        for page in &pages {
            assert!(
                matches!(page.page_type, PageType::Special),
                "Expected Special for '{}'",
                page.title
            );
        }
    }

    #[test]
    fn parse_multiple_pages() {
        let xml = r#"<mediawiki>
            <page>
                <title>Rust</title>
                <id>1</id>
                <revision><id>100</id><text>Article about Rust.</text></revision>
            </page>
            <page>
                <title>Python</title>
                <id>2</id>
                <revision><id>200</id><text>Article about Python.</text></revision>
            </page>
            <page>
                <title>JavaScript</title>
                <id>3</id>
                <revision><id>300</id><text>Article about JavaScript.</text></revision>
            </page>
        </mediawiki>"#;

        let tmp = create_bz2_xml(xml);
        let reader = WikiReader::new(tmp.path().to_str().unwrap(), false).unwrap();
        let pages: Vec<_> = reader.collect();

        assert_eq!(pages.len(), 3);
        assert_eq!(pages[0].title, "Rust");
        assert_eq!(pages[1].title, "Python");
        assert_eq!(pages[2].title, "JavaScript");
        assert_eq!(pages[0].id, 1);
        assert_eq!(pages[1].id, 2);
        assert_eq!(pages[2].id, 3);
    }

    #[test]
    fn first_id_tag_is_page_id() {
        let xml = r#"<mediawiki>
            <page>
                <title>Test</title>
                <id>42</id>
                <revision>
                    <id>99999</id>
                    <text>Content</text>
                </revision>
            </page>
        </mediawiki>"#;

        let tmp = create_bz2_xml(xml);
        let reader = WikiReader::new(tmp.path().to_str().unwrap(), false).unwrap();
        let pages: Vec<_> = reader.collect();

        assert_eq!(pages[0].id, 42);
    }

    #[test]
    fn parse_empty_dump() {
        let xml = r#"<mediawiki></mediawiki>"#;

        let tmp = create_bz2_xml(xml);
        let reader = WikiReader::new(tmp.path().to_str().unwrap(), false).unwrap();
        let pages: Vec<_> = reader.collect();

        assert!(pages.is_empty());
    }

    #[test]
    fn parse_mixed_page_types() {
        let xml = r#"<mediawiki>
            <page>
                <title>Regular Article</title>
                <id>1</id>
                <revision><id>100</id><text>Content</text></revision>
            </page>
            <page>
                <title>Old Name</title>
                <id>2</id>
                <redirect title="Regular Article" />
                <revision><id>200</id><text>#REDIRECT [[Regular Article]]</text></revision>
            </page>
            <page>
                <title>File:Photo.png</title>
                <id>3</id>
                <revision><id>300</id><text>File description</text></revision>
            </page>
        </mediawiki>"#;

        let tmp = create_bz2_xml(xml);
        let reader = WikiReader::new(tmp.path().to_str().unwrap(), true).unwrap();
        let pages: Vec<_> = reader.collect();

        assert_eq!(pages.len(), 3);
        assert!(matches!(pages[0].page_type, PageType::Article));
        assert!(matches!(pages[1].page_type, PageType::Redirect(_)));
        assert!(matches!(pages[2].page_type, PageType::Special));
    }

    #[test]
    fn parse_unicode_content() {
        let xml = r#"<mediawiki>
            <page>
                <title>日本語</title>
                <id>1</id>
                <revision><id>100</id><text>日本語の記事 with [[リンク]]</text></revision>
            </page>
        </mediawiki>"#;

        let tmp = create_bz2_xml(xml);
        let reader = WikiReader::new(tmp.path().to_str().unwrap(), false).unwrap();
        let pages: Vec<_> = reader.collect();

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].title, "日本語");
        assert!(pages[0].text.as_deref().unwrap().contains("日本語の記事"));
    }

    #[test]
    fn parse_xml_entities_in_title() {
        let xml = r#"<mediawiki>
            <page>
                <title>AT&amp;T</title>
                <id>1</id>
            </page>
        </mediawiki>"#;

        let tmp = create_bz2_xml(xml);
        let reader = WikiReader::new(tmp.path().to_str().unwrap(), true).unwrap();
        let pages: Vec<_> = reader.collect();

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].title, "AT&T");
    }

    #[test]
    fn nonexistent_file_returns_error() {
        let result = WikiReader::new("/nonexistent/path.xml.bz2", false);
        assert!(result.is_err());
    }

    #[test]
    fn parse_with_inprocess_fallback() {
        let xml = r#"<mediawiki>
            <page>
                <title>Test</title>
                <id>1</id>
                <revision><id>100</id><text>Content here.</text></revision>
            </page>
        </mediawiki>"#;

        let tmp = create_bz2_xml(xml);
        let reader = WikiReader::new_inprocess(tmp.path().to_str().unwrap(), false).unwrap();
        let pages: Vec<_> = reader.collect();

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].title, "Test");
        assert_eq!(pages[0].text.as_deref(), Some("Content here."));
    }

    #[test]
    fn page_parser_from_raw_xml() {
        let xml = b"<mediawiki>
            <page>
                <title>Direct</title>
                <id>99</id>
                <revision><id>1</id><text>Hello</text></revision>
            </page>
        </mediawiki>";

        let parser = PageParser::new(&xml[..], false);
        let pages: Vec<_> = parser.collect();

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].id, 99);
        assert_eq!(pages[0].title, "Direct");
        assert_eq!(pages[0].text.as_deref(), Some("Hello"));
    }

    #[test]
    fn malformed_page_does_not_leak_into_the_next() {
        // The first page has no <id>, so it is skipped; its redirect target
        // and text must not be attached to the second page.
        let xml = b"<mediawiki>
            <page>
                <title>Broken</title>
                <ns>0</ns>
                <redirect title=\"Elsewhere\" />
                <revision><text>Broken text</text></revision>
            </page>
            <page>
                <title>Good</title>
                <ns>0</ns>
                <id>2</id>
                <revision><id>20</id><text>Good text</text></revision>
            </page>
        </mediawiki>";

        let pages: Vec<_> = PageParser::new(&xml[..], false).collect();

        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].id, 2);
        assert_eq!(pages[0].page_type, PageType::Article);
        assert_eq!(pages[0].text.as_deref(), Some("Good text"));
    }

    #[test]
    fn redirect_title_entities_are_decoded() {
        let xml = b"<mediawiki><page><title>ATT</title><ns>0</ns><id>1</id>
            <redirect title=\"AT&amp;T\" /></page></mediawiki>";
        let pages: Vec<_> = PageParser::new(&xml[..], true).collect();
        assert_eq!(pages[0].page_type, PageType::Redirect("AT&T".to_string()));
    }

    #[test]
    fn clean_end_has_no_error() {
        let xml = b"<mediawiki><page><title>A</title><id>1</id></page></mediawiki>";
        let mut parser = PageParser::new(&xml[..], true);
        assert_eq!(parser.by_ref().count(), 1);
        assert!(parser.reached_eof());
        assert!(parser.error().is_none());
    }

    #[test]
    fn read_error_is_reported_not_swallowed() {
        struct FailingReader;
        impl Read for FailingReader {
            fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("disk on fire"))
            }
        }

        let mut parser = PageParser::new(FailingReader, true);
        assert_eq!(parser.by_ref().count(), 0);
        assert!(parser.error().is_some());
        assert!(!parser.reached_eof());
    }

    #[test]
    fn finish_reports_a_corrupt_bz2_dump() {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(b"BZh9 this is not really bzip2 data")
            .unwrap();
        tmp.flush().unwrap();

        let mut reader = WikiReader::new_inprocess(tmp.path().to_str().unwrap(), true).unwrap();
        assert_eq!(reader.by_ref().count(), 0);
        assert!(reader.finish().is_err());
    }
}
