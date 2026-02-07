use crate::models::{PageType, WikiPage};
use anyhow::{Context, Result};
use bzip2::read::MultiBzDecoder;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::fs::File;
use std::io::{BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::str;
use tracing::{info, warn};

#[cfg(test)]
use bzip2::write::BzEncoder;
#[cfg(test)]
use bzip2::Compression;
#[cfg(test)]
use std::io::Write;

pub struct WikiReader {
    reader: Reader<BufReader<Box<dyn Read + Send>>>,
    buf: Vec<u8>,
    skip_text: bool,
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

        let (source, child): (Box<dyn Read + Send>, Option<Child>) = if let Some(cmd) =
            find_decompressor()
        {
            match spawn_decompressor(cmd, path) {
                Ok(mut child) => {
                    let stdout = child
                        .stdout
                        .take()
                        .ok_or_else(|| anyhow::anyhow!("Failed to capture stdout from {}", cmd))?;
                    info!(decompressor = cmd, "Using external parallel decompressor");
                    (Box::new(stdout), Some(child))
                }
                Err(e) => {
                    warn!(error = %e, "External decompressor failed, falling back to in-process");
                    let file = File::open(path)
                        .with_context(|| format!("Could not open file: {}", path))?;
                    (Box::new(MultiBzDecoder::new(file)), None)
                }
            }
        } else {
            let file =
                File::open(path).with_context(|| format!("Could not open file: {}", path))?;
            (Box::new(MultiBzDecoder::new(file)), None)
        };

        let reader = BufReader::with_capacity(256 * 1024, source);
        let xml_reader = Reader::from_reader(reader);

        Ok(Self {
            reader: xml_reader,
            buf: Vec::with_capacity(1024),
            skip_text,
            _child: child,
        })
    }

    /// Constructor that forces in-process decompression, bypassing external tool detection.
    #[cfg(test)]
    fn new_inprocess(path: &str, skip_text: bool) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("Could not open file: {}", path))?;
        let source: Box<dyn Read + Send> = Box::new(MultiBzDecoder::new(file));
        let reader = BufReader::with_capacity(256 * 1024, source);
        let xml_reader = Reader::from_reader(reader);

        Ok(Self {
            reader: xml_reader,
            buf: Vec::with_capacity(1024),
            skip_text,
            _child: None,
        })
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
        let mut current_id = None;
        let mut current_title: Option<String> = None;
        let mut current_text: Option<String> = None;
        let mut redirect_target = None;
        let mut current_ns: Option<i32> = None;
        let mut current_timestamp: Option<String> = None;

        let mut in_title = false;
        let mut in_id = false;
        let mut in_text = false;
        let mut in_ns = false;
        let mut in_timestamp = false;

        loop {
            match self.reader.read_event_into(&mut self.buf) {
                Ok(Event::Start(e)) => match e.name().as_ref() {
                    b"page" => {}

                    b"title" => in_title = true,
                    b"id" if current_id.is_none() => in_id = true,
                    b"ns" => in_ns = true,
                    b"timestamp" => in_timestamp = true,
                    b"text" => {
                        if !self.skip_text {
                            in_text = true;
                        }
                    }
                    b"redirect" => {
                        if let Ok(Some(attr)) = e.try_get_attribute("title") {
                            redirect_target =
                                Some(String::from_utf8_lossy(&attr.value).to_string());
                        }
                    }
                    _ => (),
                },

                Ok(Event::Empty(e)) => {
                    if e.name().as_ref() == b"redirect" {
                        if let Ok(Some(attr)) = e.try_get_attribute("title") {
                            redirect_target =
                                Some(String::from_utf8_lossy(&attr.value).to_string());
                        }
                    }
                }

                Ok(Event::Text(e)) => {
                    if in_title {
                        if let Ok(s) = e.unescape() {
                            current_title = Some(s.into_owned());
                        }
                    } else if in_id {
                        let s = String::from_utf8_lossy(&e);
                        current_id = s.trim().parse::<u32>().ok();
                    } else if in_ns {
                        let s = String::from_utf8_lossy(&e);
                        current_ns = s.trim().parse::<i32>().ok();
                    } else if in_timestamp {
                        if let Ok(s) = e.unescape() {
                            current_timestamp = Some(s.into_owned());
                        }
                    } else if in_text {
                        if let Ok(s) = e.unescape() {
                            current_text = Some(s.into_owned());
                        }
                    }
                }

                Ok(Event::End(e)) => match e.name().as_ref() {
                    b"title" => in_title = false,
                    b"id" => in_id = false,
                    b"ns" => in_ns = false,
                    b"timestamp" => in_timestamp = false,
                    b"text" => in_text = false,
                    b"page" => {
                        if let (Some(id), Some(title)) = (current_id, current_title.take()) {
                            let page_type = if let Some(target) = redirect_target.take() {
                                PageType::Redirect(target)
                            } else if let Some(ns) = current_ns {
                                if ns == 0 {
                                    PageType::Article
                                } else {
                                    PageType::Special
                                }
                            } else if title.starts_with("File:")
                                || title.starts_with("Category:")
                                || title.starts_with("Template:")
                            {
                                PageType::Special
                            } else {
                                PageType::Article
                            };

                            return Some(WikiPage {
                                id,
                                title,
                                page_type,
                                text: current_text.take(),
                                ns: current_ns,
                                timestamp: current_timestamp.take(),
                            });
                        }
                    }
                    _ => (),
                },
                Ok(Event::Eof) => return None,
                Err(e) => {
                    eprintln!(
                        "XML Parse Error at position {}: {:?}",
                        self.reader.buffer_position(),
                        e
                    );
                    return None;
                }
                _ => (),
            }
            self.buf.clear();
        }
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
}
