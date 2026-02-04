use bzip2::write::BzEncoder;
use bzip2::Compression;
use dedalus::extract::run_extraction;
use dedalus::index::WikiIndex;
use dedalus::models::{ArticleBlob, PageType};
use dedalus::parser::WikiReader;
use std::io::Write;
use tempfile::{NamedTempFile, TempDir};

/// Helper: create a BZ2-compressed XML file from a string and return the temp file handle.
fn create_bz2_xml(xml: &str) -> NamedTempFile {
    let mut encoder = BzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(xml.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();

    let mut tmp = NamedTempFile::new().unwrap();
    tmp.write_all(&compressed).unwrap();
    tmp.flush().unwrap();
    tmp
}

/// Sample Wikipedia XML with articles, redirects, and special pages.
fn sample_xml() -> &'static str {
    r#"<mediawiki>
        <page>
            <title>Rust (programming language)</title>
            <id>1</id>
            <revision>
                <id>100</id>
                <text>Rust is a systems programming language. See also [[Python (programming language)]] and [[C++|C plus plus]].</text>
            </revision>
        </page>
        <page>
            <title>Python (programming language)</title>
            <id>2</id>
            <revision>
                <id>200</id>
                <text>Python is a high-level language. Related: [[Rust (programming language)]].</text>
            </revision>
        </page>
        <page>
            <title>Rust</title>
            <id>3</id>
            <redirect title="Rust (programming language)" />
            <revision>
                <id>300</id>
                <text>#REDIRECT [[Rust (programming language)]]</text>
            </revision>
        </page>
        <page>
            <title>File:Rust logo.svg</title>
            <id>4</id>
            <revision>
                <id>400</id>
                <text>File description page</text>
            </revision>
        </page>
        <page>
            <title>Category:Programming languages</title>
            <id>5</id>
            <revision>
                <id>500</id>
                <text>Category page</text>
            </revision>
        </page>
    </mediawiki>"#
}

// ---------------------------------------------------------------------------
// Parser integration tests
// ---------------------------------------------------------------------------

#[test]
fn parser_reads_all_pages() {
    let tmp = create_bz2_xml(sample_xml());
    let reader = WikiReader::new(tmp.path().to_str().unwrap(), true).unwrap();
    let pages: Vec<_> = reader.collect();
    assert_eq!(pages.len(), 5);
}

#[test]
fn parser_classifies_page_types() {
    let tmp = create_bz2_xml(sample_xml());
    let reader = WikiReader::new(tmp.path().to_str().unwrap(), true).unwrap();
    let pages: Vec<_> = reader.collect();

    assert!(matches!(pages[0].page_type, PageType::Article)); // Rust (programming language)
    assert!(matches!(pages[1].page_type, PageType::Article)); // Python (programming language)
    assert!(matches!(pages[2].page_type, PageType::Redirect(_))); // Rust -> redirect
    assert!(matches!(pages[3].page_type, PageType::Special)); // File:
    assert!(matches!(pages[4].page_type, PageType::Special)); // Category:
}

#[test]
fn parser_reads_text_when_not_skipped() {
    let tmp = create_bz2_xml(sample_xml());
    let reader = WikiReader::new(tmp.path().to_str().unwrap(), false).unwrap();
    let pages: Vec<_> = reader.collect();

    assert!(pages[0].text.is_some());
    assert!(pages[0]
        .text
        .as_ref()
        .unwrap()
        .contains("systems programming language"));
}

// ---------------------------------------------------------------------------
// Index integration tests
// ---------------------------------------------------------------------------

#[test]
fn index_builds_from_dump() {
    let tmp = create_bz2_xml(sample_xml());
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    // Direct article lookup
    assert_eq!(index.resolve_id("Rust (programming language)"), Some(1));
    assert_eq!(index.resolve_id("Python (programming language)"), Some(2));
}

#[test]
fn index_resolves_redirects() {
    let tmp = create_bz2_xml(sample_xml());
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    // "Rust" redirects to "Rust (programming language)" which has id 1
    assert_eq!(index.resolve_id("Rust"), Some(1));
}

#[test]
fn index_returns_none_for_special_pages() {
    let tmp = create_bz2_xml(sample_xml());
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    // Special pages (File:, Category:) are not indexed as articles
    assert_eq!(index.resolve_id("File:Rust logo.svg"), None);
    assert_eq!(index.resolve_id("Category:Programming languages"), None);
}

#[test]
fn index_returns_none_for_unknown() {
    let tmp = create_bz2_xml(sample_xml());
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    assert_eq!(index.resolve_id("Nonexistent Article"), None);
}

// ---------------------------------------------------------------------------
// End-to-end extraction tests
// ---------------------------------------------------------------------------

#[test]
fn extraction_produces_csv_files() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    let stats = run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        None,
        false,
    )
    .unwrap();

    // Should have processed the 2 articles (not redirects/special)
    assert_eq!(stats.articles(), 2);

    // nodes.csv should exist with header + article rows
    let nodes_path = output_dir.path().join("nodes.csv");
    assert!(nodes_path.exists());
    let nodes_content = std::fs::read_to_string(&nodes_path).unwrap();
    let lines: Vec<&str> = nodes_content.trim().lines().collect();
    assert!(lines.len() >= 3); // header + 2 articles
    assert!(lines[0].contains("id:ID"));
    assert!(lines[0].contains("title"));
    assert!(lines[0].contains(":LABEL"));

    // edges.csv should exist
    let edges_path = output_dir.path().join("edges.csv");
    assert!(edges_path.exists());
    let edges_content = std::fs::read_to_string(&edges_path).unwrap();
    let edge_lines: Vec<&str> = edges_content.trim().lines().collect();
    assert!(edge_lines[0].contains(":START_ID"));
    assert!(edge_lines[0].contains(":END_ID"));
    assert!(edge_lines[0].contains(":TYPE"));
}

#[test]
fn extraction_creates_edges() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    let stats = run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        None,
        false,
    )
    .unwrap();

    // Rust -> Python is a valid edge (both exist in index)
    // Python -> Rust is a valid edge
    // Rust -> C++ is invalid (C++ not in index)
    assert!(stats.edges() >= 2);
    assert!(stats.invalid() >= 1); // C++ link should be invalid
}

#[test]
fn extraction_writes_json_blobs() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    let stats = run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        None,
        false,
    )
    .unwrap();

    assert!(stats.blobs() >= 1);

    // Check that a blob file exists and is valid JSON
    let shard = 1 % 1000; // page id 1
    let blob_path = output_dir.path().join(format!("blobs/{:03}/1.json", shard));
    assert!(
        blob_path.exists(),
        "Blob file should exist at {:?}",
        blob_path
    );

    let blob_content = std::fs::read_to_string(&blob_path).unwrap();
    let blob: ArticleBlob = serde_json::from_str(&blob_content).unwrap();
    assert_eq!(blob.id, 1);
    assert_eq!(blob.title, "Rust (programming language)");
    assert!(blob.text.contains("systems programming language"));
}

#[test]
fn extraction_dry_run_writes_no_files() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    let stats = run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        None,
        true, // dry_run
    )
    .unwrap();

    // Stats should still be collected
    assert_eq!(stats.articles(), 2);

    // But no blob files should be written
    assert_eq!(stats.blobs(), 0);
    let blobs_dir = output_dir.path().join("blobs");
    assert!(!blobs_dir.exists());
}

#[test]
fn extraction_respects_limit() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    let stats = run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        Some(1), // limit to 1 page
        true,
    )
    .unwrap();

    // With limit=1, at most 1 article should be processed
    // (the limit applies to all page types seen, not just articles)
    assert!(stats.articles() <= 1);
}

#[test]
fn nodes_csv_format_is_neo4j_compatible() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        None,
        false,
    )
    .unwrap();

    let nodes_path = output_dir.path().join("nodes.csv");
    let mut rdr = csv::Reader::from_path(&nodes_path).unwrap();

    let headers = rdr.headers().unwrap();
    assert_eq!(headers.get(0).unwrap(), "id:ID");
    assert_eq!(headers.get(1).unwrap(), "title");
    assert_eq!(headers.get(2).unwrap(), ":LABEL");

    for record in rdr.records() {
        let record = record.unwrap();
        // id should be numeric
        record.get(0).unwrap().parse::<u32>().unwrap();
        // title should be non-empty
        assert!(!record.get(1).unwrap().is_empty());
        // label should be "Page"
        assert_eq!(record.get(2).unwrap(), "Page");
    }
}

#[test]
fn edges_csv_format_is_neo4j_compatible() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        None,
        false,
    )
    .unwrap();

    let edges_path = output_dir.path().join("edges.csv");
    let mut rdr = csv::Reader::from_path(&edges_path).unwrap();

    let headers = rdr.headers().unwrap();
    assert_eq!(headers.get(0).unwrap(), ":START_ID");
    assert_eq!(headers.get(1).unwrap(), ":END_ID");
    assert_eq!(headers.get(2).unwrap(), ":TYPE");

    for record in rdr.records() {
        let record = record.unwrap();
        // start and end IDs should be numeric
        record.get(0).unwrap().parse::<u32>().unwrap();
        record.get(1).unwrap().parse::<u32>().unwrap();
        // type should be LINKS_TO
        assert_eq!(record.get(2).unwrap(), "LINKS_TO");
    }
}
