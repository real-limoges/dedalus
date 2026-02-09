//! Comprehensive integration tests for the Dedalus Wikipedia extraction pipeline.
//!
//! This module tests the complete data flow from BZ2-compressed XML input through to
//! CSV extraction and JSON blob generation. Tests are organized into logical sections:
//!
//! - **Parser Tests** -- XML parsing, BZ2 decompression, page type classification
//! - **Index Tests** -- Title-to-ID mapping, redirect chain resolution
//! - **Extraction Tests** -- CSV generation, edge creation, JSON blob output
//! - **Feature Tests** -- Categories, images, external links, infoboxes, see-also sections
//! - **Sharding Tests** -- CSV shard distribution for parallel import
//!
//! # Test Strategy
//!
//! All tests use a shared `sample_xml()` fixture representing a minimal Wikipedia dump
//! with articles, redirects, and special pages. This approach ensures consistency and
//! makes it easy to trace expected behavior across tests.
//!
//! ## Key Patterns
//!
//! - **Fixture creation**: Use `create_bz2_xml(sample_xml())` to get a temp BZ2 file
//! - **Index building**: Always build index before extraction to resolve redirects
//! - **Output validation**: Check both file existence and content correctness
//! - **Statistics**: Verify counters match extracted data (articles, edges, categories)
//! - **Isolation**: Each test uses its own TempDir to avoid cross-test pollution
//!
//! # Sample Data
//!
//! The test fixture includes:
//! - 2 articles: "Rust (programming language)", "Python (programming language)"
//! - 1 redirect: "Rust" → "Rust (programming language)"
//! - 2 special pages: File:Rust logo.svg, Category:Programming languages
//! - Article features: wikilinks, categories, infoboxes, see-also sections, images, external links

use bzip2::write::BzEncoder;
use bzip2::Compression;
use dedalus::extract::run_extraction;
use dedalus::index::WikiIndex;
use dedalus::models::{ArticleBlob, PageType};
use dedalus::parser::WikiReader;
use std::io::Write;
use tempfile::{NamedTempFile, TempDir};

/// Helper: create a BZ2-compressed XML file from a string and return the temp file handle.
///
/// This simulates real Wikipedia dump format by compressing XML with BZ2.
/// The returned NamedTempFile keeps the file alive until it goes out of scope.
fn create_bz2_xml(xml: &str) -> NamedTempFile {
    let mut encoder = BzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(xml.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();

    let mut tmp = NamedTempFile::new().unwrap();
    tmp.write_all(&compressed).unwrap();
    tmp.flush().unwrap();
    tmp
}

/// Sample Wikipedia XML with articles, redirects, special pages, categories,
/// infoboxes, see-also sections, images, and external links.
fn sample_xml() -> &'static str {
    r#"<mediawiki>
        <page>
            <title>Rust (programming language)</title>
            <ns>0</ns>
            <id>1</id>
            <revision>
                <id>100</id>
                <timestamp>2024-01-15T10:30:00Z</timestamp>
                <text>{{Infobox programming language
| name = Rust
| designer = Graydon Hoare
}}
Rust is a systems programming language. See also [[Python (programming language)]] and [[C++|C plus plus]].

It was developed by [[Mozilla]].

[[File:Rust logo.svg|thumb|The Rust logo]]

[https://www.rust-lang.org Official website]

== History ==
Rust was first announced in 2010.

== See also ==
* [[Python (programming language)]]

== References ==
Some refs.

[[Category:Programming languages]]
[[Category:Systems programming languages]]</text>
            </revision>
        </page>
        <page>
            <title>Python (programming language)</title>
            <ns>0</ns>
            <id>2</id>
            <revision>
                <id>200</id>
                <timestamp>2024-02-20T14:00:00Z</timestamp>
                <text>Python is a high-level language. Related: [[Rust (programming language)]].

[[Category:Programming languages]]</text>
            </revision>
        </page>
        <page>
            <title>Rust</title>
            <ns>0</ns>
            <id>3</id>
            <redirect title="Rust (programming language)" />
            <revision>
                <id>300</id>
                <timestamp>2024-01-01T00:00:00Z</timestamp>
                <text>#REDIRECT [[Rust (programming language)]]</text>
            </revision>
        </page>
        <page>
            <title>File:Rust logo.svg</title>
            <ns>6</ns>
            <id>4</id>
            <revision>
                <id>400</id>
                <timestamp>2024-01-01T00:00:00Z</timestamp>
                <text>File description page</text>
            </revision>
        </page>
        <page>
            <title>Category:Programming languages</title>
            <ns>14</ns>
            <id>5</id>
            <revision>
                <id>500</id>
                <timestamp>2024-01-01T00:00:00Z</timestamp>
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
    assert!(matches!(pages[3].page_type, PageType::Special)); // File: (ns=6)
    assert!(matches!(pages[4].page_type, PageType::Special)); // Category: (ns=14)
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

#[test]
fn parser_reads_namespace() {
    let tmp = create_bz2_xml(sample_xml());
    let reader = WikiReader::new(tmp.path().to_str().unwrap(), true).unwrap();
    let pages: Vec<_> = reader.collect();

    assert_eq!(pages[0].ns, Some(0)); // Article
    assert_eq!(pages[3].ns, Some(6)); // File
    assert_eq!(pages[4].ns, Some(14)); // Category
}

#[test]
fn parser_reads_timestamp() {
    let tmp = create_bz2_xml(sample_xml());
    let reader = WikiReader::new(tmp.path().to_str().unwrap(), true).unwrap();
    let pages: Vec<_> = reader.collect();

    assert_eq!(pages[0].timestamp.as_deref(), Some("2024-01-15T10:30:00Z"));
    assert_eq!(pages[1].timestamp.as_deref(), Some("2024-02-20T14:00:00Z"));
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
        1000,
        1,
        None,
        false,
        None,
        None,
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
        1000,
        1,
        None,
        false,
        None,
        None,
    )
    .unwrap();

    // Rust -> Python is a valid edge (both exist in index)
    // Python -> Rust is a valid edge
    // Rust -> C++ is invalid (C++ not in index)
    // Rust -> Mozilla is invalid (not in index)
    assert!(stats.edges() >= 1);
    assert!(stats.invalid() >= 1); // C++ or Mozilla links should be invalid
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
        1000,
        1,
        None,
        false,
        None,
        None,
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
    // abstract_text should contain the lead section, not the full text
    assert!(blob.abstract_text.contains("systems programming language"));
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
        1000,
        1,
        None,
        true, // dry_run
        None,
        None,
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
        1000,
        1,
        Some(1), // limit to 1 page
        true,
        None,
        None,
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
        1000,
        1,
        None,
        false,
        None,
        None,
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
        1000,
        1,
        None,
        false,
        None,
        None,
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
        // type should be LINKS_TO or SEE_ALSO
        let edge_type = record.get(2).unwrap();
        assert!(
            edge_type == "LINKS_TO" || edge_type == "SEE_ALSO",
            "Unexpected edge type: {}",
            edge_type
        );
    }
}

// ---------------------------------------------------------------------------
// New feature tests
// ---------------------------------------------------------------------------

#[test]
fn extraction_produces_category_files() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    let stats = run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        1000,
        1,
        None,
        false,
        None,
        None,
    )
    .unwrap();

    // categories.csv should exist with correct headers
    let cats_path = output_dir.path().join("categories.csv");
    assert!(cats_path.exists());
    let mut rdr = csv::Reader::from_path(&cats_path).unwrap();
    let headers = rdr.headers().unwrap();
    assert_eq!(headers.get(0).unwrap(), "id:ID(Category)");
    assert_eq!(headers.get(1).unwrap(), "name");
    assert_eq!(headers.get(2).unwrap(), ":LABEL");

    // article_categories.csv should exist
    let cat_edges_path = output_dir.path().join("article_categories.csv");
    assert!(cat_edges_path.exists());
    let mut rdr = csv::Reader::from_path(&cat_edges_path).unwrap();
    let headers = rdr.headers().unwrap();
    assert_eq!(headers.get(0).unwrap(), ":START_ID");
    assert_eq!(headers.get(1).unwrap(), ":END_ID(Category)");
    assert_eq!(headers.get(2).unwrap(), ":TYPE");

    // Should have found categories
    assert!(stats.categories() >= 2); // "Programming languages" + "Systems programming languages"
    assert!(stats.category_edges() >= 3); // Rust has 2, Python has 1
}

#[test]
fn extraction_produces_images_csv() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    let stats = run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        1000,
        1,
        None,
        false,
        None,
        None,
    )
    .unwrap();

    // Check image nodes file
    let image_nodes_path = output_dir.path().join("image_nodes.csv");
    assert!(image_nodes_path.exists());
    let mut rdr = csv::Reader::from_path(&image_nodes_path).unwrap();
    let headers = rdr.headers().unwrap();
    assert_eq!(headers.get(0).unwrap(), "id:ID(Image)");
    assert_eq!(headers.get(1).unwrap(), "filename");

    // Check article-images relationship file
    let article_images_path = output_dir.path().join("article_images.csv");
    assert!(article_images_path.exists());
    let mut rdr = csv::Reader::from_path(&article_images_path).unwrap();
    let headers = rdr.headers().unwrap();
    assert_eq!(headers.get(0).unwrap(), ":START_ID");
    assert_eq!(headers.get(1).unwrap(), ":END_ID(Image)");

    assert!(stats.images() >= 1); // Rust logo.svg
}

#[test]
fn extraction_produces_external_links_csv() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    let stats = run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        1000,
        1,
        None,
        false,
        None,
        None,
    )
    .unwrap();

    // Check external link nodes file
    let extlink_nodes_path = output_dir.path().join("external_link_nodes.csv");
    assert!(extlink_nodes_path.exists());
    let mut rdr = csv::Reader::from_path(&extlink_nodes_path).unwrap();
    let headers = rdr.headers().unwrap();
    assert_eq!(headers.get(0).unwrap(), "id:ID(ExternalLink)");
    assert_eq!(headers.get(1).unwrap(), "url");

    // Check article-external-links relationship file
    let article_extlinks_path = output_dir.path().join("article_external_links.csv");
    assert!(article_extlinks_path.exists());
    let mut rdr = csv::Reader::from_path(&article_extlinks_path).unwrap();
    let headers = rdr.headers().unwrap();
    assert_eq!(headers.get(0).unwrap(), ":START_ID");
    assert_eq!(headers.get(1).unwrap(), ":END_ID(ExternalLink)");

    assert!(stats.external_links() >= 1); // rust-lang.org
}

#[test]
fn blob_contains_enriched_data() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        1000,
        1,
        None,
        false,
        None,
        None,
    )
    .unwrap();

    let shard = 1 % 1000;
    let blob_path = output_dir.path().join(format!("blobs/{:03}/1.json", shard));
    let blob_content = std::fs::read_to_string(&blob_path).unwrap();
    let blob: ArticleBlob = serde_json::from_str(&blob_content).unwrap();

    // Should have categories
    assert!(!blob.categories.is_empty());
    assert!(blob
        .categories
        .contains(&"Programming languages".to_string()));

    // Should have infobox
    assert!(!blob.infoboxes.is_empty());
    assert!(blob.infoboxes[0]
        .infobox_type
        .contains("Infobox programming language"));

    // Should have sections
    assert!(!blob.sections.is_empty());
    assert!(blob.sections.contains(&"History".to_string()));

    // Should have timestamp
    assert!(blob.timestamp.is_some());
    assert_eq!(blob.timestamp.as_deref(), Some("2024-01-15T10:30:00Z"));

    // Should not be disambiguation
    assert!(!blob.is_disambiguation);
}

#[test]
fn extraction_finds_see_also_edges() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    let stats = run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        1000,
        1,
        None,
        false,
        None,
        None,
    )
    .unwrap();

    // Rust has a "See also" section with Python
    assert!(stats.see_also_edges() >= 1);

    // Verify edge types in CSV
    let edges_path = output_dir.path().join("edges.csv");
    let content = std::fs::read_to_string(&edges_path).unwrap();
    assert!(content.contains("SEE_ALSO"));
}

#[test]
fn edges_exclude_namespace_links() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        1000,
        1,
        None,
        false,
        None,
        None,
    )
    .unwrap();

    // edges.csv should not contain Category: or File: links
    let edges_path = output_dir.path().join("edges.csv");
    let content = std::fs::read_to_string(&edges_path).unwrap();
    // Category links would show up as numeric IDs if resolved, but they can't
    // resolve since Category: pages are Special. More importantly, they should
    // be skipped by the namespace filter, not counted as invalid links.
    // Just verify the file is parseable and only has valid edge types.
    let mut rdr = csv::Reader::from_reader(content.as_bytes());
    for record in rdr.records() {
        let record = record.unwrap();
        let edge_type = record.get(2).unwrap();
        assert!(
            edge_type == "LINKS_TO" || edge_type == "SEE_ALSO",
            "Unexpected edge type: {}",
            edge_type
        );
    }
}

// ---------------------------------------------------------------------------
// CSV sharding tests
// ---------------------------------------------------------------------------

#[test]
fn sharded_csv_produces_numbered_files() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    let stats = run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        1000,
        4, // csv_shards
        None,
        false,
        None,
        None,
    )
    .unwrap();

    assert_eq!(stats.articles(), 2);

    // With csv_shards=4, should produce numbered files, not the single-file names
    assert!(!output_dir.path().join("nodes.csv").exists());
    assert!(!output_dir.path().join("edges.csv").exists());

    // Should have 4 shard files for each CSV type
    for base in &[
        "nodes",
        "edges",
        "categories",
        "article_categories",
        "image_nodes",
        "article_images",
        "external_link_nodes",
        "article_external_links",
    ] {
        for shard in 0..4u32 {
            let path = output_dir.path().join(format!("{}_{:03}.csv", base, shard));
            assert!(path.exists(), "Missing shard file: {:?}", path);
        }
    }

    // Each shard file should have a header
    for shard in 0..4u32 {
        let path = output_dir.path().join(format!("nodes_{:03}.csv", shard));
        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        assert!(lines[0].contains("id:ID"), "Shard {} missing header", shard);
    }

    // All node records across shards should total 2 articles
    let mut total_records = 0;
    for shard in 0..4u32 {
        let path = output_dir.path().join(format!("nodes_{:03}.csv", shard));
        let mut rdr = csv::Reader::from_path(&path).unwrap();
        total_records += rdr.records().count();
    }
    assert_eq!(total_records, 2, "Total node records across shards");
}

#[test]
fn single_csv_shard_produces_original_filenames() {
    let tmp = create_bz2_xml(sample_xml());
    let output_dir = TempDir::new().unwrap();
    let index = WikiIndex::build(tmp.path().to_str().unwrap()).unwrap();

    run_extraction(
        tmp.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
        &index,
        1000,
        1, // csv_shards = 1 (default)
        None,
        false,
        None,
        None,
    )
    .unwrap();

    // With csv_shards=1, should produce original filenames
    assert!(output_dir.path().join("nodes.csv").exists());
    assert!(output_dir.path().join("edges.csv").exists());
    assert!(output_dir.path().join("categories.csv").exists());
    assert!(output_dir.path().join("article_categories.csv").exists());
    assert!(output_dir.path().join("image_nodes.csv").exists());
    assert!(output_dir.path().join("article_images.csv").exists());
    assert!(output_dir.path().join("external_link_nodes.csv").exists());
    assert!(output_dir
        .path()
        .join("article_external_links.csv")
        .exists());

    // Should NOT have numbered files
    assert!(!output_dir.path().join("nodes_000.csv").exists());
}
