//! Tests for CSV shard merging and deduplication.
//!
//! This module validates the `merge::merge_csv_shards()` function, which combines
//! numbered CSV files (e.g., `nodes_000.csv`, `nodes_001.csv`) into single merged files
//! suitable for neo4j-admin bulk import.
//!
//! # Merge Strategy
//!
//! The merge process handles all 8 CSV types with special deduplication for:
//! - **categories.csv** -- Deduplicated by category name (ID field)
//! - **image_nodes.csv** -- Deduplicated by filename (ID field)
//! - **external_link_nodes.csv** -- Deduplicated by URL (ID field)
//!
//! Other CSV types (nodes, edges, article relationships) are simply concatenated
//! because they don't have cross-shard duplicates by design (page_id sharding).
//!
//! # Test Fixtures
//!
//! Tests use the `create_test_shard()` helper to generate realistic CSV shards with
//! intentional duplicates across shards to validate that deduplication works correctly.
//! The helper creates proper CSV headers and data rows matching Neo4j import format.
//!
//! # Typical Workflow
//!
//! 1. Extract with `--csv-shards 8` to get 8 shard files per CSV type (1.62x faster)
//! 2. Run `dedalus merge-csvs` to produce single files (<5 min overhead)
//! 3. Import using `--admin-import` for 10-100x faster bulk loading
//!
//! This hybrid workflow provides the best of both: fast extraction (parallel shards)
//! and fast import (neo4j-admin requires single files).

use anyhow::Result;
use dedalus::merge;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use tempfile::TempDir;

/// Helper to create a test CSV shard file.
///
/// Creates a numbered CSV shard (e.g., `nodes_003.csv`) with the given header and rows.
/// This simulates the output format from `--csv-shards N` extraction.
fn create_test_shard(
    dir: &Path,
    base_name: &str,
    shard: u32,
    header: &str,
    rows: &[&str],
) -> Result<()> {
    let path = dir.join(format!("{}_{:03}.csv", base_name, shard));
    let mut file = File::create(path)?;
    writeln!(file, "{}", header)?;
    for row in rows {
        writeln!(file, "{}", row)?;
    }
    Ok(())
}

#[test]
fn test_merge_csvs_with_category_deduplication() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let output_dir = temp_dir.path().to_str().unwrap();

    // Create all 6 CSV types with 2 shards each
    // Nodes - no duplicates
    create_test_shard(
        temp_dir.path(),
        "nodes",
        0,
        "id:ID,title,:LABEL",
        &["1,Article_One,Article", "2,Article_Two,Article"],
    )?;
    create_test_shard(
        temp_dir.path(),
        "nodes",
        1,
        "id:ID,title,:LABEL",
        &["3,Article_Three,Article", "4,Article_Four,Article"],
    )?;

    // Edges - no duplicates
    create_test_shard(
        temp_dir.path(),
        "edges",
        0,
        ":START_ID,:END_ID,:TYPE",
        &["1,2,LINKS_TO", "1,3,LINKS_TO"],
    )?;
    create_test_shard(
        temp_dir.path(),
        "edges",
        1,
        ":START_ID,:END_ID,:TYPE",
        &["3,4,LINKS_TO", "4,1,LINKS_TO"],
    )?;

    // Categories - WITH DUPLICATES (Science appears in both shards)
    create_test_shard(
        temp_dir.path(),
        "categories",
        0,
        "id:ID(Category),name,:LABEL",
        &["Science,Science,Category", "Math,Math,Category"],
    )?;
    create_test_shard(
        temp_dir.path(),
        "categories",
        1,
        "id:ID(Category),name,:LABEL",
        &[
            "Science,Science,Category",
            "History,History,Category",
            "Biology,Biology,Category",
        ],
    )?;

    // Article categories
    create_test_shard(
        temp_dir.path(),
        "article_categories",
        0,
        ":START_ID,:END_ID(Category),:TYPE",
        &["1,Science,HAS_CATEGORY", "2,Math,HAS_CATEGORY"],
    )?;
    create_test_shard(
        temp_dir.path(),
        "article_categories",
        1,
        ":START_ID,:END_ID(Category),:TYPE",
        &["3,History,HAS_CATEGORY", "4,Biology,HAS_CATEGORY"],
    )?;

    // Image nodes - WITH DUPLICATES (image1.jpg appears in both shards)
    create_test_shard(
        temp_dir.path(),
        "image_nodes",
        0,
        "id:ID(Image),filename,:LABEL",
        &["image1.jpg,image1.jpg,Image", "image2.png,image2.png,Image"],
    )?;
    create_test_shard(
        temp_dir.path(),
        "image_nodes",
        1,
        "id:ID(Image),filename,:LABEL",
        &["image1.jpg,image1.jpg,Image", "image3.gif,image3.gif,Image"],
    )?;

    // Article images
    create_test_shard(
        temp_dir.path(),
        "article_images",
        0,
        ":START_ID,:END_ID(Image),:TYPE",
        &["1,image1.jpg,HAS_IMAGE", "2,image2.png,HAS_IMAGE"],
    )?;
    create_test_shard(
        temp_dir.path(),
        "article_images",
        1,
        ":START_ID,:END_ID(Image),:TYPE",
        &["3,image3.gif,HAS_IMAGE", "4,image1.jpg,HAS_IMAGE"],
    )?;

    // External link nodes - WITH DUPLICATES (example.com appears in both shards)
    create_test_shard(
        temp_dir.path(),
        "external_link_nodes",
        0,
        "id:ID(ExternalLink),url,:LABEL",
        &["https://example.com,https://example.com,ExternalLink"],
    )?;
    create_test_shard(
        temp_dir.path(),
        "external_link_nodes",
        1,
        "id:ID(ExternalLink),url,:LABEL",
        &[
            "https://example.com,https://example.com,ExternalLink",
            "https://example.org,https://example.org,ExternalLink",
            "https://example.net,https://example.net,ExternalLink",
        ],
    )?;

    // Article external links
    create_test_shard(
        temp_dir.path(),
        "article_external_links",
        0,
        ":START_ID,:END_ID(ExternalLink),:TYPE",
        &["1,https://example.com,HAS_LINK"],
    )?;
    create_test_shard(
        temp_dir.path(),
        "article_external_links",
        1,
        ":START_ID,:END_ID(ExternalLink),:TYPE",
        &[
            "3,https://example.org,HAS_LINK",
            "4,https://example.net,HAS_LINK",
        ],
    )?;

    // Run merge
    merge::merge_csv_shards(output_dir)?;

    // Verify merged files exist
    assert!(temp_dir.path().join("nodes.csv").exists());
    assert!(temp_dir.path().join("edges.csv").exists());
    assert!(temp_dir.path().join("categories.csv").exists());
    assert!(temp_dir.path().join("article_categories.csv").exists());
    assert!(temp_dir.path().join("image_nodes.csv").exists());
    assert!(temp_dir.path().join("article_images.csv").exists());
    assert!(temp_dir.path().join("external_link_nodes.csv").exists());
    assert!(temp_dir.path().join("article_external_links.csv").exists());

    // Verify nodes - should have 4 data rows + 1 header = 5 lines
    let nodes_content = fs::read_to_string(temp_dir.path().join("nodes.csv"))?;
    assert_eq!(nodes_content.lines().count(), 5);
    assert!(nodes_content.contains("Article_One"));
    assert!(nodes_content.contains("Article_Four"));

    // Verify edges - should have 4 data rows + 1 header = 5 lines
    let edges_content = fs::read_to_string(temp_dir.path().join("edges.csv"))?;
    assert_eq!(edges_content.lines().count(), 5);

    // Verify categories - should have DEDUPLICATED to 4 unique categories (Science, Math, History, Biology)
    let categories_content = fs::read_to_string(temp_dir.path().join("categories.csv"))?;
    let category_lines: Vec<&str> = categories_content.lines().collect();

    // 1 header + 4 unique categories = 5 lines total
    assert_eq!(
        category_lines.len(),
        5,
        "Expected 5 lines (1 header + 4 unique categories)"
    );

    // Count occurrences of "Science" - should appear exactly once (excluding header)
    let science_count = category_lines
        .iter()
        .skip(1) // Skip header
        .filter(|line| line.starts_with("Science,"))
        .count();
    assert_eq!(
        science_count, 1,
        "Science should appear exactly once after deduplication"
    );

    // Verify all expected categories are present
    assert!(
        categories_content.contains("Science,Science,Category"),
        "Science category should be present"
    );
    assert!(
        categories_content.contains("Math,Math,Category"),
        "Math category should be present"
    );
    assert!(
        categories_content.contains("History,History,Category"),
        "History category should be present"
    );
    assert!(
        categories_content.contains("Biology,Biology,Category"),
        "Biology category should be present"
    );

    // Verify article_categories - should have 4 data rows + 1 header = 5 lines
    let article_categories_content =
        fs::read_to_string(temp_dir.path().join("article_categories.csv"))?;
    assert_eq!(article_categories_content.lines().count(), 5);

    // Verify image_nodes - should have DEDUPLICATED to 3 unique images (image1.jpg, image2.png, image3.gif)
    let image_nodes_content = fs::read_to_string(temp_dir.path().join("image_nodes.csv"))?;
    let image_lines: Vec<&str> = image_nodes_content.lines().collect();
    assert_eq!(
        image_lines.len(),
        4,
        "Expected 4 lines (1 header + 3 unique images)"
    );

    // Count occurrences of "image1.jpg" - should appear exactly once
    let image1_count = image_lines
        .iter()
        .skip(1)
        .filter(|line| line.starts_with("image1.jpg,"))
        .count();
    assert_eq!(
        image1_count, 1,
        "image1.jpg should appear exactly once after deduplication"
    );

    // Verify article_images - should have 4 data rows + 1 header = 5 lines
    let article_images_content = fs::read_to_string(temp_dir.path().join("article_images.csv"))?;
    assert_eq!(article_images_content.lines().count(), 5);

    // Verify external_link_nodes - should have DEDUPLICATED to 3 unique links
    let extlink_nodes_content =
        fs::read_to_string(temp_dir.path().join("external_link_nodes.csv"))?;
    let extlink_lines: Vec<&str> = extlink_nodes_content.lines().collect();
    assert_eq!(
        extlink_lines.len(),
        4,
        "Expected 4 lines (1 header + 3 unique external links)"
    );

    // Count occurrences of "example.com" - should appear exactly once
    let example_com_count = extlink_lines
        .iter()
        .skip(1)
        .filter(|line| line.starts_with("https://example.com,"))
        .count();
    assert_eq!(
        example_com_count, 1,
        "example.com should appear exactly once after deduplication"
    );

    // Verify article_external_links - should have 3 data rows + 1 header = 4 lines
    let article_extlinks_content =
        fs::read_to_string(temp_dir.path().join("article_external_links.csv"))?;
    assert_eq!(article_extlinks_content.lines().count(), 4);

    Ok(())
}

#[test]
fn test_merge_csvs_handles_many_shards() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let output_dir = temp_dir.path().to_str().unwrap();

    // Create 8 shards (typical for production)
    for shard in 0..8 {
        let node_row = format!("{},Article{},Article", shard * 10, shard);
        create_test_shard(
            temp_dir.path(),
            "nodes",
            shard,
            "id:ID,title,:LABEL",
            &[&node_row],
        )?;

        create_test_shard(
            temp_dir.path(),
            "edges",
            shard,
            ":START_ID,:END_ID,:TYPE",
            &[],
        )?;

        // Create categories with overlapping "Common" category across all shards
        let cat_row = format!("Cat{},Cat{},Category", shard, shard);
        create_test_shard(
            temp_dir.path(),
            "categories",
            shard,
            "id:ID(Category),name,:LABEL",
            &["Common,Common,Category", &cat_row],
        )?;

        create_test_shard(
            temp_dir.path(),
            "article_categories",
            shard,
            ":START_ID,:END_ID(Category),:TYPE",
            &[],
        )?;

        create_test_shard(
            temp_dir.path(),
            "image_nodes",
            shard,
            "id:ID(Image),filename,:LABEL",
            &[],
        )?;

        create_test_shard(
            temp_dir.path(),
            "article_images",
            shard,
            ":START_ID,:END_ID(Image),:TYPE",
            &[],
        )?;

        create_test_shard(
            temp_dir.path(),
            "external_link_nodes",
            shard,
            "id:ID(ExternalLink),url,:LABEL",
            &[],
        )?;

        create_test_shard(
            temp_dir.path(),
            "article_external_links",
            shard,
            ":START_ID,:END_ID(ExternalLink),:TYPE",
            &[],
        )?;
    }

    // Run merge
    merge::merge_csv_shards(output_dir)?;

    // Verify categories deduplication worked - "Common" should appear only once
    let categories_content = fs::read_to_string(temp_dir.path().join("categories.csv"))?;
    let category_lines: Vec<&str> = categories_content.lines().collect();

    // Count "Common" occurrences (excluding header)
    let common_count = category_lines
        .iter()
        .skip(1)
        .filter(|line| line.starts_with("Common,"))
        .count();

    assert_eq!(
        common_count, 1,
        "Common category should appear exactly once despite being in 8 shards"
    );

    // Should have 1 header + 1 Common + 8 unique Cat0-Cat7 = 10 lines
    assert_eq!(
        category_lines.len(),
        10,
        "Expected 10 lines (1 header + 1 Common + 8 unique categories)"
    );

    Ok(())
}
