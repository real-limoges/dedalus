//! Tests for CSV shard merging and deduplication.
//!
//! `merge::merge_csv_shards()` and `merge::merge_detected_shards()` combine numbered
//! CSV files (`nodes_000.csv`, `nodes_001.csv`, ...) into the single files that
//! `dedalus load` and `dedalus analytics` read.
//!
//! # Merge Strategy
//!
//! The merge handles all 8 CSV types. Three of them are deduplicated on their ID field:
//! - `categories.csv` by category name
//! - `image_nodes.csv` by filename
//! - `external_link_nodes.csv` by URL
//!
//! The rest (nodes, edges, article relationships) are concatenated. Rows are sharded
//! by `page_id`, so those files have no duplicates across shards.
//!
//! # Test Fixtures
//!
//! `create_test_shard()` writes shards with the same typed-column headers that
//! extraction writes. The fixtures repeat some entities across shards on purpose,
//! so the tests can count them after the merge.
//!
//! # Typical Workflow
//!
//! 1. Extract with `--csv-shards 8` to get 8 shard files per CSV type (1.62x faster).
//! 2. Run `dedalus merge-csvs` to produce single files (under 5 minutes of overhead).
//! 3. Run `dedalus load`, which reads only the merged files.

use anyhow::Result;
use dedalus::merge;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use tempfile::TempDir;

/// Writes a numbered CSV shard (`nodes_003.csv`, say) with the given header and rows,
/// in the layout that extraction with `--csv-shards N` produces.
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

    // All 8 CSV types, 2 shards each.
    // Nodes: no duplicates.
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

    // Edges: no duplicates.
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

    // Categories: Science appears in both shards.
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

    // Image nodes: image1.jpg appears in both shards.
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

    // External link nodes: example.com appears in both shards.
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

    merge::merge_csv_shards(output_dir, 2)?;

    assert!(temp_dir.path().join("nodes.csv").exists());
    assert!(temp_dir.path().join("edges.csv").exists());
    assert!(temp_dir.path().join("categories.csv").exists());
    assert!(temp_dir.path().join("article_categories.csv").exists());
    assert!(temp_dir.path().join("image_nodes.csv").exists());
    assert!(temp_dir.path().join("article_images.csv").exists());
    assert!(temp_dir.path().join("external_link_nodes.csv").exists());
    assert!(temp_dir.path().join("article_external_links.csv").exists());

    // Nodes are concatenated: 4 rows plus the header.
    let nodes_content = fs::read_to_string(temp_dir.path().join("nodes.csv"))?;
    assert_eq!(nodes_content.lines().count(), 5);
    assert!(nodes_content.contains("Article_One"));
    assert!(nodes_content.contains("Article_Four"));

    // Edges too: 4 rows plus the header.
    let edges_content = fs::read_to_string(temp_dir.path().join("edges.csv"))?;
    assert_eq!(edges_content.lines().count(), 5);

    // Categories collapse to 4: Science, Math, History and Biology.
    let categories_content = fs::read_to_string(temp_dir.path().join("categories.csv"))?;
    let category_lines: Vec<&str> = categories_content.lines().collect();

    assert_eq!(
        category_lines.len(),
        5,
        "Expected 5 lines (1 header + 4 unique categories)"
    );

    // The shared category survives exactly once.
    let science_count = category_lines
        .iter()
        .skip(1) // The header
        .filter(|line| line.starts_with("Science,"))
        .count();
    assert_eq!(
        science_count, 1,
        "Science should appear exactly once after deduplication"
    );

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

    // Article categories are concatenated: 4 rows plus the header.
    let article_categories_content =
        fs::read_to_string(temp_dir.path().join("article_categories.csv"))?;
    assert_eq!(article_categories_content.lines().count(), 5);

    // Images collapse to 3: image1.jpg, image2.png and image3.gif.
    let image_nodes_content = fs::read_to_string(temp_dir.path().join("image_nodes.csv"))?;
    let image_lines: Vec<&str> = image_nodes_content.lines().collect();
    assert_eq!(
        image_lines.len(),
        4,
        "Expected 4 lines (1 header + 3 unique images)"
    );

    let image1_count = image_lines
        .iter()
        .skip(1)
        .filter(|line| line.starts_with("image1.jpg,"))
        .count();
    assert_eq!(
        image1_count, 1,
        "image1.jpg should appear exactly once after deduplication"
    );

    // Article images are concatenated: 4 rows plus the header.
    let article_images_content = fs::read_to_string(temp_dir.path().join("article_images.csv"))?;
    assert_eq!(article_images_content.lines().count(), 5);

    // External links collapse to 3.
    let extlink_nodes_content =
        fs::read_to_string(temp_dir.path().join("external_link_nodes.csv"))?;
    let extlink_lines: Vec<&str> = extlink_nodes_content.lines().collect();
    assert_eq!(
        extlink_lines.len(),
        4,
        "Expected 4 lines (1 header + 3 unique external links)"
    );

    let example_com_count = extlink_lines
        .iter()
        .skip(1)
        .filter(|line| line.starts_with("https://example.com,"))
        .count();
    assert_eq!(
        example_com_count, 1,
        "example.com should appear exactly once after deduplication"
    );

    // Article external links are concatenated: 3 rows plus the header.
    let article_extlinks_content =
        fs::read_to_string(temp_dir.path().join("article_external_links.csv"))?;
    assert_eq!(article_extlinks_content.lines().count(), 4);

    Ok(())
}

#[test]
fn test_merge_csvs_handles_many_shards() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let output_dir = temp_dir.path().to_str().unwrap();

    // 8 shards, the usual count for a real run.
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

        // Every shard has "Common" and one category of its own.
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

    // The merge counts the shards itself.
    assert_eq!(merge::merge_detected_shards(output_dir)?, 8);

    let categories_content = fs::read_to_string(temp_dir.path().join("categories.csv"))?;
    let category_lines: Vec<&str> = categories_content.lines().collect();

    let common_count = category_lines
        .iter()
        .skip(1)
        .filter(|line| line.starts_with("Common,"))
        .count();

    assert_eq!(
        common_count, 1,
        "Common category should appear exactly once despite being in 8 shards"
    );

    // The header, Common, and Cat0 through Cat7.
    assert_eq!(
        category_lines.len(),
        10,
        "Expected 10 lines (1 header + 1 Common + 8 unique categories)"
    );

    Ok(())
}
