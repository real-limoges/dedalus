use crate::config;
use anyhow::{bail, Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use neo4rs::{query, Graph};
use std::path::Path;
use std::time::Instant;
use tokio::process::Command;
use tracing::{info, warn};

/// Cypher templates. `{file}` is replaced with the CSV URI at runtime.
const CYPHER_LOAD_PAGES: &str = r#"LOAD CSV WITH HEADERS FROM '{file}' AS row
CALL { WITH row
    CREATE (:Page {id: row.`id:ID`, title: row.title})
} IN TRANSACTIONS OF 10000 ROWS;"#;

const CYPHER_LOAD_CATEGORIES: &str = r#"LOAD CSV WITH HEADERS FROM '{file}' AS row
CALL { WITH row
    CREATE (:Category {id: row.`id:ID(Category)`, name: row.name})
} IN TRANSACTIONS OF 10000 ROWS;"#;

const CYPHER_LOAD_EDGES: &str = r#"LOAD CSV WITH HEADERS FROM '{file}' AS row
CALL { WITH row
    MATCH (a:Page {id: row.`:START_ID`}), (b:Page {id: row.`:END_ID`})
    CREATE (a)-[:LINKS_TO]->(b)
} IN TRANSACTIONS OF 10000 ROWS;"#;

const CYPHER_LOAD_ARTICLE_CATEGORIES: &str = r#"LOAD CSV WITH HEADERS FROM '{file}' AS row
CALL { WITH row
    MATCH (p:Page {id: row.`:START_ID`}), (c:Category {id: row.`:END_ID(Category)`})
    CREATE (p)-[:HAS_CATEGORY]->(c)
} IN TRANSACTIONS OF 10000 ROWS;"#;

const CYPHER_LOAD_IMAGES: &str = r#"LOAD CSV WITH HEADERS FROM '{file}' AS row
CALL { WITH row
    MATCH (p:Page {id: row.article_id})
    MERGE (i:Image {filename: row.filename})
    CREATE (p)-[:HAS_IMAGE]->(i)
} IN TRANSACTIONS OF 10000 ROWS;"#;

const CYPHER_LOAD_EXTERNAL_LINKS: &str = r#"LOAD CSV WITH HEADERS FROM '{file}' AS row
CALL { WITH row
    MATCH (p:Page {id: row.article_id})
    MERGE (e:ExternalLink {url: row.url})
    CREATE (p)-[:HAS_LINK]->(e)
} IN TRANSACTIONS OF 10000 ROWS;"#;

const CSV_TYPES: &[&str] = &[
    "nodes",
    "edges",
    "categories",
    "article_categories",
    "images",
    "external_links",
];

pub struct ImportConfig {
    pub output_dir: String,
    pub bolt_uri: String,
    pub import_prefix: String,
    pub max_parallel_edges: usize,
    pub max_parallel_light: usize,
    pub compose_file: Option<String>,
    pub no_docker: bool,
    pub clean: bool,
}

#[derive(Debug)]
enum CsvLayout {
    Single,
    Sharded { count: u32 },
}

impl CsvLayout {
    fn description(&self) -> String {
        match self {
            CsvLayout::Single => "single-file".to_string(),
            CsvLayout::Sharded { count } => format!("sharded ({count} shards)"),
        }
    }
}

pub async fn run_import(mut config: ImportConfig) -> Result<()> {
    let start = Instant::now();

    // Docker volume mounts require absolute paths; canonicalize early.
    config.output_dir = std::fs::canonicalize(&config.output_dir)
        .with_context(|| format!("Output directory does not exist: {}", config.output_dir))?
        .to_string_lossy()
        .to_string();

    let layout = detect_csv_layout(&config.output_dir)?;
    validate_csv_files(&config.output_dir, &layout)?;
    println!();
    println!("==> Detected {} CSV layout", layout.description());

    if !config.no_docker {
        let compose_file = resolve_compose_file(&config)?;
        docker_start(&compose_file, &config).await?;
    }

    println!();
    println!("==> Connecting to Neo4j at {} ...", config.bolt_uri);
    let graph = connect_with_retry(&config).await?;
    println!("    Connected.");

    let mp = MultiProgress::new();

    let pb = mp.add(make_spinner("Creating indexes for import performance ..."));
    run_cypher(
        &graph,
        "CREATE INDEX page_id IF NOT EXISTS FOR (p:Page) ON (p.id);",
    )
    .await?;
    run_cypher(
        &graph,
        "CREATE INDEX category_id IF NOT EXISTS FOR (c:Category) ON (c.id);",
    )
    .await?;
    pb.finish_with_message("Pre-import indexes created.");

    println!();
    println!("==> Loading nodes ...");
    let node_files = csv_files_for("nodes", &layout);
    let cat_files = csv_files_for("categories", &layout);

    let pb_pages = mp.add(make_progress_bar(node_files.len() as u64, "Pages"));
    let pb_cats = mp.add(make_progress_bar(cat_files.len() as u64, "Categories"));

    let (_, _) = tokio::try_join!(
        load_csv_files(
            &graph,
            &node_files,
            &config.import_prefix,
            CYPHER_LOAD_PAGES,
            "pages",
            node_files.len(),
            &pb_pages,
        ),
        load_csv_files(
            &graph,
            &cat_files,
            &config.import_prefix,
            CYPHER_LOAD_CATEGORIES,
            "categories",
            cat_files.len(),
            &pb_cats,
        ),
    )?;

    let page_count = query_count(&graph, "MATCH (p:Page) RETURN count(p) AS cnt").await?;
    let cat_count = query_count(&graph, "MATCH (c:Category) RETURN count(c) AS cnt").await?;
    println!("    Loaded {page_count} pages and {cat_count} categories.");

    println!();
    println!("==> Loading edges ...");
    let edge_files = csv_files_for("edges", &layout);
    let pb_edges = mp.add(make_progress_bar(edge_files.len() as u64, "Edges"));
    load_csv_files(
        &graph,
        &edge_files,
        &config.import_prefix,
        CYPHER_LOAD_EDGES,
        "edges",
        config.max_parallel_edges,
        &pb_edges,
    )
    .await?;
    let edge_count =
        query_count(&graph, "MATCH ()-[r:LINKS_TO]->() RETURN count(r) AS cnt").await?;
    println!("    Loaded {edge_count} edges.");

    println!();
    println!("==> Loading article-category relationships ...");
    let artcat_files = csv_files_for("article_categories", &layout);
    let pb_artcat = mp.add(make_progress_bar(artcat_files.len() as u64, "Art-Cats"));
    load_csv_files(
        &graph,
        &artcat_files,
        &config.import_prefix,
        CYPHER_LOAD_ARTICLE_CATEGORIES,
        "article_categories",
        config.max_parallel_light,
        &pb_artcat,
    )
    .await?;
    let artcat_count = query_count(
        &graph,
        "MATCH ()-[r:HAS_CATEGORY]->() RETURN count(r) AS cnt",
    )
    .await?;
    println!("    Loaded {artcat_count} article-category relationships.");

    println!();
    println!("==> Loading images and external links ...");
    run_cypher(
        &graph,
        "CREATE INDEX image_filename IF NOT EXISTS FOR (i:Image) ON (i.filename);",
    )
    .await?;
    run_cypher(
        &graph,
        "CREATE INDEX extlink_url IF NOT EXISTS FOR (e:ExternalLink) ON (e.url);",
    )
    .await?;

    let img_files = csv_files_for("images", &layout);
    let pb_img = mp.add(make_progress_bar(img_files.len() as u64, "Images"));
    load_csv_files(
        &graph,
        &img_files,
        &config.import_prefix,
        CYPHER_LOAD_IMAGES,
        "images",
        config.max_parallel_light,
        &pb_img,
    )
    .await?;

    let ext_files = csv_files_for("external_links", &layout);
    let pb_ext = mp.add(make_progress_bar(ext_files.len() as u64, "Ext Links"));
    load_csv_files(
        &graph,
        &ext_files,
        &config.import_prefix,
        CYPHER_LOAD_EXTERNAL_LINKS,
        "external_links",
        config.max_parallel_light,
        &pb_ext,
    )
    .await?;

    println!();
    let pb = mp.add(make_spinner("Creating constraints and indexes ..."));
    run_cypher(
        &graph,
        "CREATE CONSTRAINT page_id_unique IF NOT EXISTS FOR (p:Page) REQUIRE p.id IS UNIQUE;",
    )
    .await?;
    run_cypher(
        &graph,
        "CREATE INDEX page_title IF NOT EXISTS FOR (p:Page) ON (p.title);",
    )
    .await?;
    run_cypher(
        &graph,
        "CREATE CONSTRAINT category_name_unique IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE;",
    )
    .await?;
    pb.finish_with_message("Constraints and indexes created.");

    let elapsed = start.elapsed();
    println!();
    println!("============================================");
    println!("  SUCCESS: Import complete!");
    println!("============================================");
    println!();
    println!("Total time:         {:.2}s", elapsed.as_secs_f64());
    println!("Pages:              {page_count}");
    println!("Categories:         {cat_count}");
    println!("Edges:              {edge_count}");
    println!("Art-Categories:     {artcat_count}");
    println!();
    println!("Available at:");
    println!("  Bolt:   {}", config.bolt_uri);
    println!("  Browser: http://localhost:7474");

    Ok(())
}

fn detect_csv_layout(output_dir: &str) -> Result<CsvLayout> {
    let sharded_path = Path::new(output_dir).join("nodes_000.csv");
    let single_path = Path::new(output_dir).join("nodes.csv");

    if sharded_path.exists() {
        let mut count = 0u32;
        loop {
            let p = Path::new(output_dir).join(format!("nodes_{count:03}.csv"));
            if p.exists() {
                count += 1;
            } else {
                break;
            }
        }
        if count == 0 {
            bail!("Found nodes_000.csv but could not count shards");
        }
        Ok(CsvLayout::Sharded { count })
    } else if single_path.exists() {
        Ok(CsvLayout::Single)
    } else {
        bail!(
            "No CSV files found in {output_dir}. Expected nodes.csv or nodes_000.csv.\n\
             Run 'dedalus extract' first."
        );
    }
}

fn csv_files_for(base_name: &str, layout: &CsvLayout) -> Vec<String> {
    match layout {
        CsvLayout::Single => vec![format!("{base_name}.csv")],
        CsvLayout::Sharded { count } => (0..*count)
            .map(|s| format!("{base_name}_{s:03}.csv"))
            .collect(),
    }
}

fn validate_csv_files(output_dir: &str, layout: &CsvLayout) -> Result<()> {
    for base in CSV_TYPES {
        let files = csv_files_for(base, layout);
        for file in &files {
            let path = Path::new(output_dir).join(file);
            if !path.exists() {
                bail!(
                    "Missing CSV file: {path:?}\n\
                     Run 'dedalus extract' first."
                );
            }
        }
    }
    Ok(())
}

fn resolve_compose_file(config: &ImportConfig) -> Result<String> {
    if let Some(ref path) = config.compose_file {
        if !Path::new(path).exists() {
            bail!("Compose file not found: {path}");
        }
        return Ok(path.clone());
    }

    let candidates = [
        "neo4j-platform/docker-compose.yml",
        "../neo4j-platform/docker-compose.yml",
    ];
    for candidate in &candidates {
        if Path::new(candidate).exists() {
            return Ok(candidate.to_string());
        }
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(exe_dir) = exe.parent().and_then(|p| p.parent()) {
            let path = exe_dir.join("neo4j-platform/docker-compose.yml");
            if path.exists() {
                return Ok(path.to_string_lossy().to_string());
            }
        }
    }

    bail!(
        "Could not find docker-compose.yml for Neo4j.\n\
         Use --compose-file to specify the path, or --no-docker to skip Docker management."
    );
}

async fn docker_start(compose_file: &str, config: &ImportConfig) -> Result<()> {
    if config.clean {
        println!();
        println!("==> Cleaning up previous Neo4j instance ...");
        let status = Command::new("docker")
            .args(["compose", "-f", compose_file, "down", "-v"])
            .env("IMPORT_DIR", &config.output_dir)
            .status()
            .await
            .context("Failed to run 'docker compose down'")?;
        if !status.success() {
            warn!("docker compose down exited with status {status}, continuing...");
        }
    }

    println!();
    println!("==> Starting Neo4j ...");
    let output = Command::new("docker")
        .args(["compose", "-f", compose_file, "up", "-d"])
        .env("IMPORT_DIR", &config.output_dir)
        .output()
        .await
        .context("Failed to run 'docker compose up -d'")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("docker compose up failed:\n{stderr}");
    }
    println!("    Docker containers started.");

    Ok(())
}

async fn connect_with_retry(config: &ImportConfig) -> Result<Graph> {
    let max_retries = config::IMPORT_MAX_RETRIES;
    let delay = tokio::time::Duration::from_secs(config::IMPORT_RETRY_DELAY_SECS);

    for attempt in 1..=max_retries {
        match Graph::new(&config.bolt_uri, "", "") {
            Ok(graph) => match graph.run(query("RETURN 1;")).await {
                Ok(_) => return Ok(graph),
                Err(e) if attempt < max_retries => {
                    info!(attempt, "Connection test failed, retrying: {e}");
                    tokio::time::sleep(delay).await;
                }
                Err(e) => {
                    return Err(e).context(format!(
                        "Cannot connect to Neo4j at {} after {max_retries} attempts.\n\
                             Is Docker running? Check: docker ps",
                        config.bolt_uri
                    ));
                }
            },
            Err(e) if attempt < max_retries => {
                info!(
                    attempt,
                    "Cannot connect to Neo4j at {}, retrying...", config.bolt_uri
                );
                tokio::time::sleep(delay).await;
            }
            Err(e) => {
                return Err(e).context(format!(
                    "Cannot connect to Neo4j at {} after {max_retries} attempts.\n\
                     Is Docker running? Check: docker ps",
                    config.bolt_uri
                ));
            }
        }
    }

    bail!(
        "Cannot connect to Neo4j at {} after {max_retries} attempts",
        config.bolt_uri
    );
}

async fn run_cypher(graph: &Graph, cypher: &str) -> Result<()> {
    graph
        .run(query(cypher))
        .await
        .with_context(|| format!("Failed to execute: {cypher}"))?;
    Ok(())
}

async fn query_count(graph: &Graph, cypher: &str) -> Result<i64> {
    let mut result = graph
        .execute(query(cypher))
        .await
        .with_context(|| format!("Failed to execute count query: {cypher}"))?;

    if let Some(row) = result.next().await? {
        let count: i64 = row.get("cnt").context("Missing 'cnt' field in result")?;
        Ok(count)
    } else {
        Ok(0)
    }
}

/// Loads CSV files into Neo4j via LOAD CSV, throttled to `max_parallel` concurrent queries.
async fn load_csv_files(
    graph: &Graph,
    files: &[String],
    import_prefix: &str,
    cypher_template: &str,
    label: &str,
    max_parallel: usize,
    pb: &ProgressBar,
) -> Result<()> {
    if files.is_empty() {
        pb.finish_with_message(format!("{label}: nothing to load"));
        return Ok(());
    }

    let mut in_flight = FuturesUnordered::new();
    let mut file_iter = files.iter().enumerate();
    let mut failed = 0u64;
    let mut completed = 0u64;

    let initial = max_parallel.min(files.len());
    for _ in 0..initial {
        if let Some((_i, file)) = file_iter.next() {
            let cypher = cypher_template.replace("{file}", &format!("{import_prefix}/{file}"));
            let file_name = file.clone();
            let graph = graph.clone();
            in_flight.push(tokio::spawn(async move {
                let result = graph.run(query(&cypher)).await;
                (file_name, result)
            }));
        }
    }

    while let Some(join_result) = in_flight.next().await {
        let (file_name, result) = join_result.context("Task join error")?;
        match result {
            Ok(_) => {
                completed += 1;
            }
            Err(e) => {
                failed += 1;
                warn!(file = %file_name, error = %e, "LOAD CSV failed");
                eprintln!("    FAILED: {file_name}: {e}");
            }
        }
        pb.inc(1);

        if let Some((_i, file)) = file_iter.next() {
            let cypher = cypher_template.replace("{file}", &format!("{import_prefix}/{file}"));
            let file_name = file.clone();
            let graph = graph.clone();
            in_flight.push(tokio::spawn(async move {
                let result = graph.run(query(&cypher)).await;
                (file_name, result)
            }));
        }
    }

    pb.finish_with_message(format!("{label}: {completed} loaded, {failed} failed"));

    if failed > 0 {
        bail!("{failed} of {} {label} loads failed", files.len());
    }

    Ok(())
}

fn make_spinner(msg: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap(),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb.set_message(msg.to_string());
    pb
}

fn make_progress_bar(total: u64, label: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(&format!(
                "    {{spinner:.cyan}} {label:<14} [{{bar:30.cyan/blue}}] {{pos}}/{{len}} shards"
            ))
            .unwrap()
            .progress_chars("=> "),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn detect_layout_single() {
        let dir = TempDir::new().unwrap();
        for base in CSV_TYPES {
            std::fs::write(dir.path().join(format!("{base}.csv")), "header\n").unwrap();
        }
        let layout = detect_csv_layout(dir.path().to_str().unwrap()).unwrap();
        assert!(matches!(layout, CsvLayout::Single));
    }

    #[test]
    fn detect_layout_sharded() {
        let dir = TempDir::new().unwrap();
        for base in CSV_TYPES {
            for shard in 0..4u32 {
                std::fs::write(
                    dir.path().join(format!("{base}_{shard:03}.csv")),
                    "header\n",
                )
                .unwrap();
            }
        }
        let layout = detect_csv_layout(dir.path().to_str().unwrap()).unwrap();
        assert!(matches!(layout, CsvLayout::Sharded { count: 4 }));
    }

    #[test]
    fn detect_layout_missing() {
        let dir = TempDir::new().unwrap();
        let result = detect_csv_layout(dir.path().to_str().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No CSV files"));
    }

    #[test]
    fn csv_files_for_single() {
        let files = csv_files_for("edges", &CsvLayout::Single);
        assert_eq!(files, vec!["edges.csv"]);
    }

    #[test]
    fn csv_files_for_sharded() {
        let files = csv_files_for("edges", &CsvLayout::Sharded { count: 3 });
        assert_eq!(
            files,
            vec!["edges_000.csv", "edges_001.csv", "edges_002.csv"]
        );
    }

    #[test]
    fn validate_csv_files_ok() {
        let dir = TempDir::new().unwrap();
        for base in CSV_TYPES {
            std::fs::write(dir.path().join(format!("{base}.csv")), "header\n").unwrap();
        }
        let layout = CsvLayout::Single;
        assert!(validate_csv_files(dir.path().to_str().unwrap(), &layout).is_ok());
    }

    #[test]
    fn validate_csv_files_missing() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("nodes.csv"), "header\n").unwrap();
        let layout = CsvLayout::Single;
        let result = validate_csv_files(dir.path().to_str().unwrap(), &layout);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Missing CSV file"));
    }

    #[test]
    fn cypher_template_replacement() {
        let cypher = CYPHER_LOAD_PAGES.replace("{file}", "file:///nodes_000.csv");
        assert!(cypher.contains("file:///nodes_000.csv"));
        assert!(!cypher.contains("{file}"));
        assert!(cypher.contains("WITH HEADERS"));
        assert!(cypher.contains("IN TRANSACTIONS"));
        assert!(cypher.contains("CREATE (:Page"));
    }

    #[test]
    fn cypher_template_edges() {
        let cypher = CYPHER_LOAD_EDGES.replace("{file}", "file:///edges_005.csv");
        assert!(cypher.contains("file:///edges_005.csv"));
        assert!(cypher.contains("WITH HEADERS"));
        assert!(cypher.contains("IN TRANSACTIONS"));
        assert!(cypher.contains("MATCH (a:Page"));
        assert!(cypher.contains("CREATE (a)-[:LINKS_TO]->(b)"));
    }
}
