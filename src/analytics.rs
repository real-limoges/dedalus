//! Graph analytics: PageRank, Louvain community detection, and degree computation.
//!
//! Reads the extracted edge graph from CSVs, builds a CSR (Compressed Sparse Row)
//! representation in memory, computes analytics, and writes results back to SurrealDB.
//! Uses rayon for parallel PageRank iterations.

use crate::config;
use anyhow::{Context, Result};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::path::Path;
use std::time::Instant;
use surrealdb::engine::local::RocksDb;
use surrealdb::Surreal;
use tracing::info;

/// Configuration for the analytics computation step.
#[derive(Debug, Clone)]
pub struct AnalyticsConfig {
    pub db_path: String,
    pub output_dir: String,
    pub pagerank_iterations: u32,
    pub pagerank_damping: f64,
    pub pagerank_epsilon: f64,
    pub louvain_max_iterations: u32,
}

impl Default for AnalyticsConfig {
    fn default() -> Self {
        Self {
            db_path: config::DEFAULT_DB_PATH.to_string(),
            output_dir: String::new(),
            pagerank_iterations: config::PAGERANK_ITERATIONS,
            pagerank_damping: config::PAGERANK_DAMPING,
            pagerank_epsilon: config::PAGERANK_EPSILON,
            louvain_max_iterations: config::LOUVAIN_MAX_ITERATIONS,
        }
    }
}

/// Statistics returned after analytics computation.
#[derive(Debug, Default)]
pub struct AnalyticsStats {
    pub node_count: usize,
    pub edge_count: usize,
    pub pagerank_iterations_run: u32,
    pub communities_found: usize,
    pub elapsed_secs: f64,
}

/// Compressed Sparse Row graph representation.
/// Stores outgoing edges efficiently for PageRank computation.
struct CsrGraph {
    /// Number of nodes (dense index 0..n).
    n: usize,
    /// row_ptr[i]..row_ptr[i+1] gives the range of outgoing edges for node i.
    row_ptr: Vec<usize>,
    /// Column indices (targets) for all edges, indexed by row_ptr ranges.
    col_idx: Vec<u32>,
    /// Incoming edges: in_row_ptr[i]..in_row_ptr[i+1] gives incoming edges for node i.
    in_row_ptr: Vec<usize>,
    /// Source nodes for incoming edges.
    in_col_idx: Vec<u32>,
    /// Mapping from dense index back to Wikipedia page ID.
    dense_to_wiki: Vec<u32>,
}

/// Runs all analytics (PageRank, Louvain, degree) and writes results to SurrealDB.
pub async fn run_analytics(config: AnalyticsConfig) -> Result<AnalyticsStats> {
    let start = Instant::now();

    // Build graph from CSVs
    let (graph, id_map) = build_graph(&config.output_dir)?;
    info!(
        nodes = graph.n,
        edges = graph.col_idx.len(),
        "Graph built from CSVs"
    );

    // Compute PageRank
    let (pagerank, iterations_run) = compute_pagerank(
        &graph,
        config.pagerank_damping,
        config.pagerank_iterations,
        config.pagerank_epsilon,
    );
    info!(iterations = iterations_run, "PageRank computed");

    // Compute Louvain communities
    let communities = compute_louvain(&graph, config.louvain_max_iterations);
    let unique_communities: rustc_hash::FxHashSet<u32> = communities.iter().copied().collect();
    info!(
        communities = unique_communities.len(),
        "Community detection complete"
    );

    // Compute degrees
    let degrees = compute_degrees(&graph);

    // Write results to SurrealDB
    let db_path = if Path::new(&config.db_path).is_absolute() {
        config.db_path.clone()
    } else {
        Path::new(&config.output_dir)
            .join(&config.db_path)
            .to_string_lossy()
            .to_string()
    };

    let db = Surreal::new::<RocksDb>(&db_path)
        .await
        .with_context(|| format!("Failed to open SurrealDB at {}", db_path))?;

    db.use_ns(config::SURREAL_NAMESPACE)
        .use_db(config::SURREAL_DATABASE)
        .await
        .context("Failed to select namespace/database")?;

    write_analytics(&db, &graph, &pagerank, &communities, &degrees).await?;

    drop(id_map); // explicit drop for clarity

    let elapsed = start.elapsed();
    Ok(AnalyticsStats {
        node_count: graph.n,
        edge_count: graph.col_idx.len(),
        pagerank_iterations_run: iterations_run,
        communities_found: unique_communities.len(),
        elapsed_secs: elapsed.as_secs_f64(),
    })
}

/// Builds a CSR graph from nodes.csv and edges.csv.
///
/// Returns the graph and the sparse-to-dense ID mapping.
fn build_graph(output_dir: &str) -> Result<(CsrGraph, FxHashMap<u32, u32>)> {
    info!("Building graph from CSVs");

    // Read node IDs
    let nodes_path = Path::new(output_dir).join("nodes.csv");
    let mut node_reader = csv::Reader::from_path(&nodes_path)
        .with_context(|| format!("Failed to open {:?}", nodes_path))?;

    let mut wiki_ids: Vec<u32> = Vec::new();
    for result in node_reader.records() {
        let record = result.context("Failed to read node record")?;
        let id: u32 = record
            .get(0)
            .unwrap_or("0")
            .parse()
            .context("Invalid node ID")?;
        wiki_ids.push(id);
    }

    // Build sparse → dense mapping
    let mut id_map: FxHashMap<u32, u32> =
        FxHashMap::with_capacity_and_hasher(wiki_ids.len(), Default::default());
    let mut dense_to_wiki: Vec<u32> = Vec::with_capacity(wiki_ids.len());
    for (dense_idx, &wiki_id) in wiki_ids.iter().enumerate() {
        id_map.insert(wiki_id, dense_idx as u32);
        dense_to_wiki.push(wiki_id);
    }
    let n = wiki_ids.len();
    drop(wiki_ids);

    // Read edges and build adjacency lists
    let edges_path = Path::new(output_dir).join("edges.csv");
    let mut edge_reader = csv::Reader::from_path(&edges_path)
        .with_context(|| format!("Failed to open {:?}", edges_path))?;

    // Collect outgoing and incoming edges per node
    let mut out_edges: Vec<Vec<u32>> = vec![Vec::new(); n];
    let mut in_edges: Vec<Vec<u32>> = vec![Vec::new(); n];

    for result in edge_reader.records() {
        let record = result.context("Failed to read edge record")?;
        let src_wiki: u32 = record
            .get(0)
            .unwrap_or("0")
            .parse()
            .context("Invalid source ID")?;
        let dst_wiki: u32 = record
            .get(1)
            .unwrap_or("0")
            .parse()
            .context("Invalid target ID")?;

        if let (Some(&src), Some(&dst)) = (id_map.get(&src_wiki), id_map.get(&dst_wiki)) {
            out_edges[src as usize].push(dst);
            in_edges[dst as usize].push(src);
        }
    }

    // Convert to CSR format
    let mut row_ptr = Vec::with_capacity(n + 1);
    let mut col_idx = Vec::new();
    row_ptr.push(0);
    for edges in &out_edges {
        col_idx.extend_from_slice(edges);
        row_ptr.push(col_idx.len());
    }
    drop(out_edges);

    let mut in_row_ptr = Vec::with_capacity(n + 1);
    let mut in_col_idx = Vec::new();
    in_row_ptr.push(0);
    for edges in &in_edges {
        in_col_idx.extend_from_slice(edges);
        in_row_ptr.push(in_col_idx.len());
    }
    drop(in_edges);

    let graph = CsrGraph {
        n,
        row_ptr,
        col_idx,
        in_row_ptr,
        in_col_idx,
        dense_to_wiki,
    };

    Ok((graph, id_map))
}

/// Computes PageRank using power iteration with rayon parallelism.
///
/// Returns the PageRank vector and the number of iterations actually run.
fn compute_pagerank(
    graph: &CsrGraph,
    damping: f64,
    max_iterations: u32,
    epsilon: f64,
) -> (Vec<f32>, u32) {
    let n = graph.n;
    if n == 0 {
        return (vec![], 0);
    }

    let initial = 1.0f32 / n as f32;
    let mut rank = vec![initial; n];
    let mut new_rank = vec![0.0f32; n];

    // Precompute out-degree for each node
    let out_degree: Vec<u32> = (0..n)
        .map(|i| (graph.row_ptr[i + 1] - graph.row_ptr[i]) as u32)
        .collect();

    let teleport = ((1.0 - damping) / n as f64) as f32;
    let damping_f32 = damping as f32;

    let mut iterations_run = 0u32;
    for _iter in 0..max_iterations {
        // Parallel computation of new ranks
        new_rank.par_iter_mut().enumerate().for_each(|(i, new_r)| {
            let mut sum = 0.0f32;
            let in_start = graph.in_row_ptr[i];
            let in_end = graph.in_row_ptr[i + 1];
            for &src in &graph.in_col_idx[in_start..in_end] {
                let src = src as usize;
                let deg = out_degree[src];
                if deg > 0 {
                    sum += rank[src] / deg as f32;
                }
            }
            *new_r = teleport + damping_f32 * sum;
        });

        // Check convergence
        let diff: f32 = rank
            .par_iter()
            .zip(new_rank.par_iter())
            .map(|(a, b)| (a - b).abs())
            .sum();

        std::mem::swap(&mut rank, &mut new_rank);
        iterations_run += 1;

        if (diff as f64) < epsilon {
            info!(iteration = iterations_run, diff, "PageRank converged");
            break;
        }
    }

    (rank, iterations_run)
}

/// Computes communities using label propagation (simplified Louvain).
///
/// Each node starts in its own community, then iteratively adopts the most
/// frequent community label among its neighbors.
fn compute_louvain(graph: &CsrGraph, max_iterations: u32) -> Vec<u32> {
    let n = graph.n;
    if n == 0 {
        return vec![];
    }

    // Initialize: each node in its own community
    let mut labels: Vec<u32> = (0..n as u32).collect();

    for iter in 0..max_iterations {
        let mut changed = false;

        for i in 0..n {
            // Count neighbor labels (both in and out edges)
            let mut label_counts: FxHashMap<u32, u32> =
                FxHashMap::with_capacity_and_hasher(16, Default::default());

            // Outgoing neighbors
            let out_start = graph.row_ptr[i];
            let out_end = graph.row_ptr[i + 1];
            for &neighbor in &graph.col_idx[out_start..out_end] {
                *label_counts.entry(labels[neighbor as usize]).or_insert(0) += 1;
            }

            // Incoming neighbors
            let in_start = graph.in_row_ptr[i];
            let in_end = graph.in_row_ptr[i + 1];
            for &neighbor in &graph.in_col_idx[in_start..in_end] {
                *label_counts.entry(labels[neighbor as usize]).or_insert(0) += 1;
            }

            if let Some((&best_label, _)) = label_counts.iter().max_by_key(|&(_, &count)| count)
                && best_label != labels[i]
            {
                labels[i] = best_label;
                changed = true;
            }
        }

        if !changed {
            info!(iteration = iter + 1, "Label propagation converged");
            break;
        }
    }

    labels
}

/// Computes combined in+out degree for each node.
fn compute_degrees(graph: &CsrGraph) -> Vec<u32> {
    (0..graph.n)
        .map(|i| {
            let out = (graph.row_ptr[i + 1] - graph.row_ptr[i]) as u32;
            let in_deg = (graph.in_row_ptr[i + 1] - graph.in_row_ptr[i]) as u32;
            out + in_deg
        })
        .collect()
}

/// Writes PageRank, community, and degree data back to SurrealDB.
async fn write_analytics(
    db: &Surreal<surrealdb::engine::local::Db>,
    graph: &CsrGraph,
    pagerank: &[f32],
    communities: &[u32],
    degrees: &[u32],
) -> Result<()> {
    info!("Writing analytics results to SurrealDB");
    let batch_size = config::SURREAL_BATCH_SIZE;
    let mut batch = String::new();
    let mut batch_count = 0usize;
    let mut total = 0u64;

    for i in 0..graph.n {
        let wiki_id = graph.dense_to_wiki[i];
        let pr = pagerank.get(i).copied().unwrap_or(0.0);
        let community = communities.get(i).copied().unwrap_or(0);
        let degree = degrees.get(i).copied().unwrap_or(0);

        batch.push_str(&format!(
            "UPDATE article:{wiki_id} SET pagerank = {pr}, community = {community}, degree = {degree};\n"
        ));
        batch_count += 1;

        if batch_count >= batch_size {
            db.query(&batch)
                .await
                .context("Failed to write analytics batch")?;
            total += batch_count as u64;
            if total.is_multiple_of(100_000) {
                info!(total, "Analytics records written");
            }
            batch.clear();
            batch_count = 0;
        }
    }

    if batch_count > 0 {
        db.query(&batch)
            .await
            .context("Failed to write final analytics batch")?;
        total += batch_count as u64;
    }

    info!(total, "Analytics results written to SurrealDB");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_test_graph(dir: &Path) {
        // 4-node graph: 0->1, 0->2, 1->2, 2->3
        std::fs::write(
            dir.join("nodes.csv"),
            "id:ID,title,:LABEL\n10,A,Page\n20,B,Page\n30,C,Page\n40,D,Page\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("edges.csv"),
            ":START_ID,:END_ID,:TYPE\n10,20,LINKS_TO\n10,30,LINKS_TO\n20,30,LINKS_TO\n30,40,LINKS_TO\n",
        )
        .unwrap();
    }

    #[test]
    fn test_build_graph() {
        let dir = TempDir::new().unwrap();
        write_test_graph(dir.path());

        let (graph, _id_map) = build_graph(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(graph.n, 4);
        assert_eq!(graph.col_idx.len(), 4); // 4 edges
    }

    #[test]
    fn test_pagerank_basic() {
        let dir = TempDir::new().unwrap();
        write_test_graph(dir.path());

        let (graph, _) = build_graph(dir.path().to_str().unwrap()).unwrap();
        let (ranks, iters) = compute_pagerank(&graph, 0.85, 100, 1e-6);

        assert_eq!(ranks.len(), 4);
        assert!(iters > 0);
        // Node D (index 3) should have highest rank (sink node receiving from C)
        // Node A (index 0) should have lowest (no incoming edges)
        assert!(ranks[3] > ranks[0]);
    }

    #[test]
    fn test_pagerank_empty() {
        let (ranks, iters) = compute_pagerank(
            &CsrGraph {
                n: 0,
                row_ptr: vec![0],
                col_idx: vec![],
                in_row_ptr: vec![0],
                in_col_idx: vec![],
                dense_to_wiki: vec![],
            },
            0.85,
            100,
            1e-6,
        );
        assert!(ranks.is_empty());
        assert_eq!(iters, 0);
    }

    #[test]
    fn test_degrees() {
        let dir = TempDir::new().unwrap();
        write_test_graph(dir.path());

        let (graph, _) = build_graph(dir.path().to_str().unwrap()).unwrap();
        let degrees = compute_degrees(&graph);

        assert_eq!(degrees.len(), 4);
        // Node A (idx 0): 2 out, 0 in = 2
        assert_eq!(degrees[0], 2);
        // Node B (idx 1): 1 out, 1 in = 2
        assert_eq!(degrees[1], 2);
        // Node C (idx 2): 1 out, 2 in = 3
        assert_eq!(degrees[2], 3);
        // Node D (idx 3): 0 out, 1 in = 1
        assert_eq!(degrees[3], 1);
    }

    #[test]
    fn test_louvain_basic() {
        let dir = TempDir::new().unwrap();
        write_test_graph(dir.path());

        let (graph, _) = build_graph(dir.path().to_str().unwrap()).unwrap();
        let communities = compute_louvain(&graph, 100);

        assert_eq!(communities.len(), 4);
        // Connected nodes should tend toward the same community
    }
}
