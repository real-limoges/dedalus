//! Graph analytics: PageRank, label-propagation communities, and degree.
//!
//! Reads the merged `nodes.csv` and `edges.csv`, builds a CSR (Compressed
//! Sparse Row) graph over the `LINKS_TO` edges, and writes one row per article
//! to `analytics.csv`. `SEE_ALSO` edges are left out: they repeat links an
//! editor chose to list, and counting them would weight those links twice.
//! PageRank and label propagation run in parallel with rayon.

use crate::atomic_file::write_atomically;
use crate::config;
use crate::csv_util::{self, CsvLayout, CsvType};
use crate::models::EdgeType;
use anyhow::{Context, Result, bail};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::path::Path;
use std::time::Instant;
use tracing::{info, warn};

/// The file analytics writes, next to the merged CSVs.
pub const ANALYTICS_FILE: &str = "analytics.csv";

/// The header of `analytics.csv`, in the same typed-column style as the
/// extraction CSVs. `community` is the page id of one article in the
/// community, so it names the community stably across runs.
pub const ANALYTICS_HEADER: [&str; 4] =
    ["id:ID", "pagerank:double", "community:long", "degree:int"];

/// Configuration for the analytics step.
#[derive(Debug, Clone)]
pub struct AnalyticsConfig {
    pub output_dir: String,
    pub pagerank_iterations: u32,
    pub pagerank_damping: f64,
    pub pagerank_epsilon: f64,
    pub label_propagation_max_iterations: u32,
}

impl Default for AnalyticsConfig {
    fn default() -> Self {
        Self {
            output_dir: String::new(),
            pagerank_iterations: config::PAGERANK_ITERATIONS,
            pagerank_damping: config::PAGERANK_DAMPING,
            pagerank_epsilon: config::PAGERANK_EPSILON,
            label_propagation_max_iterations: config::LABEL_PROPAGATION_MAX_ITERATIONS,
        }
    }
}

/// Statistics returned after analytics computation.
#[derive(Debug, Default)]
pub struct AnalyticsStats {
    pub node_count: usize,
    pub edge_count: usize,
    /// `LINKS_TO` rows whose source or target is not in `nodes.csv`.
    pub skipped_edges: u64,
    /// Rows of `nodes.csv` that repeat an id already seen.
    pub duplicate_nodes: u64,
    pub pagerank_iterations_run: u32,
    /// Total rank that moved in the last PageRank iteration.
    pub pagerank_residual: f64,
    pub communities_found: usize,
    pub elapsed_secs: f64,
}

/// Compressed Sparse Row graph, with both directions stored.
struct CsrGraph {
    /// Number of nodes (dense index 0..n).
    n: usize,
    /// `row_ptr[i]..row_ptr[i + 1]` is the range of node i's outgoing edges.
    row_ptr: Vec<usize>,
    /// Targets of outgoing edges, indexed by `row_ptr` ranges.
    col_idx: Vec<u32>,
    /// `in_row_ptr[i]..in_row_ptr[i + 1]` is the range of node i's incoming edges.
    in_row_ptr: Vec<usize>,
    /// Sources of incoming edges, indexed by `in_row_ptr` ranges.
    in_col_idx: Vec<u32>,
    /// Wikipedia page id of each dense index.
    dense_to_wiki: Vec<u32>,
}

impl CsrGraph {
    fn out_neighbors(&self, i: usize) -> &[u32] {
        &self.col_idx[self.row_ptr[i]..self.row_ptr[i + 1]]
    }

    fn in_neighbors(&self, i: usize) -> &[u32] {
        &self.in_col_idx[self.in_row_ptr[i]..self.in_row_ptr[i + 1]]
    }
}

/// A built graph and what was dropped while building it.
struct GraphBuild {
    graph: CsrGraph,
    duplicate_nodes: u64,
    skipped_edges: u64,
}

/// Computes PageRank, communities and degree, and writes `analytics.csv`.
pub fn run_analytics(config: &AnalyticsConfig) -> Result<AnalyticsStats> {
    let start = Instant::now();
    let output_dir = &config.output_dir;

    if csv_util::detect_csv_layout(output_dir)? != CsvLayout::Single {
        bail!(
            "Analytics requires merged CSVs (nodes.csv, edges.csv).\n\
             Run 'dedalus merge-csvs -o {output_dir}' first, or extract with --csv-shards 1."
        );
    }

    let GraphBuild {
        graph,
        duplicate_nodes,
        skipped_edges,
    } = build_graph(output_dir)?;
    if graph.n == 0 {
        bail!("nodes.csv in {output_dir} has no articles");
    }
    info!(
        nodes = graph.n,
        edges = graph.col_idx.len(),
        skipped_edges,
        duplicate_nodes,
        "Graph built from CSVs"
    );

    let pagerank = compute_pagerank(
        &graph,
        config.pagerank_damping,
        config.pagerank_iterations,
        config.pagerank_epsilon,
    );
    let communities = compute_label_propagation(&graph, config.label_propagation_max_iterations);
    let communities_found = communities
        .iter()
        .copied()
        .collect::<rustc_hash::FxHashSet<u32>>()
        .len();
    info!(
        communities = communities_found,
        "Community detection complete"
    );
    let degrees = compute_degrees(&graph);

    write_analytics(output_dir, &graph, &pagerank.ranks, &communities, &degrees)?;

    Ok(AnalyticsStats {
        node_count: graph.n,
        edge_count: graph.col_idx.len(),
        skipped_edges,
        duplicate_nodes,
        pagerank_iterations_run: pagerank.iterations,
        pagerank_residual: pagerank.residual,
        communities_found,
        elapsed_secs: start.elapsed().as_secs_f64(),
    })
}

// -- Building the graph -------------------------------------------------------

/// Builds the CSR graph from `nodes.csv` and the `LINKS_TO` rows of `edges.csv`.
///
/// Edges are read twice: once to count each node's degree, and once to fill
/// the arrays at offsets taken from those counts. That costs a second read
/// but never holds a per-node `Vec` for every node at once.
fn build_graph(output_dir: &str) -> Result<GraphBuild> {
    let nodes_path = Path::new(output_dir).join(CsvType::Nodes.merged_file_name());
    let edges_path = Path::new(output_dir).join(CsvType::Edges.merged_file_name());

    let (dense_to_wiki, id_map, duplicate_nodes) = read_node_ids(&nodes_path)?;
    if duplicate_nodes > 0 {
        warn!(
            duplicate_nodes,
            "nodes.csv repeats ids; kept the first of each"
        );
    }
    let n = dense_to_wiki.len();

    let mut out_degree = vec![0u32; n];
    let mut in_degree = vec![0u32; n];
    let skipped_edges = for_each_link(&edges_path, &id_map, |src, dst| {
        out_degree[src as usize] += 1;
        in_degree[dst as usize] += 1;
    })?;

    let row_ptr = prefix_sums(&out_degree);
    let in_row_ptr = prefix_sums(&in_degree);
    let mut col_idx = vec![0u32; row_ptr[n]];
    let mut in_col_idx = vec![0u32; in_row_ptr[n]];
    let mut out_next = row_ptr[..n].to_vec();
    let mut in_next = in_row_ptr[..n].to_vec();
    for_each_link(&edges_path, &id_map, |src, dst| {
        let (src, dst) = (src as usize, dst as usize);
        col_idx[out_next[src]] = dst as u32;
        out_next[src] += 1;
        in_col_idx[in_next[dst]] = src as u32;
        in_next[dst] += 1;
    })?;

    Ok(GraphBuild {
        graph: CsrGraph {
            n,
            row_ptr,
            col_idx,
            in_row_ptr,
            in_col_idx,
            dense_to_wiki,
        },
        duplicate_nodes,
        skipped_edges,
    })
}

/// Reads the ids of `nodes.csv` in file order. Returns the dense-to-wiki
/// list, the wiki-to-dense map, and how many repeated ids were skipped.
fn read_node_ids(path: &Path) -> Result<(Vec<u32>, FxHashMap<u32, u32>, u64)> {
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| format!("Failed to open {}", path.display()))?;
    let mut dense_to_wiki = Vec::new();
    let mut id_map = FxHashMap::default();
    let mut duplicates = 0u64;
    for record in reader.records() {
        let record = record.with_context(|| format!("Failed to read {}", path.display()))?;
        let wiki_id = parse_id(&record, 0, "node id")?;
        if id_map.contains_key(&wiki_id) {
            duplicates += 1;
            continue;
        }
        id_map.insert(wiki_id, dense_to_wiki.len() as u32);
        dense_to_wiki.push(wiki_id);
    }
    Ok((dense_to_wiki, id_map, duplicates))
}

/// Calls `visit(src, dst)` with dense indices for every `LINKS_TO` row whose
/// endpoints are both known. Returns how many `LINKS_TO` rows were skipped.
fn for_each_link(
    path: &Path,
    id_map: &FxHashMap<u32, u32>,
    mut visit: impl FnMut(u32, u32),
) -> Result<u64> {
    let links_to = EdgeType::LinksTo.to_string();
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| format!("Failed to open {}", path.display()))?;
    let mut skipped = 0u64;
    for record in reader.records() {
        let record = record.with_context(|| format!("Failed to read {}", path.display()))?;
        let edge_type = record.get(2).context("Edge row without a :TYPE")?;
        if edge_type != links_to {
            continue;
        }
        let src = parse_id(&record, 0, "edge source")?;
        let dst = parse_id(&record, 1, "edge target")?;
        match (id_map.get(&src), id_map.get(&dst)) {
            (Some(&s), Some(&d)) => visit(s, d),
            _ => skipped += 1,
        }
    }
    Ok(skipped)
}

fn parse_id(record: &csv::StringRecord, column: usize, what: &str) -> Result<u32> {
    let field = record
        .get(column)
        .with_context(|| format!("Row without a {what}: {record:?}"))?;
    field
        .parse()
        .with_context(|| format!("Invalid {what} {field:?}"))
}

/// `[0, d0, d0 + d1, ...]`: the CSR row pointers for the given degrees.
fn prefix_sums(degrees: &[u32]) -> Vec<usize> {
    std::iter::once(0)
        .chain(degrees.iter().scan(0usize, |total, &d| {
            *total += d as usize;
            Some(*total)
        }))
        .collect()
}

// -- PageRank -----------------------------------------------------------------

struct PageRank {
    ranks: Vec<f64>,
    iterations: u32,
    residual: f64,
}

/// Computes PageRank by power iteration, in parallel with rayon.
///
/// A node with no outgoing links (a dangling node) would otherwise leak its
/// rank out of the graph every iteration; its rank is instead spread evenly
/// over all nodes, so the ranks always sum to 1.
fn compute_pagerank(graph: &CsrGraph, damping: f64, max_iterations: u32, epsilon: f64) -> PageRank {
    let n = graph.n;
    if n == 0 {
        return PageRank {
            ranks: vec![],
            iterations: 0,
            residual: 0.0,
        };
    }

    let out_degree: Vec<usize> = (0..n).map(|i| graph.out_neighbors(i).len()).collect();
    let mut rank = vec![1.0 / n as f64; n];
    let mut new_rank = vec![0.0f64; n];
    let mut iterations = 0u32;
    let mut residual = f64::INFINITY;

    while iterations < max_iterations && residual >= epsilon {
        let dangling_mass: f64 = (0..n)
            .into_par_iter()
            .filter(|&i| out_degree[i] == 0)
            .map(|i| rank[i])
            .sum();
        let base = (1.0 - damping) / n as f64 + damping * dangling_mass / n as f64;

        new_rank.par_iter_mut().enumerate().for_each(|(i, r)| {
            let inflow: f64 = graph
                .in_neighbors(i)
                .iter()
                .map(|&src| rank[src as usize] / out_degree[src as usize] as f64)
                .sum();
            *r = base + damping * inflow;
        });

        residual = rank
            .par_iter()
            .zip(new_rank.par_iter())
            .map(|(a, b)| (a - b).abs())
            .sum();
        std::mem::swap(&mut rank, &mut new_rank);
        iterations += 1;
    }

    info!(iterations, residual, "PageRank finished");
    PageRank {
        ranks: rank,
        iterations,
        residual,
    }
}

// -- Communities and degree ---------------------------------------------------

/// Finds communities by label propagation, in parallel with rayon.
///
/// Every node starts with its own label. Each pass computes every node's new
/// label from the previous pass's labels: the label most common among its
/// neighbors (both directions) plus one vote for its own current label. Ties
/// go to the smallest label, so the result does not depend on thread timing or
/// hash map order. The self vote stops two linked nodes from swapping labels
/// forever, which a plain synchronous update would do. Runs until no label
/// changes or `max_iterations` passes, logging how many changed in each.
fn compute_label_propagation(graph: &CsrGraph, max_iterations: u32) -> Vec<u32> {
    let mut labels: Vec<u32> = (0..graph.n as u32).collect();

    for iteration in 1..=max_iterations {
        let next: Vec<u32> = (0..graph.n)
            .into_par_iter()
            .map_init(FxHashMap::default, |counts, i| {
                most_common_label(graph, &labels, i, counts)
            })
            .collect();
        let changed = labels
            .par_iter()
            .zip(next.par_iter())
            .filter(|(old, new)| old != new)
            .count();
        labels = next;
        info!(iteration, changed, "Label propagation pass");
        if changed == 0 {
            info!(iteration, "Label propagation converged");
            break;
        }
    }
    labels
}

/// The label node `i` adopts in the next pass: the most common among its
/// neighbors' labels and its own, ties to the smallest. `counts` is scratch
/// space reused across calls.
fn most_common_label(
    graph: &CsrGraph,
    labels: &[u32],
    i: usize,
    counts: &mut FxHashMap<u32, u32>,
) -> u32 {
    counts.clear();
    counts.insert(labels[i], 1);
    for &neighbor in graph.out_neighbors(i).iter().chain(graph.in_neighbors(i)) {
        *counts.entry(labels[neighbor as usize]).or_insert(0) += 1;
    }
    counts
        .iter()
        .max_by(|(label_a, count_a), (label_b, count_b)| {
            count_a.cmp(count_b).then(label_b.cmp(label_a))
        })
        .map_or(labels[i], |(&label, _)| label)
}

/// Combined in + out degree of each node.
fn compute_degrees(graph: &CsrGraph) -> Vec<u32> {
    (0..graph.n)
        .map(|i| (graph.out_neighbors(i).len() + graph.in_neighbors(i).len()) as u32)
        .collect()
}

// -- Output -------------------------------------------------------------------

/// Writes one row per article to `analytics.csv`, replacing it atomically.
fn write_analytics(
    output_dir: &str,
    graph: &CsrGraph,
    pagerank: &[f64],
    communities: &[u32],
    degrees: &[u32],
) -> Result<()> {
    let path = Path::new(output_dir).join(ANALYTICS_FILE);
    write_atomically(&path, |out| {
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(out);
        writer.write_record(ANALYTICS_HEADER)?;
        for i in 0..graph.n {
            let community = graph.dense_to_wiki[communities[i] as usize];
            writer.serialize((graph.dense_to_wiki[i], pagerank[i], community, degrees[i]))?;
        }
        writer.flush()?;
        Ok(())
    })
    .with_context(|| format!("Failed to write {}", path.display()))?;
    info!(rows = graph.n, path = %path.display(), "Analytics written");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_graph(dir: &Path, nodes: &[u32], edges: &[(u32, u32, &str)]) {
        let node_rows: String = nodes
            .iter()
            .map(|id| format!("{id},Title {id},Page\n"))
            .collect();
        std::fs::write(
            dir.join("nodes.csv"),
            format!("id:ID,title,:LABEL\n{node_rows}"),
        )
        .unwrap();
        let edge_rows: String = edges
            .iter()
            .map(|(s, d, t)| format!("{s},{d},{t}\n"))
            .collect();
        std::fs::write(
            dir.join("edges.csv"),
            format!(":START_ID,:END_ID,:TYPE\n{edge_rows}"),
        )
        .unwrap();
    }

    /// A -> B, A -> C, B -> C, C -> D; D is dangling.
    fn write_chain(dir: &Path) {
        write_graph(
            dir,
            &[10, 20, 30, 40],
            &[
                (10, 20, "LINKS_TO"),
                (10, 30, "LINKS_TO"),
                (20, 30, "LINKS_TO"),
                (30, 40, "LINKS_TO"),
            ],
        );
    }

    fn graph_of(dir: &TempDir) -> GraphBuild {
        build_graph(dir.path().to_str().unwrap()).unwrap()
    }

    #[test]
    fn build_graph_fills_both_directions() {
        let dir = TempDir::new().unwrap();
        write_chain(dir.path());
        let graph = graph_of(&dir).graph;
        assert_eq!(graph.n, 4);
        assert_eq!(graph.out_neighbors(0), &[1, 2]);
        assert_eq!(graph.out_neighbors(3), &[] as &[u32]);
        assert_eq!(graph.in_neighbors(2), &[0, 1]);
    }

    #[test]
    fn build_graph_uses_links_to_only_and_skips_unknown_endpoints() {
        let dir = TempDir::new().unwrap();
        write_graph(
            dir.path(),
            &[1, 2],
            &[(1, 2, "LINKS_TO"), (2, 1, "SEE_ALSO"), (1, 99, "LINKS_TO")],
        );
        let built = graph_of(&dir);
        assert_eq!(built.graph.col_idx, vec![1]);
        assert_eq!(built.skipped_edges, 1);
    }

    #[test]
    fn build_graph_keeps_the_first_of_a_repeated_node() {
        let dir = TempDir::new().unwrap();
        write_graph(dir.path(), &[1, 2, 1], &[(1, 2, "LINKS_TO")]);
        let built = graph_of(&dir);
        assert_eq!(built.graph.dense_to_wiki, vec![1, 2]);
        assert_eq!(built.duplicate_nodes, 1);
    }

    #[test]
    fn build_graph_rejects_a_row_without_a_type() {
        let dir = TempDir::new().unwrap();
        write_graph(dir.path(), &[1, 2], &[]);
        std::fs::write(dir.path().join("edges.csv"), ":START_ID,:END_ID\n1,2\n").unwrap();
        assert!(build_graph(dir.path().to_str().unwrap()).is_err());
    }

    #[test]
    fn pagerank_sums_to_one_with_a_dangling_node() {
        let dir = TempDir::new().unwrap();
        write_chain(dir.path());
        let pr = compute_pagerank(&graph_of(&dir).graph, 0.85, 100, 1e-12);
        let total: f64 = pr.ranks.iter().sum();
        assert!((total - 1.0).abs() < 1e-9, "ranks sum to {total}");
        assert!(pr.residual < 1e-12);
        assert!(
            pr.ranks[3] > pr.ranks[0],
            "D receives rank, A receives none"
        );
    }

    #[test]
    fn pagerank_of_an_empty_graph_is_empty() {
        let graph = CsrGraph {
            n: 0,
            row_ptr: vec![0],
            col_idx: vec![],
            in_row_ptr: vec![0],
            in_col_idx: vec![],
            dense_to_wiki: vec![],
        };
        let pr = compute_pagerank(&graph, 0.85, 100, 1e-6);
        assert!(pr.ranks.is_empty());
        assert_eq!(pr.iterations, 0);
    }

    #[test]
    fn degrees_count_both_directions() {
        let dir = TempDir::new().unwrap();
        write_chain(dir.path());
        assert_eq!(compute_degrees(&graph_of(&dir).graph), vec![2, 2, 3, 1]);
    }

    #[test]
    fn label_propagation_separates_two_triangles() {
        let dir = TempDir::new().unwrap();
        write_graph(
            dir.path(),
            &[1, 2, 3, 4, 5, 6],
            &[
                (1, 2, "LINKS_TO"),
                (2, 3, "LINKS_TO"),
                (3, 1, "LINKS_TO"),
                (4, 5, "LINKS_TO"),
                (5, 6, "LINKS_TO"),
                (6, 4, "LINKS_TO"),
            ],
        );
        let labels = compute_label_propagation(&graph_of(&dir).graph, 50);
        assert!(labels[0] == labels[1] && labels[1] == labels[2]);
        assert!(labels[3] == labels[4] && labels[4] == labels[5]);
        assert_ne!(labels[0], labels[3]);
    }

    #[test]
    fn label_propagation_settles_a_linked_pair() {
        let dir = TempDir::new().unwrap();
        write_graph(dir.path(), &[1, 2], &[(1, 2, "LINKS_TO")]);
        let labels = compute_label_propagation(&graph_of(&dir).graph, 50);
        assert_eq!(labels, vec![0, 0]);
    }

    #[test]
    fn run_analytics_writes_one_row_per_article() {
        let dir = TempDir::new().unwrap();
        write_chain(dir.path());
        let config = AnalyticsConfig {
            output_dir: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };

        let stats = run_analytics(&config).unwrap();

        assert_eq!(stats.node_count, 4);
        assert_eq!(stats.edge_count, 4);
        let content = std::fs::read_to_string(dir.path().join(ANALYTICS_FILE)).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines[0], "id:ID,pagerank:double,community:long,degree:int");
        assert_eq!(lines.len(), 5);
        assert!(lines[1].starts_with("10,") && lines[1].ends_with(",2"));
    }

    #[test]
    fn run_analytics_fails_on_an_empty_graph() {
        let dir = TempDir::new().unwrap();
        write_graph(dir.path(), &[], &[]);
        let config = AnalyticsConfig {
            output_dir: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };
        let err = run_analytics(&config).unwrap_err();
        assert!(err.to_string().contains("no articles"));
    }

    #[test]
    fn run_analytics_requires_merged_csvs() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("nodes_000.csv"), "id:ID,title,:LABEL\n").unwrap();
        let config = AnalyticsConfig {
            output_dir: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        };
        let err = run_analytics(&config).unwrap_err();
        assert!(err.to_string().contains("requires merged CSVs"));
    }
}
