//! In-memory title-to-ID index with redirect resolution.
//!
//! `WikiIndex` holds two `FxHashMap`s keyed by title in the canonical form
//! from `title::normalize_title`: article titles to page ids, and redirect
//! titles to the page id their chain ends at. Every redirect chain is followed
//! once, up to `REDIRECT_MAX_DEPTH` hops, when the index is built, so a lookup
//! is at most two hash probes. Redirects whose chain ends nowhere (a missing
//! page, a loop, or too many hops) are dropped.
//! Supports both sequential and multistream parallel index building.

use crate::config::{PROGRESS_INTERVAL, REDIRECT_MAX_DEPTH};
use crate::models::PageType;
use crate::multistream::StreamRange;
use crate::parser::WikiReader;
use crate::title::{into_canonical, normalize_title};
use anyhow::{Context, Result};
use indicatif::ProgressBar;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info};

/// In-memory title-to-ID index with redirects already resolved to ids.
pub struct WikiIndex {
    title_to_id: FxHashMap<String, u32>,
    redirect_to_id: FxHashMap<String, u32>,
}

impl std::fmt::Debug for WikiIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WikiIndex")
            .field("articles", &self.title_to_id.len())
            .field("redirects", &self.redirect_to_id.len())
            .finish()
    }
}

/// Follows `start` through `redirects` to an article: hop 0 is `start`
/// itself, hops 1..=`REDIRECT_MAX_DEPTH` follow redirects.
fn follow_redirects(
    title_to_id: &FxHashMap<String, u32>,
    redirects: &FxHashMap<String, String>,
    start: &str,
) -> Option<u32> {
    let mut current = start;
    for _hop in 0..=REDIRECT_MAX_DEPTH {
        if let Some(&id) = title_to_id.get(current) {
            return Some(id);
        }
        current = redirects.get(current)?;
    }
    debug!(title = start, "Redirect chain too deep");
    None
}

fn progress_spinner() -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .expect("valid progress template"),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}

impl WikiIndex {
    /// Builds an index from canonical article titles and canonical redirect
    /// pairs (title to target), resolving every redirect chain once.
    pub fn new(title_to_id: FxHashMap<String, u32>, redirects: FxHashMap<String, String>) -> Self {
        let resolved: Vec<(String, u32)> = redirects
            .par_iter()
            .filter_map(|(title, _)| {
                follow_redirects(&title_to_id, &redirects, title).map(|id| (title.clone(), id))
            })
            .collect();
        info!(
            redirects = redirects.len(),
            resolved = resolved.len(),
            "Redirects resolved"
        );
        Self {
            title_to_id,
            redirect_to_id: resolved.into_iter().collect(),
        }
    }

    /// An index from maps that were already resolved, as `maps` returns them
    /// (read back from the index cache).
    pub(crate) fn from_resolved(
        title_to_id: FxHashMap<String, u32>,
        redirect_to_id: FxHashMap<String, u32>,
    ) -> Self {
        Self {
            title_to_id,
            redirect_to_id,
        }
    }

    /// Builds the index by streaming through the dump in skip-text mode.
    pub fn build(path: &str) -> Result<Self> {
        let mut title_to_id: FxHashMap<String, u32> = FxHashMap::with_capacity_and_hasher(
            crate::config::INDEX_INITIAL_ARTICLES,
            Default::default(),
        );
        let mut redirects: FxHashMap<String, String> = FxHashMap::with_capacity_and_hasher(
            crate::config::INDEX_INITIAL_REDIRECTS,
            Default::default(),
        );
        let mut reader = WikiReader::new(path, true)
            .with_context(|| format!("Failed to open wiki dump at: {}", path))?
            .skip_timestamp(true);
        let pb = progress_spinner();

        info!("Building index from: {}", path);
        let mut page_count: u64 = 0;

        for page in reader.by_ref() {
            match page.page_type {
                PageType::Article => {
                    title_to_id.insert(into_canonical(page.title), page.id);
                }
                PageType::Redirect(target) => {
                    redirects.insert(into_canonical(page.title), into_canonical(target));
                }
                _ => {}
            }
            page_count += 1;
            if page_count.is_multiple_of(PROGRESS_INTERVAL as u64) {
                pb.set_message(format!(
                    "Indexing: {} pages ({} articles, {} redirects)",
                    page_count,
                    title_to_id.len(),
                    redirects.len()
                ));
            }
        }

        pb.finish_and_clear();
        // A truncated or corrupt dump must not produce (and cache) a partial index.
        reader.finish()?;

        let index = Self::new(title_to_id, redirects);
        info!(
            articles = index.title_to_id.len(),
            redirects = index.redirect_to_id.len(),
            "Index built successfully"
        );
        Ok(index)
    }

    /// Build index using multistream parallel parsing.
    /// Each rayon worker decompresses and parses streams independently.
    pub fn build_multistream(dump_path: &str, ranges: &[StreamRange]) -> Result<Self> {
        info!(
            streams = ranges.len(),
            "Building index from multistream dump: {}", dump_path
        );

        let pb = progress_spinner();
        let page_count = AtomicU64::new(0);
        let dump = crate::multistream::open_dump(dump_path)?;

        // Parallel fold: each thread collects into local vecs, then reduce
        // merges. Any unreadable stream fails the whole build.
        let (articles_vec, redirects_vec) = ranges
            .par_iter()
            .try_fold(
                || (Vec::new(), Vec::new()),
                |(mut articles, mut redirects), range| -> Result<_> {
                    let pages = crate::multistream::parse_stream(&dump, range, true)?;
                    for page in pages {
                        let count = page_count.fetch_add(1, Ordering::Relaxed);
                        if (count + 1).is_multiple_of(PROGRESS_INTERVAL as u64) {
                            pb.set_message(format!("Indexing: {} pages", count + 1));
                        }
                        match page.page_type {
                            PageType::Article => {
                                articles.push((into_canonical(page.title), page.id));
                            }
                            PageType::Redirect(target) => {
                                redirects
                                    .push((into_canonical(page.title), into_canonical(target)));
                            }
                            _ => {}
                        }
                    }
                    Ok((articles, redirects))
                },
            )
            .try_reduce(
                || (Vec::new(), Vec::new()),
                |(mut a1, mut r1), (a2, r2)| {
                    a1.extend(a2);
                    r1.extend(r2);
                    Ok((a1, r1))
                },
            )?;

        pb.finish_and_clear();

        let index = Self::new(
            articles_vec.into_iter().collect(),
            redirects_vec.into_iter().collect(),
        );
        info!(
            articles = index.title_to_id.len(),
            redirects = index.redirect_to_id.len(),
            "Index built successfully (multistream)"
        );
        Ok(index)
    }

    /// The article map and the resolved redirect map, for the index cache.
    #[must_use]
    pub fn maps(&self) -> (&FxHashMap<String, u32>, &FxHashMap<String, u32>) {
        (&self.title_to_id, &self.redirect_to_id)
    }

    /// Returns (article_count, resolved_redirect_count).
    #[must_use]
    pub fn stats(&self) -> (usize, usize) {
        (self.title_to_id.len(), self.redirect_to_id.len())
    }

    /// Resolves a title, in any spelling MediaWiki accepts, to its page ID,
    /// following up to `REDIRECT_MAX_DEPTH` redirect hops.
    #[must_use]
    pub fn resolve_id(&self, title: &str) -> Option<u32> {
        self.resolve_canonical(&normalize_title(title))
    }

    /// `resolve_id` for a title already in `normalize_title` form, so a
    /// caller that normalized it once does not pay for it twice.
    #[must_use]
    pub(crate) fn resolve_canonical(&self, title: &str) -> Option<u32> {
        self.title_to_id
            .get(title)
            .or_else(|| self.redirect_to_id.get(title))
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_index(articles: Vec<(&str, u32)>, redirects: Vec<(&str, &str)>) -> WikiIndex {
        WikiIndex::new(
            articles
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            redirects
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    /// A chain of `hops` redirects, R0 -> R1 -> ... -> R{hops}, ending at an article.
    fn chain_index(hops: u32) -> WikiIndex {
        let redirects = (0..hops)
            .map(|i| (format!("R{i}"), format!("R{}", i + 1)))
            .collect();
        WikiIndex::new([(format!("R{hops}"), 1)].into_iter().collect(), redirects)
    }

    #[test]
    fn resolve_direct_title() {
        let index = make_index(vec![("Rust", 1), ("Python", 2)], vec![]);
        assert_eq!(index.resolve_id("Rust"), Some(1));
        assert_eq!(index.resolve_id("Python"), Some(2));
    }

    #[test]
    fn resolve_single_redirect() {
        let index = make_index(
            vec![("Rust (programming language)", 1)],
            vec![("Rust", "Rust (programming language)")],
        );
        assert_eq!(index.resolve_id("Rust"), Some(1));
    }

    #[test]
    fn resolve_redirect_chain() {
        let index = make_index(vec![("C", 1)], vec![("A", "B"), ("B", "C")]);
        assert_eq!(index.resolve_id("A"), Some(1));
        assert_eq!(index.resolve_id("B"), Some(1));
    }

    #[test]
    fn resolve_redirect_at_max_depth() {
        assert_eq!(chain_index(REDIRECT_MAX_DEPTH).resolve_id("R0"), Some(1));
    }

    #[test]
    fn resolve_redirect_exceeds_max_depth() {
        let index = chain_index(REDIRECT_MAX_DEPTH + 1);
        assert_eq!(index.resolve_id("R0"), None);
        // One hop further along, the chain is short enough.
        assert_eq!(index.resolve_id("R1"), Some(1));
    }

    #[test]
    fn resolve_circular_redirect() {
        let index = make_index(vec![], vec![("A", "B"), ("B", "C"), ("C", "A")]);
        assert_eq!(index.resolve_id("A"), None);
    }

    #[test]
    fn resolve_self_redirect() {
        let index = make_index(vec![], vec![("A", "A")]);
        assert_eq!(index.resolve_id("A"), None);
    }

    #[test]
    fn resolve_nonexistent_title() {
        let index = make_index(vec![("Rust", 1)], vec![]);
        assert_eq!(index.resolve_id("Python"), None);
    }

    #[test]
    fn resolve_redirect_to_nonexistent() {
        let index = make_index(vec![], vec![("A", "B")]);
        assert_eq!(index.resolve_id("A"), None);
    }

    #[test]
    fn article_wins_over_a_redirect_of_the_same_title() {
        let index = make_index(vec![("A", 1), ("B", 2)], vec![("A", "B")]);
        assert_eq!(index.resolve_id("A"), Some(1));
    }

    #[test]
    fn resolve_empty_index() {
        let index = make_index(vec![], vec![]);
        assert_eq!(index.resolve_id("Anything"), None);
    }

    #[test]
    fn resolve_is_case_insensitive_only_in_first_letter() {
        let index = make_index(vec![("Rust", 1)], vec![]);
        assert_eq!(index.resolve_id("Rust"), Some(1));
        assert_eq!(index.resolve_id("rust"), Some(1));
        assert_eq!(index.resolve_id("RUST"), None);
    }

    #[test]
    fn resolve_accepts_mediawiki_spellings() {
        let index = make_index(vec![("United States", 7)], vec![]);
        assert_eq!(index.resolve_id("United_States"), Some(7));
        assert_eq!(index.resolve_id(" United States "), Some(7));
        assert_eq!(index.resolve_id("united States#History"), Some(7));
        assert_eq!(index.resolve_id(":United States"), Some(7));
    }

    #[test]
    fn maps_roundtrip_through_from_resolved() {
        let original = make_index(
            vec![("Rust", 1), ("Python", 2), ("Go", 3)],
            vec![("RS", "Rust"), ("Py", "Python")],
        );
        let (articles, redirects) = original.maps();
        let restored = WikiIndex::from_resolved(articles.clone(), redirects.clone());

        assert_eq!(restored.resolve_id("Rust"), Some(1));
        assert_eq!(restored.resolve_id("Go"), Some(3));
        assert_eq!(restored.resolve_id("RS"), Some(1));
        assert_eq!(restored.resolve_id("Py"), Some(2));
    }

    #[test]
    fn stats_count_articles_and_resolved_redirects() {
        let index = make_index(
            vec![("A", 1), ("B", 2), ("C", 3)],
            vec![("X", "A"), ("Y", "B"), ("Z", "Missing")],
        );
        assert_eq!(index.stats(), (3, 2));
    }

    #[test]
    fn stats_empty_index() {
        let index = make_index(vec![], vec![]);
        assert_eq!(index.stats(), (0, 0));
    }
}
