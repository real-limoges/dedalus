//! In-memory title-to-ID index with redirect chain resolution.
//!
//! `WikiIndex` wraps pre-sized `FxHashMap`s (8M articles, 10M redirects) for
//! fast lookup of page IDs by title. Titles are stored and looked up in the
//! canonical form from `title::normalize_title`. Follows redirect chains up to
//! `REDIRECT_MAX_DEPTH` hops.
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

/// In-memory title-to-ID index with redirect resolution.
pub struct WikiIndex {
    title_to_id: FxHashMap<String, u32>,
    redirects: FxHashMap<String, String>,
}

impl std::fmt::Debug for WikiIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WikiIndex")
            .field("articles", &self.title_to_id.len())
            .field("redirects", &self.redirects.len())
            .finish()
    }
}

impl WikiIndex {
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
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            indicatif::ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .expect("valid progress template"),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

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

        info!(
            articles = title_to_id.len(),
            redirects = redirects.len(),
            "Index built successfully"
        );

        Ok(Self {
            title_to_id,
            redirects,
        })
    }

    /// Build index using multistream parallel parsing.
    /// Each rayon worker decompresses and parses streams independently.
    pub fn build_multistream(dump_path: &str, ranges: &[StreamRange]) -> Result<Self> {
        info!(
            streams = ranges.len(),
            "Building index from multistream dump: {}", dump_path
        );

        let pb = ProgressBar::new_spinner();
        pb.set_style(
            indicatif::ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .expect("valid progress template"),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        let page_count = AtomicU64::new(0);

        // Parallel fold: each thread collects into local vecs, then reduce
        // merges. Any unreadable stream fails the whole build.
        let (articles_vec, redirects_vec) = ranges
            .par_iter()
            .try_fold(
                || (Vec::new(), Vec::new()),
                |(mut articles, mut redirects), range| -> Result<_> {
                    let pages = crate::multistream::parse_stream(dump_path, range, true)?;
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

        let mut title_to_id: FxHashMap<String, u32> =
            FxHashMap::with_capacity_and_hasher(articles_vec.len(), Default::default());
        for (title, id) in articles_vec {
            title_to_id.insert(title, id);
        }

        let mut redirects: FxHashMap<String, String> =
            FxHashMap::with_capacity_and_hasher(redirects_vec.len(), Default::default());
        for (title, target) in redirects_vec {
            redirects.insert(title, target);
        }

        info!(
            articles = title_to_id.len(),
            redirects = redirects.len(),
            "Index built successfully (multistream)"
        );

        Ok(Self {
            title_to_id,
            redirects,
        })
    }

    /// Returns references to the underlying title-to-ID and redirect maps.
    #[must_use]
    pub fn maps(&self) -> (&FxHashMap<String, u32>, &FxHashMap<String, String>) {
        (&self.title_to_id, &self.redirects)
    }

    /// Constructs an index from pre-built maps (e.g. deserialized from cache).
    pub fn from_maps(
        title_to_id: FxHashMap<String, u32>,
        redirects: FxHashMap<String, String>,
    ) -> Self {
        Self {
            title_to_id,
            redirects,
        }
    }

    #[cfg(test)]
    #[allow(clippy::type_complexity)]
    pub fn to_serializable(&self) -> (Vec<(String, u32)>, Vec<(String, String)>) {
        let articles: Vec<(String, u32)> = self
            .title_to_id
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        let redirects: Vec<(String, String)> = self
            .redirects
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        (articles, redirects)
    }

    #[cfg(test)]
    pub fn from_serializable(
        articles: Vec<(String, u32)>,
        redirects: Vec<(String, String)>,
    ) -> Self {
        Self {
            title_to_id: articles.into_iter().collect(),
            redirects: redirects.into_iter().collect(),
        }
    }

    /// Returns (article_count, redirect_count).
    #[must_use]
    pub fn stats(&self) -> (usize, usize) {
        (self.title_to_id.len(), self.redirects.len())
    }

    /// Resolves a title, in any spelling MediaWiki accepts, to its page ID,
    /// following up to `REDIRECT_MAX_DEPTH` redirect hops.
    #[must_use]
    pub fn resolve_id(&self, title: &str) -> Option<u32> {
        let normalized = normalize_title(title);
        let mut current: &str = &normalized;

        // Hop 0 is the title itself; hops 1..=REDIRECT_MAX_DEPTH follow redirects.
        for _hop in 0..=REDIRECT_MAX_DEPTH {
            if let Some(id) = self.title_to_id.get(current) {
                return Some(*id);
            }
            let target = self.redirects.get(current)?;
            debug!(from = current, to = target, "Following redirect");
            current = target;
        }
        debug!(title = title, "Redirect chain too deep");
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_index(articles: Vec<(&str, u32)>, redirects: Vec<(&str, &str)>) -> WikiIndex {
        WikiIndex {
            title_to_id: articles
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            redirects: redirects
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
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
    }

    #[test]
    fn resolve_redirect_at_max_depth() {
        // A chain of exactly REDIRECT_MAX_DEPTH hops: R0 -> R1 -> ... -> R{MAX}, an article.
        let mut redirects = Vec::new();
        for i in 0..REDIRECT_MAX_DEPTH {
            redirects.push((format!("R{}", i), format!("R{}", i + 1)));
        }
        let final_title = format!("R{}", REDIRECT_MAX_DEPTH);
        let articles = vec![(final_title.as_str(), 1u32)];

        let index = WikiIndex {
            title_to_id: articles
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            redirects: redirects.into_iter().collect(),
        };

        assert_eq!(index.resolve_id("R0"), Some(1));
    }

    #[test]
    fn resolve_redirect_exceeds_max_depth() {
        // Chain of REDIRECT_MAX_DEPTH + 1 redirects should fail
        let mut redirects = Vec::new();
        for i in 0..=REDIRECT_MAX_DEPTH {
            redirects.push((format!("R{}", i), format!("R{}", i + 1)));
        }
        let final_title = format!("R{}", REDIRECT_MAX_DEPTH + 1);

        let index = WikiIndex {
            title_to_id: [(final_title, 1)].into_iter().collect(),
            redirects: redirects.into_iter().collect(),
        };

        assert_eq!(index.resolve_id("R0"), None);
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
    fn serialization_roundtrip() {
        let original = make_index(
            vec![("Rust", 1), ("Python", 2), ("Go", 3)],
            vec![("RS", "Rust"), ("Py", "Python")],
        );

        let (articles, redirects) = original.to_serializable();
        let restored = WikiIndex::from_serializable(articles, redirects);

        assert_eq!(restored.resolve_id("Rust"), Some(1));
        assert_eq!(restored.resolve_id("Python"), Some(2));
        assert_eq!(restored.resolve_id("Go"), Some(3));
        assert_eq!(restored.resolve_id("RS"), Some(1));
        assert_eq!(restored.resolve_id("Py"), Some(2));
    }

    #[test]
    fn stats_returns_correct_counts() {
        let index = make_index(
            vec![("A", 1), ("B", 2), ("C", 3)],
            vec![("X", "A"), ("Y", "B")],
        );
        let (article_count, redirect_count) = index.stats();
        assert_eq!(article_count, 3);
        assert_eq!(redirect_count, 2);
    }

    #[test]
    fn stats_empty_index() {
        let index = make_index(vec![], vec![]);
        let (article_count, redirect_count) = index.stats();
        assert_eq!(article_count, 0);
        assert_eq!(redirect_count, 0);
    }
}
