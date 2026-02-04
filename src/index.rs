use crate::config::{PROGRESS_INTERVAL, REDIRECT_MAX_DEPTH};
use crate::models::PageType;
use crate::parser::WikiReader;
use anyhow::{Context, Result};
use indicatif::ProgressBar;
use std::collections::HashMap;
use tracing::{debug, info};

pub struct WikiIndex {
    title_to_id: HashMap<String, u32>,
    redirects: HashMap<String, String>,
}

impl WikiIndex {
    pub fn build(path: &str) -> Result<Self> {
        let mut title_to_id = HashMap::new();
        let mut redirects = HashMap::new();
        let reader = WikiReader::new(path, true)
            .with_context(|| format!("Failed to open wiki dump at: {}", path))?;
        let pb = ProgressBar::new_spinner();

        info!("Building index from: {}", path);

        for page in reader {
            match page.page_type {
                PageType::Article => {
                    title_to_id.insert(page.title, page.id);
                }
                PageType::Redirect(target) => {
                    redirects.insert(page.title, target);
                }
                _ => {}
            }
            if page.id % PROGRESS_INTERVAL == 0 {
                pb.tick();
            }
        }

        pb.finish_and_clear();

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

    pub fn resolve_id(&self, title: &str) -> Option<u32> {
        let mut current = title;
        let mut depth = 0;

        while depth < REDIRECT_MAX_DEPTH {
            if let Some(id) = self.title_to_id.get(current) {
                return Some(*id);
            }
            if let Some(target) = self.redirects.get(current) {
                debug!(from = current, to = target, "Following redirect");
                current = target;
                depth += 1;
            } else {
                return None;
            }
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
        // Chain of exactly REDIRECT_MAX_DEPTH hops should still resolve
        let mut redirects = Vec::new();
        for i in 0..(REDIRECT_MAX_DEPTH - 1) {
            redirects.push((format!("R{}", i), format!("R{}", i + 1)));
        }
        let final_title = format!("R{}", REDIRECT_MAX_DEPTH - 1);
        let articles = vec![(final_title.as_str(), 1u32)];

        let index = WikiIndex {
            title_to_id: articles
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            redirects: redirects.into_iter().map(|(k, v)| (k, v)).collect(),
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
    fn resolve_case_sensitive() {
        let index = make_index(vec![("Rust", 1)], vec![]);
        assert_eq!(index.resolve_id("Rust"), Some(1));
        assert_eq!(index.resolve_id("rust"), None);
        assert_eq!(index.resolve_id("RUST"), None);
    }
}
