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
        let reader = WikiReader::new(path, false)
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
