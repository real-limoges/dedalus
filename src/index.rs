use crate::models::PageType;
use crate::parser::WikiReader;
use indicatif::ProgressBar;
use std::collections::HashMap;

pub struct WikiIndex {
    title_to_id: HashMap<String, u32>,
    redirects: HashMap<String, String>,
}

impl WikiIndex {
    pub fn build(path: &str) -> Self {
        let mut title_to_id = HashMap::new();
        let mut redirects = HashMap::new();
        let reader = WikiReader::new(path, false).expect("Failed to open wiki dump");
        let pb = ProgressBar::new_spinner();

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
            if page.id % 1000 == 0 {
                pb.tick();
            }
        }
        Self {
            title_to_id,
            redirects,
        }
    }
    pub fn resolve_id(&self, title: &str) -> Option<u32> {
        let mut current = title;
        let mut depth = 0;

        while depth < 5 {
            if let Some(id) = self.title_to_id.get(current) {
                return Some(*id);
            }
            if let Some(target) = self.redirects.get(current) {
                current = target;
                depth += 1;
            } else {
                return None;
            }
        }
        None
    }
}
