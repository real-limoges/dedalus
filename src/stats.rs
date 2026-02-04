use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics collected during the extraction process
#[derive(Default)]
pub struct ExtractionStats {
    pub articles_processed: AtomicU64,
    pub edges_extracted: AtomicU64,
    pub blobs_written: AtomicU64,
    pub invalid_links: AtomicU64,
}

impl ExtractionStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_articles(&self) {
        self.articles_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_edges(&self, count: u64) {
        self.edges_extracted.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_blobs(&self) {
        self.blobs_written.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_invalid_links(&self, count: u64) {
        self.invalid_links.fetch_add(count, Ordering::Relaxed);
    }

    pub fn articles(&self) -> u64 {
        self.articles_processed.load(Ordering::Relaxed)
    }

    pub fn edges(&self) -> u64 {
        self.edges_extracted.load(Ordering::Relaxed)
    }

    pub fn blobs(&self) -> u64 {
        self.blobs_written.load(Ordering::Relaxed)
    }

    pub fn invalid(&self) -> u64 {
        self.invalid_links.load(Ordering::Relaxed)
    }
}
