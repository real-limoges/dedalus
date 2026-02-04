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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values_are_zero() {
        let stats = ExtractionStats::new();
        assert_eq!(stats.articles(), 0);
        assert_eq!(stats.edges(), 0);
        assert_eq!(stats.blobs(), 0);
        assert_eq!(stats.invalid(), 0);
    }

    #[test]
    fn inc_articles() {
        let stats = ExtractionStats::new();
        stats.inc_articles();
        stats.inc_articles();
        stats.inc_articles();
        assert_eq!(stats.articles(), 3);
    }

    #[test]
    fn add_edges() {
        let stats = ExtractionStats::new();
        stats.add_edges(5);
        stats.add_edges(3);
        assert_eq!(stats.edges(), 8);
    }

    #[test]
    fn inc_blobs() {
        let stats = ExtractionStats::new();
        stats.inc_blobs();
        assert_eq!(stats.blobs(), 1);
    }

    #[test]
    fn add_invalid_links() {
        let stats = ExtractionStats::new();
        stats.add_invalid_links(10);
        stats.add_invalid_links(7);
        assert_eq!(stats.invalid(), 17);
    }

    #[test]
    fn mixed_operations() {
        let stats = ExtractionStats::new();
        stats.inc_articles();
        stats.add_edges(10);
        stats.inc_blobs();
        stats.add_invalid_links(2);
        stats.inc_articles();
        stats.add_edges(5);

        assert_eq!(stats.articles(), 2);
        assert_eq!(stats.edges(), 15);
        assert_eq!(stats.blobs(), 1);
        assert_eq!(stats.invalid(), 2);
    }
}
