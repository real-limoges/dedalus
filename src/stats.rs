use crate::checkpoint::CheckpointStats;
use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics collected during the extraction process
#[derive(Default)]
pub struct ExtractionStats {
    pub articles_processed: AtomicU64,
    pub edges_extracted: AtomicU64,
    pub blobs_written: AtomicU64,
    pub invalid_links: AtomicU64,
    pub categories_found: AtomicU64,
    pub category_edges: AtomicU64,
    pub see_also_edges: AtomicU64,
    pub infoboxes_extracted: AtomicU64,
    pub images_found: AtomicU64,
    pub external_links_found: AtomicU64,
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

    pub fn inc_categories(&self) {
        self.categories_found.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_categories(&self, count: u64) {
        self.categories_found.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_category_edges(&self, count: u64) {
        self.category_edges.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_see_also_edges(&self, count: u64) {
        self.see_also_edges.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_infoboxes(&self, count: u64) {
        self.infoboxes_extracted.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_images(&self, count: u64) {
        self.images_found.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_external_links(&self, count: u64) {
        self.external_links_found
            .fetch_add(count, Ordering::Relaxed);
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

    pub fn categories(&self) -> u64 {
        self.categories_found.load(Ordering::Relaxed)
    }

    pub fn category_edges(&self) -> u64 {
        self.category_edges.load(Ordering::Relaxed)
    }

    pub fn see_also_edges(&self) -> u64 {
        self.see_also_edges.load(Ordering::Relaxed)
    }

    pub fn infoboxes(&self) -> u64 {
        self.infoboxes_extracted.load(Ordering::Relaxed)
    }

    pub fn images(&self) -> u64 {
        self.images_found.load(Ordering::Relaxed)
    }

    pub fn external_links(&self) -> u64 {
        self.external_links_found.load(Ordering::Relaxed)
    }

    /// Create stats initialized from a checkpoint
    pub fn from_checkpoint(cp: &CheckpointStats) -> Self {
        Self {
            articles_processed: AtomicU64::new(cp.articles_processed),
            edges_extracted: AtomicU64::new(cp.edges_extracted),
            blobs_written: AtomicU64::new(cp.blobs_written),
            invalid_links: AtomicU64::new(cp.invalid_links),
            categories_found: AtomicU64::new(cp.categories_found),
            category_edges: AtomicU64::new(cp.category_edges),
            see_also_edges: AtomicU64::new(cp.see_also_edges),
            infoboxes_extracted: AtomicU64::new(cp.infoboxes_extracted),
            images_found: AtomicU64::new(cp.images_found),
            external_links_found: AtomicU64::new(cp.external_links_found),
        }
    }

    /// Convert to checkpoint stats for persistence
    pub fn to_checkpoint(&self) -> CheckpointStats {
        CheckpointStats {
            articles_processed: self.articles(),
            edges_extracted: self.edges(),
            blobs_written: self.blobs(),
            invalid_links: self.invalid(),
            categories_found: self.categories(),
            category_edges: self.category_edges(),
            see_also_edges: self.see_also_edges(),
            infoboxes_extracted: self.infoboxes(),
            images_found: self.images(),
            external_links_found: self.external_links(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_checkpoint_initializes_correctly() {
        let cp = CheckpointStats {
            articles_processed: 100,
            edges_extracted: 500,
            blobs_written: 90,
            invalid_links: 10,
            categories_found: 5,
            category_edges: 20,
            see_also_edges: 3,
            infoboxes_extracted: 8,
            images_found: 15,
            external_links_found: 12,
        };

        let stats = ExtractionStats::from_checkpoint(&cp);
        assert_eq!(stats.articles(), 100);
        assert_eq!(stats.edges(), 500);
        assert_eq!(stats.blobs(), 90);
        assert_eq!(stats.invalid(), 10);
        assert_eq!(stats.categories(), 5);
        assert_eq!(stats.category_edges(), 20);
        assert_eq!(stats.see_also_edges(), 3);
        assert_eq!(stats.infoboxes(), 8);
        assert_eq!(stats.images(), 15);
        assert_eq!(stats.external_links(), 12);
    }

    #[test]
    fn to_checkpoint_captures_state() {
        let stats = ExtractionStats::new();
        stats.inc_articles();
        stats.inc_articles();
        stats.add_edges(25);
        stats.inc_blobs();
        stats.add_invalid_links(5);
        stats.inc_categories();
        stats.add_category_edges(10);
        stats.add_see_also_edges(2);
        stats.add_infoboxes(3);
        stats.add_images(7);
        stats.add_external_links(4);

        let cp = stats.to_checkpoint();
        assert_eq!(cp.articles_processed, 2);
        assert_eq!(cp.edges_extracted, 25);
        assert_eq!(cp.blobs_written, 1);
        assert_eq!(cp.invalid_links, 5);
        assert_eq!(cp.categories_found, 1);
        assert_eq!(cp.category_edges, 10);
        assert_eq!(cp.see_also_edges, 2);
        assert_eq!(cp.infoboxes_extracted, 3);
        assert_eq!(cp.images_found, 7);
        assert_eq!(cp.external_links_found, 4);
    }

    #[test]
    fn checkpoint_roundtrip() {
        let original = ExtractionStats::new();
        original.inc_articles();
        original.add_edges(10);
        original.inc_blobs();
        original.add_invalid_links(2);
        original.inc_categories();
        original.add_category_edges(5);
        original.add_see_also_edges(1);
        original.add_infoboxes(2);
        original.add_images(3);
        original.add_external_links(4);

        let cp = original.to_checkpoint();
        let restored = ExtractionStats::from_checkpoint(&cp);

        assert_eq!(restored.articles(), original.articles());
        assert_eq!(restored.edges(), original.edges());
        assert_eq!(restored.blobs(), original.blobs());
        assert_eq!(restored.invalid(), original.invalid());
        assert_eq!(restored.categories(), original.categories());
        assert_eq!(restored.category_edges(), original.category_edges());
        assert_eq!(restored.see_also_edges(), original.see_also_edges());
        assert_eq!(restored.infoboxes(), original.infoboxes());
        assert_eq!(restored.images(), original.images());
        assert_eq!(restored.external_links(), original.external_links());
    }

    #[test]
    fn default_values_are_zero() {
        let stats = ExtractionStats::new();
        assert_eq!(stats.articles(), 0);
        assert_eq!(stats.edges(), 0);
        assert_eq!(stats.blobs(), 0);
        assert_eq!(stats.invalid(), 0);
        assert_eq!(stats.categories(), 0);
        assert_eq!(stats.category_edges(), 0);
        assert_eq!(stats.see_also_edges(), 0);
        assert_eq!(stats.infoboxes(), 0);
        assert_eq!(stats.images(), 0);
        assert_eq!(stats.external_links(), 0);
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
