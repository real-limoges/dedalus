# Dedalus Wikipedia Processing Pipeline Makefile
#
# Quick start:
#   make pipeline WIKI_DUMP=path/to/enwiki-latest-pages-articles.xml.bz2
#
# Or for testing with a small dump:
#   make test-pipeline WIKI_DUMP=path/to/small-dump.xml.bz2

# ============================================================================
# Configuration Variables
# ============================================================================

# Input Wikipedia dump file (required)
WIKI_DUMP ?= enwiki-latest-pages-articles.xml.bz2

# Output directory
OUTPUT_DIR ?= output

# Extraction settings
CSV_SHARDS ?= 8
SHARD_COUNT ?= 1000
LIMIT ?=

# Verbosity (-v, -vv, -vvv)
VERBOSE ?= -v

# Binary location
DEDALUS ?= ./target/release/dedalus

# Build configuration
CARGO_FLAGS ?= --release

# ============================================================================
# Phony Targets
# ============================================================================

.PHONY: all help build test clean clean-output clean-shards clean-all
.PHONY: extract merge import pipeline
.PHONY: hybrid-pipeline standard-pipeline test-pipeline
.PHONY: check-dump

# ============================================================================
# Main Targets
# ============================================================================

# Default target: show help
all: help

# Help message
help:
	@echo "Dedalus Wikipedia Processing Pipeline"
	@echo ""
	@echo "Usage:"
	@echo "  make pipeline WIKI_DUMP=<path-to-dump.xml.bz2>    Run full hybrid pipeline"
	@echo "  make standard-pipeline WIKI_DUMP=<path>           Run standard pipeline (1 shard)"
	@echo "  make test-pipeline WIKI_DUMP=<path> LIMIT=10000   Test with limited pages"
	@echo ""
	@echo "Individual Steps:"
	@echo "  make build                                        Build release binary"
	@echo "  make extract                                      Extract Wikipedia dump"
	@echo "  make merge                                        Merge sharded CSVs"
	@echo "  make import                                       Import into Neo4j"
	@echo ""
	@echo "Configuration (set with VAR=value):"
	@echo "  WIKI_DUMP    Path to Wikipedia dump              (default: enwiki-latest-pages-articles.xml.bz2)"
	@echo "  OUTPUT_DIR   Output directory                    (default: output)"
	@echo "  CSV_SHARDS   Number of CSV shards                (default: 8)"
	@echo "  SHARD_COUNT  Number of JSON blob shards          (default: 1000)"
	@echo "  LIMIT        Limit articles processed (testing)  (default: none)"
	@echo "  VERBOSE      Verbosity level                     (default: -v)"
	@echo ""
	@echo "Testing & Cleanup:"
	@echo "  make test                                         Run all tests"
	@echo "  make clean                                        Clean build artifacts"
	@echo "  make clean-output                                 Clean output directory"
	@echo "  make clean-shards                                 Archive sharded CSVs to shards/ subdirectory"
	@echo "  make clean-all                                    Clean everything"
	@echo ""
	@echo "Examples:"
	@echo "  make pipeline WIKI_DUMP=enwiki.xml.bz2"
	@echo "  make test-pipeline WIKI_DUMP=small.xml.bz2 LIMIT=10000"
	@echo "  make extract VERBOSE=-vv CSV_SHARDS=16"

# ============================================================================
# Build Target
# ============================================================================

build:
	@echo "==> Building Dedalus (release mode)..."
	cargo build $(CARGO_FLAGS)
	@echo ""

# ============================================================================
# Pipeline Workflows
# ============================================================================

# Hybrid pipeline (recommended): 8 shards → merge → admin import
# Best for full Wikipedia dumps (fastest overall)
pipeline: hybrid-pipeline

hybrid-pipeline: check-dump build extract merge import
	@echo ""
	@echo "==> Hybrid pipeline complete!"
	@echo "    Extraction: $(OUTPUT_DIR) ($(CSV_SHARDS) shards merged)"
	@echo "    Neo4j:      bolt://localhost:7687"
	@echo "    Browser:    http://localhost:7474"
	@echo ""

# Standard pipeline: 1 shard → admin import
# Slower extraction but simpler (no merge step)
standard-pipeline: check-dump build
	@echo "==> Running standard pipeline (single shard)..."
	$(DEDALUS) extract -i $(WIKI_DUMP) -o $(OUTPUT_DIR) --csv-shards 1 --shard-count $(SHARD_COUNT) $(if $(LIMIT),--limit $(LIMIT)) $(VERBOSE)
	$(DEDALUS) import -o $(OUTPUT_DIR) --admin-import
	@echo ""
	@echo "==> Standard pipeline complete!"
	@echo "    Neo4j: bolt://localhost:7687"
	@echo ""

# Test pipeline: limited pages for quick validation
test-pipeline: LIMIT ?= 10000
test-pipeline: check-dump build
	@echo "==> Running test pipeline (limit: $(LIMIT) pages)..."
	$(DEDALUS) extract -i $(WIKI_DUMP) -o $(OUTPUT_DIR) --csv-shards $(CSV_SHARDS) --shard-count $(SHARD_COUNT) --limit $(LIMIT) $(VERBOSE)
	@if [ $(CSV_SHARDS) -gt 1 ]; then \
		echo "==> Merging CSVs..."; \
		$(DEDALUS) merge-csvs -o $(OUTPUT_DIR); \
		echo "==> Archiving sharded CSV files..."; \
		mkdir -p $(OUTPUT_DIR)/shards; \
		mv -f $(OUTPUT_DIR)/*_[0-9][0-9][0-9].csv $(OUTPUT_DIR)/shards/ 2>/dev/null || true; \
	fi
	$(DEDALUS) import -o $(OUTPUT_DIR) --admin-import
	@echo ""
	@echo "==> Test pipeline complete!"
	@echo "    Processed: $(LIMIT) pages"
	@echo "    Neo4j:     bolt://localhost:7687"
	@echo ""

# ============================================================================
# Individual Pipeline Steps
# ============================================================================

# Check if dump file exists
check-dump:
	@if [ ! -f "$(WIKI_DUMP)" ]; then \
		echo "ERROR: Wikipedia dump not found: $(WIKI_DUMP)"; \
		echo ""; \
		echo "Please specify a valid dump file:"; \
		echo "  make pipeline WIKI_DUMP=path/to/dump.xml.bz2"; \
		echo ""; \
		echo "Download Wikipedia dumps from:"; \
		echo "  https://dumps.wikimedia.org/enwiki/latest/"; \
		echo ""; \
		exit 1; \
	fi
	@echo "==> Using Wikipedia dump: $(WIKI_DUMP)"
	@du -h $(WIKI_DUMP)
	@echo ""

# Extract: Parse Wikipedia dump into CSVs and JSON blobs
extract: check-dump build
	@echo "==> Extracting Wikipedia dump..."
	@echo "    Input:       $(WIKI_DUMP)"
	@echo "    Output:      $(OUTPUT_DIR)"
	@echo "    CSV shards:  $(CSV_SHARDS)"
	@echo "    Blob shards: $(SHARD_COUNT)"
	@if [ -n "$(LIMIT)" ]; then echo "    Limit:       $(LIMIT) pages"; fi
	@echo ""
	$(DEDALUS) extract \
		-i $(WIKI_DUMP) \
		-o $(OUTPUT_DIR) \
		--csv-shards $(CSV_SHARDS) \
		--shard-count $(SHARD_COUNT) \
		$(if $(LIMIT),--limit $(LIMIT)) \
		$(VERBOSE)
	@echo ""

# Merge: Combine sharded CSVs into single files (required for admin import)
merge: build
	@if [ $(CSV_SHARDS) -eq 1 ]; then \
		echo "==> Skipping merge (CSV_SHARDS=1, no sharding used)"; \
		echo ""; \
	else \
		echo "==> Merging $(CSV_SHARDS) CSV shards..."; \
		$(DEDALUS) merge-csvs -o $(OUTPUT_DIR); \
		echo "==> Archiving sharded CSV files..."; \
		mkdir -p $(OUTPUT_DIR)/shards; \
		mv -f $(OUTPUT_DIR)/nodes_[0-9][0-9][0-9].csv $(OUTPUT_DIR)/shards/ 2>/dev/null || true; \
		mv -f $(OUTPUT_DIR)/edges_[0-9][0-9][0-9].csv $(OUTPUT_DIR)/shards/ 2>/dev/null || true; \
		mv -f $(OUTPUT_DIR)/categories_[0-9][0-9][0-9].csv $(OUTPUT_DIR)/shards/ 2>/dev/null || true; \
		mv -f $(OUTPUT_DIR)/article_categories_[0-9][0-9][0-9].csv $(OUTPUT_DIR)/shards/ 2>/dev/null || true; \
		mv -f $(OUTPUT_DIR)/images_[0-9][0-9][0-9].csv $(OUTPUT_DIR)/shards/ 2>/dev/null || true; \
		mv -f $(OUTPUT_DIR)/external_links_[0-9][0-9][0-9].csv $(OUTPUT_DIR)/shards/ 2>/dev/null || true; \
		echo "    Archived shards to $(OUTPUT_DIR)/shards/"; \
		echo ""; \
	fi

# Import: Load data into Neo4j using admin bulk import
import: build
	@echo "==> Importing into Neo4j (admin bulk import)..."
	@echo "    Output dir: $(OUTPUT_DIR)"
	@echo ""
	$(DEDALUS) import -o $(OUTPUT_DIR) --admin-import
	@echo ""
	@echo "==> Import complete!"
	@echo "    Bolt:    bolt://localhost:7687"
	@echo "    Browser: http://localhost:7474"
	@echo ""

# ============================================================================
# Testing & Quality
# ============================================================================

# Run all tests
test:
	@echo "==> Running tests..."
	cargo test --verbose
	@echo ""
	@echo "==> Running clippy..."
	cargo clippy -- -D warnings
	@echo ""
	@echo "==> Checking formatting..."
	cargo fmt -- --check
	@echo ""
	@echo "==> All checks passed!"
	@echo ""

# ============================================================================
# Cleanup Targets
# ============================================================================

# Clean build artifacts
clean:
	@echo "==> Cleaning build artifacts..."
	cargo clean
	@echo ""

# Clean output directory only
clean-output:
	@echo "==> Cleaning output directory: $(OUTPUT_DIR)"
	@if [ -d "$(OUTPUT_DIR)" ]; then \
		rm -rf $(OUTPUT_DIR); \
		echo "    Removed $(OUTPUT_DIR)"; \
	else \
		echo "    $(OUTPUT_DIR) does not exist"; \
	fi
	@echo ""

# Archive sharded CSV files to shards/ subdirectory (keeps merged single files)
clean-shards:
	@echo "==> Archiving sharded CSV files in: $(OUTPUT_DIR)"
	@if [ -d "$(OUTPUT_DIR)" ]; then \
		mkdir -p $(OUTPUT_DIR)/shards; \
		mv -f $(OUTPUT_DIR)/nodes_[0-9][0-9][0-9].csv $(OUTPUT_DIR)/shards/ 2>/dev/null || true; \
		mv -f $(OUTPUT_DIR)/edges_[0-9][0-9][0-9].csv $(OUTPUT_DIR)/shards/ 2>/dev/null || true; \
		mv -f $(OUTPUT_DIR)/categories_[0-9][0-9][0-9].csv $(OUTPUT_DIR)/shards/ 2>/dev/null || true; \
		mv -f $(OUTPUT_DIR)/article_categories_[0-9][0-9][0-9].csv $(OUTPUT_DIR)/shards/ 2>/dev/null || true; \
		mv -f $(OUTPUT_DIR)/images_[0-9][0-9][0-9].csv $(OUTPUT_DIR)/shards/ 2>/dev/null || true; \
		mv -f $(OUTPUT_DIR)/external_links_[0-9][0-9][0-9].csv $(OUTPUT_DIR)/shards/ 2>/dev/null || true; \
		echo "    Archived to $(OUTPUT_DIR)/shards/"; \
	else \
		echo "    $(OUTPUT_DIR) does not exist"; \
	fi
	@echo ""

# Clean everything (build + output)
clean-all: clean clean-output
	@echo "==> Cleaned all artifacts"
	@echo ""

# ============================================================================
# Advanced Targets
# ============================================================================

# Resume extraction from last checkpoint
resume: check-dump build
	@echo "==> Resuming extraction from checkpoint..."
	$(DEDALUS) extract \
		-i $(WIKI_DUMP) \
		-o $(OUTPUT_DIR) \
		--csv-shards $(CSV_SHARDS) \
		--shard-count $(SHARD_COUNT) \
		--resume \
		$(VERBOSE)
	@echo ""

# Clean extraction and start fresh
clean-extract: check-dump build
	@echo "==> Clean extraction (clearing previous output)..."
	$(DEDALUS) extract \
		-i $(WIKI_DUMP) \
		-o $(OUTPUT_DIR) \
		--csv-shards $(CSV_SHARDS) \
		--shard-count $(SHARD_COUNT) \
		--clean \
		$(VERBOSE)
	@echo ""

# Import with clean slate (removes existing Neo4j data)
clean-import: build
	@echo "==> Clean import (removing existing Neo4j data)..."
	$(DEDALUS) import -o $(OUTPUT_DIR) --admin-import --clean
	@echo ""

# Bolt-based import (slower, works with existing data)
bolt-import: build
	@echo "==> Importing via Bolt protocol..."
	$(DEDALUS) import -o $(OUTPUT_DIR)
	@echo ""

# Show statistics about output
stats:
	@echo "==> Output Statistics"
	@echo ""
	@echo "Directory: $(OUTPUT_DIR)"
	@echo ""
	@if [ -d "$(OUTPUT_DIR)" ]; then \
		echo "CSV Files:"; \
		ls -lh $(OUTPUT_DIR)/*.csv 2>/dev/null | awk '{print "  " $$9 " (" $$5 ")"}' || echo "  None found"; \
		echo ""; \
		echo "Blob Directories:"; \
		if [ -d "$(OUTPUT_DIR)/blobs" ]; then \
			echo "  Total blobs: $$(find $(OUTPUT_DIR)/blobs -name "*.json" | wc -l)"; \
			echo "  Disk usage: $$(du -sh $(OUTPUT_DIR)/blobs | awk '{print $$1}')"; \
		else \
			echo "  None found"; \
		fi; \
		echo ""; \
		echo "Total size: $$(du -sh $(OUTPUT_DIR) | awk '{print $$1}')"; \
	else \
		echo "Output directory does not exist"; \
	fi
	@echo ""
