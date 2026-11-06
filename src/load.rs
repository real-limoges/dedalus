//! Load pass: copies the merged CSVs into a DuckDB database file.
//!
//! Each CSV type becomes one table, read with DuckDB's `read_csv` and explicit
//! column types, so the typed headers (`id:ID`, `:START_ID`) are not parsed.
//! `analytics.csv` becomes the `analytics` table when it exists.
//!
//! The database is built in a `.tmp` file and renamed over the target only
//! once every table is loaded, so a failed load leaves any earlier database
//! untouched.

use crate::analytics::ANALYTICS_FILE;
use crate::csv_util::{self, CsvLayout, CsvType};
use anyhow::{Context, Result, bail};
use duckdb::Connection;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::info;

/// Default database file name, relative to the output directory.
pub const DEFAULT_DB_FILE: &str = "wikipedia.duckdb";

/// Configuration for the load step.
#[derive(Debug, Clone)]
pub struct LoadConfig {
    /// Directory holding the merged CSVs.
    pub output_dir: String,
    /// Database file; relative paths are taken from `output_dir`.
    pub db_path: String,
    /// Replace an existing database instead of refusing to.
    pub replace: bool,
}

impl LoadConfig {
    /// A config for `output_dir` with the default database path.
    pub fn new(output_dir: &str) -> Self {
        Self {
            output_dir: output_dir.to_string(),
            db_path: DEFAULT_DB_FILE.to_string(),
            replace: false,
        }
    }

    /// The database path, resolved against the output directory.
    pub fn resolved_db_path(&self) -> PathBuf {
        let path = Path::new(&self.db_path);
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            Path::new(&self.output_dir).join(path)
        }
    }
}

/// What a load wrote.
#[derive(Debug)]
pub struct LoadStats {
    pub db_path: PathBuf,
    /// Each table created, with its row count, in load order.
    pub tables: Vec<(&'static str, u64)>,
    pub elapsed_secs: f64,
}

/// One table and the CSV it is read from.
struct TableSpec {
    name: &'static str,
    file: String,
    /// The CSV's columns, as a DuckDB struct literal of name to type.
    columns: &'static str,
    /// The columns kept in the table. The constant `:LABEL` and `:TYPE`
    /// columns are dropped, except `:TYPE` on `links`, where it varies.
    select: &'static str,
}

fn table_spec(csv_type: CsvType) -> TableSpec {
    let (name, columns, select) = match csv_type {
        CsvType::Nodes => (
            "articles",
            "{'id': 'UINTEGER', 'title': 'VARCHAR', 'label': 'VARCHAR'}",
            "id, title",
        ),
        CsvType::Edges => (
            "links",
            "{'src': 'UINTEGER', 'dst': 'UINTEGER', 'type': 'VARCHAR'}",
            "src, dst, type",
        ),
        CsvType::Categories => (
            "categories",
            "{'id': 'VARCHAR', 'name': 'VARCHAR', 'label': 'VARCHAR'}",
            "id, name",
        ),
        CsvType::ArticleCategories => (
            "article_categories",
            "{'article_id': 'UINTEGER', 'category_id': 'VARCHAR', 'type': 'VARCHAR'}",
            "article_id, category_id",
        ),
        CsvType::ImageNodes => (
            "images",
            "{'id': 'VARCHAR', 'filename': 'VARCHAR', 'label': 'VARCHAR'}",
            "id, filename",
        ),
        CsvType::ArticleImages => (
            "article_images",
            "{'article_id': 'UINTEGER', 'image_id': 'VARCHAR', 'type': 'VARCHAR'}",
            "article_id, image_id",
        ),
        CsvType::ExternalLinkNodes => (
            "external_links",
            "{'id': 'VARCHAR', 'url': 'VARCHAR', 'label': 'VARCHAR'}",
            "id, url",
        ),
        CsvType::ArticleExternalLinks => (
            "article_external_links",
            "{'article_id': 'UINTEGER', 'link_id': 'VARCHAR', 'type': 'VARCHAR'}",
            "article_id, link_id",
        ),
    };
    TableSpec {
        name,
        file: csv_type.merged_file_name(),
        columns,
        select,
    }
}

fn analytics_spec() -> TableSpec {
    TableSpec {
        name: "analytics",
        file: ANALYTICS_FILE.to_string(),
        columns: "{'id': 'UINTEGER', 'pagerank': 'DOUBLE', 'community': 'UINTEGER', \
                  'degree': 'UINTEGER'}",
        select: "id, pagerank, community, degree",
    }
}

/// Loads the merged CSVs (and `analytics.csv`, if present) into DuckDB.
pub fn run_load(config: &LoadConfig) -> Result<LoadStats> {
    let start = Instant::now();
    let output_dir = &config.output_dir;
    let db_path = config.resolved_db_path();

    // Check every input before touching any database file.
    if csv_util::detect_csv_layout(output_dir)? != CsvLayout::Single {
        bail!(
            "Load requires merged CSVs.\n\
             Run 'dedalus merge-csvs -o {output_dir}' first, or extract with --csv-shards 1."
        );
    }
    let mut specs: Vec<TableSpec> = CsvType::ALL.iter().map(|&t| table_spec(t)).collect();
    for spec in &specs {
        let path = Path::new(output_dir).join(&spec.file);
        if !path.is_file() {
            bail!("Missing {}; run 'dedalus extract' first", path.display());
        }
    }
    if Path::new(output_dir).join(ANALYTICS_FILE).is_file() {
        specs.push(analytics_spec());
    } else {
        info!("No {ANALYTICS_FILE}; loading without the analytics table");
    }
    if db_path.exists() && !config.replace {
        bail!(
            "{} already exists; pass --clean to replace it",
            db_path.display()
        );
    }

    let tmp_path = with_suffix(&db_path, ".tmp");
    remove_database_files(&tmp_path)?;
    let tables = build_database(&tmp_path, output_dir, &specs)
        .with_context(|| format!("Failed to build {}", tmp_path.display()))?;

    remove_database_files(&db_path)?;
    fs::rename(&tmp_path, &db_path)
        .with_context(|| format!("Failed to move the database to {}", db_path.display()))?;

    info!(path = %db_path.display(), "DuckDB load complete");
    Ok(LoadStats {
        db_path,
        tables,
        elapsed_secs: start.elapsed().as_secs_f64(),
    })
}

/// Creates every table in a new database at `path`. Returns row counts.
fn build_database(
    path: &Path,
    output_dir: &str,
    specs: &[TableSpec],
) -> Result<Vec<(&'static str, u64)>> {
    let conn = Connection::open(path)?;
    let mut tables = Vec::with_capacity(specs.len());
    for spec in specs {
        let csv_path = Path::new(output_dir).join(&spec.file);
        info!(table = spec.name, file = %csv_path.display(), "Loading table");
        conn.execute_batch(&format!(
            "CREATE TABLE {name} AS SELECT {select} FROM read_csv({path}, header = true, \
             quote = '\"', escape = '\"', columns = {columns});",
            name = spec.name,
            select = spec.select,
            path = sql_string(&csv_path.to_string_lossy()),
            columns = spec.columns,
        ))
        .with_context(|| format!("Failed to load {}", csv_path.display()))?;
        let rows: i64 =
            conn.query_row(&format!("SELECT count(*) FROM {}", spec.name), [], |row| {
                row.get(0)
            })?;
        tables.push((spec.name, rows as u64));
    }
    conn.execute_batch("CHECKPOINT;")?;
    conn.close().map_err(|(_, e)| e)?;
    Ok(tables)
}

/// A SQL string literal for `s`.
fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// Removes a database file and its write-ahead log, if they exist.
fn remove_database_files(path: &Path) -> Result<()> {
    for file in [path.to_path_buf(), with_suffix(path, ".wal")] {
        if file.exists() {
            fs::remove_file(&file)
                .with_context(|| format!("Failed to remove {}", file.display()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_merged_csvs(dir: &Path) {
        let rows = |csv_type: CsvType| -> &str {
            match csv_type {
                CsvType::Nodes => "1,\"O'Brien, \"\"Pat\"\"\",Page\n2,Back\\slash,Page\n",
                CsvType::Edges => "1,2,LINKS_TO\n2,1,SEE_ALSO\n",
                CsvType::Categories => "Science,Science,Category\n",
                CsvType::ArticleCategories => "1,Science,HAS_CATEGORY\n",
                _ => "",
            }
        };
        for &csv_type in CsvType::ALL {
            let header = csv_type.header().join(",");
            std::fs::write(
                dir.join(csv_type.merged_file_name()),
                format!("{header}\n{}", rows(csv_type)),
            )
            .unwrap();
        }
    }

    fn config_for(dir: &TempDir) -> LoadConfig {
        LoadConfig::new(dir.path().to_str().unwrap())
    }

    fn query_strings(db: &Path, sql: &str) -> Vec<String> {
        let conn = Connection::open(db).unwrap();
        let mut stmt = conn.prepare(sql).unwrap();
        stmt.query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    #[test]
    fn load_creates_a_table_per_csv_and_keeps_awkward_titles() {
        let dir = TempDir::new().unwrap();
        write_merged_csvs(dir.path());

        let stats = run_load(&config_for(&dir)).unwrap();

        let counts: Vec<(&str, u64)> = stats.tables.clone();
        assert_eq!(counts[0], ("articles", 2));
        assert_eq!(counts[1], ("links", 2));
        assert_eq!(counts.len(), CsvType::ALL.len());
        assert_eq!(
            query_strings(&stats.db_path, "SELECT title FROM articles ORDER BY id"),
            vec!["O'Brien, \"Pat\"", "Back\\slash"]
        );
        assert_eq!(
            query_strings(&stats.db_path, "SELECT type FROM links ORDER BY src"),
            vec!["LINKS_TO", "SEE_ALSO"]
        );
        assert!(!with_suffix(&stats.db_path, ".tmp").exists());
    }

    #[test]
    fn load_includes_analytics_when_present() {
        let dir = TempDir::new().unwrap();
        write_merged_csvs(dir.path());
        std::fs::write(
            dir.path().join(ANALYTICS_FILE),
            "id:ID,pagerank:double,community:long,degree:int\n1,0.6,1,2\n2,0.4,1,2\n",
        )
        .unwrap();

        let stats = run_load(&config_for(&dir)).unwrap();

        assert_eq!(stats.tables.last(), Some(&("analytics", 2)));
    }

    #[test]
    fn load_refuses_to_replace_without_clean_and_replaces_with_it() {
        let dir = TempDir::new().unwrap();
        write_merged_csvs(dir.path());
        run_load(&config_for(&dir)).unwrap();

        let err = run_load(&config_for(&dir)).unwrap_err();
        assert!(err.to_string().contains("pass --clean"));

        let replace = LoadConfig {
            replace: true,
            ..config_for(&dir)
        };
        assert!(run_load(&replace).is_ok());
    }

    #[test]
    fn a_failed_load_keeps_the_old_database() {
        let dir = TempDir::new().unwrap();
        write_merged_csvs(dir.path());
        let db = run_load(&config_for(&dir)).unwrap().db_path;
        std::fs::write(
            dir.path().join("edges.csv"),
            ":START_ID,:END_ID,:TYPE\nx,2,LINKS_TO\n",
        )
        .unwrap();

        let replace = LoadConfig {
            replace: true,
            ..config_for(&dir)
        };
        assert!(run_load(&replace).is_err());

        assert_eq!(
            query_strings(&db, "SELECT type FROM links ORDER BY src"),
            vec!["LINKS_TO", "SEE_ALSO"]
        );
    }

    #[test]
    fn load_checks_inputs_before_touching_the_database() {
        let dir = TempDir::new().unwrap();
        write_merged_csvs(dir.path());
        std::fs::remove_file(dir.path().join("article_images.csv")).unwrap();
        std::fs::write(dir.path().join(DEFAULT_DB_FILE), "keep me").unwrap();

        let replace = LoadConfig {
            replace: true,
            ..config_for(&dir)
        };
        let err = run_load(&replace).unwrap_err();

        assert!(err.to_string().contains("article_images.csv"));
        assert_eq!(
            std::fs::read_to_string(dir.path().join(DEFAULT_DB_FILE)).unwrap(),
            "keep me"
        );
    }
}
