//! `dedalus download`: fetch one dated multistream dump and its index, verified
//! against Wikimedia's SHA-1 list.
//!
//! The files come from the dated directory (`enwiki/20261101/`), never from
//! `latest/`, so a resume cannot stitch two dumps together when `latest/` moves.
//! Each file downloads into `<name>.part` and is renamed only once its SHA-1
//! matches; a mismatched part is removed so a rerun starts over.

use anyhow::{Context, Result, bail, ensure};
use indicatif::{ProgressBar, ProgressStyle};
use sha1::{Digest, Sha1};
use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use tracing::{info, warn};
use ureq::Agent;
use ureq::http::Response;

/// Tried in this order.
pub const MIRRORS: &[&str] = &[
    "https://dumps.wikimedia.your.org/",
    "https://ftp.acc.umu.se/mirror/wikimedia.org/dumps/",
    "https://dumps.wikimedia.org/",
];

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Per read, so a mirror that stalls mid-file fails over instead of hanging.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

const BUFFER_SIZE: usize = 1 << 20;

/// `YYYYMMDD`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DumpDate(String);

impl FromStr for DumpDate {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        ensure!(
            s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit()),
            "a dump date is eight digits (YYYYMMDD), not {s:?}"
        );
        Ok(DumpDate(s.to_string()))
    }
}

impl fmt::Display for DumpDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub fn dump_name(date: &DumpDate) -> String {
    format!("enwiki-{date}-pages-articles-multistream.xml.bz2")
}

pub fn index_name(date: &DumpDate) -> String {
    format!("enwiki-{date}-pages-articles-multistream-index.txt.bz2")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpFile {
    pub name: String,
    pub sha1: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DumpRelease {
    pub date: DumpDate,
    pub dump: DumpFile,
    pub index: DumpFile,
}

pub struct DownloadJob {
    pub output_dir: PathBuf,
    /// Without one, the release `latest/` points at.
    pub date: Option<DumpDate>,
    /// Base URLs ending in `/`.
    pub mirrors: Vec<String>,
}

pub struct Downloaded {
    pub date: DumpDate,
    pub dump: PathBuf,
    pub index: PathBuf,
}

pub fn run_download(job: &DownloadJob) -> Result<Downloaded> {
    ensure!(!job.mirrors.is_empty(), "no mirrors to download from");
    fs::create_dir_all(&job.output_dir)
        .with_context(|| format!("creating {}", job.output_dir.display()))?;

    let agent = build_agent();
    let checksums = fetch_checksums(&agent, &job.mirrors, job.date.as_ref())?;
    let release = release_from_checksums(&checksums, job.date.as_ref())?;
    info!(date = %release.date, "Downloading the multistream dump and index");

    let index = fetch_verified(&agent, job, &release.date, &release.index)?;
    let dump = fetch_verified(&agent, job, &release.date, &release.dump)?;
    Ok(Downloaded {
        date: release.date,
        dump,
        index,
    })
}

fn build_agent() -> Agent {
    Agent::config_builder()
        // 206 and 416 are answers this module reads, not failures.
        .http_status_as_error(false)
        .timeout_connect(Some(CONNECT_TIMEOUT))
        .timeout_per_read(Some(READ_TIMEOUT))
        .build()
        .into()
}

fn checksums_url(mirror: &str, date: Option<&DumpDate>) -> String {
    match date {
        Some(date) => format!("{mirror}enwiki/{date}/enwiki-{date}-sha1sums.txt"),
        None => format!("{mirror}enwiki/latest/enwiki-latest-sha1sums.txt"),
    }
}

fn file_url(mirror: &str, date: &DumpDate, name: &str) -> String {
    format!("{mirror}enwiki/{date}/{name}")
}

fn fetch_checksums(agent: &Agent, mirrors: &[String], date: Option<&DumpDate>) -> Result<String> {
    for mirror in mirrors {
        let url = checksums_url(mirror, date);
        match fetch_text(agent, &url) {
            Ok(text) => return Ok(text),
            Err(e) => {
                warn!(%url, error = %format!("{e:#}"), "Checksum list failed, trying the next mirror")
            }
        }
    }
    bail!("no mirror served the SHA-1 list")
}

fn fetch_text(agent: &Agent, url: &str) -> Result<String> {
    let mut response = agent
        .get(url)
        .call()
        .with_context(|| format!("requesting {url}"))?;
    let status = response.status().as_u16();
    ensure!(status == 200, "{url} answered HTTP {status}");
    response
        .body_mut()
        .read_to_string()
        .with_context(|| format!("reading {url}"))
}

/// Refuses the whole list over one malformed line: a list that parses halfway
/// cannot be trusted for the rest.
pub fn parse_checksums(text: &str) -> Result<Vec<DumpFile>> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            match fields.as_slice() {
                [sha1, name] if is_sha1(sha1) => Ok(DumpFile {
                    name: name.to_string(),
                    sha1: sha1.to_ascii_lowercase(),
                }),
                _ => bail!("not a `<sha1>  <name>` line: {line:?}"),
            }
        })
        .collect()
}

fn is_sha1(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Without `date`, the list must name exactly one multistream dump.
pub fn release_from_checksums(text: &str, date: Option<&DumpDate>) -> Result<DumpRelease> {
    let entries = parse_checksums(text)?;
    let date = match date {
        Some(date) => date.clone(),
        None => only_dump_date(&entries)?,
    };
    Ok(DumpRelease {
        dump: only_entry(&entries, &dump_name(&date))?,
        index: only_entry(&entries, &index_name(&date))?,
        date,
    })
}

fn only_dump_date(entries: &[DumpFile]) -> Result<DumpDate> {
    let dates: BTreeSet<&str> = entries
        .iter()
        .filter_map(|entry| {
            entry
                .name
                .strip_prefix("enwiki-")?
                .strip_suffix("-pages-articles-multistream.xml.bz2")
        })
        .collect();
    match dates.into_iter().collect::<Vec<_>>().as_slice() {
        [date] => date.parse(),
        [] => bail!("the SHA-1 list names no multistream dump"),
        many => bail!(
            "the SHA-1 list names several dump dates ({}); pass --date",
            many.join(", ")
        ),
    }
}

fn only_entry(entries: &[DumpFile], name: &str) -> Result<DumpFile> {
    let matches: Vec<&DumpFile> = entries.iter().filter(|entry| entry.name == name).collect();
    match matches.as_slice() {
        [entry] => Ok((*entry).clone()),
        [] => bail!("the SHA-1 list has no entry for {name}"),
        _ => bail!("the SHA-1 list has several entries for {name}"),
    }
}

/// An existing file is hashed too, since something other than this module may
/// have put it there.
fn fetch_verified(
    agent: &Agent,
    job: &DownloadJob,
    date: &DumpDate,
    file: &DumpFile,
) -> Result<PathBuf> {
    let path = job.output_dir.join(&file.name);
    if path.exists() {
        info!(path = %path.display(), "Already present; verifying");
        verify(&path, &file.sha1).with_context(|| {
            format!(
                "{} exists but is wrong; delete it to download again",
                path.display()
            )
        })?;
        return Ok(path);
    }

    let part = job.output_dir.join(format!("{}.part", file.name));
    download_part(agent, &job.mirrors, date, &file.name, &part)?;
    if let Err(e) = verify(&part, &file.sha1) {
        fs::remove_file(&part).with_context(|| format!("removing {}", part.display()))?;
        return Err(e.context(format!("removed {} so a rerun starts over", part.display())));
    }
    fs::rename(&part, &path)
        .with_context(|| format!("renaming {} to {}", part.display(), path.display()))?;
    Ok(path)
}

/// Each mirror resumes from whatever the earlier ones left on disk.
fn download_part(
    agent: &Agent,
    mirrors: &[String],
    date: &DumpDate,
    name: &str,
    part: &Path,
) -> Result<()> {
    for mirror in mirrors {
        let url = file_url(mirror, date, name);
        match fetch_into(agent, &url, part) {
            Ok(()) => return Ok(()),
            Err(e) => {
                warn!(%url, error = %format!("{e:#}"), "Download failed, trying the next mirror")
            }
        }
    }
    bail!("every mirror failed for {name}")
}

fn part_len(part: &Path) -> Result<u64> {
    match fs::metadata(part) {
        Ok(meta) => Ok(meta.len()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e).with_context(|| format!("reading {}", part.display())),
    }
}

fn fetch_into(agent: &Agent, url: &str, part: &Path) -> Result<()> {
    let offset = part_len(part)?;
    let request = match offset {
        0 => agent.get(url),
        _ => agent.get(url).header("Range", format!("bytes={offset}-")),
    };
    let response = request
        .call()
        .with_context(|| format!("requesting {url}"))?;

    match response.status().as_u16() {
        // The whole file: a fresh start, or a server that ignored the Range.
        200 => {
            let total = header(&response, "content-length").and_then(|v| v.parse().ok());
            write_body(response, part, 0, total)
        }
        206 => {
            let range = header(&response, "content-range")
                .with_context(|| format!("{url} sent 206 without a Content-Range"))?;
            let (start, total) = parse_content_range(&range)?;
            ensure!(
                start == offset,
                "{url} resumed at byte {start}, not {offset}"
            );
            write_body(response, part, offset, Some(total))
        }
        // Nothing past `offset`: the part already holds the whole file, or is
        // longer than it. The SHA-1 tells which.
        416 if offset > 0 => Ok(()),
        status => bail!("{url} answered HTTP {status}"),
    }
}

fn header(response: &Response<ureq::Body>, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

/// `bytes <start>-<end>/<total>` to `(start, total)`.
pub fn parse_content_range(value: &str) -> Result<(u64, u64)> {
    let parsed = value
        .strip_prefix("bytes ")
        .and_then(|rest| rest.split_once('/'))
        .and_then(|(range, total)| {
            let (start, _end) = range.split_once('-')?;
            Some((start.parse().ok()?, total.parse().ok()?))
        });
    parsed.with_context(|| format!("not a byte Content-Range: {value:?}"))
}

fn write_body(
    response: Response<ureq::Body>,
    part: &Path,
    offset: u64,
    total: Option<u64>,
) -> Result<()> {
    let file = match offset {
        0 => File::create(part),
        _ => OpenOptions::new().append(true).open(part),
    }
    .with_context(|| format!("opening {}", part.display()))?;

    let pb = byte_progress(total, &format!("Downloading {}", file_name(part)));
    pb.set_position(offset);
    let mut reader = pb.wrap_read(response.into_body().into_reader());
    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);
    io::copy(&mut reader, &mut writer).with_context(|| format!("writing {}", part.display()))?;
    writer.flush()?;
    writer
        .into_inner()
        .map_err(|e| e.into_error())?
        .sync_all()
        .with_context(|| format!("syncing {}", part.display()))?;
    pb.finish_and_clear();
    Ok(())
}

fn verify(path: &Path, expected: &str) -> Result<()> {
    let actual = sha1_hex(path)?;
    ensure!(
        actual == expected,
        "SHA-1 mismatch for {}: expected {expected}, got {actual}",
        path.display()
    );
    info!(path = %path.display(), sha1 = %actual, "Checksum OK");
    Ok(())
}

fn sha1_hex(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let len = file.metadata()?.len();
    let pb = byte_progress(Some(len), &format!("Verifying {}", file_name(path)));
    let mut reader = pb.wrap_read(file);

    let mut hasher = Sha1::new();
    let mut buffer = vec![0u8; BUFFER_SIZE];
    loop {
        let n = reader
            .read(&mut buffer)
            .with_context(|| format!("reading {}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    pb.finish_and_clear();
    Ok(hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

/// A spinner when the size is unknown.
fn byte_progress(total: Option<u64>, message: &str) -> ProgressBar {
    let pb = match total {
        Some(total) => {
            let pb = ProgressBar::new(total);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template(
                        "{msg} [{bar:30.cyan}] {bytes}/{total_bytes} {bytes_per_sec} eta {eta}",
                    )
                    .expect("valid progress template"),
            );
            pb
        }
        None => {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.cyan} {msg} {bytes} {bytes_per_sec}")
                    .expect("valid progress template"),
            );
            pb
        }
    };
    pb.set_message(message.to_string());
    pb
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::net::TcpListener;
    use std::thread;
    use tempfile::TempDir;

    const DUMP_SHA1: &str = "bfdb7ead45e262d7b9d55d86c8958789e7efd1ae";
    const INDEX_SHA1: &str = "9aa04cdaf3b5b7f78d1f28b3c19c0bcc05b7611c";

    fn date(s: &str) -> DumpDate {
        s.parse().unwrap()
    }

    fn sample_list() -> String {
        format!(
            "0000000000000000000000000000000000000000  enwiki-20261101-pages-articles.xml.bz2\n\
             {DUMP_SHA1}  enwiki-20261101-pages-articles-multistream.xml.bz2\n\
             {INDEX_SHA1}  enwiki-20261101-pages-articles-multistream-index.txt.bz2\n"
        )
    }

    #[test]
    fn dump_date_takes_eight_digits_only() {
        assert!("20261101".parse::<DumpDate>().is_ok());
        assert!("2026110".parse::<DumpDate>().is_err());
        assert!("latest".parse::<DumpDate>().is_err());
        assert!("2026-11-01".parse::<DumpDate>().is_err());
    }

    #[test]
    fn release_comes_from_the_one_dump_date_in_the_list() {
        let release = release_from_checksums(&sample_list(), None).unwrap();
        assert_eq!(release.date, date("20261101"));
        assert_eq!(release.dump.sha1, DUMP_SHA1);
        assert_eq!(release.index.sha1, INDEX_SHA1);
        assert_eq!(
            release.index.name,
            "enwiki-20261101-pages-articles-multistream-index.txt.bz2"
        );
    }

    #[test]
    fn an_explicit_date_must_be_in_the_list() {
        let err = release_from_checksums(&sample_list(), Some(&date("20261120"))).unwrap_err();
        assert!(
            err.to_string().contains("no entry for enwiki-20261120"),
            "{err}"
        );
    }

    #[test]
    fn several_dump_dates_need_an_explicit_date() {
        let list = format!(
            "{}{DUMP_SHA1}  enwiki-20261020-pages-articles-multistream.xml.bz2\n",
            sample_list()
        );
        let err = release_from_checksums(&list, None).unwrap_err();
        assert!(err.to_string().contains("pass --date"), "{err}");
        assert!(release_from_checksums(&list, Some(&date("20261101"))).is_ok());
    }

    #[test]
    fn a_malformed_line_rejects_the_whole_list() {
        let list = format!("{}not a checksum line\n", sample_list());
        assert!(parse_checksums(&list).is_err());
        assert!(parse_checksums("abc123  file.bz2").is_err());
    }

    #[test]
    fn content_range_gives_start_and_total() {
        assert_eq!(
            parse_content_range("bytes 100-199/200").unwrap(),
            (100, 200)
        );
        assert!(parse_content_range("bytes */200").is_err());
        assert!(parse_content_range("items 0-1/2").is_err());
    }

    #[test]
    fn dated_names_let_the_index_be_detected() {
        let dump = dump_name(&date("20261101"));
        assert_eq!(
            dump.replace(".xml.bz2", "-index.txt.bz2"),
            index_name(&date("20261101"))
        );
    }

    /// Plain HTTP that honors `Range: bytes=N-`.
    fn serve(body: &'static [u8], requests: usize) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}/", listener.local_addr().unwrap());
        thread::spawn(move || {
            for stream in listener.incoming().take(requests) {
                let mut stream = stream.unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut start = None;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    let lower = line.to_ascii_lowercase();
                    if let Some(rest) = lower.strip_prefix("range: bytes=") {
                        start = rest.trim().trim_end_matches('-').parse::<usize>().ok();
                    }
                }
                let reply = match start {
                    None => format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()),
                    Some(s) if s >= body.len() => {
                        "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\n\r\n"
                            .to_string()
                    }
                    Some(s) => format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {s}-{}/{}\r\nContent-Length: {}\r\n\r\n",
                        body.len() - 1,
                        body.len(),
                        body.len() - s
                    ),
                };
                stream.write_all(reply.as_bytes()).unwrap();
                if !reply.starts_with("HTTP/1.1 416") {
                    stream.write_all(&body[start.unwrap_or(0)..]).unwrap();
                }
            }
        });
        base
    }

    fn job(dir: &TempDir, mirror: String) -> DownloadJob {
        DownloadJob {
            output_dir: dir.path().to_path_buf(),
            date: Some(date("20261101")),
            mirrors: vec![mirror],
        }
    }

    fn sha1_of(bytes: &[u8]) -> String {
        Sha1::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    const BODY: &[u8] = b"multistream bytes, standing in for a dump";

    fn file() -> DumpFile {
        DumpFile {
            name: dump_name(&date("20261101")),
            sha1: sha1_of(BODY),
        }
    }

    #[test]
    fn a_partial_download_resumes_and_is_renamed_once_verified() {
        let dir = TempDir::new().unwrap();
        let part = dir.path().join(format!("{}.part", file().name));
        fs::write(&part, &BODY[..10]).unwrap();

        let job = job(&dir, serve(BODY, 1));
        let path = fetch_verified(&build_agent(), &job, &date("20261101"), &file()).unwrap();

        assert_eq!(fs::read(&path).unwrap(), BODY);
        assert!(!part.exists());
    }

    #[test]
    fn a_complete_part_is_verified_without_refetching() {
        let dir = TempDir::new().unwrap();
        let part = dir.path().join(format!("{}.part", file().name));
        fs::write(&part, BODY).unwrap();

        let job = job(&dir, serve(BODY, 1));
        let path = fetch_verified(&build_agent(), &job, &date("20261101"), &file()).unwrap();
        assert_eq!(fs::read(&path).unwrap(), BODY);
    }

    #[test]
    fn a_mismatched_download_is_removed_and_never_renamed() {
        let dir = TempDir::new().unwrap();
        let wrong = DumpFile {
            sha1: "0".repeat(40),
            ..file()
        };

        let job = job(&dir, serve(BODY, 1));
        let err = fetch_verified(&build_agent(), &job, &date("20261101"), &wrong).unwrap_err();

        assert!(format!("{err:#}").contains("SHA-1 mismatch"), "{err:#}");
        assert!(!dir.path().join(&wrong.name).exists());
        assert!(!dir.path().join(format!("{}.part", wrong.name)).exists());
    }

    #[test]
    fn a_wrong_file_under_the_final_name_is_refused() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join(file().name), b"something else").unwrap();

        let job = job(&dir, "http://127.0.0.1:9/".to_string());
        let err = fetch_verified(&build_agent(), &job, &date("20261101"), &file()).unwrap_err();
        assert!(
            format!("{err:#}").contains("delete it to download again"),
            "{err:#}"
        );
    }

    #[test]
    fn a_failing_mirror_falls_through_to_the_next() {
        let dir = TempDir::new().unwrap();
        let mut job = job(&dir, serve(BODY, 1));
        // Port 9 (discard) refuses connections on a machine that runs nothing there.
        job.mirrors.insert(0, "http://127.0.0.1:9/".to_string());

        let path = fetch_verified(&build_agent(), &job, &date("20261101"), &file()).unwrap();
        assert_eq!(fs::read(&path).unwrap(), BODY);
    }
}
