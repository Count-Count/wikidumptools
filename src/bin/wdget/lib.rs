// wdget
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Instant;

use fs::remove_file;
use futures::stream::{self, StreamExt};
use futures::TryFutureExt;
use lazy_static::lazy_static;
use regex::Regex;
use reqwest::{Client, StatusCode};
use scopeguard::defer;
use serde::Deserialize;
use sha1::{Digest, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::{select, time};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Network I/O error {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("Error parsing JSON: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Error running decompression process: {0}")]
    DecompressorError(std::io::Error),
    #[error("Received invalid JSON data from Wikidata")]
    InvalidJsonFromWikidata(),
    #[error("Dump of this type was not found")]
    DumpTypeNotFound(),
    #[error("Dump is still in progress")]
    DumpNotComplete(),
    #[error("Dump does not contain any files")]
    DumpStatusFileNotFound(),
    #[error("Dump status file not found")]
    DumpHasNoFiles(),
    #[error("No dump runs found")]
    NoDumpDatesFound(),
    #[error("Specified date is invalid, must be YYYYMMDD or 'latest'")]
    InvalidDumpDate(),
    #[error("Error accessing file {0} - {1}")]
    DumpFileAccessError(PathBuf, String),
    #[error("Aborted by user")]
    AbortedByUser(),
    #[error("Target directory {0} does not exist")]
    TargetDirectoryDoesNotExist(PathBuf),
    #[error("Decompressed file {0} cannot be verified")]
    DecompressedFileCannotBeVerified(String),
    #[error("Expected file {0} not found")]
    FileToBeVerifiedNotFound(String),
}

type Result<T> = std::result::Result<T, Error>;

pub struct Wiki {
    pub id: String,
    pub name: String,
}

pub async fn get_available_wikis_from_wikidata(client: &Client) -> Result<Vec<Wiki>> {
    let mut wikis = Vec::with_capacity(50);
    let sparql_url = "https://query.wikidata.org/sparql";
    let query = r#"
    SELECT DISTINCT ?id ?itemLabel WHERE {
        ?item p:P1800/ps:P1800 ?id.
        SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
        FILTER(NOT EXISTS { ?item wdt:P31 wd:Q33120923. })
    }
    "#;
    let blacklist = ["ecwikimedia", "labswiki", "labtestwiki", "ukwikiversity"];
    let r = client
        .get(sparql_url)
        .query(&[("format", "json"), ("query", query.trim())])
        .send()
        .await?
        .error_for_status()?;
    let body = r.text().await?;
    let json: serde_json::Value = serde_json::from_str(body.as_str())?;
    let bindings = json["results"]["bindings"]
        .as_array()
        .ok_or(Error::InvalidJsonFromWikidata())?;
    for binding in bindings {
        let id = binding["id"]["value"]
            .as_str()
            .ok_or(Error::InvalidJsonFromWikidata())?;
        let label = binding["itemLabel"]["value"]
            .as_str()
            .ok_or(Error::InvalidJsonFromWikidata())?;
        if !blacklist.contains(&id) {
            wikis.push(Wiki {
                id: id.to_owned(),
                name: label.to_owned(),
            });
        }
    }
    Ok(wikis)
}

#[derive(Deserialize)]
pub struct DumpStatus {
    pub version: String,
    pub jobs: BTreeMap<String, DumpJobInfo>,
}

#[derive(Deserialize)]
pub struct DumpJobInfo {
    pub updated: String,
    pub status: String,
    pub files: Option<BTreeMap<String, DumpFileInfo>>,
}

#[derive(Deserialize)]
pub struct DumpFileInfo {
    pub url: Option<String>,
    pub sha1: Option<String>,
    pub size: Option<u64>,
    pub md5: Option<String>,
}

pub async fn get_dump_status(client: &Client, wiki: &str, date: &str) -> Result<DumpStatus> {
    let url = format!("https://dumps.wikimedia.org/{}/{}/dumpstatus.json", wiki, date);
    let r = client.get(url.as_str()).send().await?.error_for_status().map_err(|e| {
        if let Some(StatusCode::NOT_FOUND) = e.status() {
            Error::DumpStatusFileNotFound()
        } else {
            Error::from(e)
        }
    })?;
    let body = r.text().await?;
    Ok(serde_json::from_str(body.as_str())?)
}

pub async fn get_latest_available_date(client: &Client, wiki: &str, dump_type: Option<&str>) -> Result<String> {
    let mut available_dates = get_available_dates(client, wiki).await?;
    available_dates.reverse();
    for date in &available_dates {
        let res = get_dump_status(client, wiki, date).await;
        match res {
            Ok(dump_status) => {
                if let Some(dump_type) = dump_type {
                    if dump_status
                        .jobs
                        .get(dump_type)
                        .map_or(false, |job| job.status == "done")
                    {
                        return Ok(date.to_owned());
                    }
                } else {
                    return Ok(date.to_owned());
                }
            }
            Err(Error::DumpStatusFileNotFound()) => continue,
            Err(e) => return Err(e),
        }
    }
    return Err(Error::NoDumpDatesFound());
}

fn get_file_name_expect(file_path: &Path) -> &str {
    file_path
        .file_name()
        .expect("Path has no filename")
        .to_str()
        .expect("Filename is not in UTF-8")
}

fn create_partfile_path(file_path: &Path) -> PathBuf {
    let part_file_name = get_file_name_expect(file_path).to_owned() + ".part";
    let mut part_path = file_path.to_owned();
    part_path.set_file_name(part_file_name);
    part_path
}

#[derive(Debug)]
enum DownloadProgress {
    BytesReadFromNet(u64),
    BytesWrittenToDisk(u64),
}

async fn download_file(
    url: String,
    file_path: PathBuf,
    partfile_path: PathBuf,
    client: &Client,
    decompress: bool,
    progress_send: UnboundedSender<DownloadProgress>,
) -> Result<()> {
    let file_name = get_file_name_expect(&file_path);
    let mut r = client.get(url).send().await?.error_for_status()?;
    let mut partfile = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&partfile_path)
        .map_err(|e| {
            Error::DumpFileAccessError(
                partfile_path.to_owned(),
                std::format!("Could not create part file: {0}", e),
            )
        })?;

    defer! {
        if partfile_path.is_file() {
            remove_file(&partfile_path)
                .or_else::<(), _>(|err| {
                    eprintln!("Could not remove {}: {}", partfile_path.file_name().unwrap().to_string_lossy(), &err);
                    Ok(())
                })
                .unwrap();
        }
    }

    if decompress {
        let mut decompressor = Command::new("bunzip2")
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(Error::DecompressorError)?;

        let mut decompressor_in = decompressor.stdin.take().expect("Subprocess stdin is None");
        let copy_net_to_decompressor_in = async move {
            while let Some(chunk) = r.chunk().await? {
                decompressor_in
                    .write_all(chunk.as_ref())
                    .await
                    .map_err(Error::DecompressorError)?;
                progress_send
                    .send(DownloadProgress::BytesReadFromNet(chunk.len() as u64))
                    .unwrap();
            }
            decompressor_in.shutdown().await.map_err(Error::DecompressorError)
        };

        let mut decompressor_out = decompressor.stdout.take().expect("Subprocess stdout is None");
        let partfile_path = partfile_path.clone(); // clone since captured
        let copy_decompressor_out_to_file = async move {
            let mut buf = Vec::with_capacity(65536);
            loop {
                let read_len = decompressor_out
                    .read_buf(&mut buf)
                    .await
                    .map_err(Error::DecompressorError)?;
                if read_len > 0 {
                    partfile.write_all(buf.as_ref()).map_err(|e| {
                        Error::DumpFileAccessError(partfile_path.to_owned(), std::format!("Write error: {0}", e))
                    })?;
                    buf.clear();
                } else {
                    break;
                }
            }
            Result::Ok(())
        };

        let wait_for_decompressor_exit = async move {
            let output = decompressor
                .wait_with_output()
                .await
                .map_err(Error::DecompressorError)?;
            if output.status.success() {
                Ok(())
            } else {
                Err(Error::DecompressorError(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "Decompressor exited with status {} - stderr: {}",
                        output.status,
                        String::from_utf8_lossy(output.stderr.as_ref())
                    ),
                )))
            }
        };

        tokio::try_join!(
            copy_net_to_decompressor_in,
            copy_decompressor_out_to_file,
            wait_for_decompressor_exit
        )?;
    } else {
        while let Some(chunk) = r.chunk().await? {
            partfile.write_all(chunk.as_ref()).map_err(|e| {
                Error::DumpFileAccessError(partfile_path.to_owned(), std::format!("Write error: {0}", e))
            })?;
            progress_send
                .send(DownloadProgress::BytesReadFromNet(chunk.len() as u64))
                .unwrap();
        }
    }

    std::fs::rename(&partfile_path, &file_name).map_err(|e| {
        Error::DumpFileAccessError(
            partfile_path.to_owned(),
            std::format!("Could not rename part file: {0}", e),
        )
    })?;

    Ok(())
}

fn verify_existing_file(file_path: &Path, file_data: &DumpFileInfo, verbose: bool) -> Result<()> {
    let file_name = get_file_name_expect(file_path);
    let file_metadata = fs::metadata(file_path).map_err(|e| {
        Error::DumpFileAccessError(
            file_path.to_owned(),
            std::format!("Could not get file information: {0}", e),
        )
    })?;
    if let Some(expected_file_size) = &file_data.size {
        if *expected_file_size != file_metadata.len() {
            return Err(Error::DumpFileAccessError(
                file_path.to_owned(),
                std::format!(
                    "Dump file size does not match the expected size. Expected: {}, actual: {}.",
                    expected_file_size,
                    file_metadata.len()
                ),
            ));
        }
    }
    match file_data.sha1.as_ref() {
        Some(expected_sha1) => {
            let mut file = fs::File::open(file_path).map_err(|e| {
                Error::DumpFileAccessError(file_path.to_owned(), std::format!("Could not read mapping file: {}", e))
            })?;
            if verbose {
                eprint!("Verifying {}...", file_name);
                std::io::stderr().flush().unwrap();
            }
            let start_time = Instant::now();
            let mut hasher = Sha1::new();
            let hashed_bytes = std::io::copy(&mut file, &mut hasher).map_err(|e| {
                Error::DumpFileAccessError(file_path.to_owned(), std::format!("Could not read mapping file: {}", e))
            })?;
            let sha1_bytes = hasher.finalize();
            let actual_sha1 = format!("{:x}", sha1_bytes);
            if expected_sha1 != &actual_sha1 {
                return Err(Error::DumpFileAccessError(
                    file_path.to_owned(),
                    "SHA1 digest differs from the expected one.".to_owned(),
                ));
            };
            if verbose {
                eprintln!(
                    "\rVerified {} - OK - {:.2} MiB in {:.2} seconds ({:.2} MiB/s)",
                    file_name,
                    hashed_bytes as f64 / 1024.0 / 1024.0,
                    start_time.elapsed().as_secs_f64(),
                    hashed_bytes as f64 / 1024.0 / 1024.0 / start_time.elapsed().as_secs_f64()
                );
            } else {
                println!("Verified {} - OK.", &file_name);
            }
        }
        None => {
            eprintln!(
                "WARNING: {} cannot be checked due to missing SHA1 checksum.",
                &file_name
            );
        }
    }
    Ok(())
}

fn get_target_file_path(target_directory: &Path, file_name: &str, decompress: bool) -> PathBuf {
    let target_file_name = if decompress {
        file_name.strip_suffix(".bz2").unwrap_or(file_name)
    } else {
        file_name
    };
    let mut target_file_pathbuf = target_directory.to_owned();
    target_file_pathbuf.push(&target_file_name);
    target_file_pathbuf
}

pub async fn verify<T>(client: &Client, wiki: &str, date: &str, dump_type: &str, dump_files_directory: T) -> Result<()>
where
    T: AsRef<Path> + Send,
{
    let dump_files_directory = dump_files_directory.as_ref();
    if !dump_files_directory.exists() {
        return Err(Error::TargetDirectoryDoesNotExist(dump_files_directory.to_owned()));
    }
    let dump_status = get_dump_status(client, wiki, date).await?;
    let job_info = dump_status.jobs.get(dump_type).ok_or(Error::DumpTypeNotFound())?;
    if &job_info.status != "done" {
        return Err(Error::DumpNotComplete());
    }
    let files = job_info.files.as_ref().ok_or(Error::DumpHasNoFiles())?;
    for (file_name, file_data) in files {
        let target_file_path = get_target_file_path(dump_files_directory, file_name, false);
        if !target_file_path.exists() {
            let decompressed_target_file_path = get_target_file_path(dump_files_directory, file_name, true);
            if decompressed_target_file_path.exists() {
                return Err(Error::DecompressedFileCannotBeVerified(
                    decompressed_target_file_path
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                ));
            } else {
                return Err(Error::FileToBeVerifiedNotFound(
                    target_file_path.file_name().unwrap().to_string_lossy().into_owned(),
                ));
            }
        }
        verify_existing_file(&target_file_path, file_data, true)?;
    }
    Ok(())
}

#[derive(Default)]
pub struct DownloadOptions<'a> {
    pub mirror: Option<&'a str>,
    pub verbose: bool,
    pub decompress: bool,
}

pub async fn download<T>(
    client: &Client,
    wiki: &str,
    date: &str,
    dump_type: &str,
    target_directory: T,
    download_options: &DownloadOptions<'_>,
) -> Result<()>
where
    T: AsRef<Path> + Send,
{
    let target_directory = target_directory.as_ref();
    if !target_directory.exists() {
        return Err(Error::TargetDirectoryDoesNotExist(target_directory.to_owned()));
    }
    let dump_status = get_dump_status(client, wiki, date).await?;
    let job_info = dump_status.jobs.get(dump_type).ok_or(Error::DumpTypeNotFound())?;
    if &job_info.status != "done" {
        return Err(Error::DumpNotComplete());
    }
    let files = job_info.files.as_ref().ok_or(Error::DumpHasNoFiles())?;
    let root_url = download_options.mirror.unwrap_or("https://dumps.wikimedia.org");

    // create futures for missing files
    let mut futures = Vec::with_capacity(files.len());
    let (progress_send, mut progress_receive) = unbounded_channel::<DownloadProgress>();
    let mut total_data_size = Some(0_u64);
    for (file_name, file_data) in files {
        let target_file_path = get_target_file_path(target_directory, file_name, download_options.decompress);
        if target_file_path.exists() {
            eprintln!(
                "{} exists, skipping.",
                target_file_path.file_name().unwrap().to_string_lossy()
            );
            continue;
        }
        let partfile_name = create_partfile_path(target_file_path.as_path());
        if let Some(ref mut len) = total_data_size {
            match file_data.size {
                Some(cur_len) => {
                    *len += cur_len;
                }
                None => {
                    total_data_size = None;
                }
            }
        }
        let url = format!("{}/{}/{}/{}", root_url, wiki, date, file_name);
        let download_res = download_file(
            url,
            target_file_path.clone(),
            partfile_name.to_owned(),
            client,
            download_options.decompress,
            progress_send.clone(),
        )
        .map_ok(|_| target_file_path);
        futures.push(download_res);
    }

    // download missing files
    let stream_of_downloads = stream::iter(futures);

    let max_concurrent_downloads = if download_options.mirror.is_some() { 4 } else { 1 }; // TODO: option! default: 4? numcpus? numcpus/2? different for decompress?
    let mut buffered = stream_of_downloads.buffer_unordered(max_concurrent_downloads);

    let progress_update_period = time::Duration::from_secs(1);
    let mut progress_update_interval = time::interval_at(
        tokio::time::Instant::now() + tokio::time::Duration::from_secs(1),
        progress_update_period,
    );
    let start_time = Instant::now();
    let mut prev_time = Instant::now();
    let mut prev_bytes_received = 0_u64;
    let mut last_printed_progress_len = 0;
    let mut bytes_received = 0_u64;
    loop {
        select! {
            res = buffered.next() => {
                match res {
                    Some(res) => {
                        let finished_file = res?;
                        if download_options.verbose {
                            eprint!("\r{:1$}\r","",last_printed_progress_len);
                            eprintln!("Downloaded {}.", finished_file.file_name().unwrap().to_string_lossy());
                        }
                    },
                    None => break
                }
            }
            _ = tokio::signal::ctrl_c() => {
                return Err(Error::AbortedByUser());
            }
            download_progress = progress_receive.recv() => {
                if let Some(DownloadProgress::BytesReadFromNet(count)) = download_progress {
                    bytes_received += count;
                }
            }
            _ = progress_update_interval.tick() => {
                if download_options.verbose {
                    let mib_per_sec = (bytes_received - prev_bytes_received) as f64 / 1024.0 / 1024.0 / prev_time.elapsed().as_secs_f64();
                    let mut progress_string =
                        if let Some(total_data_size) = total_data_size {
                            let total_mib = total_data_size as f64 / 1024.0 / 1024.0;
                            std::format!(
                                "\rDownloading {}- {:.2} MiB of {:.2} MiB downloaded ({:.2} MiB/s).",
                                if download_options.decompress {"and decompressing "} else {""},
                                bytes_received as f64 / 1024.0 / 1024.0,
                                total_mib,
                                mib_per_sec)
                        } else {
                            std::format!(
                                "\rDownloading {}- {:.2} MiB downloaded ({:.2} MiB/s).",
                                if download_options.decompress {"and decompressing "} else {""},
                                bytes_received as f64 / 1024.0 / 1024.0,
                                mib_per_sec)
                        };
                    let new_printed_progress_len = progress_string.chars().count();
                    for _ in new_printed_progress_len..last_printed_progress_len {
                        progress_string.push(' ');
                    }
                    eprint!("{}", progress_string);
                    std::io::stderr().flush().unwrap();
                    last_printed_progress_len = new_printed_progress_len;
                    prev_bytes_received = bytes_received;
                    prev_time = Instant::now();
                }
            }

        }
    }
    if download_options.verbose {
        let total_mib = bytes_received as f64 / 1024.0 / 1024.0;
        let mib_per_sec = total_mib / start_time.elapsed().as_secs_f64();
        eprintln!(
            "\rDownloaded {}{:.2} MiB ({:.2} MiB/s).",
            if download_options.decompress {
                "and decompressed "
            } else {
                ""
            },
            total_mib,
            mib_per_sec
        );
    }
    Ok(())
}

pub async fn get_available_dates(client: &Client, wiki: &str) -> Result<Vec<String>> {
    let url = format!("https://dumps.wikimedia.org/{}/", wiki);
    let r = client.get(url.as_str()).send().await?.error_for_status()?;
    lazy_static! {
        static ref RE: Regex = Regex::new(r#"<a href="([1-9][0-9]{7})/">([1-9][0-9]{7})/</a>"#)
            .expect("Error parsing HTML dump date regex");
    }
    let body = r.text().await?;
    let mut dates = Vec::with_capacity(10);
    for cap in RE.captures_iter(&body) {
        if cap[1] == cap[2] {
            dates.push(cap[1].to_owned());
        }
    }
    if dates.is_empty() {
        return Err(Error::NoDumpDatesFound());
    }
    dates.sort_unstable();
    Ok(dates)
}
