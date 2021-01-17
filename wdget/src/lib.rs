// wdget
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use fs::remove_file;
use lazy_static::lazy_static;
use regex::Regex;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use sha1::{Digest, Sha1};
use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::time;

#[derive(thiserror::Error, Debug)]
pub enum WDGetError {
    #[error("Network I/O error {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("Error parsing JSON: {0}")]
    JsonError(#[from] serde_json::Error),
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
}

type Result<T> = std::result::Result<T, WDGetError>;

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
        .ok_or(WDGetError::InvalidJsonFromWikidata())?;
    for binding in bindings {
        let id = binding["id"]["value"]
            .as_str()
            .ok_or(WDGetError::InvalidJsonFromWikidata())?;
        let label = binding["itemLabel"]["value"]
            .as_str()
            .ok_or(WDGetError::InvalidJsonFromWikidata())?;
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
            WDGetError::DumpStatusFileNotFound()
        } else {
            WDGetError::from(e)
        }
    })?;
    let body = r.text().await?;
    Ok(serde_json::from_str(body.as_str())?)
}

pub async fn get_latest_available_date(client: &Client, wiki: &str, dump_type: Option<&str>) -> Result<String> {
    let mut available_dates = get_available_dates(client, wiki).await?;
    available_dates.reverse();
    for date in available_dates.iter() {
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
            Err(WDGetError::DumpStatusFileNotFound()) => continue,
            Err(e) => return Err(e),
        }
    }
    return Err(WDGetError::NoDumpDatesFound());
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

async fn download_file(
    url: &str,
    file_path: &Path,
    partfile_path: &Path,
    file_data: &DumpFileInfo,
    client: &Client,
    verbose: bool,
) -> Result<()> {
    let file_name = get_file_name_expect(file_path);
    if verbose {
        eprint!("Downloading {}...", file_name);
        std::io::stderr().flush().unwrap();
    }
    let mut r = client.get(url).send().await?.error_for_status()?;
    let mut partfile = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&partfile_path)
        .map_err(|e| {
            WDGetError::DumpFileAccessError(
                partfile_path.to_owned(),
                std::format!("Could not create part file: {0}", e),
            )
        })?;
    let mut bytes_read: u64 = 0;
    let progress_update_period = time::Duration::from_secs(1);
    let mut progress_update_interval = time::interval_at(
        tokio::time::Instant::now() + tokio::time::Duration::from_secs(1),
        progress_update_period,
    );
    let start_time = Instant::now();
    let mut prev_bytes_read = 0_u64;
    let mut prev_time = Instant::now();
    let mut last_printed_progress_len = 0;
    loop {
        tokio::select! {
            chunk = r.chunk() => {
                if let Some(chunk) = chunk? {
                    partfile.write_all(chunk.as_ref()).map_err(|e| {
                        WDGetError::DumpFileAccessError(
                            partfile_path.to_owned(),
                            std::format!("Write error: {0}", e),
                        )
                    })?;
                    bytes_read += chunk.len() as u64;
                } else {
                    // done
                    if verbose {
                        // clear progress
                        eprint!("\r{:1$}\r","",last_printed_progress_len);
                        std::io::stderr().flush().unwrap();
                    }
                    break;
                }
            },
            _ = progress_update_interval.tick() => {
                if verbose {
                    if let Some(file_data_size) = file_data.size {
                        let total_mib = file_data_size as f64 / 1024.0 / 1024.0;
                        let mib_per_sec = (bytes_read - prev_bytes_read) as f64 / 1024.0 / 1024.0 / prev_time.elapsed().as_secs_f64();
                        let mut progress_string = std::format!(
                            "\rDownloading {} - {:.2} MiB of {:.2} MiB downloaded ({:.2} MiB/s).",
                            &file_name,
                            bytes_read as f64 / 1024.0 / 1024.0,
                            total_mib,
                            mib_per_sec);
                        let new_printed_progress_len = progress_string.chars().count();
                        for _ in new_printed_progress_len..last_printed_progress_len {
                            progress_string.push(' ');
                        }
                        eprint!("{}", progress_string);
                        std::io::stderr().flush().unwrap();
                        last_printed_progress_len = new_printed_progress_len;
                        prev_bytes_read = bytes_read;
                        prev_time = Instant::now();
                    }
                }
            },
            _ = tokio::signal::ctrl_c() => {
                return Err(WDGetError::AbortedByUser());
            }
        };
    }
    std::fs::rename(&partfile_path, &file_name).map_err(|e| {
        WDGetError::DumpFileAccessError(
            partfile_path.to_owned(),
            std::format!("Could not rename part file: {0}", e),
        )
    })?;

    if verbose {
        eprintln!(
            "Downloaded {} - {:.2} MiB in {:.2} seconds ({:.2} MiB/s)",
            &file_name,
            bytes_read as f64 / 1024.0 / 1024.0,
            start_time.elapsed().as_secs_f64(),
            bytes_read as f64 / 1024.0 / 1024.0 / start_time.elapsed().as_secs_f64()
        );
    } else {
        println!("Downloaded {}.", &file_name);
    }
    Ok(())
}

fn check_existing_file(file_path: &Path, file_data: &DumpFileInfo, verbose: bool) -> Result<()> {
    let file_name = get_file_name_expect(file_path);
    let file_metadata = fs::metadata(file_path).map_err(|e| {
        WDGetError::DumpFileAccessError(
            file_path.to_owned(),
            std::format!("Could not get file information: {0}", e),
        )
    })?;
    if let Some(expected_file_size) = &file_data.size {
        if *expected_file_size != file_metadata.len() {
            return Err(WDGetError::DumpFileAccessError(
                file_path.to_owned(),
                std::format!(
                    "Dump file already exists, but its size does not match the expected size. Expected: {}, actual: {}.",
                    expected_file_size, file_metadata.len()
                ),
            ));
        }
    }
    match file_data.sha1.as_ref() {
        Some(expected_sha1) => {
            let mut file = fs::File::open(file_path).map_err(|e| {
                WDGetError::DumpFileAccessError(
                    file_path.to_owned(),
                    std::format!("Could not read mapping file: {}", e),
                )
            })?;
            if verbose {
                eprint!("Verifying {}...", file_name);
                std::io::stderr().flush().unwrap();
            }
            let start_time = Instant::now();
            let mut hasher = Sha1::new();
            let hashed_bytes = std::io::copy(&mut file, &mut hasher).map_err(|e| {
                WDGetError::DumpFileAccessError(
                    file_path.to_owned(),
                    std::format!("Could not read mapping file: {}", e),
                )
            })?;
            let sha1_bytes = hasher.finalize();
            let actual_sha1 = format!("{:x}", sha1_bytes);
            if expected_sha1 != &actual_sha1 {
                return Err(WDGetError::DumpFileAccessError(
                    file_path.to_owned(),
                    "File already exists but the SHA1 digest differs from the expected one.".to_owned(),
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
                "WARNING: {} already exists but cannot be checked due to missing SHA1 checksum, skipping download.",
                &file_name
            );
        }
    }
    Ok(())
}

pub async fn download<T>(
    client: &Client,
    wiki: &str,
    date: &str,
    dump_type: &str,
    mirror: Option<&str>,
    target_directory: T,
    verbose: bool,
    keep_partial: bool,
    resume_partial: bool,
) -> Result<()>
where
    T: AsRef<Path>,
{
    let target_directory = target_directory.as_ref();
    if !target_directory.exists() {
        return Err(WDGetError::TargetDirectoryDoesNotExist(target_directory.to_owned()));
    }
    let dump_status = get_dump_status(client, wiki, date).await?;
    let job_info = dump_status.jobs.get(dump_type).ok_or(WDGetError::DumpTypeNotFound())?;
    if &job_info.status != "done" {
        return Err(WDGetError::DumpNotComplete());
    }
    let files = job_info.files.as_ref().ok_or(WDGetError::DumpHasNoFiles())?;
    let root_url = mirror.unwrap_or("https://dumps.wikimedia.org");
    for (file_name, file_data) in files {
        let mut target_file_pathbuf = target_directory.to_owned();
        target_file_pathbuf.push(&file_name);
        let target_file_path = target_file_pathbuf.as_path();
        if target_file_pathbuf.exists() {
            check_existing_file(target_file_path, file_data, verbose)?;
            continue;
        }
        let partfile_name = create_partfile_path(target_file_path);
        if resume_partial && Path::new(&partfile_name).exists() {
            let partfile_metadata = fs::metadata(&partfile_name).map_err(|e| {
                WDGetError::DumpFileAccessError(
                    partfile_name.clone(),
                    std::format!("Could not get file information: {0}", e),
                )
            })?;
            if !partfile_metadata.is_file() {
                return Err(WDGetError::DumpFileAccessError(
                    partfile_name.clone(),
                    "Expected regular file".to_owned(),
                ));
            }
            let part_len = partfile_metadata.len();
            if file_data.size.is_some() && part_len > file_data.size.unwrap() {
                return Err(WDGetError::DumpFileAccessError(
                    partfile_name.clone(),
                    std::format!(
                        "Existing part file is longer than expected: {0} > {1}",
                        part_len,
                        file_data.size.unwrap(),
                    ),
                ));
            }
            // partial download not yet implemented
            todo!();
        }
        let url = format!("{}/{}/{}/{}", root_url, wiki, date, file_name);
        let download_res = download_file(&url, target_file_path, &partfile_name, file_data, client, verbose).await;
        if !keep_partial && download_res.is_err() && Path::new(&partfile_name).is_file() {
            remove_file(&partfile_name)
                .or_else::<(), _>(|err| {
                    eprintln!("Could not remove {}: {}", partfile_name.display(), &err);
                    Ok(())
                })
                .unwrap();
        }
        download_res?;
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
        return Err(WDGetError::NoDumpDatesFound());
    }
    dates.sort_unstable();
    Ok(dates)
}
