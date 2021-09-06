// wdget
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.
use std::cmp::min;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use bzip2::read::MultiBzDecoder;
use fs::remove_file;
use futures::stream::{self, StreamExt};
use futures::TryFutureExt;
use lazy_static::lazy_static;
use regex::Regex;
use reqwest::{Client, StatusCode};
use scopeguard::defer;
use serde::Deserialize;
use sha1::{Digest, Sha1};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::task::{spawn_blocking, JoinError};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Network I/O error {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("Error parsing JSON: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Error running decompression process: {0}")]
    DecompressorError(std::io::Error),
    #[error("Error running decompression process: {0}")]
    DecompressorJoinError(JoinError),
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
    #[error("Aborted by the user")]
    AbortedByUser(),
    #[error("Target directory {0} does not exist")]
    TargetDirectoryDoesNotExist(PathBuf),
    #[error("Decompressed file {0} cannot be verified")]
    DecompressedFileCannotBeVerified(String),
    #[error("Expected file {0} not found")]
    FileToBeVerifiedNotFound(String),
    #[error("Could not send to progress channel")]
    ProgressChannelSendError(#[from] tokio::sync::mpsc::error::SendError<DownloadProgress>),
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
    for date in available_dates {
        let res = get_dump_status(client, wiki, &date).await;
        match res {
            Ok(dump_status) => {
                if let Some(dump_type) = dump_type {
                    if dump_status
                        .jobs
                        .get(dump_type)
                        .map_or(false, |job| job.status == "done")
                    {
                        return Ok(date);
                    }
                } else {
                    return Ok(date);
                }
            }
            Err(Error::DumpStatusFileNotFound()) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(Error::NoDumpDatesFound())
}

fn get_target_file_name(file_name: &str, decompress: bool) -> &str {
    if decompress {
        file_name.strip_suffix(".bz2").unwrap_or(file_name)
    } else {
        file_name
    }
}

fn get_file_in_dir(directory: &Path, file_name: &str) -> PathBuf {
    let mut file = directory.to_owned();
    file.push(&file_name);
    file
}

fn verify_hash(expected_sha1: Option<&String>, hasher: Sha1, file_path: &Path) -> Result<()> {
    if let Some(expected_sha1) = expected_sha1 {
        let sha1_bytes = hasher.finalize();
        let actual_sha1 = format!("{:x}", sha1_bytes);
        if expected_sha1 != &actual_sha1 {
            return Err(Error::DumpFileAccessError(
                file_path.to_owned(),
                "SHA1 digest differs from the expected one.".to_owned(),
            ));
        };
    }
    Ok(())
}

struct BytesChannelRead {
    current_bytes: Bytes,
    receiver: tokio::sync::mpsc::Receiver<Bytes>,
}
impl BytesChannelRead {
    fn from(receiver: tokio::sync::mpsc::Receiver<Bytes>) -> Self {
        Self {
            current_bytes: Bytes::new(),
            receiver,
        }
    }
}
impl Read for BytesChannelRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.current_bytes.is_empty() {
            match self.receiver.blocking_recv() {
                None => {
                    return std::io::Result::Ok(0);
                }
                Some(bytes) => self.current_bytes = bytes,
            }
        }
        let len = min(self.current_bytes.len(), buf.len());
        buf[..len].copy_from_slice(&self.current_bytes[..len]);
        self.current_bytes = self.current_bytes.slice(len..);
        std::io::Result::Ok(len)
    }
}

async fn download_file(
    url: String,
    file_path: PathBuf,
    partfile_path: PathBuf,
    client: &Client,
    decompress: bool,
    verify_file_data: Option<&DumpFileInfo>,
    progress_send: Option<UnboundedSender<DownloadProgress>>,
) -> Result<()> {
    let mut r = client.get(url).send().await?.error_for_status()?;
    let mut partfile = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&partfile_path)
        .map_err(|e| {
            Error::DumpFileAccessError(
                partfile_path.clone(),
                std::format!("Could not create part file: {0}", e),
            )
        })?;

    let progress_send_clone = progress_send.clone();
    defer! {
        if partfile_path.is_file() {
            if let Err(err) = remove_file(&partfile_path) {
                if let Some(progress_send_clone) = progress_send_clone {
                    progress_send_clone
                        .send(DownloadProgress::CouldNotRemoveTempFile(
                            partfile_path.clone(),
                            partfile_path
                                .file_name()
                                .unwrap_or_else(|| OsStr::new("<unknown>"))
                                .to_string_lossy()
                                .to_string(),
                            err,
                        ))
                        .ok();
                }
            }
        }
    }

    let expected_sha1 = verify_file_data.and_then(|info| info.sha1.as_ref());

    if decompress {
        let (decompress_send, decompress_receive) = mpsc::channel(1);

        let file_path = file_path.clone(); // clone since captured
        let copy_net_to_decompressor_in = {
            let progress_send = progress_send.clone();
            async move {
                let mut hasher = Sha1::new();
                while let Some(chunk) = r.chunk().await? {
                    if expected_sha1.is_some() {
                        hasher.update(chunk.as_ref());
                    }
                    let len = chunk.len() as u64;
                    if decompress_send.send(chunk).await.is_err() {
                        // decompressor has gone away unexpectedly - error handled there
                        return Ok(());
                    }
                    if let Some(ref progress_send) = progress_send {
                        progress_send.send(DownloadProgress::BytesReadFromNet(len))?;
                    }
                }
                verify_hash(expected_sha1, hasher, file_path.as_ref())?;
                Result::Ok(())
            }
        };

        let partfile_path = partfile_path.clone(); // clone since captured
        let decompression = spawn_blocking(move || {
            let compressed_read = BytesChannelRead::from(decompress_receive);
            let mut decompressor = MultiBzDecoder::new(compressed_read);
            let mut buf = [0; 65536];
            loop {
                let read_len = decompressor.read(&mut buf).map_err(Error::DecompressorError)?;
                if read_len > 0 {
                    let write_buf = &buf[..read_len];
                    partfile.write_all(write_buf).map_err(|e| {
                        Error::DumpFileAccessError(partfile_path.clone(), std::format!("Write error: {0}", e))
                    })?;
                    if let Some(ref progress_send) = progress_send {
                        progress_send.send(DownloadProgress::DecompressedBytesWrittenToDisk(read_len as u64))?;
                    }
                } else {
                    break;
                }
            }
            Result::Ok(())
        })
        .map_err(Error::DecompressorJoinError);

        let (_, decompression_joined) = tokio::try_join!(copy_net_to_decompressor_in, decompression)?;
        decompression_joined?;
    } else {
        let mut hasher = Sha1::new();
        while let Some(chunk) = r.chunk().await? {
            if expected_sha1.is_some() {
                hasher.update(chunk.as_ref());
            }
            partfile
                .write_all(chunk.as_ref())
                .map_err(|e| Error::DumpFileAccessError(partfile_path.clone(), std::format!("Write error: {0}", e)))?;
            if let Some(ref progress_send) = progress_send {
                progress_send.send(DownloadProgress::BytesReadFromNet(chunk.len() as u64))?;
            }
        }
        verify_hash(expected_sha1, hasher, file_path.as_ref())?;
    }

    std::fs::rename(&partfile_path, &file_path).map_err(|e| {
        Error::DumpFileAccessError(
            partfile_path.clone(),
            std::format!("Could not rename part file: {0}", e),
        )
    })?;

    Ok(())
}
#[derive(Default)]
pub struct DownloadOptions<'a> {
    pub mirror: Option<&'a str>,
    pub decompress: bool,
    pub concurrency: Option<NonZeroUsize>,
}

#[derive(Debug)]
pub enum DownloadProgress {
    TotalDownloadSize(u64),
    BytesReadFromNet(u64),
    DecompressedBytesWrittenToDisk(u64),
    ExistingFileIgnored(PathBuf, String),
    CouldNotRemoveTempFile(PathBuf, String, std::io::Error),
    FileFinished(PathBuf, String),
}

pub async fn download_dump<T>(
    client: &Client,
    wiki: &str,
    date: &str,
    dump_type: &str,
    target_directory: T,
    download_options: &DownloadOptions<'_>,
    progress_send: Option<UnboundedSender<DownloadProgress>>,
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
    let mut total_data_size = Some(0_u64);
    for (file_name, file_data) in files {
        let target_file_name = get_target_file_name(file_name, download_options.decompress).to_owned();
        let target_file_path = get_file_in_dir(target_directory, target_file_name.as_str());
        if target_file_path.exists() {
            if let Some(ref progress_send) = progress_send {
                progress_send.send(DownloadProgress::ExistingFileIgnored(
                    target_file_path,
                    target_file_name,
                ))?;
            }
            continue;
        }
        let part_file_path = get_file_in_dir(target_directory, (target_file_name.clone() + ".part").as_str());
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
            part_file_path.clone(),
            client,
            download_options.decompress,
            Some(file_data),
            progress_send.clone(),
        )
        .map_ok(|_| (target_file_name, target_file_path));
        futures.push(download_res);
    }
    if let Some(total_data_size) = total_data_size {
        if let Some(ref progress_send) = progress_send {
            progress_send.send(DownloadProgress::TotalDownloadSize(total_data_size))?;
        }
    }

    // download missing files
    let stream_of_downloads = stream::iter(futures);

    let max_concurrent_downloads = download_options.concurrency.map_or_else(
        || {
            if download_options.mirror.is_some() {
                if download_options.decompress {
                    num_cpus::get()
                } else {
                    4
                }
            } else {
                1
            }
        },
        NonZeroUsize::get,
    );
    let mut buffered = stream_of_downloads.buffer_unordered(max_concurrent_downloads);
    while let Some(res) = buffered.next().await {
        let (finished_file_name, finished_file_path) = res?;
        if let Some(ref progress_send) = progress_send {
            progress_send.send(DownloadProgress::FileFinished(finished_file_path, finished_file_name))?;
        }
    }

    Ok(())
}

pub async fn get_available_dates(client: &Client, wiki: &str) -> Result<Vec<String>> {
    let url = format!("https://dumps.wikimedia.org/{}/", wiki);
    let r = client.get(url.as_str()).send().await?.error_for_status()?;
    lazy_static! {
        static ref RE: Regex = Regex::new(r#"<a href="([1-9][0-9]{7})/">([1-9][0-9]{7})/</a>"#)
            .expect("Error parsing HTML dump date regex constant");
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
