// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use atty;
use clap::{App, AppSettings, Arg};
use reqwest::Client;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::time::Instant;
use termcolor::ColorChoice;
use thiserror::Error;
use tokio::time;

#[derive(Error, Debug)]
pub enum WDGetError {
    #[error("Network I/O error {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("Error parsing JSON: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Received invalid JSON data from Wikidata")]
    InvalidJsonFromWikidata(),
    #[error("Dump status JSON data is invalid")]
    InvalidJsonFromDumpStatus(),
    #[error("Dump of this type was not found")]
    DumpTypeNotFound(),
    #[error("Dump is still in progress")]
    DumpNotComplete(),
    #[error("Dump does not contain any files")]
    DumpHasNoFiles(),
    #[error("Error accessing dump part file {0} - {1}")]
    DumpPartFileAccessError(String, String),
}

type Result<T> = std::result::Result<T, WDGetError>;

async fn get_available_wikis_from_wikidata() -> Result<Vec<String>> {
    let mut wikis = Vec::with_capacity(50);
    let sparql_url = "https://query.wikidata.org/sparql";
    let query = "
      SELECT DISTINCT ?id WHERE {
        ?item p:P1800/ps:P1800 ?id.
        FILTER(NOT EXISTS { ?item wdt:P31 wd:Q33120923. })
      }
      ";
    let blacklist = ["ecwikimedia", "labswiki", "labtestwiki", "ukwikiversity"];
    let client = create_client()?;
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
        let value = binding["id"]["value"]
            .as_str()
            .ok_or(WDGetError::InvalidJsonFromWikidata())?;
        if !blacklist.contains(&value) {
            wikis.push(value.to_owned());
        }
    }
    Ok(wikis)
}

async fn list_wikis() -> Result<()> {
    let mut wikis = get_available_wikis_from_wikidata().await?;
    wikis.sort();
    for wiki in wikis {
        println!("{}", wiki);
    }
    Ok(())
}

fn create_client() -> Result<Client> {
    Ok(reqwest::Client::builder()
        .user_agent("wdget/0.1 (https://github.com/Count-Count/wikidumptools)")
        .build()?)
}

#[derive(Deserialize)]
struct DumpStatus {
    #[allow(dead_code)]
    version: String,
    jobs: BTreeMap<String, DumpJobInfo>,
}

#[derive(Deserialize)]
struct DumpJobInfo {
    #[allow(dead_code)]
    updated: String,
    status: String,
    files: Option<BTreeMap<String, DumpFileInfo>>,
}

#[derive(Deserialize)]
struct DumpFileInfo {
    #[allow(dead_code)]
    url: Option<String>,
    #[allow(dead_code)]
    sha1: Option<String>,
    #[allow(dead_code)]
    size: Option<u64>,
    #[allow(dead_code)]
    md5: Option<String>,
}

async fn get_dump_status(wiki: &str, date: &str) -> Result<DumpStatus> {
    let client = create_client()?;
    let url = format!(
        "https://dumps.wikimedia.org/{}/{}/dumpstatus.json",
        wiki, date
    );
    let r = client.get(url.as_str()).send().await?.error_for_status()?;
    let body = r.text().await?;
    Ok(serde_json::from_str(body.as_str())?)
}

async fn list_types(wiki: &str, date: &str) -> Result<()> {
    let dump_status = get_dump_status(wiki, date).await?;
    for (job_name, job_info) in &dump_status.jobs {
        println!("{} - status: {}", &job_name, &job_info.status);
    }
    Ok(())
}

fn create_partfile_name(filename: &str) -> String {
    let mut res = String::from(filename);
    res.push_str(".part");
    res
}

async fn download(
    wiki: &str,
    date: &str,
    dump_type: &str,
    mirror: Option<&str>,
    verbose: bool,
) -> Result<()> {
    let dump_status = get_dump_status(wiki, date).await?;
    let job_info = dump_status
        .jobs
        .get(dump_type)
        .ok_or(WDGetError::DumpTypeNotFound())?;
    if &job_info.status != "done" {
        return Err(WDGetError::DumpNotComplete());
    }
    let files = job_info
        .files
        .as_ref()
        .ok_or(WDGetError::DumpHasNoFiles())?;
    let client = create_client()?;
    for (filename, file_data) in files {
        // TODO: sha1/md5/size checking
        let partfile_name = create_partfile_name(filename);
        if Path::new(&partfile_name).exists() {
            let partfile_metadata = fs::metadata(&partfile_name).map_err(|e| {
                WDGetError::DumpPartFileAccessError(
                    partfile_name.clone(),
                    std::format!("Could not get file information: {0}", e),
                )
            })?;
            if !partfile_metadata.is_file() {
                return Err(WDGetError::DumpPartFileAccessError(
                    partfile_name.clone(),
                    "Expected regular file".to_owned(),
                ));
            }
            let part_len = partfile_metadata.len();
            if file_data.size.is_some() && part_len > file_data.size.unwrap() {
                return Err(WDGetError::DumpPartFileAccessError(
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
        if verbose {
            eprint!("Downloading {}...", filename);
            std::io::stderr().flush().unwrap();
        }
        let root_url = mirror.unwrap_or("https://dumps.wikimedia.org");
        let url = format!("{}/{}/{}/{}", root_url, wiki, date, filename);
        let mut r = client.get(url.as_str()).send().await?.error_for_status()?;
        let mut bytes_read: u64 = 0;
        let progress_update_period = time::Duration::from_secs(1);
        let mut progress_update_interval = time::interval_at(
            tokio::time::Instant::now() + tokio::time::Duration::from_secs(1),
            progress_update_period,
        );
        let start_time = Instant::now();
        let mut prev_bytes_read = 0u64;
        let mut prev_time = Instant::now();
        let mut last_printed_progress_len = 0;
        let mut partfile = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&partfile_name)
            .map_err(|e| {
                WDGetError::DumpPartFileAccessError(
                    partfile_name.clone(),
                    std::format!("Could not create part file: {0}", e),
                )
            })?;
        loop {
            tokio::select! {
                chunk = r.chunk() => {
                    if let Some(chunk) = chunk? {
                        partfile.write_all(chunk.as_ref()).map_err(|e| {
                            WDGetError::DumpPartFileAccessError(
                                partfile_name.clone(),
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
                                &filename,
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
                }
            };
        }
        std::fs::rename(&partfile_name, &filename).map_err(|e| {
            WDGetError::DumpPartFileAccessError(
                partfile_name.clone(),
                std::format!("Could not rename part file: {0}", e),
            )
        })?;

        if verbose {
            eprintln!(
                "Downloaded {} - {:.2} MiB in {:.2} seconds ({:.2} MiB/s)",
                &filename,
                bytes_read as f64 / 1024.0 / 1024.0,
                start_time.elapsed().as_secs_f64(),
                bytes_read as f64 / 1024.0 / 1024.0 / start_time.elapsed().as_secs_f64()
            );
        } else {
            println!("Downloaded {}.", &filename);
        }
    }
    Ok(())
}

async fn run() -> Result<()> {
    let wiki_name_arg = Arg::with_name("wiki name")
        .help("Name of the wiki")
        .required(true);
    let dump_date_arg = Arg::with_name("dump date")
        .help("Date of the dump (YYYYMMDD)")
        .required(true);

    let matches = App::new("wikidumget")
        .version("0.1")
        .author("Count Count <countvoncount123456@gmail.com>")
        .about("Download Wikipedia dumps from the internet.")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::DeriveDisplayOrder)
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .help("Print performance statistics"),
        )
        .subcommand(
            App::new("download")
                .about("Download a wiki dump")
                .arg(wiki_name_arg.clone())
                .arg(dump_date_arg.clone())
                .arg(
                    Arg::with_name("dump type")
                        .help("Type of the dump")
                        .required(true),
                )
                .arg(
                    Arg::with_name("mirror")
                        .long("mirror")
                        .help("Root mirror URL")
                        .takes_value(true)
                        .max_values(1),
                ),
        )
        .subcommand(App::new("list-wikis").about("List all wikis for which dumps are available"))
        .subcommand(
            App::new("list-dates")
                .about("List all dump dates available for this wiki")
                .arg(wiki_name_arg.clone()),
        )
        .subcommand(
            App::new("list-types")
                .about("List all types available in this dump")
                .arg(wiki_name_arg.clone())
                .arg(dump_date_arg),
        )
        .subcommand(App::new("list-mirrors").about("List avaliable mirrors"))
        .get_matches();

    let _color_choice = if atty::is(atty::Stream::Stdout) {
        ColorChoice::Auto
    } else {
        ColorChoice::Never
    };
    match matches.subcommand_name().unwrap() {
        "list-wikis" => list_wikis().await?,
        "list-dates" => todo!(),
        "list-types" => {
            // todo: check args
            let subcommand_matches = matches.subcommand_matches("list-types").unwrap();
            list_types(
                subcommand_matches.value_of("wiki name").unwrap(),
                subcommand_matches.value_of("dump date").unwrap(),
            )
            .await?
        }
        "download" => {
            // todo: check args
            let subcommand_matches = matches.subcommand_matches("download").unwrap();
            download(
                subcommand_matches.value_of("wiki name").unwrap(),
                subcommand_matches.value_of("dump date").unwrap(),
                subcommand_matches.value_of("dump type").unwrap(),
                subcommand_matches.value_of("mirror"),
                matches.is_present("verbose"),
            )
            .await?
        }
        _ => unreachable!("Unknown subcommand, should be caught by arg matching."),
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    run().await.unwrap();
}
