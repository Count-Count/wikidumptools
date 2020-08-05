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
use termcolor::ColorChoice;
use thiserror::Error;

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
    let mut wikis = get_available_wikis_from_wikidata().await.unwrap();
    wikis.sort();
    for wiki in wikis {
        println!("{}", wiki);
    }
    Ok(())
}

fn create_client() -> Result<Client> {
    Ok(reqwest::Client::builder().user_agent("wdget/0.1").build()?)
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
                        .help("Mirror to use")
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
        "list-types" => {
            // todo: check args
            let subcommand_matches = matches.subcommand_matches("list-types").unwrap();
            list_types(
                subcommand_matches.value_of("wiki name").unwrap(),
                subcommand_matches.value_of("dump date").unwrap(),
            )
            .await?
        }
        _ => panic!("Unknown subcommand, should be caught by arg matching."),
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    run().await.unwrap();
}
