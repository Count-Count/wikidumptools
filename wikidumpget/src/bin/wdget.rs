// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use atty;
use clap::{App, Arg};
use regex::RegexBuilder;
use reqwest::Client;
use std::collections::BTreeSet;
use std::iter::FromIterator;
use termcolor::ColorChoice;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WDGetError {
    #[error("Network I/O error {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("Error parsing JSON: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Error in JSON structure")]
    JsonStructureError(),
}

type Result<T> = std::result::Result<T, WDGetError>;

async fn get_available_wikis_from_wikidata(client: &Client) -> Result<Vec<String>> {
    let mut wikis = Vec::with_capacity(50);
    let sparql_url = "https://query.wikidata.org/sparql";
    let query = "
      SELECT DISTINCT ?id WHERE {
        ?item p:P1800/ps:P1800 ?id.
        FILTER(NOT EXISTS { ?item wdt:P31 wd:Q33120923. })
      }
      ";
    let client = reqwest::Client::builder().user_agent("wdget/0.1").build()?;
    let r = client
        .get(sparql_url)
        .query(&[("format", "json"), ("query", query.trim())])
        .send()
        .await?
        .error_for_status()?;
    let body = r.text().await?;
    let json: serde_json::Value = serde_json::from_str(body.as_str())?;
    let x = json.get("results").unwrap();
    if let serde_json::Value::Array(vec) = &json["results"]["bindings"] {
        for entry in vec {
            if let serde_json::Value::String(s) = &entry["id"]["value"] {
                wikis.push(s.to_owned());
            } else {
                return Err(WDGetError::JsonStructureError());
            }
        }
    } else {
        return Err(WDGetError::JsonStructureError());
    }
    Ok(wikis)
}

async fn get_available_wikis_dumps(client: &Client) -> Result<Vec<String>> {
    let mut wikis = Vec::with_capacity(50);
    let res = client
        .get("https://dumps.wikimedia.org/backup-index.html")
        .send()
        .await?
        .error_for_status()?;
    let body = res.text().await?;
    let regex = RegexBuilder::new(r#"<a href="([^/]+)/[0-9]{8}">"#)
        .build()
        .unwrap();
    for caps in regex.captures_iter(&body) {
        wikis.push(caps.get(1).unwrap().as_str().to_owned());
    }
    Ok(wikis)
}

async fn runx() -> Result<()> {
    let res = reqwest::get("http://httpbin.orgx/get").await?;
    println!("Status: {}", res.status());
    println!("Headers:\n{:#?}", res.headers());

    let body = res.text().await?;
    println!("Body:\n{}", body);
    Ok(())
}

async fn run() -> Result<()> {
    let matches = App::new("wikidumget")
        .version("0.1")
        .author("Count Count <countvoncount123456@gmail.com>")
        .about("Download Wikipedia dumps from the internet.")
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .help("print performance statistics"),
        )
        .get_matches();

    let color_choice = if atty::is(atty::Stream::Stdout) {
        ColorChoice::Auto
    } else {
        ColorChoice::Never
    };
    let client = reqwest::Client::builder().user_agent("wdget/0.1").build()?;
    let wdwikis = get_available_wikis_from_wikidata(&client).await.unwrap();
    let wdwikis_set: BTreeSet<String> = BTreeSet::from_iter(wdwikis);
    let dumpwikis = get_available_wikis_dumps(&client).await.unwrap();
    let dumpwikis_set: BTreeSet<String> = BTreeSet::from_iter(dumpwikis);
    println!("Wikis (currently) dumped but not in Wikidata:");
    for x in dumpwikis_set.difference(&wdwikis_set) {
        println!("* {}", x);
    }
    println!();
    println!("Wikis in Wikidata but not (currently) dumped:");
    for x in wdwikis_set.difference(&dumpwikis_set) {
        println!("* {}", x);
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    run().await.unwrap();
}
