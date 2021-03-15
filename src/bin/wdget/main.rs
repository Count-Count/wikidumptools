// wdget
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

mod lib;

use std::env::current_dir;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process;

use anyhow::{anyhow, bail, Result};
use clap::{crate_authors, crate_version, App, AppSettings, Arg};
use lazy_static::lazy_static;
use lib::*;
use regex::Regex;
use reqwest::Client;
use termcolor::ColorChoice;

fn create_client() -> Result<Client> {
    Ok(reqwest::Client::builder()
        .user_agent(concat!(
            "wdget/",
            crate_version!(),
            " (https://github.com/Count-Count/wikidumptools)"
        ))
        .build()?)
}

async fn list_wikis(client: &Client) -> Result<()> {
    let mut wikis = get_available_wikis_from_wikidata(client).await?;
    wikis.sort_unstable_by(|e1, e2| e1.id.cmp(&e2.id));
    for ref wiki in wikis {
        println!("{} - {}", wiki.id.as_str(), wiki.name.as_str());
    }
    Ok(())
}

async fn list_dates(client: &Client, wiki: &str) -> Result<()> {
    let dates = get_available_dates(client, wiki).await?;
    for date in dates {
        println!("{}", date);
    }
    Ok(())
}

async fn list_types(client: &Client, wiki: &str, date: &str) -> Result<()> {
    let dump_status = get_dump_status(client, wiki, date).await?;
    for (job_name, job_info) in &dump_status.jobs {
        if let Some(files) = &job_info.files {
            let sum = files.values().map(|info| info.size.unwrap_or(0)).sum::<u64>();
            println!(
                "{} - status: {} - size: {:.2} MiB",
                &job_name,
                &job_info.status,
                sum as f64 / 1024.0 / 1024.0
            );
        } else {
            println!("{} - status: {}", &job_name, &job_info.status);
        }
    }
    Ok(())
}

fn check_date_valid(date_spec: &str) -> Result<()> {
    lazy_static! {
        static ref RE: Regex = Regex::new("[1-9][0-9]{7}$").expect("Error parsing dump date regex");
    }
    if RE.is_match(date_spec) {
        Ok(())
    } else {
        Err(anyhow::Error::from(Error::InvalidDumpDate()))
    }
}

async fn check_date_may_retrieve_latest(
    client: &Client,
    wiki: &str,
    date_spec: &str,
    dump_type: Option<&str>,
) -> Result<String> {
    if date_spec == "latest" {
        Ok(get_latest_available_date(client, wiki, dump_type).await?)
    } else {
        check_date_valid(date_spec).map(|_| date_spec.to_owned())
    }
}

async fn run() -> Result<()> {
    let wiki_name_arg = Arg::new("wiki name").about("Name of the wiki").required(true);
    let dump_date_arg = Arg::new("dump date")
        .about("Date of the dump (YYYYMMDD or 'latest')")
        .required(true);

    let matches = App::new("WikiDumpGet")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Download Wikipedia and other Wikimedia wiki dumps from the internet.")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::DeriveDisplayOrder)
        .setting(AppSettings::VersionlessSubcommands)
        .subcommand(
            App::new("download")
                .about("Download a wiki dump")
                .arg(wiki_name_arg.clone())
                .arg(dump_date_arg.clone())
                .arg(Arg::new("dump type").about("Type of the dump").required(true))
                .arg(
                    Arg::new("quiet")
                        .short('q')
                        .long("quiet")
                        .about("Don't print progress updates"),
                )
                .arg(
                    Arg::new("decompress")
                        .short('d')
                        .long("decompress")
                        .about("Decompress .bz2 files during download"),
                )
                .arg(
                    Arg::new("target-dir")
                        .short('t')
                        .long("target-dir")
                        .about("Target directory")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("mirror")
                        .short('m')
                        .long("mirror")
                        .about("Mirror root URL or one of the shortcuts 'acc.umu.se' and 'your.org'")
                        .takes_value(true),
                )
                .arg(
                    Arg::new("concurrency")
                        .short('j')
                        .long("concurrency")
                        .about("Number of parallel connections, defaults to 1 if no mirror, determined heuristically otherwise.")
                        .takes_value(true),
                ),
        )
        .subcommand(
            App::new("verify")
                .about("Verify an already downloaded wiki dump")
                .arg(wiki_name_arg.clone())
                .arg(dump_date_arg.clone())
                .arg(Arg::new("dump type").about("Type of the dump").required(true))
                .arg(
                    Arg::new("dir")
                        .short('d')
                        .long("dir")
                        .about("Directory with the dump files")
                        .takes_value(true),
                ),
        )
        .subcommand(App::new("list-wikis").about("List all wikis for which dumps are available"))
        .subcommand(
            App::new("list-dates")
                .about("List all dump dates available for this wiki")
                .arg(wiki_name_arg.clone())
                .arg(Arg::new("dump type").about("Type of the dump").required(false)),
        )
        .subcommand(
            App::new("list-dumps")
                .about("List all dumps available for this wiki at this date")
                .arg(wiki_name_arg.clone())
                .arg(dump_date_arg),
        )
        .subcommand(App::new("list-mirrors").about("List available mirrors"))
        .get_matches();

    let _color_choice = if atty::is(atty::Stream::Stdout) {
        ColorChoice::Auto
    } else {
        ColorChoice::Never
    };
    let client = create_client()?;
    match matches.subcommand_name().unwrap() {
        "list-wikis" => list_wikis(&client).await?,

        "list-dates" => {
            // todo: check args: wiki name, handle optional type, handle no dump found condition
            let subcommand_matches = matches.subcommand_matches("list-dates").unwrap();
            list_dates(&client, subcommand_matches.value_of("wiki name").unwrap()).await?;
        }

        "list-dumps" => {
            // todo: check args: wiki name; handle wiki/date not found, dump status file does not exist (yet)
            let subcommand_matches = matches.subcommand_matches("list-dumps").unwrap();
            let wiki = subcommand_matches.value_of("wiki name").unwrap();
            let date_spec = subcommand_matches.value_of("dump date").unwrap();
            let date = check_date_may_retrieve_latest(&client, wiki, date_spec, None).await?;
            eprintln!("Listing dumps for {}, dump run from {}", wiki, date);
            list_types(&client, wiki, &date).await?
        }

        "download" => {
            // todo: check args
            let subcommand_matches = matches.subcommand_matches("download").unwrap();
            let wiki = subcommand_matches.value_of("wiki name").unwrap();
            let date_spec = subcommand_matches.value_of("dump date").unwrap();
            let dump_type = subcommand_matches.value_of("dump type").unwrap();
            let date = check_date_may_retrieve_latest(&client, wiki, date_spec, Some(dump_type)).await?;
            let target_dir = match subcommand_matches.value_of("target-dir") {
                None => current_dir().map_err(|e| anyhow!("Current directory not accessible: {}", e))?,
                Some(dir) => PathBuf::from(dir),
            };
            if !target_dir.is_dir() {
                bail!("Target directory does not exist or is not accessible.")
            };
            let mirror = match subcommand_matches.value_of("mirror") {
                Some("acc.umu.se") => Some("https://ftp.acc.umu.se/mirror/wikimedia.org/dumps"),
                Some("your.org") => Some("http://dumps.wikimedia.your.org/"),
                Some(url) => Some(url),
                None => None,
            };

            let concurrency = subcommand_matches
                .value_of("concurrency")
                .map(str::parse::<NonZeroUsize>)
                .transpose()
                .map_err(|_| anyhow!("Invalid number for concurrency option."))?;
            match concurrency {
                Some(concurrency) if mirror.is_none() && concurrency.get() > 2 => {
                    bail!("A maximum of two concurrent connections are allowed for main Wikimedia dump website")
                }
                _ => {}
            }

            let download_options = DownloadOptions {
                mirror,
                verbose: !matches.is_present("quiet"),
                decompress: subcommand_matches.is_present("decompress"),
                concurrency,
            };
            download(&client, wiki, &date, dump_type, target_dir, &download_options).await?
        }
        "verify" => {
            let subcommand_matches = matches.subcommand_matches("verify").unwrap();
            let wiki = subcommand_matches.value_of("wiki name").unwrap();
            let date_spec = subcommand_matches.value_of("dump date").unwrap();
            check_date_valid(date_spec)?;
            let dump_type = subcommand_matches.value_of("dump type").unwrap();
            let dump_files_dir = match subcommand_matches.value_of("dir") {
                None => current_dir().map_err(|e| anyhow!("Current directory not accessible: {}", e))?,
                Some(dir) => PathBuf::from(dir),
            };
            if !dump_files_dir.is_dir() {
                bail!("Dump files directory does not exist or is not accessible.")
            };
            verify(&client, wiki, date_spec, dump_type, dump_files_dir).await?
        }
        _ => unreachable!("Unknown subcommand, should be caught by arg matching."),
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let res = run().await;
    if let Err(e) = res {
        eprintln!("{}", e);
        process::exit(1);
    }
}
