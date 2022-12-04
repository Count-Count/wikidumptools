// wdget
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

mod verify;

use std::env::current_dir;
use std::io::{stdout, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process;
use std::time::Instant;

use anyhow::{anyhow, bail, Result};
use clap::{crate_authors, crate_version, Arg, ArgAction, Command};
use lazy_static::lazy_static;
use regex::Regex;
use reqwest::Client;
use tabwriter::TabWriter;
use termcolor::ColorChoice;
use tokio::sync::mpsc::unbounded_channel;
use tokio::{pin, select, time};
use wdgetlib::*;

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
    let mut tw = TabWriter::new(stdout());
    writeln!(tw, "Wiki\tDescription").unwrap();
    for ref wiki in wikis {
        writeln!(tw, "{}\t{}", wiki.id.as_str(), wiki.name.as_str()).unwrap();
    }
    tw.flush().unwrap();
    Ok(())
}

async fn list_dates(client: &Client, wiki: &str) -> Result<()> {
    let dates = get_available_dates(client, wiki).await?;
    for date in dates {
        println!("{date}");
    }
    Ok(())
}

async fn list_types(client: &Client, wiki: &str, date: &str) -> Result<()> {
    let dump_status = get_dump_status(client, wiki, date).await?;
    let mut tw = TabWriter::new(stdout());
    writeln!(tw, "Dump\tStatus\tNo. of files\tCompressed size").unwrap();
    for (job_name, job_info) in &dump_status.jobs {
        if let Some(files) = &job_info.files {
            let sum = files.values().map(|info| info.size.unwrap_or(0)).sum::<u64>();
            writeln!(
                tw,
                "{}\t{}\t{:3} file(s)\t{:>10}",
                &job_name,
                &job_info.status,
                files.len(),
                get_human_size(sum)
            )
            .unwrap();
        } else {
            writeln!(tw, "{}\t{}", &job_name, &job_info.status).unwrap();
        }
    }
    tw.flush().unwrap();
    Ok(())
}

fn get_human_size(byte_len: u64) -> String {
    let mut len = byte_len as f64;
    let units = ["KiB", "MiB", "GiB", "TiB", "PiB"];
    if len < 1000.0 {
        return std::format!("{len:.0} bytes");
    }
    for unit in units {
        len /= 1024.0;
        if len < 1000.0 {
            return std::format!("{len:.2} {unit}");
        }
    }
    std::format!("{len:6.2} PiB")
}

fn check_date_valid(date_spec: &str) -> Result<()> {
    lazy_static! {
        static ref RE: Regex = Regex::new("[1-9][0-9]{7}$").expect("Error parsing dump date regex constant");
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

async fn download<T>(
    client: &Client,
    wiki: &str,
    date: &str,
    dump_type: &str,
    target_directory: T,
    download_options: &DownloadOptions<'_>,
    show_progress: bool,
    show_warnings: bool,
) -> Result<()>
where
    T: AsRef<Path> + Send,
{
    use DownloadProgress::*;
    let (progress_send, mut progress_receive) = unbounded_channel::<DownloadProgress>();
    let download_fut = download_dump(
        client,
        wiki,
        date,
        dump_type,
        target_directory,
        download_options,
        Some(progress_send),
    );
    pin!(download_fut);

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
    let mut decompressed_bytes_written = 0_u64;
    let mut total_data_size: Option<u64> = None;
    let mut download_finished = false;
    let mut progress_reporting_finished = false;
    let mut downloaded_file_count = 0;
    while !download_finished || !progress_reporting_finished {
        select! {
            download_res = &mut download_fut, if !download_finished => {
                download_res?;
                download_finished = true;
            }
            _ = tokio::signal::ctrl_c() => {
                return Err(anyhow::Error::from(wdgetlib::Error::AbortedByUser()));
            }
            download_progress = progress_receive.recv(), if !progress_reporting_finished => {
                match download_progress {
                    Some(BytesReadFromNet(count)) => {
                        bytes_received += count;
                    },
                    Some(DecompressedBytesWrittenToDisk(count)) => {
                        decompressed_bytes_written += count;
                    },
                    Some(TotalDownloadSize(size)) => {
                        total_data_size.replace(size);
                    },
                    Some(ExistingFileIgnored(_path, file_name)) => {
                        if show_warnings {
                            eprintln!("{file_name} exists, skipping.");
                        }
                    },
                    Some(FileFinished(_path, file_name)) => {
                        if show_progress {
                            eprint!("\r{:1$}\r","",last_printed_progress_len);
                            eprintln!("Completed download of {}.", &file_name);
                            downloaded_file_count += 1;
                        }
                    },
                    Some(CouldNotRemoveTempFile(_path, file_name, error)) => {
                        if show_warnings {
                            eprintln!("Could not remove temporary file {}: {}", file_name, &error);
                        }
                    }
                    None => {
                        progress_reporting_finished = true;
                    }
                }
            }
            _ = progress_update_interval.tick() => {
                if show_progress {
                    let speed =
                    if bytes_received - prev_bytes_received != 0  {
                        let bytes_per_sec = (bytes_received - prev_bytes_received) as f64 / prev_time.elapsed().as_secs_f64();
                        std::format!("({}/s)", get_human_size(bytes_per_sec as u64))
                    } else {
                        "(stalled)".to_string()
                    };
                    let mut progress_string =
                        if let Some(total_data_size) = total_data_size {
                            std::format!(
                                "\rDownloading {}- {} ({} %) of {} downloaded {}.",
                                if download_options.decompress {"and decompressing "} else {""},
                                get_human_size(bytes_received),
                                bytes_received * 100 / total_data_size,
                                get_human_size(total_data_size),
                                speed)
                        } else {
                            std::format!(
                                "\rDownloading {}- {} downloaded {}.",
                                if download_options.decompress {"and decompressing "} else {""},
                                get_human_size(bytes_received),
                                speed)
                        };
                    let new_printed_progress_len = progress_string.chars().count();
                    for _ in new_printed_progress_len..last_printed_progress_len {
                        progress_string.push(' ');
                    }
                    eprint!("{progress_string}");
                    std::io::stderr().flush().unwrap();
                    last_printed_progress_len = new_printed_progress_len;
                    prev_bytes_received = bytes_received;
                    prev_time = Instant::now();
                }
            }

        }
    }
    if show_progress {
        if downloaded_file_count > 0 {
            let total_mib = bytes_received as f64 / 1024.0 / 1024.0;
            let mib_per_sec = total_mib / start_time.elapsed().as_secs_f64();
            if download_options.decompress {
                eprintln!(
                    "\rDownloaded {:.2} MiB ({:.2} MiB/s) and decompressed to {:.2} MiB.",
                    total_mib,
                    mib_per_sec,
                    decompressed_bytes_written as f64 / 1024.0 / 1024.0
                );
            } else {
                eprintln!("\rDownloaded {total_mib:.2} MiB ({mib_per_sec:.2} MiB/s).");
            }
        } else {
            eprintln!("No files downloaded.");
        }
    }

    Ok(())
}

async fn run() -> Result<()> {
    let wiki_name_arg = Arg::new("wiki name").help("Name of the wiki").required(true);
    let dump_date_arg = Arg::new("dump date")
        .help("Date of the dump (YYYYMMDD or 'latest')")
        .required(true);

    let matches = Command::new("WikiDumpGet")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Download Wikipedia and other Wikimedia wiki dumps from the internet.")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("download")
                .about("Download a wiki dump")
                .arg(wiki_name_arg.clone())
                .arg(dump_date_arg.clone())
                .arg(Arg::new("dump type").help("Type of the dump").required(true))
                .arg(
                    Arg::new("quiet")
                        .short('q')
                        .long("quiet")
                        .help("Don't print progress updates")
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("decompress")
                        .short('d')
                        .long("decompress")
                        .help("Decompress .bz2 files during download")
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("target-dir")
                        .short('t')
                        .long("target-dir")
                        .help("Target directory"),
                )
                .arg(
                    Arg::new("mirror")
                        .short('m')
                        .long("mirror")
                        .help("Mirror root URL or one of the shortcuts 'acc.umu.se', 'your.org' and 'bringyour.com'"),
                )
                .arg(Arg::new("concurrency").short('j').long("concurrency").help(
                    "Number of parallel connections, defaults to 1 if no mirror, determined heuristically otherwise.",
                )),
        )
        .subcommand(
            Command::new("verify")
                .about("Verify an already downloaded wiki dump")
                .arg(wiki_name_arg.clone())
                .arg(dump_date_arg.clone())
                .arg(Arg::new("dump type").help("Type of the dump").required(true))
                .arg(
                    Arg::new("dir")
                        .short('d')
                        .long("dir")
                        .help("Directory with the dump files"),
                ),
        )
        .subcommand(Command::new("list-wikis").about("List all wikis for which dumps are available"))
        .subcommand(
            Command::new("list-dates")
                .about("List all dump dates available for this wiki")
                .arg(wiki_name_arg.clone())
                .arg(Arg::new("dump type").help("Type of the dump").required(false)),
        )
        .subcommand(
            Command::new("list-dumps")
                .about("List all dumps available for this wiki at this date")
                .arg(wiki_name_arg.clone())
                .arg(dump_date_arg),
        )
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
            list_dates(&client, subcommand_matches.get_one::<String>("wiki name").unwrap()).await?;
        }

        "list-dumps" => {
            // todo: check args: wiki name; handle wiki/date not found, dump status file does not exist (yet)
            let subcommand_matches = matches.subcommand_matches("list-dumps").unwrap();
            let wiki = subcommand_matches.get_one::<String>("wiki name").unwrap();
            let date_spec = subcommand_matches.get_one::<String>("dump date").unwrap();
            let date = check_date_may_retrieve_latest(&client, wiki, date_spec, None).await?;
            eprintln!("Listing dumps for {wiki}, dump run from {date}");
            list_types(&client, wiki, &date).await?;
        }

        "download" => {
            // todo: check args
            let subcommand_matches = matches.subcommand_matches("download").unwrap();
            let wiki = subcommand_matches.get_one::<String>("wiki name").unwrap();
            let date_spec = subcommand_matches.get_one::<String>("dump date").unwrap();
            let dump_type = subcommand_matches.get_one::<String>("dump type").unwrap();
            let date = check_date_may_retrieve_latest(&client, wiki, date_spec, Some(dump_type)).await?;
            let target_dir = match subcommand_matches.get_one::<String>("target-dir") {
                None => current_dir().map_err(|e| anyhow!("Current directory not accessible: {}", e))?,
                Some(dir) => PathBuf::from(dir),
            };
            if !target_dir.is_dir() {
                bail!("Target directory does not exist or is not accessible.")
            };
            let mirror = match subcommand_matches.get_one::<String>("mirror").map(String::as_str) {
                Some("acc.umu.se") => Some("https://ftp.acc.umu.se/mirror/wikimedia.org/dumps"),
                Some("your.org") => Some("http://dumps.wikimedia.your.org/"),
                Some("bringyour.com") => Some("https://wikimedia.bringyour.com/"),
                Some(url) => Some(url),
                None => None,
            };

            let concurrency = subcommand_matches
                .get_one::<String>("concurrency")
                .map(|s| str::parse::<NonZeroUsize>(s))
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
                decompress: subcommand_matches.get_flag("decompress"),
                concurrency,
            };
            let show_progress = !subcommand_matches.get_flag("quiet") && atty::is(atty::Stream::Stderr);
            let show_warnings = !subcommand_matches.get_flag("quiet");
            download(
                &client,
                wiki,
                &date,
                dump_type,
                target_dir,
                &download_options,
                show_progress,
                show_warnings,
            )
            .await?;
        }
        "verify" => {
            let subcommand_matches = matches.subcommand_matches("verify").unwrap();
            let wiki = subcommand_matches.get_one::<String>("wiki name").unwrap();
            let date_spec = subcommand_matches.get_one::<String>("dump date").unwrap();
            check_date_valid(date_spec)?;
            let dump_type = subcommand_matches.get_one::<String>("dump type").unwrap();
            let dump_files_dir = match subcommand_matches.get_one::<String>("dir") {
                None => current_dir().map_err(|e| anyhow!("Current directory not accessible: {}", e))?,
                Some(dir) => PathBuf::from(dir),
            };
            if !dump_files_dir.is_dir() {
                bail!("Dump files directory does not exist or is not accessible.")
            };
            verify::verify_downloaded_dump(&client, wiki, date_spec, dump_type, dump_files_dir).await?;
        }
        _ => unreachable!("Unknown subcommand, should be caught by arg matching."),
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let res = run().await;
    if let Err(e) = res {
        eprintln!("{e}");
        process::exit(1);
    }
}
