// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use atty;
use clap::{App, Arg};
use std::fs;
use std::process;
use std::time::Instant;
use termcolor::ColorChoice;
use wikidumpgrep::search_dump;

fn main() {
    let matches = App::new("wikidumpgrep")
        .version("0.1")
        .author("Count Count <countvoncount123456@gmail.com>")
        .about("Search through Wikipedia dumps using a regex search term.")
        .arg(Arg::with_name("search term").help("regex search term").required(true))
        .arg(
            Arg::with_name("dump file")
                .help("the uncompressed dump file to search")
                .required(true),
        )
        .arg(
            Arg::with_name("namespaces")
                .long("ns")
                .takes_value(true)
                .use_delimiter(true)
                .help("restrict search to those namespaces (comma-separated list of numeric namespaces)"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .help("print performance statistics"),
        )
        .get_matches();

    let search_term = matches.value_of("search term").unwrap();
    let dump_file = matches.value_of("dump file").unwrap();
    if dump_file.len() == 0 {
        eprintln!("{}", matches.usage());
        process::exit(1);
    }

    let namespaces: Vec<&str> = matches
        .values_of("namespaces")
        .unwrap_or_default()
        .map(str::trim)
        .filter(|x| x.len() > 0)
        .collect();

    let dump_metadata = fs::metadata(dump_file).unwrap_or_else(|err| {
        match err.kind() {
            std::io::ErrorKind::NotFound => {
                eprintln!("Dump file {} not found.", dump_file);
            }
            _ => {
                eprintln!("{}", err);
            }
        }
        process::exit(1);
    });
    let dump_len = dump_metadata.len();

    let color_choice = if atty::is(atty::Stream::Stdout) {
        ColorChoice::Auto
    } else {
        ColorChoice::Never
    };

    let now = Instant::now();
    match search_dump(search_term, dump_file, &namespaces, color_choice) {
        Ok(()) => {
            let elapsed_seconds = now.elapsed().as_secs_f32();
            let mib_read = dump_len as f32 / 1024.0 / 1024.0;
            if matches.is_present("verbose") {
                eprintln!(
                    "Searched {} MiB in {} seconds ({} MiB/s).",
                    mib_read,
                    elapsed_seconds,
                    mib_read / elapsed_seconds
                );
            }
        }
        Err(err) => {
            eprintln!("Error during search: {}", err);
            process::exit(1);
        }
    }
}
