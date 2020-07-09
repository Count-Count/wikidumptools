// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use clap::{App, Arg};
use std::fs;
use std::time::Instant;
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
    let namespaces: Vec<&str> = matches.values_of("namespaces").unwrap_or_default().collect();

    let dump_len = fs::metadata(matches.value_of("dump file").unwrap()).unwrap().len();

    let now = Instant::now();
    search_dump(
        matches.value_of("search term").unwrap(),
        matches.value_of("dump file").unwrap(),
        &namespaces,
    );
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
