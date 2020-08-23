// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use clap::{App, Arg};
use rayon::ThreadPoolBuilder;
use std::process;
use std::time::Instant;
use termcolor::ColorChoice;
use wikidumpgrep::{get_dump_files, search_dump, SearchDumpResult};

fn main() {
    let matches = App::new("wikidumpgrep")
        .version("0.1")
        .author("Count Count <countvoncount123456@gmail.com>")
        .about("Search through Wikipedia dumps using a regex search term.")
        .arg(Arg::with_name("search term").help("regex search term").required(true))
        .arg(
            Arg::with_name("dump file or prefix")
                .help("The dump file or common prefix of muliple dump files to search")
                .required(true),
        )
        .arg(
            Arg::with_name("namespaces")
                .long("ns")
                .takes_value(true)
                .use_delimiter(true)
                .help("Restrict search to those namespaces (comma-separated list of numeric namespaces)"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .help("Print performance statistics"),
        )
        .arg(
            Arg::with_name("list-titles")
                .short("l")
                .long("list-titles")
                .help("Only list title of articles containing matching text"),
        )
        .arg(
            Arg::with_name("threads")
                .short("j")
                .long("threads")
                .takes_value(true)
                .value_name("NUM")
                .help("Number of parallel threads to use. The default is the number of logical cpus."),
        )
        .arg(
            Arg::with_name("color")
                .long("color")
                .takes_value(true)
                .possible_values(&["always", "auto", "never"])
                .help("Colorize output, defaults to \"auto\" - output is colorized only if a terminal is detected"),
        )
        .get_matches();

    let search_term = matches.value_of("search term").unwrap();
    let dump_file_or_prefix = matches.value_of("dump file or prefix").unwrap();
    if dump_file_or_prefix.is_empty() {
        eprintln!("{}", matches.usage());
        process::exit(1);
    }

    let namespaces: Vec<&str> = matches
        .values_of("namespaces")
        .unwrap_or_default()
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .collect();

    let (dump_files, total_size) = get_dump_files(dump_file_or_prefix).unwrap_or_else(|err| {
        eprintln!("{}", err);
        process::exit(1);
    });

    let thread_count = matches
        .value_of("threads")
        .map(|val| val.parse::<usize>())
        .unwrap_or(Ok(0))
        .unwrap_or_else(|_err| {
            eprintln!("Invalid number specified for thread count");
            process::exit(1);
        });
    if thread_count != 0 {
        ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .build_global()
            .unwrap();
    }

    let color_choice = match matches.value_of("color").unwrap_or("auto") {
        "auto" => {
            if atty::is(atty::Stream::Stdout) {
                ColorChoice::Auto
            } else {
                ColorChoice::Never
            }
        }
        "always" => ColorChoice::Always,
        "never" => ColorChoice::Never,
        _ => unreachable!(),
    };
    let only_print_title = matches.is_present("list-titles");

    let now = Instant::now();
    match search_dump(search_term, &dump_files, &namespaces, only_print_title, color_choice) {
        Ok(SearchDumpResult {
            bytes_processed,
            compressed_files_found,
        }) => {
            let elapsed_seconds = now.elapsed().as_secs_f64();
            let mib_read = total_size as f64 / 1024.0 / 1024.0;
            let mib_read_uncompressed = bytes_processed as f64 / 1024.0 / 1024.0;
            if matches.is_present("verbose") {
                if compressed_files_found {
                    eprintln!(
                        "Searched {:.2} MiB compressed, {:.2} MiB uncompressed in {:.2} seconds ({:.2} MiB/s compressed, {:.2} MiB/s uncompressed).",
                        mib_read, mib_read_uncompressed,
                        elapsed_seconds,
                        mib_read / elapsed_seconds,
                        mib_read_uncompressed / elapsed_seconds
                    );
                } else {
                    eprintln!(
                        "Searched {:.2} MiB in {:.2} seconds ({:.2} MiB/s).",
                        mib_read,
                        elapsed_seconds,
                        mib_read / elapsed_seconds
                    );
                }
            }
        }
        Err(err) => {
            eprintln!("Error during search: {}", err);
            process::exit(1);
        }
    }
}
