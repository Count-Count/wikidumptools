// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use clap::{App, AppSettings, Arg};
use std::io::Write;
use std::num::NonZeroUsize;
use std::process;
use std::time::Instant;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use wikidumpgrep::{get_dump_files, search_dump, SearchDumpResult, SearchOptions};

fn exit_with_error(stderr: &mut StandardStream, msg: &str) -> ! {
    stderr.set_color(ColorSpec::new().set_fg(Some(Color::Red))).unwrap();
    writeln!(stderr, "{}", msg).unwrap();
    process::exit(1);
}
fn main() {
    let matches = App::new("wikidumpgrep")
        .version("0.1")
        .author("Count Count <countvoncount123456@gmail.com>")
        .about("Search through Wikipedia dumps using a regex search term.")
        .setting(AppSettings::ColoredHelp)
        .arg(Arg::with_name("search term").about("regex search term").required(true))
        .arg(
            Arg::with_name("dump file or prefix")
                .about("The dump file or common prefix of muliple dump files to search")
                .required(true),
        )
        .arg(
            Arg::with_name("namespaces")
                .long("ns")
                .takes_value(true)
                .use_delimiter(true)
                .about("Restrict search to those namespaces (comma-separated list of numeric namespaces)"),
        )
        .arg(
            Arg::with_name("verbose")
                .short('v')
                .long("verbose")
                .about("Print performance statistics"),
        )
        .arg(
            Arg::with_name("revisions-with-matches")
                .short('l')
                .long("revisions-with-matches")
                .about("Only list title and revision of articles containing matching text"),
        )
        .arg(
            Arg::with_name("threads")
                .short('j')
                .long("threads")
                .takes_value(true)
                .value_name("num")
                .about("Number of parallel threads to use. The default is the number of logical cpus."),
        )
        .arg(
            Arg::with_name("color")
                .long("color")
                .takes_value(true)
                .possible_values(&["always", "auto", "never"])
                .value_name("mode")
                .about("Colorize output, defaults to \"auto\" - output is colorized only if a terminal is detected"),
        )
        .arg(
            Arg::with_name("7z-binary")
                .long("7z-binary")
                .takes_value(true)
                .value_name("path")
                .about("Binary for extracting text from .7z files, defaults to \"7z\"."),
        )
        .arg(
            Arg::with_name("7z-options")
                .long("7z-options")
                .takes_value(true)
                .value_name("options")
                .about(
                    "Options passed to 7z binary for extracting text from .7z files to stdout, defaults to \"e -so\".",
                ),
        )
        .arg(
            Arg::with_name("bzcat-binary")
                .long("bzcat-binary")
                .takes_value(true)
                .value_name("path")
                .about("Binary for extracting text from .bz2 files to stdout, defaults to \"bzcat\"."),
        )
        .arg(
            Arg::with_name("bzcat-options")
                .long("bzcat-options")
                .takes_value(true)
                .value_name("options")
                .about("Options passed to bzcat binary for extracting text from .bz2 files, defaults to no options."),
        )
        .get_matches();

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

    let mut stderr = StandardStream::stderr(color_choice);

    let search_term = matches.value_of("search term").unwrap();
    let dump_file_or_prefix = matches.value_of("dump file or prefix").unwrap();
    if dump_file_or_prefix.is_empty() {
        exit_with_error(&mut stderr, "Non-empty dump file (prefix) needs to be specified.");
    }

    let (dump_files, total_size) = get_dump_files(dump_file_or_prefix).unwrap_or_else(|err| {
        exit_with_error(&mut stderr, format!("{}", err).as_str());
    });

    let mut search_options = SearchOptions::new();

    search_options.with_color_choice(color_choice);

    let namespaces: Option<Vec<&str>> = matches
        .values_of("namespaces")
        .map(|val| val.map(str::trim).filter(|x| !x.is_empty()).collect());
    namespaces
        .as_deref()
        .map(|namespaces| search_options.restrict_namespaces(namespaces));

    matches
        .value_of("threads")
        .map(|val| val.parse::<NonZeroUsize>())
        .transpose()
        .unwrap_or_else(|_err| {
            exit_with_error(&mut stderr, "Invalid number specified for thread count");
        })
        .map(|thread_count| search_options.with_thread_count(thread_count));

    search_options.only_print_title(matches.is_present("revisions-with-matches"));

    matches
        .value_of("7z-binary")
        .map(|binary| search_options.with_binary_7z(binary));

    let options_7z = matches.value_of("7z-options").map(|s| s.split(' ').collect::<Vec<_>>());
    if let Some(options) = options_7z.as_ref() {
        search_options.with_options_7z(options);
    }

    matches
        .value_of("bzcat-binary")
        .map(|binary| search_options.with_binary_bzcat(binary));

    let options_bzcat = matches
        .value_of("bzcat-options")
        .map(|s| s.split(' ').collect::<Vec<_>>());
    if let Some(options) = options_bzcat.as_ref() {
        search_options.with_options_bzcat(options);
    }

    if dump_files.iter().any(|f| f.ends_with(".bz2")) {
        stderr.set_color(ColorSpec::new().set_fg(Some(Color::Yellow))).unwrap();
        writeln!(
            stderr,
            "Warning: Searching compressed .bz2 files is very slow, use .7z files or uncompressed files instead."
        )
        .unwrap();
    }

    let now = Instant::now();
    match search_dump(search_term, &dump_files, &search_options) {
        Ok(SearchDumpResult {
            bytes_processed,
            compressed_files_found,
        }) => {
            let elapsed_seconds = now.elapsed().as_secs_f64();
            let mib_read = total_size as f64 / 1024.0 / 1024.0;
            let mib_read_uncompressed = bytes_processed as f64 / 1024.0 / 1024.0;
            if matches.is_present("verbose") {
                let mut number_hl_color = ColorSpec::new();
                number_hl_color.set_fg(Some(Color::Yellow));

                stderr.reset().unwrap();
                if compressed_files_found {
                    write!(stderr, "Searched ").unwrap();
                    stderr.set_color(&number_hl_color).unwrap();
                    write!(stderr, "{:.2}", mib_read).unwrap();
                    stderr.reset().unwrap();
                    write!(stderr, " MiB compressed, ").unwrap();
                    stderr.set_color(&number_hl_color).unwrap();
                    write!(stderr, "{:.2}", mib_read_uncompressed).unwrap();
                    stderr.reset().unwrap();
                    write!(stderr, " MiB uncompressed in ").unwrap();
                    stderr.set_color(&number_hl_color).unwrap();
                    write!(stderr, "{:.2}", elapsed_seconds).unwrap();
                    stderr.reset().unwrap();
                    write!(stderr, " seconds (").unwrap();
                    stderr.set_color(&number_hl_color).unwrap();
                    write!(stderr, "{:.2}", mib_read / elapsed_seconds).unwrap();
                    stderr.reset().unwrap();
                    write!(stderr, " MiB/s compressed, ").unwrap();
                    stderr.set_color(&number_hl_color).unwrap();
                    write!(stderr, "{:.2}", mib_read_uncompressed / elapsed_seconds).unwrap();
                    stderr.reset().unwrap();
                    writeln!(stderr, " MiB/s uncompressed).").unwrap();
                } else {
                    write!(stderr, "Searched ").unwrap();
                    stderr.set_color(&number_hl_color).unwrap();
                    write!(stderr, "{:.2}", mib_read).unwrap();
                    stderr.reset().unwrap();
                    write!(stderr, " MiB in ").unwrap();
                    stderr.set_color(&number_hl_color).unwrap();
                    write!(stderr, "{:.2}", elapsed_seconds).unwrap();
                    stderr.reset().unwrap();
                    write!(stderr, " seconds (").unwrap();
                    stderr.set_color(&number_hl_color).unwrap();
                    write!(stderr, "{:.2}", mib_read / elapsed_seconds).unwrap();
                    stderr.reset().unwrap();
                    writeln!(stderr, " MiB/s).").unwrap();
                }
            }
        }
        Err(err) => {
            exit_with_error(&mut stderr, format!("Error during search: {}", err).as_str());
        }
    }
}
