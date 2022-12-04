// wdgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

mod lib;

use std::io::Write;
use std::num::NonZeroUsize;
use std::process;
use std::time::Instant;

use clap::{crate_authors, crate_version, Arg, ArgAction, Command};
use lib::{get_dump_files, search_dump, SearchDumpResult, SearchOptions};
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn exit_with_error(stderr: &mut StandardStream, msg: &str) -> ! {
    stderr.set_color(ColorSpec::new().set_fg(Some(Color::Red))).unwrap();
    writeln!(stderr, "{msg}").unwrap();
    stderr.reset().unwrap();
    process::exit(1);
}
fn main() {
    let matches = Command::new("WikiDumpGrep")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Search through Wikipedia and other Wikimedia wiki dumps using regular expressions.")
        .arg(Arg::new("search term").help("regex search term").required(true))
        .arg(
            Arg::new("dump file or prefix")
                .help("The dump file or common prefix of muliple dump files to search")
                .required(true),
        )
        .arg(
            Arg::new("namespaces")
                .long("ns")
                .value_delimiter(',')
                .help("Restrict search to those namespaces (comma-separated list of numeric namespaces)"),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Print performance statistics")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("revisions-with-matches")
                .short('l')
                .long("revisions-with-matches")
                .help("Only list title and revision of articles containing matching text")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("threads")
                .short('j')
                .long("threads")
                .value_name("num")
                .help("Number of parallel threads to use. The default is the number of logical cpus."),
        )
        .arg(
            Arg::new("color")
                .long("color")
                .value_parser(["always", "auto", "never"])
                .default_value("auto")
                .value_name("mode")
                .help("Colorize output, defaults to \"auto\" - output is colorized only if a terminal is detected"),
        )
        .arg(
            Arg::new("7z-binary")
                .long("7z-binary")
                .value_name("path")
                .help("Binary for extracting text from .7z files, defaults to \"7z\"."),
        )
        .arg(
            Arg::new("7z-options").long("7z-options").value_name("options").help(
                "Options passed to 7z binary for extracting text from .7z files to stdout, defaults to \"e -so\".",
            ),
        )
        .arg(
            Arg::new("bzcat-binary")
                .long("bzcat-binary")
                .value_name("path")
                .help("Binary for extracting text from .bz2 files to stdout, defaults to \"bzcat\"."),
        )
        .arg(
            Arg::new("bzcat-options")
                .long("bzcat-options")
                .value_name("options")
                .help("Options passed to bzcat binary for extracting text from .bz2 files, defaults to no options."),
        )
        .get_matches();

    let color_choice = match matches.get_one::<String>("color").unwrap().as_str() {
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

    let search_term = matches.get_one::<String>("search term").unwrap();
    let dump_file_or_prefix = matches.get_one::<String>("dump file or prefix").unwrap();
    if dump_file_or_prefix.is_empty() {
        exit_with_error(&mut stderr, "Non-empty dump file (prefix) needs to be specified.");
    }

    let (dump_files, total_size) = get_dump_files(dump_file_or_prefix).unwrap_or_else(|err| {
        exit_with_error(&mut stderr, format!("{err}").as_str());
    });

    let mut search_options = SearchOptions::new();

    search_options.with_color_choice(color_choice);

    let namespaces: Option<Vec<&str>> = matches
        .get_many::<String>("namespaces")
        .map(|val| val.map(|s| str::trim(s)).filter(|x| !x.is_empty()).collect());
    namespaces
        .as_deref()
        .map(|namespaces| search_options.restrict_namespaces(namespaces));

    matches
        .get_one::<String>("threads")
        .map(|s| str::parse::<NonZeroUsize>(s))
        .transpose()
        .unwrap_or_else(|_err| {
            exit_with_error(&mut stderr, "Invalid number specified for thread count");
        })
        .map(|thread_count| search_options.with_thread_count(thread_count));

    search_options.only_print_title(matches.get_flag("revisions-with-matches"));

    matches
        .get_one::<String>("7z-binary")
        .map(|binary| search_options.with_binary_7z(binary));

    let options_7z = matches
        .get_one::<String>("7z-options")
        .map(|s| s.split(' ').collect::<Vec<_>>());
    if let Some(options) = options_7z.as_ref() {
        search_options.with_options_7z(options);
    }

    matches
        .get_one::<String>("bzcat-binary")
        .map(|binary| search_options.with_binary_bzcat(binary));

    let options_bzcat = matches
        .get_one::<String>("bzcat-options")
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
            if matches.get_flag("verbose") {
                let mut number_hl_color = ColorSpec::new();
                number_hl_color.set_fg(Some(Color::Yellow));

                stderr.reset().unwrap();
                if compressed_files_found {
                    write!(stderr, "Searched ").unwrap();
                    stderr.set_color(&number_hl_color).unwrap();
                    write!(stderr, "{mib_read:.2}").unwrap();
                    stderr.reset().unwrap();
                    write!(stderr, " MiB compressed, ").unwrap();
                    stderr.set_color(&number_hl_color).unwrap();
                    write!(stderr, "{mib_read_uncompressed:.2}").unwrap();
                    stderr.reset().unwrap();
                    write!(stderr, " MiB uncompressed in ").unwrap();
                    stderr.set_color(&number_hl_color).unwrap();
                    write!(stderr, "{elapsed_seconds:.2}").unwrap();
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
                    write!(stderr, "{mib_read:.2}").unwrap();
                    stderr.reset().unwrap();
                    write!(stderr, " MiB in ").unwrap();
                    stderr.set_color(&number_hl_color).unwrap();
                    write!(stderr, "{elapsed_seconds:.2}").unwrap();
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
            exit_with_error(&mut stderr, format!("Error during search: {err}").as_str());
        }
    }
}
