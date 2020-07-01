// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use clap::{App, Arg};
use quick_xml::events::BytesText;
use quick_xml::events::Event;
use quick_xml::Reader;
use regex::RegexBuilder;
use std::borrow::Cow;
use std::fs;
use std::time::Instant;
use std::{io::BufRead, str::from_utf8_unchecked};

fn from_unicode(s: &[u8]) -> &str {
    unsafe { from_utf8_unchecked(s) }
}

fn read_text_unwrap<'a, 'b, T: BufRead>(reader: &'a mut Reader<T>, buf: &'b mut Vec<u8>) -> BytesText<'b> {
    if let Event::Text(t) = reader.read_event(buf).unwrap() {
        t
    } else {
        panic!("Text expected");
    }
}

pub fn unescape_unwrap<'a>(text: &'a BytesText) -> Cow<'a, [u8]> {
    text.unescaped().unwrap()
}

fn read_dump(regex: &str, dump_file: &str, namespaces: Vec<&str>) {
    let re = RegexBuilder::new(regex).build().unwrap();
    let mut reader = Reader::from_file(dump_file).unwrap();

    let mut buf: Vec<u8> = Vec::with_capacity(1000 * 1024);
    let mut title: String = String::with_capacity(10000);
    loop {
        match reader.read_event(&mut buf).unwrap() {
            Event::Start(ref e) => match e.name() {
                b"title" => {
                    let escaped_text = read_text_unwrap(&mut reader, &mut buf);
                    let unescaped_text = unescape_unwrap(&escaped_text);
                    let text = from_unicode(&unescaped_text);
                    title.clear();
                    title.push_str(text);
                }
                b"ns" => {
                    let escaped_text = read_text_unwrap(&mut reader, &mut buf);
                    let unescaped_text = unescape_unwrap(&escaped_text);
                    let text = from_unicode(&unescaped_text);
                    if !namespaces.is_empty() && !namespaces.iter().any(|&i| i == text) {
                        // skip this page
                        reader.read_to_end(b"page", &mut buf).unwrap();
                    }
                }
                b"text" => {
                    let escaped_text = read_text_unwrap(&mut reader, &mut buf);
                    let unescaped_text = unescape_unwrap(&escaped_text);
                    let text = from_unicode(&unescaped_text);
                    if re.is_match(text) {
                        println!("* [[{}]]", title);
                    }
                }
                _other_tag => { /* ignore */ }
            },
            Event::Eof => {
                break;
            }
            _other_event => (),
        }
        buf.clear();
    }
}

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
            Arg::with_name("namespace")
                .long("ns")
                .takes_value(true)
                .help("restrict search to those namespace (numeric)"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .help("print performance statistics"),
        )
        .get_matches();
    let namespaces: Vec<&str> = matches.values_of("namespace").unwrap_or_default().collect();

    let dump_len = fs::metadata(matches.value_of("dump file").unwrap()).unwrap().len();

    let now = Instant::now();
    read_dump(
        matches.value_of("search term").unwrap(),
        matches.value_of("dump file").unwrap(),
        namespaces,
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
