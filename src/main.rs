// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use clap::{App, Arg};
use quick_xml::events::Event;
use quick_xml::Reader;
use regex::RegexBuilder;
use std::str::from_utf8_unchecked;

fn from_unicode(s: &[u8]) -> &str {
    unsafe { from_utf8_unchecked(s) }
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
                    if let Event::Text(ref t) = reader.read_event(&mut buf).unwrap() {
                        title.clear();
                        title.push_str(from_unicode(&t.unescaped().unwrap()));
                    } else {
                        panic!("Text expected");
                    }
                }
                b"ns" => {
                    if let Event::Text(ref t) = reader.read_event(&mut buf).unwrap() {
                        let unescaped = &t.unescaped().unwrap();
                        let ns = from_unicode(unescaped);
                        if !namespaces.is_empty() && !namespaces.iter().any(|&i| i == ns) {
                            // skip this page
                            reader.read_to_end(b"page", &mut buf).unwrap();
                        }
                    } else {
                        panic!("Text expected");
                    }
                }
                b"text" => {
                    if let Event::Text(ref t) = reader.read_event(&mut buf).unwrap() {
                        let unescaped = &t.unescaped().unwrap();
                        let text = from_unicode(unescaped);
                        if re.is_match(text) {
                            println!("* [[{}]]", title);
                        }
                    } else {
                        panic!("Text expected");
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
        .get_matches();
    let namespaces: Vec<&str> = matches.values_of("namespace").unwrap_or_default().collect();
    read_dump(
        matches.value_of("search term").unwrap(),
        matches.value_of("dump file").unwrap(),
        namespaces,
    );
}
