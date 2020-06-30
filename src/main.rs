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
    let mut buf2: Vec<u8> = Vec::with_capacity(1000 * 1024);
    loop {
        match reader.read_event(&mut buf) {
            Ok(Event::Start(ref e)) => match e.name() {
                b"title" => match reader.read_event(&mut buf) {
                    Ok(Event::Text(ref t)) => {
                        let unescaped = &t.unescaped().unwrap();
                        let title = from_unicode(unescaped);
                        loop {
                            match reader.read_event(&mut buf2) {
                                Ok(Event::Start(ref e)) => match e.name() {
                                    b"ns" => match reader.read_event(&mut buf2) {
                                        Ok(Event::Text(ref t)) => {
                                            let unescaped = &t.unescaped().unwrap();
                                            let ns = from_unicode(unescaped);
                                            if !namespaces.is_empty() && !namespaces.iter().any(|&i| i == ns) {
                                                break;
                                            }
                                        }
                                        _ => {
                                            panic!("Text expected");
                                        }
                                    },
                                    b"text" => match reader.read_event(&mut buf2) {
                                        Ok(Event::Text(ref t)) => {
                                            let unescaped = &t.unescaped().unwrap();
                                            let text = from_unicode(unescaped);
                                            if re.is_match(text) {
                                                println!("* [[{}]]", title);
                                            }
                                            break;
                                        }
                                        _ => {
                                            panic!("Text expected");
                                        }
                                    },
                                    _tag => { /* ignore */ }
                                },
                                _ => { /* ignore */ }
                            }
                        }
                        buf2.clear();
                    }
                    _ => {
                        panic!("Text expected");
                    }
                },
                _tag => {}
            },
            Ok(Event::Eof) => {
                break;
            }
            _ => (),
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
