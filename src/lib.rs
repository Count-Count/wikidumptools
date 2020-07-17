// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use memchr::{memchr, memrchr};
use quick_xml::events::Event;
use quick_xml::Reader;
use regex::{Match, RegexBuilder};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::str::from_utf8_unchecked;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

fn from_unicode(s: &[u8]) -> &str {
    unsafe { from_utf8_unchecked(s) }
}

fn read_text_and_then<T: BufRead, ResT, F>(reader: &mut Reader<T>, buf: &mut Vec<u8>, mut f: F) -> ResT
where
    F: FnMut(&str) -> ResT,
{
    if let Event::Text(escaped_text) = reader.read_event(buf).unwrap() {
        let unescaped_text = escaped_text.unescaped().unwrap();
        let text = from_unicode(&unescaped_text);
        f(text)
    } else {
        panic!("Text expected");
    }
}

fn set_color(stream: &mut StandardStream, c: Color) {
    stream.set_color(ColorSpec::new().set_fg(Some(c))).unwrap();
}

fn set_plain(stream: &mut StandardStream) {
    stream.set_color(ColorSpec::new().set_fg(None)).unwrap();
}

pub fn search_dump(regex: &str, dump_file: &str, namespaces: &[&str]) {
    let re = RegexBuilder::new(regex).build().unwrap();
    let buf_size = 2 * 1024 * 1024;
    let file = File::open(&dump_file).unwrap();
    let buf_reader = BufReader::with_capacity(buf_size, file);
    let mut reader = Reader::from_reader(buf_reader);

    let mut buf: Vec<u8> = Vec::with_capacity(1000 * 1024);
    let mut title: String = String::with_capacity(10000);

    let only_print_title = false; // TODO: param

    let mut stdout = StandardStream::stdout(ColorChoice::Always);

    loop {
        match reader.read_event(&mut buf).unwrap() {
            Event::Start(ref e) => match e.name() {
                b"title" => {
                    read_text_and_then(&mut reader, &mut buf, |text| {
                        title.clear();
                        title.push_str(text);
                    });
                }
                b"ns" => {
                    let skip = read_text_and_then(&mut reader, &mut buf, |text| {
                        !namespaces.is_empty() && !namespaces.iter().any(|&i| i == text)
                    });
                    if skip {
                        reader.read_to_end(b"page", &mut buf).unwrap();
                    }
                }
                b"text" => {
                    read_text_and_then(&mut reader, &mut buf, |text| {
                        if only_print_title {
                            if re.is_match(text) {
                                set_color(&mut stdout, Color::Cyan);
                                writeln!(&mut stdout, "{}", title.as_str()).unwrap();
                                set_plain(&mut stdout);
                            }
                        } else {
                            let mut line_start_preceding_last_match: usize = 0;
                            let mut last_printed_lines_start: i64 = -1;
                            let mut last_printed_lines_end: i64 = -1;
                            let mut process_match_func = |m: Match| {
                                let lines_start = match memrchr(
                                    b'\n',
                                    &text.as_bytes()[line_start_preceding_last_match..m.start()],
                                ) {
                                    None => line_start_preceding_last_match,
                                    Some(newline_char_pos) => line_start_preceding_last_match + newline_char_pos + 1,
                                };
                                line_start_preceding_last_match = lines_start;
                                let lines_end = if m.end() > 0 && text.as_bytes()[m.end() - 1] == b'\n' {
                                    m.end() - 1
                                } else {
                                    match memchr(b'\n', &text.as_bytes()[m.end()..]) {
                                        None => text.len(),
                                        Some(newline_char_pos) => m.end() + newline_char_pos,
                                    }
                                };
                                // only print each region once
                                if last_printed_lines_start != lines_start as i64
                                    || last_printed_lines_end != lines_end as i64
                                {
                                    println!("{}", &text[lines_start..lines_end]);
                                    last_printed_lines_start = lines_start as i64;
                                    last_printed_lines_end = lines_end as i64;
                                }
                            };
                            let mut iter = re.find_iter(text);
                            if let Some(m) = iter.next() {
                                set_color(&mut stdout, Color::Cyan);
                                writeln!(&mut stdout, "{}", title.as_str()).unwrap();
                                set_plain(&mut stdout);
                                process_match_func(m);
                                for m in iter {
                                    process_match_func(m);
                                }
                                writeln!(&mut stdout).unwrap();
                            }
                        }
                    });
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
