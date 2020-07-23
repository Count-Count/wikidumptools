// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use memchr::{memchr, memrchr};
use quick_xml::events::Event;
use quick_xml::Reader;
use rayon::prelude::*;
use regex::{Regex, RegexBuilder};
use std::fs::{metadata, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::str::from_utf8;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

fn read_text_and_then<T: BufRead, ResT, F>(reader: &mut Reader<T>, buf: &mut Vec<u8>, mut f: F) -> ResT
where
    F: FnMut(&str) -> ResT,
{
    if let Event::Text(escaped_text) = reader.read_event(buf).unwrap() {
        let unescaped_text = escaped_text.unescaped().unwrap();
        let text = from_utf8(&unescaped_text).unwrap();
        f(text)
    } else {
        panic!("Text expected");
    }
}

enum SkipResult {
    StartTagFound,
    EOF,
}

fn skip_to_start_tag<T: BufRead>(reader: &mut Reader<T>, buf: &mut Vec<u8>, tag_name: &[u8]) -> SkipResult {
    loop {
        match reader.read_event(buf).unwrap() {
            Event::Start(ref e) if e.name() == tag_name => {
                return SkipResult::StartTagFound;
            }
            Event::Eof => {
                return SkipResult::EOF;
            }
            _other_event => {}
        }
        buf.clear();
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
    let parts: usize = 120;
    let len = metadata(dump_file).unwrap().len();
    let slice_size = len / parts as u64;
    (0..parts as u64).into_par_iter().for_each(|i| {
        let re_clone = re.clone();
        let dump_file_clone = dump_file.to_owned();
        let namespaces_clone: Vec<String> = namespaces.iter().cloned().map(String::from).collect();
        search_dump_part(
            re_clone,
            dump_file_clone.as_str(),
            i * slice_size,
            (i + 1) * slice_size,
            &namespaces_clone,
        );
    });
}

pub fn search_dump_part(re: Regex, dump_file: &str, start: u64, end: u64, namespaces: &[String]) {
    let mut file = File::open(&dump_file).unwrap();
    file.seek(SeekFrom::Start(start)).unwrap();
    let buf_size = 2 * 1024 * 1024;
    let buf_reader = BufReader::with_capacity(buf_size, file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.check_end_names(false);

    let mut buf: Vec<u8> = Vec::with_capacity(1000 * 1024);
    let mut title: String = String::with_capacity(10000);

    let only_print_title = false; // TODO: param

    let mut stdout = StandardStream::stdout(ColorChoice::Always);

    loop {
        if let SkipResult::EOF = skip_to_start_tag(&mut reader, &mut buf, b"page") {
            break;
        }
        let page_tag_start_pos = reader.buffer_position() as u64 + start - b"<page>".len() as u64;
        if page_tag_start_pos >= end {
            break;
        }
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
                            !namespaces.is_empty() && !namespaces.iter().any(|i| i == text)
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
                                find_in_page(&mut stdout, title.as_str(), text, &re);
                            }
                        });
                        break;
                    }
                    _other_tag => { /* ignore */ }
                },
                Event::Eof => {
                    panic!("Unexpected EOF during file reading");
                }
                _other_event => (),
            }
            buf.clear();
        }
    }
}

fn find_in_page(stdout: &mut StandardStream, title: &str, text: &str, re: &Regex) {
    let mut last_match_end: usize = 0;
    let mut first_match = true;
    for m in re.find_iter(text) {
        if first_match {
            // print title once
            set_color(stdout, Color::Cyan);
            writeln!(stdout, "{}", title).unwrap();
            set_plain(stdout);
        }

        match memrchr(b'\n', &text.as_bytes()[last_match_end..m.start()]) {
            None => {
                // match starting on same line that the last match ended

                // print text between matches
                write!(stdout, "{}", &text[last_match_end..m.start()]).unwrap();
            }
            Some(pos) => {
                // match starting on a new line

                // finish line from previous match
                if !first_match {
                    match memchr(b'\n', &text.as_bytes()[last_match_end..m.start()]) {
                        None => {
                            panic!("Memchr/Memrchr inconsistency");
                        }
                        Some(pos) => {
                            writeln!(stdout, "{}", &text[last_match_end..last_match_end + pos]).unwrap();
                        }
                    }
                }
                // print text in line preceding match
                write!(stdout, "{}", &text[last_match_end + pos + 1..m.start()]).unwrap();
            }
        };
        // print matched text

        // don't print extra newline and the following line if match end with \n
        let actual_match_end = if m.start() < m.end() && text.as_bytes()[m.end() - 1] == b'\n' {
            m.end() - 1
        } else {
            m.end()
        };
        set_color(stdout, Color::Red);
        write!(stdout, "{}", &text[m.start()..actual_match_end]).unwrap();
        set_plain(stdout);
        last_match_end = actual_match_end;
        if first_match {
            first_match = false;
        }
    }
    let matches_found = !first_match;
    if matches_found {
        // print rest of last matching line
        match memchr(b'\n', &text.as_bytes()[last_match_end..]) {
            None => {
                writeln!(stdout, "{}", &text[last_match_end..]).unwrap();
            }
            Some(pos) => {
                writeln!(stdout, "{}", &text[last_match_end..last_match_end + pos]).unwrap();
            }
        }
        // separate from next match
        writeln!(stdout).unwrap();
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_print() {
        let mut stdout = StandardStream::stdout(ColorChoice::Always);
        find_in_page(
            &mut stdout,
            "title",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz",
            &RegexBuilder::new("Abc").build().unwrap(),
        );
        find_in_page(
            &mut stdout,
            "title",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz",
            &RegexBuilder::new("^").build().unwrap(),
        );
        find_in_page(
            &mut stdout,
            "title",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz\n",
            &RegexBuilder::new("Xyz\n").build().unwrap(),
        );
        find_in_page(
            &mut stdout,
            "title",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz\n",
            &RegexBuilder::new("\n").build().unwrap(),
        );
        find_in_page(
            &mut stdout,
            "title",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz\n",
            &RegexBuilder::new("123").build().unwrap(),
        );
    }
}
