// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use memchr::{memchr, memrchr};
use quick_xml::events::Event;
use quick_xml::Reader;
use regex::{Regex, RegexBuilder};
use slice::IoSlice;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::str::from_utf8_unchecked;
use std::thread;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

fn from_unicode(s: &[u8]) -> &str {
    unsafe { from_utf8_unchecked(s) }
}

pub fn get_split_points(file: &File, parts: u64) -> Vec<u64> {
    let len = file.metadata().unwrap().len();
    let slice_size = len / parts;
    let mut res: Vec<u64> = Vec::with_capacity(parts as usize + 1);
    res.push(0);
    for i in 1..parts {
        let slice = IoSlice::new(file, i * slice_size, slice_size).unwrap();
        let buf_reader = BufReader::with_capacity(2 * 1024 * 1024, slice);
        let mut reader = Reader::from_reader(buf_reader);
        reader.check_end_names(false);
        let mut buf: Vec<u8> = Vec::with_capacity(1000 * 1024);
        skip_to_start_tag(&mut reader, &mut buf, b"page");
        let buf_pos = reader.buffer_position();
        let file_pos = buf_pos as u64 + i * slice_size - b"<page>".len() as u64;
        res.push(file_pos)
    }
    res.push(len);
    res
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

fn skip_to_start_tag<T: BufRead>(reader: &mut Reader<T>, buf: &mut Vec<u8>, tag_name: &[u8]) {
    loop {
        match reader.read_event(buf).unwrap() {
            Event::Start(ref e) if e.name() == tag_name => {
                return;
            }
            Event::Eof => {
                panic!("Unexpected EOF");
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
    let file = File::open(&dump_file).unwrap();
    let parts: usize = 8;
    let split_points = get_split_points(&file, parts as u64);
    let mut thread_handles = Vec::with_capacity(parts as usize);
    drop(file);
    for i in 0..parts {
        let re_clone = re.clone();
        let dump_file_clone = dump_file.to_owned();
        let start = split_points[i];
        let end = split_points[i + 1];
        let namespaces_clone: Vec<String> = namespaces.iter().cloned().map(String::from).collect();
        let handle =
            thread::spawn(move || search_dump_part(re_clone, dump_file_clone.as_str(), start, end, &namespaces_clone));
        thread_handles.push(handle);
    }
    for handle in thread_handles {
        handle.join().unwrap();
    }
}

pub fn search_dump_part(re: Regex, dump_file: &str, start: u64, end: u64, namespaces: &[String]) {
    let file = File::open(&dump_file).unwrap();
    let slice = IoSlice::new(file, start, end - start).unwrap();
    let buf_size = 2 * 1024 * 1024;
    let buf_reader = BufReader::with_capacity(buf_size, slice);
    let mut reader = Reader::from_reader(buf_reader);
    reader.check_end_names(false);

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
