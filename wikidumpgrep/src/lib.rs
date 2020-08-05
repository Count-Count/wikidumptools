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
use termcolor::{Buffer, BufferWriter, Color, ColorChoice, ColorSpec, WriteColor};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Utf8(std::str::Utf8Error),
    Xml(quick_xml::Error),
    Regex(regex::Error),
    OnlyTextExpectedInTag(String),
}

impl From<std::io::Error> for Error {
    #[inline]
    fn from(error: std::io::Error) -> Error {
        Error::Io(error)
    }
}

impl From<std::str::Utf8Error> for Error {
    #[inline]
    fn from(error: std::str::Utf8Error) -> Error {
        Error::Utf8(error)
    }
}

impl From<quick_xml::Error> for Error {
    #[inline]
    fn from(error: quick_xml::Error) -> Error {
        match error {
            quick_xml::Error::Utf8(e) => Error::Utf8(e),
            quick_xml::Error::Io(e) => Error::Io(e),
            error => Error::Xml(error),
        }
    }
}

impl From<regex::Error> for Error {
    #[inline]
    fn from(error: regex::Error) -> Error {
        Error::Regex(error)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {}", e),
            Error::Utf8(e) => write!(f, "UTF8 format error: {}", e),
            Error::Xml(e) => write!(f, "XML format error: {}", e),
            Error::Regex(e) => write!(f, "Regex error: {}", e),
            Error::OnlyTextExpectedInTag(tag) => write!(f, "Only text expected in {}", tag),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Utf8(e) => Some(e),
            Error::Xml(e) => Some(e),
            Error::Regex(e) => Some(e),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[inline(always)]
fn read_text_and_then<T: BufRead, ResT, F>(
    reader: &mut Reader<T>,
    buf: &mut Vec<u8>,
    tag: &str,
    mut f: F,
) -> Result<ResT>
where
    F: FnMut(&str) -> Result<ResT>,
{
    if let Event::Text(escaped_text) = reader.read_event(buf)? {
        let unescaped_text = escaped_text.unescaped()?;
        let text = from_utf8(&unescaped_text)?;
        f(text)
    } else {
        Err(Error::OnlyTextExpectedInTag(tag.to_owned()))
    }
}

enum SkipResult {
    StartTagFound,
    EOF,
}

#[inline(always)]
fn skip_to_start_tag<T: BufRead>(reader: &mut Reader<T>, buf: &mut Vec<u8>, tag_name: &[u8]) -> Result<SkipResult> {
    loop {
        match reader.read_event(buf)? {
            Event::Start(ref e) if e.name() == tag_name => {
                return Ok(SkipResult::StartTagFound);
            }
            Event::Eof => {
                return Ok(SkipResult::EOF);
            }
            _other_event => {}
        }
        buf.clear();
    }
}

#[inline(always)]
fn set_color(buffer: &mut Buffer, c: Color) {
    buffer.set_color(ColorSpec::new().set_fg(Some(c))).unwrap();
}

#[inline(always)]
fn set_plain(buffer: &mut Buffer) {
    buffer.set_color(ColorSpec::new().set_fg(None)).unwrap();
}

pub fn ceiling_div(x: u64, y: u64) -> u64 {
    (x + y - 1) / y
}

pub fn search_dump(regex: &str, dump_file: &str, namespaces: &[&str], color_choice: ColorChoice) -> Result<()> {
    let re = RegexBuilder::new(regex).build()?;
    let len = metadata(&dump_file as &str)?.len();
    let parts = ceiling_div(len, 500 * 1024 * 1024); // parts are at most 500 MiB
    let slice_size = ceiling_div(len, parts); // make sure to read to end
    let stdout_writer = BufferWriter::stdout(color_choice);

    (0..parts).into_par_iter().try_for_each(|i| {
        search_dump_part(
            &stdout_writer,
            &re,
            dump_file,
            i * slice_size,
            (i + 1) * slice_size,
            &namespaces as &[&str],
        )
    })?;
    Ok(())
}

pub fn search_dump_part(
    stdout_writer: &BufferWriter,
    re: &Regex,
    dump_file: &str,
    start: u64,
    end: u64,
    namespaces: &[&str],
) -> Result<()> {
    let mut file = File::open(&dump_file)?;
    file.seek(SeekFrom::Start(start))?;
    let buf_size = 2 * 1024 * 1024;
    let buf_reader = BufReader::with_capacity(buf_size, file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.check_end_names(false);

    let mut buf: Vec<u8> = Vec::with_capacity(1000 * 1024);
    let mut title: String = String::with_capacity(10000);

    let only_print_title = false; // TODO: param

    let mut stdout_buffer = stdout_writer.buffer();

    loop {
        if let SkipResult::EOF = skip_to_start_tag(&mut reader, &mut buf, b"page")? {
            break;
        }
        let page_tag_start_pos = reader.buffer_position() as u64 + start - b"<page>".len() as u64;
        if page_tag_start_pos >= end {
            break;
        }
        loop {
            match reader.read_event(&mut buf)? {
                Event::Start(ref e) => match e.name() {
                    b"title" => {
                        read_text_and_then(&mut reader, &mut buf, "title", |text| {
                            title.clear();
                            title.push_str(text);
                            Ok(())
                        })?;
                    }
                    b"ns" => {
                        let skip = read_text_and_then(&mut reader, &mut buf, "ns", |text| {
                            Ok(!namespaces.is_empty() && !namespaces.iter().any(|i| *i == text))
                        })?;
                        if skip {
                            reader.read_to_end(b"page", &mut buf)?;
                        }
                    }
                    b"text" => {
                        read_text_and_then(&mut reader, &mut buf, "text", |text| {
                            if only_print_title {
                                if re.is_match(text) {
                                    set_color(&mut stdout_buffer, Color::Cyan);
                                    writeln!(&mut stdout_buffer, "{}", title.as_str()).unwrap();
                                    set_plain(&mut stdout_buffer);
                                    stdout_writer.print(&stdout_buffer).unwrap();
                                    stdout_buffer.clear();
                                }
                            } else {
                                find_in_page(&mut stdout_buffer, title.as_str(), text, &re)?;
                                stdout_writer.print(&stdout_buffer).unwrap();
                                stdout_buffer.clear();
                            }
                            Ok(())
                        })?;
                        break;
                    }
                    _other_tag => { /* ignore */ }
                },
                Event::Eof => return Err(Error::Xml(quick_xml::Error::UnexpectedEof("page".to_owned()))),
                _other_event => (),
            }
            buf.clear();
        }
    }
    Ok(())
}

#[inline(always)]
fn find_in_page(buffer: &mut Buffer, title: &str, text: &str, re: &Regex) -> Result<()> {
    let mut last_match_end: usize = 0;
    let mut first_match = true;
    for m in re.find_iter(text) {
        if first_match {
            // print title once
            set_color(buffer, Color::Cyan);
            writeln!(buffer, "{}", title).unwrap();
            set_plain(buffer);
        }

        match memrchr(b'\n', &text.as_bytes()[last_match_end..m.start()]) {
            None => {
                // match starting on same line that the last match ended

                // print text between matches
                write!(buffer, "{}", &text[last_match_end..m.start()]).unwrap();
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
                            writeln!(buffer, "{}", &text[last_match_end..last_match_end + pos]).unwrap();
                        }
                    }
                }
                // print text in line preceding match
                write!(buffer, "{}", &text[last_match_end + pos + 1..m.start()]).unwrap();
            }
        };
        // print matched text

        // don't print extra newline and the following line if match end with \n
        let actual_match_end = if m.start() < m.end() && text.as_bytes()[m.end() - 1] == b'\n' {
            m.end() - 1
        } else {
            m.end()
        };
        set_color(buffer, Color::Red);
        write!(buffer, "{}", &text[m.start()..actual_match_end]).unwrap();
        set_plain(buffer);
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
                writeln!(buffer, "{}", &text[last_match_end..]).unwrap();
            }
            Some(pos) => {
                writeln!(buffer, "{}", &text[last_match_end..last_match_end + pos]).unwrap();
            }
        }
        // separate from next match
        writeln!(buffer).unwrap();
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_print() {
        let stdout_writer = BufferWriter::stdout(ColorChoice::Auto);
        let mut stdout_buffer = stdout_writer.buffer();
        find_in_page(
            &mut stdout_buffer,
            "title",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz",
            &RegexBuilder::new("Abc").build().unwrap(),
        )
        .unwrap();
        find_in_page(
            &mut stdout_buffer,
            "title",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz",
            &RegexBuilder::new("^").build().unwrap(),
        )
        .unwrap();
        find_in_page(
            &mut stdout_buffer,
            "title",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz\n",
            &RegexBuilder::new("Xyz\n").build().unwrap(),
        )
        .unwrap();
        find_in_page(
            &mut stdout_buffer,
            "title",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz\n",
            &RegexBuilder::new("\n").build().unwrap(),
        )
        .unwrap();
        find_in_page(
            &mut stdout_buffer,
            "title",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz\n",
            &RegexBuilder::new("123").build().unwrap(),
        )
        .unwrap();
        stdout_writer.print(&stdout_buffer).unwrap();
    }
}