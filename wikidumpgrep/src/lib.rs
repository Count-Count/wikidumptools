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
use std::fs;
use std::fs::{metadata, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::Path;
use std::str::from_utf8;
use termcolor::{Buffer, BufferWriter, Color, ColorChoice, ColorSpec, WriteColor};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error {0}")]
    Io(#[from] std::io::Error),
    #[error("UTF8 format error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("XML format error: {0}")]
    Xml(quick_xml::Error),
    #[error("Regex error: {0}")]
    Regex(#[from] regex::Error),
    #[error("Only text expected in {0}")]
    OnlyTextExpectedInTag(String),
    #[error("Could not get current directory: {0}")]
    CouldNotGetCurrentDir(std::io::Error),
    #[error("File name not in UTF-8 format")]
    FileNameNotInUtf8(),
    #[error("Dump file (or prefix) is invalid")]
    DumpFileOrPrefixInvalid(),
    #[error("No dump files found")]
    NoDumpFilesFound(),
}

// unnest some XML parsing errors
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

pub fn search_dump(
    regex: &str,
    dump_files: &[String],
    namespaces: &[&str],
    only_print_title: bool,
    color_choice: ColorChoice,
) -> Result<()> {
    let re = RegexBuilder::new(regex).build()?;
    let stdout_writer = BufferWriter::stdout(color_choice);
    dump_files.into_par_iter().try_for_each(|dump_file| {
        let dump_file = dump_file.as_ref();
        let len = metadata(dump_file)?.len();
        let parts = ceiling_div(len, 500 * 1024 * 1024); // parts are at most 500 MiB
        let slice_size = ceiling_div(len, parts); // make sure to read to end

        (0..parts).into_par_iter().try_for_each(|i| {
            search_dump_part(
                &stdout_writer,
                &re,
                dump_file,
                i * slice_size,
                (i + 1) * slice_size,
                &namespaces,
                only_print_title,
            )
        })
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
    only_print_title: bool,
) -> Result<()> {
    let mut file = File::open(&dump_file)?;
    file.seek(SeekFrom::Start(start))?;
    let buf_size = 2 * 1024 * 1024;
    let mut buf_reader = BufReader::with_capacity(buf_size, file);
    search_dump_reader(
        stdout_writer,
        re,
        &mut buf_reader,
        start,
        end,
        namespaces,
        only_print_title,
    )
}

pub fn search_dump_reader<B: BufRead>(
    stdout_writer: &BufferWriter,
    re: &Regex,
    buf_reader: &mut B,
    start: u64,
    end: u64,
    namespaces: &[&str],
    only_print_title: bool,
) -> Result<()> {
    let mut reader = Reader::from_reader(buf_reader);
    reader.check_end_names(false);

    let mut buf: Vec<u8> = Vec::with_capacity(1000 * 1024);
    let mut title: String = String::with_capacity(10000);

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

pub fn get_dump_files(dump_file_or_prefix: &str) -> Result<(Vec<String>, u64)> {
    let mut dump_files = Vec::new();
    let mut total_size = 0;
    let metadata = fs::metadata(dump_file_or_prefix);
    match metadata {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(Error::DumpFileOrPrefixInvalid());
            }
            total_size += metadata.len();
            dump_files.push(dump_file_or_prefix.to_owned());
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // check if prefix
            let dump_file_or_prefix_path = Path::new(dump_file_or_prefix);
            let parent_dir = dump_file_or_prefix_path.parent().map_or_else(
                || Ok(std::env::current_dir().map_err(Error::CouldNotGetCurrentDir)?),
                |path| Result::Ok(path.to_owned()),
            )?;
            if !parent_dir.is_dir() {
                return Err(Error::DumpFileOrPrefixInvalid());
            }
            let prefix = dump_file_or_prefix_path
                .file_name()
                .ok_or(Error::DumpFileOrPrefixInvalid())?
                .to_str()
                .unwrap(); // must be UTF-8 because split from UTF-8 path
            for entry in fs::read_dir(parent_dir)? {
                let entry = entry?;
                let metadata = entry.metadata()?;
                if metadata.is_file() {
                    let file_name = entry.file_name();
                    let utf8_file_name = file_name.to_str().ok_or(Error::FileNameNotInUtf8())?;
                    if utf8_file_name.starts_with(prefix) {
                        dump_files.push(entry.path().to_str().ok_or(Error::FileNameNotInUtf8())?.to_owned());
                        total_size += metadata.len();
                    }
                }
            }
        }
        Err(e) => {
            return Err(Error::Io(e));
        }
    }
    if dump_files.is_empty() {
        return Err(Error::NoDumpFilesFound());
    }
    Ok((dump_files, total_size))
}
