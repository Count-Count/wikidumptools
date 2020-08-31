// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use memchr::{memchr, memrchr};
use quick_xml::events::Event;
use quick_xml::Reader;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use regex::{Regex, RegexBuilder};
use std::fs;
use std::fs::{metadata, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::str::from_utf8;
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};
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
    #[error("Unexpected empty tag found: {0}")]
    UnexpectedEmptyTag(String),
    #[error("Could not get current directory: {0}")]
    CouldNotGetCurrentDir(std::io::Error),
    #[error("File name not in UTF-8 format")]
    FileNameNotInUtf8(),
    #[error("Dump file (or prefix) is invalid")]
    DumpFileOrPrefixInvalid(),
    #[error("No dump files found")]
    NoDumpFilesFound(),
    #[error("Subcommand could not be started: {0}")]
    SubCommandCouldNotBeStarted(std::io::Error),
    #[error("Subcommand terminated unsuccessfully. {0} Error output: '{1}'")]
    SubCommandTerminatedUnsuccessfully(std::process::ExitStatus, String),
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

enum SkipToStartTagOrEofResult {
    StartTagFound,
    EOF,
}

#[inline(always)]
fn skip_to_start_tag_or_eof<T: BufRead>(
    reader: &mut Reader<T>,
    buf: &mut Vec<u8>,
    tag_name: &[u8],
) -> Result<SkipToStartTagOrEofResult> {
    loop {
        match reader.read_event(buf)? {
            Event::Start(ref e) if e.name() == tag_name => {
                return Ok(SkipToStartTagOrEofResult::StartTagFound);
            }
            Event::Empty(ref e) if e.name() == tag_name => {
                return Err(Error::UnexpectedEmptyTag(from_utf8(tag_name)?.to_owned()));
            }
            Event::Eof => {
                return Ok(SkipToStartTagOrEofResult::EOF);
            }
            _other_event => {}
        }
        buf.clear();
    }
}

#[inline(always)]
fn skip_to_start_tag<T: BufRead>(reader: &mut Reader<T>, buf: &mut Vec<u8>, tag_name: &[u8]) -> Result<()> {
    if let SkipToStartTagOrEofResult::EOF = skip_to_start_tag_or_eof(reader, buf, tag_name)? {
        return Err(Error::Xml(quick_xml::Error::UnexpectedEof(
            from_utf8(tag_name)?.to_owned(),
        )));
    }
    Ok(())
}

enum SkipToStartTagOrEmptyTagResult {
    StartTagFound,
    EmptyTagFound,
}

#[inline(always)]
fn skip_to_start_tag_or_empty_tag<T: BufRead>(
    reader: &mut Reader<T>,
    buf: &mut Vec<u8>,
    tag_name: &[u8],
) -> Result<SkipToStartTagOrEmptyTagResult> {
    loop {
        let event = reader.read_event(buf)?;
        match event {
            Event::Start(ref e) if e.name() == tag_name => {
                return Ok(SkipToStartTagOrEmptyTagResult::StartTagFound);
            }
            Event::Empty(ref e) if e.name() == tag_name => {
                return Ok(SkipToStartTagOrEmptyTagResult::EmptyTagFound);
            }
            Event::Eof => {
                return Err(Error::Xml(quick_xml::Error::UnexpectedEof(
                    from_utf8(tag_name)?.to_owned(),
                )));
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

fn ceiling_div(x: u64, y: u64) -> u64 {
    (x + y - 1) / y
}

pub struct SearchDumpResult {
    pub bytes_processed: u64,
    pub compressed_files_found: bool,
}

pub struct SearchOptions<'a> {
    restrict_namespaces: Option<&'a [&'a str]>,
    only_print_title: bool,
    thread_count: Option<NonZeroUsize>,
    binary_7z: &'a str,
    options_7z: &'a [&'a str],
    binary_bzcat: &'a str,
    options_bzcat: &'a [&'a str],
    color_choice: ColorChoice,
}
impl<'a> SearchOptions<'a> {
    pub fn new() -> SearchOptions<'a> {
        SearchOptions {
            restrict_namespaces: None,
            only_print_title: false,
            thread_count: None,
            binary_7z: "7z",
            options_7z: &["e", "-so"],
            binary_bzcat: "bzcat",
            options_bzcat: &[],
            color_choice: ColorChoice::Never,
        }
    }
    pub fn restrict_namespaces(&mut self, restrict_namespaces: &'a [&'a str]) -> &mut SearchOptions<'a> {
        self.restrict_namespaces = Some(restrict_namespaces);
        self
    }
    pub fn only_print_title(&mut self, only_print_title: bool) -> &mut SearchOptions<'a> {
        self.only_print_title = only_print_title;
        self
    }
    pub fn with_thread_count(&mut self, thread_count: NonZeroUsize) -> &mut SearchOptions<'a> {
        self.thread_count = Some(thread_count);
        self
    }
    pub fn with_binary_7z(&mut self, binary_7z: &'a str) -> &mut SearchOptions<'a> {
        self.binary_7z = binary_7z;
        self
    }
    pub fn with_options_7z(&mut self, options_7z: &'a [&'a str]) -> &mut SearchOptions<'a> {
        self.options_7z = options_7z;
        self
    }
    pub fn with_binary_bzcat(&mut self, binary_bzcat: &'a str) -> &mut SearchOptions<'a> {
        self.binary_bzcat = binary_bzcat;
        self
    }
    pub fn with_options_bzcat(&mut self, options_bzcat: &'a [&'a str]) -> &mut SearchOptions<'a> {
        self.options_bzcat = options_bzcat;
        self
    }
    pub fn with_color_choice(&mut self, color_choice: ColorChoice) -> &mut SearchOptions<'a> {
        self.color_choice = color_choice;
        self
    }
}
impl<'a> Default for SearchOptions<'a> {
    fn default() -> Self {
        SearchOptions::new()
    }
}

pub fn search_dump(regex: &str, dump_files: &[String], search_options: &SearchOptions) -> Result<SearchDumpResult> {
    if let Some(thread_count) = search_options.thread_count {
        ThreadPoolBuilder::new()
            .num_threads(thread_count.get())
            .build_global()
            .unwrap();
    }
    let re = RegexBuilder::new(regex).build()?;
    let stdout_writer = BufferWriter::stdout(search_options.color_choice);
    let bytes_processed = AtomicU64::new(0);
    let compressed_file_found = AtomicBool::new(false);
    dump_files.into_par_iter().try_for_each(|dump_file| {
        let dump_file: &str = dump_file.as_ref();
        if dump_file.ends_with(".7z") || dump_file.ends_with(".bz2") {
            let mut command;
            if dump_file.ends_with(".7z") {
                command = Command::new(search_options.binary_7z);
                command.args(search_options.options_7z);
            } else {
                command = Command::new(search_options.binary_bzcat);
                command.args(search_options.options_bzcat);
            };
            // necessary on Windows otherwise terminal colors are messed up with MSYS binaries (even /bin/false)
            command.stderr(Stdio::piped()).stdin(Stdio::piped());

            let mut handle = command
                .arg(dump_file)
                .stdout(Stdio::piped())
                .spawn()
                .map_err(Error::SubCommandCouldNotBeStarted)?;
            let stdout = handle.stdout.take().unwrap(); // we have stdout bcs of command config
            let buf_size = 2 * 1024 * 1024;
            let mut buf_reader = BufReader::with_capacity(buf_size, stdout);
            let search_res = search_dump_reader(
                &stdout_writer,
                &re,
                &mut buf_reader,
                0,
                u64::MAX,
                search_options.restrict_namespaces.unwrap_or(&[]),
                search_options.only_print_title,
            );
            if search_res.is_err() {
                eprintln!("Error searching {}", dump_file);
            }
            let bytes_processed_0 = search_res?;
            compressed_file_found.fetch_or(true, Ordering::Relaxed);
            bytes_processed.fetch_add(bytes_processed_0, Ordering::Relaxed);
            let res = handle.wait_with_output()?; // needed since stderr is piped
            if res.status.success() {
                Ok(())
            } else {
                Err(Error::SubCommandTerminatedUnsuccessfully(
                    res.status,
                    from_utf8(res.stderr.as_ref())?.to_owned(),
                ))
            }
        } else {
            let len = metadata(dump_file)?.len();
            let parts = ceiling_div(len, 500 * 1024 * 1024); // parts are at most 500 MiB
            let slice_size = ceiling_div(len, parts); // make sure to read to end

            (0..parts).into_par_iter().try_for_each(|i| {
                let bytes_processed_0 = search_dump_part(
                    &stdout_writer,
                    &re,
                    dump_file,
                    i * slice_size,
                    (i + 1) * slice_size,
                    search_options.restrict_namespaces.unwrap_or(&[]),
                    search_options.only_print_title,
                )?;
                bytes_processed.fetch_add(bytes_processed_0, Ordering::Relaxed);
                Ok(())
            })
        }
    })?;

    Ok(SearchDumpResult {
        bytes_processed: bytes_processed.load(Ordering::Relaxed),
        compressed_files_found: compressed_file_found.load(Ordering::Relaxed),
    })
}

fn search_dump_part(
    stdout_writer: &BufferWriter,
    re: &Regex,
    dump_file: &str,
    start: u64,
    end: u64,
    namespaces: &[&str],
    only_print_title: bool,
) -> Result<u64> {
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

fn search_dump_reader<B: BufRead>(
    stdout_writer: &BufferWriter,
    re: &Regex,
    buf_reader: &mut B,
    start: u64,
    end: u64,
    namespaces: &[&str],
    only_print_title_and_revision: bool,
) -> Result<u64> {
    let mut reader = Reader::from_reader(buf_reader);
    reader.check_end_names(false);

    let mut buf: Vec<u8> = Vec::with_capacity(1000 * 1024);
    let mut title: String = String::with_capacity(10000);
    let mut revision_id: String = String::with_capacity(50);

    let mut stdout_buffer = stdout_writer.buffer();

    loop {
        if let SkipToStartTagOrEofResult::EOF = skip_to_start_tag_or_eof(&mut reader, &mut buf, b"page")? {
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
                            break;
                        }
                    }
                    b"revision" => {
                        skip_to_start_tag(&mut reader, &mut buf, b"id")?;
                        read_text_and_then(&mut reader, &mut buf, "id", |text| {
                            revision_id.clear();
                            revision_id.push_str(text);
                            Ok(())
                        })?;
                        if let SkipToStartTagOrEmptyTagResult::StartTagFound =
                            skip_to_start_tag_or_empty_tag(&mut reader, &mut buf, b"text")?
                        {
                            read_text_and_then(&mut reader, &mut buf, "text", |text| {
                                if only_print_title_and_revision {
                                    if re.is_match(text) {
                                        set_color(&mut stdout_buffer, Color::Cyan);
                                        write!(&mut stdout_buffer, "{}", title.as_str()).unwrap();
                                        set_plain(&mut stdout_buffer);
                                        write!(&mut stdout_buffer, "@").unwrap();
                                        set_color(&mut stdout_buffer, Color::Yellow);
                                        writeln!(&mut stdout_buffer, "{}", revision_id.as_str()).unwrap();
                                        set_plain(&mut stdout_buffer);
                                        stdout_writer.print(&stdout_buffer).unwrap();
                                        stdout_buffer.clear();
                                    }
                                } else {
                                    find_in_text(&mut stdout_buffer, title.as_str(), revision_id.as_str(), text, &re)?;
                                    stdout_writer.print(&stdout_buffer).unwrap();
                                    stdout_buffer.clear();
                                }
                                Ok(())
                            })?;
                        }
                    }
                    _other_tag => { /* ignore */ }
                },
                Event::End(bytes_end) if bytes_end.name() == b"page" => {
                    break;
                }
                Event::Eof => return Err(Error::Xml(quick_xml::Error::UnexpectedEof("page".to_owned()))),
                _other_event => (),
            }
            buf.clear();
        }
    }
    Ok(reader.buffer_position() as u64)
}

#[inline(always)]
fn find_in_text(buffer: &mut Buffer, title: &str, revision_id: &str, text: &str, re: &Regex) -> Result<()> {
    let mut last_match_end: usize = 0;
    let mut first_match = true;
    for m in re.find_iter(text) {
        if first_match {
            // print title once
            set_color(buffer, Color::Cyan);
            write!(buffer, "{}", title).unwrap();
            set_plain(buffer);
            write!(buffer, "@").unwrap();
            set_color(buffer, Color::Yellow);
            writeln!(buffer, "{}", revision_id).unwrap();
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

            // if there are multiple versions of the same file prefer plain to .7z to .bz2
            dump_files.sort_unstable();

            fn get_stem(s: &str) -> &str {
                if s.ends_with(".7z") {
                    &s[..s.len() - ".7z".len()]
                } else if s.ends_with(".bz2") {
                    &s[..s.len() - ".bz2".len()]
                } else {
                    s
                }
            }
            let mut i = 0;
            while i + 1 < dump_files.len() {
                if get_stem(dump_files[i].as_str()) == get_stem(dump_files[i + 1].as_str()) {
                    dump_files.remove(i + 1);
                    continue;
                }
                i += 1;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_print() {
        let stdout_writer = BufferWriter::stdout(ColorChoice::Auto);
        let mut stdout_buffer = stdout_writer.buffer();
        find_in_text(
            &mut stdout_buffer,
            "title",
            "revision_id",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz",
            &RegexBuilder::new("Abc").build().unwrap(),
        )
        .unwrap();
        find_in_text(
            &mut stdout_buffer,
            "title",
            "revision_id",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz",
            &RegexBuilder::new("^").build().unwrap(),
        )
        .unwrap();
        find_in_text(
            &mut stdout_buffer,
            "title",
            "revision_id",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz\n",
            &RegexBuilder::new("Xyz\n").build().unwrap(),
        )
        .unwrap();
        find_in_text(
            &mut stdout_buffer,
            "title",
            "revision_id",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz\n",
            &RegexBuilder::new("\n").build().unwrap(),
        )
        .unwrap();
        find_in_text(
            &mut stdout_buffer,
            "title",
            "revision_id",
            "Abc Xyz Abc Xyz\n123 456\nAbc Xyz Abc Xyz\n",
            &RegexBuilder::new("123").build().unwrap(),
        )
        .unwrap();
        stdout_writer.print(&stdout_buffer).unwrap();
    }
}
