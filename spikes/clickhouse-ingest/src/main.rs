use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::str::from_utf8;

use anyhow::{anyhow, Result};
use chrono::DateTime;
use chrono_tz::Tz;
use clickhouse_rs::types::Block;
use clickhouse_rs::{row, ClientHandle, Pool};
use env::VarError;
use quick_xml::de::Deserializer;
use quick_xml::events::Event;
use quick_xml::{DeError, Reader};
use serde::Deserialize;

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Page {
    title: String,
    ns: i16,
    id: u32,
    redirect: Option<Redirect>, // TODO?
    #[serde(rename = "revision", default)]
    revisions: Vec<Revision>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Revision {
    id: u32,
    parentid: Option<u32>,
    contributor: Contributor,
    timestamp: String,
    comment: Option<Comment>,
    model: String,
    format: String,
    text: Text,
    sha1: String,
    minor: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Comment {
    #[serde(rename = "$value")]
    comment: Option<String>,
    deleted: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Contributor {
    ip: Option<String>,
    username: Option<String>,
    id: Option<u32>,
    deleted: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Text {
    bytes: Option<u32>,
    id: Option<u32>,
    deleted: Option<String>,
    #[serde(rename = "$value")]
    text: Option<String>,
    #[serde(rename = "xml:space")]
    xml_space: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Redirect {
    title: String,
}

#[inline(always)]
fn skip_to_end_tag<T: BufRead>(reader: &mut Reader<T>, buf: &mut Vec<u8>, tag_name: &[u8]) -> Result<()> {
    loop {
        match reader.read_event(buf)? {
            Event::End(ref e) if e.name() == tag_name => {
                return Ok(());
            }
            Event::Eof => {
                return Err(anyhow!(
                    "EOF while looking for end tag </{}>",
                    from_utf8(tag_name)?.to_owned()
                ));
            }
            _other_event => {}
        }
        buf.clear();
    }
}

async fn process_stream<T: BufRead + Send>(
    buf_reader: &mut T,
    client: &mut ClientHandle,
    database_name: &str,
    fill_revision_table: bool,
    dry_run: bool,
) -> Result<()> {
    let mut reader = Reader::from_reader(buf_reader);
    reader.expand_empty_elements(true).check_end_names(true).trim_text(true);
    let mut buf: Vec<u8> = Vec::with_capacity(1000 * 1024);
    skip_to_end_tag(&mut reader, &mut buf, b"siteinfo")?;
    let mut deserializer = Deserializer::new(reader);
    let mut record_count: u32 = 0;
    let table = if fill_revision_table {
        format!("{database_name}.revision")
    } else {
        format!("{database_name}.latest")
    };
    let mut block = Block::with_capacity(1000);
    loop {
        let page_res = Page::deserialize(&mut deserializer);
        if let Err(DeError::End) = page_res {
            // done
            break;
        }
        let page = page_res?;
        for revision in page.revisions {
            let timestamp = DateTime::parse_from_rfc3339(revision.timestamp.as_ref())
                .unwrap()
                .with_timezone(&Tz::Zulu);

            let mut comment = "";
            let mut commentdeleted = 0_u8;
            if let Some(ref rev_comment) = revision.comment {
                if let Some(ref rev_comment_text) = rev_comment.comment {
                    comment = rev_comment_text.as_str();
                } else if rev_comment.deleted.is_some() {
                    commentdeleted = 1;
                }
            }
            let mut ipv4 = "0.0.0.0";
            let mut ipv6 = "::";
            if let Some(s) = revision.contributor.ip.as_deref() {
                if s.contains('.') {
                    ipv4 = s;
                } else if s.contains(':') {
                    ipv6 = s;
                } else {
                    return Err(anyhow!("Could not parse IP address '{}'", s.to_owned()));
                }
            }
            if fill_revision_table {
                block.push(row! {
                    pageid: page.id,
                    namespace: page.ns,
                    title: page.title.as_str(),
                    revisionid: revision.id,
                    parentid: revision.parentid.unwrap_or(0),
                    timestamp: timestamp,
                    comment: comment,
                    model: revision.model.as_str(),
                    format: revision.format.as_str(),
                    sha1: revision.sha1.as_str(),
                    ipv4: ipv4,
                    ipv6: ipv6,
                    username: revision.contributor.username.as_deref().unwrap_or(""),
                    userid: revision.contributor.id.unwrap_or(0),
                    textid: revision.text.id.unwrap_or(0),
                    textbytes: revision.text.bytes.unwrap_or(0),
                    text: revision.text.text.as_deref().unwrap_or(""),
                    commentdeleted: commentdeleted,
                    userdeleted: u8::from(revision.contributor.deleted.is_some()),
                    textdeleted: u8::from(revision.text.deleted.is_some()),
                    minor: u8::from(revision.minor.is_some())
                })?;
            } else {
                block.push(row! {
                    pageid: page.id,
                    namespace: page.ns,
                    title: page.title.as_str(),
                    revisionid: revision.id,
                    parentid: revision.parentid.unwrap_or(0),
                    timestamp: timestamp,
                    comment: comment,
                    model: revision.model.as_str(),
                    format: revision.format.as_str(),
                    sha1: revision.sha1.as_str(),
                    ipv4: ipv4,
                    ipv6: ipv6,
                    username: revision.contributor.username.as_deref().unwrap_or(""),
                    userid: revision.contributor.id.unwrap_or(0),
                    textid: revision.text.id.unwrap_or(0),
                    text: revision.text.text.as_deref().unwrap_or(""),
                    textbytes: revision.text.bytes.unwrap_or(0),
                    commentdeleted: commentdeleted,
                    userdeleted: u8::from(revision.contributor.deleted.is_some()),
                    textdeleted: u8::from(revision.text.deleted.is_some()),
                    minor: u8::from(revision.minor.is_some())
                })?;
            }
            record_count += 1;
            if record_count == 1000 {
                if !dry_run {
                    client.insert(&table, block).await?;
                }
                record_count = 0;
                block = Block::with_capacity(1000);
            }
        }
    }
    if record_count > 0 && !dry_run {
        client.insert(&table, block).await?;
    }
    // let mib_read = file_size as f64 / 1024.0 / 1024.0;
    // let elapsed_seconds = now.elapsed().as_secs_f64();

    // eprintln!(
    //     "Read {} revisions ({:.2} MiB) in {:.2} seconds ({:.2} MiB/s).",
    //     total_record_count,
    //     mib_read,
    //     elapsed_seconds,
    //     mib_read / elapsed_seconds
    // );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let dry_run = env::args().into_iter().nth(1).map_or(false, |arg| arg == "-n");

    let database_url = "tcp://localhost:9000/?compression=lz4";

    // env::set_var("RUST_LOG", "clickhouse_rs=debug");
    // env_logger::init();

    let home_dir = env::var("HOME").or_else::<VarError, _>(|_err| {
        let mut home = env::var("HOMEDRIVE")?;
        let homepath = env::var("HOMEPATH")?;
        home.push_str(homepath.as_ref());
        Ok(home)
    })?;

    let file_name = env::args().into_iter().nth(1).unwrap();
    let mut dump_file = PathBuf::from(home_dir);
    dump_file.push("wpdumps");
    dump_file.push(file_name.as_str());

    let database_name = &file_name.as_str()[..file_name.find('-').unwrap()];

    let fill_revision_table = file_name.contains("-history");

    let create_revision_stmt = format!(
        "
    CREATE TABLE IF NOT EXISTS {database_name}.revision
    (
        pageid UInt32 CODEC(Delta, ZSTD),
        namespace Int16 CODEC(Delta, ZSTD),
        title String CODEC(ZSTD),
        timestamp DateTime('UTC') CODEC(Delta, ZSTD),
        revisionid UInt32 CODEC(Delta, ZSTD),
        parentid UInt32 CODEC(Delta, ZSTD),
        userid UInt32 CODEC(Delta, ZSTD),
        username String CODEC(ZSTD),
        ipv4 IPv4 CODEC(Delta, ZSTD),
        ipv6 IPv6 CODEC(ZSTD),
        comment String CODEC(ZSTD),
        text String CODEC(ZSTD(5)),
        textid UInt32 CODEC(Delta, ZSTD),
        textbytes UInt32 CODEC(Delta, ZSTD),
        model LowCardinality(String) CODEC(ZSTD),
        format LowCardinality(String) CODEC(ZSTD),
        sha1 FixedString(32) CODEC(ZSTD),
        minor UInt8 CODEC(Delta, ZSTD),
        commentdeleted UInt8 CODEC(Delta, ZSTD),
        userdeleted UInt8 CODEC(Delta, ZSTD),
        textdeleted UInt8 CODEC(Delta, ZSTD)
    )
    ENGINE = MergeTree()
--    PARTITION BY toYYYYMM(timestamp)
    PRIMARY KEY (pageid, timestamp)
    "
    );
    let create_latest_stmt = format!(
        "
    CREATE TABLE IF NOT EXISTS {database_name}.latest
    (
        pageid UInt32 CODEC(Delta, ZSTD),
        namespace Int16 CODEC(Delta, ZSTD),
        title String CODEC(ZSTD),
        timestamp DateTime('UTC') CODEC(Delta, ZSTD),
        revisionid UInt32 CODEC(Delta, ZSTD),
        parentid UInt32 CODEC(Delta, ZSTD),
        userid UInt32 CODEC(Delta, ZSTD),
        username String CODEC(ZSTD),
        ipv4 IPv4 CODEC(Delta, ZSTD),
        ipv6 IPv6 CODEC(ZSTD),
        comment String CODEC(ZSTD),
        textid UInt32 CODEC(Delta, ZSTD),
        textbytes UInt32 CODEC(Delta, ZSTD),
        text String CODEC(ZSTD(5)),
        model LowCardinality(String) CODEC(ZSTD),
        format LowCardinality(String) CODEC(ZSTD),
        sha1 FixedString(32) CODEC(ZSTD),
        minor UInt8 CODEC(Delta, ZSTD),
        commentdeleted UInt8 CODEC(Delta, ZSTD),
        userdeleted UInt8 CODEC(Delta, ZSTD),
        textdeleted UInt8 CODEC(Delta, ZSTD)
    )
    ENGINE = ReplacingMergeTree(revisionid)
    ORDER BY pageid
    "
    );
    let pool = Pool::new(database_url);
    let mut client = pool.get_handle().await?;
    if !dry_run {
        client
            .execute(format!("CREATE DATABASE IF NOT EXISTS {database_name}"))
            .await?;
        // client
        //     .execute(format!("DROP TABLE IF EXISTS {}.revision", database_name))
        //     .await?;
        if fill_revision_table {
            client.execute(create_revision_stmt).await?;
        } else {
            client.execute(create_latest_stmt).await?;
        }
    }

    let buf_size = 2 * 1024 * 1024;
    if file_name.ends_with(".gz") || file_name.ends_with(".bz2") || file_name.ends_with(".7z") {
        let mut command: Command;
        if file_name.ends_with(".gz") {
            command = Command::new("gzip");
            command.arg("-dc");
        } else if file_name.ends_with(".bz2") {
            command = Command::new("bzcat");
        } else {
            command = Command::new("7z");
            command.args(["e", "-so"]);
        }
        // necessary on Windows otherwise terminal colors are messed up with MSYS binaries (even /bin/false)
        command.stderr(Stdio::piped()).stdin(Stdio::piped());

        let mut handle = command.arg(dump_file).stdout(Stdio::piped()).spawn()?;
        let stdout = handle.stdout.take().unwrap(); // we have stdout bcs of command config
        let mut buf_reader = BufReader::with_capacity(buf_size, stdout);
        process_stream(
            &mut buf_reader,
            &mut client,
            database_name,
            fill_revision_table,
            dry_run,
        )
        .await?;
        let res = handle.wait_with_output()?; // needed since stderr is piped
        if !res.status.success() {
            return Err(anyhow!("gunzip failed: {}", from_utf8(res.stderr.as_ref())?.to_owned()));
        }
    } else {
        let file = File::open(&dump_file)?;
        let mut buf_reader = BufReader::with_capacity(buf_size, file);
        process_stream(
            &mut buf_reader,
            &mut client,
            database_name,
            fill_revision_table,
            dry_run,
        )
        .await?;
    }

    Ok(())
}
