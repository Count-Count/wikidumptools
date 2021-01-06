use anyhow::anyhow;
use anyhow::Result;
use chrono::DateTime;
use chrono_tz::Tz;
use clickhouse_rs::{row, types::Block, Pool};
use env::VarError;
use quick_xml::de::Deserializer;
use quick_xml::{events::Event, Reader};
use serde::Deserialize;
use std::env;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::str::from_utf8;
use std::{fs::File, time::Instant};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Page {
    title: String,
    ns: i16,
    id: u32,
    redirect: Option<Redirect>,
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
    text: Text, // TODO
    sha1: String,
    minor: Option<String>, // TODO
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Comment {
    #[serde(rename = "$value")]
    comment: Option<String>,
    deleted: Option<String>, // TODO
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Contributor {
    ip: Option<String>,
    username: Option<String>,
    id: Option<u32>,
    deleted: Option<String>, // TODO
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Text {
    bytes: Option<u32>,
    id: Option<u32>,
    deleted: Option<String>,
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

#[tokio::main]
async fn main() -> Result<()> {
    let dry_run = env::args().into_iter().nth(1).map_or(false, |arg| arg == "-n");

    let database_url = "tcp://localhost:9000/?compression=lz4";

    // env::set_var("RUST_LOG", "clickhouse_rs=debug");
    // env_logger::init();

    let create_stmt = "
    CREATE TABLE dewiki.revision
    (
        pageid UInt32 CODEC(Delta, ZSTD),
        namespace Int16 CODEC(Delta, ZSTD),
        title String CODEC(ZSTD),
        timestamp DateTime('UTC') CODEC(Delta, ZSTD),
        id UInt32 CODEC(Delta, ZSTD),
        parentid UInt32 CODEC(Delta, ZSTD),
        userid UInt32 CODEC(Delta, ZSTD),
        username String CODEC(ZSTD),
        ipv4 IPv4 CODEC(Delta, ZSTD),
        ipv6 IPv6 CODEC(ZSTD),
        comment String CODEC(ZSTD),
        textid UInt32 CODEC(Delta, ZSTD),
        textbytes UInt32 CODEC(Delta, ZSTD),
        model LowCardinality(String) CODEC(ZSTD),
        format LowCardinality(String) CODEC(ZSTD),
        sha1 FixedString(32) CODEC(NONE)
    )
    ENGINE = MergeTree()
--    PARTITION BY toYYYYMM(timestamp)
    PRIMARY KEY (pageid, timestamp)
    ";
    let pool = Pool::new(database_url);
    let mut client = pool.get_handle().await?;
    if !dry_run {
        client.execute("CREATE DATABASE IF NOT EXISTS dewiki").await?;
        client.execute("DROP TABLE IF EXISTS dewiki.revision").await?;
        client.execute(create_stmt).await?;
    }

    let ts = DateTime::parse_from_rfc3339("2001-05-31T08:19:59Z")
        .unwrap()
        .with_timezone(&Tz::Zulu);

    let home_dir = env::var("HOME").or_else::<VarError, _>(|_err| {
        let mut home = env::var("HOMEDRIVE")?;
        let homepath = env::var("HOMEPATH")?;
        home.push_str(homepath.as_ref());
        Ok(home)
    })?;
    let mut dump_file = PathBuf::from(home_dir);
    dump_file.push("wpdumps");
    dump_file.push("dewiki-20201201-stub-meta-history.xml");
    let file = File::open(&dump_file)?;
    let file_size = file.metadata().unwrap().len();
    let buf_size = 2 * 1024 * 1024;
    let buf_reader = BufReader::with_capacity(buf_size, file);
    let mut reader = Reader::from_reader(buf_reader);
    // let mut reader = Reader::from_str(test_page);
    reader.expand_empty_elements(true).check_end_names(true).trim_text(true);
    let mut buf: Vec<u8> = Vec::with_capacity(1000 * 1024);
    skip_to_end_tag(&mut reader, &mut buf, b"siteinfo")?;
    let mut deserializer = Deserializer::new(reader);
    let mut record_count: u32 = 0;
    let mut total_record_count: u32 = 0;
    let now = Instant::now();
    loop {
        let mut block = Block::with_capacity(100);
        let page = Page::deserialize(&mut deserializer).unwrap();
        // println!("Revisions: {}", page.revisions.len());
        for revision in page.revisions {
            let timestamp = DateTime::parse_from_rfc3339(revision.timestamp.as_ref())
                .unwrap()
                .with_timezone(&Tz::Zulu);

            let mut comment = "";
            if let Some(ref rev_comment) = revision.comment {
                if let Some(ref rev_comment_text) = rev_comment.comment {
                    comment = rev_comment_text.as_str();
                }
            }
            let username = revision.contributor.username.as_deref().unwrap_or("");
            let userid = revision.contributor.id.unwrap_or(0);
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

            block.push(row! {
                pageid: page.id,
                namespace: page.ns,
                title: page.title.as_str(),
                id: revision.id,
                parentid: revision.parentid.unwrap_or(0),
                timestamp: timestamp,
                comment: comment,
                model: revision.model.as_str(),
                format: revision.format.as_str(),
                sha1: revision.sha1.as_str(),
                ipv4: ipv4,
                ipv6: ipv6,
                username: username,
                userid: userid
            })?;
            total_record_count += 1;
            record_count += 1;
            if record_count == 100 {
                if !dry_run {
                    client.insert("dewiki.revision", block).await?;
                }
                record_count = 0;
                block = Block::with_capacity(100);
            }
        }
        if record_count > 0 {
            if !dry_run {
                client.insert("dewiki.revision", block).await?;
            }
        }
    }
    // if total_record_count == 10000 {
    //     break;
    // }
    // if record_count > 0 {
    //     client.insert("dewiki.revision", block).await?;
    // }
    let mib_read = file_size as f64 / 1024.0 / 1024.0;
    let elapsed_seconds = now.elapsed().as_secs_f64();

    eprintln!(
        "Read {} revisions ({:.2} MiB) in {:.2} seconds.",
        total_record_count, mib_read, elapsed_seconds,
    );
    Ok(())
}
