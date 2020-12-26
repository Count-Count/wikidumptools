use chrono::DateTime;
use chrono_tz::Tz;
use clickhouse_rs::{row, types::Block, Pool};
use std::env;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let database_url = "tcp://localhost:9000/dewiki?compression=lz4";

    // env::set_var("RUST_LOG", "clickhouse_rs=debug");
    // env_logger::init();

    let delete_stmt = "DROP TABLE IF exists revision";
    let create_stmt = "
    CREATE TABLE revision 
    (
        pageid UInt32 CODEC(Delta, ZSTD),
        namespace Int8 CODEC(Delta, ZSTD),
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
    PARTITION BY toYYYYMM(timestamp)
    PRIMARY KEY (pageid, timestamp)
    ";
    let pool = Pool::new(database_url);
    let mut client = pool.get_handle().await?;
    client.execute(delete_stmt).await?;
    client.execute(create_stmt).await?;
    let mut block = Block::with_capacity(5);

    let ts = DateTime::parse_from_rfc3339("2001-05-31T08:19:59Z")
        .unwrap()
        .with_timezone(&Tz::Zulu);

    block.push(row! {
        pageid: 1_u32,
        namespace: 2_i8,
        title: "title",
        timestamp: ts,
        ipv4: "127.0.0.1",
        ipv6: "2001:4CA0:6FFF:3:0:0:0:17"
    })?;
    client.insert("revision", block).await?;
    Ok(())
}
