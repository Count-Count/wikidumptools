// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use criterion::*;
use std::env;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::Duration;
use wikidumpgrep::search_dump;

pub fn criterion_benchmark_file_reading(c: &mut Criterion) {
    let mut group = c.benchmark_group("file-io");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(140))
        .throughput(Throughput::Bytes(fs::metadata(get_dump_path()).unwrap().len()));

    static KB: usize = 1024;
    static MB: usize = KB * 1024;
    for buf_size in [2 * MB].iter() {
        group.bench_with_input(BenchmarkId::new("file-reading", buf_size), &buf_size, |b, &buf_size| {
            b.iter(|| test_dump_reading(*buf_size));
        });
    }
    group.finish();
}

pub fn criterion_benchmark_file_reading_direct(c: &mut Criterion) {
    let mut group = c.benchmark_group("file-io");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(10))
        .throughput(Throughput::Bytes(fs::metadata(get_dump_path()).unwrap().len()));

    static KB: usize = 1024;
    static MB: usize = KB * 1024;
    for buf_size in [2 * MB].iter() {
        group.bench_with_input(
            BenchmarkId::new("file-reading-direct", buf_size),
            &buf_size,
            |b, &buf_size| {
                b.iter(|| test_dump_reading_direct(*buf_size));
            },
        );
    }
    group.finish();
}

pub fn criterion_benchmark_simple_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("dump-search");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(200))
        .throughput(Throughput::Bytes(fs::metadata(get_dump_path()).unwrap().len()));

    group.bench_function("simple-search", |b| {
        b.iter(|| test_dump_searching());
    });
    group.finish();
}

criterion_group!(
    benches,
    criterion_benchmark_file_reading,
    criterion_benchmark_file_reading_direct
);
criterion_main!(benches);

fn get_dump_path() -> PathBuf {
    let env_var =
        env::var("WIKIPEDIA_DUMPS_DIRECTORY").expect("WIKIPEDIA_DUMPS_DIRECTORY environment variable not set.");
    let dump_path = Path::new(env_var.as_str()).join(Path::new("dewiki-20200620-pages-articles-multistream.xml"));
    fs::metadata(&dump_path).expect("Dump file not found or inaccessible.");
    dump_path
}

fn test_dump_reading(buf_size: usize) {
    let dump_path = get_dump_path();
    let file = File::open(&dump_path).unwrap();
    let mut reader = BufReader::with_capacity(buf_size, file);
    loop {
        let read_buf = reader.fill_buf().unwrap();
        let length = read_buf.len();
        if length == 0 {
            break;
        }
        reader.consume(length);
    }
}

fn test_dump_reading_direct(buf_size: usize) {
    let dump_path = get_dump_path();
    let mut file = File::open(&dump_path).unwrap();
    let mut buf: Vec<u8> = vec![0; buf_size];
    loop {
        match file.read(&mut buf) {
            Ok(0) => {
                break;
            }
            Ok(_n) => {
                // ok
            }
            Err(_error) => {
                panic!("Error reading file");
            }
        }
    }
}

fn test_dump_searching() {
    search_dump("xyabcdefghijk", get_dump_path().to_str().unwrap(), &vec!["0"]);
}
