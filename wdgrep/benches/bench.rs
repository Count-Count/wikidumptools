// wikidumpgrep
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use criterion::*;
use slice::IoSlice;
use std::env;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use wikidumpgrep::{search_dump, SearchOptions};

pub fn criterion_benchmark_file_reading(c: &mut Criterion) {
    let mut group = c.benchmark_group("file-io");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(140))
        .throughput(Throughput::Bytes(fs::metadata(get_dump_path()).unwrap().len()));

    static KB: usize = 1024;
    static MB: usize = KB * 1024;
    for buf_size in [MB, 2 * MB, 4 * MB].iter() {
        group.bench_with_input(BenchmarkId::new("file-reading", buf_size), &buf_size, |b, &buf_size| {
            b.iter(|| test_dump_reading(*buf_size));
        });
    }
    group.finish();
}

pub fn criterion_benchmark_file_reading_bz2(c: &mut Criterion) {
    let mut group = c.benchmark_group("file-io");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(140))
        .throughput(Throughput::Bytes(fs::metadata(get_dump_path()).unwrap().len()));

    static KB: usize = 1024;
    static MB: usize = KB * 1024;
    for buf_size in [MB, 2 * MB, 4 * MB].iter() {
        group.bench_with_input(
            BenchmarkId::new("file-reading-bz2", buf_size),
            &buf_size,
            |b, &buf_size| {
                b.iter(|| test_dump_reading_bz2(*buf_size));
            },
        );
    }
    group.finish();
}

pub fn criterion_benchmark_file_reading_in_parallel(c: &mut Criterion) {
    let mut group = c.benchmark_group("file-io");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(140))
        .throughput(Throughput::Bytes(fs::metadata(get_dump_path()).unwrap().len()));

    for thread_count in [2, 4, 6, 8, 12].iter() {
        group.bench_with_input(
            BenchmarkId::new("file-reading-parallel", thread_count),
            &thread_count,
            |b, &thread_count| {
                b.iter(|| test_dump_reading_in_parallel(2 * 1024 * 1024, *thread_count));
            },
        );
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
        b.iter(test_dump_searching);
    });
    group.finish();
}

fn test_dump_reading_in_parallel(buf_size: usize, thread_count: u32) {
    let thread_count = thread_count as u64;
    let mut thread_handles = Vec::with_capacity(thread_count as usize);
    for i in 0..thread_count {
        let handle = thread::spawn(move || {
            let dump_path = get_dump_path();
            let file = File::open(&dump_path).unwrap();
            let len = fs::metadata(dump_path).unwrap().len();
            let slice_size = len / thread_count;
            let slice = IoSlice::new(file, i * slice_size, slice_size).unwrap();
            let mut reader = BufReader::with_capacity(buf_size, slice);
            loop {
                let read_buf = reader.fill_buf().unwrap();
                let length = read_buf.len();
                if length == 0 {
                    break;
                }
                reader.consume(length);
            }
        });
        thread_handles.push(handle);
    }
    for handle in thread_handles {
        handle.join().unwrap();
    }
}

criterion_group!(
    benches,
    // criterion_benchmark_file_reading,
    criterion_benchmark_file_reading_bz2,
    // criterion_benchmark_file_reading_direct,
    // criterion_benchmark_file_reading_in_parallel,
    // criterion_benchmark_simple_search
);
criterion_main!(benches);

fn get_dump_path() -> PathBuf {
    let env_var =
        env::var("WIKIPEDIA_DUMPS_DIRECTORY").expect("WIKIPEDIA_DUMPS_DIRECTORY environment variable not set.");
    let dump_path = Path::new(env_var.as_str()).join(Path::new("dewiki-20200620-pages-articles-multistream.xml"));
    fs::metadata(&dump_path).expect("Dump file not found or inaccessible.");
    dump_path
}

fn get_dump_path_bz2() -> PathBuf {
    let env_var =
        env::var("WIKIPEDIA_DUMPS_DIRECTORY").expect("WIKIPEDIA_DUMPS_DIRECTORY environment variable not set.");
    let dump_path = Path::new(env_var.as_str()).join(Path::new("dewiki-20200701-pages-articles-multistream.xml.bz2"));
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

fn test_dump_reading_bz2(buf_size: usize) {
    let dump_path = get_dump_path_bz2();
    let file = File::open(&dump_path).unwrap();
    let mut bz2reader = bzip2::read::MultiBzDecoder::new(file);
    let mut bytes_read = 0;
    let mut buf: Vec<u8> = vec![0; buf_size];
    loop {
        match bz2reader.read(&mut buf) {
            Ok(0) => {
                break;
            }
            Ok(n) => {
                // ok
                bytes_read += n;
            }
            Err(_error) => {
                panic!("Error reading file");
            }
        }
    }
    println!("Decompressed bytes read: {}", bytes_read);
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
    let mut search_options = SearchOptions::new();
    search_options.restrict_namespaces(&["0"]);
    search_dump(
        "xyabcdefghijk",
        &[get_dump_path().to_str().unwrap().to_owned()],
        &search_options,
    )
    .unwrap();
}
