// wdget
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use reqwest::Client;
use sha1::{Digest, Sha1};
use wdgetlib::{get_dump_status, DumpFileInfo, Error};

type Result<T> = std::result::Result<T, Error>;

pub async fn verify_downloaded_dump<T>(
    client: &Client,
    wiki: &str,
    date: &str,
    dump_type: &str,
    dump_files_directory: T,
) -> Result<()>
where
    T: AsRef<Path> + Send,
{
    let dump_files_directory = dump_files_directory.as_ref();
    if !dump_files_directory.exists() {
        return Err(Error::TargetDirectoryDoesNotExist(dump_files_directory.to_owned()));
    }
    let dump_status = get_dump_status(client, wiki, date).await?;
    let job_info = dump_status.jobs.get(dump_type).ok_or(Error::DumpTypeNotFound())?;
    if &job_info.status != "done" {
        return Err(Error::DumpNotComplete());
    }
    let files = job_info.files.as_ref().ok_or(Error::DumpHasNoFiles())?;
    for (file_name, file_data) in files {
        let target_file_name = get_target_file_name(file_name, false);
        let target_file_path = get_file_in_dir(dump_files_directory, target_file_name);
        if !target_file_path.exists() {
            let decompressed_target_file_name = get_target_file_name(file_name, true);
            let decompressed_target_file_path = get_file_in_dir(dump_files_directory, decompressed_target_file_name);
            if decompressed_target_file_path.exists() {
                return Err(Error::DecompressedFileCannotBeVerified(
                    decompressed_target_file_name.to_owned(),
                ));
            } else {
                return Err(Error::FileToBeVerifiedNotFound(target_file_name.to_owned()));
            }
        }
        verify_existing_file(&target_file_path, target_file_name, file_data, true)?;
    }
    Ok(())
}

fn verify_existing_file(file_path: &Path, file_name: &str, file_data: &DumpFileInfo, verbose: bool) -> Result<()> {
    let file_metadata = fs::metadata(file_path).map_err(|e| {
        Error::DumpFileAccessError(
            file_path.to_owned(),
            std::format!("Could not get file information: {0}", e),
        )
    })?;
    if let Some(expected_file_size) = &file_data.size {
        if *expected_file_size != file_metadata.len() {
            return Err(Error::DumpFileAccessError(
                file_path.to_owned(),
                std::format!(
                    "Dump file size does not match the expected size. Expected: {}, actual: {}.",
                    expected_file_size,
                    file_metadata.len()
                ),
            ));
        }
    }
    match file_data.sha1.as_ref() {
        Some(expected_sha1) => {
            let mut file = fs::File::open(file_path).map_err(|e| {
                Error::DumpFileAccessError(file_path.to_owned(), std::format!("Could not read mapping file: {}", e))
            })?;
            if verbose {
                eprint!("Verifying {}...", file_name);
                std::io::stderr().flush().unwrap();
            }
            let start_time = Instant::now();
            let mut hasher = Sha1::new();
            let hashed_bytes = std::io::copy(&mut file, &mut hasher).map_err(|e| {
                Error::DumpFileAccessError(file_path.to_owned(), std::format!("Could not read mapping file: {}", e))
            })?;
            let sha1_bytes = hasher.finalize();
            let actual_sha1 = format!("{:x}", sha1_bytes);
            if expected_sha1 != &actual_sha1 {
                return Err(Error::DumpFileAccessError(
                    file_path.to_owned(),
                    "SHA1 digest differs from the expected one.".to_owned(),
                ));
            };
            if verbose {
                eprintln!(
                    "\rVerified {} - OK - {:.2} MiB in {:.2} seconds ({:.2} MiB/s)",
                    file_name,
                    hashed_bytes as f64 / 1024.0 / 1024.0,
                    start_time.elapsed().as_secs_f64(),
                    hashed_bytes as f64 / 1024.0 / 1024.0 / start_time.elapsed().as_secs_f64()
                );
            } else {
                println!("Verified {} - OK.", &file_name);
            }
        }
        None => {
            eprintln!(
                "WARNING: {} cannot be checked due to missing SHA1 checksum.",
                &file_name
            );
        }
    }
    Ok(())
}

fn get_target_file_name(file_name: &str, decompress: bool) -> &str {
    if decompress {
        file_name.strip_suffix(".bz2").unwrap_or(file_name)
    } else {
        file_name
    }
}

fn get_file_in_dir(directory: &Path, file_name: &str) -> PathBuf {
    let mut file = directory.to_owned();
    file.push(&file_name);
    file
}
