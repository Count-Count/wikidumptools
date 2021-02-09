// dump update spike
//
// (C) 2020 Count Count
//
// Distributed under the terms of the MIT license.

use std::collections::BTreeMap;
use std::io::Write;

use mediawiki::media_wiki_error::MediaWikiError;

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Received invalid JSON data from Mediawiki")]
    InvalidJsonFromMediawiki(),
    #[error("Mediawiki API error: {0}")]
    MediawikiError(#[from] MediaWikiError),
}

type Result<T> = std::result::Result<T, Error>;

struct WikiCredentials<'a> {
    username: &'a str,
    password: &'a str,
}

async fn update(credentials: Option<WikiCredentials<'_>>) -> Result<()> {
    let mut api = mediawiki::api::Api::new("https://en.wikipedia.org/w/api.php").await?;
    if let Some(credentials) = credentials {
        api.login(credentials.username, credentials.password).await?;
    }
    let params = api.params_into(&[("action", "query"), ("meta", "userinfo"), ("uiprop", "rights")]);
    let res = api.get_query_api_json_all(&params).await?;
    let apihighlimits = res["query"]["userinfo"]["rights"]
        .as_array()
        .ok_or(Error::InvalidJsonFromMediawiki())?
        .iter()
        .any(|val| val.as_str() == Some("apihighlimits"));
    let rc_per_batch = if apihighlimits { 5000 } else { 500 };
    let revs_per_batch = if apihighlimits { 500 } else { 50 };

    let params = api.params_into(&[
        ("action", "query"),
        ("list", "recentchanges"),
        ("rcend", "2020-10-26T08:35:48Z"),
        ("rclimit", rc_per_batch.to_string().as_str()),
        ("rcprop", "ids|loginfo"),
        ("rctype", "new|edit|log"),
    ]);
    let res = api.get_query_api_json_all(&params).await?;
    let mut page_to_last_revision = BTreeMap::new();
    let mut rev_count = 0;

    // capture: moved pages, deleted pages, restored pages specially
    for val in res["query"]["recentchanges"]
        .as_array()
        .ok_or(Error::InvalidJsonFromMediawiki())?
    {
        match val["type"].as_str() {
            Some("new") | Some("edit") => {
                let pageid = val["pageid"].as_u64().ok_or(Error::InvalidJsonFromMediawiki())?;
                let revid = val["revid"].as_u64().ok_or(Error::InvalidJsonFromMediawiki())?;
                page_to_last_revision.entry(pageid).or_insert(revid);
                rev_count += 1;
            }
            Some(x) => println!("type: {}: {}", x, val),
            None => {}
        }
    }
    drop(res);

    eprintln!(
        "Most recent revs: {}, total revs: {}",
        page_to_last_revision.len(),
        rev_count
    );

    let mut count = 0_u64;
    let mut total_count = 0;
    let mut total_bytes = 0_usize;
    let mut revs: String = String::new();
    for (page, last_revision) in &page_to_last_revision {
        let _ = page; // will be used in the future
        revs.push_str(last_revision.to_string().as_str());
        count += 1;
        if count == revs_per_batch {
            total_count += count;
            count = 0;
            let params = api.params_into(&[
                ("action", "query"),
                ("prop", "revisions"),
                ("rvprop", "ids|flags|timestamp|user|userid|content|comment|tags"),
                ("revids", revs.as_str()),
            ]);
            let res = api.get_query_api_json_all(&params).await?;
            total_bytes += res.to_string().len();
            revs.clear();
            eprint!(
                "\r{} of {} revisions downloaded ({} MiB) ",
                total_count,
                page_to_last_revision.len(),
                total_bytes as f64 / 1024.0 / 1024.0
            );
            std::io::stderr().flush().unwrap();
        } else {
            revs.push('|');
        }
    }
    println!("Total: {}", total_bytes as f64 / 1024.0 / 1024.0);

    Ok(())
}

#[tokio::main]
async fn main() {
    update(None).await.unwrap();
}
