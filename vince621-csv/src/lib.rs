pub mod tags;
pub mod posts;
pub mod pools;
mod util;

use std::io::{Read,BufRead};

use vince621_core::db::{pools::PoolDatabase, posts::PostDatabase, tags::TagDatabase};

pub struct E6Database {
    pub tag: TagDatabase,
    pub post: PostDatabase,
    pub pool: PoolDatabase,
}

/// The first argument to the download callback is a URL, which is guaranteed to begin with
/// "https://e621.net/".  The second argument is the MIME type that URL is expected to return.
///
/// The implementation of the callback is deliberately left up to the caller.  It may return a file
/// from disk, an HTTP stream from the reqwest crate, or something more exotic.  For use from
/// asynchronous applications, the `fifo_bufread` crate (which was developed specifically for this
/// purpose) may come in handy.  The only requirements are these:
///
/// The first argument provided is a URL.  The second is the expected Content-Type header.  If the
/// HTTP request to the given URL (performed either at the time of the callback or at some point in
/// the past if returning a file from a disk cache) returns a status code different from 200, or a
/// Content-Type header different from the expected value provided, either the callback itself or the
/// first call to `read()` should return an `Err` value.  Otherwise, it should return something
/// from which bytes can be read synchronously.  Streaming decompression is handled by the caller
/// -- the callback need only return bytes exactly as they appear from the HTTP stream.
///
/// The URLs passed to the method are guaranteed to begin with "https://e621.net/db_export/".  The
/// first call to the callback will be this string exactly, which is expected to return an HTML
/// page from which the date of the most recent DB export is parsed.  To avoid this first call, you
/// may call [load_date] instead.
///
pub fn load<R: BufRead>(download_callback: impl Fn(String, &'static str) -> std::io::Result<R> + Sync) -> csv::Result<E6Database> {
    let mut response = Vec::new();
    download_callback("https://e621.net/db_export/".into(), "text/html")?.read_to_end(&mut response)?;
    let mut data = std::str::from_utf8(response.as_slice()).map_err(std::io::Error::other)?;
    let mut date = None;

    while let Some((_, end)) = data.split_once("href=\"posts-") {
        let (this_date, rest_of_string) = end.split_once('.').ok_or_else(|| std::io::Error::other("unterminated string in HTML"))?;
        if !date.is_some_and(|date| this_date<date) {
            date = Some(this_date);
        }
        data = rest_of_string;
    }

    let date = date.ok_or_else(|| std::io::Error::other("HTML did not contain any posts- links"))?;
    dbg!(&date);

    load_date(date, download_callback)
}

/// Load the database as it appeared on the specified date, which must be YYYY-MM-DD format.  If the
/// download_callback performs an actual HTTP request, this date may be up to two days in the past.
pub fn load_date<R: BufRead>(date: &str, download_callback: impl Fn(String, &'static str) -> std::io::Result<R> + Sync) -> csv::Result<E6Database> {
    let (tag_and_post, pool_db) = rayon::join( || {
    let tag_csvgz = download_callback(format!("https://e621.net/db_export/tags-{}.csv.gz", date), "application/octet-stream")?;

    let tag_db = tags::load_tag_database(csv::Reader::from_reader(flate2::bufread::GzDecoder::new(tag_csvgz)))?;

    let post_csvgz = download_callback(format!("https://e621.net/db_export/posts-{}.csv.gz", date), "application/octet-stream")?;

    let post_db = posts::load_post_database(&tag_db, csv::Reader::from_reader(flate2::bufread::GzDecoder::new(post_csvgz)))?;
    std::io::Result::Ok((tag_db, post_db))
    },
    || {
    let pool_csvgz = download_callback(format!("https://e621.net/db_export/pools-{}.csv.gz", date), "application/octet-stream")?;

    let pool_db = pools::load_pool_database(csv::Reader::from_reader(flate2::bufread::GzDecoder::new(pool_csvgz)))?;

    std::io::Result::Ok(pool_db)
    }
    );

    let (tag_db, post_db) = tag_and_post?;
    let pool_db = pool_db?;
    
    Ok(E6Database {
        tag: tag_db,
        post: post_db,
        pool: pool_db,
    })
}
