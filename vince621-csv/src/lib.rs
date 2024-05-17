pub mod tags;
pub mod posts;
pub mod pools;
mod util;

use std::{collections::HashMap, io::{BufRead, Read}};

use vince621_core::db::{pools::PoolDatabase, posts::PostDatabase, tags::TagDatabase};
use winnow::{error::FromExternalError as _, token::{take_till, take_until}, Parser as _};

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

#[derive(Eq,PartialEq,Clone,Copy,Debug,strum::EnumString,strum::IntoStaticStr)]
#[strum(serialize_all="snake_case")]
#[repr(u8)]
pub enum LoadWhat {
    Posts, Tags, Pools, TagImplications, TagAliases, WikiPages,
}

pub trait Loader {
    fn load(self, what: LoadWhat) -> std::io::Result<impl std::io::BufRead>;
}

pub struct AlreadyLoaded<T>(pub T);

impl<T> Loader for AlreadyLoaded<T> where T: std::io::BufRead {
    fn load(self, _: LoadWhat) -> std::io::Result<impl std::io::BufRead> {
        Ok(self.0)
    }
}

pub trait UrlLoader {
    fn load(&self, url: String) -> std::io::Result<impl std::io::BufRead>;
}

pub struct DateInfo {
    pub date: Date,
    pub post_db_size: u64,
    pub tag_db_size: u64,
    pub tag_implication_size: u64,
    pub tag_alias_size: u64,
    pub wiki_page_size: u64,
}

impl DateInfo {
    fn from(date: Date, data: [u64;5])->Self {
        Self {
            date,
            tag_db_size: data[LoadWhat::Tags as usize],
            post_db_size: data[LoadWhat::Posts as usize],
            tag_alias_size: data[LoadWhat::TagAliases as usize],
            tag_implication_size: data[LoadWhat::TagImplications as usize],
            wiki_page_size: data[LoadWhat::WikiPages as usize],
        }
    }
}

#[derive(Hash,PartialEq,Eq)]
pub struct Date {
    pub year: u16,
    pub month: u8,
    pub day: u8,
}

pub struct DateLoader<T>(pub Date, pub T);

impl<T> Loader for &DateLoader<T> where T: UrlLoader {
    fn load(self, what: LoadWhat) -> std::io::Result<impl std::io::BufRead> {
        self.1.load(format!("https://e621.net/db_export/{}-{:04}-{:02}-{:02}.csv.gz", <&'static str>::from(what), self.0.year, self.0.month, self.0.day))
    }
}

pub struct BadHTML(pub &'static str);


fn parse_html_row(input: &mut &str) -> winnow::PResult<Option<(Date, LoadWhat, u64)>> {
    "<a href=\"".parse_next(input)?;
    let s = take_till(1.., '-').parse_next(input)?;

    let which: LoadWhat = match s.parse() {
        Ok(x) => x,
        Err(_) => {
            // although i find it unlikely, e621 may add more download options in the future.
            // if this happens, we want to skip them rather than fail, opting not to inform our
            // caller about CSV files we don't know how to parse.
            return Ok(None);
        }
    };
    
    let s = &s[1..];

    let year:u16 = take_till(4..=4,'-').parse_to().parse_next(input)?;
    let s = &s[1..];
    let month:u8 = take_till(2..=2,'-').parse_to().parse_next(input)?;
    let s = &s[1..];
    let day:u8 = take_till(2..=2,'.').parse_to().parse_next(input)?;
    let s = &s[1..];
    
    let date = Date{year,month,day};

    // winnow is really not built to deal with parsing stuff at the *end* of a string so we have to do this terribleness to convince it to let us use regular str operators.
    // was using winnow for this a mistake? quite possibly.
    let last_space_position = s.rfind(' ').ok_or(winnow::error::ErrMode::Backtrack(winnow::error::ContextError::new()))?;

    let s = s[last_space_position..].trim();

    let size = s.parse().map_err(|e| winnow::error::ErrMode::Backtrack(winnow::error::ContextError::from_external_error(&s,winnow::error::ErrorKind::Slice,e)))?;

    Ok(Some((date, which, size)))
}

pub fn parse_html(html: &str) -> Result<Vec<DateInfo>, BadHTML> {
    let mut output = HashMap::new();
    let pos = html.find("\n<a ").ok_or(BadHTML("could not find start of list"))?+1;
    for line in html[pos..].lines() {
        if !line.starts_with("<a") {
            if line.starts_with("</pre>") {
                return Ok(
                    output.into_iter()
                    .map(|(k,v)| DateInfo::from(k,v))
                    .collect()
                );
            }
        }
        let mut l2 = line;
        if let Some((date, which, size)) = parse_html_row(&mut l2).map_err(|_| BadHTML(""))? { 
            output.entry(date).or_default()[which as usize]=size;
        }
    }

    todo!()
}

pub fn load_tag_database(loader: impl Loader) -> csv::Result<TagDatabase> {
    tags::load_tag_database(csv::Reader::from_reader(flate2::bufread::GzDecoder::new(loader.load(LoadWhat::Tags)?)))
}

pub fn load_post_database(tag_db: &TagDatabase, loader: impl Loader) -> csv::Result<PostDatabase> {
    posts::load_post_database(tag_db, csv::Reader::from_reader(flate2::bufread::GzDecoder::new(loader.load(LoadWhat::Posts)?)))
}

/*
pub fn load_tag_alias_database(loader: impl Loader) -> csv::Result<> {
}
*/
