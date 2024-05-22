use super::{Date,LoadWhat};
use std::{collections::HashMap, str::FromStr};
use thiserror::Error;

#[derive(Error,Debug)]
pub enum BadDate {
    #[error("date must be 10 characters")]
    WrongLength,
    #[error("bad integer")]
    BadInteger(#[from] std::num::ParseIntError),
}

impl FromStr for Date {
    type Err=BadDate;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 10 {
            return Err(BadDate::WrongLength);
        }
        Ok(Self {
            year: s[0..4].parse()?,
            month: s[5..7].parse()?,
            day: s[8..10].parse()?,

        })
    }
}

#[derive(Debug,PartialEq,Eq)]
pub struct DateInfo {
    pub date: Date,
    pub post_db_size: u64,
    pub tag_db_size: u64,
    pub tag_implication_size: u64,
    pub tag_alias_size: u64,
    pub wiki_page_size: u64,
    pub pool_db_size: u64,
}

impl DateInfo {
    fn from(date: Date, data: [u64;6])->Self {
        Self {
            date,
            tag_db_size: data[LoadWhat::Tags as usize],
            post_db_size: data[LoadWhat::Posts as usize],
            pool_db_size: data[LoadWhat::Pools as usize],
            tag_alias_size: data[LoadWhat::TagAliases as usize],
            tag_implication_size: data[LoadWhat::TagImplications as usize],
            wiki_page_size: data[LoadWhat::WikiPages as usize],
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BadHTML {
    #[error("Could not find start of list")]
    CouldNotFindStartOfList,
    #[error("Missing prefix on line {0}")]
    MissingPrefix(String),
    #[error("MissingHyphenAfterFilename")]
    MisisngHyphenAfterFilename,
    #[error("could not find space at end")]
    CouldNotFindSpaceAtEnd,
    #[error("could not parse file size: {0}")]
    CouldNotParseFileSize(String),
    #[error("bad date {0}")]
    BadDate(String),
}


// using winnow for this turned out to be more trouble than it's worth.
fn parse_html_row(mut input: &str) -> Result<Option<(Date, LoadWhat, u64)>, BadHTML> {
    input = input.strip_prefix("<a href=\"").ok_or_else(|| BadHTML::MissingPrefix(input.into()))?;
    let (which, input) = input.split_once('-').ok_or(BadHTML::MisisngHyphenAfterFilename)?;

    let which: LoadWhat = match which.parse() {
        Ok(x) => x,
        Err(_) => {
            // although i find it unlikely, e621 may add more download options in the future.
            // if this happens, we want to skip them rather than fail, opting not to inform our
            // caller about CSV files we don't know how to parse.
            return Ok(None);
        }
    };
    
    let s = input;

    let date: Date = s[..10].parse().map_err(|_| BadHTML::BadDate(s[..10].to_owned()))?;

    let last_space_position = s.rfind(' ').ok_or(BadHTML::CouldNotFindSpaceAtEnd)?;

    let s = s[last_space_position..].trim();

    let size = s.parse().map_err(|_| BadHTML::CouldNotParseFileSize(s.into()))?;

    Ok(Some((date, which, size)))
}

pub fn parse_html(html: &str) -> Result<Vec<DateInfo>, BadHTML> {
    let mut output = Vec::new();
    let pos = html.find("\n<a ").ok_or(BadHTML::CouldNotFindStartOfList)?+1;
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
        if let Some((date, which, size)) = parse_html_row(&mut l2)? { 
            // poor man's for/else
            // hate me all you want.  i am a pythonista at heart.
            'a: {
                for (k,v) in output.iter_mut() {
                    if *k==date {
                        v[which as usize]=size;
                        break 'a;
                    }
                }
                // else
                let mut v = [0u64;6];
                v[which as usize]=size;
                output.push((date, v));
            }
        }
    }

    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_html_parser() {
        let res = parse_html(r#"
<html>
<head><title>Index of /db_export/</title></head>
<body>
<h1>Index of /db_export/</h1><hr><pre><a href="../">../</a>
<a href="pools-2024-05-13.csv.gz">pools-2024-05-13.csv.gz</a>                            16-May-2024 07:44             4421349
<a href="pools-2024-05-14.csv.gz">pools-2024-05-14.csv.gz</a>                            16-May-2024 07:44             4423777
<a href="pools-2024-05-15.csv.gz">pools-2024-05-15.csv.gz</a>                            16-May-2024 07:44             4425419
<a href="pools-2024-05-16.csv.gz">pools-2024-05-16.csv.gz</a>                            16-May-2024 07:44             4427775
<a href="posts-2024-05-13.csv.gz">posts-2024-05-13.csv.gz</a>                            16-May-2024 07:44          1254849058
<a href="posts-2024-05-14.csv.gz">posts-2024-05-14.csv.gz</a>                            16-May-2024 07:45          1255514930
<a href="posts-2024-05-15.csv.gz">posts-2024-05-15.csv.gz</a>                            16-May-2024 07:45          1256193112
<a href="posts-2024-05-16.csv.gz">posts-2024-05-16.csv.gz</a>                            16-May-2024 07:45          1256621585
<a href="tag_aliases-2024-05-13.csv.gz">tag_aliases-2024-05-13.csv.gz</a>                      16-May-2024 07:45             1169745
<a href="tag_aliases-2024-05-14.csv.gz">tag_aliases-2024-05-14.csv.gz</a>                      16-May-2024 07:45             1170143
<a href="tag_aliases-2024-05-15.csv.gz">tag_aliases-2024-05-15.csv.gz</a>                      16-May-2024 07:45             1170668
<a href="tag_aliases-2024-05-16.csv.gz">tag_aliases-2024-05-16.csv.gz</a>                      16-May-2024 07:45             1170928
<a href="tag_implications-2024-05-13.csv.gz">tag_implications-2024-05-13.csv.gz</a>                 16-May-2024 07:45              944925
<a href="tag_implications-2024-05-14.csv.gz">tag_implications-2024-05-14.csv.gz</a>                 16-May-2024 07:45              944988
<a href="tag_implications-2024-05-15.csv.gz">tag_implications-2024-05-15.csv.gz</a>                 16-May-2024 07:45              946352
<a href="tag_implications-2024-05-16.csv.gz">tag_implications-2024-05-16.csv.gz</a>                 16-May-2024 07:45              946433
<a href="tags-2024-05-13.csv.gz">tags-2024-05-13.csv.gz</a>                             16-May-2024 07:45            13360937
<a href="tags-2024-05-14.csv.gz">tags-2024-05-14.csv.gz</a>                             16-May-2024 07:45            13367522
<a href="tags-2024-05-15.csv.gz">tags-2024-05-15.csv.gz</a>                             16-May-2024 07:45            13372687
<a href="tags-2024-05-16.csv.gz">tags-2024-05-16.csv.gz</a>                             16-May-2024 07:45            13376850
<a href="wiki_pages-2024-05-13.csv.gz">wiki_pages-2024-05-13.csv.gz</a>                       16-May-2024 07:45            11174249
<a href="wiki_pages-2024-05-14.csv.gz">wiki_pages-2024-05-14.csv.gz</a>                       16-May-2024 07:45            11180404
<a href="wiki_pages-2024-05-15.csv.gz">wiki_pages-2024-05-15.csv.gz</a>                       16-May-2024 07:45            11184731
<a href="wiki_pages-2024-05-16.csv.gz">wiki_pages-2024-05-16.csv.gz</a>                       16-May-2024 07:45            11193931
</pre><hr></body>
</html>"#);
        assert_eq!(res.unwrap(), vec![
            DateInfo {
                date: Date{year:2024,month:05,day:13},
                post_db_size: 1254849058,
                pool_db_size: 4421349,
                tag_db_size: 13360937,
                tag_alias_size: 1169745,
                tag_implication_size: 944925,
                wiki_page_size: 11174249,
            },
            DateInfo {
                date: Date{year:2024,month:05,day:14},
                post_db_size: 1255514930,
                pool_db_size: 4423777,
                tag_db_size: 13367522,
                tag_alias_size: 1170143,
                tag_implication_size: 944988,
                wiki_page_size: 11180404,
            },
            DateInfo {
                date: Date{year:2024,month:05,day:15},
                post_db_size: 1256193112,
                pool_db_size: 4425419,
                tag_db_size: 13372687,
                tag_alias_size: 1170668,
                tag_implication_size: 946352,
                wiki_page_size: 11184731,
            },
            DateInfo {
                date: Date{year:2024,month:05,day:16},
                post_db_size: 1256621585,
                pool_db_size: 4427775,
                tag_db_size: 13376850,
                tag_alias_size: 1170928,
                tag_implication_size: 946433,
                wiki_page_size: 11193931,
            },

        ]);
    }
}
