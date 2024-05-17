use super::{Date,LoadWhat};
use winnow::{error::FromExternalError as _, token::take_till, Parser as _};
use std::collections::HashMap;

#[derive(Debug)]
pub struct DateInfo {
    pub date: Date,
    pub post_db_size: u64,
    pub tag_db_size: u64,
    pub tag_implication_size: u64,
    pub tag_alias_size: u64,
    pub wiki_page_size: u64,
}

impl DateInfo {
    fn from(date: Date, data: [u64;6])->Self {
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

#[derive(Debug)]
pub struct BadHTML(pub String);

impl std::error::Error for BadHTML{}
impl std::fmt::Display for BadHTML {
    fn fmt(&self,fmt:&mut std::fmt::Formatter)->std::fmt::Result{fmt.write_str(&self.0)}
}

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
    let pos = html.find("\n<a ").ok_or(BadHTML("could not find start of list".into()))?+1;
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
        if let Some((date, which, size)) = parse_html_row(&mut l2).map_err(|e| BadHTML(e.to_string()))? { 
            output.entry(date).or_default()[which as usize]=size;
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
        dbg!(res);
        panic!();
    }
}
