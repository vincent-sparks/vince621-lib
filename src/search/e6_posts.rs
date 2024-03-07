use std::cell::RefCell;
use std::num::NonZeroU8;

use winnow::ascii::space0;
use winnow::combinator::{repeat, alt, seq};
use winnow::error::{ContextError, StrContext, TreeError};
use winnow::error::ErrMode;
use winnow::error::ErrorKind;
use winnow::error::FromExternalError;
use winnow::stream::Stream as _;
use winnow::token::take_till;
use winnow::error::ParserError;
use winnow::Parser;
use winnow::prelude::*;

use crate::db::tags::TagDatabase;

use super::{NestedQueryParser, Predicate};
use super::NestedQuery;

#[derive(Debug)]
struct Tag {
    tag_id: u32,
    bucket: usize,
}

#[derive(Debug)]
pub struct TagPredicate {
    tags: Box<[Tag]>,
}

struct TagQueryParser<'a> {
    db: &'a TagDatabase,
    tags: Vec<Tag>,
    nested_query: NestedQueryParser,
}
impl<'a> TagQueryParser<'a> {
    fn new(db: &'a TagDatabase) -> Self {
        Self {
            db,
            tags: Vec::new(),
            nested_query: NestedQueryParser::new(),
        }
    }

    fn finalize(mut self) -> Result<NestedQuery<TagPredicate>, super::ParseError> {
        let buckets = self.nested_query.finalize()?;
        self.tags.sort_unstable_by_key(|tag| tag.tag_id);
        Ok(NestedQuery::new(buckets, TagPredicate{tags: self.tags.into_boxed_slice()}))
    }
}

#[derive(Debug)]
pub struct UnknownTag(String);

impl std::fmt::Display for UnknownTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the tag \"")?;
        f.write_str(self.0.as_str())?;
        f.write_str("\" did not match any tag in the e621 database")
    }
}

impl std::error::Error for UnknownTag {}

impl<'a, 'db, E: ParserError<&'a str> + FromExternalError<&'a str, UnknownTag> + FromExternalError<&'a str, super::ParseError>> Parser<&'a str, (), E> for TagQueryParser<'db> {
    fn parse_next(&mut self, input: &mut &'a str) -> PResult<(), E> {
        let checkpoint = input.checkpoint();
        match self.nested_query.parse_next(input) {
            Ok(_bucket) => Ok(()),
            Err(ErrMode::Incomplete(_)) => panic!("NestedQueryParser should not operate in partial mode and never return Incomplete"),
            Err(ErrMode::Cut(e)) => Err(ErrMode::Cut(e)),
            Err(ErrMode::Backtrack(_)) => {
                input.reset(&checkpoint);
                let token = take_till(1.., |c: char| c.is_whitespace()||c=='}').parse_next(input)?;
                let mut found_any = false;
                let bucket = if token.starts_with('-') {
                    0
                } else if token.starts_with('~') {
                    0
                } else {
                    self.nested_query.get_current_bucket()
                };
                for tag in self.db.search_wildcard(token) {
                    found_any = true;
                    self.tags.push(Tag {tag_id: tag.id, bucket});
                    self.nested_query.increment_count(1);
                }
                if found_any {
                    Ok(())
                } else {
                    // the caller should reset to some suitable point for us, but we need to point to the
                    // error location for better error reporting.
                    input.reset(&checkpoint);
                    Err(ErrMode::Backtrack(E::from_external_error(input, ErrorKind::Slice, UnknownTag(token.to_owned()))))
                }
            }
        }
    }
}

impl Predicate for TagPredicate {
    type Post = crate::db::posts::Post;

    fn validate(&self, post: &Self::Post, buckets: &mut [u8]) {
        let mut post_tags = post.tags.iter().copied();
        let mut query_tags = self.tags.iter();
        let mut cur_query_tag_opt = query_tags.next();
        let mut cur_post_tag_opt = post_tags.next();
        loop {
            match (cur_query_tag_opt, cur_post_tag_opt) {
                (Some(cur_query_tag), Some(cur_post_tag)) => {
                    if cur_query_tag.tag_id >= cur_post_tag {
                        if cur_query_tag.tag_id == cur_post_tag {
                            buckets[cur_query_tag.bucket] += 1;
                        }
                        cur_post_tag_opt = post_tags.next();
                    } else {
                        cur_query_tag_opt = query_tags.next();
                    }
                },
                _ => break,
            }
        }
    }
}

pub fn parse_query<'a>(db: &TagDatabase, query: &'a str) -> Result<NestedQuery<TagPredicate>, TreeError<&'a str>> {
    let mut tag_parser = TagQueryParser::new(db);
    let tag_parser_ref = &mut tag_parser;
    repeat(1..,
           seq!(
               |input: &mut &'a str| tag_parser_ref.parse_next(input),
               space0,
               )
      ).parse(query).map_err(|e| e.into_inner())?;
    tag_parser.finalize().map_err(|e| TreeError::from_external_error(&"", ErrorKind::Eof, e))
}

#[cfg(test)]
mod test {
    use crate::db::posts::Post;

    use super::*;
    #[test]
    fn test_tag_query_validator() {
        use crate::db::tags::Tag;
        let tag_db = TagDatabase::new(vec![
            Tag::new_debug("a", 1),
            Tag::new_debug("aa", 2),
            Tag::new_debug("ab", 3),
            Tag::new_debug("ac", 4),
            Tag::new_debug("abc", 5),
            Tag::new_debug("d", 6),
        ]);
        let query = parse_query(&tag_db, "1-{a*}d").unwrap();
        dbg!(&query);
        assert!(query.validate(&Post {
            tags: Box::from([2,3,4,6]),
            ..Post::debug_default()
        }));
    }
}
