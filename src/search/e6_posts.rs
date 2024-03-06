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
    current_bucket: usize, // I really don't like storing this value in multiple places, but I
                           // don't see how else to do it.
}
impl<'a> TagQueryParser<'a> {
    fn new(db: &'a TagDatabase) -> Self {
        Self {
            db,
            tags: Vec::new(),
            current_bucket: 0,
        }
    }

    fn set_current_bucket(&mut self, bucket: usize) {
        self.current_bucket = bucket;
    }

    fn finalize(mut self) -> TagPredicate {
        self.tags.sort_unstable_by_key(|tag| tag.tag_id);
        TagPredicate{tags: self.tags.into_boxed_slice()}
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

impl<'a, 'db, E: ParserError<&'a str> + FromExternalError<&'a str, UnknownTag>> Parser<&'a str, NonZeroU8, E> for TagQueryParser<'db> {
    fn parse_next(&mut self, input: &mut &'a str) -> PResult<NonZeroU8, E> {
        let checkpoint = input.checkpoint();
        let token = take_till(1.., |c: char| c.is_whitespace()||c=='}').parse_next(input)?;
        let mut count = 0u8;
        for tag in self.db.search_wildcard(token) {
            count += 1;
            self.tags.push(Tag {tag_id: tag.id, bucket: self.current_bucket});
        }
        NonZeroU8::new(count).ok_or_else(|| {
            input.reset(&checkpoint);
            ErrMode::Backtrack(E::from_external_error(input, ErrorKind::Slice, UnknownTag(token.to_owned())))
        })
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

pub fn parse_query<'a>(db: &TagDatabase, mut query: &'a str) -> Result<NestedQuery<TagPredicate>, TreeError<&'a str>> {
    let nested_query = RefCell::new(NestedQueryParser::new());
    let tag_parser = RefCell::new(TagQueryParser::new(db));

    let nested_query_ref = &nested_query;
    let tag_parser_ref = &tag_parser;
    repeat(1..,
           seq!(
               alt((
                       (|query: &mut &'a str| nested_query_ref.borrow_mut().parse_next(query).map(|bucket| tag_parser_ref.borrow_mut().set_current_bucket(bucket))).context(StrContext::Label("brackets")),
                       (|query: &mut &'a str| tag_parser_ref.borrow_mut().parse_next(query).map(|count| nested_query_ref.borrow_mut().increment_count(count.get()))).context(StrContext::Label("tag")),
               )),
               space0,
               )
      ).parse(query).map_err(|e| e.into_inner())?;
    let buckets = nested_query.into_inner().finalize().map_err(|e| TreeError::from_external_error(&"", ErrorKind::Eof, e))?;
    let tag_query = tag_parser.into_inner().finalize();
    Ok(NestedQuery::new(buckets, tag_query))
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
