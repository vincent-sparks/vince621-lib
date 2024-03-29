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

#[derive(Default)]
struct StackFrameData {
    minus_slot: Option<usize>,
    tilde_slot: Option<usize>,
}

#[derive(Debug)]
pub struct TagPredicate {
    tags: Box<[Tag]>,
}

struct TagQueryParser<'a> {
    db: &'a TagDatabase,
    tags: Vec<Tag>,
    nested_query: NestedQueryParser<StackFrameData>,
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
                let mut token = take_till(1.., |c: char| c.is_whitespace()||c=='}').parse_next(input)?;
                let mut found_any = false;
                let slot = if token.starts_with('-') {
                    token=&token[1..];
                    let bucket = self.nested_query.buckets[self.nested_query.get_current_bucket(0)];
                    if bucket.min.is_none() && bucket.max.is_none() {
                        if let Some(slot) = self.nested_query.get_data().minus_slot {
                            slot
                        } else {
                            let new_slot = self.nested_query.new_ancillary_bucket(Some(0), Some(0));
                            self.nested_query.get_data().minus_slot = Some(new_slot);
                            new_slot
                        }
                    } else {
                        self.nested_query.new_ancillary_bucket(Some(0),Some(0))
                    }
                } else if token.starts_with('~') || (self.nested_query.get_current_bucket(0) == 0 && token.contains('*')) {
                    // explanation of the line above:
                    // e621.net has some very simple query parsing rules.  a post matches a query
                    // if it has all of the tags in the query with no prefix, none of the tags in
                    // the query prefixed with -, and one or more of the tags prefixed with ~.
                    // if we want to support parsing all valid e621 queries and have them behave
                    // identically, we must support this.  i've opted to do this by having each
                    // grouping in the parser maintain multiple buckets, one for the all and one
                    // for the one or more.  in this way, the query 2{ a ~b c ~d ~e } will behave
                    // identically to 2{ a c 1-{ b d e } }.  of the possible things such a
                    // nonsensical query could do, this behavior seems to make the most intuitive
                    // sense.
                    //
                    // we must also support asterisks.  queries on e621 may contain asterisks which
                    // function as wildcards.  if a token in the query referring to a tag contains
                    // an asterisk, the query will match any post that contains one or more of the
                    // tags matched by the wildcard.  for maximum flexibility, i would like to
                    // support matching an arbitrary number of tags matched by the wildcard, so
                    // 2-{a*b} would match any post with at least two of the tags matched by the
                    // wildcard expression a*b (tags that begin with an a and end with a b).
                    // however, this does not match the e621 behavior.  to properly parse user
                    // blacklists using this code, which client code will almost certainly want to
                    // do, we MUST match the e621 behavior exactly.
                    //
                    // i have decided to split the difference.  tokens matched by a wildcard go
                    // into the "any" bucket if and only if they are in the outermost grouping
                    // (i.e. if the currently targeted bucket in ancillary slot 0 (the default
                    // slot) is bucket 0 (the root bucket).
                    // since e621 does not support grouping at all, this will match the e621
                    // behavior, while still allowing me to have that flexibility.  with some
                    // slightly nonintuitive parsing rules i can have my cake and eat it too.

                    if token.starts_with('~') {
                        token=&token[1..];
                    }
                    
                    // side note: unfortunately i cannot use get_or_insert_with() here due to the
                    // borrow checker being dumb.
                    match self.nested_query.get_data().tilde_slot {
                        Some(x) => x,
                        None => {
                            let new_slot = self.nested_query.new_ancillary_bucket(Some(1), None);
                            self.nested_query.get_data().tilde_slot = Some(new_slot);
                            new_slot
                        }
                    }
                } else {
                    0
                };
                for tag in self.db.search_wildcard(token) {
                    found_any = true;
                    self.tags.push(Tag {tag_id: tag.id, bucket: self.nested_query.get_current_bucket(slot)});
                    self.nested_query.increment_count(slot, 1);
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
                    if cur_query_tag.tag_id <= cur_post_tag {
                        if cur_query_tag.tag_id == cur_post_tag {
                            buckets[cur_query_tag.bucket] += 1;
                        } 
                        cur_query_tag_opt = query_tags.next();
                    } else {
                        cur_post_tag_opt = post_tags.next();
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
    use crate::db::tags::Tag;

    use super::*;
    #[test]
    fn test_tag_query() {
        let tag_db = TagDatabase::new([
            Tag::new_debug("a", 1),
            Tag::new_debug("aa", 2),
            Tag::new_debug("ab", 3),
            Tag::new_debug("ac", 4),
            Tag::new_debug("abc", 5),
            Tag::new_debug("d", 6),
        ].into());
        let query = parse_query(&tag_db, "1-{a*}d").unwrap();
        dbg!(&query);
        assert!(query.validate(&Post {
            tags: Box::from([2,3,4,6]),
            ..Post::debug_default()
        }));
    }

    #[test]
    fn test_same_tag() {
        let db = TagDatabase::new(vec![
            Tag::new_debug("a", 1),
        ].into());
        let validator = parse_query(&db, "a 0{ a }").unwrap();
        let (buckets, validator) = validator.into_inner();
        let mut a = [0u8;2];
        let post = Post {
            tags: Box::from([1]),
            ..Post::debug_default()
        };
        validator.validate(&post, &mut a);
        assert_eq!(a, [1,1]);
        let validator = NestedQuery::new(buckets, validator);
        assert!(!validator.validate(&post));
    }
}
