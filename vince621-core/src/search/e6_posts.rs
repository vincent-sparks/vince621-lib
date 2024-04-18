use std::str::FromStr;

use winnow::ascii::space0;
use winnow::combinator::{alt, cut_err, eof, repeat, repeat_till, seq};
use winnow::error::{AddContext, ContextError, StrContext, TreeError};
use winnow::error::ErrMode;
use winnow::error::ErrorKind;
use winnow::error::FromExternalError;
use winnow::stream::Stream as _;
use winnow::token::take_till;
use winnow::error::ParserError;
use winnow::Parser;
use winnow::prelude::*;

use crate::db::posts::{self, Post, Rating};
use crate::db::tags::TagDatabase;

use super::{NestedQueryParser, Kernel};
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
pub struct PostKernel {
    tags: Box<[Tag]>,
    ratings: [Vec<usize>; 3],
}

pub(crate) struct TagQueryParser<'a> {
    db: &'a TagDatabase,
    tags: Vec<Tag>,
    ratings: [Vec<usize>; 3],
    nested_query: NestedQueryParser<StackFrameData>,
}
impl<'a> TagQueryParser<'a> {
    pub fn new(db: &'a TagDatabase) -> Self {
        Self {
            db,
            tags: Vec::new(),
            nested_query: NestedQueryParser::new(),
            ratings: [Vec::new(), Vec::new(), Vec::new()],
        }
    }

    pub fn finalize(mut self) -> Result<NestedQuery<PostKernel>, super::ParseError> {
        let buckets = self.nested_query.finalize()?;
        self.tags.sort_unstable_by_key(|tag| tag.tag_id);
        Ok(NestedQuery::new(buckets, PostKernel{tags: self.tags.into_boxed_slice(), ratings: self.ratings}))
    }
}

#[derive(Debug)]
pub enum BadTag {
    UnknownTag(String),
    BadRating,
}

#[derive(Debug,PartialEq)]
pub enum SortOrder {
    Date,
    DateAscending,
    Score,
    ScoreAscending,
    FavCount,
    FavCountAscending,
    Random,
}

impl SortOrder {
    pub fn sort(&self, results: &mut [posts::Post]) {
        match self {
            Self::DateAscending => {
                // list is already sorted by date ascending -- we're done here.
            },
            Self::Date => {
                results.reverse();
            },
            Self::Random => {
                todo!()
            },
            other => {
                results.sort_unstable_by(match other {
                    Self::ScoreAscending => |a:&Post,b:&Post| a.score.cmp(&b.score),
                    Self::Score => |a:&Post,b:&Post| b.score.cmp(&a.score),
                    Self::FavCount => |a:&Post,b:&Post| a.fav_count.cmp(&b.fav_count),
                    Self::FavCountAscending => |a:&Post,b:&Post| b.fav_count.cmp(&a.fav_count),
                    Self::Date | Self::DateAscending | Self::Random => unreachable!(),
                });
            }
        }
    }
}

#[derive(thiserror::Error,Debug)]
pub enum BadSortOrder {
    #[error("Unknown sort order")]
    UnknownSortOrder,
    #[error("Sort order was specified twice")]
    DuplicateSortOrder,
}

impl FromStr for SortOrder {
    type Err=BadSortOrder;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "id_desc" => Ok(Self::Date),
            "id" | "id_asc" => Ok(Self::DateAscending),
            "score" => Ok(Self::Score),
            "score_asc" => Ok(Self::ScoreAscending),
            "favcount" => Ok(Self::FavCount),
            "favcount_asc" => Ok(Self::FavCountAscending),
            "random" => Ok(Self::Random),
            _ => Err(BadSortOrder::UnknownSortOrder),
        }
    }
}

pub struct SortOrderParser(Option<SortOrder>);
impl SortOrderParser {
    fn new() -> Self {
        Self(None)
    }
    fn finalize(self) -> SortOrder {
        self.0.unwrap_or(SortOrder::Date)
    }
}

impl<'a, E> Parser<&'a str, (), E> for SortOrderParser where E: ParserError<&'a str> + FromExternalError<&'a str, BadSortOrder> {
    fn parse_next(&mut self, input: &mut &'a str) -> PResult<(), E> {
        if self.0.is_some() && input.starts_with("order:") {
            return Err(ErrMode::Cut(E::from_external_error(input, ErrorKind::Verify, BadSortOrder::DuplicateSortOrder)));
        }
        "order:".parse_next(input)?;
        let cp = input.checkpoint();
        let token = take_till(1.., |c: char| c.is_whitespace()||c=='}').parse_next(input)?;
        match token.parse::<SortOrder>() {
            Ok(o) => {
                self.0 = Some(o);
                Ok(())
            },
            Err(e) => {
                input.reset(&cp);
                Err(ErrMode::Cut(E::from_external_error(&token, ErrorKind::Verify, e)))
            }
        }
    }
}

impl std::fmt::Display for BadTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownTag(t) => {
                f.write_str("the tag \"")?;
                f.write_str(t.as_str())?;
                f.write_str("\" did not match any tag in the e621 database")
            }
            Self::BadRating => f.write_str("Rating must be \"safe\", \"questionable\", or \"explicit\" (you may abbreviate these to the first letter)")
        }
    }
}

impl std::error::Error for BadTag {}

impl<'a, 'db, E: ParserError<&'a str> + FromExternalError<&'a str, BadTag> + FromExternalError<&'a str, super::ParseError>> Parser<&'a str, (), E> for TagQueryParser<'db> {
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
                    // into an "any" bucket if and only if they are in the outermost grouping
                    // (i.e. if the currently targeted bucket in ancillary slot 0 (the default
                    // slot) is bucket 0 (the root bucket).
                    // since e621 does not support grouping at all, this will match the e621
                    // behavior, while still allowing me to have that flexibility.  with some
                    // slightly nonintuitive parsing rules i can have my cake and eat it too.

                    if token.starts_with('~') {
                        token=&token[1..];
                        // important: only reuse the "any" bucket if the token starts with a tilde!
                        // we don't want a*b c*d to go into the same bucket otherwise!

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
                        // if there is no ~ at the beginning and this is a wildcard expression, put
                        // it in its own bucket.
                        self.nested_query.new_ancillary_bucket(Some(1), None)
                    }
                    
                } else {
                    0
                };
                if token.starts_with("rating:") {
                    match token[7..].parse::<Rating>() {
                        Ok(rating) => {
                            self.ratings[rating as usize].push(self.nested_query.get_current_bucket(slot));
                            self.nested_query.increment_count(slot,1);
                            return Ok(());
                        },
                        Err(_) => {
                            input.reset(&checkpoint);
                            return Err(ErrMode::Cut(E::from_external_error(&&token[7..], ErrorKind::Slice, BadTag::BadRating))); // FIXME
                        }
                    }
                }
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
                    Err(ErrMode::Backtrack(E::from_external_error(&token, ErrorKind::Verify, BadTag::UnknownTag(token.to_owned()))))
                }
            }
        }
    }
}

impl Kernel for PostKernel {
    type Post = posts::Post;

    fn validate(&self, post: &Self::Post, buckets: &mut [u8]) {
        for item in self.ratings[post.rating as usize].iter() {
            buckets[*item] += 1;
        }

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

#[derive(Debug)]
pub struct ExternalError<'a>(&'a str, String, bool);

impl<'a, E> FromExternalError<&'a str, E> for ExternalError<'a> where E: std::fmt::Display {
    fn from_external_error(input: &&'a str, _: ErrorKind, e: E) -> Self {
        ExternalError(input, e.to_string(), true)
    }
}

impl<'a> ParserError<&'a str> for ExternalError<'a> {
    fn from_error_kind(input: &&'a str, kind: ErrorKind) -> Self {
        ExternalError(&input[..0], kind.to_string(), false)
    }

    fn append(self, input: &&'a str, token_start: &<&'a str as winnow::stream::Stream>::Checkpoint, _kind: ErrorKind) -> Self {
        // get the offending token as a &str.
        let end = *input;
        let mut input = *input;
        input.reset(token_start);
        // SAFETY: both are guaranteed to be within the bounds of the same allocated object.
        let delta = unsafe {end.as_ptr().offset_from(input.as_ptr())};
        assert!(delta>=0);
        let offending_token = &input[..delta as usize];
        Self(if offending_token.is_empty() {self.0} else {offending_token}, self.1, self.2)
    }

    fn assert(input: &&'a str, error_message: &'static str) -> Self {
        Self(&input[..0], error_message.into(), true)
    }

    fn or(self, other: Self) -> Self {
        if !self.2 {
            other
        } else {
            self
        }
    }
}

impl<'a> AddContext<&'a str, StrContext> for ExternalError<'a>{
    fn add_context(
        mut self,
        _input: &&'a str,
        _token_start: &<&'a str as winnow::stream::Stream>::Checkpoint,
        context: StrContext,
    ) -> Self {
        self.1=context.to_string();
        self.2=true;
        self
    }
}

impl<'a> ExternalError<'a> {
    pub fn get_range(&self, full_input: &'a str) -> (usize, usize) {
        let inner_range = self.0.as_bytes().as_ptr_range();
        let outer_range = full_input.as_bytes().as_ptr_range();
        assert!(inner_range.start >= outer_range.start && inner_range.start <= outer_range.end);
        assert!(inner_range.end >= outer_range.start && inner_range.end <= outer_range.end);
        // SAFETY:
        //  * full_input is a pointer to a single allocated object, with the same lifetime as our
        //  error pointer.
        //  * we have just verified that the pointer range of inner_range is a subset of
        //  outer_range, so they must belong to the same allocated object.
        unsafe {
            (inner_range.start.sub_ptr(outer_range.start), inner_range.end.sub_ptr(outer_range.start))
        }

    }
    pub fn into_reason(self)->String{
        self.1
    }
}

pub fn parse_query<'a>(db: &TagDatabase, query: &'a str) -> Result<NestedQuery<PostKernel>, ExternalError<'a>> {
    let mut tag_parser = TagQueryParser::new(db);
    let tag_parser_ref = &mut tag_parser;
    repeat(0..,
           seq!(
               |input: &mut &'a str| tag_parser_ref.parse_next(input),
               space0,
               )
      ).parse(query).map_err(|e| e.into_inner())?;
    tag_parser.finalize().map_err(|e| ExternalError::from_external_error(&&query[query.len()..], ErrorKind::Eof, e))
}

pub fn parse_query_and_sort_order<'a>(db: &TagDatabase, query: &'a str) -> Result<(NestedQuery<PostKernel>, SortOrder), ExternalError<'a>> {
    let mut tag_parser = TagQueryParser::new(db);
    let mut sort_order_parser = SortOrderParser::new();
    let tag_parser_ref = &mut tag_parser;
    let sort_order_parser_ref = &mut sort_order_parser;
    let _: ((),_) = repeat_till(0..,
           cut_err(seq!(
               alt((
                   |input: &mut &'a str| sort_order_parser_ref.parse_next(input),
                   |input: &mut &'a str| tag_parser_ref.parse_next(input),
               )),
               space0,
               )),
            eof
      ).parse(query).map_err(|e| e.into_inner())?;
    tag_parser.finalize().map_err(|e| ExternalError::from_external_error(&&query[query.len()..], ErrorKind::Eof, e)).map(|q| (q, sort_order_parser.finalize()))
}

/// Parse a partial query that the user is currently editing, and return the token the cursor is in
/// the middle of, as well as all the tokens in the same bucket as that token or an ancestor.  
pub fn parse_query_for_autocomplete<'a>(mut query: &'a str, cursor_position: usize) -> Option<(&'a str, Vec<&'a str>)> {
    let cursor_ptr = if cursor_position == query.len() {
        None
    } else {
        Some(&query.as_bytes()[cursor_position] as *const u8)
    };
    let mut nested_parser = NestedQueryParser::<Vec<&'a str>>::new();
    
    while !query.is_empty() {
        match Parser::<_,_,ErrorKind>::parse_next(&mut nested_parser, &mut query) {
            Ok(_) => {},
            Err(ErrMode::Cut(_)) => return None,
            Err(ErrMode::Backtrack(_)) => {
                let end_pos = query.find([' ','}']).unwrap_or(query.len());
                let token = &query[..end_pos];
                query = &query[end_pos..];
                let did_we_find_it = match &cursor_ptr {
                    Some(ptr) => token.as_bytes().as_ptr_range().contains(ptr),
                    None => query.is_empty(),
                };
                if did_we_find_it {
                    // we found it!  enter phase two!
                    
                    // first, find all string lists in the current call stack and concatenate them.
                    // we want to return a list of all tag names checked anywhere in the current
                    // stack so we can avoid showing the user duplicate entries.
                    let mut stack = nested_parser.stack.into_iter();
                    let bottom_frame = stack.next().expect("NestedQueryParserStack should NEVER become empty!");
                    let mut ancestors = bottom_frame.data;
                    for frame in stack {
                        // we now work through the stack from the second-bottom frame toward the
                        // top, expanding our list of seen tokens.  this means that our list of
                        // seen tokens will be returned to the caller in the order they appeared in
                        // the string.  we do it this way, using a full fledged NestedQueryParser
                        // instead of just keeping a single Vec, appending tokens we find and
                        // skipping nested query brackets, partly because it's easier and partly
                        // because if we do it this way, we can avoid returning any tokens that
                        // appeared in other buckets that the user might want to autocomplete again.
                        //
                        // for example in the following query, where $ is the cursor position:
                        // all{ hyena 1-{ dog cat } 1-{ mouse $
                        // autocompleting "cat" again might be desirable, producing in our
                        // contrived example a query equivalent to "cat 1-{ dog mouse }", but
                        // autocompleting "hyena" would not, since it is already guaranteed to be
                        // present by the parent bucket.
                        let mut data = frame.data;
                        ancestors.append(&mut data);
                    }

                    // now we need to parse the rest of the query, in case the user went back and
                    // edited what they had already written.  in this case, the cursor will be in
                    // the middle, and we'll still have plenty of query left.  we need to extract
                    // all tokens that are attached to either the bucket the cursor is in or one of
                    // its direct ancestors, but no other buckets.

                    'a: while !query.is_empty() {
                        let mut start_of_discarded_region = query.find(['{','}']);
                        let mut end_of_discarded_region = start_of_discarded_region;
                        if let Some(pos2) = start_of_discarded_region {
                            if query.as_bytes()[pos2]==b'{' {
                                let mut depth = 1u32;
                                let mut pos3 = pos2+1; // it is 2AM.  this is the best variable name
                                                       // i can come up with.
                                'b:{
                                    while depth > 0 {
                                        dbg!(pos3);
                                        dbg!(query);
                                        dbg!(&query[pos3..]);
                                        match query[pos3..].find(['{','}']) {
                                            Some(pos) => {
                                                pos3+=pos;
                                                if query.as_bytes()[pos3]==b'{' {
                                                    depth += 1;
                                                } else if query.as_bytes()[pos3]==b'}'{
                                                    depth -= 1;
                                                } else{panic!()}
                                                dbg!(depth);
                                                pos3+=1;//advance past the curly brace
                                            }
                                            None => {
                                                start_of_discarded_region = None;
                                                end_of_discarded_region = None;
                                                dbg!(query);
                                                break 'b;
                                            }
                                        }
                                    }
                                    end_of_discarded_region = Some(pos3);
                                    start_of_discarded_region = query[..pos2].rfind([' ','}']);
                                    assert!(end_of_discarded_region.is_none() || end_of_discarded_region.unwrap() > start_of_discarded_region.unwrap());
                                }

                            }
                        }
                        let partial_query = start_of_discarded_region.map(|pos| &query[..pos]).unwrap_or(query);
                        partial_query.split(' ').filter(|x|!x.is_empty()).for_each(|token| ancestors.push(token));
                        match end_of_discarded_region {
                            Some(pos) => query = &query[pos+1..],
                            None => break
                        }

                    }
                    // put any remaining tokens in the query into the list.
                    query.split(' ').filter(|x|!x.is_empty()).for_each(|token| ancestors.push(token));

                    return Some((token, ancestors));
                } else {
                    nested_parser.get_data().push(token);
                }
            },
            _ => {},
        }
        space0::<_, ErrorKind>.parse_next(&mut query).unwrap();
    }

    None
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

    #[test]
    fn test_sort_order() {
        let mut sort_order_parser = SortOrderParser::new();
        let e: PResult<_, ContextError> = sort_order_parser.parse_peek("abcde");
        assert!(matches!(e.unwrap_err(), ErrMode::Backtrack(_)));
        let e: PResult<_, ContextError> = sort_order_parser.parse_peek("order:score ");
        assert_eq!(e.unwrap(), (" ",()));
        assert_eq!(sort_order_parser.finalize(), SortOrder::Score);

        let tag_db = TagDatabase::new([
            Tag::new_debug("a", 1),
            Tag::new_debug("b", 2),
            Tag::new_debug("c", 3),
            Tag::new_debug("order:favcount_asc", 4),
        ].into());

        let res = parse_query_and_sort_order(&tag_db, "a b c").unwrap();
        assert_eq!(res.0.kernel.tags.iter().map(|x|x.tag_id).collect::<Vec<_>>(), vec![1,2,3]);
        assert_eq!(res.1, SortOrder::Date);

        let res = parse_query_and_sort_order(&tag_db, "a b order:favcount_asc c").unwrap();
        dbg!(&res.0);
        assert_eq!(res.0.kernel.tags.iter().map(|x|x.tag_id).collect::<Vec<_>>(), vec![1,2,3]);
        assert_eq!(res.1, SortOrder::FavCountAscending);
    }

    #[test]
    fn test_autocomplete() {
        let s = "yes1 all{ yes2 1-{ no1 } all{no2} yes3$ yes4  yes5 all{no3 all{ no4 } } yes6 } yes7 all{ no4}1-{no5} 1-{no6 1-{no7}}yes8";
        let pos = s.find('$').unwrap();
        let (token, others) = parse_query_for_autocomplete(s, pos).unwrap();
        assert_eq!(token, "yes3$");
        assert_eq!(others, vec!["yes1","yes2","yes4","yes5","yes6","yes7", "yes8"]);
        let pos = s.len();
        let (token, others) = parse_query_for_autocomplete(s, pos).unwrap();
        assert_eq!(token,"yes6");
        assert_eq!(others,vec!["yes1","yes8"]);
        // if the cursor is positioned on a nested query token (curly brace) we should return None
        let pos = s.find('-').unwrap();
        assert!(parse_query_for_autocomplete(s,pos).is_none());

    }
}
