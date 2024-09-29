use std::num::NonZeroU32;

use winnow::ascii::{digit1, space0};
use winnow::combinator::{alt, eof, opt, repeat, repeat_till, seq};
use winnow::error::{ErrMode, ErrorKind, FromExternalError, ParserError, StrContext};
use winnow::stream::Stream as _;
use winnow::token::take_until;
use winnow::{PResult, Parser};

use crate::db::pools::Pool;
use crate::db::posts::{Post, PostDatabase};
use crate::db::tags::TagDatabase;

use super::{Kernel, NestedQuery, NestedQueryParser};
use super::e6_posts::{ExternalError, PostKernel, TagQueryParser};

#[derive(Debug)]
struct SequenceStep {
    query: NestedQuery<PostKernel>,
    allow_next_step_on_same_post: bool,
}

#[derive(Debug,Clone,Copy,Eq,PartialEq)]
enum CounterBound {
    Absolute(usize),
    Percentage(u8),
    All,
}

impl CounterBound {
    fn resolve(&self, length: usize) -> usize {
        match *self {
            Self::Absolute(val) => val,
            Self::Percentage(val) => {val as usize * length / 100},
            Self::All => length,
        }
    }
}

#[derive(Debug)]
struct Counter {
    query: NestedQuery<PostKernel>,
    min: CounterBound,
    max: CounterBound,
}

#[derive(Debug)]
pub struct PoolKernelData {
    counters: Vec<(Counter, usize)>,
    sequences: Vec<(Vec<SequenceStep>, usize)>,
}

pub struct PoolKernel<'db> {
    data: PoolKernelData,
    post_db: &'db PostDatabase,
}

impl<'db> PoolKernel<'db> {
    pub fn new(data: PoolKernelData, post_db: &'db PostDatabase) -> Self {
        Self {data, post_db}
    }
}

struct Peeker<I: Iterator> {
    iterator: I,
    value: I::Item,
}

impl<I:Iterator> Peeker<I> {
    fn new(mut iterator: I) -> Self {
        let value = iterator.next().expect("iterator must have at least one value");
        Self {iterator, value}
    }
    fn peek(&self)->&I::Item {
        &self.value
    }
    fn next(&mut self)->bool {
        let val = self.iterator.next();
        let res=val.is_some();
        if let Some(val) = val {
            self.value=val;
        }
        res
    }
}

impl<'b> Kernel for PoolKernel<'b> {
    type Post =  Pool;

    fn validate(&self, pool: &Pool, buckets: &mut [u8]) {
        self.validate(pool.iter_posts(self.post_db), buckets, false);
    }
}

struct PostChildSequenceKernel<'b>(pub PoolKernel<'b>);

impl<'b> Kernel for PostChildSequenceKernel<'b> {
    type Post = Post;
    fn validate(&self, post: &Post, buckets: &mut [u8]) {
        struct PostChildSequence<'a>{
            db: &'a PostDatabase,
            next: Option<&'a Post>,
            seen: Vec<NonZeroU32>,
        }

        impl<'a> Iterator for PostChildSequence<'a> {
            type Item=&'a Post;
            fn next(&mut self) -> Option<&'a Post> {
                let current = self.next;
                self.next = current
                    .and_then(|post| post.parent_id)
                    .and_then(|id| self.db.get_by_id(id))
                    .filter(|post| !self.seen.contains(&post.id));

                if let Some(n) = self.next {
                    self.seen.push(n.id);
                }
                current
            }
        }

        self.0.validate(PostChildSequence{db: self.0.post_db, next: Some(post), seen: vec![post.id]}, buckets, true)
    }
}

impl<'b> PoolKernel<'b> {
    fn validate<'p>(&self, posts: impl Iterator<Item=&'p Post> + 'p, buckets: &mut [u8], reverse: bool) {
        struct CounterState<'a> {
            counter: &'a Counter,
            count: usize,
            target_bucket: usize,
        }
        let mut tally = 0usize;
        let mut counters = self.data.counters.iter().map(|(counter, target_bucket)| CounterState{counter, target_bucket: *target_bucket, count: 0}).collect::<Vec<_>>();
        let mut sequences = self.data.sequences.iter().map(|(steps, bucket)| (Peeker::new(steps.iter()), bucket)).collect::<Vec<_>>();
        if reverse {
            counters.reverse();
            sequences.reverse();
        }
        for post in posts {
            tally += 1;
            for counter in counters.iter_mut() {
                if counter.counter.query.validate(&post) {
                    counter.count += 1;
                }
            }
            let mut to_evict: Vec<usize> = Vec::new();
            for (i, (sequence, bucket)) in sequences.iter_mut().enumerate() {
                while sequence.peek().query.validate(post) {
                    let allow_next = sequence.peek().allow_next_step_on_same_post;
                    // advance the sequence.  if we ran out of steps, increment the bucket and
                    // evict the sequence.
                    if !sequence.next() {
                        buckets[**bucket]+=1;
                        to_evict.push(i);
                        break;
                    }
                    // we didn't hit the end of the sequence.  if the sequence says to try the next
                    // step in the sequence on the same post, loop back to the top, otherwise break.
                    if !allow_next {
                        break;
                    }
                }
            }
            for idx in to_evict.iter().rev() {
                sequences.swap_remove(*idx);
            }
            /*
            if sequences.is_empty() && anys.is_empty() && alls.is_empty() {
                return;
            }
            */
        }
        for counter in counters {
            let min = counter.counter.min.resolve(tally);
            let max = counter.counter.max.resolve(tally);
            if counter.count >= min && counter.count <= max {
                buckets[counter.target_bucket]+=1;
            }
        }
    }
}

#[derive(thiserror::Error,Debug)]
pub enum PoolQueryParseError {
    #[error("Missing ]")]
    MissingClose,
    #[error("Extra ]")]
    ExtraClose,
}

struct PTry<T,E>(pub PResult<T,E>);

impl<T,E> std::ops::Try for PTry<T,E> {
    type Output=Result<T,E>;

    type Residual=Result<std::convert::Infallible, ErrMode<E>>;

    fn from_output(output: Self::Output) -> Self {
        Self(output.map_err(ErrMode::Cut))
    }

    fn branch(self) -> std::ops::ControlFlow<Self::Residual, Self::Output> {
        use std::ops::ControlFlow;
        match self.0 {
            Ok(val) => ControlFlow::Continue(Ok(val)),
            Err(ErrMode::Backtrack(val)) => ControlFlow::Continue(Err(val)),
            Err(val) => ControlFlow::Break(Err(val))
        }
    }
}

impl<T,E> std::ops::FromResidual for PTry<T,E> {
    fn from_residual(residual: Result<std::convert::Infallible, ErrMode<E>>) -> Self {
        Self(residual.map(|e| match e {}))
    }
}

fn parse_counter_bound<'a, E>(input: &mut &'a str) -> PResult<CounterBound, E> where E: ParserError<&'a str> {
    let num = digit1.parse_to::<usize>().parse_next(&mut *input)?;
    if input.starts_with('%') {
        // tried using opt(...)?.is_some() here and it didn't work
        *input=&input[1..];
        Ok(CounterBound::Percentage(num as u8))
    } else {
        Ok(CounterBound::Absolute(num))
    }
}

pub fn parse_query<'a, 'db>(mut parse_tag: impl FnMut(&'a str) -> Vec<u32>, post_db: &'db PostDatabase, query: &'a str) -> Result<NestedQuery<PoolKernel<'db>>, ExternalError<'a>> {
    let mut nqp = NestedQueryParser::<()>::new();

    let mut counters = Vec::new();
    let mut sequences = Vec::new();

    enum BracketKind {
        Sequence,
        Single(CounterBound, CounterBound)
    }

    repeat(1.., |input: &mut &'a str| {
        let checkpoint = input.checkpoint();
        if let Ok(_) = PTry(nqp.parse_next(&mut *input))? {
            return Ok(());
        }
        input.reset(&checkpoint);
        let (mode, square_brace_is_backslashed) = seq!(
                alt((
                        "any".map(|_| BracketKind::Single(CounterBound::Absolute(1), CounterBound::All)),
                        "all".map(|_| BracketKind::Single(CounterBound::All, CounterBound::All)),
                        seq!(parse_counter_bound.context(StrContext::Label("lower bound")), opt(seq!('-', opt(parse_counter_bound).context(StrContext::Label("upper bound")))).context(StrContext::Label("opt"))).map(|(start,rest)| {
                            let end = match rest {
                                None => start,
                                Some((_, None)) => CounterBound::All,
                                Some((_, Some(x))) => x,
                            };
                            BracketKind::Single(start, end)
                        }),
                        "".map(|_| BracketKind::Sequence),
                    )),
                alt((
                        "[".map(|_| false),
                        "\\[".map(|_| true),
                    )),
        ).parse_next(&mut *input)?;
        
        space0.parse_next(&mut *input)?;
        
        let input2 = take_until(0.., if square_brace_is_backslashed {"\\]"} else {"]"}).parse_next(&mut *input)?;

        if square_brace_is_backslashed {
            *input = &input[2..];
        } else {
            *input = &input[1..];
        }

        match mode {
            BracketKind::Sequence => {
                
               let v: Vec<_> = repeat(1.., 
                       |input: &mut &'a str| parse_single(&mut parse_tag, input),
               ).parse(input2).map_err(|e|e.into_inner())?;

               sequences.push((v.into_iter().map(|(query, terminator)| SequenceStep{query, allow_next_step_on_same_post: match terminator {
                    Terminator::Greater => true,
                    Terminator::PipeGreater => false,
                    Terminator::Eof => false, // doesn't matter which option we put here since it will never be read.
               }}).collect(), nqp.get_current_bucket(0)));
            },
            BracketKind::Single(start, end) => {
                let query = super::e6_posts::parse_query(&mut parse_tag, input2).map_err(ErrMode::Cut)?;
                counters.push((Counter {
                    query, min: start, max: end
                }, nqp.get_current_bucket(0)));
            },
        }
        nqp.increment_count(0, 1);
        space0.parse_next(&mut *input)?;
        Ok(())
    }
    ).parse(query).map_err(|e|e.into_inner())?;
    /*
    while !query.is_empty() {
        if query.starts_with('[') {
        } else if query.starts_with("all[") || query.starts_with("any[") {
            let was_all = query.starts_with("all[");
            query = &query[4..];
            let mut pos = query.find(']');
            let mut requires_replace = false;
            while let Some(pos) = pos {
                if pos == query.len() - 1 || query.as_bytes()[pos+1] != b']' {
                    break;
                }
                requires_replace = true;
            }
            let Some(pos) = pos else {
                return Err(TreeError::from_external_error(&query, ErrorKind::Verify, PoolQueryParseError::MissingClose));
            };
            /*
             * TODO figure out how the hell I'm supposed to support escape characters,
             * TODO or just decide I'm not going to support ] in queries at all.
            let sub_query = if requires_replace {
                Cow::Owned(query[..pos].replace("]]","]"))
            } else {
                Cow::Borrowed(query)
            };
            */
            let query = super::e6_posts::parse_query(tag_db, &query[..pos])?;

        }
    }
*/
    // query[query.len()..] will of course be an empty string.
    // we use this rather than &"" because the implementation of from_external_error() will panic
    // if the argument passed is not a subslice of the original query.
    Ok(NestedQuery::new(
            nqp.finalize()
            .map_err(|e| ExternalError::from_external_error(&&query[query.len()..],ErrorKind::Eof,e))?, 
            PoolKernel {post_db, data: PoolKernelData {
                counters, sequences
            }}))
}

#[derive(PartialEq,Eq,Debug)]
enum Terminator {
    Greater,
    PipeGreater,
    //CloseSquare,
    Eof,
}

fn parse_single<'a, E>(parse_tag: &mut impl FnMut(&'a str) -> Vec<u32>, query: &mut &'a str) -> PResult<(NestedQuery<PostKernel>, Terminator), E> where E: ParserError<&'a str> + FromExternalError<&'a str, super::ParseError> + FromExternalError<&'a str, super::e6_posts::BadTag>{
    let mut parser = TagQueryParser::new(parse_tag);
    let parser_ref = &mut parser;

    let ((), terminator) = repeat_till(1..,
        seq!(
            |input: &mut &'a str| parser_ref.parse_next(input),
            space0,
            ),
        alt((
            eof.map(|_| Terminator::Eof),
            ">".map(|_| Terminator::Greater),
            "|>".map(|_| Terminator::PipeGreater),
            //"]".map(|_| Terminator::CloseSquare),
        )),
    ).parse_next(query)?;
    space0.parse_next(query)?;
    Ok((parser.finalize().map_err(|e| ErrMode::Cut(E::from_external_error(query, ErrorKind::Eof, e)))?, terminator))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use winnow::error::TreeError;

    use super::*;
    use crate::db::{posts::Post, tags::{Tag, TagDatabase}};

    #[test]
    fn test_parse_single() {
        let db = TagDatabase::new(Box::from([
            Tag::new_debug("a",1),
            Tag::new_debug("b",2),
            Tag::new_debug("c",3),
            Tag::new_debug("d",4),
            Tag::new_debug("e",5),
            Tag::new_debug("f",6),
            Tag::new_debug("g",7),
            Tag::new_debug("h",8),
        ]));
        let s = "a b > c d |> e f";// ] g h";
        let cursor = &mut &*s;
        let mut search_func = |s| db.get(s).map(|tag| tag.id).into_iter().collect::<Vec<u32>>();
        let (parsed, terminator) = parse_single::<TreeError<_>>(&mut search_func, cursor).unwrap();
        assert_eq!(terminator, Terminator::Greater);
        let (parsed, terminator) = parse_single::<TreeError<_>>(&mut search_func, cursor).unwrap();
        assert_eq!(terminator, Terminator::PipeGreater);
        //let (parsed, terminator) = parse_single::<TreeError<_>>(&db, cursor).unwrap();
        //assert_eq!(terminator, Terminator::CloseSquare);
        assert_eq!(cursor.trim(), "e f");
        let (parsed, terminator) = parse_single::<TreeError<_>>(&mut search_func, cursor).unwrap();
        assert_eq!(terminator, Terminator::Eof);
    }

    #[test]
    fn test_parse_counter_bound() {
        assert_eq!(parse_counter_bound::<ErrorKind>.parse("80").unwrap(), CounterBound::Absolute(80));
        assert_eq!(parse_counter_bound::<ErrorKind>.parse("80%").unwrap(), CounterBound::Percentage(80));
    }

    fn post(id: u32, tags: &[u32]) -> Post {
        Post {
            id: NonZeroU32::new(id).unwrap(),
            tags: tags.into(),
            ..Post::debug_default()
        }
    }

    #[test]
    fn test_sequence() {
        let tag_db = TagDatabase::new(Box::from([
            Tag::new_debug("a",1),
            Tag::new_debug("b",2),
            Tag::new_debug("c",3),
            Tag::new_debug("d",4),
        ]));

        let post_db = PostDatabase::new(Box::from([
            post(1, &[1]),
            post(2, &[2]),
            post(3, &[1,2]),
            post(4, &[3]),
            post(5, &[4]),
            post(6, &[3,4]),
        ]));

        let search_func = |s| tag_db.get(s).map(|tag| tag.id).into_iter().collect::<Vec<u32>>();
        let validator = parse_query(search_func, &post_db, "[a |> b > c > d]").unwrap();

        assert!(validator.validate(&Pool::debug_default([1,2,4,5])));
        assert!(validator.validate(&Pool::debug_default([1,2,1,4,5])));
        assert!(!validator.validate(&Pool::debug_default([1,2,4])));
        assert!(!validator.validate(&Pool::debug_default([3,4,5])));
        assert!(validator.validate(&Pool::debug_default([1,2,6])));
    }

    #[test]
    fn test_count() {
        let tag_db = TagDatabase::new(Box::from([
            Tag::new_debug("a",1),
            Tag::new_debug("b",2),
            Tag::new_debug("c",3),
            Tag::new_debug("d",4),
        ]));

        let post_db = PostDatabase::new(Box::from([
            post(1, &[1]),
            post(2, &[2]),
            post(3, &[1,2]),
            post(4, &[3]),
            post(5, &[4]),
            post(6, &[3,4]),
        ]));

        let search_func = |s| tag_db.get(s).map(|tag| tag.id).into_iter().collect::<Vec<u32>>();
        let validator = parse_query(search_func, &post_db, "2-3[ a ]").unwrap();

        assert!(validator.validate(&Pool::debug_default([2,2,1,2,1,2,2])));
        assert!(!validator.validate(&Pool::debug_default([2,2,1,2,1,2,2,1,1])));
        assert!(validator.validate(&Pool::debug_default([2,2,1,2,3,2,2])));

        let validator = parse_query(search_func, &post_db, "50%-[ a ]").unwrap();

        assert!(validator.validate(&Pool::debug_default([2,2,1,2,1,1])));
        assert!(!validator.validate(&Pool::debug_default([2,2,1,2,2,1])));
        assert!(validator.validate(&Pool::debug_default([1,1,1])));
    }

}
