use std::borrow::Cow;

use winnow::ascii::{space0, space1};
use winnow::combinator::{alt, eof, repeat, repeat_till, seq};
use winnow::error::{ErrMode, ErrorKind, FromExternalError, ParseError, ParserError, TreeError};
use winnow::stream::Stream as _;
use winnow::token::{take_till, take_until};
use winnow::{PResult, Parser};

use crate::db::pools::Pool;
use crate::db::posts::{Post, PostDatabase};
use crate::db::tags::TagDatabase;
use crate::search::parse_range;

use super::{Kernel, NestedQuery, NestedQueryParser};
use super::e6_posts::{PostKernel, TagQueryParser};

#[derive(Debug)]
struct SequenceStep {
    query: NestedQuery<PostKernel>,
    allow_next_step_on_same_post: bool,
}

#[derive(Debug)]
pub struct PoolKernelData {
    alls: Vec<(NestedQuery<PostKernel>, usize)>,
    anys: Vec<(NestedQuery<PostKernel>, usize)>,
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
        let mut anys = self.data.anys.iter().collect::<Vec<_>>();
        let mut alls = self.data.alls.iter().collect::<Vec<_>>();
        let mut sequences = self.data.sequences.iter().map(|(steps, bucket)| (Peeker::new(steps.iter()), bucket)).collect::<Vec<_>>();
        for post in pool.iter_posts(self.post_db) {
            let mut to_evict: Vec<usize> = Vec::new();
            for (i, (query, bucket)) in anys.iter().enumerate() {
                if query.validate(post) {
                    buckets[*bucket]+=1;
                    to_evict.push(i);
                }
            }
            for idx in to_evict.iter().rev() {
                anys.swap_remove(*idx);
            }
            to_evict.clear();
            for (i, (query, _)) in alls.iter().enumerate() {
                if !query.validate(post) {
                    to_evict.push(i);
                }
            }
            for idx in to_evict.iter().rev() {
                alls.swap_remove(*idx);
            }
            to_evict.clear();
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
            if sequences.is_empty() && anys.is_empty() && alls.is_empty() {
                return;
            }
        }
        // increment the buckets of any surviving alls
        for (_, bucket) in alls {
            buckets[*bucket]+=1;
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

pub fn parse_query<'a, 'db>(tag_db: &TagDatabase, post_db: &'db PostDatabase, mut query: &'a str) -> Result<NestedQuery<PoolKernel<'db>>, TreeError<&'a str>> {
    let mut nqp = NestedQueryParser::<()>::new();

    let mut alls = Vec::new();
    let mut anys = Vec::new();
    let mut sequences = Vec::new();

    enum BracketKind {
        Sequence,
        Single(Option<u8>, Option<u8>)
    }

    repeat(1.., |input: &mut &'a str| {
        let checkpoint = input.checkpoint();
        if let Ok(_) = PTry(nqp.parse_next(&mut *input))? {
            return Ok(());
        }
        input.reset(&checkpoint);
        let (mode, square_brace_is_backslashed) = seq!(
                alt((
                        parse_range.map(|(start,end)|BracketKind::Single(start,end)),
                        "any".map(|_| BracketKind::Single(Some(1), None)),
                        "".map(|_| BracketKind::Sequence),
                    )),
                alt((
                        "[".map(|_| false),
                        "\\[".map(|_| true),
                    )),
        ).parse_next(&mut *input)?;
        
        space0.parse_next(&mut *input)?;
        
        let input2 = take_until(1.., if square_brace_is_backslashed {"\\]"} else {"]"}).parse_next(&mut *input)?;

        if square_brace_is_backslashed {
            *input = &input[2..];
        } else {
            *input = &input[1..];
        }

        match mode {
            BracketKind::Sequence => {
                
               let v: Vec<_> = repeat(1.., 
                       |input: &mut &'a str| parse_single(tag_db, input),
               ).parse(input2).map_err(|e|e.into_inner())?;

               sequences.push((v.into_iter().map(|(query, terminator)| SequenceStep{query, allow_next_step_on_same_post: match terminator {
                    Terminator::Greater => true,
                    Terminator::PipeGreater => false,
                    Terminator::Eof => false, // doesn't matter which option we put here since it will never be read.
               }}).collect(), nqp.get_current_bucket(0)));
            },
            BracketKind::Single(start, end) => {
                todo!()
            },
        }
        nqp.increment_count(0, 1);
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
    Ok(NestedQuery::new(
            nqp.finalize()
            .map_err(|e| TreeError::from_external_error(&"",ErrorKind::Eof,e))?, 
            PoolKernel {post_db, data: PoolKernelData {
                anys, alls, sequences
            }}))
}

#[derive(PartialEq,Eq,Debug)]
enum Terminator {
    Greater,
    PipeGreater,
    //CloseSquare,
    Eof,
}

fn parse_single<'a, E>(db: &TagDatabase, query: &mut &'a str) -> PResult<(NestedQuery<PostKernel>, Terminator), E> where E: ParserError<&'a str> + FromExternalError<&'a str, super::ParseError> + FromExternalError<&'a str, super::e6_posts::UnknownTag>{
    let mut parser = TagQueryParser::new(db);
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
    Ok((parser.finalize().map_err(|e| ErrMode::Cut(E::from_external_error(&"", ErrorKind::Eof, e)))?, terminator))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::db::tags::{Tag, TagDatabase};

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
        let (parsed, terminator) = parse_single::<TreeError<_>>(&db, cursor).unwrap();
        assert_eq!(terminator, Terminator::Greater);
        let (parsed, terminator) = parse_single::<TreeError<_>>(&db, cursor).unwrap();
        assert_eq!(terminator, Terminator::PipeGreater);
        //let (parsed, terminator) = parse_single::<TreeError<_>>(&db, cursor).unwrap();
        //assert_eq!(terminator, Terminator::CloseSquare);
        assert_eq!(cursor.trim(), "e f");
        let (parsed, terminator) = parse_single::<TreeError<_>>(&db, cursor).unwrap();
        assert_eq!(terminator, Terminator::Eof);
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

        let validator = parse_query(&tag_db, &post_db, "[ a |> b > c > d ]").unwrap();

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

        let validator = parse_query(&tag_db, &post_db, "2-3[ a ]").unwrap();

        assert!(validator.validate(&Pool::debug_default([2,2,1,2,1,2,2])));
        assert!(!validator.validate(&Pool::debug_default([2,2,1,2,1,2,2,1,1])));
        assert!(validator.validate(&Pool::debug_default([2,2,1,2,3,2,2])));
    }

}
