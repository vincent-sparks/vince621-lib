use winnow::ascii::{space0,digit1};
use winnow::error::ErrMode;
use winnow::stream::{Offset, Stream};
use winnow::{PResult, Parser};
use winnow::token::one_of;
use std::fmt::{Debug,Display};

pub trait BucketQueryParser<'a> {
    type Error: Debug + Display;
    type Result: Predicate;
    fn parse_next(&mut self, token: &mut &'a str, target_bucket: usize) -> PResult<u8, Self::Error>;
    fn finalize(self) -> Self::Result;
}

pub trait Predicate {
    type Post;
    fn validate(&self, obj: &Self::Post, buckets: &mut [u8]);
}

struct Bucket {
    min: u8,
    max: u8,
    target: usize,
}

struct NestedQueryParser<V> {
    inner: V,
    buckets: Vec<Bucket>,
}

pub struct NestedQuery<V> {
    inner: V,
    buckets: Vec<Bucket>,
}

#[derive(thiserror::Error, Debug)]
pub enum ErrorKind<E: Debug + Display> {
    #[error("Missing }}")]
    MissingClose,
    #[error("Extra }}")]
    ExtraClose,
    #[error("{0}")]
    Validator(#[from] E),
}

#[derive(Debug)]
struct ParseError<E: Debug + Display> {
    offset: usize, 
    error: ErrorKind<E>,
}

impl<V> NestedQuery<V> {
    pub fn new<'a, U: BucketQueryParser<'a, Result=V>>(inner_parser: U, mut query: &'a str) -> Result<Self, ParseError<U::Error>> {
        let mut parser = NestedQueryParser {
            buckets: Vec::new(),
            inner: inner_parser,
        };
        let start = query.checkpoint();
        match parser.parse_inner(&mut query, 0, true) {
            Ok(count) => {
                let mut buckets = parser.buckets;
                buckets[0].min = count;
                buckets[0].max = count;
                Ok(Self {
                    buckets,
                    inner: parser.inner.finalize(),
                })
            },
            Err(error) => {
                Err(ParseError {
                    offset: query.offset_from(&start),
                    error,
                })
            }
        }
    }
}

impl<'a, V> NestedQueryParser<V> where V: BucketQueryParser<'a> {
    fn parse_inner(&mut self, query: &mut &'a str, target_bucket: usize, expect_eof: bool) -> Result<u8, ErrorKind<V::Error>> {
        let mut count = 0;
        loop {
            if query.is_empty() {
                if !expect_eof {
                    return Err(ErrorKind::MissingClose)
                }
                return Ok(count);
            }
            if query.starts_with('}') {
                if expect_eof {
                    return Err(ErrorKind::ExtraClose)
                }
                *query=&query[1..];
                return Ok(count);
            }
            let checkpoint = query.checkpoint();
            println!("about to parse, query: {:?}", *query);
            if let Ok((lower, upper)) = parse_range.parse_next(query) {
                println!("parsed ok, query: {:?}", *query);
                let next_target = self.buckets.len();
                // the 0s are just temporary values that will be overwritten after the recursive
                // call to parse_inner returns.
                self.buckets.push(Bucket{
                    min: lower.unwrap_or(0),
                    max: upper.unwrap_or(0),
                    target: target_bucket,
                });
                let sub_count = self.parse_inner(&mut *query, next_target, false)?;
                if lower.is_none() {
                    self.buckets[next_target].min=sub_count;
                }
                if upper.is_none() {
                    self.buckets[next_target].max=sub_count;
                }
                count+=1;
            } else {
                query.reset(&checkpoint);
                match self.inner.parse_next(&mut *query, target_bucket) {
                    Ok(sub_count) => {
                        count+=sub_count;
                    },
                    Err(e) => {
                        return Err(e
                                   .into_inner()
                                   .expect("validator parser should not operate in partial mode and should never ask for more data")
                                   .into())
                    },
                }
            }
        }
    }
}

fn parse_range(input: &mut &str) -> winnow::PResult<(Option<u8>, Option<u8>)> {
    if input.starts_with("all{") {
        *input=&input[4..];
        space0.parse_next(input)?; // should never fail
        return Ok((None,None));
    }
    let lower_bound = digit1.parse_to().parse_next(input)?;
    let upper_bound = match one_of(('-','{')).parse_next(input)? {
        '{' => {
            // "number{" means upper bound is same as lower bound
            Some(lower_bound)
        },
        '-' => {
            if !input.is_empty() && input.as_bytes()[0]==b'{' {
                *input=&input[1..];
                None
            } else {
                let upper_bound = digit1.parse_to().parse_next(input)?;
                '{'.parse_next(input)?;
                Some(upper_bound)
            }
        },
        _ => unreachable!(),
    };
    space0.parse_next(input)?;
    Ok((Some(lower_bound), upper_bound))
}

/// Winnow combinator that matches either EOF or a closing curly brace, which is not consumed.
/// This will match and leave the cursor where the caller of `Predicate::parse_next` wants you to
/// stop parsing.
pub fn end_of_tag<'a, E: winnow::error::ParserError<&'a str>>(input: &mut &'a str) -> PResult<(), E> {
    if input.is_empty() || input.starts_with('}') {
        Ok(())
    } else {
        Err(ErrMode::Backtrack(E::from_error_kind(input, winnow::error::ErrorKind::Token)))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use winnow::{combinator::opt, error::StrContext, PResult};

    struct NullPredicate;
    impl Predicate for NullPredicate {
        type Post=();
        fn validate(&self, obj: &Self::Post, buckets: &mut [u8]) {
        }
    }
    #[test]
    fn test_parse_range() {
        assert_eq!(parse_range.parse("1{  ").unwrap(), (Some(1), Some(1)));
        assert_eq!(parse_range.parse("1-{").unwrap(), (Some(1), None));
        assert_eq!(parse_range.parse("1-2{").unwrap(), (Some(1), Some(2)));
        assert_eq!(parse_range.parse("all{  ").unwrap(), (None, None));
        assert_eq!(parse_range.parse("all{").unwrap(), (None, None));
    }

    #[test]
    fn test_parse_query() {
        use winnow::error::ContextError;
        struct TestQueryParser;
        impl<'a> BucketQueryParser<'a> for TestQueryParser {
            type Error=winnow::error::TreeError<&'a str>;

            type Result = NullPredicate;

            fn parse_next(&mut self, token: &mut &'a str, target_bucket: usize) -> PResult<u8, Self::Error> {
                use winnow::combinator::{repeat_till, terminated};
                let count: usize;
                //let res = winnow::combinator::repeat_till::<_,_,Vec<_>,_,winnow::error::TreeError<&'a str>,_,_>(0..,winnow::combinator::terminated(winnow::ascii::digit1.parse_to::<usize>(), winnow::combinator::opt(' ')), winnow::combinator::eof).parse_next(token)?;
                (count, _) = repeat_till(0.., terminated(digit1.parse_to::<usize>().map(|a: usize| {assert_eq!(a, target_bucket); 1}).context(StrContext::Label("bad digit")), opt(' ')).context(StrContext::Label("bad terminated")), end_of_tag.context(StrContext::Label("end of tag"))).parse_next(token)?;
                Ok(count as u8)
            }

            fn finalize(self) -> Self::Result {
                NullPredicate
            }
        }
        NestedQuery::new(TestQueryParser, "0 0 all{ 1 1 1-{ 2 2 } 1 } 0 1-2{ 3 }").unwrap();
    }
}
