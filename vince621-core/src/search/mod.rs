/// General purpose parser for grammars of the type `a 1-{ b c d 2-3{ e f g h } } 2{ i j k }`.
/// 
/// 
use winnow::ascii::{space0,digit0,digit1};
use winnow::error::{ErrMode, ErrorKind, ParserError};
use winnow::stream::{Offset, Stream};
use winnow::{PResult, Parser};
use winnow::token::one_of;
use winnow::combinator::{seq, alt,opt};
use std::fmt::{Debug,Display};

pub mod e6_posts;
pub mod e6_pools;

pub trait Kernel {
    type Post;
    fn validate(&self, obj: &Self::Post, buckets: &mut [u8]);
}

#[derive(Debug, PartialEq,Clone,Copy)]
pub struct Bucket {
    min: u8,
    max: u8,
    target: usize,
}

#[derive(Debug, PartialEq,Clone,Copy)]
pub struct ParseBucket {
    pub min: Option<u8>,
    pub max: Option<u8>,
    target: usize,
}

// this is only a struct for readability's sake.  it could just as easily be a tuple.
struct StackItem {
    bucket_idx: usize,
    count: u8,
}

struct StackFrame<Data> {
    data: Data,
    items: Vec<StackItem>,
}

impl<Data> StackFrame<Data> where Data: Default {
    fn new(bucket_idx: usize) -> Self {
        StackFrame {
            data: Default::default(),
            items: vec![
                StackItem {bucket_idx, count: 0},
            ]
        }
    }
}

pub struct NestedQueryParser<Data=()> {
    stack: Vec<StackFrame<Data>>,
    pub buckets: Vec<ParseBucket>,
}

#[derive(Debug,PartialEq)]
pub struct NestedQuery<K> {
    pub(crate) buckets: Vec<Bucket>,
    pub(crate) kernel: K,
}

impl<K> NestedQuery<K> where K: Kernel {
    pub fn new(buckets: Vec<Bucket>, kernel: K) -> Self {
        Self{buckets,kernel}
    }
    pub fn validate(&self, post: &K::Post) -> bool {
        let mut buckets = unsafe {Box::new_zeroed_slice(self.buckets.len()).assume_init()};
        self.kernel.validate(post, &mut buckets);
        for (idx, bucket) in self.buckets.iter().enumerate().rev() {
            // don't work on the root bucket.
            if idx==0 {break;}
            // necessary precondition: bucket.target < idx
            if buckets[idx] >= bucket.min && buckets[idx] <= bucket.max {
                buckets[bucket.target]+=1;
            }
        }
        buckets[0] >= self.buckets[0].min && buckets[0] <= self.buckets[0].max
    }
    pub fn into_inner(self) -> (Vec<Bucket>, K) {
        let Self{buckets,kernel} = self;
        (buckets,kernel)
    }
}

impl<Data: Default> NestedQueryParser<Data> {
    pub fn new() -> Self {
        Self {
            stack: vec![StackFrame::new(0)],
            buckets: vec![ParseBucket {min: None, max: None, target: 0}],
        }
    }
    pub fn increment_count(&mut self, idx: usize, count: u8) {
        let last_idx = self.stack.len()-1;
        self.stack[last_idx].items[idx].count += count;
    }
    pub fn get_current_bucket(&self, idx: usize) -> usize {
        let last_idx = self.stack.len()-1;
        self.stack[last_idx].items[idx].bucket_idx
    }
    pub fn finalize(mut self) -> Result<Vec<Bucket>, ParseError> {
        if self.stack.len() > 1 {
            return Err(ParseError::MissingClose);
        }
        for item in self.stack[0].items.iter() {
            self.buckets[item.bucket_idx].min.get_or_insert(item.count);
            self.buckets[item.bucket_idx].max.get_or_insert(item.count);
        }
        Ok(self.buckets.into_iter().map(|b| Bucket {
            min: b.min.expect("bucket min should have been filled before calling finalize"),
            max: b.max.expect("bucket max should have been filled before calling finalize"),
            target: b.target,
        }).collect())
    }
    /**
     * Creates a new ancillary bucket and returns the index of the newly created slot.  To get the
     * index of the bucket, use get_current_bucket().
     */
    pub fn new_ancillary_bucket(&mut self, min: Option<u8>, max: Option<u8>) -> usize {
        let last_idx = self.stack.len()-1;
        let stack_frame = &mut self.stack[last_idx].items;
        let new_slot_idx = stack_frame.len();
        let new_bucket_idx = self.buckets.len();
        stack_frame[0].count+=1;
        self.buckets.push(ParseBucket{min, max, target: stack_frame[0].bucket_idx});
        stack_frame.push(StackItem {
            bucket_idx: new_bucket_idx,
            count: 0,
        });
        new_slot_idx
    }
    /**
     * Returns a mutable reference to user data pertaining to the current stack frame.
     */
    pub fn get_data(&mut self) -> &mut Data {
        let last_idx = self.stack.len() - 1;
        &mut self.stack[last_idx].data
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("Missing }}")]
    MissingClose,
    #[error("Extra }}")]
    ExtraClose,
}

impl<'s, E, D> Parser<&'s str, usize, E> for NestedQueryParser<D> where E: ParserError<&'s str> + winnow::error::FromExternalError<&'s str, ParseError>, D: Default {
    fn parse_next(&mut self, query: &mut &'s str) -> PResult<usize, E> {
        if query.starts_with('}') {
            if self.stack.len() < 2 {
                return Err(ErrMode::Cut(E::from_external_error(query, ErrorKind::Token, ParseError::ExtraClose)));
            }
            *query=&query[1..];
            let top = self.stack.pop().unwrap();
            for item in top.items.iter() {
                let bucket = &mut self.buckets[item.bucket_idx];
                bucket.min.get_or_insert(item.count);
                bucket.max.get_or_insert(item.count);
            }
            return Ok(self.stack[self.stack.len()-1].items[0].bucket_idx);
        }
        let (lower, upper) = parse_range.parse_next(query)?;
        println!("parsed ok, query: {:?}", *query);

        self.increment_count(0,1);

        let next_target = self.buckets.len();
        // the 0s are just temporary values that will be overwritten after the recursive
        // call to parse_inner returns.
        self.buckets.push(ParseBucket{
            min: lower,
            max: upper,
            target: self.get_current_bucket(0),
        });
        self.stack.push(StackFrame::new(next_target));
        Ok(next_target)
    }
}

fn parse_range<'a, E>(input: &mut &'a str) -> winnow::PResult<(Option<u8>, Option<u8>), E> where E: ParserError<&'a str> {
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
    Ok((Some(lower_bound), upper_bound))
}

/// Winnow combinator that matches either EOF or a closing curly brace, which is not consumed.
/// This will match and leave the cursor where the caller of `Predicate::parse_next` wants you to
/// stop parsing.
pub fn end_of_tag<'a, E: winnow::error::ParserError<&'a str>>(input: &mut &'a str) -> PResult<(), E> {
    if input.is_empty() || input.starts_with('}') || input.starts_with("all{") {
        Ok(())
    } else {
        let e: winnow::error::IResult<_,_,winnow::error::ErrorKind> = seq!(digit1, opt(seq!('-',digit0)), '{').parse_peek(*input);
        if e.is_ok() {
            return Ok(());
        }
        Err(ErrMode::Backtrack(E::from_error_kind(input, winnow::error::ErrorKind::Verify)))
    }
}

#[cfg(test)]
mod test {
    use std::cell::RefCell;

    use crate::search::test::why_do_i_have_to_hack_this::TryMapCut;

    use super::*;
    use winnow::combinator::repeat;

    struct NullKernel;
    impl Kernel for NullKernel {
        type Post=();
        fn validate(&self, obj: &Self::Post, buckets: &mut [u8]) {
        }
    }
    #[test]
    fn test_parse_range() {
        assert_eq!(parse_range::<ErrorKind>.parse("1{").unwrap(), (Some(1), Some(1)));
        assert_eq!(parse_range::<ErrorKind>.parse("1-{").unwrap(), (Some(1), None));
        assert_eq!(parse_range::<ErrorKind>.parse("1-2{").unwrap(), (Some(1), Some(2)));
        assert_eq!(parse_range::<ErrorKind>.parse("all{").unwrap(), (None, None));
    }

    #[derive(Debug)]
    struct DummyError(String);

    impl std::fmt::Display for DummyError{fn fmt(&self,fmt:&mut std::fmt::Formatter<'_>)->std::fmt::Result{fmt.write_str(self.0.as_str())}}

    impl std::error::Error for DummyError{}
    
    /// Winnow does not include built-in functionality for a .try_map() to generate a ErrMode::Cut
    /// rather than an ErrMode::Backtrack.  This is a copy paste of the entire TryMap class with one
    /// line changed.
    mod why_do_i_have_to_hack_this {
        use winnow::*;
        use winnow::stream::*;
        use winnow::error::*;
        pub struct TryMapCut<F, G, I, O, O2, E, E2>
            where
            F: Parser<I, O, E>,
            G: FnMut(O) -> Result<O2, E2>,
            I: Stream,
            E: FromExternalError<I, E2>,
            {
                parser: F,
                map: G,
                i: core::marker::PhantomData<I>,
                o: core::marker::PhantomData<O>,
                o2: core::marker::PhantomData<O2>,
                e: core::marker::PhantomData<E>,
                e2: core::marker::PhantomData<E2>,
            }

        impl<F, G, I, O, O2, E, E2> TryMapCut<F, G, I, O, O2, E, E2>
            where
                F: Parser<I, O, E>,
                G: FnMut(O) -> Result<O2, E2>,
                I: Stream,
                E: FromExternalError<I, E2>,
                {
                    #[inline(always)]
                    pub(crate) fn new(parser: F, map: G) -> Self {
                        Self {
                            parser,
                            map,
                            i: Default::default(),
                            o: Default::default(),
                            o2: Default::default(),
                            e: Default::default(),
                            e2: Default::default(),
                        }
                    }
                }

        impl<F, G, I, O, O2, E, E2> Parser<I, O2, E> for TryMapCut<F, G, I, O, O2, E, E2>
            where
                F: Parser<I, O, E>,
                G: FnMut(O) -> Result<O2, E2>,
                I: Stream,
                E: FromExternalError<I, E2>,
                {
                    #[inline]
                    fn parse_next(&mut self, input: &mut I) -> PResult<O2, E> {
                        let start = input.checkpoint();
                        let o = self.parser.parse_next(input)?;
                        let res = (self.map)(o).map_err(|err| {
                            input.reset(&start);
                            ErrMode::from_external_error(input, ErrorKind::Verify, err).cut()
                        });
                        res
                    }
                }
    }

    #[test]
    fn test_parse_query() {
        let parser = RefCell::new(NestedQueryParser::<()>::new());
        let parser_ref = &parser;
        let res: Result<(), winnow::error::ParseError<&'static str, winnow::error::ContextError<&'static str>>> = 
            repeat(0.., seq!(
                    alt((
                            (|input: &mut &'static str| parser_ref.borrow_mut().parse_next(input).map(|_|())).context("parse"),
                            TryMapCut::new(digit1.parse_to(), |idx: usize| {
                                let mut parser = parser_ref.borrow_mut();
                                parser.increment_count(0, 1);
                                if parser.get_current_bucket(0) == idx {
                                    Ok(())
                                } else {
                                    Err(DummyError(format!("expected: {} found: {}", parser.get_current_bucket(0), idx)))
                                }
                            }).context("number"),
                    )),
                    space0.context("space"),
                    )).parse("0 all{ 1 1 1-{ 2 1{3 3} 1-2{4 4 4} 2 }1 1{5}}1{6} 0");
        res.unwrap();
        let buckets = parser.into_inner().finalize().unwrap();
        assert_eq!(buckets, vec![
                   Bucket{min: 4, max: 4, target: 0},
                   Bucket{min: 5, max: 5, target: 0},
                   Bucket{min: 1, max: 4, target: 1},
                   Bucket{min: 1, max: 1, target: 2},
                   Bucket{min: 1, max: 2, target: 2},
                   Bucket{min: 1, max: 1, target: 1},
                   Bucket{min: 1, max: 1, target: 0},
        ]);
    }

    struct DummyValidator<'a>(&'a [u8]);
    impl Kernel for DummyValidator<'_> {
        type Post=();

        fn validate(&self, _: &(), buckets: &mut [u8]) {
            buckets.copy_from_slice(self.0);
        }
    }
    #[test]
    fn test_validate() {
        let buckets = vec![
            Bucket{min:2,max:2,target:0},
            Bucket{min:2,max:2,target:0},
            Bucket{min:1,max:1,target:1},
            Bucket{min:1,max:1,target:1},
            Bucket{min:1,max:1,target:2},
        ];
        assert!(NestedQuery::new(buckets.clone(), DummyValidator(&[1,0,0,1,1])).validate(&()));
        assert!(!NestedQuery::new(buckets.clone(), DummyValidator(&[1,0,1,1,1])).validate(&()));
    }
    #[test]
    fn test_zero() {
        let buckets = vec![
            Bucket{min:2,max:2,target:0},
            Bucket{min:0,max:0,target:1},
        ];
        assert!(!NestedQuery::new(buckets, DummyValidator(&[1,1])).validate(&()));
    }
}
