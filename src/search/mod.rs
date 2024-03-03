trait Searcher: Default {
    const DELIMITER: char;
    type Post;
    type Error: std::fmt::Display;
    fn parse(&mut self, token: &str, target_bucket: usize) -> Result<(), Self::Error>;
    fn validate(&self, obj: &Self::Post, buckets: &mut [u8]);
}

struct Bucket {
    min: u8,
    max: u8,
    target: usize,
}

struct Validator<V> {
    searcher: V,
    buckets: Vec<Bucket>,
}

#[derive(thiserror::Error, Debug)]
#[error(transparent)]
struct E<V: std::fmt::Display> {
    byte_offset: usize,
    kind: QueryParseError<V>,
}

#[derive(thiserror::Error, Debug)]
enum QueryParseError<V: std::fmt::Display> {
    #[error("empty token")]
    EmptyToken,
    #[error("{0}")]
    BadToken(#[from] V),
}

impl<V> Validator<V> where V: Searcher {
    fn new(mut query: &str) -> Result<Self, V::Error> {
        let mut me = Self {
            searcher: V::default(),
            buckets: Vec::new(),
        };
        me.buckets.push(Bucket{min: 0, max: 0, target: usize::MAX});
        let count = me.parse(&mut query.split_inclusive(['{','}',V::DELIMITER]), 0)?;
        me.buckets[0].min = count;
        me.buckets[0].max = count;
        Ok(me)
    }
    fn parse<'a>(&mut self, query: &mut impl Iterator<Item=&'a str>, target_bucket: usize) -> Result<u8, QueryParseError<V::Error>> {
        let mut count = 0;
        while let Some(token) = query.next() {
            let Some(last_char) = token.chars().last() else {
                return Err(QueryParseError::EmptyToken);
            };
            if last_char=='{' {
                let mut min=None;
                let mut max=0;
            }
        }
        Ok(count)
    }
    fn validate(&self, obj: &V::Post) {
    }
}
