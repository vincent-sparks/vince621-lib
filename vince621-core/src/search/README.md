So.  The nested query algorithm.
=

e621, like every booru in existence, lets you search for posts by tag.  Each post can have an arbitrary number of tags, which are identified by strings, and a post either has a tag or it doesn't (no two of the same tag, for example, and order doesn't matter).  Most boorus just let you enter a bunch of tags and show you posts with all the tags you specify.  e621 goes a tiny bit beyond that and supports the following advanced search features:

* Tags in the query are separated by spaces
* If a tag is prefixed with a -, the query will match only posts that do NOT contain that tag
* If several tags in the query are prefixed with a ~, the query will match posts that contain one or more of those tags

That's it.  There's no way to search for one of these two but not both, or one or more of these AND one or more of those.

I thought I could do better.  So I did.


## A bit of history

Way back in the misty days of 2018, when I was working on the first version of Botty McBotface (an allegedly multi-function Discord bot that had roughly 70% of its total development effort concentrated on the e621 search functionality), I came up with an idea for a better search system, which I called ctag (short for "complex tag").  You could write a search string like `1{ dog cat } 2-{ insert your favorite tags here }` and it would match any post with exactly one of "dog" and "cat", and two or more of "insert", "your", "favorite", "tags", and "here".  The complete grammar was (and still is) as follows:


(TODO figure out how to write EBNF)




I wrote up a quick, inefficient parser for these tags that would reparse the entire query for every post it checked (in my defense, I was 16 and hornier than I have ever been at any point since, and writing that software was the only thing standing between me and resolving that -- computational efficiency was not my primary focus).  Unfortunately, that bot used the e621 API, which meant that the only way it could obtain a list of posts to check against was by compiling a list of all tags the query checked against (the union of all buckets) and running an e621 search for _all of them_ prefixed with a ~.  The number of posts this returned, combined with the maximum search page size of 999 and e621's API ratelimit of 1 call per second, meant that even at the maximum allowable search page size, I couldn't get even simple queries to return a single result within 30 seconds.  I wondered, idly, if there was a way to download a list of all posts so I could search through them at my leisure, but there didn't seem to be one, so I gave up on the idea and went back to making it display comments by using a webhook to customize the bot's username and avatar for each message to match the e621 user and allowing people (all one of my bot's users) to vote on/favorite posts and comments by leaving a thumbs-up react on the message.  (That bot was feature crept to hell and back.)

### The second version

A few years later, a much more mature version of me with significantly more experience both in programming and in being a furry was exploring the e621 site and stumbled across the database export section.  There it was, the answer to my five-year-old prayers -- every post on e621 along with image URLs and lists of tags, all packaged up nicely in one gigabyte of gzipped CSV.  I immediately revisited the ctag idea, this time building a compiler that parsed the ctag string once and returned a struct that could be used to quickly and efficiently check whether a given set of tags matched.

_NOTE: To explain the inner workings of the current version of my software, I think it prudent to first explain the inner workings of a previous version with fewer features and less architectural complexity.  This will give you a good idea of the basic principle on which the parser works, after which I will explain the new features and new complexity in the current version.  Keep in mind that the architecture described in the following section is not fully reflective of the architecture in this repository._

I split the query on spaces (or, more precisely, let the OS do it for me by iterating over command line arguments) and iterate through the tokens with a recursive parser that builds a list of "buckets".  In this version, each bucket exactly corresponded to one `{ }` grouping.  Each bucket is a struct with a min, a max, and a target.  The min and max are simply the values specified next to the open curly brace (`1-{` type tokens had their "max" filled with 0 which was then replaced by an actual count at the end of the recursive call) and the "target" was the index of a bucket in the list that was less than the index of the bucket the field was on.  More on what these fields do in a moment.

```rust
struct Bucket {
    min: Option<u32>,
    max: Option<u32>,
    target: usize,
}

fn parse(input: impl Iterator<Item=String>) Result<CompiledQuery, ParseError> {
    let output_tags: Vec<(String, usize)> = Vec::new();
    let output_buckets: Vec<Bucket> = Vec::new();
    output_buckets.push(Bucket {min: None, max: None, target: 0}); // doesn't matter what we set as the target for the root bucket -- it will never fill anything.
    // target_bucket = 0 (the root bucket)
    // expect_eof = true (terminate on end of stream rather than on close curly brace)
    parse_inner(&mut input, &mut output_tags, &mut output_buckets, 0, true);    


}

fn parse_inner(input: &mut impl Iterator<Item=String>, output_tags: &mut Vec<(String, usize)>, output_buckets: &mut Vec<Bucket>, target_bucket: usize, expect_eof: bool) -> Result<u32, ParseError> {
    let mut count = 0;
    while let Some(token) = input.next() {
        if token == "}" {
            if !expect_eof {
                return Ok(count);
            } else {
                return Err(ParseError::UnmatchedCloseCurly);
            }
        } else if token.ends_with('{') {
            let (min, max) = parse_range(token)?;
            let new_bucket_idx = output_buckets.len();
            output_buckets.push(Bucket {min, max, target: target_bucket});
            // recursively call our parse function, telling it to place its output into our newly created bucket.
            let sub_count = parse_inner(&mut *input, &mut *output_tags, &mut *output_buckets, new_bucket_idx, false)?;
            // if either of the two buckets were None, insert the final count of the recursive call into them.
            if buckets[new_bucket_idx].min.is_none() {
                buckets[new_bucket_idx].min = Some(sub_count);
            }
            if buckets[new_bucket_idx].max.is_none() {
                buckets[new_bucket_idx].max = Some(sub_count);
            }
            count += 1;
        } else {
            // assume anything else is a tag name
            output_tags.push((token, target_bucket));
            count += 1;
        }
    }
    // if we get here, the iterator has been exhausted.
    // check to make sure we're the outermost recursive call (i.e. we're the root bucket).
    if expect_eof {
        return Ok(count);
    } else {
        return Err(ParseError::UnmatchedOpenCurly);
    }
}

fn parse_range(input: &str) -> Result<(Option<u32>, Option<u32>), ParseError> {
    // I've omitted the definition of parse_range() since it's not very interesting and it's easier to just describe it semantically.
    // given "1{" it returns Ok((Some(1), Some(1)))
    // given "1-2{" it returns Ok((Some(1), Some(2)))
    // given "1-{" it returns Ok((Some(1), None))
    // given the exact string "all{" it returns Ok((None, None))
    // given a string of any other format it returns Err(ParseError::BadRange)
}
```

The parse function takes three arguments: a mutable reference to the token stream (to note which tokens have been consumed), a mutable reference to the output list, and the index of a target bucket

At the start of each recursive call to my parse function, I add a new bucket to the list.  If the next token in the stream appears to be a tag (does not end with a `{` or `}`), I add its name and the target bucket I was passed as an argument
