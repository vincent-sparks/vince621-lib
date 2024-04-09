use std::{io::Read, num::{NonZeroU16, NonZeroU32}};

use vince621_core::db::{posts::{FileExtension, Post, PostDatabase, Rating}, tags::TagDatabase};


/// Some of the posts in the e621 database, speicifically a handful of flash files,
/// have negative resolutions.
/// I've remarked several times in the comments in this codebase that the e621 database is not
/// very well maintained, but that one really takes the cake.  Especially since I wasn't aware
/// Flash files even *had* a resolution.  (Maybe that's why.)
fn skip_if_negative<'d, D:serde::de::Deserializer<'d>>(d:D) ->Result<u16,D::Error> {
    struct Visitor;
    impl<'de> serde::de::Visitor<'de> for Visitor {
        type Value=u16;
        fn expecting(&self,fmt:&mut std::fmt::Formatter)->std::fmt::Result{
            fmt.write_str("a possibly-negative u16")
        }
        fn visit_str<E: serde::de::Error>(self, s:&str)->Result<u16,E> {
            if s.starts_with('-') {
                Ok(0)
            } else {
                s.parse().map_err(E::custom)
            }
        }
    }
    d.deserialize_str(Visitor)
}

#[derive(serde::Deserialize)]
struct CSVPost {
    id: NonZeroU32,
    parent_id: Option<NonZeroU32>,
    //description: Box<str>,
    fav_count: u32,
    rating: Rating,
    score: i32,
    file_ext: FileExtension,
    #[serde(deserialize_with="hex::serde::deserialize")]
    md5: [u8;16],
    #[serde(deserialize_with="crate::util::t_or_f")]
    is_deleted: bool,

    #[serde(deserialize_with="skip_if_negative")]
    image_width: u16,
    #[serde(deserialize_with="skip_if_negative")]
    image_height: u16,
}

pub fn load_post_database<R: Read>(tag_db: &TagDatabase, mut rdr: csv::Reader<R>) -> csv::Result<PostDatabase> {
    let post_parse_start=std::time::Instant::now();
    let mut posts: Vec<Post> = Vec::new();
    // I really shouldn't have to clone this, but the borrow checker has forced my hand.
    // If I *really* cared about performance, I could use a raw pointer here, sidestepping the
    // borrow checker entirely, but getting yelled at by the entire Rust community for using an
    // `unsafe` block is not worth the microseconds of compute time that would save.
    let headers = rdr.headers()?.clone();
    let tag_string_idx = headers.iter().position(|header| header=="tag_string").unwrap();
    let mut row = csv::StringRecord::new();
    while rdr.read_record(&mut row)? {
        let post: CSVPost = row.deserialize(Some(&headers))?;
        if post.is_deleted {continue;}
        let tag_string = row.get(tag_string_idx).unwrap();
        let mut tags: Vec<u32> = Vec::new();
        for tag in tag_string.split(' ') {
            if let Some(tag) = tag_db.get(tag) {
                tags.push(tag.id);
            }
        }
        tags.sort_unstable();
        posts.push(Post {
            id: post.id,
            parent_id: post.parent_id,
            //description: post.description,
            fav_count: post.fav_count,
            rating: post.rating,
            score: post.score,
            file_ext: post.file_ext,
            md5: post.md5,
            tags: tags.into(),
            height: post.image_height,
            width: post.image_width,
        });
    }
    dbg!(post_parse_start.elapsed());
    Ok(PostDatabase::new(posts.into_boxed_slice()))
}
