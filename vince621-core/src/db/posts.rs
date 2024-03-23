use std::{num::NonZeroU32, str::FromStr};

use hex::ToHex;



#[derive(Debug, serde::Serialize,serde::Deserialize, strum::Display,Clone,Copy, num_derive::FromPrimitive,Eq,PartialEq)]
#[serde(rename_all="lowercase")]
#[strum(serialize_all="lowercase")]
#[repr(u8)]
pub enum FileExtension {
    JPG,PNG,GIF,WEBM,SWF
}

pub struct BadRating;

impl FromStr for Rating {
    type Err=BadRating;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "s" | "safe" => Ok(Rating::Safe),
            "q" | "questionable" => Ok(Rating::Safe),
            "e" | "epxlicit" => Ok(Rating::Safe),
            _ => Err(BadRating),
        }
    }
}

impl<'de> serde::Deserialize<'de> for Rating {
    fn deserialize<D:serde::Deserializer<'de>>(de: D) -> Result<Rating, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value=Rating;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("one of \"safe\", \"questionable\", \"explicit\", \"s\", \"q\", or \"e\"")
            }
            fn visit_str<E: serde::de::Error>(self, s:&str)->Result<Rating,E>{
                s.parse().map_err(|_| E::invalid_value(serde::de::Unexpected::Str(s), &self))
            }
        }
        de.deserialize_str(Visitor)
    }
}

#[derive(Debug,serde::Serialize,Clone,Copy, num_derive::FromPrimitive,Eq,PartialEq)]
#[repr(u8)]
pub enum Rating {
    Safe, Questionable, Explicit
}

#[derive(Debug,serde::Serialize,PartialEq)]
pub struct Post {
    pub id: NonZeroU32,
    pub rating: Rating,
    //pub description: Box<str>,
    pub fav_count: u32,
    pub score: i32,
    pub md5: [u8;16], // not storing this as a string.  we have four million of these.  every byte counts!
    pub file_ext: FileExtension,
    pub tags: Box<[u32]>,
    pub parent_id: Option<NonZeroU32>,
}

impl Post {
    pub(crate) fn debug_default() -> Self {
        Self {
            id: unsafe {NonZeroU32::new_unchecked(u32::MAX)},
            rating: Rating::Safe,
            //description: Box::from(""),
            fav_count: 0,
            score: 0,
            md5: [0;16],
            file_ext: FileExtension::PNG,
            tags: Box::from([]),
            parent_id: None,
        }
    }
    pub fn url(&self) -> String {
        format!("https://static1.e621.net/data/{:02x}/{:02x}/{}.{}", self.md5[0],self.md5[1], self.md5.encode_hex::<String>(), self.file_ext)
    }
}

pub struct PostDatabase {
    posts: Box<[Post]>,
}

impl PostDatabase {
    /**
     * Create a new post database given a list of posts loaded from somewhere.  This list is
     * assumed to already be sorted by ID.
     */
    pub fn new(posts: Box<[Post]>) -> Self {
        Self {posts}
    }
    
    pub fn get_all(&self) -> &[Post] {
        &self.posts
    }
}
