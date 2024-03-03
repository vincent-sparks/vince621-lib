use std::{num::NonZeroU32, str::FromStr};



#[derive(Debug, serde::Serialize,serde::Deserialize)]
#[serde(rename_all="lowercase")]
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

#[derive(Debug)]
pub enum Rating {
    Safe, Questionable, Explicit
}

#[derive(Debug)]
pub struct Post {
    pub id: NonZeroU32,
    pub rating: Rating,
    pub description: Box<str>,
    pub fav_count: u32,
    pub score: i32,
    pub md5: [u8;16], // not storing this as a string.  we have four million of these.  every byte counts!
    pub file_ext: FileExtension,
    pub tags: Box<[u32]>,
    pub parent_id: Option<NonZeroU32>,
}

pub struct PostDatabase {
    posts: Box<[Post]>,
}

impl PostDatabase {
    pub(crate) fn new(posts: Box<[Post]>) -> Self {
        Self {posts}
    }
    
    pub fn get_all(&self) -> &[Post] {
        &self.posts
    }
}
