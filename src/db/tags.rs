use byteyarn::Yarn;
use either::Either;
use num_traits::FromPrimitive;

mod byteyarn_serialize {
    use byteyarn::Yarn;
    use serde::{Serialize, Serializer, Deserializer};
    pub fn serialize<S: Serializer>(yarn: &Yarn, serializer: S) -> Result<S::Ok,S::Error> {
        yarn.as_str().serialize(serializer)
    }
    struct YarnVisitor;
    impl<'de> serde::de::Visitor<'de> for YarnVisitor {
        type Value = Yarn;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string")
        }

        fn visit_str<E>(self, s: &str) -> Result<Yarn, E> where E: serde::de::Error {
            Ok(Yarn::copy(s))
        }
        fn visit_string<E>(self, s: String) -> Result<Yarn, E> where E: serde::de::Error {
            Ok(Yarn::from_string(s))
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Yarn, D::Error> {
        de.deserialize_str(YarnVisitor)
    }
}

struct TagCategoryVisitor;

impl serde::de::Visitor<'_> for TagCategoryVisitor {
    type Value=TagCategory;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("an integer or string matching a tag category")
    }

    fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<TagCategory,E> {
        if let Ok(val) = s.parse() {
            return self.visit_u64(val);
        }

        s.parse().map_err(E::custom)
    }

    fn visit_u64<E>(self, val: u64) -> Result<TagCategory,E> where E: serde::de::Error {
        TagCategory::from_u64(val)
            .ok_or_else(|| E::custom("integer value out of range"))
    }
}

impl<'de> serde::Deserialize<'de> for TagCategory {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        de.deserialize_u64(TagCategoryVisitor)
    }
}

#[repr(u8)]
#[derive(strum::EnumString, num_derive::FromPrimitive, serde::Serialize, Debug)]
pub enum TagCategory {
    General=0,
    Artist=1,
    // category 2 does not appear anywhere in the CSV file.
    // what was here?  who knows. until and unless i find out we will panic if we encounter it.
    Copyright=3,
    Character=4,
    Species=5,
    Invalid=6,
    Meta=7,
    Lore=8,
}


#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Tag {
    #[serde(with="byteyarn_serialize")]
    pub name: Yarn,
    pub id: u32,
    pub category: TagCategory,
    // for some reason that is completely beyond me, some of the tags have negative post counts
    // no one has vacuumed the e621 database in a while and it *really shows*
    pub post_count: i32,
}

pub struct TagDatabase {
    tags: Box<[Tag]>,
    #[cfg(feature="phf")]
    map: phf_generator::HashState,
}

#[cfg(feature="phf")]
mod phfwrapper {
    use super::Tag;
    use phf_shared::PhfHash;
    #[repr(transparent)]
    pub(crate) struct TagHashWrapper(Tag);

    impl PhfHash for TagHashWrapper {
        fn phf_hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.0.name.as_str().phf_hash(state)
        }
    }
}

impl TagDatabase {
    pub(crate) fn new(mut tags: Vec<Tag>) -> Self {
        tags.sort_unstable_by(|t1,t2| t1.name.cmp(&t2.name));
        #[cfg(feature="phf")]
        let map = phf_generator::generate_hash(unsafe { std::mem::transmute::<&[Tag], &[phfwrapper::TagHashWrapper]>(&tags) });
        Self {
            tags: tags.into_boxed_slice(),
            #[cfg(feature="phf")] map,
        }
    }
    pub fn get_all(&self) -> &[Tag] {
        &self.tags
    }
    pub fn get(&self, name: &str) -> Option<&Tag> {
        #[cfg(feature="phf")] {
            let hash = phf_shared::hash(name, &self.map.key);
            let idx = phf_shared::get_index(&hash, &self.map.disps, self.map.map.len());
            let idx2 = self.map.map[idx as usize];
            let val = &self.tags[idx2 as usize];
            if val.name.as_str() == name {Some(val)} else {None}
        }
        #[cfg(not(feature="phf"))]
        self.tags.binary_search_by(|x| x.name.as_str().cmp(name)).ok().map(|x|&self.tags[x])
    }
    pub fn search_raw<'a>(&'a self, name: &'a str) -> impl Iterator<Item=&'a Tag> + 'a {
        let pos = match self.tags.binary_search_by(|tag| tag.name.as_str().cmp(name)) {
            Ok(x) => x,
            Err(x) => x,
        };
        self.tags[pos..].iter().take_while(move |x| x.name.as_str().starts_with(name))
    }
    pub fn search_wildcard<'a>(&'a self, query: &'a str) -> impl Iterator<Item=&'a Tag> + 'a {
        if let Some(pos) = query.find(['*','?']) {
            Either::Left(self.search_raw(&query[..pos]).filter(move |tag| {
                let mut my_query = &query[pos..];
                let mut my_tag = &tag.name.as_str()[pos..];
                while !my_query.is_empty() {
                    match my_query.as_bytes()[0] {
                        b'?' => {
                            my_query = &my_query[1..];
                            if my_tag.len() == 0 {return false;}
                            my_tag = &my_tag[1..];
                        },
                        b'*' => {
                            my_query = &my_query[1..];
                            let next_pos = my_query.find(['*','?']);
                            let next_text = next_pos.map(|pos| &my_query[..pos]).unwrap_or(my_query);
                            dbg!(&next_text);
                            if let Some(text_pos) = my_tag.find(next_text) {
                                my_tag = &my_tag[text_pos+next_text.len()..];
                                match next_pos {
                                    Some(pos) => {
                                        my_query=&my_query[pos..];
                                    },
                                    None => {
                                        // we matched everything after the asterisk, and there
                                        // are no more wildcards in the query.  if there's
                                        // more data in the input after the text we matched,
                                        // we fail -- the user would've added another * at the end
                                        // of the query if they meant to include that.  if the
                                        // query did end with an *, next_text (the text after the
                                        // asterisk we found) will be empty, and we should succeed.
                                        return my_tag.is_empty() || next_text.is_empty();
                                    },
                                }
                            } else {
                                return false;
                            }
                        },
                        _ => {
                            if let Some(next_pos) = my_query.find(['*','?']) {
                                if my_tag.len() < next_pos {return false;}
                                if my_tag[..next_pos] != my_query[..next_pos] {return false;}
                                my_tag = &my_tag[next_pos..];
                                my_query = &my_query[next_pos..];
                            } else {
                                // no more wildcards -- check the entire rest of the string
                                return my_tag==my_query;
                            }
                        }
                    }
                }
                return true;
            }
            ))
        } else {
            // there are no wildcards anywhere in the string.  we can optimize.
            Either::Right(self.search_raw(&query))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_wildcards() {
        let tag_db = TagDatabase::new(vec![
            Tag {
                category: TagCategory::General,
                id: 1,
                name: Yarn::from_static("abc_defg"),
                post_count: 0,
            },
            Tag {
                category: TagCategory::General,
                id: 2,
                name: Yarn::from_static("abc_fghj"),
                post_count: 0,
            },
            Tag {
                category: TagCategory::General,
                id: 3,
                name: Yarn::from_static("abd_defg"),
                post_count: 0,
            },
            Tag {
                category: TagCategory::General,
                id: 3,
                name: Yarn::from_static("abd_fghj"),
                post_count: 0,
            },
        ]);

        let responses: Vec<&str> = tag_db.search_wildcard("abc*").map(|x|x.name.as_str()).collect();
        assert_eq!(responses, vec!["abc_defg", "abc_fghj"]);

        let responses: Vec<&str> = tag_db.search_wildcard("ab?_defg").map(|x|x.name.as_str()).collect();
        assert_eq!(responses, vec!["abc_defg", "abd_defg"]);

        let responses: Vec<&str> = tag_db.search_wildcard("ab?_def?").map(|x|x.name.as_str()).collect();
        assert_eq!(responses, vec!["abc_defg", "abd_defg"]);

        let responses: Vec<&str> = tag_db.search_wildcard("ab?_defg?").map(|x|x.name.as_str()).collect();
        assert_eq!(responses, Vec::<&str>::new());

        let responses: Vec<&str> = tag_db.search_wildcard("abc*fg").map(|x|x.name.as_str()).collect();
        assert_eq!(responses, vec!["abc_defg"]);

        let responses: Vec<&str> = tag_db.search_wildcard("abc*fg*").map(|x|x.name.as_str()).collect();
        assert_eq!(responses, vec!["abc_defg", "abc_fghj"]);
    }
}
