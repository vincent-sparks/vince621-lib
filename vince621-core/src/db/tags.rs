use std::collections::{BinaryHeap, HashMap};

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
        de.deserialize_string(YarnVisitor)
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
#[derive(strum::EnumString, num_derive::FromPrimitive, serde::Serialize, Debug, Clone, Copy, Eq, PartialEq)]
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


#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub struct Tag {
    #[serde(with="byteyarn_serialize")]
    pub name: Yarn,
    pub id: u32,
    pub category: TagCategory,
    // for some reason that is completely beyond me, some of the tags have negative post counts
    // no one has vacuumed the e621 database in a while and it *really shows*
    pub post_count: i32,
}

impl Tag {
    // this is currently used by the unit tests which are not always compiled
    #[allow(unused)]
    pub(crate) fn new_debug(name: &'static str, id: u32) -> Self {
        Self {
            name: Yarn::from_static(name),
            id,
            category: TagCategory::General,
            post_count: 1,
        }
    }
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
    /**
     * Create a new tag database given a list of tags, which is not assumed to be sorted.
     */
    pub fn new(mut tags: Box<[Tag]>) -> Self {
        tags.sort_unstable_by(|t1,t2| t1.name.cmp(&t2.name));
        #[cfg(feature="phf")]
        let map = phf_generator::generate_hash(unsafe { std::mem::transmute::<&[Tag], &[phfwrapper::TagHashWrapper]>(&tags) });
        Self {
            tags,
            #[cfg(feature="phf")] map,
        }
    }

    /**
     * This method only exists for use by the serialization machinery.  You should not use it
     * directly.
     */
    #[cfg(feature="phf")]
    pub fn from_phf(tags: Box<[Tag]>, map: phf_generator::HashState) -> Self {
        Self {tags, map}
    }

    /**
     * This method only exists for use by the serialization machinery.  You should not use it
     * directly.
     */
    #[cfg(feature="phf")]
    pub fn get_phf(&self) -> &phf_generator::HashState {
        &self.map
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
    pub fn get_as_index(&self, name: &str) -> Option<usize> {
        #[cfg(feature="phf")] {
            let hash = phf_shared::hash(name, &self.map.key);
            let idx = phf_shared::get_index(&hash, &self.map.disps, self.map.map.len());
            let idx2 = self.map.map[idx as usize];
            let val = &self.tags[idx2 as usize];
            if val.name.as_str() == name {Some(idx2)} else {None}
        }
        #[cfg(not(feature="phf"))]
        self.tags.binary_search_by(|x| x.name.as_str().cmp(name)).ok()
    }
    pub fn search_raw<'a,'b>(&'a self, name: &'b str) -> impl Iterator<Item=&'a Tag> + 'b where 'a:'b {
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
                            my_tag = &my_tag[my_tag.ceil_char_boundary(1)..];
                        },
                        b'*' => {
                            my_query = &my_query[1..];
                            let next_pos = my_query.find(['*','?']);
                            let next_text = next_pos.map(|pos| &my_query[..pos]).unwrap_or(my_query);
                            if let Some(text_pos) = my_tag.find(next_text) {
                                my_tag = &my_tag[text_pos+next_text.len()..];
                                match next_pos {
                                    Some(pos) => {
                                        my_query=&my_query[pos..];
                                    },
                                    None => {
                                        // we matched everything after the asterisk, and there
                                        // are no more wildcards in the query.  if there's
                                        // more data in the tag after the text we matched,
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
                                // compare using byte slices rather than string slices, in case
                                // next_pos happens to not fall on a char boundary when indexed
                                // into my_tag.
                                if my_tag.as_bytes()[..next_pos] != my_query.as_bytes()[..next_pos] {return false;}
                                my_tag = &my_tag[next_pos..];
                                my_query = &my_query[next_pos..];
                            } else {
                                // no more wildcards -- check the entire rest of the string
                                return my_tag==my_query;
                            }
                        }
                    }
                }
                return my_tag.is_empty();
            }
            ))
        } else {
            // there are no wildcards anywhere in the string.  this means it is a literal token.
            Either::Right(self.get(query).into_iter())
        }
    }

    pub fn autocomplete(&self, partial: &str, max_results: usize, ignore: &[u32]) -> Vec<&Tag> {
        #[repr(transparent)]
        #[derive(Debug)]
        struct PostCountOrderTagWrapper<'a>(&'a Tag);
        impl Ord for PostCountOrderTagWrapper<'_> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                // into_sorted_vec() defaults to sorting in ascending order.
                // we want to sort in descending order by post count, so we reverse the comparison.
                other.0.post_count.cmp(&self.0.post_count)
            }
        }
        impl Eq for PostCountOrderTagWrapper<'_> {}
        impl PartialOrd for PostCountOrderTagWrapper<'_> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl PartialEq for PostCountOrderTagWrapper<'_> {
            fn eq(&self,other:&Self)->bool{self.0.post_count==other.0.post_count}
        }


        let mut res = BinaryHeap::with_capacity(max_results.saturating_add(1));
        for tag in self.search_raw(partial) {
            if !ignore.contains(&tag.id) {
                res.push(PostCountOrderTagWrapper(tag));
                if res.len() > max_results {
                    res.pop();
                }
            }
        }
        
        // I'm tempted to do a transmute here, but the nomicon says that might be UB
        // and this will get optimized away anyway.
        res.into_sorted_vec().into_iter().map(|x|x.0).collect()
    }
}

// now this is where OOP style inheritance would come in *real* handy
// alas
pub struct TagAndImplicationDatabase {
    pub tags: TagDatabase,
    pub implications: HashMap<u32, Vec<u32>>,
    pub aliases: Vec<(Yarn, usize)>,
}

impl TagAndImplicationDatabase {
    pub fn new(tags: TagDatabase, implications: HashMap<u32, Vec<u32>>, mut aliases: Vec<(Yarn, usize)>) -> Self {
//        let mut aliases_sorted = aliases.keys().map(|x| x.as_str() as *const str).collect::<Vec<_>>();
//        aliases_sorted.sort_unstable_by(|a, b| unsafe {a.as_ref().cmp(&b.as_ref())});
        aliases.sort_unstable_by(|a,b| a.0.cmp(&b.0));
        Self {
            tags,
            implications,
            aliases,
        }
    }

    pub fn get(&self, name: &str) -> Option<&Tag> {
        // important: try aliases first
        // some alias names are also names of deprecated tags that are empty
        if let Ok(idx) = self.aliases.binary_search_by(|(k, _)| k.as_str().cmp(name)) {
            let (_, idx2) = &self.aliases[idx];
            return Some(&self.tags.get_all()[*idx2]);
        }
        self.tags.get(name)
    }

    pub fn remove_implied_tags<'a>(&self, tags: &'a [Tag]) -> impl Iterator<Item=&'a Tag> {
        let mut s = std::collections::HashSet::new();
        for tag in tags {
            if let Some(implications) = self.implications.get(&tag.id) {
                implications.iter().for_each(|tag| {s.insert(*tag);});
            }
        }
        tags.iter().filter(move |tag| !s.contains(&tag.id))
    }

    pub fn search_raw_aliases<'a,'b>(&'a self, name: &'b str) -> impl Iterator<Item=(&'a Tag, &'a str)> + 'b where 'a:'b {
        // TODO sort the aliases by name so we can do the cool optimization we did earlier.
        let pos = self.aliases.binary_search_by(|(k, _)| k.as_str().cmp(name)).unwrap_or_else(|x| x);
        
        self.aliases[pos..].iter().take_while(move |(k, _)| k.as_str().starts_with(name)).map(move |(s, idx)| (&self.tags.get_all()[*idx], s.as_str()))
    }

    /**
     *
     * Retrieve a list of tags that begin with the given prefix, including aliases.
     *
     * Unlike on e621.net, tag names always sort before alias names, so if you start typing the
     * name of an actual tag, the entire space won't get cluttered up by aliases for things you
     * aren't looking for which of course have much higher post counts.
     * *cough* whyhasn'te621fixedthis *cough*
     *
     * The "ignore" argument specifies IDs of tags that should not be returned even if they would
     * otherwise match.  You can use this, for example, to avoid showing autocomplete results for
     * tags that the user has already typed into the current query.
     */
    pub fn autocomplete<'a>(&'a self, partial: &str, max_results: usize, ignore: &[u32]) -> Vec<(&'a Tag, Option<&'a str>)> {

        #[derive(Debug)]
        struct PostCountOrderTagWrapper<'a>(&'a Tag, &'a str);
        impl Ord for PostCountOrderTagWrapper<'_> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                // into_sorted_vec() defaults to sorting in ascending order.
                // we want to sort in descending order by post count, so we reverse the comparison.
                other.0.post_count.cmp(&self.0.post_count)
            }
        }
        impl Eq for PostCountOrderTagWrapper<'_> {}
        impl PartialOrd for PostCountOrderTagWrapper<'_> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl PartialEq for PostCountOrderTagWrapper<'_> {
            fn eq(&self,other:&Self)->bool{self.0.post_count==other.0.post_count}
        }

        let mut v = self.tags.autocomplete(partial, max_results, ignore).into_iter().map(|x| (x, None)).collect::<Vec<(&'a Tag, Option<&'a str>)>>();
        let offset = v.partition_point(|(tag, _)| tag.post_count!=0);
        if offset < max_results {
            let mut rest = v[offset..].iter().copied().collect::<Vec<_>>();
            v.truncate(offset);
            let remaining = max_results - v.len();
            
            let mut collector = BinaryHeap::with_capacity(remaining.saturating_add(1));
            for (tag, alias) in self.search_raw_aliases(partial) {
                if !ignore.contains(&tag.id) {
                    collector.push(PostCountOrderTagWrapper(tag, alias));
                    if collector.len() > remaining {
                        collector.pop();
                    }
                }
            }
            let more_tags = collector.into_sorted_vec();
            v.extend(more_tags.into_iter().map(|a| (a.0, Some(a.1))));
            if v.len() < max_results {
                rest.truncate(max_results-v.len());
                v.append(&mut rest);
            }
        }
        v
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_wildcards() {
        let tag_db = TagDatabase::new([
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
        ].into());

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

        let responses: Vec<&str> = tag_db.search_wildcard("abc_*fg*").map(|x|x.name.as_str()).collect();
        assert_eq!(responses, vec!["abc_defg", "abc_fghj"]);

        let responses: Vec<&str> = tag_db.search_wildcard("abc?").map(|x|x.name.as_str()).collect();
        assert_eq!(responses, Vec::<&str>::new());
    }

    #[test]
    fn test_autocomplete() {
        let tag_db = TagDatabase::new([
            Tag {
                category: TagCategory::General,
                id: 1,
                name: Yarn::from_static("abd"),
                post_count: 5,
            },
            Tag {
                category: TagCategory::General,
                id: 2,
                name: Yarn::from_static("abc"),
                post_count: 4,
            },
            Tag {
                category: TagCategory::General,
                id: 3,
                name: Yarn::from_static("abeq"),
                post_count: 6,
            },
            Tag {
                category: TagCategory::General,
                id: 4,
                name: Yarn::from_static("ac"),
                post_count: 10,
            },
            Tag {
                category: TagCategory::General,
                id: 5,
                name: Yarn::from_static("abz_fghj"),
                post_count: 0,
            },
            Tag {
                category: TagCategory::General,
                id: 6,
                name: Yarn::from_static("abef"),
                post_count: 1,
            },
        ].into());

        assert_eq!(tag_db.autocomplete("ab", 6, &[]).iter().map(|i|i.id).collect::<Vec<_>>(), [3,1,2,6,5]);
        assert_eq!(tag_db.autocomplete("ab", 4, &[]).iter().map(|i|i.id).collect::<Vec<_>>(), [3,1,2,6]);
        assert_eq!(tag_db.autocomplete("ab", 3, &[]).iter().map(|i|i.id).collect::<Vec<_>>(), [3,1,2]);
        assert_eq!(tag_db.autocomplete("ab", 2, &[]).iter().map(|i|i.id).collect::<Vec<_>>(), [3,1]);

        let ac = tag_db.get_as_index("ac").unwrap();

        let tag_and_alias_db = TagAndImplicationDatabase::new(tag_db, HashMap::new(), vec![
            (Yarn::from_static("abq"),ac),
        ]);

        assert_eq!(tag_and_alias_db.autocomplete("ab", 6, &[]).iter().map(|i|i.0.id).collect::<Vec<_>>(), [3,1,2,6,4,5]);
        assert_eq!(tag_and_alias_db.autocomplete("ab", 5, &[]).iter().map(|i|i.0.id).collect::<Vec<_>>(), [3,1,2,6,4]);
        assert_eq!(tag_and_alias_db.autocomplete("ab", 4, &[]).iter().map(|i|i.0.id).collect::<Vec<_>>(), [3,1,2,6]);
        assert_eq!(tag_and_alias_db.autocomplete("ab", 3, &[]).iter().map(|i|i.0.id).collect::<Vec<_>>(), [3,1,2]);

        let abd = tag_and_alias_db.tags.get_as_index("abd").unwrap();
        let abc = tag_and_alias_db.tags.get_as_index("abc").unwrap();

        let tag_and_alias_db = TagAndImplicationDatabase::new(tag_and_alias_db.tags, HashMap::new(), vec![
            (Yarn::from_static("ace"),abd),
            (Yarn::from_static("aca"),abc),
        ]);

        assert_eq!(tag_and_alias_db.autocomplete("ac", 6, &[]).iter().map(|i|i.0.id).collect::<Vec<_>>(), [4,1,2]);
        assert_eq!(tag_and_alias_db.autocomplete("ac", 2, &[]).iter().map(|i|i.0.id).collect::<Vec<_>>(), [4,1]);

    }
}
