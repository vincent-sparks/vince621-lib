use std::num::NonZeroU32;

use chrono::{DateTime, Utc};

use super::posts::{Post, PostDatabase};

#[derive(Debug,PartialEq)]
pub enum PoolCategory {
    Series, Collection
}

pub struct Pool {
    pub id: NonZeroU32,
    pub name: Box<str>,
    pub description: Option<Box<str>>,
    pub category: PoolCategory,
    pub is_active: bool,
    pub last_updated: DateTime<Utc>,
    pub post_ids: Box<[NonZeroU32]>,
}

impl Pool {
    pub fn iter_posts<'s, 'a>(&'s self, post_db: &'a PostDatabase) -> PoolPostsIter<'s, 'a> {
        PoolPostsIter {db: post_db.get_all(), post_ids: self.post_ids.iter(), last_post_id: 0, last_idx: 0}
    }

    #[allow(unused)]
    pub(crate) fn debug_default(post_ids: impl IntoIterator<Item=u32>) -> Pool {
        Pool {
            id: unsafe {NonZeroU32::new_unchecked(1)},
            name: Box::from(""),
            description: None,
            category: PoolCategory::Series,
            is_active: false,
            last_updated: DateTime::UNIX_EPOCH,
            post_ids: post_ids.into_iter().map(|id| NonZeroU32::new(id).unwrap()).collect(),
        }
    }
}

pub struct PoolPostsIter<'pool, 'db> {
    db: &'db [Post],
    post_ids: std::slice::Iter<'pool, NonZeroU32>,
    last_post_id: u32,
    last_idx: usize,
}

impl<'pool, 'db> Iterator for PoolPostsIter<'pool, 'db> {
    type Item=&'db Post;

    fn next(&mut self) -> Option<&'db Post> {
        let id = *self.post_ids.next()?;
        let Some((idx, post)) = 
        (if self.last_post_id < id.get() {
            let remaining_posts = &self.db[self.last_idx..];
            let increment_option = if id.get() < self.last_post_id + 10 {
                // optimization: most pools contain posts with IDs that are either sequential or close to
                // it.  In that case, a linear search starting from the position of the last post will
                // almost certainly complete in only one or two comparisons, which will be faster than a
                // binary search.
                //
                // side note, loops are passé.  all the cool kids write their array searches using
                // combinators.
                self.db[self.last_idx..].iter()
                    .enumerate()
                    .filter(|(_,post)| post.id==id)
                    .next()
            } else {
                // even if they're not, most pools' post IDs are still in ascending order, meaning we
                // can narrow the binary search to posts after ones we've already seen.
                self.db[self.last_idx..]
                    .binary_search_by(|post| post.id.cmp(&id))
                    .ok()
                    .map(|idx| (idx, &remaining_posts[idx]))
            };
            increment_option.map(|(idx, post)| (self.last_idx + idx, post))
        } else {
            // if all else fails, fall back to a full binary search.
            self.db.binary_search_by(|post| post.id.cmp(&id)).ok().map(|idx| (idx, &self.db[idx]))
        }) else {
            // post was not in the database.  skip it and loop back to the top by tail recursing.
            return self.next();
        };
        debug_assert!(std::ptr::eq(post, &self.db[idx]));
        debug_assert!(post.id == id);
        self.last_post_id = id.get();
        self.last_idx = idx;
        Some(post)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.post_ids.size_hint().1)
    }
}

/*
impl ExactSizeIterator for PoolPostsIter<'_,'_> {
    fn len(&self) -> usize {
        self.post_ids.len()
    }
}
*/

pub struct PoolDatabase {
    pools: Box<[Pool]>,
}

impl PoolDatabase {
    pub fn new(pools: Box<[Pool]>) -> Self {
        Self{pools}
    }
    pub fn get_all(&self)->&[Pool] {
        &self.pools
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn post_with_id(id: u32) -> Post {
        Post {
            id: unsafe {NonZeroU32::new_unchecked(id)},
            ..Post::debug_default()
        }
    }
    #[test]
    fn test_pool_posts_iter() {
        let posts = &[
            post_with_id(1),
            post_with_id(2),
            post_with_id(3),
            post_with_id(20),
            post_with_id(50),
        ];
        let post_ids: Vec<NonZeroU32> = vec![1u32,2,2,20,9999,50,3].into_iter().filter_map(NonZeroU32::new).collect();
        let res: Vec<&Post> = PoolPostsIter {
            db: posts,
            last_idx: 0,
            last_post_id: 0,
            post_ids: post_ids.iter(),
        }.collect();
        assert_eq!(res, vec![&posts[0], &posts[1], &posts[1], &posts[3], &posts[4], &posts[2]]);
    }
}
