use std::num::{NonZeroU32};

use num_traits::FromPrimitive as _;
use varint_rs::{VarintReader,VarintWriter};

use vince621_core::db::{posts::{FileExtension, Post, PostDatabase, Rating}, tags::TagDatabase};

fn serialize_tag_list<T: VarintWriter>(tags: &[u32], out: &mut T) -> Result<(),T::Error> {
    out.write_usize_varint(tags.len())?;
    if tags.is_empty() {
        return Ok(());
    }
    let mut last = tags[0];
    out.write_u32_varint(last)?;
    for tag in &tags[1..] {
        out.write_u32_varint(tag-last-1)?;
        last=*tag;
    }

    Ok(())
}

fn deserialize_tag_list<T: VarintReader>(mut input: T) -> Result<Vec<u32>, T::Error> {
    let n = input.read_usize_varint()?;
    let mut output = Vec::with_capacity(n);
    if n==0 {
        return Ok(output);
    }
    let mut val = input.read_u32_varint()?;
    output.push(val);
    for _ in 1..n {
        val += input.read_u32_varint()? + 1;
        output.push(val);
    }
    Ok(output)
}

struct VecWrapper(Vec<u8>);
impl VarintWriter for VecWrapper {
    type Error=std::convert::Infallible;
    fn write(&mut self, val: u8) -> Result<(), std::convert::Infallible> {
        self.0.push(val);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_serialization() {
        let v = [1,2,3,5,600,9999];
        let mut out = VecWrapper(Vec::new());
        let a = serialize_tag_list(&v, &mut out);
        match a {
            Ok(()) => (),
            Err(e) => match e {}
        }

        assert_eq!(deserialize_tag_list(std::io::Cursor::new(out.0)).unwrap(), v);

    }
}
fn write_post(post: &Post, prev_id: u32, out: &mut impl std::io::Write) -> Result<(), std::io::Error> {
    out.write_u32_varint(post.id.get() - prev_id)?;
    out.write_all(&post.md5)?;
    let rating_and_extension = (post.rating as u8) | ((post.file_ext as u8) << 2);
    out.write_all(&[rating_and_extension])?;
    out.write_u32_varint(post.fav_count)?;
    out.write_i32_varint(post.score)?;
    out.write_u32_varint(post.parent_id.map(|x|x.get()).unwrap_or(0))?;
    out.write_u16_varint(post.width)?;
    out.write_u16_varint(post.height)?;
    serialize_tag_list(&post.tags, out)
}


pub fn serialize_post_database(posts: &[Post], out: &mut impl std::io::Write) -> Result<(), std::io::Error> {
    out.write_all(b"v621POST")?;
    out.write_usize_varint(posts.len())?;
    let mut last_id = 0;
    for post in posts {
        write_post(&post, last_id, &mut *out)?;
        last_id = post.id.get();
    }
    
    Ok(())
}

pub fn deserialize_post_database(file: &mut impl std::io::Read) -> std::io::Result<PostDatabase> {
    let mut data=[0u8;8];
    file.read_exact(&mut data)?;
    if &data!=b"v621POST" {
        return Err(std::io::Error::other("bad magic number"));
    }

    let count = file.read_usize_varint()?;
    let mut v = Vec::with_capacity(count);

    let mut id = 0u32;
    for _ in 0..count {
        id += file.read_u32_varint()?;
        let mut md5 = [0u8;16];
        file.read_exact(&mut md5)?;
        let mut rating_and_extension = 0u8;
        file.read_exact(std::slice::from_mut(&mut rating_and_extension))?;
        let rating = Rating::from_u8(rating_and_extension & 0x3);
        let file_ext = FileExtension::from_u8(rating_and_extension >> 2);
        let (rating, file_ext) = rating.zip(file_ext)
            .ok_or_else(|| std::io::Error::other(format!("bad rating_and_extension value of {:x} on post #{}", rating_and_extension, id)))?;
        let fav_count = file.read_u32_varint()?;
        let score = file.read_i32_varint()?;
        let parent_id = NonZeroU32::new(file.read_u32_varint()?);
        let width = file.read_u16_varint()?;
        let height = file.read_u16_varint()?;
        let tags = deserialize_tag_list(&mut *file)?;
        v.push(Post {
            id: NonZeroU32::new(id).ok_or_else(|| std::io::Error::other("post had an ID of 0"))?, 
            md5, rating, file_ext, fav_count, score, tags: tags.into(), parent_id, width, height,
        });
    }

    Ok(PostDatabase::new(v.into_boxed_slice()))
}

pub fn serialize_tag_database(file: &mut impl std::io::Write, tag_database: TagDatabase) {
    
}
