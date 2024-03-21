use varint_rs::{VarintReader,VarintWriter};

use vince621_core::db::posts::Post;

pub mod strings;

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

pub fn serialize_post_database(posts: &[Post], out: &mut impl std::io::Write) -> Result<(), std::io::Error> {
    out.write_usize_varint(posts.len())?;
    for post in posts {
        out.write_u32_varint(post.id.get())?;
        out.write_all(&post.md5)?;
        let rating_and_extension = (post.rating as u8) | ((post.file_ext as u8) << 2);
        out.write_all(&[rating_and_extension])?;
        out.write_u32_varint(post.fav_count)?;
        out.write_i32_varint(post.score)?;
        out.write_u32_varint(post.parent_id.map(|x|x.get()).unwrap_or(0))?;
        serialize_tag_list(&post.tags, &mut *out)?;
    }
    
    Ok(())
}
