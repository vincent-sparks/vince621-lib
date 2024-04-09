use std::io;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt as _, WriteBytesExt as _};
use num_traits::FromPrimitive as _;
#[cfg(feature="phf")]
use phf_generator::HashState;
use varint_rs::{VarintReader, VarintWriter as _};
use vince621_core::db::tags::{Tag, TagCategory, TagDatabase};

pub fn serialize_tag_database(db: &TagDatabase, file: &mut impl io::Write) -> io::Result<()> {

    file.write_all(b"v621TAGS\x00\x00\x00")?;
    
    #[cfg(feature="phf")]
        file.write_all(b"\x01")?;
    #[cfg(not(feature="phf"))]
    file.write_all(b"\x00")?;
    file.write_usize_varint(db.get_all().len())?;
    #[cfg(feature="phf")]
    {
        let phf = db.get_phf();
        let mut v = Vec::new();
        v.write_u64::<LittleEndian>(phf.key)?;
        v.write_usize_varint(phf.disps.len())?;
        for (a,b) in phf.disps.iter().copied() {
            v.write_u32_varint(a)?;
            v.write_u32_varint(b)?;
        }
        assert_eq!(phf.map.len(), db.get_all().len());
        for i in phf.map.iter().copied() {
            v.write_usize_varint(i)?;
        }
        file.write_usize_varint(v.len())?;
        file.write_all(&v)?;
    }

    for tag in db.get_all() {
        file.write_all(tag.name.as_bytes())?;
        file.write_all(b"\0")?;
        file.write_u32_varint(tag.id)?;
        file.write_u8(tag.category as u8)?;
        file.write_i32_varint(tag.post_count)?;
    }


    Ok(())
}

pub fn deserialize_tag_database(file: &mut impl io::BufRead) -> io::Result<TagDatabase> {
    let mut magic = [0u8;8];
    file.read_exact(&mut magic)?;
    if &magic != b"v621TAGS" {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Bad magic number"));
    }
    let version = file.read_u32::<BigEndian>()?;
    let number_of_tags = file.read_usize_varint()?;
    #[cfg(feature="phf")]
    let phf: Option<HashState>;
    match version {
        0 => {
            #[cfg(feature="phf")] {
                phf = None;
            }
        },
        1 => {
            #[allow(unused)]
            let phf_data_len = file.read_usize_varint()?;
            #[cfg(feature="phf")] {
                let key = file.read_u64::<LittleEndian>()?;
                let disps_count = file.read_usize_varint()?;
                let mut disps = Vec::with_capacity(disps_count);
                for _ in 0..disps_count {
                    let a = file.read_u32_varint()?;
                    let b = file.read_u32_varint()?;
                    disps.push((a,b));
                }
                let mut map = Vec::with_capacity(number_of_tags);
                for _ in 0..number_of_tags {
                    map.push(file.read_usize_varint()?);
                }
                phf = Some(HashState {
                    key, disps, map
                });
            }
            #[cfg(not(feature="phf"))]
            {
                // we don't care about the PHF data -- skip it.
                let mut remaining = phf_data_len;
                while remaining > 0 {
                    let n = file.fill_buf()?.len().min(remaining);
                    file.consume(n);
                    remaining -= n;
                }
            }
        },
        v => return Err(io::Error::new(io::ErrorKind::InvalidData, format!("Unknown version {}", v)))
    }
    let mut tags = Vec::with_capacity(number_of_tags);

    let mut name_buf = Vec::new();
    for _ in 0..number_of_tags {
        file.read_until(b'\0', &mut name_buf)?;
        let idx = name_buf.len()-1;
        if name_buf[idx] != b'\0' {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF in the middle of a tag name"));
        }
        let name = std::str::from_utf8(&name_buf[..idx]).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let id = file.read_u32_varint()?;
        let category = file.read_u8()?;
        let category = TagCategory::from_u8(category).ok_or_else(|| io::Error::new(std::io::ErrorKind::InvalidData, "unknown category ID"))?;
        let post_count = file.read_i32_varint()?;
        tags.push(Tag {
            name: byteyarn::Yarn::copy(name),
            id,
            category,
            post_count,
        });
        name_buf.clear();
    }

    let tags = tags.into_boxed_slice();
    #[cfg(feature="phf")]
    match phf {
        Some(phf) => Ok(TagDatabase::from_phf(tags, phf)),
        None => Ok(TagDatabase::new(tags)),
    }
    #[cfg(not(feature="phf"))]
    Ok(TagDatabase::new(tags))
}
