use std::{collections::HashMap, io};

use byteorder::{BigEndian, LittleEndian, ReadBytesExt as _, WriteBytesExt as _};
use byteyarn::Yarn;
use num_traits::FromPrimitive as _;
#[cfg(feature="phf")]
use phf_generator::HashState;
use varint_rs::{VarintReader, VarintWriter as _};
use vince621_core::db::tags::{Tag, TagCategory, TagDatabase, TagAndImplicationDatabase};

pub struct TagHeader {
    version: u32,
    tag_count: usize,
    alias_count: usize,
    implication_count: usize,
    #[cfg(feature="phf")]
    phf: Option<HashState>,
}

pub fn read_tag_header(file: &mut impl io::Read) -> io::Result<TagHeader> {
    let mut magic = [0u8;8];
    file.read_exact(&mut magic)?;
    if &magic != b"v621TAGS" {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Bad magic number"));
    }
    let version = file.read_u32::<BigEndian>()?;
    let tag_count = file.read_usize_varint()?;
    let (alias_count, implication_count) = match version {
        0 | 1 => (0,0),
        2 | 3 => {
            let a = file.read_usize_varint()?;
            let b = file.read_usize_varint()?;
            (a,b)
        },
        _ => unreachable!(), // handled by the return Err() above.
    };
    #[cfg(feature="phf")]
    let phf: Option<HashState>;
    match version {
        0 | 2 => {
            #[cfg(feature="phf")] {
                phf = None;
            }
        },
        1 | 3 => {
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
                let mut map = Vec::with_capacity(tag_count);
                for _ in 0..tag_count {
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
    Ok(TagHeader {
        version,
        tag_count,
        alias_count,
        implication_count,
        #[cfg(feature="phf")]
        phf,
    })
}

pub fn write_tag_db_header(file: &mut impl io::Write, #[cfg(feature="phf")] phf: &HashState, tag_count: usize, alias_count: usize, implication_count: usize) -> io::Result<()> {
    file.write_all(b"v621TAGS")?;
    let mut version = if implication_count == 0 && alias_count == 0 {
        0
    } else {
        2
    };

    #[cfg(feature="phf")] {version+=1;}

    file.write_u32::<BigEndian>(version)?;

    file.write_usize_varint(tag_count)?;
    if version >= 2 {
        file.write_usize_varint(alias_count)?;
        file.write_usize_varint(implication_count)?;
    }

    #[cfg(feature="phf")]
    {
        let mut v = Vec::new();
        v.write_u64::<LittleEndian>(phf.key)?;
        v.write_usize_varint(phf.disps.len())?;
        for (a,b) in phf.disps.iter().copied() {
            v.write_u32_varint(a)?;
            v.write_u32_varint(b)?;
        }
        assert_eq!(phf.map.len(), tag_count);
        for i in phf.map.iter().copied() {
            v.write_usize_varint(i)?;
        }
        file.write_usize_varint(v.len())?;
        file.write_all(&v)?;
    }

    Ok(())
}

pub fn serialize_tag_database(db: &TagDatabase, file: &mut impl io::Write) -> io::Result<()> {

    write_tag_db_header(&mut *file, #[cfg(feature="phf")] db.get_phf(), db.get_all().len(), 0, 0)?;

    for tag in db.get_all() {
        file.write_all(tag.name.as_bytes())?;
        file.write_all(b"\0")?;
        file.write_u32_varint(tag.id)?;
        file.write_u8(tag.category as u8)?;
        file.write_i32_varint(tag.post_count)?;
    }


    Ok(())
}

pub fn deserialize_tag_database(header: TagHeader, file: &mut impl io::BufRead) -> io::Result<TagDatabase> {
    let TagHeader {tag_count, phf, ..} = header;
    let mut tags = Vec::with_capacity(tag_count);

    let mut name_buf = Vec::new();
    for _ in 0..tag_count {
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
    Ok((TagDatabase::new(tags), version==2 || version==3))
}


pub fn deserialize_tag_and_implication_database(header: TagHeader, file: &mut impl io::BufRead) -> io::Result<TagAndImplicationDatabase> {
    let &TagHeader{version, alias_count, implication_count, ..} = &header;
    let tag_db = deserialize_tag_database(header, &mut *file)?;
    if version==2 || version==3 {
        let mut aliases = Vec::with_capacity(alias_count);
        let mut implications = HashMap::with_capacity(implication_count);

        let mut v = Vec::new();
        for _ in 0..alias_count {
            file.read_until(b'\0', &mut v)?;
            let idx=v.len()-1;
            if v[idx] != b'\0' {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF in the middle of an alias name"));
            }
            let name = std::str::from_utf8(&v[..idx]).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let tag_idx = file.read_usize_varint()?;
            aliases.push((Yarn::copy(name), tag_idx));
        }

        for _ in 0..implication_count {
            let antecedent = file.read_u32_varint()?;
            let consequent_count = file.read_usize_varint()?;
            let mut consequents = Vec::with_capacity(consequent_count);
            for _ in 0..consequent_count {
                consequents.push(file.read_u32_varint()?);
            }
            implications.insert(antecedent, consequents);
        }

        Ok(TagAndImplicationDatabase {tags:tag_db, aliases, implications})
    } else {
        Ok(TagAndImplicationDatabase {tags: tag_db, aliases: Vec::with_capacity(0), implications: HashMap::with_capacity(0)})
    }
}

pub fn serialize_tag_and_implication_database(db: &TagAndImplicationDatabase, file: &mut impl io::BufRead) -> io::Result<()> {
    Ok(())
}
