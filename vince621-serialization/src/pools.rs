use std::{io::{BufRead,Write}, num::NonZeroU32};

use super::read_string_null_terminated;

use byteorder::{BigEndian, ReadBytesExt as _, WriteBytesExt as _};
use chrono::{DateTime, Utc};
use varint_rs::{VarintReader as _, VarintWriter as _};
use vince621_core::db::pools::{Pool, PoolCategory, PoolDatabase};

pub fn serialize_pool_database(pools: &[Pool], mut file: impl Write) -> std::io::Result<()> {
    file.write_all(b"v621POOL")?;
    file.write_u32::<BigEndian>(1)?;
    file.write_usize_varint(pools.len())?;
    let mut last_id = 0;
    for pool in pools {
        file.write_u32_varint(pool.id.get() - last_id)?;
        last_id = pool.id.get();
        let mut flags = 0u8;
        if pool.is_active {
            flags |= 0x1;
        }
        flags |= match pool.category {
            PoolCategory::Collection=>0x0,
            PoolCategory::Series=>0x2,
        };
        file.write_u8(flags)?;
        
        file.write_u64_varint(pool.last_updated.timestamp_micros() as u64)?;
        
        file.write_all(pool.name.as_bytes())?;
        file.write_u8(b'\0')?;
        file.write_all(pool.description.as_ref().map(|x| x.as_ref()).unwrap_or("").as_bytes())?;
        file.write_u8(b'\0')?;

        file.write_usize_varint(pool.post_ids.len())?;
        let mut last_post_id=0;
        for post_id in pool.post_ids.iter() {
            let post_id = post_id.get();
            file.write_i32_varint(post_id.wrapping_sub(last_post_id) as i32)?;
            last_post_id = post_id;
        }
    }
    Ok(())
}

pub fn deserialize_pool_database(mut file: impl BufRead) -> std::io::Result<PoolDatabase> {
    let mut buf = [0u8;8];
    file.read_exact(&mut buf)?;
    if buf != *b"v621POOL" {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad magic number"));
    }
    let version = file.read_u32::<BigEndian>()?;
    let has_descriptions = version & 1 == 1;
    if version >= 2 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Format version is too new (please update vince621)"))
    }
    let total_count = file.read_usize_varint()?;
    let mut pools: Vec<Pool> = Vec::with_capacity(total_count);
    let mut last_id = 0;

    let mut read_buffer = Vec::new();

    for _ in 0..total_count {
        last_id += file.read_u32_varint()?;
        let id = NonZeroU32::new(last_id).expect("pool IDs should not be zero");
        let flags = file.read_u8()?;
        let is_active = flags & 0x1 == 1;
        let category = match flags & 0x2 {
            0x0 => PoolCategory::Collection,
            0x2 => PoolCategory::Series,
            _ => unreachable!(),
        };

        let last_updated = DateTime::<Utc>::from_timestamp_micros(file.read_u64_varint()? as i64).unwrap();

        let name = read_string_null_terminated(&mut file, &mut read_buffer)?.into();
        read_buffer.clear();

        let description = if has_descriptions {
            let r = match read_string_null_terminated(&mut file, &mut read_buffer)? {
                "" => None,
                s  => Some(Box::from(s)),
            };
            read_buffer.clear();
            r
        } else {
            None
        };

        let post_id_count = file.read_usize_varint()?;
        let mut last_post_id = 0u32;

        let mut post_ids = Vec::with_capacity(post_id_count);

        for _ in 0..post_id_count {
            last_post_id = last_post_id.checked_add_signed(file.read_i32_varint()?).expect("post ID overflowed a u32???");
            let post_id = NonZeroU32::new(last_post_id).expect("post IDs may not be zero");
            post_ids.push(post_id);
        }
        
        pools.push(Pool {
            id,
            category,
            is_active,
            description,
            name,
            post_ids: post_ids.into(),
            last_updated,
        });
    }

    Ok(PoolDatabase::new(pools.into_boxed_slice()))
}
