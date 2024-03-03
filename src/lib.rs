use core::num::NonZeroU32;
use std::str::FromStr;

#[cfg(feature="load")]
pub mod load;

pub mod db;
pub mod search;

#[derive(strum::EnumString, Debug)]
#[strum(serialize_all="lowercase")]
pub enum FileType {
    JPG, PNG, GIF, WEBM, SWF
}

#[derive(Debug)]
pub struct Post {
    pub id: NonZeroU32,
    pub md5: [u8;16],
    pub extension: FileType,
}

pub fn read_csv(f: impl std::io::Read) -> csv::Result<Vec<Post>> {
    let mut v = Vec::new();
    let mut rdr = csv::Reader::from_reader(f);
    let headers = rdr.headers()?;
    let mut id_pos = None;
    let mut md5_pos = None;
    let mut extension_pos = None;
    for (idx, field) in headers.iter().enumerate() {
        if field == "id" {
            id_pos = Some(idx);
        } else if field == "md5" {
            md5_pos = Some(idx);
        } else if field == "file_ext" {
            extension_pos = Some(idx);
        }
    }
    let id_pos = id_pos.unwrap();
    let md5_pos = md5_pos.unwrap();
    let extension_pos = extension_pos.unwrap();

    for row in rdr.records() {
        let row = row?;
        v.push(Post {
            id: row.get(id_pos).unwrap().parse().unwrap(),
            md5: hex::FromHex::from_hex(row.get(md5_pos).unwrap()).unwrap(),
            extension: FileType::from_str(row.get(extension_pos).unwrap()).unwrap_or_else(|_| panic!("unexpected variant {}", row.get(extension_pos).unwrap())),
        });
    }

    Ok(v)
}
