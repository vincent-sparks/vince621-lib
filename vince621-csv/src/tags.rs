use crate::db::tags::{Tag,TagDatabase};
use std::{io::Read, time::Instant};

pub fn load_tag_database<R: Read>(mut rdr: csv::Reader<R>) -> csv::Result<TagDatabase> {
    let t1=Instant::now();
    let res = Ok(TagDatabase::new(rdr.deserialize().collect::<csv::Result<_>>()?));
    dbg!(t1.elapsed());
    res
}
