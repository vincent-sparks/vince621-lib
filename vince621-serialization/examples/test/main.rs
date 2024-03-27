use std::fs::File;
use std::io::{self, BufReader, BufWriter, Seek as _};
use std::path::Path;
use std::time::Instant;

fn disk_load(url: String, _ignored: &'static str) -> std::io::Result<BufReader<File>> {
    let path = url.strip_prefix("https://e621.net/db_export/").unwrap();
    let path = Path::join(env!("CARGO_MANIFEST_DIR").as_ref(), path);
    dbg!(&path);
    File::open(path).map(BufReader::new)
}

fn main() -> io::Result<()> {
    let db = vince621_csv::load_date("2024-03-22", disk_load).unwrap();

    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open("posts.v621")?;
    let t1 = Instant::now();
    let mut f2 = BufWriter::new(f);
    vince621_serialization::serialize_post_database(db.post.get_all(), &mut f2)?;
    let t2 = Instant::now();
    println!("serialize done -- it took {:?}", t2-t1);
    let f = f2.into_inner().unwrap();
    let mut f = BufReader::new(f);
    f.seek(io::SeekFrom::Start(0))?;
    let db2 = vince621_serialization::deserialize_post_database(&mut f)?;
    println!("deserialize done -- it took {:?}", t2.elapsed());

    let t1=Instant::now();
    assert_eq!(db.post.get_all().len(), db2.get_all().len());

    for (p1,p2) in db.post.get_all().iter().zip(db2.get_all()) {
        assert_eq!(p1,p2);
    }

    println!("validate done -- it took {:?}", t1.elapsed());

    Ok(())
}
