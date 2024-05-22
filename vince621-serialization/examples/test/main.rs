use std::fs::File;
use std::io::{self, BufReader, BufWriter, Seek as _};
use std::path::Path;
use std::time::Instant;

use vince621_core::db::tags::TagAndImplicationDatabase;

fn disk_load(url: String, _ignored: &'static str) -> std::io::Result<BufReader<File>> {
    let path = url.strip_prefix("https://e621.net/db_export/").unwrap();
    let path = Path::join(env!("CARGO_MANIFEST_DIR").as_ref(), path);
    dbg!(&path);
    File::open(path).map(BufReader::new)
}

fn main() -> io::Result<()> {
    println!("loading tags");
    let tag_db_file = BufReader::new(File::open("tags.csv.gz")?);
    let tag_db = vince621_csv::load_tag_database(vince621_csv::AlreadyLoaded(tag_db_file))?;

    println!("loading implications");
    let implication_db_file = BufReader::new(File::open("tag_implications.csv.gz")?);
    let implication_db = vince621_csv::load_tag_implication_database(&tag_db, vince621_csv::AlreadyLoaded(implication_db_file))?;
    
    println!("loading implications");
    let alias_db_file = BufReader::new(File::open("tag_aliases.csv.gz")?);
    let alias_db = vince621_csv::load_tag_alias_database(&tag_db, vince621_csv::AlreadyLoaded(alias_db_file))?;

    let tag_db = TagAndImplicationDatabase::new(tag_db, implication_db, alias_db);
    
    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open("tags.v621")?;

    let t1 = Instant::now();
    let mut f2 = BufWriter::new(f);
    vince621_serialization::serialize_tag_and_implication_database(&tag_db, &mut f2)?;
    let t2 = Instant::now();
    println!("serialize done -- it took {:?}", t2-t1);
    let f = f2.into_inner().unwrap();
    let mut f = BufReader::new(f);
    f.seek(io::SeekFrom::Start(0))?;
    let hdr = vince621_serialization::tags::read_tag_header(&mut f)?;
    let db2 = vince621_serialization::deserialize_tag_and_implication_database(hdr, &mut f)?;
    println!("deserialize done -- it took {:?}", t2.elapsed());

    let t1=Instant::now();
    assert_eq!(tag_db.tags.get_all().len(), db2.tags.get_all().len());

    for (p1,p2) in tag_db.tags.get_all().iter().zip(db2.tags.get_all()) {
        assert_eq!(p1,p2);
    }

    assert_eq!(tag_db.aliases.len(), db2.aliases.len());

    for (p1,p2) in tag_db.aliases.iter().zip(db2.aliases.iter()) {
        assert_eq!(p1,p2);
    }

    assert_eq!(tag_db.implications, db2.implications);

    for tag in tag_db.tags.get_all().iter() {
        assert_eq!(tag.id, db2.tags.get(&tag.name).unwrap().id);
    }

    println!("validate done -- it took {:?}", t1.elapsed());

    println!("reading post database");
    
    let post_db_file = BufReader::new(File::open("posts.csv.gz")?);
    let post_db = vince621_csv::load_post_database(&tag_db.tags, vince621_csv::AlreadyLoaded(post_db_file))?;

    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .read(true)
        .write(true)
        .open("posts.v621")?;
    let t1 = Instant::now();
    let mut f2 = BufWriter::new(f);
    vince621_serialization::serialize_post_database(post_db.get_all(), &mut f2)?;
    let t2 = Instant::now();
    println!("serialize done -- it took {:?}", t2-t1);
    let f = f2.into_inner().unwrap();
    let mut f = BufReader::new(f);
    f.seek(io::SeekFrom::Start(0))?;
    let db2 = vince621_serialization::deserialize_post_database(&mut f)?;
    println!("deserialize done -- it took {:?}", t2.elapsed());

    let t1=Instant::now();
    assert_eq!(post_db.get_all().len(), db2.get_all().len());

    for (p1,p2) in post_db.get_all().iter().zip(db2.get_all()) {
        assert_eq!(p1,p2);
    }

    println!("validate done -- it took {:?}", t1.elapsed());

    println!("{} posts have no width", post_db.get_all().iter().filter(|x| x.width==0).count());
    println!("{} posts have no height", post_db.get_all().iter().filter(|x| x.height==0).count());

    Ok(())
}
