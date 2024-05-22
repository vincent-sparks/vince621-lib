use vince621_core::db::tags::TagDatabase;
use std::{collections::HashMap, io::{self, Read}, time::Instant};
use byteyarn::Yarn;

pub fn load_tag_database<R: Read>(mut rdr: csv::Reader<R>) -> csv::Result<TagDatabase> {
    let t1=Instant::now();
    let res = Ok(TagDatabase::new(rdr.deserialize().collect::<csv::Result<_>>()?));
    dbg!(t1.elapsed());
    res
}

fn load_implication_or_alias_database<R: Read>(mut rdr: csv::Reader<R>, mut out: impl FnMut(&str, &str)) -> csv::Result<()> {
    let headers = rdr.headers()?;
    let status_column = headers.iter().position(|s| s=="status").ok_or_else(|| io::Error::other("missing status column"))?;
    let antecedent_name_column = headers.iter().position(|s| s=="antecedent_name").ok_or_else(|| io::Error::other("missing antecedent_name column"))?;
    let consequent_name_column = headers.iter().position(|s| s=="consequent_name").ok_or_else(|| io::Error::other("missing consequent_name column"))?;

    let mut row = csv::StringRecord::new();
    while rdr.read_record(&mut row)? {
        if row.get(status_column).unwrap() == "active" {
            out(
                row.get(antecedent_name_column).unwrap(),
                row.get(consequent_name_column).unwrap(),
            );
        }
    }

    Ok(())
}

pub fn load_alias_database<R: Read>(tag_db: &TagDatabase, rdr: csv::Reader<R>) -> csv::Result<Vec<(Yarn, usize)>> {
    let mut res = Vec::new();
    load_implication_or_alias_database(rdr, |antecedent, consequent| {
        let Some(consequent) = tag_db.get_as_index(consequent) else {return};
        res.push((Yarn::copy(antecedent), consequent));
    })?;

    res.sort_unstable_by(|x,y|x.0.cmp(&y.0));

    Ok(res)
}

pub fn load_implication_database<R: Read>(tag_db: &TagDatabase, rdr: csv::Reader<R>) -> csv::Result<std::collections::HashMap<u32, Vec<u32>>> {
    let mut res = HashMap::<u32, Vec<u32>>::new();
    load_implication_or_alias_database(rdr, |antecedent, consequent| {
        let Some(antecedent) = tag_db.get(antecedent) else {return};
        let Some(consequent) = tag_db.get(consequent) else {return};
        res.entry(antecedent.id).or_default().push(consequent.id);
    })?;

    Ok(res)
}
