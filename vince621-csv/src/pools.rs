use std::num::NonZeroU32;
use vince621_core::db::pools::{Pool,PoolCategory,PoolDatabase};
use chrono::{DateTime,Utc};

// this code is *not* well engineered
// it is ugly af but works
// the only reason i did this this way is because serde and multicrate shenanigans forced my hand
// do not try this at home

pub fn category<'a,D: serde::Deserializer<'a>>(d: D) -> Result<PoolCategory, D::Error> {
    struct Visitor;
    impl serde::de::Visitor<'_> for Visitor {
        type Value=PoolCategory;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("either \"t\" or \"f\"")
        }

        fn visit_str<E: serde::de::Error>(self, s:&str) -> Result<PoolCategory,E> {
            match s {
                "series" => Ok(PoolCategory::Series),
                "collection" => Ok(PoolCategory::Collection),
                other => Err(E::invalid_value(serde::de::Unexpected::Str(other),&self)),
            }
        }
    }
    d.deserialize_str(Visitor)
}

#[derive(serde::Deserialize)]
struct CSVPool {
    id: NonZeroU32,
    name: Box<str>,
    description: Box<str>,
    #[serde(deserialize_with="crate::util::e621_date")]
    updated_at: DateTime<Utc>,

    #[serde(deserialize_with="category")]
    category: PoolCategory,

    #[serde(deserialize_with="crate::util::t_or_f")]
    is_active: bool,
    #[serde(deserialize_with="crate::util::bracketed_list")]
    post_ids: Box<[u32]>,
}

impl From<CSVPool> for Pool {
    fn from(me: CSVPool) -> Pool {
        Pool {
            id: me.id,
            name: me.name,
            description: if !me.description.is_empty() {Some(me.description)} else {None},
            is_active: me.is_active,
            last_updated: me.updated_at,
            category: me.category,
            post_ids: me.post_ids.iter().copied().filter_map(NonZeroU32::new).collect(),
        }
    }
}

pub fn load_pool_database<R: std::io::Read>(mut csv: csv::Reader<R>) -> csv::Result<PoolDatabase> {
    csv.deserialize::<CSVPool>().map(|r| r.map(Into::into)).collect::<Result<_,_>>().map(PoolDatabase::new)
}

#[test]
fn load_csv() {
    load_pool_database(csv::Reader::from_reader(flate2::read::GzDecoder::new(std::fs::File::open("pools.csv.gz").unwrap()))).unwrap();
}
