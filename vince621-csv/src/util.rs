use std::num::NonZeroU32;

use serde::de::{Deserializer, Unexpected};
use time::{format_description::well_known::Rfc2822, PrimitiveDateTime};

pub fn t_or_f<'a,D: Deserializer<'a>>(d: D) -> Result<bool, D::Error> {
    struct Visitor;
    impl serde::de::Visitor<'_> for Visitor {
        type Value=bool;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("either \"t\" or \"f\"")
        }

        fn visit_str<E: serde::de::Error>(self, s:&str) -> Result<bool,E> {
            match s {
                "t" => Ok(true),
                "f" => Ok(false),
                other => Err(E::invalid_value(Unexpected::Str(other),&self)),
            }
        }
    }
    d.deserialize_str(Visitor)
}

pub fn bracketed_list<'a,D: Deserializer<'a>>(d: D) -> Result<Box<[NonZeroU32]>, D::Error> {
    struct Visitor;
    impl serde::de::Visitor<'_> for Visitor {
        type Value=Box<[NonZeroU32]>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a comma-separated list of integers surrounded by {}")
        }

        fn visit_str<E: serde::de::Error>(self, s:&str) -> Result<Self::Value,E> {
            let s2 = s.strip_prefix('{').ok_or_else(|| E::invalid_value(Unexpected::Str(s),&self))?;
            let s = s2.strip_suffix('}').ok_or_else(|| E::invalid_value(Unexpected::Str(s),&self))?;
            s.split(',').map(|v|v.parse().map_err(E::custom)).collect()
        }
    }
    d.deserialize_str(Visitor)
}

pub fn rfc3339<'a,D: Deserializer<'a>>(d: D) -> Result<PrimitiveDateTime, D::Error> {
    struct Visitor;
    impl serde::de::Visitor<'_> for Visitor {
        type Value=PrimitiveDateTime;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a date and time formatted with RFC 3339")
        }

        fn visit_str<E: serde::de::Error>(self, s:&str) -> Result<Self::Value,E> {
            PrimitiveDateTime::parse(s, &Rfc3339).map_err(E::custom)
        }
    }
    d.deserialize_str(Visitor)
}


