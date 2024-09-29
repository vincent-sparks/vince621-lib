use std::num::NonZeroU32;

use serde::de::{Deserializer, Unexpected};
//use time::{PrimitiveDateTime};

//use time::macros::format_description;

use chrono::{DateTime,Utc};

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
            if s.is_empty() {
                Ok(Box::from([]))
            } else {
                s.split(',').map(|v|v.parse().map_err(E::custom)).collect()
            }
        }
    }
    d.deserialize_str(Visitor)
}

//const DATE_FORMAT: &'static [time::format_description::FormatItem] = format_description!("[year]-[month padding:zero]-[day padding:zero] [hour padding:zero]:[minute padding:zero]:[second padding:zero].[subsecond digits:1+]");
//const DATE_FORMAT: &'static str = "%Y-%m-%d %H:%M:%S%.f";
const DATE_FORMAT: [chrono::format::Item<'static>; 12] = {
    use chrono::format::{Item, Numeric, Pad, Fixed};
    [
        Item::Numeric(Numeric::Year, Pad::Zero),
        Item::Literal("-"),
        Item::Numeric(Numeric::Month, Pad::Zero),
        Item::Literal("-"),
        Item::Numeric(Numeric::Day, Pad::Zero),
        Item::Literal(" "),
        Item::Numeric(Numeric::Hour, Pad::Zero),
        Item::Literal(":"),
        Item::Numeric(Numeric::Minute, Pad::Zero),
        Item::Literal(":"),
        Item::Numeric(Numeric::Second, Pad::Zero),
        Item::Fixed(Fixed::Nanosecond),
    ]
};

pub fn e621_date<'a,D: Deserializer<'a>>(d: D) -> Result<DateTime<Utc>, D::Error> {
    struct Visitor;
    impl serde::de::Visitor<'_> for Visitor {
        //type Value=PrimitiveDateTime;
        type Value=DateTime<Utc>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a date and time formatted with RFC 3339")
        }

        fn visit_str<E: serde::de::Error>(self, s:&str) -> Result<Self::Value,E> {
            //PrimitiveDateTime::parse(s, &DATE_FORMAT).map_err(E::custom)
            let mut parsed = chrono::format::Parsed::new();
            chrono::format::parse(&mut parsed, s, DATE_FORMAT.iter()).map_err(E::custom)?;
            parsed.to_datetime_with_timezone(&Utc).map_err(E::custom)
        }
    }
    d.deserialize_str(Visitor)
}


