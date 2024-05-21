//pub mod strings;
pub mod posts;
pub mod tags;

use std::io;

pub use posts::{serialize_post_database, deserialize_post_database};
pub use tags::{serialize_tag_database, deserialize_tag_database, serialize_tag_and_implication_database, deserialize_tag_and_implication_database};

pub trait Header {
    /// Total number of records to load, used for displaying progress bars.
    fn total_record_count(&self) -> usize;
    /// Total size the full unloaded data will occupy in memory.  This is used to decide how much
    /// memory to allocate and the application may crash if it is inaccurate.
    fn total_memory_size_bytes(&self) -> usize;
}

pub trait Loader {
    type Header: Header;
    type Result;
    fn read_header(&mut self) -> io::Result<Self::Header>;
    fn load(self, header: Self::Header) -> io::Result<Self::Result>;
}


