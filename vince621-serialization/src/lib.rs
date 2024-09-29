//pub mod strings;
pub mod posts;
pub mod tags;
pub mod pools;

use std::io;

pub use posts::{serialize_post_database, deserialize_post_database};
pub use pools::{serialize_pool_database, deserialize_pool_database};
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

/**
 * Read a null-terminated string from the file into the specified Vec and return a reference to it.
 * The caller should call vec.clear() between invocations of this method.
 */
pub(crate) fn read_string_null_terminated<'a>(file: &mut impl std::io::BufRead, buf: &'a mut Vec<u8>) -> std::io::Result<&'a str> {
    debug_assert!(buf.is_empty(), "buf should be cleared between invocations of read_string_null_terminated()");
    file.read_until(b'\0', &mut *buf)?;
    let idx = buf.len() - 1;
    std::str::from_utf8(&buf[..idx]).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
