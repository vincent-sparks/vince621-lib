pub mod tags;
pub mod posts;

use crossbeam_channel::{Sender, Receiver, unbounded};
use std::io::{Read,BufRead};

use crate::db::{posts::PostDatabase, tags::TagDatabase};

/// Low budget way of turning an asynchronous file stream into a synchronous one.
struct StreamRead<T: AsRef<[u8]>> {
    buf: Option<T>,
    offset: usize,
    receiver: Receiver<std::io::Result<T>>,
}

impl<T: AsRef<[u8]>> StreamRead<T> {
    fn new(receiver: Receiver<std::io::Result<T>>) -> Self {
        Self {
            buf: None,
            offset: 0,
            receiver,
        }
    }
}


impl<T: AsRef<[u8]>> BufRead for StreamRead<T> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        
        // I tried *hard* to get rustc to understand that I wasn't breaking any rules without
        // writing `unsafe`.
        // Since the implementation of `Option::insert()` calls `as_mut()` (at least, I think
        // that's why), Rust thinks it creates a temporary value, and won't let me return the
        // returned reference (even though it ostensibly has the same lifetime as the reference
        // of its self argument, which is also the lifetime of this method's self argument).  I
        // think I should be able to return that value, but I can't.  So I wrote an unsafe block.
        //
        // SAFETY: If self.buf is None, we set it to Some before calling unwrap_unchecked.

        if !self.buf.as_ref().is_some_and(|buf| self.offset < buf.as_ref().len()) {
            if let Ok(next) = self.receiver.recv() {
                self.buf = Some(next?);
                self.offset=0;
            } else {
                return Ok(&[]);
            }
        } 
        let buf = unsafe {self.buf.as_ref().unwrap_unchecked()}.as_ref();

        Ok(&buf[self.offset..])
    }

    fn consume(&mut self, count: usize) {
        self.offset+=count;
    }
}

impl<T: AsRef<[u8]>> Read for StreamRead<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf2 = self.fill_buf()?;
        let count = buf2.len().min(buf.len());
        buf[..count].clone_from_slice(&buf2[..count]);
        self.consume(count);
        Ok(count)
    }
}

pub struct E6Database {
    pub tag: TagDatabase,
    pub post: PostDatabase,
}

/// The first argument to the download callback is a URL, which is guaranteed to begin with
/// "https://e621.net/".  The callback is expected to spawn a separate task (e.g. using tokio::spawn 
/// or starting a separate thread) which will send a GET request to the URL.  If the returned status
/// is different from 200, or if its `Content-Type` header is different from the expected value
/// provided in the second argument, the callback should report the error over the channel (send a
/// human-readable `Err` value into the Sender) and return immediately.  
///
/// If the status is 200, the task should stream the response body in arbitrary-sized chunks
/// and send them over the channel, dropping the Sender when the body reaches end-of-file.
/// If the task encounters an error, it should send an `Err` value on the channel
/// detailing what went wrong in some human-readable way, then drop the Sender and return.  There
/// should be no reason for this subtask to return anything other than the unit type -- all errors
/// should be reported to the channel instead.
///
/// Values sent into the Sender will be consumed from the same thread calling the
/// callback, so it is imperative that the callback not block while the file is downloading.  It
/// does not matter what I/O subsystem (synchronous or asynchronous) is ultimately used as long as
/// it happens in a different thread.  The callback is expected to "fire and forget" a task that
/// does the actual downloading.
pub fn load<T: AsRef<[u8]>>(download_callback: impl Fn(String, &'static str, Sender<std::io::Result<T>>) -> ()) -> csv::Result<E6Database> {
    let (sender, receiver) = crossbeam_channel::bounded(1);
    download_callback("https://e621.net/db_export/".into(), "text/html", sender);
    let mut response = Vec::new();
    StreamRead::new(receiver).read_to_end(&mut response)?;
    let mut data = std::str::from_utf8(response.as_slice()).map_err(std::io::Error::other)?;
    let mut date = None;

    while let Some((_, end)) = data.split_once("href=\"posts-") {
        let (this_date, rest_of_string) = end.split_once('.').ok_or_else(|| std::io::Error::other("unterminated string in HTML"))?;
        if !date.is_some_and(|date| this_date<date) {
            date = Some(this_date);
        }
        data = rest_of_string;
    }

    let date = date.ok_or_else(|| std::io::Error::other("HTML did not contain any posts- links"))?;
    dbg!(&date);

    let (sender, receiver) = crossbeam_channel::bounded(8);
    download_callback(format!("https://e621.net/db_export/tags-{}.csv.gz", date), "application/octet-stream", sender);

    let tag_db = tags::load_tag_database(csv::Reader::from_reader(flate2::bufread::GzDecoder::new(StreamRead::new(receiver))))?;

    let (sender, receiver) = crossbeam_channel::bounded(8);
    download_callback(format!("https://e621.net/db_export/posts-{}.csv.gz", date), "application/octet-stream", sender);

    let post_db = posts::load_post_database(&tag_db, csv::Reader::from_reader(flate2::bufread::GzDecoder::new(StreamRead::new(receiver))))?;
    
    Ok(E6Database {
        tag: tag_db,
        post: post_db,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_stream_read() {
        let (sender, receiver) = unbounded();
        let _ = sender.send(Ok([0u8,1,2,3,4,5,6,7,8,9,10,11].as_slice()));
        let mut reader = StreamRead::new(receiver);
        let mut data = [0u8;4];
        assert_eq!(reader.read(&mut data).unwrap(), 4);
        assert_eq!(data, [0,1,2,3]);
        assert_eq!(reader.read(&mut data).unwrap(), 4);
        assert_eq!(data, [4,5,6,7]);
        let _ = sender.send(Ok([12,13,14].as_slice()));
        assert_eq!(reader.read(&mut data).unwrap(), 4);
        assert_eq!(data, [8,9,10,11]);
        assert_eq!(reader.read(&mut data).unwrap(), 3);
        assert_eq!(data[..3], [12,13,14]);
        let _ = sender.send(Err(std::io::ErrorKind::Other.into()));
        assert!(reader.read(&mut data).is_err());
        std::mem::drop(sender);
        assert_eq!(reader.read(&mut data).unwrap(), 0);
    }
}
