use std::io::Seek;
use std::{collections::HashMap, io::Write};

use brotli::enc::backward_references::BrotliEncoderMode;
use brotli::CompressorWriter;
use brotli::enc::{BrotliEncoderInitParams, BrotliEncoderParams};
use replace_with::replace_with_or_abort;

fn encoder_params() -> BrotliEncoderParams {
    BrotliEncoderParams {
                appendable: false,
                catable: false,
                magic_number: false,
                large_window: false,
                size_hint: 0, // I have absolutely no idea how I would provide this unless I chunked based on
                              // input size rather than output size.
                mode: BrotliEncoderMode::BROTLI_MODE_TEXT,
                quality: 10, // max is 11 -- todo: performance tune this.
                ..BrotliEncoderInitParams()
            }
}

struct StringDatabaseRecord {
    pub start: usize,
    pub compressed_len: usize,
}

struct Block {
    start_offset: u64,
    block_indices: HashMap<u32, StringDatabaseRecord>,
}

enum MaybeCompressed<W: Write> {
    Compressed(brotli::CompressorWriter<W>),
    NotCompressed(W),
}

impl<W: Write> Write for MaybeCompressed<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Compressed(w) => w.write(buf),
            Self::NotCompressed(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Compressed(w) => w.flush(),
            Self::NotCompressed(w) => w.flush(),
        }
    }
}

impl<W: Write> MaybeCompressed<W> {
    fn end_compress(&mut self) {
        replace_with_or_abort(self, |me|
            match me {
                Self::Compressed(w) => Self::NotCompressed(w.into_inner()),
                me @ Self::NotCompressed(_) => me,
            }
        );
    }

}

impl<W: Write + Seek> MaybeCompressed<W> {
    fn tell(&mut self) -> std::io::Result<u64> {
        let f = match self {
            Self::Compressed(w) => w.get_mut(),
            Self::NotCompressed(w) => w,
        };
        f.seek(std::io::SeekFrom::Current(0))
    }
}

struct StringDatabaseSerializer<W: Write> {
    fout: MaybeCompressed<W>,
    past_blocks: Vec<Block>,
    current_block: Block,
    current_block_len: usize,
}

impl<W: Write + Seek> StringDatabaseSerializer<W> {
    pub fn new(fout: W) -> Self {
        Self {
            fout: MaybeCompressed::NotCompressed(fout),
            past_blocks: Vec::new(),
            current_block: Block {
                start_offset: 0,
                block_indices: HashMap::new(),
            },
            current_block_len: 0,
        }

    }

    fn end_block(&mut self) {
        let new_block = Block {
            start_offset: self.current_block.start_offset + self.current_block_len as u64,
            block_indices: HashMap::new(),
        };
        let old_block = std::mem::replace(&mut self.current_block, new_block);
        self.past_blocks.push(old_block);
        self.current_block_len=0;
        self.fout.end_compress();
    }
    fn tell(&mut self) -> u64 {
        self.fout.tell().unwrap()
    }
    pub fn add_record(&mut self, key: u32, s: &str) -> std::io::Result<()> {
        debug_assert!(self.current_block_len as u64 == self.tell());
        Ok(())
    }
}
