use std::io::{self,Write};

use brotli::enc::backward_references::BrotliEncoderMode;
use brotli::CompressorWriter;
use brotli::enc::{BrotliEncoderInitParams, BrotliEncoderParams};
use byteorder::{LittleEndian, WriteBytesExt as _};
use replace_with::replace_with_or_abort;
use varint_rs::VarintWriter;

// max number of uninteresting compressed bytes a client will have to read before getting to the record
// they want.
const MAX_COMPRESSED_BYTES_PER_BLOCK: u64 = 16*1024*1024;

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
    key: u32,
    uncompressed_length: usize,
}

struct Block {
    start_offset: u64,
    string_index: Vec<StringDatabaseRecord>,
}

enum MaybeCompressed<W: Write> {
    Compressed(brotli::CompressorWriter<W>),
    NotCompressed(W),
}

impl<W: Write> Write for MaybeCompressed<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Compressed(w) => Write::write(w, buf),
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
    fn enter_compress(&mut self) {
        replace_with_or_abort(self, |me|
            match me {
                me @ Self::Compressed(_) => me,
                Self::NotCompressed(w) => Self::Compressed(CompressorWriter::with_params(w, 8192, &encoder_params())),
            }
        );
    }

    fn end_compress(&mut self) {
        replace_with_or_abort(self, |me|
            match me {
                Self::Compressed(w) => Self::NotCompressed(w.into_inner()),
                me @ Self::NotCompressed(_) => me,
            }
        );
    }

    fn get_ref(&self) -> &W {
        match self {
            Self::Compressed(w)=>w.get_ref(),
            Self::NotCompressed(w)=>w,
        }
    }

    fn get_mut(&mut self) -> &mut W {
        match self {
            Self::Compressed(w)=>w.get_mut(),
            Self::NotCompressed(w)=>w,
        }
    }
}

struct CountingWriter<W> {
    count: u64,
    w: W,
}

impl<W> CountingWriter<W> {
    fn get_count(&self) -> u64 {
        self.count
    }
    fn reset_count(&mut self) {
        self.count=0;
    }
    fn new(w:W) -> Self {
        Self {w, count:0}
    }

    fn into_inner(self) -> W {
        self.w
    }
}

impl<W:Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let res = self.w.write(buf);
        if let Ok(count)=&res {
            self.count += *count as u64;
        }
        res
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.w.flush()
    }
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let res = self.w.write_all(buf);
        if let Ok(()) = res {
            self.count += buf.len() as u64;
        }
        res
    }
}

struct StringDatabaseSerializer<W: Write> {
    fout: MaybeCompressed<CountingWriter<W>>,
    past_blocks: Vec<Block>,
    current_block: Block,
}

impl<W: Write> StringDatabaseSerializer<W> {
    pub fn new(fout: W) -> Self {
        Self {
            fout: MaybeCompressed::NotCompressed(CountingWriter::new(fout)),
            past_blocks: Vec::new(),
            current_block: Block {
                start_offset: 0,
                string_index: Vec::new(),
            },
        }

    }

    fn end_block(&mut self) {
        self.fout.end_compress();
        let w = self.fout.get_mut();

        let new_block = Block {
            start_offset: self.current_block.start_offset + w.get_count() as u64,
            string_index: Vec::new(),
        };
        let old_block = std::mem::replace(&mut self.current_block, new_block);
        self.past_blocks.push(old_block);
        w.reset_count();

    }
    pub fn add_record(&mut self, key: u32, s: &str) -> std::io::Result<()> {
        self.fout.enter_compress();
        self.fout.write_all(s.as_bytes())?;
        self.current_block.string_index.push(StringDatabaseRecord{key, uncompressed_length: s.len()});
        if self.fout.get_ref().get_count() > MAX_COMPRESSED_BYTES_PER_BLOCK {
            self.end_block();
        }
        Ok(())
    }
    pub fn finalize(mut self) -> io::Result<W> {
        self.fout.end_compress();

        let current_block = self.current_block;
        if !current_block.string_index.is_empty() {
            self.past_blocks.push(current_block);
        }

        self.fout.get_mut().reset_count();
        self.fout.write_all(b"v6ST")?;

        for block in self.past_blocks {
            self.fout.write_u64::<LittleEndian>(block.start_offset)?;
            self.fout.write_usize_varint(block.string_index.len())?;
            for record in block.string_index {
                self.fout.write_u32_varint(record.key)?;
                self.fout.write_usize_varint(record.uncompressed_length)?;
            }
        }

        self.fout.write_u64_varint(self.fout.get_ref().get_count())?;
        self.fout.write_all(b"v621SDB\n")?;


        match self.fout {
            MaybeCompressed::NotCompressed(w) => Ok(w.into_inner()),
            // should be unreachable because we called end_compress() earlier.
            MaybeCompressed::Compressed(_) => unreachable!(),
        }
    }
}
