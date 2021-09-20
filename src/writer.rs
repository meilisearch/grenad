use std::convert::TryInto;
use std::num::NonZeroUsize;
use std::{cmp, io};

use byteorder::{BigEndian, WriteBytesExt};

use crate::block_writer::BlockWriter;
use crate::compression::{compress, CompressionType};
use crate::count_write::CountWrite;
use crate::metadata::{FileVersion, Metadata};

pub const DEFAULT_BLOCK_SIZE: usize = 8192;
pub const MIN_BLOCK_SIZE: usize = 1024;

/// A struct that is used to configure a [`Writer`].
pub struct WriterBuilder {
    compression_type: CompressionType,
    compression_level: u32,
    index_key_interval: Option<NonZeroUsize>,
    block_size: usize,
}

impl Default for WriterBuilder {
    fn default() -> WriterBuilder {
        WriterBuilder {
            compression_type: CompressionType::None,
            compression_level: 0,
            index_key_interval: None,
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }
}

impl WriterBuilder {
    /// Creates a [`WriterBuilder`], it can be used to
    /// configure your [`Writer`] to better fit your needs.
    pub fn new() -> WriterBuilder {
        WriterBuilder::default()
    }

    /// Defines the [`CompressionType`] that will be used to compress the writer blocks.
    pub fn compression_type(&mut self, ctype: CompressionType) -> &mut Self {
        self.compression_type = ctype;
        self
    }

    /// Defines the copression level of the defined [`CompressionType`]
    /// that will be used to compress the writer blocks.
    pub fn compression_level(&mut self, level: u32) -> &mut Self {
        self.compression_level = level;
        self
    }

    /// Defines the size of the blocks that the writer will writer.
    ///
    /// The bigger the blocks are the better they are compressed
    /// but the more time it takes to compress and decompress them.
    pub fn block_size(&mut self, size: usize) -> &mut Self {
        self.block_size = cmp::max(MIN_BLOCK_SIZE, size);
        self
    }

    /// The interval at which we store the index of a key in the
    /// footer index, used to seek into a block.
    pub fn index_key_interval(&mut self, interval: NonZeroUsize) -> &mut Self {
        self.index_key_interval = Some(interval);
        self
    }

    /// Creates the [`Writer`] that will write into the provided [`io::Write`] type.
    pub fn build<W: io::Write>(&self, writer: W) -> Writer<W> {
        let mut block_writer_builder = BlockWriter::builder();
        if let Some(interval) = self.index_key_interval {
            block_writer_builder.index_key_interval(interval);
        }

        let mut index_block_writer_builder = BlockWriter::builder();
        if let Some(interval) = self.index_key_interval {
            index_block_writer_builder.index_key_interval(interval);
        }

        Writer {
            block_writer: block_writer_builder.build(),
            index_block_writer: index_block_writer_builder.build(),
            compression_type: self.compression_type,
            compression_level: self.compression_level,
            block_size: self.block_size,
            entries_count: 0,
            writer: CountWrite::new(writer),
        }
    }

    /// Creates the [`Writer`] that will write into a [`Vec`] of bytes.
    pub fn memory(&mut self) -> Writer<Vec<u8>> {
        self.build(Vec::new())
    }
}

/// A struct you can use to write entries into any [`io::Write`] type,
/// entries must be inserted in key-order.
pub struct Writer<W> {
    /// The block writer that is currently storing the key/values entries.
    block_writer: BlockWriter,
    /// The block writer that associates the offset (big endian u64) of the
    /// blocks in the file with the last key of these given blocks.
    index_block_writer: BlockWriter,
    /// The compression method used to compress individual blocks.
    compression_type: CompressionType,
    /// The compression level used to compress individual blocks.
    compression_level: u32,
    /// The amount of bytes to reach before dumping this block on disk.
    block_size: usize,
    /// The amount of key already inserted.
    entries_count: u64,
    /// The writer in which we write the block, index block and footer metadata.
    writer: CountWrite<W>,
}

impl Writer<Vec<u8>> {
    /// Creates a [`Writer`] that will write into a [`Vec`] of bytes.
    pub fn memory() -> Writer<Vec<u8>> {
        WriterBuilder::new().memory()
    }
}

impl Writer<()> {
    /// Creates a [`WriterBuilder`], it can be used to configure your [`Writer`].
    pub fn builder() -> WriterBuilder {
        WriterBuilder::default()
    }
}

impl<W: io::Write> Writer<W> {
    /// Creates a [`Writer`] that will write into the provided [`io::Write`] type.
    pub fn new(writer: W) -> Writer<W> {
        WriterBuilder::new().build(writer)
    }

    /// Writes the provided entry into the underlying [`io::Write`] type,
    /// key-values must be given in key-order.
    pub fn insert<A, B>(&mut self, key: A, val: B) -> io::Result<()>
    where
        A: AsRef<[u8]>,
        B: AsRef<[u8]>,
    {
        self.block_writer.insert(key.as_ref(), val.as_ref());
        self.entries_count += 1;

        if self.block_writer.current_size_estimate() >= self.block_size {
            // Only write a block if there is at least a key in it.
            if let Some(last_key) = self.block_writer.last_key() {
                // Get the current offset and last key of the current block,
                // write it in the index block writer.
                let offset = self.writer.count();
                self.index_block_writer.insert(last_key, &offset.to_be_bytes());

                compress_and_write_block(
                    &mut self.writer,
                    &mut self.block_writer,
                    self.compression_type,
                    self.compression_level,
                )?;
            }
        }

        Ok(())
    }

    /// Consumes this [`Writer`] and write the latest block currently being built.
    ///
    /// You must call this method before using the underlying [`io::Write`] type.
    pub fn finish(self) -> io::Result<()> {
        self.into_inner().map(drop)
    }

    /// Consumes this [`Writer`] and write the latest block currenty being built.
    ///
    /// Returns the underlying [`io::Write`] provided type.
    pub fn into_inner(mut self) -> io::Result<W> {
        // Write the last block only if it is not empty.
        if let Some(last_key) = self.block_writer.last_key() {
            // Get the current offset and last key of the current block,
            // write it in the index block writer.
            let offset = self.writer.count();
            self.index_block_writer.insert(last_key, &offset.to_be_bytes());

            compress_and_write_block(
                &mut self.writer,
                &mut self.block_writer,
                self.compression_type,
                self.compression_level,
            )?;
        }

        // We must write the index block to the file.
        let index_block_offset = self.writer.count();
        compress_and_write_block(
            &mut self.writer,
            &mut self.index_block_writer,
            self.compression_type,
            self.compression_level,
        )?;

        // Then we can write the metadata that specify where the index block is stored.
        let metadata = Metadata {
            file_version: FileVersion::FormatV1,
            index_block_offset,
            compression_type: self.compression_type,
            entries_count: self.entries_count,
        };

        metadata.write_into(&mut self.writer)?;
        self.writer.into_inner()
    }
}

/// Compress and write the block into the writer prefixed by the length of it as an `u64`.
fn compress_and_write_block<W: io::Write>(
    mut writer: W,
    block_writer: &mut BlockWriter,
    compression_type: CompressionType,
    compression_level: u32,
) -> io::Result<()> {
    let buffer = block_writer.finish();

    // Compress, write the length of the compressed block then the block itself.
    let buffer = compress(compression_type, compression_level, buffer.as_ref())?;
    let block_len = buffer.len().try_into().unwrap();
    writer.write_u64::<BigEndian>(block_len)?;
    writer.write_all(&buffer)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_compression() {
        let wb = Writer::builder();
        let mut writer = wb.build(Vec::new());

        for x in 0..2000u32 {
            let x = x.to_be_bytes();
            writer.insert(&x, &x).unwrap();
        }

        let bytes = writer.into_inner().unwrap();
        assert_ne!(bytes.len(), 0);
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn snappy_compression() {
        let mut wb = Writer::builder();
        wb.compression_type(CompressionType::Snappy);
        let mut writer = wb.build(Vec::new());

        for x in 0..2000u32 {
            let x = x.to_be_bytes();
            writer.insert(&x, &x).unwrap();
        }

        let bytes = writer.into_inner().unwrap();
        assert_ne!(bytes.len(), 0);
    }
}
