use std::convert::TryInto;
use std::num::NonZeroUsize;
use std::{cmp, io};

use byteorder::{BigEndian, WriteBytesExt};

use crate::block_builder::{BlockBuilder, DEFAULT_BLOCK_SIZE, DEFAULT_RESTART_INTERVAL};
use crate::compression::{compress, CompressionType};

pub const MIN_BLOCK_SIZE: usize = 1024;

/// A struct that is used to configure a [`StreamWriter`].
pub struct StreamWriterBuilder {
    compression_type: CompressionType,
    compression_level: u32,
    block_restart_interval: NonZeroUsize,
    block_size: usize,
}

impl Default for StreamWriterBuilder {
    fn default() -> StreamWriterBuilder {
        StreamWriterBuilder {
            compression_type: CompressionType::None,
            compression_level: 0,
            block_restart_interval: DEFAULT_RESTART_INTERVAL,
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }
}

impl StreamWriterBuilder {
    /// Creates a [`StreamWriterBuilder`], it can be used to
    /// configure your [`StreamWriter`] to better fit your needs.
    pub fn new() -> StreamWriterBuilder {
        StreamWriterBuilder::default()
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

    /// Defines the interval at which we will add keys into the footer index.
    pub fn block_restart_interval(&mut self, size: NonZeroUsize) -> &mut Self {
        self.block_restart_interval = size;
        self
    }

    /// Creates the [`StreamWriter`] that will write into the provided [`io::Write`] type.
    pub fn build<W: io::Write>(&self, mut writer: W) -> io::Result<StreamWriter<W>> {
        // We first write the compression type.
        // TODO write a magic number too.
        writer.write_u8(self.compression_type as u8)?;

        Ok(StreamWriter {
            block_builder: BlockBuilder::new(self.block_restart_interval),
            compression_type: self.compression_type,
            compression_level: self.compression_level,
            block_size: self.block_size,
            writer,
        })
    }

    /// Creates the [`StreamWriter`] that will write into a [`Vec`] of bytes.
    pub fn memory(&mut self) -> StreamWriter<Vec<u8>> {
        self.build(Vec::new()).unwrap()
    }
}

/// A struct you can use to write entries into any [`io::Write`] type,
/// entries must be inserted in key-order.
pub struct StreamWriter<W> {
    block_builder: BlockBuilder,
    compression_type: CompressionType,
    compression_level: u32,
    block_size: usize,
    writer: W,
}

impl StreamWriter<Vec<u8>> {
    /// Creates a [`StreamWriter`] that will write into a [`Vec`] of bytes.
    pub fn memory() -> StreamWriter<Vec<u8>> {
        StreamWriterBuilder::new().memory()
    }
}

impl StreamWriter<()> {
    /// Creates a [`StreamWriterBuilder`], it can be used to configure your [`StreamWriter`].
    pub fn builder() -> StreamWriterBuilder {
        StreamWriterBuilder::default()
    }
}

impl<W: io::Write> StreamWriter<W> {
    /// Creates a [`StreamWriter`] that will write into the provided [`io::Write`] type.
    pub fn new(writer: W) -> io::Result<StreamWriter<W>> {
        StreamWriterBuilder::new().build(writer)
    }

    /// Writes the provided entry into the underlying [`io::Write`] type,
    /// key-values must be given in key-order.
    pub fn insert<A, B>(&mut self, key: A, val: B) -> io::Result<()>
    where
        A: AsRef<[u8]>,
        B: AsRef<[u8]>,
    {
        self.block_builder.insert(key.as_ref(), val.as_ref());
        if self.block_builder.current_size_estimate() >= self.block_size {
            let buffer = self.block_builder.finish();

            // Compress, write the compressed block length then the compressed block itself.
            let buffer = compress(self.compression_type, self.compression_level, buffer.as_ref())?;
            let block_len = buffer.len().try_into().unwrap();
            self.writer.write_u64::<BigEndian>(block_len)?;
            self.writer.write_all(&buffer)?;
        }
        Ok(())
    }

    /// Consumes this [`StreamWriter`] and fetches the latest block currently being built.
    ///
    /// You must call this method before using the underlying [`io::Write`] type.
    pub fn finish(self) -> io::Result<()> {
        self.into_inner().map(drop)
    }

    /// Consumes this [`StreamWriter`] and fetches the latest block currenty being built.
    ///
    /// Returns the underlying [`io::Write`] provided type.
    pub fn into_inner(mut self) -> io::Result<W> {
        if !self.block_builder.is_empty() {
            let buffer = self.block_builder.finish();
            let buffer = buffer.as_ref();

            // Compress, write the compressed block length then the compressed block itself.
            let buffer = compress(self.compression_type, self.compression_level, buffer)?;
            let block_len = buffer.len().try_into().unwrap();
            self.writer.write_u64::<BigEndian>(block_len)?;
            self.writer.write_all(&buffer)?;
        }

        Ok(self.writer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_compression() {
        let wb = StreamWriter::builder();
        let mut writer = wb.build(Vec::new()).unwrap();

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
        let mut wb = StreamWriter::builder();
        wb.compression_type(CompressionType::Snappy);
        let mut writer = wb.build(Vec::new()).unwrap();

        for x in 0..2000u32 {
            let x = x.to_be_bytes();
            writer.insert(&x, &x).unwrap();
        }

        let bytes = writer.into_inner().unwrap();
        assert_ne!(bytes.len(), 0);
    }
}
