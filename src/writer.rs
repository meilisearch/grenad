use std::convert::TryInto;
use std::io;

use byteorder::{BigEndian, WriteBytesExt};

use crate::block_builder::{BlockBuilder, DEFAULT_BLOCK_SIZE};
use crate::compression::{compress, CompressionType};

/// A struct that is used to configure a [`Writer`].
pub struct WriterBuilder {
    compression_type: CompressionType,
    compression_level: u32,
    block_size: usize,
}

impl Default for WriterBuilder {
    fn default() -> WriterBuilder {
        WriterBuilder {
            compression_type: CompressionType::None,
            compression_level: 0,
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
        self.block_size = size;
        self
    }

    /// Creates the [`Writer`] that will write into the provided [`io::Write`] type.
    pub fn build<W: io::Write>(&self, mut writer: W) -> io::Result<Writer<W>> {
        // We first write the compression type.
        // TODO write a magic number too.
        writer.write_u8(self.compression_type as u8)?;

        Ok(Writer {
            block_builder: BlockBuilder::new(self.block_size),
            compression_type: self.compression_type,
            compression_level: self.compression_level,
            writer,
        })
    }

    /// Creates the [`Writer`] that will write into a [`Vec`] of bytes.
    pub fn memory(&mut self) -> Writer<Vec<u8>> {
        self.build(Vec::new()).unwrap()
    }
}

/// A struct you can use to write entries into any [`io::Write`] type,
/// entries must be inserted in key-order.
pub struct Writer<W> {
    block_builder: BlockBuilder,
    compression_type: CompressionType,
    compression_level: u32,
    writer: W,
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
    pub fn new(writer: W) -> io::Result<Writer<W>> {
        WriterBuilder::new().build(writer)
    }

    /// Writes the provided entry into the underlying [`io::Write`] type,
    /// key-values must be given in key-order.
    pub fn insert<A, B>(&mut self, key: A, val: B) -> io::Result<()>
    where
        A: AsRef<[u8]>,
        B: AsRef<[u8]>,
    {
        if self.block_builder.insert(key.as_ref(), val.as_ref()) {
            let buffer = self.block_builder.finish();

            // Compress, write the compressed block length then the compressed block itself.
            let buffer = compress(self.compression_type, self.compression_level, buffer.as_ref())?;
            let block_len = buffer.len().try_into().unwrap();
            self.writer.write_u64::<BigEndian>(block_len)?;
            self.writer.write_all(&buffer)?;
        }
        Ok(())
    }

    /// Consumes this [`Writer`] and fetches the latest block currently being built.
    ///
    /// You must call this method before using the underlying [`io::Write`] type.
    pub fn finish(self) -> io::Result<()> {
        self.into_inner().map(drop)
    }

    /// Consumes this [`Writer`] and fetches the latest block currenty being built.
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
        let wb = Writer::builder();
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
        let mut wb = Writer::builder();
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
