use std::borrow::Cow;
use std::io::{self, ErrorKind};
use std::mem;

use byteorder::{BigEndian, ReadBytesExt};

use crate::compression::{decompress, CompressionType};
use crate::varint::varint_decode32;
use crate::Error;

/// A struct that is able to read a grenad file that has been created by a [`crate::StreamWriter`].
#[derive(Clone)]
pub struct StreamReader<R> {
    compression_type: CompressionType,
    reader: R,
    current_block: Option<BlockReader>,
}

impl<R: io::Read> StreamReader<R> {
    /// Creates a [`StreamReader`] that will read from the provided [`io::Read`] type.
    pub fn new(mut reader: R) -> Result<StreamReader<R>, Error> {
        let compression = match reader.read_u8() {
            Ok(compression) => compression,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => CompressionType::None as u8,
            Err(e) => return Err(Error::from(e)),
        };
        let compression_type = match CompressionType::from_u8(compression) {
            Some(compression_type) => compression_type,
            None => return Err(Error::InvalidCompressionType),
        };
        let current_block = BlockReader::new(&mut reader, compression_type)?;
        Ok(StreamReader { compression_type, reader, current_block })
    }

    /// Yields the entries in key-order.
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        match &mut self.current_block {
            Some(block) => {
                match block.next() {
                    Some((key, val)) => {
                        // This is a trick to make the compiler happy...
                        // https://github.com/rust-lang/rust/issues/47680
                        let key: &'static _ = unsafe { mem::transmute(key) };
                        let val: &'static _ = unsafe { mem::transmute(val) };
                        Ok(Some((key, val)))
                    }
                    None => {
                        if !block.read_from(&mut self.reader)? {
                            return Ok(None);
                        }
                        Ok(block.next())
                    }
                }
            }
            None => Ok(None),
        }
    }
}

impl<R> StreamReader<R> {
    /// Returns the [`CompressionType`] used by the underlying [`io::Read`] type.
    pub fn compression_type(&self) -> CompressionType {
        self.compression_type
    }

    /// Returns the value currently pointed by the [`BlockReader`].
    pub(crate) fn current(&self) -> Option<(&[u8], &[u8])> {
        let b = self.current_block.as_ref()?;
        let offset = b.current_offset?;
        let (key, value, _offset) = BlockReader::current(&b.buffer, offset)?;
        Some((key, value))
    }

    /// Consumes the [`StreamReader`] and returns the underlying [`io::Read`] type.
    ///
    /// The returned [`io::Read`] type has been [`io::Seek`]ed which means that
    /// you must seek it back to the front to be read from the start.
    pub fn into_inner(self) -> R {
        self.reader
    }
}

#[derive(Clone)]
struct BlockReader {
    compression_type: CompressionType,
    buffer: Vec<u8>,
    current_offset: Option<usize>,
    next_offset: usize,
}

impl BlockReader {
    fn new<R: io::Read>(
        reader: &mut R,
        _type: CompressionType,
    ) -> Result<Option<BlockReader>, Error> {
        let mut block_reader = BlockReader {
            compression_type: _type,
            buffer: Vec::new(),
            current_offset: None,
            next_offset: 0,
        };

        if block_reader.read_from(reader)? {
            Ok(Some(block_reader))
        } else {
            Ok(None)
        }
    }

    /// Returns `true` if it was able to read a new BlockReader.
    fn read_from<R: io::Read>(&mut self, reader: &mut R) -> Result<bool, Error> {
        let block_len = match reader.read_u64::<BigEndian>() {
            Ok(block_len) => block_len,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(Error::from(e)),
        };

        // We reset the cursor's position and decompress
        // the block into the cursor's buffer.
        self.current_offset = None;
        self.next_offset = 0;
        self.buffer.resize(block_len as usize, 0);
        reader.read_exact(&mut self.buffer)?;

        if let Cow::Owned(vec) = decompress(self.compression_type, &self.buffer)? {
            self.buffer = vec;
        }

        Ok(true)
    }

    /// Returns the current key-value pair and the amount
    /// of bytes to advance to read the next one.
    fn current(buffer: &[u8], start_offset: usize) -> Option<(&[u8], &[u8], usize)> {
        if buffer.len() == start_offset {
            return None;
        }

        let mut offset = start_offset;

        // Read the key length.
        let mut key_len = 0;
        let len = varint_decode32(&buffer[offset..], &mut key_len);
        offset += len;

        // Read the value length.
        let mut val_len = 0;
        let len = varint_decode32(&buffer[offset..], &mut val_len);
        offset += len;

        // Read the key itself.
        let key = &buffer[offset..offset + key_len as usize];
        offset += key_len as usize;

        // Read the value itself.
        let val = &buffer[offset..offset + val_len as usize];
        offset += val_len as usize;

        Some((key, val, offset - start_offset))
    }

    fn next(&mut self) -> Option<(&[u8], &[u8])> {
        match Self::current(&self.buffer, self.next_offset) {
            Some((key, value, offset)) => {
                self.current_offset = Some(self.next_offset);
                self.next_offset += offset;
                Some((key, value))
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream_writer::StreamWriter;

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

        let mut reader = StreamReader::new(bytes.as_slice()).unwrap();
        let mut x: u32 = 0;

        while let Some((k, v)) = reader.next().unwrap() {
            assert_eq!(k, x.to_be_bytes());
            assert_eq!(v, x.to_be_bytes());
            x += 1;
        }

        assert_eq!(x, 2000);
    }

    #[test]
    fn empty() {
        let mut reader = StreamReader::new(&[][..]).unwrap();
        assert_eq!(reader.next().unwrap(), None);
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

        let mut reader = StreamReader::new(bytes.as_slice()).unwrap();
        let mut x: u32 = 0;

        while let Some((k, v)) = reader.next().unwrap() {
            assert_eq!(k, x.to_be_bytes());
            assert_eq!(v, x.to_be_bytes());
            x += 1;
        }

        assert_eq!(x, 2000);
    }
}
