use std::borrow::Cow;
use std::io::{self, ErrorKind};
use std::mem;

use byteorder::{BigEndian, ReadBytesExt};

use crate::compression::{decompress, CompressionType};
use crate::varint::varint_decode32;
use crate::Error;

#[derive(Clone)]
pub struct Reader<R> {
    compression_type: CompressionType,
    reader: R,
    current_block: Option<BlockReader>,
}

impl<R: io::Read> Reader<R> {
    pub fn new(mut reader: R) -> Result<Reader<R>, Error> {
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
        Ok(Reader { compression_type, reader, current_block })
    }

    pub fn compression_type(&self) -> CompressionType {
        self.compression_type
    }

    #[allow(clippy::should_implement_trait, clippy::type_complexity)]
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        match &mut self.current_block {
            Some(block) => {
                match block.next()? {
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
                        block.next()
                    }
                }
            }
            None => Ok(None),
        }
    }

    pub fn into_inner(self) -> R {
        self.reader
    }
}

#[derive(Clone)]
struct BlockReader {
    compression_type: CompressionType,
    buffer: Vec<u8>,
    offset: usize,
}

impl BlockReader {
    fn new<R: io::Read>(
        reader: &mut R,
        _type: CompressionType,
    ) -> Result<Option<BlockReader>, Error> {
        let mut block_reader =
            BlockReader { compression_type: _type, buffer: Vec::new(), offset: 0 };

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
        self.offset = 0;
        self.buffer.resize(block_len as usize, 0);
        reader.read_exact(&mut self.buffer)?;

        if let Cow::Owned(vec) = decompress(self.compression_type, &self.buffer)? {
            self.buffer = vec;
        }

        Ok(true)
    }

    #[allow(clippy::should_implement_trait, clippy::type_complexity)]
    fn next(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        if self.buffer.len() == self.offset {
            return Ok(None);
        }

        // Read the key length.
        let mut key_len = 0;
        let len = varint_decode32(&self.buffer[self.offset..], &mut key_len);
        self.offset += len;

        // Read the value length.
        let mut val_len = 0;
        let len = varint_decode32(&self.buffer[self.offset..], &mut val_len);
        self.offset += len;

        // Read the key itself.
        let key = &self.buffer[self.offset..self.offset + key_len as usize];
        self.offset += key_len as usize;

        // Read the value itself.
        let val = &self.buffer[self.offset..self.offset + val_len as usize];
        self.offset += val_len as usize;

        Ok(Some((key, val)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::Writer;

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

        let mut reader = Reader::new(bytes.as_slice()).unwrap();
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
        let mut reader = Reader::new(&[][..]).unwrap();
        assert_eq!(reader.next().unwrap(), None);
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

        let mut reader = Reader::new(bytes.as_slice()).unwrap();
        let mut x: u32 = 0;

        while let Some((k, v)) = reader.next().unwrap() {
            assert_eq!(k, x.to_be_bytes());
            assert_eq!(v, x.to_be_bytes());
            x += 1;
        }

        assert_eq!(x, 2000);
    }

    #[cfg(feature = "file-fuse")]
    #[test]
    fn file_fusing() {
        use crate::file_fuse::FileFuse;
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom};

        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open("target/reader-file-fusing")
            .unwrap();

        let wb = Writer::builder();
        let mut writer = wb.build(file).unwrap();

        for x in 0..2000u32 {
            let x = x.to_be_bytes();
            writer.insert(&x, &x).unwrap();
        }

        let mut file = writer.into_inner().unwrap();
        assert_ne!(file.metadata().unwrap().len(), 0);

        file.seek(SeekFrom::Start(0)).unwrap();
        let file = FileFuse::builder().shrink_size(4096).build(file);

        let mut reader = Reader::new(file).unwrap();
        let mut x: u32 = 0;

        while let Some((k, v)) = reader.next().unwrap() {
            assert_eq!(k, x.to_be_bytes());
            assert_eq!(v, x.to_be_bytes());
            x += 1;
        }

        assert_eq!(x, 2000);

        let file = reader.into_inner().into_inner();
        assert_eq!(file.metadata().unwrap().len(), 0);
    }
}
