use std::io::{self, ErrorKind};
use std::mem;

use byteorder::ReadBytesExt;

use crate::block::Block;
use crate::compression::CompressionType;
use crate::Error;

/// A struct that is able to read a grenad file that has been created by a [`crate::Writer`].
#[derive(Clone)]
pub struct Reader<R> {
    compression_type: CompressionType,
    reader: R,
    current_block: Option<Block>,
}

impl<R: io::Read> Reader<R> {
    /// Creates a [`Reader`] that will read from the provided [`io::Read`] type.
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
        let current_block = Block::new(&mut reader, compression_type)?;
        Ok(Reader { compression_type, reader, current_block })
    }

    /// Yields the entries in key-order.
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        // match &mut self.current_block {
        //     Some(block) => {
        //         match block.next() {
        //             Some((key, val)) => {
        //                 // This is a trick to make the compiler happy...
        //                 // https://github.com/rust-lang/rust/issues/47680
        //                 let key: &'static _ = unsafe { mem::transmute(key) };
        //                 let val: &'static _ = unsafe { mem::transmute(val) };
        //                 Ok(Some((key, val)))
        //             }
        //             None => {
        //                 if !block.read_from(&mut self.reader)? {
        //                     return Ok(None);
        //                 }
        //                 Ok(block.next())
        //             }
        //         }
        //     }
        //     None => Ok(None),
        // }
        todo!()
    }
}

impl<R> Reader<R> {
    /// Returns the [`CompressionType`] used by the underlying [`io::Read`] type.
    pub fn compression_type(&self) -> CompressionType {
        self.compression_type
    }

    /// Returns the value currently pointed by the [`BlockReader`].
    pub(crate) fn current(&self) -> Option<(&[u8], &[u8])> {
        // let b = self.current_block.as_ref()?;
        // let (key, value, _offset) = b.current()?;
        // Some((key, value))
        todo!()
    }

    /// Consumes the [`Reader`] and returns the underlying [`io::Read`] type.
    ///
    /// The returned [`io::Read`] type has been [`io::Seek`]ed which means that
    /// you must seek it back to the front to be read from the start.
    pub fn into_inner(self) -> R {
        self.reader
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
}
