use std::convert::TryInto;
use std::io::{self, SeekFrom};
use std::mem;

use crate::block::{Block, BlockCursor};
use crate::metadata::{FileVersion, Metadata};
use crate::{CompressionType, Error};

/// A struct that is able to read a grenad file that has been created by a [`crate::Writer`].
#[derive(Clone)]
pub struct Reader<R> {
    metadata: Metadata,
    reader: R,
    index_block_cursor: BlockCursor<Block>,
    current_cursor: Option<BlockCursor<Block>>,
}

impl<R: io::Read + io::Seek> Reader<R> {
    /// Creates a [`Reader`] that will read from the provided [`io::Read`] type.
    pub fn new(mut reader: R) -> Result<Reader<R>, Error> {
        let metadata = Metadata::read_from(&mut reader)?;

        reader.seek(SeekFrom::Start(metadata.index_block_offset))?;
        let index_block_cursor =
            Block::new(&mut reader, metadata.compression_type).map(BlockCursor::new)?;

        Ok(Reader { metadata, reader, index_block_cursor, current_cursor: None })
    }

    /// Yields the entries in key-order.
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        match &mut self.current_cursor {
            Some(cursor) => {
                match cursor.move_on_next() {
                    Some((key, val)) => {
                        // This is a trick to make the compiler happy...
                        // https://github.com/rust-lang/rust/issues/47680
                        let key: &'static _ = unsafe { mem::transmute(key) };
                        let val: &'static _ = unsafe { mem::transmute(val) };
                        Ok(Some((key, val)))
                    }
                    None => match self.index_block_cursor.move_on_next() {
                        Some((_, offset_bytes)) => {
                            let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                            self.reader.seek(SeekFrom::Start(offset))?;
                            self.current_cursor = Some(
                                Block::new(&mut self.reader, self.metadata.compression_type)
                                    .map(BlockCursor::new)?,
                            );
                            self.next()
                        }
                        None => Ok(None),
                    },
                }
            }
            None => match self.index_block_cursor.move_on_next() {
                Some((_, offset_bytes)) => {
                    let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                    self.reader.seek(SeekFrom::Start(offset))?;
                    self.current_cursor = Some(
                        Block::new(&mut self.reader, self.metadata.compression_type)
                            .map(BlockCursor::new)?,
                    );
                    self.next()
                }
                None => Ok(None),
            },
        }
    }
}

impl<R> Reader<R> {
    /// Returns the version of this file.
    pub fn file_version(&self) -> FileVersion {
        self.metadata.file_version
    }

    /// Returns the compression type of this file.
    pub fn compression_type(&self) -> CompressionType {
        self.metadata.compression_type
    }

    /// Returns the number of entries in this file.
    pub fn len(&self) -> u64 {
        self.metadata.entries_count
    }

    /// Returns weither this file contains entries or is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the value currently pointed by the [`BlockReader`].
    pub(crate) fn current(&self) -> Option<(&[u8], &[u8])> {
        self.current_cursor.as_ref().and_then(BlockCursor::current)
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
    use std::io::Cursor;

    use super::*;
    use crate::compression::CompressionType;
    use crate::writer::Writer;

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

        let mut reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();
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
        let buffer = Writer::memory().into_inner().unwrap();
        let mut reader = Reader::new(Cursor::new(buffer)).unwrap();
        assert_eq!(reader.next().unwrap(), None);
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

        let mut reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();
        let mut x: u32 = 0;

        while let Some((k, v)) = reader.next().unwrap() {
            assert_eq!(k, x.to_be_bytes());
            assert_eq!(v, x.to_be_bytes());
            x += 1;
        }

        assert_eq!(x, 2000);
    }
}
