use std::convert::TryInto;
use std::io::{self, SeekFrom};
use std::mem;
use std::ops::Deref;

use crate::block::{Block, BlockCursor};
use crate::metadata::{FileVersion, Metadata};
use crate::{CompressionType, Error};

/// A struct that is able to read a grenad file that has been created by a [`crate::Writer`].
#[derive(Clone)]
pub struct Reader<R> {
    metadata: Metadata,
    reader: R,
}

impl<R: io::Read + io::Seek> Reader<R> {
    /// Creates a [`Reader`] that will read from the provided [`io::Read`] type.
    pub fn new(mut reader: R) -> Result<Reader<R>, Error> {
        Metadata::read_from(&mut reader).map(|metadata| Reader { metadata, reader })
    }

    pub fn into_cursor(mut self) -> Result<ReaderCursor<R>, Error> {
        self.reader.seek(SeekFrom::Start(self.metadata.index_block_offset))?;
        let index_block = Block::new(&mut self.reader, self.metadata.compression_type)?;
        Ok(ReaderCursor {
            index_block_cursor: BlockCursor::new(index_block),
            current_cursor: None,
            reader: self,
        })
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

    /// Consumes the [`Reader`] and returns the underlying [`io::Read`] type.
    ///
    /// The returned [`io::Read`] type has been [`io::Seek`]ed which means that
    /// you must seek it back to the front to read it from the start.
    pub fn into_inner(self) -> R {
        self.reader
    }
}

#[derive(Clone)]
pub struct ReaderCursor<R> {
    index_block_cursor: BlockCursor<Block>,
    current_cursor: Option<BlockCursor<Block>>,
    reader: Reader<R>,
}

impl<R> ReaderCursor<R> {
    /// The currently pointed key and value pair.
    pub fn current(&self) -> Option<(&[u8], &[u8])> {
        self.current_cursor.as_ref().and_then(BlockCursor::current)
    }

    /// Consumes the [`Reader`] and returns the underlying [`io::Read`] type.
    ///
    /// The returned [`io::Read`] type has been [`io::Seek`]ed which means that
    /// you must seek it back to the front to be read from the start.
    pub fn into_inner(self) -> R {
        self.reader.into_inner()
    }
}

impl<R: io::Read + io::Seek> ReaderCursor<R> {
    pub(crate) fn next_block_from_index(&mut self) -> Result<Option<Block>, Error> {
        match self.index_block_cursor.move_on_next() {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                Block::new(&mut self.reader.reader, self.reader.metadata.compression_type).map(Some)
            }
            None => Ok(None),
        }
    }

    pub(crate) fn prev_block_from_index(&mut self) -> Result<Option<Block>, Error> {
        match self.index_block_cursor.move_on_prev() {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                Block::new(&mut self.reader.reader, self.reader.metadata.compression_type).map(Some)
            }
            None => Ok(None),
        }
    }

    pub fn move_on_first(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        match self.index_block_cursor.move_on_first() {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                let current_cursor = self.current_cursor.insert(
                    Block::new(&mut self.reader.reader, self.reader.metadata.compression_type)
                        .map(BlockCursor::new)?,
                );
                Ok(current_cursor.move_on_first())
            }
            None => {
                self.current_cursor = None;
                Ok(None)
            }
        }
    }

    pub fn move_on_last(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        match self.index_block_cursor.move_on_last() {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                let current_cursor = self.current_cursor.insert(
                    Block::new(&mut self.reader.reader, self.reader.metadata.compression_type)
                        .map(BlockCursor::new)?,
                );
                Ok(current_cursor.move_on_last())
            }
            None => {
                self.current_cursor = None;
                Ok(None)
            }
        }
    }

    pub fn move_on_next(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        match self.current_cursor.as_mut().map(BlockCursor::move_on_next) {
            Some(Some((key, val))) => {
                // This is a trick to make the compiler happy...
                // https://github.com/rust-lang/rust/issues/47680
                let key: &'static _ = unsafe { mem::transmute(key) };
                let val: &'static _ = unsafe { mem::transmute(val) };
                Ok(Some((key, val)))
            }
            Some(None) => match self.next_block_from_index()?.map(BlockCursor::new) {
                Some(current_cursor) => {
                    let current_cursor = self.current_cursor.insert(current_cursor);
                    Ok(current_cursor.move_on_first())
                }
                None => Ok(None),
            },
            None => self.move_on_first(),
        }
    }

    pub fn move_on_prev(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        match self.current_cursor.as_mut().map(BlockCursor::move_on_prev) {
            Some(Some((key, val))) => {
                // This is a trick to make the compiler happy...
                // https://github.com/rust-lang/rust/issues/47680
                let key: &'static _ = unsafe { mem::transmute(key) };
                let val: &'static _ = unsafe { mem::transmute(val) };
                Ok(Some((key, val)))
            }
            Some(None) => match self.prev_block_from_index()?.map(BlockCursor::new) {
                Some(current_cursor) => {
                    let current_cursor = self.current_cursor.insert(current_cursor);
                    Ok(current_cursor.move_on_last())
                }
                None => Ok(None),
            },
            None => self.move_on_last(),
        }
    }

    pub fn move_on_key_lower_than_or_equal_to(
        &mut self,
        key: &[u8],
    ) -> Result<Option<(&[u8], &[u8])>, Error> {
        // We move on the block which has a key greater than or equal to the key we are
        // searching for as the key stored in the index block is the last key of the block.
        // The key is assured to be between the last key of this block and the start of the previous block.
        match self.index_block_cursor.move_on_key_greater_than_or_equal_to(key) {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                let current_cursor = self.current_cursor.insert(
                    Block::new(&mut self.reader.reader, self.reader.metadata.compression_type)
                        .map(BlockCursor::new)?,
                );

                match current_cursor.move_on_key_lower_than_or_equal_to(key) {
                    Some((key, val)) => {
                        // This is a trick to make the compiler happy...
                        // https://github.com/rust-lang/rust/issues/47680
                        let key: &'static _ = unsafe { mem::transmute(key) };
                        let val: &'static _ = unsafe { mem::transmute(val) };
                        Ok(Some((key, val)))
                    }
                    None => {
                        // We search in the previous block if we can't find a key lower than
                        // the queried key in this one.
                        self.current_cursor = self.prev_block_from_index()?.map(BlockCursor::new);
                        Ok(self
                            .current_cursor
                            .as_mut()
                            .and_then(|cc| cc.move_on_key_lower_than_or_equal_to(key)))
                    }
                }
            }
            None => Ok(None),
        }
    }

    pub fn move_on_key_greater_than_or_equal_to(
        &mut self,
        key: &[u8],
    ) -> Result<Option<(&[u8], &[u8])>, Error> {
        // We move on the block which has a key greater than or equal to the key we are
        // searching for as the key stored in the index block is the last key of the block.
        match self.index_block_cursor.move_on_key_greater_than_or_equal_to(key) {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                let current_cursor = self.current_cursor.insert(
                    Block::new(&mut self.reader.reader, self.reader.metadata.compression_type)
                        .map(BlockCursor::new)?,
                );
                Ok(current_cursor.move_on_key_greater_than_or_equal_to(key))
            }
            None => Ok(None),
        }
    }
}

impl<R> Deref for ReaderCursor<R> {
    type Target = Reader<R>;

    fn deref(&self) -> &Self::Target {
        &self.reader
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

        let reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();
        let mut cursor = reader.into_cursor().unwrap();
        let mut x: u32 = 0;

        while let Some((k, v)) = cursor.move_on_next().unwrap() {
            assert_eq!(k, x.to_be_bytes());
            assert_eq!(v, x.to_be_bytes());
            x += 1;
        }

        assert_eq!(x, 2000);
    }

    #[test]
    fn empty() {
        let buffer = Writer::memory().into_inner().unwrap();
        let reader = Reader::new(Cursor::new(buffer)).unwrap();
        let mut cursor = reader.into_cursor().unwrap();
        assert_eq!(cursor.move_on_next().unwrap(), None);
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

        let reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();
        let mut cursor = reader.into_cursor().unwrap();
        let mut x: u32 = 0;

        while let Some((k, v)) = cursor.move_on_next().unwrap() {
            assert_eq!(k, x.to_be_bytes());
            assert_eq!(v, x.to_be_bytes());
            x += 1;
        }

        assert_eq!(x, 2000);
    }

    #[test]
    fn easy_move_on_key_greater_than_or_equal() {
        let mut writer = Writer::memory();
        let mut nums = Vec::new();
        for x in (10..24000i32).step_by(3) {
            nums.push(x);
            let x = x.to_be_bytes();
            writer.insert(&x, &x).unwrap();
        }

        let bytes = writer.into_inner().unwrap();
        assert_ne!(bytes.len(), 0);

        let reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();
        let mut cursor = reader.into_cursor().unwrap();

        for n in 0..24020i32 {
            match nums.binary_search(&n) {
                Ok(i) => {
                    let n = nums[i];
                    let (k, _) = cursor
                        .move_on_key_greater_than_or_equal_to(&n.to_be_bytes())
                        .unwrap()
                        .unwrap();
                    let k = k.try_into().map(i32::from_be_bytes).unwrap();
                    assert_eq!(k, n);
                }
                Err(i) => {
                    let k = cursor
                        .move_on_key_greater_than_or_equal_to(&n.to_be_bytes())
                        .unwrap()
                        .map(|(k, _)| k.try_into().map(i32::from_be_bytes).unwrap());
                    assert_eq!(k, nums.get(i).copied());
                }
            }
        }
    }

    #[test]
    fn easy_move_on_key_lower_than_or_equal() {
        let mut writer = Writer::memory();
        let mut nums = Vec::new();
        for x in (10..24000i32).step_by(3) {
            nums.push(x);
            let x = x.to_be_bytes();
            writer.insert(&x, &x).unwrap();
        }

        let bytes = writer.into_inner().unwrap();
        assert_ne!(bytes.len(), 0);

        let reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();
        let mut cursor = reader.into_cursor().unwrap();
        // for n in 0..24020i32 {
        let n = 2348;
        match nums.binary_search(&n) {
            Ok(i) => {
                let n = nums[i];
                let (k, _) =
                    cursor.move_on_key_lower_than_or_equal_to(&n.to_be_bytes()).unwrap().unwrap();
                let k = k.try_into().map(i32::from_be_bytes).unwrap();
                assert_eq!(k, n);
            }
            Err(i) => {
                let k = cursor
                    .move_on_key_lower_than_or_equal_to(&n.to_be_bytes())
                    .unwrap()
                    .map(|(k, _)| k.try_into().map(i32::from_be_bytes).unwrap());
                let expected = i.checked_sub(1).and_then(|i| nums.get(i)).copied();
                assert_eq!(k, expected, "queried value {}", n);
            }
        }
        // }
    }
}
