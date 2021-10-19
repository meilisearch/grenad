use std::convert::TryInto;
use std::io;
use std::io::SeekFrom;
use std::ops::Deref;

use crate::reader::{Block, BlockCursor};
use crate::{Error, Reader};

/// A cursor that can move forward backward and move on a specified key.
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

    /// Consumes the [`ReaderCursor`] and returns the underlying [`Reader`] type.
    pub fn into_reader(self) -> Reader<R> {
        self.reader
    }

    /// Consumes the [`ReaderCursor`] and returns the underlying [`io::Read`] type.
    ///
    /// The returned [`io::Read`] type has been [`io::Seek`]ed which means that
    /// you must seek it back to the front to be read from the start.
    pub fn into_inner(self) -> R {
        self.reader.into_inner()
    }

    /// Gets a reference to the underlying reader.
    pub fn get_ref(&self) -> &R {
        self.reader.get_ref()
    }
}

impl<R: io::Read + io::Seek> ReaderCursor<R> {
    /// Creates a new [`ReaderCursor`] by consumming a [`Reader`].
    pub(crate) fn new(mut reader: Reader<R>) -> Result<ReaderCursor<R>, Error> {
        reader.reader.seek(SeekFrom::Start(reader.metadata.index_block_offset))?;
        let index_block = Block::new(&mut reader.reader, reader.metadata.compression_type)?;
        Ok(ReaderCursor {
            index_block_cursor: index_block.into_cursor(),
            current_cursor: None,
            reader,
        })
    }

    /// Returns the block containing the entries that is following the current one.
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

    /// Returns the block containing the entries that is preceding the current one.
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

    /// Moves the cursor on the first entry and returns it.
    pub fn move_on_first(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        match self.index_block_cursor.move_on_first() {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                let current_cursor = self.current_cursor.insert(
                    Block::new(&mut self.reader.reader, self.reader.metadata.compression_type)
                        .map(Block::into_cursor)?,
                );
                Ok(current_cursor.move_on_first())
            }
            None => {
                self.current_cursor = None;
                Ok(None)
            }
        }
    }

    /// Moves the cursor on the last entry and returns it.
    pub fn move_on_last(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        match self.index_block_cursor.move_on_last() {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                let current_cursor = self.current_cursor.insert(
                    Block::new(&mut self.reader.reader, self.reader.metadata.compression_type)
                        .map(Block::into_cursor)?,
                );
                Ok(current_cursor.move_on_last())
            }
            None => {
                self.current_cursor = None;
                Ok(None)
            }
        }
    }

    /// Moves the cursor on the entry following the current one and returns it.
    pub fn move_on_next(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        match self.current_cursor.as_mut().map(BlockCursor::move_on_next) {
            Some(Some((key, val))) => {
                let (key, val) = unsafe { crate::transmute_entry_to_static(key, val) };
                Ok(Some((key, val)))
            }
            Some(None) => match self.next_block_from_index()?.map(Block::into_cursor) {
                Some(current_cursor) => {
                    let current_cursor = self.current_cursor.insert(current_cursor);
                    Ok(current_cursor.move_on_first())
                }
                None => Ok(None),
            },
            None => self.move_on_first(),
        }
    }

    /// Moves the cursor on the entry preceding the current one and returns it.
    pub fn move_on_prev(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        match self.current_cursor.as_mut().map(BlockCursor::move_on_prev) {
            Some(Some((key, val))) => {
                let (key, val) = unsafe { crate::transmute_entry_to_static(key, val) };
                Ok(Some((key, val)))
            }
            Some(None) => match self.prev_block_from_index()?.map(Block::into_cursor) {
                Some(current_cursor) => {
                    let current_cursor = self.current_cursor.insert(current_cursor);
                    Ok(current_cursor.move_on_last())
                }
                None => Ok(None),
            },
            None => self.move_on_last(),
        }
    }

    /// Moves the cursor on the entry with a key lower than or equal to the
    /// specified one and returns the corresponding entry.
    pub fn move_on_key_lower_than_or_equal_to<A: AsRef<[u8]>>(
        &mut self,
        target_key: A,
    ) -> Result<Option<(&[u8], &[u8])>, Error> {
        let target_key = target_key.as_ref();
        match self.move_on_key_greater_than_or_equal_to(target_key)? {
            Some((key, val)) if key == target_key => {
                let (key, val) = unsafe { crate::transmute_entry_to_static(key, val) };
                Ok(Some((key, val)))
            }
            Some(_) => self.move_on_prev(),
            None => self.move_on_last().map(|opt| opt.filter(|(key, _)| *key <= target_key)),
        }
    }

    /// Moves the cursor on the entry with a key greater than or equal to the
    /// specified one and returns the corresponding entry.
    pub fn move_on_key_greater_than_or_equal_to<A: AsRef<[u8]>>(
        &mut self,
        key: A,
    ) -> Result<Option<(&[u8], &[u8])>, Error> {
        // We move on the block which has a key greater than or equal to the key we are
        // searching for as the key stored in the index block is the last key of the block.
        let key = key.as_ref();
        match self.index_block_cursor.move_on_key_greater_than_or_equal_to(key) {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                let current_cursor = self.current_cursor.insert(
                    Block::new(&mut self.reader.reader, self.reader.metadata.compression_type)
                        .map(Block::into_cursor)?,
                );
                Ok(current_cursor.move_on_key_greater_than_or_equal_to(key))
            }
            None => Ok(None),
        }
    }

    /// Moves the cursor on the entry with a key equal to the key specified and
    /// returns the corresponding entry.
    pub fn move_on_key_equal_to<A: AsRef<[u8]>>(
        &mut self,
        key: A,
    ) -> Result<Option<(&[u8], &[u8])>, Error> {
        let key = key.as_ref();
        self.move_on_key_greater_than_or_equal_to(key).map(|opt| opt.filter(|(k, _)| *k == key))
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
    use std::convert::TryInto;
    use std::io::Cursor;

    use super::*;
    use crate::compression::CompressionType;
    use crate::writer::Writer;

    #[test]
    #[cfg_attr(miri, ignore)]
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

    #[test]
    #[cfg_attr(miri, ignore)]
    #[cfg(feature = "snappy")]
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
    #[cfg_attr(miri, ignore)]
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
    #[cfg_attr(miri, ignore)]
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
        for n in 0..24020i32 {
            match nums.binary_search(&n) {
                Ok(i) => {
                    let n = nums[i];
                    let (k, _) = cursor
                        .move_on_key_lower_than_or_equal_to(&n.to_be_bytes())
                        .unwrap()
                        .unwrap();
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
        }
    }
}
