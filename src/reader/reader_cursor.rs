use std::convert::TryInto;
use std::io;
use std::io::SeekFrom;
use std::ops::Deref;

use crate::reader::{Block, BlockCursor};
use crate::{CompressionType, Error, Reader};

/// A cursor that can move forward backward and move on a specified key.
#[derive(Clone)]
pub struct ReaderCursor<R> {
    index_block_cursor: IndexBlockCursor,
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

    /// Resets the position of the cursor.
    ///
    /// Useful when you want to be able to call `move_on_next` or `move_on_prev` in a loop
    /// and ensure that it will start from the first or the last value of the cursor.
    pub fn reset(&mut self) {
        self.current_cursor = None;
        self.index_block_cursor.reset();
    }
}

impl<R: io::Read + io::Seek> ReaderCursor<R> {
    /// Creates a new [`ReaderCursor`] by consumming a [`Reader`].
    pub(crate) fn new(reader: Reader<R>) -> Result<ReaderCursor<R>, Error> {
        Ok(ReaderCursor {
            index_block_cursor: IndexBlockCursor::new(
                reader.index_block_offset(),
                reader.compression_type(),
                reader.index_levels(),
            ),
            current_cursor: None,
            reader,
        })
    }

    /// Returns the block containing the entries that is following the current one.
    pub(crate) fn next_block_from_index(&mut self) -> Result<Option<Block>, Error> {
        match self.index_block_cursor.move_on_next(&mut self.reader.reader)? {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                let compression_type = self.reader.compression_type();
                Block::new(&mut self.reader.reader, compression_type).map(Some)
            }
            None => Ok(None),
        }
    }

    /// Returns the block containing the entries that is preceding the current one.
    pub(crate) fn prev_block_from_index(&mut self) -> Result<Option<Block>, Error> {
        match self.index_block_cursor.move_on_prev(&mut self.reader.reader)? {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                let compression_type = self.reader.compression_type();
                Block::new(&mut self.reader.reader, compression_type).map(Some)
            }
            None => Ok(None),
        }
    }

    /// Moves the cursor on the first entry and returns it.
    pub fn move_on_first(&mut self) -> crate::Result<Option<(&[u8], &[u8])>> {
        match self.index_block_cursor.move_on_first(&mut self.reader.reader)? {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                let compression_type = self.reader.compression_type();
                let current_cursor = self.current_cursor.insert(
                    Block::new(&mut self.reader.reader, compression_type)
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
    pub fn move_on_last(&mut self) -> crate::Result<Option<(&[u8], &[u8])>> {
        match self.index_block_cursor.move_on_last(&mut self.reader.reader)? {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                let compression_type = self.reader.compression_type();
                let current_cursor = self.current_cursor.insert(
                    Block::new(&mut self.reader.reader, compression_type)
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
    pub fn move_on_next(&mut self) -> crate::Result<Option<(&[u8], &[u8])>> {
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
    pub fn move_on_prev(&mut self) -> crate::Result<Option<(&[u8], &[u8])>> {
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
    ) -> crate::Result<Option<(&[u8], &[u8])>> {
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
    ) -> crate::Result<Option<(&[u8], &[u8])>> {
        // We move on the block which has a key greater than or equal to the key we are
        // searching for as the key stored in the index block is the last key of the block.
        let key = key.as_ref();
        match self
            .index_block_cursor
            .move_on_key_greater_than_or_equal_to(key, &mut self.reader.reader)?
        {
            Some((_, offset_bytes)) => {
                let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                self.reader.reader.seek(SeekFrom::Start(offset))?;
                let compression_type = self.reader.compression_type();
                let current_cursor = self.current_cursor.insert(
                    Block::new(&mut self.reader.reader, compression_type)
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
    ) -> crate::Result<Option<(&[u8], &[u8])>> {
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

/// Represent an n-depth index block cursor.
#[derive(Clone)]
struct IndexBlockCursor {
    base_block_offset: u64,
    compression_type: CompressionType,
    index_levels: u8,
    /// Defines the different index block cursor for each depth,
    /// associated with the offset at which it has been retrieved.
    /// The length of it must be index_levels + 1 once initialized.
    inner: Option<Vec<(u64, BlockCursor<Block>)>>,
}

impl IndexBlockCursor {
    fn new(
        base_block_offset: u64,
        compression_type: CompressionType,
        index_levels: u8,
    ) -> IndexBlockCursor {
        IndexBlockCursor { base_block_offset, compression_type, index_levels, inner: None }
    }

    fn reset(&mut self) {
        self.inner = None;
    }

    fn move_on_first<R: io::Read + io::Seek>(
        &mut self,
        reader: R,
    ) -> crate::Result<Option<(&[u8], &[u8])>> {
        self.iter_index_blocks(reader, |c| c.move_on_first())
    }

    fn move_on_last<R: io::Read + io::Seek>(
        &mut self,
        reader: R,
    ) -> crate::Result<Option<(&[u8], &[u8])>> {
        self.iter_index_blocks(reader, |c| c.move_on_last())
    }

    fn move_on_next<R: io::Read + io::Seek>(
        &mut self,
        reader: R,
    ) -> crate::Result<Option<(&[u8], &[u8])>> {
        self.recursive_index_block(reader, |c| c.move_on_next())
    }

    fn move_on_prev<R: io::Read + io::Seek>(
        &mut self,
        reader: R,
    ) -> crate::Result<Option<(&[u8], &[u8])>> {
        self.recursive_index_block(reader, |c| c.move_on_prev())
    }

    fn move_on_key_greater_than_or_equal_to<R: io::Read + io::Seek>(
        &mut self,
        key: &[u8],
        reader: R,
    ) -> crate::Result<Option<(&[u8], &[u8])>> {
        self.iter_index_blocks(reader, |c| c.move_on_key_greater_than_or_equal_to(key))
    }

    fn iter_index_blocks<R, F>(
        &mut self,
        mut reader: R,
        mut mov: F,
    ) -> crate::Result<Option<(&[u8], &[u8])>>
    where
        R: io::Read + io::Seek,
        F: FnMut(&mut BlockCursor<Block>) -> Option<(&[u8], &[u8])>,
    {
        match self.inner.as_mut() {
            Some(inner) => {
                let mut jump_to_offset = self.base_block_offset;
                for (offset, cursor) in inner {
                    // Only seek and load the Block if it is not already the one that we
                    // have in memory, we know that by checking the offset of the blocks.
                    if jump_to_offset != *offset {
                        reader.seek(SeekFrom::Start(jump_to_offset))?;
                        *cursor = Block::new(&mut reader, self.compression_type)
                            .map(Block::into_cursor)?;
                        *offset = jump_to_offset;
                    }

                    match (mov)(cursor) {
                        Some((_, offset_bytes)) => {
                            let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                            jump_to_offset = offset;
                        }
                        None => return Ok(None),
                    }
                }
            }
            None => self.inner = self.initial_index_blocks(reader, mov)?,
        }

        // We return the position pointed by the last index block level.
        // The last index blocks exposes the offsets of the data blocks.
        match self.inner.as_ref().and_then(|inner| inner.last()) {
            Some((_, cursor)) => Ok(cursor.current()),
            None => Ok(None),
        }
    }

    fn recursive_index_block<R, FM>(
        &mut self,
        mut reader: R,
        mut mov: FM,
    ) -> crate::Result<Option<(&[u8], &[u8])>>
    where
        R: io::Read + io::Seek,
        FM: FnMut(&mut BlockCursor<Block>) -> Option<(&[u8], &[u8])>,
    {
        fn recursive<'a, S, FN>(
            reader: &mut S,
            compression_type: CompressionType,
            blocks: &'a mut [(u64, BlockCursor<Block>)],
            mov: &mut FN,
        ) -> crate::Result<Option<(&'a [u8], &'a [u8])>>
        where
            S: io::Read + io::Seek,
            FN: FnMut(&mut BlockCursor<Block>) -> Option<(&[u8], &[u8])>,
        {
            match blocks.split_last_mut() {
                Some(((_offset, cursor), head)) => {
                    match (mov)(cursor) {
                        Some((_key, _offset)) => Ok(cursor.current()),
                        None => {
                            // We reached the end of the current index block, so we ask for the
                            // parent index block to execute the exact same user function and
                            // return the offset where is located the block on which we must be
                            // able to try again.
                            match recursive(reader, compression_type, head, mov)? {
                                Some((_, offset_bytes)) => {
                                    let offset =
                                        offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                                    reader.seek(SeekFrom::Start(offset))?;
                                    *cursor = Block::new(reader, compression_type)
                                        .map(Block::into_cursor)?;

                                    // We return the result of the call has is. If it returns None
                                    // it means we are not able to execute the `mov` function.
                                    Ok((mov)(cursor))
                                }
                                None => Ok(None),
                            }
                        }
                    }
                }
                // If we reach this branch it means that the base index block was
                // not able to execute the `mov` function and that we reached the
                // end of everything! We must STOP!
                None => Ok(None),
            }
        }

        if self.inner.is_none() {
            self.inner = self.initial_index_blocks(&mut reader, &mut mov)?;
        }

        match &mut self.inner {
            Some(inner) => recursive(&mut reader, self.compression_type, inner, &mut mov),
            None => Ok(None),
        }
    }

    /// Returns the index block cursors by calling the user function to load the blocks.
    #[allow(clippy::type_complexity)] // Return type is not THAT complex
    fn initial_index_blocks<R, FM>(
        &mut self,
        mut reader: R,
        mut mov: FM,
    ) -> crate::Result<Option<Vec<(u64, BlockCursor<Block>)>>>
    where
        R: io::Read + io::Seek,
        FM: FnMut(&mut BlockCursor<Block>) -> Option<(&[u8], &[u8])>,
    {
        let depth = self.index_levels as usize + 1;
        let mut inner = Vec::with_capacity(depth);

        let mut jump_to_offset = self.base_block_offset;
        for _ in 0..depth {
            reader.seek(SeekFrom::Start(jump_to_offset))?;
            let mut cursor =
                Block::new(&mut reader, self.compression_type).map(Block::into_cursor)?;

            match (mov)(&mut cursor) {
                Some((_key, offset_bytes)) => {
                    let offset = offset_bytes.try_into().map(u64::from_be_bytes).unwrap();
                    jump_to_offset = offset;
                    inner.push((jump_to_offset, cursor));
                }
                None => return Ok(None),
            }
        }

        Ok(Some(inner))
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
    fn simple_empty() {
        let writer = Writer::builder().index_levels(2).memory();
        let bytes = writer.into_inner().unwrap();
        let reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();

        let mut cursor = reader.into_cursor().unwrap();
        let result = cursor.move_on_key_greater_than_or_equal_to([0, 0, 0, 0]).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn no_compression() {
        let wb = Writer::builder();
        let mut writer = wb.build(Vec::new());

        for x in 0..2000u32 {
            let x = x.to_be_bytes();
            writer.insert(x, x).unwrap();
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
            writer.insert(x, x).unwrap();
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
            writer.insert(x, x).unwrap();
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
                        .move_on_key_greater_than_or_equal_to(n.to_be_bytes())
                        .unwrap()
                        .unwrap();
                    let k = k.try_into().map(i32::from_be_bytes).unwrap();
                    assert_eq!(k, n);
                }
                Err(i) => {
                    let k = cursor
                        .move_on_key_greater_than_or_equal_to(n.to_be_bytes())
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
            writer.insert(x, x).unwrap();
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
                        .move_on_key_lower_than_or_equal_to(n.to_be_bytes())
                        .unwrap()
                        .unwrap();
                    let k = k.try_into().map(i32::from_be_bytes).unwrap();
                    assert_eq!(k, n);
                }
                Err(i) => {
                    let k = cursor
                        .move_on_key_lower_than_or_equal_to(n.to_be_bytes())
                        .unwrap()
                        .map(|(k, _)| k.try_into().map(i32::from_be_bytes).unwrap());
                    let expected = i.checked_sub(1).and_then(|i| nums.get(i)).copied();
                    assert_eq!(k, expected, "queried value {}", n);
                }
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn easy_move_on_key_greater_than_or_equal_index_levels_2() {
        let mut wb = Writer::builder();
        wb.index_levels(2);
        let mut writer = wb.memory();
        let mut nums = Vec::new();
        for x in (10..24000i32).step_by(3) {
            nums.push(x);
            let x = x.to_be_bytes();
            writer.insert(x, x).unwrap();
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
                        .move_on_key_greater_than_or_equal_to(n.to_be_bytes())
                        .unwrap()
                        .unwrap();
                    let k = k.try_into().map(i32::from_be_bytes).unwrap();
                    assert_eq!(k, n);
                }
                Err(i) => {
                    let k = cursor
                        .move_on_key_greater_than_or_equal_to(n.to_be_bytes())
                        .unwrap()
                        .map(|(k, _)| k.try_into().map(i32::from_be_bytes).unwrap());
                    assert_eq!(k, nums.get(i).copied());
                }
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn easy_move_on_key_lower_than_or_equal_index_levels_2() {
        let mut wb = Writer::builder();
        wb.index_levels(2);
        let mut writer = wb.memory();
        let mut nums = Vec::new();
        for x in (10..24000i32).step_by(3) {
            nums.push(x);
            let x = x.to_be_bytes();
            writer.insert(x, x).unwrap();
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
                        .move_on_key_lower_than_or_equal_to(n.to_be_bytes())
                        .unwrap()
                        .unwrap();
                    let k = k.try_into().map(i32::from_be_bytes).unwrap();
                    assert_eq!(k, n);
                }
                Err(i) => {
                    let k = cursor
                        .move_on_key_lower_than_or_equal_to(n.to_be_bytes())
                        .unwrap()
                        .map(|(k, _)| k.try_into().map(i32::from_be_bytes).unwrap());
                    let expected = i.checked_sub(1).and_then(|i| nums.get(i)).copied();
                    assert_eq!(k, expected, "queried value {}", n);
                }
            }
        }
    }

    quickcheck! {
        #[cfg_attr(miri, ignore)]
        fn qc_compare_to_binary_search(nums: Vec<u32>, queries: Vec<u32>) -> bool {
            let mut nums = nums;
            nums.sort_unstable();
            nums.dedup();

            let mut writer = Writer::builder().index_levels(2).memory();
            for &x in &nums {
                let x = x.to_be_bytes();
                writer.insert(x, x).unwrap();
            }

            let bytes = writer.into_inner().unwrap();
            let reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();
            let mut cursor = reader.into_cursor().unwrap();

            for q in queries {
                match nums.binary_search(&q) {
                    Ok(i) => {
                        let q = nums[i];
                        let (k, _) = cursor
                            .move_on_key_lower_than_or_equal_to(q.to_be_bytes())
                            .unwrap()
                            .unwrap();
                        let k = k.try_into().map(u32::from_be_bytes).unwrap();
                        if k != q {
                            return false;
                        }
                    }
                    Err(i) => {
                        let k = cursor
                            .move_on_key_lower_than_or_equal_to(q.to_be_bytes())
                            .unwrap()
                            .map(|(k, _)| k.try_into().map(u32::from_be_bytes).unwrap());
                        let expected = i.checked_sub(1).and_then(|i| nums.get(i)).copied();
                        if k != expected {
                            return false;
                        }
                    }
                }
            }

            return true;
        }
    }
}
