use std::convert::TryInto;
use std::io::{self, SeekFrom};
use std::mem;
use std::ops::{Bound, Deref, RangeBounds};

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

    /// Converts this [`Reader`] into a [`ReaderCursor`].
    pub fn into_cursor(mut self) -> Result<ReaderCursor<R>, Error> {
        self.reader.seek(SeekFrom::Start(self.metadata.index_block_offset))?;
        let index_block = Block::new(&mut self.reader, self.metadata.compression_type)?;
        Ok(ReaderCursor {
            index_block_cursor: index_block.into_cursor(),
            current_cursor: None,
            reader: self,
        })
    }

    /// Converts this [`Reader`] into a [`PrefixIter`].
    pub fn into_prefix_iter(self, prefix: Vec<u8>) -> Result<PrefixIter<R>, Error> {
        self.into_cursor().map(|cursor| PrefixIter::new(cursor, prefix))
    }

    /// Converts this [`Reader`] into a [`RangeIter`].
    pub fn into_range_iter<S, A>(self, range: S) -> Result<RangeIter<R>, Error>
    where
        S: RangeBounds<A>,
        A: AsRef<[u8]>,
    {
        self.into_cursor().map(|cursor| RangeIter::new(cursor, range))
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

    /// Consumes the [`Reader`] and returns the underlying [`io::Read`] type.
    ///
    /// The returned [`io::Read`] type has been [`io::Seek`]ed which means that
    /// you must seek it back to the front to be read from the start.
    pub fn into_inner(self) -> R {
        self.reader.into_inner()
    }
}

impl<R: io::Read + io::Seek> ReaderCursor<R> {
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
                // This is a trick to make the compiler happy...
                // https://github.com/rust-lang/rust/issues/47680
                let key: &'static _ = unsafe { mem::transmute(key) };
                let val: &'static _ = unsafe { mem::transmute(val) };
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
                // This is a trick to make the compiler happy...
                // https://github.com/rust-lang/rust/issues/47680
                let key: &'static _ = unsafe { mem::transmute(key) };
                let val: &'static _ = unsafe { mem::transmute(val) };
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
    pub fn move_on_key_lower_than_or_equal_to(
        &mut self,
        target_key: &[u8],
    ) -> Result<Option<(&[u8], &[u8])>, Error> {
        match self.move_on_key_greater_than_or_equal_to(target_key)? {
            Some((key, val)) if key == target_key => {
                // This is a trick to make the compiler happy...
                // https://github.com/rust-lang/rust/issues/47680
                let key: &'static _ = unsafe { mem::transmute(key) };
                let val: &'static _ = unsafe { mem::transmute(val) };
                Ok(Some((key, val)))
            }
            Some(_) => self.move_on_prev(),
            None => self.move_on_last().map(|opt| opt.filter(|(key, _)| *key <= target_key)),
        }
    }

    /// Moves the cursor on the entry with a key greater than or equal to the
    /// specified one and returns the corresponding entry.
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
                        .map(Block::into_cursor)?,
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

/// An iterator that is able to yield all the entries with
/// a key that starts with a given prefix.
#[derive(Clone)]
pub struct PrefixIter<R> {
    cursor: ReaderCursor<R>,
    move_on_first_prefix: bool,
    prefix: Vec<u8>,
}

impl<R: io::Read + io::Seek> PrefixIter<R> {
    fn new(cursor: ReaderCursor<R>, prefix: Vec<u8>) -> PrefixIter<R> {
        PrefixIter { cursor, prefix, move_on_first_prefix: true }
    }

    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        let entry = if self.move_on_first_prefix {
            self.move_on_first_prefix = false;
            self.cursor.move_on_key_greater_than_or_equal_to(&self.prefix)?
        } else {
            self.cursor.move_on_next()?
        };

        match entry {
            Some((key, val)) if key.starts_with(&self.prefix) => {
                // This is a trick to make the compiler happy...
                // https://github.com/rust-lang/rust/issues/47680
                let key: &'static _ = unsafe { mem::transmute(key) };
                let val: &'static _ = unsafe { mem::transmute(val) };
                Ok(Some((key, val)))
            }
            _otherwise => Ok(None),
        }
    }
}

/// An iterator that is able to yield all the entries lying in a specified range.
#[derive(Clone)]
pub struct RangeIter<R> {
    cursor: ReaderCursor<R>,
    range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    move_on_start: bool,
}

impl<R: io::Read + io::Seek> RangeIter<R> {
    fn new<S, A>(cursor: ReaderCursor<R>, range: S) -> RangeIter<R>
    where
        S: RangeBounds<A>,
        A: AsRef<[u8]>,
    {
        let start = map_bound(range.start_bound(), |bytes| bytes.as_ref().to_vec());
        let end = map_bound(range.end_bound(), |bytes| bytes.as_ref().to_vec());
        RangeIter { cursor, range: (start, end), move_on_start: true }
    }

    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        let entry = if self.move_on_start {
            self.move_on_start = false;
            match self.range.start_bound() {
                Bound::Unbounded => self.cursor.move_on_first()?,
                Bound::Included(start) => {
                    self.cursor.move_on_key_greater_than_or_equal_to(start)?
                }
                Bound::Excluded(start) => {
                    match self.cursor.move_on_key_greater_than_or_equal_to(start)? {
                        Some((key, _)) if key == start => self.cursor.move_on_next()?,
                        Some((key, val)) => Some((key, val)),
                        None => None,
                    }
                }
            }
        } else {
            self.cursor.move_on_next()?
        };

        match entry {
            Some((key, val)) if end_contains(self.range.end_bound(), key) => {
                // This is a trick to make the compiler happy...
                // https://github.com/rust-lang/rust/issues/47680
                let key: &'static _ = unsafe { mem::transmute(key) };
                let val: &'static _ = unsafe { mem::transmute(val) };
                Ok(Some((key, val)))
            }
            _otherwise => Ok(None),
        }
    }
}

/// Map the internal bound type to another type.
fn map_bound<T, U, F: FnOnce(T) -> U>(bound: Bound<T>, f: F) -> Bound<U> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(x) => Bound::Included(f(x)),
        Bound::Excluded(x) => Bound::Excluded(f(x)),
    }
}

/// Returns weither the provided key doesn't outbound this end bound.
fn end_contains(end: Bound<&Vec<u8>>, key: &[u8]) -> bool {
    match end {
        Bound::Unbounded => true,
        Bound::Included(end) => key <= end,
        Bound::Excluded(end) => key < end,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::io::Cursor;

    use rand::Rng;

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

    #[test]
    fn simple_range() {
        let mut writer = Writer::memory();
        let mut nums = BTreeSet::new();
        for x in (10..24000i32).step_by(3) {
            nums.insert(x);
            let x = x.to_be_bytes();
            writer.insert(&x, &x).unwrap();
        }

        let bytes = writer.into_inner().unwrap();
        assert_ne!(bytes.len(), 0);

        let reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();

        let mut rng = rand::thread_rng();
        for _ in 0..2000 {
            let a: i32 = rng.gen_range(0..=24020);
            let b: i32 = rng.gen_range(a..=24020);

            let expected: Vec<_> = nums.range(a..=b).copied().collect();

            let range = a.to_be_bytes()..=b.to_be_bytes();
            let mut range_iter = reader.clone().into_range_iter(range).unwrap();
            let mut found = Vec::with_capacity(expected.len());
            while let Some((k, v)) = range_iter.next().unwrap() {
                let k = k.try_into().map(i32::from_be_bytes).unwrap();
                let v = v.try_into().map(i32::from_be_bytes).unwrap();
                found.push(k);
                assert_eq!(k, v);
            }

            assert_eq!(expected, found);
        }
    }
}
