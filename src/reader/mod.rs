use std::ops::{Bound, RangeBounds};
use std::{io, mem};

pub use self::prefix_iter::PrefixIter;
pub use self::reader_cursor::ReaderCursor;
use crate::block::{Block, BlockCursor};
use crate::metadata::{FileVersion, Metadata};
use crate::{CompressionType, Error};

mod prefix_iter;
mod reader_cursor;

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
    pub fn into_cursor(self) -> Result<ReaderCursor<R>, Error> {
        ReaderCursor::new(self)
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

    /// Returns the next entry that is inside of the given range.
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
    use std::convert::TryInto;
    use std::io::Cursor;

    use rand::Rng;

    use super::*;
    use crate::writer::Writer;

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
