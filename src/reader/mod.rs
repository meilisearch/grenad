use std::io;
use std::ops::RangeBounds;

pub use self::prefix_iter::{PrefixIter, RevPrefixIter};
pub use self::range_iter::{RangeIter, RevRangeIter};
pub use self::reader_cursor::ReaderCursor;
use crate::block::{Block, BlockCursor};
use crate::metadata::{FileVersion, Metadata};
use crate::{CompressionType, Error};

mod prefix_iter;
mod range_iter;
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

    /// Converts this [`Reader`] into a [`RevPrefixIter`].
    pub fn into_rev_prefix_iter(self, prefix: Vec<u8>) -> Result<RevPrefixIter<R>, Error> {
        self.into_cursor().map(|cursor| RevPrefixIter::new(cursor, prefix))
    }

    /// Converts this [`Reader`] into a [`RangeIter`].
    pub fn into_range_iter<S, A>(self, range: S) -> Result<RangeIter<R>, Error>
    where
        S: RangeBounds<A>,
        A: AsRef<[u8]>,
    {
        self.into_cursor().map(|cursor| RangeIter::new(cursor, range))
    }

    /// Converts this [`Reader`] into a [`RevRangeIter`].
    pub fn into_rev_range_iter<S, A>(self, range: S) -> Result<RevRangeIter<R>, Error>
    where
        S: RangeBounds<A>,
        A: AsRef<[u8]>,
    {
        self.into_cursor().map(|cursor| RevRangeIter::new(cursor, range))
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

    /// Gets a reference to the underlying reader.
    pub fn get_ref(&self) -> &R {
        &self.reader
    }
}
