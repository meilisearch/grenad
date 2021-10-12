#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

use std::mem;

mod block;
mod block_writer;
mod compression;
mod count_write;
mod error;
mod merger;
mod metadata;
mod reader;
mod sorter;
mod varint;
mod writer;

pub use self::compression::CompressionType;
pub use self::error::Error;
pub use self::merger::{Merger, MergerBuilder, MergerIter};
pub use self::metadata::FileVersion;
pub use self::reader::{PrefixIter, RangeIter, Reader, ReaderCursor, RevPrefixIter, RevRangeIter};
#[cfg(feature = "tempfile")]
pub use self::sorter::TempFileChunk;
pub use self::sorter::{ChunkCreator, CursorVec, DefaultChunkCreator, Sorter, SorterBuilder};
pub use self::writer::{Writer, WriterBuilder};

/// Sometimes we need to use an unsafe trick to make the compiler happy.
/// You can read more about the issue [on the Rust's Github issues].
///
/// [on the Rust's Github issues]: https://github.com/rust-lang/rust/issues/47680
unsafe fn transmute_entry_to_static(key: &[u8], val: &[u8]) -> (&'static [u8], &'static [u8]) {
    (mem::transmute(key), mem::transmute(val))
}
