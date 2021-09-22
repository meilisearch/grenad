#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

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
pub use self::reader::{PrefixIter, RangeIter, Reader, ReaderCursor};
#[cfg(feature = "tempfile")]
pub use self::sorter::TempFileChunk;
pub use self::sorter::{ChunkCreator, CursorVec, DefaultChunkCreator, Sorter, SorterBuilder};
pub use self::writer::{Writer, WriterBuilder};
