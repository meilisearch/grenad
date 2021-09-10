#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

mod block_builder;
mod compression;
mod error;
mod sorter;
mod stream_merger;
mod stream_reader;
mod stream_writer;
mod varint;

pub use self::compression::CompressionType;
pub use self::error::Error;
#[cfg(feature = "tempfile")]
pub use self::sorter::TempFileChunk;
pub use self::sorter::{ChunkCreator, CursorVec, DefaultChunkCreator, Sorter, SorterBuilder};
pub use self::stream_merger::{StreamMerger, StreamMergerBuilder, StreamMergerIter};
pub use self::stream_reader::StreamReader;
pub use self::stream_writer::{StreamWriter, StreamWriterBuilder};
