#[cfg(test)]
#[macro_use]
extern crate quickcheck;

mod block_builder;
mod compression;
mod error;
mod merger;
mod reader;
mod sorter;
mod varint;
mod writer;

pub use self::compression::CompressionType;
pub use self::error::Error;
pub use self::merger::{Merger, MergerBuilder, MergerIter};
pub use self::reader::Reader;
#[cfg(feature = "tempfile")]
pub use self::sorter::TempFileChunk;
pub use self::sorter::{ChunkCreation, CursorVec, DefaultChunkCreation, Sorter, SorterBuilder};
pub use self::writer::{Writer, WriterBuilder};
