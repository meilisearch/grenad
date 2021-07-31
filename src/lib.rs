#[cfg(test)]
#[macro_use]
extern crate quickcheck;

mod block_builder;
mod compression;
mod error;
mod file_fuse;
mod merger;
mod reader;
mod sorter;
mod varint;
mod writer;

pub use self::compression::CompressionType;
pub use self::error::Error;
pub use self::file_fuse::{FileFuse, FileFuseBuilder};
pub use self::merger::{Merger, MergerBuilder, MergerIter};
pub use self::reader::Reader;
pub use self::sorter::{Sorter, SorterBuilder};
pub use self::writer::{Writer, WriterBuilder};
