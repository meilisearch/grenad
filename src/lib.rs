#[cfg(test)]
#[macro_use] extern crate quickcheck;

mod block_builder;
mod compression;
mod error;
mod merger;
mod reader;
mod varint;
mod writer;

#[cfg(feature = "file-fuse")]
mod file_fuse;

pub use self::compression::CompressionType;
pub use self::error::Error;
pub use self::merger::Merger;
pub use self::reader::Reader;
pub use self::writer::{Writer, WriterBuilder};

#[cfg(feature = "file-fuse")]
pub use self::file_fuse::FileFuse;
