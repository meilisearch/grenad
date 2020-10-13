#[cfg(test)]
#[macro_use] extern crate quickcheck;

mod block_builder;
mod compression;
mod reader;
mod varint;
mod writer;

pub use self::compression::CompressionType;
pub use self::reader::Reader;
pub use self::writer::{Writer, WriterBuilder};
