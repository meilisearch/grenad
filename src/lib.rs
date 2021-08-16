//! This library provides different ways to sort, merge, write,
//! and read key-value pairs efficiently.
//!
//! # Example: use the Writer and Reader structs
//!
//! You can use the [`Writer`] struct to store key-value pairs
//! into the specified [`std::io::Write`] type. The [`Reader`] type
//! can then be used to read the entries.
//!
//! ```
//! use std::io::Cursor;
//!
//! use grenad::{Reader, Writer};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut writer = Writer::memory();
//! writer.insert("first-counter", 119_u32.to_ne_bytes())?;
//! writer.insert("second-counter", 384_u32.to_ne_bytes())?;
//!
//! // We create a reader from our writer.
//! let cursor = writer.into_inner().map(Cursor::new)?;
//! let mut reader = Reader::new(cursor)?;
//!
//! // We can see that the sum of u32s is valid here.
//! assert_eq!(reader.next()?, Some((&b"first-counter"[..], &119_u32.to_ne_bytes()[..])));
//! assert_eq!(reader.next()?, Some((&b"second-counter"[..], &384_u32.to_ne_bytes()[..])));
//! assert_eq!(reader.next()?, None);
//! # Ok(()) }
//! ```
//!
//! # Example: use the Merger struct
//!
//! In this example we show how you can merge multiple [`Reader`]s
//! by using a merge function when a conflict is encountered.
//!
//! ```
//! use std::array::TryFromSliceError;
//! use std::borrow::Cow;
//! use std::convert::TryInto;
//! use std::io::Cursor;
//!
//! use grenad::{Merge, MergerBuilder, Reader, Writer};
//!
//! // This merger:
//! //  - parses u32s from native-endian bytes,
//! //  - wrapping sums them and,
//! //  - outputs the result as native-endian bytes.
//! #[derive(Clone, Copy)]
//! struct WrappingSumU32s;
//!
//! impl Merge for WrappingSumU32s {
//!     type Error = TryFromSliceError;
//!     type Output = [u8; 4];
//!
//!     fn merge<I, A>(&self, key: &[u8], values: I) -> Result<Self::Output, Self::Error>
//!     where
//!         I: IntoIterator<Item = A>,
//!         A: AsRef<[u8]>
//!     {
//!         let mut output: u32 = 0;
//!         for value in values {
//!             let num = value.as_ref().try_into().map(u32::from_ne_bytes)?;
//!             output = output.wrapping_add(num);
//!         }
//!         Ok(output.to_ne_bytes())
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // We create our writers in memory to insert our key-value pairs.
//! let mut writera = Writer::memory();
//! let mut writerb = Writer::memory();
//! let mut writerc = Writer::memory();
//!
//! // We insert our key-value pairs in order and
//! // mix them between our writers.
//! writera.insert("first-counter", 32_u32.to_ne_bytes())?;
//! writera.insert("second-counter", 64_u32.to_ne_bytes())?;
//! writerb.insert("first-counter", 23_u32.to_ne_bytes())?;
//! writerb.insert("second-counter", 320_u32.to_ne_bytes())?;
//! writerc.insert("first-counter", 64_u32.to_ne_bytes())?;
//!
//! // We create readers from our writers.
//! let cursora = writera.into_inner().map(Cursor::new)?;
//! let cursorb = writerb.into_inner().map(Cursor::new)?;
//! let cursorc = writerc.into_inner().map(Cursor::new)?;
//! let readera = Reader::new(cursora)?;
//! let readerb = Reader::new(cursorb)?;
//! let readerc = Reader::new(cursorc)?;
//!
//! // We create a merger that will sum our u32s when necessary,
//! // and we add our readers to the list of readers to merge.
//! let merger_builder = MergerBuilder::new(WrappingSumU32s);
//! let merger = merger_builder.add(readera).add(readerb).add(readerc).build();
//!
//! // We can iterate over the entries in key-order.
//! let mut iter = merger.into_merger_iter()?;
//!
//! // We can see that the sum of u32s is valid here.
//! assert_eq!(iter.next()?, Some((&b"first-counter"[..], &119_u32.to_ne_bytes()[..])));
//! assert_eq!(iter.next()?, Some((&b"second-counter"[..], &384_u32.to_ne_bytes()[..])));
//! assert_eq!(iter.next()?, None);
//! # Ok(()) }
//! ```
//!
//! # Example: use the Sorter struct
//!
//! In this example we show how by defining a merge function,
//! you can insert multiple entries with the same key and output
//! them in key-order.
//!
//! ```
//! use std::array::TryFromSliceError;
//! use std::borrow::Cow;
//! use std::convert::TryInto;
//!
//! use grenad::{Merge, CursorVec, SorterBuilder};
//!
//! // This merger:
//! //  - parses u32s from native-endian bytes,
//! //  - wrapping sums them and,
//! //  - outputs the result as native-endian bytes.
//! #[derive(Clone, Copy)]
//! struct WrappingSumU32s;
//!
//! impl Merge for WrappingSumU32s {
//!     type Error = TryFromSliceError;
//!     type Output = [u8; 4];
//!
//!     fn merge<I, A>(&self, key: &[u8], values: I) -> Result<Self::Output, Self::Error>
//!     where
//!         I: IntoIterator<Item = A>,
//!         A: AsRef<[u8]>
//!     {
//!         let mut output: u32 = 0;
//!         for value in values {
//!             let num = value.as_ref().try_into().map(u32::from_ne_bytes)?;
//!             output = output.wrapping_add(num);
//!         }
//!         Ok(output.to_ne_bytes())
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // We create a sorter that will sum our u32s when necessary.
//! let mut sorter = SorterBuilder::new(WrappingSumU32s).chunk_creator(CursorVec).build();
//!
//! // We insert multiple entries with the same key but different values
//! // in arbitrary order, the sorter will take care of merging them for us.
//! sorter.insert("first-counter", 32_u32.to_ne_bytes())?;
//! sorter.insert("first-counter", 23_u32.to_ne_bytes())?;
//! sorter.insert("first-counter", 64_u32.to_ne_bytes())?;
//!
//! sorter.insert("second-counter", 320_u32.to_ne_bytes())?;
//! sorter.insert("second-counter", 64_u32.to_ne_bytes())?;
//!
//! // We can iterate over the entries in key-order.
//! let mut iter = sorter.into_merger_iter()?;
//!
//! // We can see that the sum of u32s is valid here.
//! assert_eq!(iter.next()?, Some((&b"first-counter"[..], &119_u32.to_ne_bytes()[..])));
//! assert_eq!(iter.next()?, Some((&b"second-counter"[..], &384_u32.to_ne_bytes()[..])));
//! assert_eq!(iter.next()?, None);
//!
//! # Ok(()) }
//! ```

#![warn(missing_docs)]

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

mod block_builder;
mod compression;
mod error;
mod merge;
mod merger;
mod reader;
mod sorter;
mod varint;
mod writer;

pub use self::compression::CompressionType;
pub use self::error::Error;
pub use self::merge::Merge;
pub use self::merger::{Merger, MergerBuilder, MergerIter};
pub use self::reader::Reader;
#[cfg(feature = "tempfile")]
pub use self::sorter::TempFileChunk;
pub use self::sorter::{ChunkCreator, CursorVec, DefaultChunkCreator, Sorter, SorterBuilder};
pub use self::writer::{Writer, WriterBuilder};
