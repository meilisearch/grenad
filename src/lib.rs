//!
//!
//! # Example: Using the Sorter struct
//!
//! ```
//! use std::array::TryFromSliceError;
//! use std::borrow::Cow;
//! use std::convert::TryInto;
//!
//! use grenad::Sorter;
//!
//! // This merge function:
//! //  - parses u32s from native-endian bytes,
//! //  - wrapping sums them and,
//! //  - outputs the result as native-endian bytes.
//! fn wrapping_sum_u32s<'a>(
//!     _key: &[u8],
//!     values: &[Cow<'a, [u8]>],
//! ) -> Result<Cow<'a, [u8]>, TryFromSliceError>
//! {
//!     let mut output: u32 = 0;
//!     for bytes in values.iter().map(AsRef::as_ref) {
//!         let num = bytes.try_into().map(u32::from_ne_bytes)?;
//!         output = output.wrapping_add(num);
//!     }
//!     Ok(Cow::Owned(output.to_ne_bytes().to_vec()))
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // We create a sorter that will sum our u32s when necessary.
//! let mut sorter = Sorter::new(wrapping_sum_u32s);
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
pub use self::sorter::{ChunkCreator, CursorVec, DefaultChunkCreator, Sorter, SorterBuilder};
pub use self::writer::{Writer, WriterBuilder};
