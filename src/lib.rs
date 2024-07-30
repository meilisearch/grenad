#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/meilisearch/grenad/main/assets/grenad-pomegranate.ico?raw=true"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/meilisearch/grenad/main/assets/grenad-pomegranate-logo.png?raw=true"
)]

//! This library provides tools to sort, merge, write, and read immutable key-value pairs.
//! The entries in the grenad files are _immutable_ and the only way to modify them is by _creating
//! a new file_ with the changes.
//!
//! # Features
//!
//! You can define which compression schemes to support, there are currently a few
//! available choices, these determine which types will be available inside the above modules:
//!
//! - _Snappy_ with the [`snap`](https://crates.io/crates/snap) crate.
//! - _Zlib_ with the [`flate2`](https://crates.io/crates/flate2) crate.
//! - _Lz4_ with the [`lz4_flex`](https://crates.io/crates/lz4_flex) crate.
//!
//! If you need more performances you can enable the `rayon` feature that will enable a bunch
//! of new settings like being able to make the `Sorter` sort in parallel.
//!
//! # Examples
//!
//! ## Use the `Writer` and `Reader` structs
//!
//! You can use the [`Writer`] struct to store key-value pairs into the specified
//! [`std::io::Write`] type. The [`Reader`] type can then be used to read the entries.
//!
//! The entries provided to the [`Writer`] struct must be given in lexicographic order.
//!
//! ```rust
//! use std::io::Cursor;
//!
//! use grenad::{Reader, Writer};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut writer = Writer::memory();
//!
//! // We insert our key-value pairs in lexicographic order.
//! writer.insert("first-counter", 119_u32.to_ne_bytes())?;
//! writer.insert("second-counter", 384_u32.to_ne_bytes())?;
//!
//! // We create a reader from our writer.
//! let cursor = writer.into_inner().map(Cursor::new)?;
//! let mut cursor = Reader::new(cursor)?.into_cursor()?;
//!
//! // We can see that the sum of u32s is valid here.
//! assert_eq!(cursor.move_on_next()?, Some((&b"first-counter"[..], &119_u32.to_ne_bytes()[..])));
//! assert_eq!(cursor.move_on_next()?, Some((&b"second-counter"[..], &384_u32.to_ne_bytes()[..])));
//! assert_eq!(cursor.move_on_next()?, None);
//!
//! // We can also jum on any given entry.
//! assert_eq!(cursor.move_on_key_greater_than_or_equal_to("first")?, Some((&b"first-counter"[..], &119_u32.to_ne_bytes()[..])));
//! assert_eq!(cursor.move_on_key_equal_to("second-counter")?, Some((&b"second-counter"[..], &384_u32.to_ne_bytes()[..])));
//! assert_eq!(cursor.move_on_key_lower_than_or_equal_to("abracadabra")?, None);
//! # Ok(()) }
//! ```
//!
//! ## Use the `Merger` struct
//!
//! In this example we show how you can merge multiple [`Reader`]s
//! by using a _merge function_ when a conflict is encountered.
//!
//! The entries yielded by the [`Merger`] struct are returned in lexicographic order,
//! a good way to write them back into a new [`Writer`].
//!
//! ```rust
//! use std::array::TryFromSliceError;
//! use std::borrow::Cow;
//! use std::convert::TryInto;
//! use std::io::Cursor;
//!
//! use grenad::{MergerBuilder, Reader, Writer};
//!
//! // This merge function:
//! //  - parses u32s from native-endian bytes,
//! //  - wrapping sums them and,
//! //  - outputs the result as native-endian bytes.
//! fn wrapping_sum_u32s<'a>(
//!  _key: &[u8],
//!  values: &[Cow<'a, [u8]>],
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
//! // We create our writers in memory to insert our key-value pairs.
//! let mut writera = Writer::memory();
//! let mut writerb = Writer::memory();
//! let mut writerc = Writer::memory();
//!
//! // We insert our key-value pairs in lexicographic order
//! // and mix them between our writers.
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
//! let readera = Reader::new(cursora)?.into_cursor()?;
//! let readerb = Reader::new(cursorb)?.into_cursor()?;
//! let readerc = Reader::new(cursorc)?.into_cursor()?;
//!
//! // We create a merger that will sum our u32s when necessary,
//! // and we add our readers to the list of readers to merge.
//! let merger_builder = MergerBuilder::new(wrapping_sum_u32s);
//! let merger = merger_builder.add(readera).add(readerb).add(readerc).build();
//!
//! // We can iterate over the entries in key-order.
//! let mut iter = merger.into_stream_merger_iter()?;
//!
//! // We can see that the sum of u32s is valid here.
//! assert_eq!(iter.next()?, Some((&b"first-counter"[..], &119_u32.to_ne_bytes()[..])));
//! assert_eq!(iter.next()?, Some((&b"second-counter"[..], &384_u32.to_ne_bytes()[..])));
//! assert_eq!(iter.next()?, None);
//! # Ok(()) }
//! ```
//!
//! ## Use the `Sorter` struct
//!
//! In this example we show how by defining a _merge function_, we can insert
//! multiple entries with the same key and output them in lexicographic order.
//!
//! The [`Sorter`] accepts the entries in any given order, will reorder them in-memory and
//! merge them with the _merge function_ when required. It is authorized to have a memory budget
//! during its construction and will try to follow it as closely as possible.
//!
//! ```rust
//! use std::array::TryFromSliceError;
//! use std::borrow::Cow;
//! use std::convert::TryInto;
//!
//! use grenad::{CursorVec, SorterBuilder};
//!
//! // This merge function:
//! //  - parses u32s from native-endian bytes,
//! //  - wrapping sums them and,
//! //  - outputs the result as native-endian bytes.
//! fn wrapping_sum_u32s<'a>(
//!  _key: &[u8],
//!  values: &[Cow<'a, [u8]>],
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
//! let mut sorter = SorterBuilder::new(wrapping_sum_u32s).chunk_creator(CursorVec).build();
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
//! let mut iter = sorter.into_stream_merger_iter()?;
//!
//! // We can see that the sum of u32s is valid here.
//! assert_eq!(iter.next()?, Some((&b"first-counter"[..], &119_u32.to_ne_bytes()[..])));
//! assert_eq!(iter.next()?, Some((&b"second-counter"[..], &384_u32.to_ne_bytes()[..])));
//! assert_eq!(iter.next()?, None);
//! # Ok(()) }
//! ```

#[cfg(test)]
#[macro_use]
extern crate quickcheck;
use std::convert::Infallible;
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
pub use self::sorter::{
    ChunkCreator, CursorVec, DefaultChunkCreator, SortAlgorithm, Sorter, SorterBuilder,
};
pub use self::writer::{Writer, WriterBuilder};

pub type Result<T, U = Infallible> = std::result::Result<T, Error<U>>;

/// Sometimes we need to use an unsafe trick to make the compiler happy.
/// You can read more about the issue [on the Rust's Github issues].
///
/// [on the Rust's Github issues]: https://github.com/rust-lang/rust/issues/47680
unsafe fn transmute_entry_to_static(key: &[u8], val: &[u8]) -> (&'static [u8], &'static [u8]) {
    (mem::transmute(key), mem::transmute(val))
}
