This library provides different ways to sort, merge, write,
and read key-value pairs efficiently.

# Example: use the StreamWriter and StreamReader structs

You can use the [`StreamWriter`] struct to store key-value pairs
into the specified [`std::io::Write`] type. The [`StreamReader`] type
can then be used to read the entries.

```rust
use std::io::Cursor;

use grenad::{StreamReader, StreamWriter};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut writer = StreamWriter::memory();
writer.insert("first-counter", 119_u32.to_ne_bytes())?;
writer.insert("second-counter", 384_u32.to_ne_bytes())?;

// We create a reader from our writer.
let cursor = writer.into_inner().map(Cursor::new)?;
let mut reader = StreamReader::new(cursor)?;

// We can see that the sum of u32s is valid here.
assert_eq!(reader.next()?, Some((&b"first-counter"[..], &119_u32.to_ne_bytes()[..])));
assert_eq!(reader.next()?, Some((&b"second-counter"[..], &384_u32.to_ne_bytes()[..])));
assert_eq!(reader.next()?, None);
# Ok(()) }
```

# Example: use the Merger struct

In this example we show how you can merge multiple [`StreamReader`]s
by using a merge function when a conflict is encountered.

```rust
use std::array::TryFromSliceError;
use std::borrow::Cow;
use std::convert::TryInto;
use std::io::Cursor;

use grenad::{StreamMergerBuilder, StreamReader, StreamWriter};

// This merge function:
//  - parses u32s from native-endian bytes,
//  - wrapping sums them and,
//  - outputs the result as native-endian bytes.
fn wrapping_sum_u32s<'a>(
 _key: &[u8],
 values: &[Cow<'a, [u8]>],
) -> Result<Cow<'a, [u8]>, TryFromSliceError>
{
    let mut output: u32 = 0;
    for bytes in values.iter().map(AsRef::as_ref) {
        let num = bytes.try_into().map(u32::from_ne_bytes)?;
        output = output.wrapping_add(num);
    }
    Ok(Cow::Owned(output.to_ne_bytes().to_vec()))
}

# fn main() -> Result<(), Box<dyn std::error::Error>> {
// We create our writers in memory to insert our key-value pairs.
let mut writera = StreamWriter::memory();
let mut writerb = StreamWriter::memory();
let mut writerc = StreamWriter::memory();

// We insert our key-value pairs in order and
// mix them between our writers.
writera.insert("first-counter", 32_u32.to_ne_bytes())?;
writera.insert("second-counter", 64_u32.to_ne_bytes())?;
writerb.insert("first-counter", 23_u32.to_ne_bytes())?;
writerb.insert("second-counter", 320_u32.to_ne_bytes())?;
writerc.insert("first-counter", 64_u32.to_ne_bytes())?;

// We create readers from our writers.
let cursora = writera.into_inner().map(Cursor::new)?;
let cursorb = writerb.into_inner().map(Cursor::new)?;
let cursorc = writerc.into_inner().map(Cursor::new)?;
let readera = StreamReader::new(cursora)?;
let readerb = StreamReader::new(cursorb)?;
let readerc = StreamReader::new(cursorc)?;

// We create a merger that will sum our u32s when necessary,
// and we add our readers to the list of readers to merge.
let merger_builder = StreamMergerBuilder::new(wrapping_sum_u32s);
let merger = merger_builder.add(readera).add(readerb).add(readerc).build();

// We can iterate over the entries in key-order.
let mut iter = merger.into_stream_merger_iter()?;

// We can see that the sum of u32s is valid here.
assert_eq!(iter.next()?, Some((&b"first-counter"[..], &119_u32.to_ne_bytes()[..])));
assert_eq!(iter.next()?, Some((&b"second-counter"[..], &384_u32.to_ne_bytes()[..])));
assert_eq!(iter.next()?, None);
# Ok(()) }
```

# Example: use the Sorter struct

In this example we show how by defining a merge function,
you can insert multiple entries with the same key and output
them in key-order.

```rust
use std::array::TryFromSliceError;
use std::borrow::Cow;
use std::convert::TryInto;

use grenad::{CursorVec, SorterBuilder};

// This merge function:
//  - parses u32s from native-endian bytes,
//  - wrapping sums them and,
//  - outputs the result as native-endian bytes.
fn wrapping_sum_u32s<'a>(
 _key: &[u8],
 values: &[Cow<'a, [u8]>],
) -> Result<Cow<'a, [u8]>, TryFromSliceError>
{
    let mut output: u32 = 0;
    for bytes in values.iter().map(AsRef::as_ref) {
        let num = bytes.try_into().map(u32::from_ne_bytes)?;
        output = output.wrapping_add(num);
    }
    Ok(Cow::Owned(output.to_ne_bytes().to_vec()))
}

# fn main() -> Result<(), Box<dyn std::error::Error>> {
// We create a sorter that will sum our u32s when necessary.
let mut sorter = SorterBuilder::new(wrapping_sum_u32s).chunk_creator(CursorVec).build();

// We insert multiple entries with the same key but different values
// in arbitrary order, the sorter will take care of merging them for us.
sorter.insert("first-counter", 32_u32.to_ne_bytes())?;
sorter.insert("first-counter", 23_u32.to_ne_bytes())?;
sorter.insert("first-counter", 64_u32.to_ne_bytes())?;

sorter.insert("second-counter", 320_u32.to_ne_bytes())?;
sorter.insert("second-counter", 64_u32.to_ne_bytes())?;

// We can iterate over the entries in key-order.
let mut iter = sorter.into_stream_merger_iter()?;

// We can see that the sum of u32s is valid here.
assert_eq!(iter.next()?, Some((&b"first-counter"[..], &119_u32.to_ne_bytes()[..])));
assert_eq!(iter.next()?, Some((&b"second-counter"[..], &384_u32.to_ne_bytes()[..])));
assert_eq!(iter.next()?, None);

# Ok(()) }
```
