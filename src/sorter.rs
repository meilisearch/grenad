use std::alloc::{alloc, dealloc, Layout};
use std::borrow::Cow;
use std::convert::Infallible;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::mem::{align_of, size_of};
use std::{cmp, io, ops, slice};

use bytemuck::{cast_slice, cast_slice_mut, Pod, Zeroable};

const INITIAL_SORTER_VEC_SIZE: usize = 131_072; // 128KB
const DEFAULT_SORTER_MEMORY: usize = 1_073_741_824; // 1GB
const MIN_SORTER_MEMORY: usize = 10_485_760; // 10MB

const DEFAULT_NB_CHUNKS: usize = 25;
const MIN_NB_CHUNKS: usize = 1;

use crate::{CompressionType, Error, Merger, MergerIter, Reader, Writer, WriterBuilder};

/// A struct that is used to configure a [`Sorter`] to better fit your needs.
#[derive(Debug, Clone, Copy)]
pub struct SorterBuilder<MF, CC> {
    dump_threshold: usize,
    allow_realloc: bool,
    max_nb_chunks: usize,
    chunk_compression_type: CompressionType,
    chunk_compression_level: u32,
    chunk_creator: CC,
    merge: MF,
}

impl<MF> SorterBuilder<MF, DefaultChunkCreator> {
    /// Creates a [`SorterBuilder`] from a merge function, it can be
    /// used to configure your [`Sorter`] to better fit your needs.
    pub fn new(merge: MF) -> Self {
        SorterBuilder {
            dump_threshold: DEFAULT_SORTER_MEMORY,
            allow_realloc: true,
            max_nb_chunks: DEFAULT_NB_CHUNKS,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: 0,
            chunk_creator: DefaultChunkCreator::default(),
            merge,
        }
    }
}

impl<MF, CC> SorterBuilder<MF, CC> {
    /// The amount of memory to reach that will trigger a memory dump from in memory to disk.
    pub fn dump_threshold(&mut self, memory: usize) -> &mut Self {
        self.dump_threshold = cmp::max(memory, MIN_SORTER_MEMORY);
        self
    }

    /// Whether the sorter is allowed or not to reallocate the internal vector.
    ///
    /// Note that reallocating involve a more important memory usage and disallowing
    /// it will make the sorter to **always** consume the dump threshold memory.
    pub fn allow_realloc(&mut self, allow: bool) -> &mut Self {
        self.allow_realloc = allow;
        self
    }

    /// The maximum number of chunks on disk, if this number of chunks is reached
    /// they will be merged into a single chunk. Merging can reduce the disk usage.
    pub fn max_nb_chunks(&mut self, nb_chunks: usize) -> &mut Self {
        self.max_nb_chunks = cmp::max(nb_chunks, MIN_NB_CHUNKS);
        self
    }

    /// Defines the compression type the built [`Sorter`] will use when buffering.
    pub fn chunk_compression_type(&mut self, compression: CompressionType) -> &mut Self {
        self.chunk_compression_type = compression;
        self
    }

    /// Defines the compression level that the defined compression type will use.
    pub fn chunk_compression_level(&mut self, level: u32) -> &mut Self {
        self.chunk_compression_level = level;
        self
    }

    /// The [`ChunkCreator`] strutc used to generate the chunks used
    /// by the [`Sorter`] to bufferize when required.
    pub fn chunk_creator<CC2>(self, creation: CC2) -> SorterBuilder<MF, CC2> {
        SorterBuilder {
            dump_threshold: self.dump_threshold,
            allow_realloc: self.allow_realloc,
            max_nb_chunks: self.max_nb_chunks,
            chunk_compression_type: self.chunk_compression_type,
            chunk_compression_level: self.chunk_compression_level,
            chunk_creator: creation,
            merge: self.merge,
        }
    }
}

impl<MF, CC: ChunkCreator> SorterBuilder<MF, CC> {
    /// Creates the [`Sorter`] configured by this builder.
    pub fn build(self) -> Sorter<MF, CC> {
        let capacity =
            if self.allow_realloc { INITIAL_SORTER_VEC_SIZE } else { self.dump_threshold };

        Sorter {
            chunks: Vec::new(),
            entries: Entries::with_capacity(capacity),
            allow_realloc: self.allow_realloc,
            dump_threshold: self.dump_threshold,
            max_nb_chunks: self.max_nb_chunks,
            chunk_compression_type: self.chunk_compression_type,
            chunk_compression_level: self.chunk_compression_level,
            chunk_creator: self.chunk_creator,
            merge: self.merge,
        }
    }
}

/// Stores entries memory efficiently in a buffer.
struct Entries {
    /// The internal buffer that contains the bounds of the buffer
    /// on the front and the key and data bytes on the back of it.
    ///
    /// [----bounds---->--remaining--<--key+data--]
    ///
    buffer: EntryBoundAlignedBuffer,

    /// The amount of bytes stored in the buffer.
    entries_len: usize,

    /// The number of bounds stored in the buffer.
    bounds_count: usize,
}

impl Entries {
    /// Creates a buffer which will consumes this amount of memory,
    /// rounded up to the size of one `EntryBound` more.
    ///
    /// It will use this amount of memory until it needs to reallocate
    /// where it will create a new buffer of twice the size of the current one
    /// copies the entries inside and replace the current one by the new one.
    ///
    /// If you want to be sure about the amount of memory used you can use
    /// the `fits` method.
    pub fn with_capacity(capacity: usize) -> Self {
        Self { buffer: EntryBoundAlignedBuffer::new(capacity), entries_len: 0, bounds_count: 0 }
    }

    /// Clear the entries.
    pub fn clear(&mut self) {
        self.entries_len = 0;
        self.bounds_count = 0;
    }

    /// Inserts a new entry into the buffer, if there is not
    /// enough space for it to be stored, we double the buffer size.
    pub fn insert(&mut self, key: &[u8], data: &[u8]) {
        assert!(key.len() <= u32::max_value() as usize);
        assert!(data.len() <= u32::max_value() as usize);

        if self.fits(key, data) {
            // We store the key and data bytes one after the other at the back of the buffer.
            self.entries_len += key.len() + data.len();
            let entries_start = self.buffer.len() - self.entries_len;
            self.buffer[entries_start..][..key.len()].copy_from_slice(key);
            self.buffer[entries_start + key.len()..][..data.len()].copy_from_slice(data);

            let bound = EntryBound {
                key_start: self.entries_len,
                key_length: key.len() as u32,
                data_length: data.len() as u32,
            };

            // We store the bounds at the front of the buffer and grow from the end to the start
            // of it. We interpret the front of the buffer as a slice of EntryBounds + 1 entry
            // that is not assigned and replace it with the new one we want to insert.
            let bounds_end = (self.bounds_count + 1) * size_of::<EntryBound>();
            let bounds = cast_slice_mut::<_, EntryBound>(&mut self.buffer[..bounds_end]);
            bounds[self.bounds_count] = bound;
            self.bounds_count += 1;
        } else {
            self.reallocate_buffer();
            self.insert(key, data);
        }
    }

    /// Returns `true` if inserting this entry will not trigger a reallocation.
    pub fn fits(&self, key: &[u8], data: &[u8]) -> bool {
        // The number of memory aligned EntryBounds that we can store.
        let aligned_bounds_count = unsafe { self.buffer.align_to::<EntryBound>().1.len() };
        let remaining_aligned_bounds = aligned_bounds_count - self.bounds_count;

        self.remaining() >= Self::entry_size(key, data) && remaining_aligned_bounds >= 1
    }

    /// Simply returns the size of the internal buffer.
    pub fn memory_usage(&self) -> usize {
        self.buffer.len()
    }

    /// Sorts the entry bounds by the entries keys, after a sort
    /// the `iter` method will yield the entries sorted.
    pub fn sort_unstable_by_key(&mut self) {
        let bounds_end = self.bounds_count * size_of::<EntryBound>();
        let (bounds, tail) = self.buffer.split_at_mut(bounds_end);
        let bounds = cast_slice_mut::<_, EntryBound>(bounds);
        bounds.sort_unstable_by_key(|b| &tail[tail.len() - b.key_start..][..b.key_length as usize]);
    }

    /// Returns an iterator over the keys and datas.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
        let bounds_end = self.bounds_count * size_of::<EntryBound>();
        let (bounds, tail) = self.buffer.split_at(bounds_end);
        let bounds = cast_slice::<_, EntryBound>(bounds);

        bounds.iter().map(move |b| {
            let entries_start = tail.len() - b.key_start;
            let key = &tail[entries_start..][..b.key_length as usize];
            let data = &tail[entries_start + b.key_length as usize..][..b.data_length as usize];
            (key, data)
        })
    }

    /// The remaining amount of bytes before we need to reallocate a new buffer.
    fn remaining(&self) -> usize {
        self.buffer.len() - self.entries_len - self.bounds_count * size_of::<EntryBound>()
    }

    /// The size that this entry will need to be stored in the buffer.
    fn entry_size(key: &[u8], data: &[u8]) -> usize {
        size_of::<EntryBound>() + key.len() + data.len()
    }

    /// Doubles the size of the internal buffer, copies the entries and bounds into the new buffer.
    fn reallocate_buffer(&mut self) {
        let bounds_end = self.bounds_count * size_of::<EntryBound>();
        let bounds_bytes = &self.buffer[..bounds_end];

        let entries_start = self.buffer.len() - self.entries_len;
        let entries_bytes = &self.buffer[entries_start..];

        let mut new_buffer = EntryBoundAlignedBuffer::new(self.buffer.len() * 2);
        new_buffer[..bounds_end].copy_from_slice(bounds_bytes);
        let new_entries_start = new_buffer.len() - self.entries_len;
        new_buffer[new_entries_start..].copy_from_slice(entries_bytes);

        self.buffer = new_buffer;
    }
}

#[derive(Default, Copy, Clone, Pod, Zeroable)]
#[repr(C)]
struct EntryBound {
    key_start: usize,
    key_length: u32,
    data_length: u32,
}

/// Representes an `EntryBound` aligned buffer.
struct EntryBoundAlignedBuffer(&'static mut [u8]);

impl EntryBoundAlignedBuffer {
    /// Allocates a new buffer of the given size, it is correctly aligned to store `EntryBound`s.
    fn new(size: usize) -> EntryBoundAlignedBuffer {
        let entry_bound_size = size_of::<EntryBound>();
        let size = (size + entry_bound_size - 1) / entry_bound_size * entry_bound_size;
        let layout = Layout::from_size_align(size, align_of::<EntryBound>()).unwrap();
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "the allocator is unable to allocate that much memory");
        let slice = unsafe { slice::from_raw_parts_mut(ptr, size) };
        EntryBoundAlignedBuffer(slice)
    }
}

impl ops::Deref for EntryBoundAlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl ops::DerefMut for EntryBoundAlignedBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

impl Drop for EntryBoundAlignedBuffer {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.0.len(), align_of::<EntryBound>()).unwrap();
        unsafe { dealloc(self.0.as_mut_ptr(), layout) }
    }
}

/// A struct you can use to automatically sort and merge duplicate entries.
///
/// You can insert key-value pairs in arbitrary order, it will use the
/// [`ChunkCreator`] and you the generated chunks to buffer when the `dump_threashold`
/// setting is reached.
pub struct Sorter<MF, CC: ChunkCreator = DefaultChunkCreator> {
    chunks: Vec<CC::Chunk>,
    entries: Entries,
    allow_realloc: bool,
    dump_threshold: usize,
    max_nb_chunks: usize,
    chunk_compression_type: CompressionType,
    chunk_compression_level: u32,
    chunk_creator: CC,
    merge: MF,
}

impl<MF> Sorter<MF, DefaultChunkCreator> {
    /// Creates a [`SorterBuilder`] from a merge function, it can be
    /// used to configure your [`Sorter`] to better fit your needs.
    pub fn builder(merge: MF) -> SorterBuilder<MF, DefaultChunkCreator> {
        SorterBuilder::new(merge)
    }

    /// Creates a [`Sorter`] from a merge function, with the default parameters.
    pub fn new(merge: MF) -> Sorter<MF, DefaultChunkCreator> {
        SorterBuilder::new(merge).build()
    }
}

impl<MF, CC, U> Sorter<MF, CC>
where
    MF: for<'a> Fn(&[u8], &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>, U>,
    CC: ChunkCreator,
{
    /// Insert an entry into the [`Sorter`] making sure that conflicts
    /// are resolved by the provided merge function.
    pub fn insert<K, V>(&mut self, key: K, val: V) -> Result<(), Error<U>>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let val = val.as_ref();

        #[allow(clippy::branches_sharing_code)]
        if self.entries.fits(key, val) || (!self.threshold_exceeded() && self.allow_realloc) {
            self.entries.insert(key, val);
        } else {
            self.write_chunk()?;
            self.entries.insert(key, val);
            if self.chunks.len() >= self.max_nb_chunks {
                self.merge_chunks()?;
            }
        }

        Ok(())
    }

    fn threshold_exceeded(&self) -> bool {
        self.entries.memory_usage() >= self.dump_threshold
    }

    fn write_chunk(&mut self) -> Result<(), Error<U>> {
        let chunk =
            self.chunk_creator.create().map_err(Into::into).map_err(Error::convert_merge_error)?;
        let mut writer = WriterBuilder::new()
            .compression_type(self.chunk_compression_type)
            .compression_level(self.chunk_compression_level)
            .build(chunk)?;

        self.entries.sort_unstable_by_key();

        let mut current = None;
        for (key, value) in self.entries.iter() {
            match current.as_mut() {
                None => current = Some((key, vec![Cow::Borrowed(value)])),
                Some((current_key, vals)) => {
                    if current_key != &key {
                        let merged_val = (self.merge)(current_key, vals).map_err(Error::Merge)?;
                        writer.insert(&current_key, &merged_val)?;
                        vals.clear();
                        *current_key = key;
                    }
                    vals.push(Cow::Borrowed(value));
                }
            }
        }

        if let Some((key, vals)) = current.take() {
            let merged_val = (self.merge)(key, &vals).map_err(Error::Merge)?;
            writer.insert(&key, &merged_val)?;
        }

        let chunk = writer.into_inner()?;
        self.chunks.push(chunk);
        self.entries.clear();

        Ok(())
    }

    fn merge_chunks(&mut self) -> Result<(), Error<U>> {
        let chunk =
            self.chunk_creator.create().map_err(Into::into).map_err(Error::convert_merge_error)?;
        let mut writer = WriterBuilder::new()
            .compression_type(self.chunk_compression_type)
            .compression_level(self.chunk_compression_level)
            .build(chunk)?;

        let sources: Result<Vec<_>, Error<U>> = self
            .chunks
            .drain(..)
            .map(|mut chunk| {
                chunk.seek(SeekFrom::Start(0))?;
                Reader::new(chunk).map_err(Error::convert_merge_error)
            })
            .collect();

        // Create a merger to merge all those chunks.
        let mut builder = Merger::builder(&self.merge);
        builder.extend(sources?);
        let merger = builder.build();

        let mut iter = merger.into_merger_iter().map_err(Error::convert_merge_error)?;
        while let Some((key, val)) = iter.next()? {
            writer.insert(key, val)?;
        }

        let chunk = writer.into_inner()?;
        self.chunks.push(chunk);

        Ok(())
    }

    /// Consumes this [`Sorter`] and streams the entries to the [`Writer`] given in parameter.
    pub fn write_into<W: io::Write>(self, writer: &mut Writer<W>) -> Result<(), Error<U>> {
        let mut iter = self.into_merger_iter()?;
        while let Some((key, val)) = iter.next()? {
            writer.insert(key, val)?;
        }
        Ok(())
    }

    /// Consumes this [`Sorter`] and outputs a stream of the merged entries in key-order.
    pub fn into_merger_iter(mut self) -> Result<MergerIter<CC::Chunk, MF>, Error<U>> {
        // Flush the pending unordered entries.
        self.write_chunk()?;

        let sources: Result<Vec<_>, Error<U>> = self
            .chunks
            .into_iter()
            .map(|mut file| {
                file.seek(SeekFrom::Start(0))?;
                Reader::new(file).map_err(Error::convert_merge_error)
            })
            .collect();

        let mut builder = Merger::builder(self.merge);
        builder.extend(sources?);

        builder.build().into_merger_iter().map_err(Error::convert_merge_error)
    }
}

/// A trait that represent a `ChunkCreator`.
pub trait ChunkCreator {
    /// The generated chunk by this `ChunkCreator`.
    type Chunk: Write + Seek + Read;
    /// The error that can be thrown by this `ChunkCreator`.
    type Error: Into<Error>;

    /// The method called by the sorter that returns the created chunk.
    fn create(&self) -> Result<Self::Chunk, Self::Error>;
}

/// The default chunk creator.
#[cfg(feature = "tempfile")]
pub type DefaultChunkCreator = TempFileChunk;

/// The default chunk creator.
#[cfg(not(feature = "tempfile"))]
pub type DefaultChunkCreator = CursorVec;

impl<C: Write + Seek + Read, E: Into<Error>> ChunkCreator for dyn Fn() -> Result<C, E> {
    type Chunk = C;
    type Error = E;

    fn create(&self) -> Result<Self::Chunk, Self::Error> {
        self()
    }
}

/// A [`ChunkCreator`] that generates temporary [`File`]s for chunks.
#[cfg(feature = "tempfile")]
#[derive(Debug, Default, Copy, Clone)]
pub struct TempFileChunk;

#[cfg(feature = "tempfile")]
impl ChunkCreator for TempFileChunk {
    type Chunk = File;
    type Error = io::Error;

    fn create(&self) -> Result<Self::Chunk, Self::Error> {
        tempfile::tempfile()
    }
}

/// A [`ChunkCreator`] that generates [`Vec`] of bytes wrapped by a [`Cursor`] for chunks.
#[derive(Debug, Default, Copy, Clone)]
pub struct CursorVec;

impl ChunkCreator for CursorVec {
    type Chunk = Cursor<Vec<u8>>;
    type Error = Infallible;

    fn create(&self) -> Result<Self::Chunk, Self::Error> {
        Ok(Cursor::new(Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::iter::repeat;

    use super::*;

    fn merge<'a>(_key: &[u8], vals: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>, Infallible> {
        Ok(vals.iter().map(AsRef::as_ref).flatten().cloned().collect())
    }

    #[test]
    fn simple_cursorvec() {
        let mut sorter = SorterBuilder::new(merge)
            .chunk_compression_type(CompressionType::Snappy)
            .chunk_creator(CursorVec)
            .build();

        sorter.insert(b"hello", "kiki").unwrap();
        sorter.insert(b"abstract", "lol").unwrap();
        sorter.insert(b"allo", "lol").unwrap();
        sorter.insert(b"abstract", "lol").unwrap();

        let mut bytes = WriterBuilder::new().memory();
        sorter.write_into(&mut bytes).unwrap();
        let bytes = bytes.into_inner().unwrap();

        let mut reader = Reader::new(bytes.as_slice()).unwrap();
        while let Some((key, val)) = reader.next().unwrap() {
            match key {
                b"hello" => assert_eq!(val, b"kiki"),
                b"abstract" => assert_eq!(val, b"lollol"),
                b"allo" => assert_eq!(val, b"lol"),
                bytes => panic!("{:?}", bytes),
            }
        }
    }

    #[test]
    fn hard_cursorvec() {
        let mut sorter = SorterBuilder::new(merge)
            .dump_threshold(1024) // 1KiB
            .allow_realloc(false)
            .chunk_compression_type(CompressionType::Snappy)
            .chunk_creator(CursorVec)
            .build();

        // make sure that we reach the threshold we store the keys,
        // values and EntryBound inline in the buffer so we are likely
        // to reach it by inserting 200x 5+4 bytes long entries.
        for _ in 0..200 {
            sorter.insert(b"hello", "kiki").unwrap();
        }

        let mut bytes = WriterBuilder::new().memory();
        sorter.write_into(&mut bytes).unwrap();
        let bytes = bytes.into_inner().unwrap();

        let mut reader = Reader::new(bytes.as_slice()).unwrap();
        let (key, val) = reader.next().unwrap().unwrap();
        assert_eq!(key, b"hello");
        assert!(val.iter().eq(repeat(b"kiki").take(200).flatten()));
        assert!(reader.next().unwrap().is_none());
    }
}
