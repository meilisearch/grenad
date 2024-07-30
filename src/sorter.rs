use std::alloc::{alloc, dealloc, Layout};
use std::borrow::Cow;
use std::convert::Infallible;
use std::fmt::Debug;
#[cfg(feature = "tempfile")]
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::mem::{align_of, size_of};
use std::num::NonZeroUsize;
use std::{cmp, io, ops, slice};

use bytemuck::{cast_slice, cast_slice_mut, Pod, Zeroable};

use crate::count_write::CountWrite;

const INITIAL_SORTER_VEC_SIZE: usize = 131_072; // 128KB
const DEFAULT_SORTER_MEMORY: usize = 1_073_741_824; // 1GB
const MIN_SORTER_MEMORY: usize = 10_485_760; // 10MB

const DEFAULT_NB_CHUNKS: usize = 25;
const MIN_NB_CHUNKS: usize = 1;

use crate::{
    CompressionType, Error, Merger, MergerIter, Reader, ReaderCursor, Writer, WriterBuilder,
};

/// The kind of sort algorithm used by the sorter to sort its internal vector.
#[derive(Debug, Clone, Copy)]
pub enum SortAlgorithm {
    /// The stable sort algorithm maintains the relative order of values with equal keys,
    /// but it is slower than the unstable algorithm.
    Stable,
    /// The unstable sort algorithm is faster than the unstable algorithm, but it
    /// does not keep the relative order of values with equal keys.
    Unstable,
}

/// A struct that is used to configure a [`Sorter`] to better fit your needs.
#[derive(Debug, Clone, Copy)]
pub struct SorterBuilder<MF, CC> {
    dump_threshold: usize,
    allow_realloc: bool,
    max_nb_chunks: usize,
    chunk_compression_type: Option<CompressionType>,
    chunk_compression_level: Option<u32>,
    index_key_interval: Option<NonZeroUsize>,
    block_size: Option<usize>,
    index_levels: Option<u8>,
    chunk_creator: CC,
    sort_algorithm: SortAlgorithm,
    sort_in_parallel: bool,
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
            chunk_compression_type: None,
            chunk_compression_level: None,
            index_key_interval: None,
            block_size: None,
            index_levels: None,
            chunk_creator: DefaultChunkCreator::default(),
            sort_algorithm: SortAlgorithm::Stable,
            sort_in_parallel: false,
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
        self.chunk_compression_type = Some(compression);
        self
    }

    /// Defines the compression level that the defined compression type will use.
    pub fn chunk_compression_level(&mut self, level: u32) -> &mut Self {
        self.chunk_compression_level = Some(level);
        self
    }

    /// The interval at which we store the index of a key in the
    /// index footer, used to seek into a block.
    pub fn index_key_interval(&mut self, interval: NonZeroUsize) -> &mut Self {
        self.index_key_interval = Some(interval);
        self
    }

    /// Defines the size of the blocks that the writer will write.
    ///
    /// The bigger the blocks are the better they are compressed
    /// but the more time it takes to compress and decompress them.
    pub fn block_size(&mut self, size: usize) -> &mut Self {
        self.block_size = Some(size);
        self
    }

    /// The number of levels/indirection we will use to write the index footer.
    ///
    /// An indirection of 1 or 2 is sufficient to reduce the impact of
    /// decompressing/reading a big index footer.
    ///
    /// The default is 0 which means that the index footer values directly
    /// specifies the block where the requested entry can be found. The disavantage of this
    /// is that the index block can be quite big and take time to be decompressed and read.
    pub fn index_levels(&mut self, levels: u8) -> &mut Self {
        self.index_levels = Some(levels);
        self
    }

    /// The algorithm used to sort the internal vector.
    ///
    /// The default is [`SortAlgorithm::Stable`](crate::SortAlgorithm::Stable).
    pub fn sort_algorithm(&mut self, algo: SortAlgorithm) -> &mut Self {
        self.sort_algorithm = algo;
        self
    }

    /// Whether we use [rayon to sort](https://docs.rs/rayon/latest/rayon/slice/trait.ParallelSliceMut.html#method.par_sort_by_key) the entries.
    ///
    /// By default we do not sort in parallel, the value is `false`.
    #[cfg(feature = "rayon")]
    pub fn sort_in_parallel(&mut self, value: bool) -> &mut Self {
        self.sort_in_parallel = value;
        self
    }

    /// The [`ChunkCreator`] struct used to generate the chunks used
    /// by the [`Sorter`] to bufferize when required.
    pub fn chunk_creator<CC2>(self, creation: CC2) -> SorterBuilder<MF, CC2> {
        SorterBuilder {
            dump_threshold: self.dump_threshold,
            allow_realloc: self.allow_realloc,
            max_nb_chunks: self.max_nb_chunks,
            chunk_compression_type: self.chunk_compression_type,
            chunk_compression_level: self.chunk_compression_level,
            index_key_interval: self.index_key_interval,
            block_size: self.block_size,
            index_levels: self.index_levels,
            chunk_creator: creation,
            sort_algorithm: self.sort_algorithm,
            sort_in_parallel: self.sort_in_parallel,
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
            chunks_total_size: 0,
            allow_realloc: self.allow_realloc,
            dump_threshold: self.dump_threshold,
            max_nb_chunks: self.max_nb_chunks,
            chunk_compression_type: self.chunk_compression_type,
            chunk_compression_level: self.chunk_compression_level,
            index_key_interval: self.index_key_interval,
            block_size: self.block_size,
            index_levels: self.index_levels,
            chunk_creator: self.chunk_creator,
            sort_algorithm: self.sort_algorithm,
            sort_in_parallel: self.sort_in_parallel,
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
    pub fn sort_by_key(&mut self, algorithm: SortAlgorithm) {
        let bounds_end = self.bounds_count * size_of::<EntryBound>();
        let (bounds, tail) = self.buffer.split_at_mut(bounds_end);
        let bounds = cast_slice_mut::<_, EntryBound>(bounds);
        let sort = match algorithm {
            SortAlgorithm::Stable => <[EntryBound]>::sort_by_key,
            SortAlgorithm::Unstable => <[EntryBound]>::sort_unstable_by_key,
        };
        sort(bounds, |b: &EntryBound| &tail[tail.len() - b.key_start..][..b.key_length as usize]);
    }

    /// Sorts in **parallel** the entry bounds by the entries keys,
    /// after a sort the `iter` method will yield the entries sorted.
    #[cfg(feature = "rayon")]
    pub fn par_sort_by_key(&mut self, algorithm: SortAlgorithm) {
        use rayon::slice::ParallelSliceMut;

        let bounds_end = self.bounds_count * size_of::<EntryBound>();
        let (bounds, tail) = self.buffer.split_at_mut(bounds_end);
        let bounds = cast_slice_mut::<_, EntryBound>(bounds);
        let sort = match algorithm {
            SortAlgorithm::Stable => <[EntryBound]>::par_sort_by_key,
            SortAlgorithm::Unstable => <[EntryBound]>::par_sort_unstable_by_key,
        };
        sort(bounds, |b: &EntryBound| &tail[tail.len() - b.key_start..][..b.key_length as usize]);
    }

    #[cfg(not(feature = "rayon"))]
    pub fn par_sort_by_key(&mut self, algorithm: SortAlgorithm) {
        self.sort_by_key(algorithm);
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

    /// Returns the approximative memory usage of the rough entries.
    ///
    /// This is a very bad estimate in the sense that it does not calculate the amount of
    /// duplicate entries and the fact that entries can be compressed once dumped to disk.
    /// This estimate will always be greater than the actual end space usage on disk.
    pub fn estimated_entries_memory_usage(&self) -> usize {
        self.memory_usage() - self.remaining()
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

/// Represents an `EntryBound` aligned buffer.
struct EntryBoundAlignedBuffer(&'static mut [u8]);

impl EntryBoundAlignedBuffer {
    /// Allocates a new buffer of the given size, it is correctly aligned to store `EntryBound`s.
    fn new(size: usize) -> EntryBoundAlignedBuffer {
        let entry_bound_size = size_of::<EntryBound>();
        let size = (size + entry_bound_size - 1) / entry_bound_size * entry_bound_size;
        let layout = Layout::from_size_align(size, align_of::<EntryBound>()).unwrap();
        let ptr = unsafe { alloc(layout) };
        assert!(
            !ptr.is_null(),
            "the allocator is unable to allocate that much memory ({} bytes requested)",
            size
        );
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
    chunks_total_size: u64,
    allow_realloc: bool,
    dump_threshold: usize,
    max_nb_chunks: usize,
    chunk_compression_type: Option<CompressionType>,
    chunk_compression_level: Option<u32>,
    index_key_interval: Option<NonZeroUsize>,
    block_size: Option<usize>,
    index_levels: Option<u8>,
    chunk_creator: CC,
    sort_algorithm: SortAlgorithm,
    sort_in_parallel: bool,
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

    /// A rough estimate of how much memory usage it will take on the disk once dumped to disk.
    ///
    /// This is a very bad estimate in the sense that it does not calculate the amount of
    /// duplicate entries that are in the dumped chunks and neither the fact that the in-memory
    /// buffer will likely be compressed once written to disk. This estimate will always
    /// be greater than the actual end space usage on disk.
    pub fn estimated_dumped_memory_usage(&self) -> u64 {
        self.entries.estimated_entries_memory_usage() as u64 + self.chunks_total_size
    }
}

impl<MF, CC, U> Sorter<MF, CC>
where
    MF: for<'a> Fn(&[u8], &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>, U>,
    CC: ChunkCreator,
{
    /// Insert an entry into the [`Sorter`] making sure that conflicts
    /// are resolved by the provided merge function.
    pub fn insert<K, V>(&mut self, key: K, val: V) -> crate::Result<(), U>
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
            self.chunks_total_size += self.write_chunk()?;
            self.entries.insert(key, val);
            if self.chunks.len() >= self.max_nb_chunks {
                self.chunks_total_size = self.merge_chunks()?;
            }
        }

        Ok(())
    }

    fn threshold_exceeded(&self) -> bool {
        self.entries.memory_usage() >= self.dump_threshold
    }

    /// Returns the exact amount of bytes written to disk, the value can be trusted,
    /// this is not an estimate.
    ///
    /// Writes the in-memory entries to disk, using the specify settings
    /// to compress the block and entries. It clears the in-memory entries.
    fn write_chunk(&mut self) -> crate::Result<u64, U> {
        let count_write_chunk = self
            .chunk_creator
            .create()
            .map_err(Into::into)
            .map_err(Error::convert_merge_error)
            .map(CountWrite::new)?;

        let mut writer_builder = WriterBuilder::new();
        if let Some(compression_type) = self.chunk_compression_type {
            writer_builder.compression_type(compression_type);
        }
        if let Some(compression_level) = self.chunk_compression_level {
            writer_builder.compression_level(compression_level);
        }
        if let Some(index_key_interval) = self.index_key_interval {
            writer_builder.index_key_interval(index_key_interval);
        }
        if let Some(block_size) = self.block_size {
            writer_builder.block_size(block_size);
        }
        if let Some(index_levels) = self.index_levels {
            writer_builder.index_levels(index_levels);
        }
        let mut writer = writer_builder.build(count_write_chunk);

        if self.sort_in_parallel {
            self.entries.par_sort_by_key(self.sort_algorithm);
        } else {
            self.entries.sort_by_key(self.sort_algorithm);
        }

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
            writer.insert(key, &merged_val)?;
        }

        // We retrieve the wrapped CountWrite and extract
        // the amount of bytes effectively written.
        let mut count_write_chunk = writer.into_inner()?;
        count_write_chunk.flush()?;
        let written_bytes = count_write_chunk.count();
        let chunk = count_write_chunk.into_inner()?;

        self.chunks.push(chunk);
        self.entries.clear();

        Ok(written_bytes)
    }

    /// Returns the exact amount of bytes written to disk, the value can be trusted,
    /// this is not an estimate.
    ///
    /// Merges all of the chunks into a final chunk that replaces them.
    /// It uses the user provided merge function to resolve merge conflicts.
    fn merge_chunks(&mut self) -> crate::Result<u64, U> {
        let count_write_chunk = self
            .chunk_creator
            .create()
            .map_err(Into::into)
            .map_err(Error::convert_merge_error)
            .map(CountWrite::new)?;

        let mut writer_builder = WriterBuilder::new();
        if let Some(compression_type) = self.chunk_compression_type {
            writer_builder.compression_type(compression_type);
        }
        if let Some(compression_level) = self.chunk_compression_level {
            writer_builder.compression_level(compression_level);
        }
        if let Some(index_key_interval) = self.index_key_interval {
            writer_builder.index_key_interval(index_key_interval);
        }
        if let Some(block_size) = self.block_size {
            writer_builder.block_size(block_size);
        }
        if let Some(index_levels) = self.index_levels {
            writer_builder.index_levels(index_levels);
        }
        let mut writer = writer_builder.build(count_write_chunk);

        let sources: crate::Result<Vec<_>, U> = self
            .chunks
            .drain(..)
            .map(|mut chunk| {
                chunk.seek(SeekFrom::Start(0))?;
                Reader::new(chunk).and_then(Reader::into_cursor).map_err(Error::convert_merge_error)
            })
            .collect();

        // Create a merger to merge all those chunks.
        let mut builder = Merger::builder(&self.merge);
        builder.extend(sources?);
        let merger = builder.build();

        let mut iter = merger.into_stream_merger_iter().map_err(Error::convert_merge_error)?;
        while let Some((key, val)) = iter.next()? {
            writer.insert(key, val)?;
        }

        let mut count_write_chunk = writer.into_inner()?;
        count_write_chunk.flush()?;
        let written_bytes = count_write_chunk.count();
        let chunk = count_write_chunk.into_inner()?;

        self.chunks.push(chunk);

        Ok(written_bytes)
    }

    /// Consumes this [`Sorter`] and streams the entries to the [`Writer`] given in parameter.
    pub fn write_into_stream_writer<W: io::Write>(
        self,
        writer: &mut Writer<W>,
    ) -> crate::Result<(), U> {
        let mut iter = self.into_stream_merger_iter()?;
        while let Some((key, val)) = iter.next()? {
            writer.insert(key, val)?;
        }
        Ok(())
    }

    /// Consumes this [`Sorter`] and outputs a stream of the merged entries in key-order.
    pub fn into_stream_merger_iter(self) -> crate::Result<MergerIter<CC::Chunk, MF>, U> {
        let (sources, merge) = self.extract_reader_cursors_and_merger()?;
        let mut builder = Merger::builder(merge);
        builder.extend(sources);
        builder.build().into_stream_merger_iter().map_err(Error::convert_merge_error)
    }

    /// Consumes this [`Sorter`] and outputs the list of reader cursors.
    pub fn into_reader_cursors(self) -> crate::Result<Vec<ReaderCursor<CC::Chunk>>, U> {
        self.extract_reader_cursors_and_merger().map(|(readers, _)| readers)
    }

    /// A helper function to extract the readers and the merge function.
    fn extract_reader_cursors_and_merger(
        mut self,
    ) -> crate::Result<(Vec<ReaderCursor<CC::Chunk>>, MF), U> {
        // Flush the pending unordered entries.
        self.chunks_total_size = self.write_chunk()?;

        let Sorter { chunks, merge, .. } = self;
        let result: Result<Vec<_>, _> = chunks
            .into_iter()
            .map(|mut chunk| {
                chunk.seek(SeekFrom::Start(0))?;
                Reader::new(chunk).and_then(Reader::into_cursor).map_err(Error::convert_merge_error)
            })
            .collect();

        result.map(|readers| (readers, merge))
    }
}

impl<MF, CC: ChunkCreator> Debug for Sorter<MF, CC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sorter")
            .field("chunks_count", &self.chunks.len())
            .field("remaining_entries", &self.entries.remaining())
            .field("chunks_total_size", &self.chunks_total_size)
            .field("allow_realloc", &self.allow_realloc)
            .field("dump_threshold", &self.dump_threshold)
            .field("max_nb_chunks", &self.max_nb_chunks)
            .field("chunk_compression_type", &self.chunk_compression_type)
            .field("chunk_compression_level", &self.chunk_compression_level)
            .field("index_key_interval", &self.index_key_interval)
            .field("block_size", &self.block_size)
            .field("index_levels", &self.index_levels)
            .field("chunk_creator", &"[chunck creator]")
            .field("sort_algorithm", &self.sort_algorithm)
            .field("sort_in_parallel", &self.sort_in_parallel)
            .field("merge", &"[merge function]")
            .finish()
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
    use std::io::Cursor;
    use std::iter::repeat;

    use super::*;

    fn merge<'a>(_key: &[u8], vals: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>, Infallible> {
        Ok(vals.iter().flat_map(AsRef::as_ref).cloned().collect())
    }

    #[test]
    #[cfg_attr(miri, ignore)]
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
        sorter.write_into_stream_writer(&mut bytes).unwrap();
        let bytes = bytes.into_inner().unwrap();

        let reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();
        let mut cursor = reader.into_cursor().unwrap();
        while let Some((key, val)) = cursor.move_on_next().unwrap() {
            match key {
                b"hello" => assert_eq!(val, b"kiki"),
                b"abstract" => assert_eq!(val, b"lollol"),
                b"allo" => assert_eq!(val, b"lol"),
                bytes => panic!("{:?}", bytes),
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
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
        sorter.write_into_stream_writer(&mut bytes).unwrap();
        let bytes = bytes.into_inner().unwrap();

        let reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();
        let mut cursor = reader.into_cursor().unwrap();
        let (key, val) = cursor.move_on_next().unwrap().unwrap();
        assert_eq!(key, b"hello");
        assert!(val.iter().eq(repeat(b"kiki").take(200).flatten()));
        assert!(cursor.move_on_next().unwrap().is_none());
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn correct_key_ordering() {
        use std::borrow::Cow;

        use rand::prelude::{SeedableRng, SliceRandom};
        use rand::rngs::StdRng;

        // This merge function concat bytes in the order they are received.
        fn concat_bytes<'a>(
            _key: &[u8],
            values: &[Cow<'a, [u8]>],
        ) -> Result<Cow<'a, [u8]>, Infallible> {
            let mut output = Vec::new();
            for value in values {
                output.extend_from_slice(value);
            }
            Ok(Cow::from(output))
        }

        // We create a sorter that will sum our u32s when necessary.
        let mut sorter = SorterBuilder::new(concat_bytes).chunk_creator(CursorVec).build();

        // We insert all the possible values of an u8 in ascending order
        // but we split them along different keys.
        let mut rng = StdRng::seed_from_u64(42);
        let possible_keys = ["first", "second", "third", "fourth", "fifth", "sixth"];
        for n in 0..=255 {
            let key = possible_keys.choose(&mut rng).unwrap();
            sorter.insert(key, [n]).unwrap();
        }

        // We can iterate over the entries in key-order.
        let mut iter = sorter.into_stream_merger_iter().unwrap();
        while let Some((_key, value)) = iter.next().unwrap() {
            assert!(value.windows(2).all(|w| w[0] <= w[1]), "{:?}", value);
        }
    }

    #[test]
    #[should_panic(
        expected = "the allocator is unable to allocate that much memory (281474976710656 bytes requested)"
    )]
    #[cfg_attr(miri, ignore)]
    fn too_big_allocation() {
        EntryBoundAlignedBuffer::new(1 << 48);
    }
}
