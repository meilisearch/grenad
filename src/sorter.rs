use std::borrow::Cow;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::mem::size_of;
use std::time::Instant;
use std::{cmp, io};

use bytemuck::{cast_slice, cast_slice_mut, Pod, Zeroable};
use log::debug;

const INITIAL_SORTER_VEC_SIZE: usize = 131_072; // 128KB
const DEFAULT_SORTER_MEMORY: usize = 1_073_741_824; // 1GB
const MIN_SORTER_MEMORY: usize = 10_485_760; // 10MB

const DEFAULT_NB_CHUNKS: usize = 25;
const MIN_NB_CHUNKS: usize = 1;

use crate::file_fuse::DEFAULT_SHRINK_SIZE;
use crate::{CompressionType, Writer, WriterBuilder};
use crate::{Error, FileFuse, FileFuseBuilder, Reader};
use crate::{Merger, MergerIter};

#[derive(Debug, Clone, Copy)]
pub struct SorterBuilder<MF> {
    pub dump_threshold: usize,
    pub allow_realloc: bool,
    pub max_nb_chunks: usize,
    pub chunk_compression_type: CompressionType,
    pub chunk_compression_level: u32,
    pub file_fusing_shrink_size: u64,
    pub enable_fusing: bool,
    pub merge: MF,
}

impl<MF> SorterBuilder<MF> {
    pub fn new(merge: MF) -> Self {
        SorterBuilder {
            dump_threshold: DEFAULT_SORTER_MEMORY,
            allow_realloc: true,
            max_nb_chunks: DEFAULT_NB_CHUNKS,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: 0,
            file_fusing_shrink_size: DEFAULT_SHRINK_SIZE,
            enable_fusing: false,
            merge,
        }
    }

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

    pub fn chunk_compression_type(&mut self, compression: CompressionType) -> &mut Self {
        self.chunk_compression_type = compression;
        self
    }

    pub fn chunk_compression_level(&mut self, level: u32) -> &mut Self {
        self.chunk_compression_level = level;
        self
    }

    pub fn file_fusing_shrink_size(&mut self, shrink_size: u64) -> &mut Self {
        self.file_fusing_shrink_size = shrink_size;
        self.enable_fusing = true;
        self
    }

    pub fn enable_fusing(&mut self) -> &mut Self {
        self.enable_fusing = true;
        self
    }

    pub fn build(self) -> Sorter<MF> {
        let mut file_fuse_builder = FileFuseBuilder::new();
        if self.enable_fusing {
            file_fuse_builder.shrink_size(self.file_fusing_shrink_size);
        }

        let capacity = if self.allow_realloc {
            INITIAL_SORTER_VEC_SIZE
        } else {
            self.dump_threshold
        };

        Sorter {
            chunks: Vec::new(),
            entries: Entries::with_capacity(capacity),
            allow_realloc: self.allow_realloc,
            dump_threshold: self.dump_threshold,
            max_nb_chunks: self.max_nb_chunks,
            chunk_compression_type: self.chunk_compression_type,
            chunk_compression_level: self.chunk_compression_level,
            file_fuse_builder,
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
    buffer: Box<[u8]>,

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
        Self {
            buffer: Self::new_buffer(capacity),
            entries_len: 0,
            bounds_count: 0,
        }
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

    /// Allocates a new buffer of the given size, it is correctly aligned to store `EntryBound`s.
    fn new_buffer(size: usize) -> Box<[u8]> {
        // We create a boxed slice of EntryBounds to make sure that the memory
        // alignment is valid as we will not only store bytes but also EntryBounds.
        let size = size / size_of::<EntryBound>() + size_of::<EntryBound>();
        let mut buffer = Vec::new();
        buffer.reserve_exact(size);
        buffer.resize_with(size, EntryBound::default);
        let buffer = buffer.into_boxed_slice();

        // We then convert the boxed slice of EntryBounds into a boxed slice of bytes.
        let ptr = Box::into_raw(buffer) as *mut [u8];
        unsafe { Box::from_raw(ptr) }
    }

    /// Doubles the size of the internal buffer, copies the entries and bounds into the new buffer.
    fn reallocate_buffer(&mut self) {
        let bounds_end = self.bounds_count * size_of::<EntryBound>();
        let bounds_bytes = &self.buffer[..bounds_end];

        let entries_start = self.buffer.len() - self.entries_len;
        let entries_bytes = &self.buffer[entries_start..];

        let mut new_buffer = Self::new_buffer(self.buffer.len() * 2);
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

pub struct Sorter<MF> {
    chunks: Vec<File>,
    entries: Entries,
    allow_realloc: bool,
    dump_threshold: usize,
    max_nb_chunks: usize,
    chunk_compression_type: CompressionType,
    chunk_compression_level: u32,
    file_fuse_builder: FileFuseBuilder,
    merge: MF,
}

impl<MF> Sorter<MF> {
    pub fn builder(merge: MF) -> SorterBuilder<MF> {
        SorterBuilder::new(merge)
    }

    pub fn new(merge: MF) -> Sorter<MF> {
        SorterBuilder::new(merge).build()
    }
}

impl<MF, U> Sorter<MF>
where
    MF: for<'a> Fn(&[u8], &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>, U>,
{
    pub fn insert<K, V>(&mut self, key: K, val: V) -> Result<(), Error<U>>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let val = val.as_ref();

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
        debug!("writing a chunk...");
        let before_write = Instant::now();

        let file = tempfile::tempfile()?;
        let mut writer = WriterBuilder::new()
            .compression_type(self.chunk_compression_type)
            .compression_level(self.chunk_compression_level)
            .build(file)?;

        self.entries.sort_unstable_by_key();

        let mut current = None;
        for (key, value) in self.entries.iter() {
            match current.as_mut() {
                None => {
                    let key = key.to_vec();
                    let value = Cow::Owned(value.to_vec());
                    current = Some((key, vec![value]));
                }
                Some((current_key, vals)) => {
                    if current_key == key {
                        vals.push(Cow::Owned(value.to_vec()));
                    } else {
                        let merged_val = (self.merge)(&current_key, &vals).map_err(Error::Merge)?;
                        writer.insert(&current_key, &merged_val)?;
                        current_key.clear();
                        vals.clear();
                        current_key.extend_from_slice(key);
                        vals.push(Cow::Owned(value.to_vec()));
                    }
                }
            }
        }

        self.entries.clear();

        if let Some((key, vals)) = current.take() {
            let merged_val = (self.merge)(&key, &vals).map_err(Error::Merge)?;
            writer.insert(&key, &merged_val)?;
        }

        let file = writer.into_inner()?;
        self.chunks.push(file);

        debug!("writing a chunk took {:.02?}", before_write.elapsed());

        Ok(())
    }

    fn merge_chunks(&mut self) -> Result<(), Error<U>> {
        debug!("merging {} chunks...", self.chunks.len());
        let before_merge = Instant::now();
        let original_nb_chunks = self.chunks.len();

        let file = tempfile::tempfile()?;
        let mut writer = WriterBuilder::new()
            .compression_type(self.chunk_compression_type)
            .compression_level(self.chunk_compression_level)
            .build(file)?;

        // Drain the chunks to mmap them and store them into a vector.
        let file_fuse_builder = self.file_fuse_builder;
        let sources: Result<Vec<_>, Error<U>> = self
            .chunks
            .drain(..)
            .map(|mut file| {
                file.seek(SeekFrom::Start(0))?;
                let file = file_fuse_builder.build(file);
                Reader::new(file).map_err(Error::convert_merge_error)
            })
            .collect();

        // Create a merger to merge all those chunks.
        let mut builder = Merger::builder(&self.merge);
        builder.extend(sources?);
        let merger = builder.build();

        let mut iter = merger
            .into_merge_iter()
            .map_err(Error::convert_merge_error)?;
        while let Some((key, val)) = iter.next()? {
            writer.insert(key, val)?;
        }

        let file = writer.into_inner()?;
        self.chunks.push(file);

        debug!(
            "merging {} chunks took {:.02?}",
            original_nb_chunks,
            before_merge.elapsed()
        );

        Ok(())
    }

    pub fn write_into<W: io::Write>(self, writer: &mut Writer<W>) -> Result<(), Error<U>> {
        let mut iter = self.into_iter()?;
        while let Some((key, val)) = iter.next()? {
            writer.insert(key, val)?;
        }
        Ok(())
    }

    pub fn into_iter(mut self) -> Result<MergerIter<FileFuse, MF>, Error<U>> {
        // Flush the pending unordered entries.
        self.write_chunk()?;

        let file_fuse_builder = self.file_fuse_builder;
        let sources: Result<Vec<_>, Error<U>> = self
            .chunks
            .into_iter()
            .map(|mut file| {
                file.seek(SeekFrom::Start(0))?;
                let file = file_fuse_builder.build(file);
                Reader::new(file).map_err(Error::convert_merge_error)
            })
            .collect();

        let mut builder = Merger::builder(self.merge);
        builder.extend(sources?);

        builder
            .build()
            .into_merge_iter()
            .map_err(Error::convert_merge_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    #[test]
    fn simple() {
        fn merge<'a>(_key: &[u8], vals: &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>, Infallible> {
            Ok(vals.iter().map(AsRef::as_ref).flatten().cloned().collect())
        }

        let mut sorter = SorterBuilder::new(merge)
            .chunk_compression_type(CompressionType::Snappy)
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
                _ => panic!(),
            }
        }
    }
}
