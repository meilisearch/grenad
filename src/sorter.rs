use std::borrow::Cow;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::mem::size_of;
use std::time::Instant;
use std::{cmp, io};

use log::debug;

const INITIAL_SORTER_VEC_SIZE: usize = 131_072; // 128KB
const DEFAULT_SORTER_MEMORY: usize = 1_073_741_824; // 1GB
const MIN_SORTER_MEMORY: usize = 10_485_760; // 10MB

const DEFAULT_NB_CHUNKS: usize = 25;
const MIN_NB_CHUNKS: usize = 1;

use crate::file_fuse::DEFAULT_SHRINK_SIZE;
use crate::{CompressionType, Writer, WriterBuilder};
use crate::{Error, FileFuse, FileFuseBuilder, Reader};
use crate::{Merger, MergerStream, SliceAtLeast};

#[derive(Debug, Clone, Copy)]
pub struct SorterBuilder<MF> {
    pub max_memory: usize,
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
            max_memory: DEFAULT_SORTER_MEMORY,
            max_nb_chunks: DEFAULT_NB_CHUNKS,
            chunk_compression_type: CompressionType::None,
            chunk_compression_level: 0,
            file_fusing_shrink_size: DEFAULT_SHRINK_SIZE,
            enable_fusing: false,
            merge,
        }
    }

    pub fn max_memory(&mut self, memory: usize) -> &mut Self {
        self.max_memory = cmp::max(memory, MIN_SORTER_MEMORY);
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

        Sorter {
            chunks: Vec::new(),
            entries: Vec::with_capacity(INITIAL_SORTER_VEC_SIZE),
            entry_bytes: 0,
            max_memory: self.max_memory,
            max_nb_chunks: self.max_nb_chunks,
            chunk_compression_type: self.chunk_compression_type,
            chunk_compression_level: self.chunk_compression_level,
            file_fuse_builder,
            merge: self.merge,
        }
    }
}

struct Entry {
    data: Vec<u8>,
    key_len: usize,
}

impl Entry {
    pub fn new(key: &[u8], val: &[u8]) -> Entry {
        let mut data = Vec::new();
        data.reserve_exact(key.len() + val.len());
        data.extend_from_slice(key);
        data.extend_from_slice(val);
        Entry { data, key_len: key.len() }
    }

    pub fn key(&self) -> &[u8] {
        &self.data[..self.key_len]
    }

    pub fn val(&self) -> &[u8] {
        &self.data[self.key_len..]
    }
}

pub struct Sorter<MF> {
    chunks: Vec<File>,
    entries: Vec<Entry>,
    /// The number of bytes allocated by the entries.
    entry_bytes: usize,
    max_memory: usize,
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
    MF: for<'a> Fn(&[u8], &SliceAtLeast<Cow<'a, [u8]>, 2>) -> Result<Vec<u8>, U>,
{
    pub fn insert<K, V>(&mut self, key: K, val: V) -> Result<(), Error<U>>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let val = val.as_ref();

        let ent = Entry::new(key, val);
        self.entry_bytes += ent.data.len();
        self.entries.push(ent);

        let entries_vec_size = self.entries.len() * size_of::<Entry>();
        if self.entry_bytes + entries_vec_size >= self.max_memory {
            self.write_chunk()?;
            if self.chunks.len() > self.max_nb_chunks {
                self.merge_chunks()?;
            }
        }

        Ok(())
    }

    fn write_chunk(&mut self) -> Result<(), Error<U>> {
        debug!("writing a chunk...");
        let before_write = Instant::now();

        let file = tempfile::tempfile()?;
        let mut writer = WriterBuilder::new()
            .compression_type(self.chunk_compression_type)
            .compression_level(self.chunk_compression_level)
            .build(file)?;

        self.entries.sort_unstable_by(|a, b| a.key().cmp(b.key()));

        let mut current = None;
        for entry in self.entries.drain(..) {
            match current.as_mut() {
                None => {
                    let key = entry.key().to_vec();
                    let val = Cow::Owned(entry.val().to_vec());
                    current = Some((key, vec![val]));
                }
                Some((key, vals)) => {
                    if key != entry.key() {
                        let merged_val: Vec<u8> = if vals.len() == 1 {
                            vals.pop().map(Cow::into_owned).unwrap()
                        } else {
                            let vals = SliceAtLeast::new(vals).unwrap();
                            (self.merge)(key, vals).map_err(Error::Merge)?
                        };
                        writer.insert(&key, &merged_val)?;
                        key.clear();
                        vals.clear();
                        key.extend_from_slice(entry.key());
                    }
                    vals.push(Cow::Owned(entry.val().to_vec()));
                }
            }
        }

        if let Some((key, mut vals)) = current.take() {
            let merged_val = if vals.len() == 1 {
                vals.pop().map(Cow::into_owned).unwrap()
            } else {
                let vals = SliceAtLeast::new(&vals).unwrap();
                (self.merge)(&key, vals).map_err(Error::Merge)?
            };
            writer.insert(&key, &merged_val)?;
        }

        let file = writer.into_inner()?;
        self.chunks.push(file);
        self.entry_bytes = 0;

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

        let mut iter = merger.into_merge_stream().map_err(Error::convert_merge_error)?;
        while let Some((key, val)) = iter.next()? {
            writer.insert(key, val)?;
        }

        let file = writer.into_inner()?;
        self.chunks.push(file);

        debug!("merging {} chunks took {:.02?}", original_nb_chunks, before_merge.elapsed());

        Ok(())
    }

    pub fn write_into<W: io::Write>(self, writer: &mut Writer<W>) -> Result<(), Error<U>> {
        let mut iter = self.into_stream()?;
        while let Some((key, val)) = iter.next()? {
            writer.insert(key, val)?;
        }
        Ok(())
    }

    pub fn into_stream(mut self) -> Result<MergerStream<FileFuse, MF>, Error<U>> {
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

        builder.build().into_merge_stream().map_err(Error::convert_merge_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    #[test]
    fn simple() {
        fn merge(_key: &[u8], vals: &SliceAtLeast<Cow<[u8]>, 2>) -> Result<Vec<u8>, Infallible> {
            Ok(vals.iter().map(AsRef::as_ref).flatten().cloned().collect())
        }

        let mut sorter =
            SorterBuilder::new(merge).chunk_compression_type(CompressionType::Snappy).build();

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
