use std::borrow::Cow;
use std::cmp::{Ordering, Reverse};
use std::collections::binary_heap::{BinaryHeap, PeekMut};
use std::{io, mem};

use crate::{Error, Reader, SliceAtLeast, Writer};

pub struct Entry<R> {
    iter: Reader<R>,
    key: Vec<u8>,
    val: Vec<u8>,
}

impl<R: io::Read> Entry<R> {
    // also fills the entry
    fn new(iter: Reader<R>) -> Result<Option<Entry<R>>, Error> {
        let mut entry = Entry { iter, key: Vec::new(), val: Vec::new() };

        if !entry.fill()? {
            return Ok(None);
        }

        Ok(Some(entry))
    }

    fn fill(&mut self) -> Result<bool, Error> {
        self.key.clear();
        self.val.clear();

        match self.iter.next()? {
            Some((key, val)) => {
                self.key.extend_from_slice(key);
                self.val.extend_from_slice(val);
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

impl<R: io::Read> Ord for Entry<R> {
    fn cmp(&self, other: &Entry<R>) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<R: io::Read> Eq for Entry<R> {}

impl<R: io::Read> PartialEq for Entry<R> {
    fn eq(&self, other: &Entry<R>) -> bool {
        self.key == other.key
    }
}

impl<R: io::Read> PartialOrd for Entry<R> {
    fn partial_cmp(&self, other: &Entry<R>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct MergerBuilder<R, MF> {
    sources: Vec<Reader<R>>,
    merge: MF,
}

impl<R, MF> MergerBuilder<R, MF> {
    pub fn new(merge: MF) -> Self {
        MergerBuilder { merge, sources: Vec::new() }
    }

    pub fn add(&mut self, source: Reader<R>) -> &mut Self {
        self.push(source);
        self
    }

    pub fn push(&mut self, source: Reader<R>) {
        self.sources.push(source);
    }

    pub fn build(self) -> Merger<R, MF> {
        Merger { sources: self.sources, merge: self.merge }
    }
}

impl<R, MF> Extend<Reader<R>> for MergerBuilder<R, MF> {
    fn extend<T: IntoIterator<Item = Reader<R>>>(&mut self, iter: T) {
        self.sources.extend(iter);
    }
}

pub struct Merger<R, MF> {
    sources: Vec<Reader<R>>,
    merge: MF,
}

impl<R, MF> Merger<R, MF> {
    pub fn builder(merge: MF) -> MergerBuilder<R, MF> {
        MergerBuilder::new(merge)
    }
}

impl<R: io::Read, MF> Merger<R, MF> {
    pub fn into_merge_stream(self) -> Result<MergerStream<R, MF>, Error> {
        let mut heap = BinaryHeap::new();
        for source in self.sources {
            if let Some(entry) = Entry::new(source)? {
                heap.push(Reverse(entry));
            }
        }

        Ok(MergerStream {
            merge: self.merge,
            heap,
            cur_key: Vec::new(),
            cur_vals: Vec::new(),
            merged_val: Vec::new(),
            pending: false,
        })
    }
}

impl<R, MF, U> Merger<R, MF>
where
    R: io::Read,
    MF: for<'a> Fn(&[u8], &SliceAtLeast<Cow<'a, [u8]>, 2>) -> Result<Vec<u8>, U>,
{
    pub fn write_into<W: io::Write>(self, writer: &mut Writer<W>) -> Result<(), Error<U>> {
        let mut iter = self.into_merge_stream().map_err(Error::convert_merge_error)?;
        while let Some((key, val)) = iter.next()? {
            writer.insert(key, val)?;
        }
        Ok(())
    }
}

pub struct MergerStream<R, MF> {
    merge: MF,
    heap: BinaryHeap<Reverse<Entry<R>>>,
    cur_key: Vec<u8>,
    cur_vals: Vec<Cow<'static, [u8]>>,
    merged_val: Vec<u8>,
    pending: bool,
}

impl<R, MF, U> MergerStream<R, MF>
where
    R: io::Read,
    MF: for<'a> Fn(&[u8], &SliceAtLeast<Cow<'a, [u8]>, 2>) -> Result<Vec<u8>, U>,
{
    #[allow(clippy::should_implement_trait, clippy::type_complexity)]
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>, Error<U>> {
        self.cur_key.clear();
        self.cur_vals.clear();

        while let Some(mut entry) = self.heap.peek_mut() {
            if self.cur_key.is_empty() {
                self.cur_key.extend_from_slice(&entry.0.key);
                self.cur_vals.clear();
                self.pending = true;
            }

            if self.cur_key == entry.0.key {
                self.cur_vals.push(Cow::Owned(mem::take(&mut entry.0.val)));
                match entry.0.fill() {
                    Ok(filled) => {
                        if !filled {
                            PeekMut::pop(entry);
                        }
                    }
                    Err(e) => return Err(e.convert_merge_error()),
                }
            } else {
                break;
            }
        }

        if self.pending {
            self.merged_val = if self.cur_vals.len() == 1 {
                self.cur_vals.pop().map(Cow::into_owned).unwrap()
            } else {
                let cur_vals = SliceAtLeast::new(&self.cur_vals).unwrap();
                match (self.merge)(&self.cur_key, cur_vals) {
                    Ok(val) => val,
                    Err(e) => return Err(Error::Merge(e)),
                }
            };
            self.pending = false;
            Ok(Some((&self.cur_key, &self.merged_val)))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[cfg(target_os = "linux")]
    fn file_fusing() {
        use std::convert::Infallible;
        use std::fs::OpenOptions;
        use std::io::{Seek, SeekFrom};

        use super::*;

        use crate::{FileFuse, SliceAtLeast, Writer};

        fn merge(_key: &[u8], vals: &SliceAtLeast<Cow<[u8]>, 2>) -> Result<Vec<u8>, Infallible> {
            assert!(vals.windows(2).all(|win| win[0] == win[1]));
            let ([first, _second], _tail) = vals.deconstruct_front();
            Ok(first.to_vec())
        }

        let mut options = OpenOptions::new();
        options.create(true).truncate(true).read(true).write(true);

        let file1 = options.open("target/merger-file-fusing-1").unwrap();
        let file2 = options.open("target/merger-file-fusing-2").unwrap();

        let mut readers = vec![];
        for file in vec![file1, file2] {
            let wb = Writer::builder();
            let mut writer = wb.build(file).unwrap();

            for x in 0..2000u32 {
                let x = x.to_be_bytes();
                writer.insert(&x, &x).unwrap();
            }

            let mut file = writer.into_inner().unwrap();
            assert_ne!(file.metadata().unwrap().len(), 0);

            file.seek(SeekFrom::Start(0)).unwrap();
            let file = FileFuse::builder().shrink_size(4096).build(file);
            let reader = Reader::new(file).unwrap();
            readers.push(reader);
        }

        let mut builder = Merger::builder(merge);
        builder.extend(readers);
        let merger = builder.build();

        let file3 = options.open("target/merger-file-fusing-3").unwrap();
        let mut writer = Writer::builder().build(file3).unwrap();

        merger.write_into(&mut writer).unwrap();

        let mut file = writer.into_inner().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = Reader::new(file).unwrap();

        let mut x: u32 = 0;
        while let Some((k, v)) = reader.next().unwrap() {
            assert_eq!(k, x.to_be_bytes());
            assert_eq!(v, x.to_be_bytes());
            x += 1;
        }

        assert_eq!(x, 2000);
    }
}
