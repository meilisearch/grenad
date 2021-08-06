use std::borrow::Cow;
use std::cmp::{Ordering, Reverse};
use std::collections::binary_heap::{BinaryHeap, PeekMut};
use std::{io, mem};

use crate::{Error, Reader, Writer};

/// A struct that is used to configure a [`Merger`] with the sources to merge.
pub struct MergerBuilder<R, MF> {
    sources: Vec<Reader<R>>,
    merge: MF,
}

impl<R, MF> MergerBuilder<R, MF> {
    /// Creates a [`MergerBuilder`] from a merge function, it can be
    /// used to configure your [`Merger`] with the sources to merge.
    pub fn new(merge: MF) -> Self {
        MergerBuilder { merge, sources: Vec::new() }
    }

    /// Add a source to merge, this function can be chained.
    pub fn add(&mut self, source: Reader<R>) -> &mut Self {
        self.push(source);
        self
    }

    /// Push a source to merge, this function can be used in a loop.
    pub fn push(&mut self, source: Reader<R>) {
        self.sources.push(source);
    }

    /// Creates a [`Merger`] that will merge the sources.
    pub fn build(self) -> Merger<R, MF> {
        Merger { sources: self.sources, merge: self.merge }
    }
}

impl<R, MF> Extend<Reader<R>> for MergerBuilder<R, MF> {
    fn extend<T: IntoIterator<Item = Reader<R>>>(&mut self, iter: T) {
        self.sources.extend(iter);
    }
}

struct Entry<R> {
    iter: Reader<R>,
    key: Vec<u8>,
    value: Vec<u8>,
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

/// A struct you can use to merge entries from the sources by using the merge
/// function provided to merge values when entries have the same key.
pub struct Merger<R, MF> {
    sources: Vec<Reader<R>>,
    merge: MF,
}

impl<R, MF> Merger<R, MF> {
    /// Creates a [`MergerBuilder`] from a merge function, it can be
    /// used to configure your [`Merger`] with the sources to merge.
    pub fn builder(merge: MF) -> MergerBuilder<R, MF> {
        MergerBuilder::new(merge)
    }
}

impl<R: io::Read, MF> Merger<R, MF> {
    /// Consumes this [`Merger`] and outputs a stream of the merged entries in key-order.
    pub fn into_merger_iter(self) -> Result<MergerIter<R, MF>, Error> {
        let mut heap = BinaryHeap::new();
        for mut source in self.sources {
            if let Some((key, value)) = source.next()? {
                let key = key.to_vec();
                let value = value.to_vec();
                heap.push(Reverse(Entry { iter: source, key, value }));
            }
        }

        Ok(MergerIter {
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
    MF: for<'a> Fn(&[u8], &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>, U>,
{
    /// Consumes this [`Merger`] and streams the entries to the [`Writer`] given in parameter.
    pub fn write_into<W: io::Write>(self, writer: &mut Writer<W>) -> Result<(), Error<U>> {
        let mut iter = self.into_merger_iter().map_err(Error::convert_merge_error)?;
        while let Some((key, val)) = iter.next()? {
            writer.insert(key, val)?;
        }
        Ok(())
    }
}

/// An iterator that yield the merged entries in key-order.
pub struct MergerIter<R, MF> {
    merge: MF,
    heap: BinaryHeap<Reverse<Entry<R>>>,
    cur_key: Vec<u8>,
    cur_vals: Vec<Cow<'static, [u8]>>,
    merged_val: Vec<u8>,
    pending: bool,
}

impl<R, MF, U> MergerIter<R, MF>
where
    R: io::Read,
    MF: for<'a> Fn(&[u8], &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>, U>,
{
    /// Yield the entries in key-order where values have been merged when needed.
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
                self.cur_vals.push(Cow::Owned(mem::take(&mut entry.0.value)));
                // Replace the key/value buffers by empty ones, empty Vecs don't
                // allocate, this way we reuse the previously allocated memory.
                let mut key_buffer = mem::take(&mut entry.0.key);
                let mut value_buffer = mem::take(&mut entry.0.value);
                match entry.0.iter.next().map_err(Error::convert_merge_error)? {
                    Some((key, value)) => {
                        key_buffer.clear();
                        value_buffer.clear();
                        key_buffer.extend_from_slice(key);
                        value_buffer.extend_from_slice(value);
                        entry.0.key = key_buffer;
                        entry.0.value = value_buffer;
                    }
                    None => {
                        PeekMut::pop(entry);
                    }
                }
            } else {
                break;
            }
        }

        if self.pending {
            match (self.merge)(&self.cur_key, &self.cur_vals) {
                Ok(value) => self.merged_val = value.into_owned(),
                Err(e) => return Err(Error::Merge(e)),
            }
            self.pending = false;
            Ok(Some((&self.cur_key, &self.merged_val)))
        } else {
            Ok(None)
        }
    }
}
