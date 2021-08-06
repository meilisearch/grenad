use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io;
use std::iter::once;

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
    pub fn add(mut self, source: Reader<R>) -> Self {
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
}

impl<R> Ord for Entry<R> {
    fn cmp(&self, other: &Entry<R>) -> Ordering {
        let skey = self.iter.current().map(|(k, _)| k);
        let okey = other.iter.current().map(|(k, _)| k);
        skey.cmp(&okey).reverse()
    }
}

impl<R> Eq for Entry<R> {}

impl<R> PartialEq for Entry<R> {
    fn eq(&self, other: &Entry<R>) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<R> PartialOrd for Entry<R> {
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
            if source.next()?.is_some() {
                heap.push(Entry { iter: source });
            }
        }

        Ok(MergerIter {
            merge: self.merge,
            heap,
            current_key: Vec::new(),
            merged_value: Vec::new(),
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
    heap: BinaryHeap<Entry<R>>,
    current_key: Vec<u8>,
    merged_value: Vec<u8>,
}

impl<R, MF, U> MergerIter<R, MF>
where
    R: io::Read,
    MF: for<'a> Fn(&[u8], &[Cow<'a, [u8]>]) -> Result<Cow<'a, [u8]>, U>,
{
    /// Yield the entries in key-order where values have been merged when needed.
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>, Error<U>> {
        let first_entry = match self.heap.pop() {
            Some(entry) => entry,
            None => return Ok(None),
        };

        let (first_key, first_value) = match first_entry.iter.current() {
            Some((key, value)) => (key, value),
            None => return Ok(None),
        };

        let mut other_entries = Vec::new();
        while let Some(entry) = self.heap.peek() {
            if let Some((key, _value)) = entry.iter.current() {
                if first_key == key {
                    if let Some(entry) = self.heap.pop() {
                        other_entries.push(entry);
                    }
                } else {
                    break;
                }
            }
        }

        let other_values = other_entries.iter().filter_map(|e| e.iter.current().map(|(_k, v)| v));
        let values: Vec<_> = once(first_value).chain(other_values).map(Cow::Borrowed).collect();

        match (self.merge)(&first_key, &values) {
            Ok(value) => {
                self.current_key.clear();
                self.current_key.extend_from_slice(first_key);
                match value {
                    Cow::Owned(value) => self.merged_value = value,
                    Cow::Borrowed(value) => {
                        self.merged_value.clear();
                        self.merged_value.extend_from_slice(value);
                    }
                }
            }
            Err(e) => return Err(Error::Merge(e)),
        }

        // Don't forget to put the entries back into the heap.
        for mut entry in once(first_entry).chain(other_entries) {
            if entry.iter.next().map_err(Error::convert_merge_error)?.is_some() {
                self.heap.push(entry);
            }
        }

        Ok(Some((&self.current_key, &self.merged_value)))
    }
}
