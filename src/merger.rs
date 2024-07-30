use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io;
use std::iter::once;

use crate::{Error, MergeFunction, ReaderCursor, Writer};

/// A struct that is used to configure a [`Merger`] with the sources to merge.
pub struct MergerBuilder<R, MF> {
    sources: Vec<ReaderCursor<R>>,
    merge: MF,
}

impl<R, MF> MergerBuilder<R, MF> {
    /// Creates a [`MergerBuilder`] from a merge function, it can be
    /// used to configure your [`Merger`] with the sources to merge.
    pub fn new(merge: MF) -> Self {
        MergerBuilder { merge, sources: Vec::new() }
    }

    /// Add a source to merge, this function can be chained.
    pub fn add(mut self, source: ReaderCursor<R>) -> Self {
        self.push(source);
        self
    }

    /// Push a source to merge, this function can be used in a loop.
    pub fn push(&mut self, source: ReaderCursor<R>) {
        self.sources.push(source);
    }

    /// Creates a [`Merger`] that will merge the sources.
    pub fn build(self) -> Merger<R, MF> {
        Merger { sources: self.sources, merge: self.merge }
    }
}

impl<R, MF> Extend<ReaderCursor<R>> for MergerBuilder<R, MF> {
    fn extend<T: IntoIterator<Item = ReaderCursor<R>>>(&mut self, iter: T) {
        self.sources.extend(iter);
    }
}

struct Entry<R> {
    cursor: ReaderCursor<R>,
    source_index: usize,
}

impl<R> Ord for Entry<R> {
    fn cmp(&self, other: &Entry<R>) -> Ordering {
        let skey = self.cursor.current().map(|(k, _)| k);
        let okey = other.cursor.current().map(|(k, _)| k);
        skey.cmp(&okey).then(self.source_index.cmp(&other.source_index)).reverse()
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
    sources: Vec<ReaderCursor<R>>,
    merge: MF,
}

impl<R, MF> Merger<R, MF> {
    /// Creates a [`MergerBuilder`] from a merge function, it can be
    /// used to configure your [`Merger`] with the sources to merge.
    pub fn builder(merge: MF) -> MergerBuilder<R, MF> {
        MergerBuilder::new(merge)
    }
}

impl<R: io::Read + io::Seek, MF> Merger<R, MF> {
    /// Consumes this [`Merger`] and outputs a stream of the merged entries in key-order.
    pub fn into_stream_merger_iter(self) -> Result<MergerIter<R, MF>, Error> {
        let mut heap = BinaryHeap::new();
        for (index, mut source) in self.sources.into_iter().enumerate() {
            if source.move_on_next()?.is_some() {
                heap.push(Entry { cursor: source, source_index: index });
            }
        }

        Ok(MergerIter {
            merge_function: self.merge,
            heap,
            current_key: Vec::new(),
            merged_value: Vec::new(),
            tmp_entries: Vec::new(),
        })
    }
}

impl<R, MF> Merger<R, MF>
where
    R: io::Read + io::Seek,
    MF: MergeFunction,
{
    /// Consumes this [`Merger`] and streams the entries to the [`Writer`] given in parameter.
    pub fn write_into_stream_writer<W: io::Write>(
        self,
        writer: &mut Writer<W>,
    ) -> crate::Result<(), MF::Error> {
        let mut iter = self.into_stream_merger_iter().map_err(Error::convert_merge_error)?;
        while let Some((key, val)) = iter.next()? {
            writer.insert(key, val)?;
        }
        Ok(())
    }
}

/// An iterator that yield the merged entries in key-order.
pub struct MergerIter<R, MF> {
    merge_function: MF,
    heap: BinaryHeap<Entry<R>>,
    current_key: Vec<u8>,
    merged_value: Vec<u8>,
    /// We keep this buffer to avoid allocating a vec every time.
    tmp_entries: Vec<Entry<R>>,
}

impl<R, MF> MergerIter<R, MF>
where
    R: io::Read + io::Seek,
    MF: MergeFunction,
{
    /// Yield the entries in key-order where values have been merged when needed.
    pub fn next(&mut self) -> crate::Result<Option<(&[u8], &[u8])>, MF::Error> {
        let first_entry = match self.heap.pop() {
            Some(entry) => entry,
            None => return Ok(None),
        };

        let (first_key, first_value) = match first_entry.cursor.current() {
            Some((key, value)) => (key, value),
            None => return Ok(None),
        };

        self.tmp_entries.clear();
        while let Some(entry) = self.heap.peek() {
            if let Some((key, _value)) = entry.cursor.current() {
                if first_key == key {
                    if let Some(entry) = self.heap.pop() {
                        self.tmp_entries.push(entry);
                    }
                } else {
                    break;
                }
            }
        }

        // Extract the currently pointed values from the entries.
        let other_values =
            self.tmp_entries.iter().filter_map(|e| e.cursor.current().map(|(_, v)| v));
        let values: Vec<_> = once(first_value).chain(other_values).map(Cow::Borrowed).collect();

        match self.merge_function.merge(first_key, &values) {
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
        for mut entry in once(first_entry).chain(self.tmp_entries.drain(..)) {
            if entry.cursor.move_on_next().map_err(Error::convert_merge_error)?.is_some() {
                self.heap.push(entry);
            }
        }

        Ok(Some((&self.current_key, &self.merged_value)))
    }
}
