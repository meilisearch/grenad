use std::convert::TryInto;
use std::mem;
use std::num::NonZeroUsize;
use std::ops::Deref;

use crate::varint::varint_encode32;

const DEFAULT_INDEX_KEY_INTERVAL: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(8) };

#[derive(Debug, Copy, Clone)]
pub struct BlockWriterBuilder {
    index_key_interval: NonZeroUsize,
}

impl BlockWriterBuilder {
    pub fn new() -> BlockWriterBuilder {
        BlockWriterBuilder { index_key_interval: DEFAULT_INDEX_KEY_INTERVAL }
    }

    /// The interval at which we store the index of a key in the
    /// footer index, used to seek into the block.
    pub fn index_key_interval(&mut self, interval: NonZeroUsize) -> &mut Self {
        self.index_key_interval = interval;
        self
    }

    pub fn build(&self) -> BlockWriter {
        BlockWriter {
            buffer: Vec::new(),
            last_key: None,
            index_key_interval: self.index_key_interval,
            index_offsets: vec![0],
            index_key_counter: 0,
        }
    }
}

impl Default for BlockWriterBuilder {
    fn default() -> BlockWriterBuilder {
        BlockWriterBuilder::new()
    }
}

/// A `BlockWriter` generates blocks where keys and values are one after the other.
///
/// The key values are appended one after the other in the block, these entries are
/// followed by an index that indicates the offset of some of the keys. This footer
/// index stores an offset every `index_key_interval`.
pub struct BlockWriter {
    /// The buffer in which we store the indexed bytes.
    /// It contains the key, values and the footer index.
    buffer: Vec<u8>,
    /// The last key written in this block, used to check
    /// that the keys are appended in order.
    last_key: Option<Vec<u8>>,
    /// The interval at which we store the index of a key in the
    /// footer index, used to seek into the block.
    index_key_interval: NonZeroUsize,
    /// The index itself that store some key offsets.
    index_offsets: Vec<u64>,
    /// This counter increments every time we put a key in the key index and
    /// resets every `index_key_interval`, every time it resets we store
    /// the offset of the inserted key in the footer `index_offsets` vector.
    index_key_counter: usize,
}

impl BlockWriter {
    pub fn new() -> BlockWriter {
        BlockWriterBuilder::new().build()
    }

    pub fn builder() -> BlockWriterBuilder {
        BlockWriterBuilder::new()
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.last_key = None;
        self.index_offsets.truncate(1); // equivalent to vec![0]
        self.index_key_counter = 0;
    }

    pub fn current_size_estimate(&self) -> usize {
        // The size of the buffer + the total size of the indexes in the footer + the count of indexes.
        self.buffer.len() + self.index_offsets.len() * mem::size_of::<u64>() + mem::size_of::<u32>()
    }

    /// Returns the last key inserted in this block.
    pub fn last_key(&self) -> Option<&[u8]> {
        self.last_key.as_ref().map(AsRef::as_ref)
    }

    /// Insert a key that must be greater than the previously added one.
    pub fn insert(&mut self, key: &[u8], val: &[u8]) {
        debug_assert!(self.index_key_counter <= self.index_key_interval.get());
        assert!(key.len() <= u32::max_value() as usize);
        assert!(val.len() <= u32::max_value() as usize);

        if self.index_key_counter == self.index_key_interval.get() {
            self.index_offsets.push(self.buffer.len() as u64);
            self.index_key_counter = 0;
        }

        // Check if the key is greater than the last added key
        // and save the current key to become the last key.
        match &mut self.last_key {
            Some(last_key) => {
                assert!(key > last_key, "{:?} must be greater than {:?}", key, last_key);
                last_key.clear();
                last_key.extend_from_slice(key);
            }
            None => self.last_key = Some(key.to_vec()),
        }

        // add "[key length][value length]" to buffer
        let mut buf = [0; 10];
        self.buffer.extend_from_slice(varint_encode32(&mut buf, key.len() as u32));
        self.buffer.extend_from_slice(varint_encode32(&mut buf, val.len() as u32));
        // add key to buffer followed by value
        self.buffer.extend_from_slice(key);
        self.buffer.extend_from_slice(val);
        self.index_key_counter += 1;
    }

    pub fn finish(&mut self) -> BlockBuffer {
        // We write the index offsets at the end of the file,
        // followed by the number of index offsets.
        let index_offsets_count: u32 = self.index_offsets.len().try_into().unwrap();
        self.buffer.extend(self.index_offsets.iter().copied().map(u64::to_be_bytes).flatten());
        self.buffer.extend_from_slice(&index_offsets_count.to_be_bytes());

        BlockBuffer { block_builder: self }
    }
}

impl Default for BlockWriter {
    fn default() -> BlockWriter {
        BlockWriter::new()
    }
}

/// A type that represents the finished content of a `BlockWriter`.
///
/// This type implements `Deref` which exposes useful methods.
pub struct BlockBuffer<'a> {
    block_builder: &'a mut BlockWriter,
}

impl Deref for BlockBuffer<'_> {
    type Target = BlockWriter;

    fn deref(&self) -> &Self::Target {
        &self.block_builder
    }
}

impl AsRef<[u8]> for BlockBuffer<'_> {
    fn as_ref(&self) -> &[u8] {
        &self.block_builder.buffer
    }
}

impl Drop for BlockBuffer<'_> {
    fn drop(&mut self) {
        self.block_builder.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn easy_block() {
        let mut bb = BlockWriter::new();

        for x in 0..2000i32 {
            let x = x.to_be_bytes();
            bb.insert(&x, &x);
            if bb.current_size_estimate() >= 1024 {
                let buffer = bb.finish();
                assert!(buffer.as_ref().len() >= 1024);
            }
        }
    }
}
