use std::convert::TryInto;
use std::mem;
use std::num::NonZeroUsize;
use std::ops::Deref;

use crate::varint::varint_encode32;

pub const DEFAULT_BLOCK_SIZE: usize = 8192;
pub const DEFAULT_RESTART_INTERVAL: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(8) };

/// a `BlockBuilder` generates blocks where keys and values are one after the other.
///
/// The key values are appended one after the other in the block, these entries are
/// followed by an index that indicates the offset of some of the keys. This footer
/// index stores an offset every `index_key_interval`.
pub struct BlockBuilder {
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
    index_offsets: Vec<u32>,
    /// This counter increments every time we put a key in the key index and
    /// resets every `index_key_interval`, every time it resets we store
    /// the offset of the inserted key in the footer `index_offsets` vector.
    index_key_counter: usize,
}

impl BlockBuilder {
    pub fn new(index_key_interval: NonZeroUsize) -> BlockBuilder {
        BlockBuilder {
            buffer: Vec::new(),
            last_key: None,
            index_key_interval,
            index_offsets: vec![0],
            index_key_counter: 0,
        }
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.last_key = None;
        self.index_offsets.truncate(1); // equivalent to vec![0]
        self.index_key_counter = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn current_size_estimate(&self) -> usize {
        // The size of the buffer + the total size of the indexes in the footer + the count of indexes.
        self.buffer.len() + self.index_offsets.len() * mem::size_of::<u32>() + mem::size_of::<u32>()
    }

    /// Insert a key that must be greater than the previously added one.
    pub fn insert(&mut self, key: &[u8], val: &[u8]) {
        debug_assert!(self.index_key_counter <= self.index_key_interval.get());
        assert!(key.len() <= u32::max_value() as usize);
        assert!(val.len() <= u32::max_value() as usize);

        if self.index_key_counter == self.index_key_interval.get() {
            self.index_offsets.push(self.buffer.len() as u32);
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
        self.buffer.extend(self.index_offsets.iter().map(|off| off.to_be_bytes()).flatten());
        self.buffer.extend_from_slice(&index_offsets_count.to_be_bytes());

        dbg!(index_offsets_count);

        BlockBuffer { block_builder: self }
    }
}

/// A type that represents the finished content of a `BlockBuilder`.
///
/// This type implements `Deref` which exposes useful methods.
pub struct BlockBuffer<'a> {
    block_builder: &'a mut BlockBuilder,
}

impl Deref for BlockBuffer<'_> {
    type Target = BlockBuilder;

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
        let mut bb = BlockBuilder::new(DEFAULT_RESTART_INTERVAL);

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
