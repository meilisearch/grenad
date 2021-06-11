use crate::varint::varint_encode32;
use std::cmp;

pub const DEFAULT_BLOCK_SIZE: usize = 8192;
pub const MIN_BLOCK_SIZE: usize = 1024;

pub struct BlockBuilder {
    block_size: usize,
    buffer: Vec<u8>,
    last_key: Option<Vec<u8>>,
}

impl BlockBuilder {
    pub fn new(block_size: usize) -> BlockBuilder {
        let block_size = cmp::max(MIN_BLOCK_SIZE, block_size);
        BlockBuilder { block_size, buffer: Vec::with_capacity(block_size), last_key: None }
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Insert a key that must be greater than the previously added one.
    /// Returns `true` when the block size is reached and the block builder must be consumed.
    pub fn insert(&mut self, key: &[u8], val: &[u8]) -> bool {
        assert!(self.buffer.len() < self.block_size);
        assert!(key.len() <= u32::max_value() as usize);
        assert!(val.len() <= u32::max_value() as usize);

        // Check if the key is greater than the last added key
        // and save the current key to become the last key.
        match &mut self.last_key {
            Some(last_key) => {
                assert!(key > last_key);
                last_key.clear();
                last_key.extend_from_slice(key);
            }
            None => self.last_key = Some(key.to_vec()),
        }

        // reserve enough space for the key length, value length and key and value.
        self.buffer.reserve(5 + 5 + key.len() + val.len());

        // add "[key length][value length]" to buffer
        let mut buf = [0; 10];
        self.buffer.extend_from_slice(varint_encode32(&mut buf, key.len() as u32));
        self.buffer.extend_from_slice(varint_encode32(&mut buf, val.len() as u32));

        // add key to buffer followed by value
        self.buffer.extend_from_slice(key);
        self.buffer.extend_from_slice(val);

        self.buffer.len() >= self.block_size
    }

    pub fn finish(&mut self) -> BlockBuffer {
        self.last_key = None;
        BlockBuffer { block_builder: self }
    }
}

pub struct BlockBuffer<'a> {
    block_builder: &'a mut BlockBuilder,
}

impl AsRef<[u8]> for BlockBuffer<'_> {
    fn as_ref(&self) -> &[u8] {
        &self.block_builder.buffer
    }
}

impl Drop for BlockBuffer<'_> {
    fn drop(&mut self) {
        self.block_builder.buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn easy_block() {
        let mut bb = BlockBuilder::new(1024);

        for x in 0..2000i32 {
            let x = x.to_be_bytes();
            if bb.insert(&x, &x) {
                let buffer = bb.finish();
                assert!(buffer.as_ref().len() >= 1024);
            }
        }
    }
}
