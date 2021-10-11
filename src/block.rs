use std::borrow::{Borrow, Cow};
use std::convert::TryInto;
use std::io;
use std::mem::{self, size_of};

use byteorder::{BigEndian, ReadBytesExt};

use crate::compression::decompress;
use crate::varint::varint_decode32;
use crate::{CompressionType, Error};

/// Represent a `Block`, with the index offsets and the key value payload.
#[derive(Clone)]
pub struct Block {
    /// The compression type that is used to compress/decompress this block.
    compression_type: CompressionType,
    /// The whole buffer that contains the key values and the index footer.
    buffer: Vec<u8>,
    /// The actual number of bytes where key and values resides,
    /// without the footer index.
    payload_size: usize,
    /// The list of index offsets where some key resides,
    /// can be used to jump inside of the block.
    index_offsets: Vec<u64>,
}

impl Block {
    pub fn new<R: io::Read>(
        reader: &mut R,
        compression_type: CompressionType,
    ) -> Result<Block, Error> {
        let mut block_reader = Block {
            compression_type,
            buffer: Vec::new(),
            payload_size: 0,
            index_offsets: Vec::new(),
        };

        block_reader.read_from(reader)?;

        Ok(block_reader)
    }

    /// Reads a new block and stores it in the internal buffers.
    ///
    /// This block must not be used if an error occurs while reading from the reader.
    pub fn read_from<R: io::Read>(&mut self, reader: &mut R) -> Result<(), Error> {
        let block_len = reader.read_u64::<BigEndian>()?;

        // We reset the cursor's position and decompress
        // the block into the cursor's buffer.
        self.buffer.resize(block_len as usize, 0);
        reader.read_exact(&mut self.buffer)?;

        if let Cow::Owned(vec) = decompress(self.compression_type, &self.buffer)? {
            self.buffer = vec;
        }

        // We retrieve the size of the index footer, the footer and
        // then compute the size of the payload.
        let buffer_len = self.buffer.len();
        let index_size_bytes = &self.buffer[buffer_len - size_of::<u32>()..][..size_of::<u32>()];
        let index_size = index_size_bytes.try_into().map(u32::from_be_bytes).unwrap() as usize;

        let index_bytes_size = index_size * size_of::<u64>();
        let index_bytes =
            &self.buffer[buffer_len - size_of::<u32>() - index_bytes_size..][..index_bytes_size];
        let index_chunk_iter = index_bytes
            .chunks_exact(size_of::<u64>())
            .filter_map(|s| TryInto::try_into(s).ok())
            .map(u64::from_be_bytes);
        self.index_offsets.clear();
        self.index_offsets.extend(index_chunk_iter);
        self.payload_size = buffer_len - index_bytes_size - size_of::<u32>();

        Ok(())
    }

    /// Returns the payload bytes.
    pub fn payload(&self) -> &[u8] {
        &self.buffer[..self.payload_size]
    }

    /// Returns the index offsets.
    pub fn index_offsets(&self) -> &[u64] {
        &self.index_offsets
    }

    /// Returns the key and value with the offset for the next entry.
    pub fn entry_at(&self, start_offset: usize) -> Option<(&[u8], &[u8], usize)> {
        let payload = self.payload();

        if start_offset >= payload.len() {
            return None;
        }

        let mut offset = start_offset;

        // Read the key length.
        let mut key_len = 0;
        let len = varint_decode32(&payload[offset..], &mut key_len);
        offset += len;

        // Read the value length.
        let mut val_len = 0;
        let len = varint_decode32(&payload[offset..], &mut val_len);
        offset += len;

        // Read the key itself.
        let key = &payload[offset..offset + key_len as usize];
        offset += key_len as usize;

        // Read the value itself.
        let val = &payload[offset..offset + val_len as usize];
        offset += val_len as usize;

        Some((key, val, offset))
    }

    pub fn into_cursor(self) -> BlockCursor<Block> {
        BlockCursor::new(self)
    }
}

#[derive(Clone)]
pub struct BlockCursor<B> {
    block: B,
    current_offset: Option<usize>,
}

impl<B> BlockCursor<B> {
    fn new(block: B) -> BlockCursor<B> {
        BlockCursor { block, current_offset: None }
    }
}

impl<B: Borrow<Block>> BlockCursor<B> {
    /// Returns the currently pointed key/value or `None` if the cursor hasn't been seeked yet.
    pub fn current(&self) -> Option<(&[u8], &[u8])> {
        self.current_offset
            .and_then(|off| self.block.borrow().entry_at(off).map(|(k, v, _)| (k, v)))
    }

    /// Moves the cursor on the first key/value and returns the pair.
    pub fn move_on_first(&mut self) -> Option<(&[u8], &[u8])> {
        self.current_offset = self.block.borrow().index_offsets().first().map(|off| *off as usize);
        self.current()
    }

    /// Moves the cursor on the last key/value and returns the pair.
    pub fn move_on_last(&mut self) -> Option<(&[u8], &[u8])> {
        match self.block.borrow().index_offsets().last().map(|off| *off as usize) {
            Some(mut off) => {
                while let Some((_, _, next)) = self.block.borrow().entry_at(off) {
                    // We store the current offset as the valid last offset seen.
                    self.current_offset = Some(off);
                    off = next;
                }
            }
            None => self.current_offset = None,
        }
        self.current()
    }

    /// Moves the cursor on the key following the currently pointed key.
    ///
    /// Automatically moves on the first key if the cursor hasn't been initialized yet.
    pub fn move_on_next(&mut self) -> Option<(&[u8], &[u8])> {
        match self.current_offset.map(|off| self.block.borrow().entry_at(off)) {
            Some(Some((_, _, next))) => {
                self.current_offset = Some(next);
                self.current()
            }
            Some(None) => None,
            None => self.move_on_first(),
        }
    }

    /// Moves the cursor on the key preceding the currently pointed key.
    ///
    /// Automatically moves on the last key if the cursor hasn't been initialized yet.
    pub fn move_on_prev(&mut self) -> Option<(&[u8], &[u8])> {
        match self.current_offset {
            Some(current_offset) => {
                let offsets = self.block.borrow().index_offsets();
                // We go to the previous offset corresponding to the current pointed key.
                let i = offsets
                    .binary_search(&(current_offset as u64))
                    .unwrap_or_else(|x| x) // extract Err and Ok
                    .checked_sub(1)?;

                // We retrieve the currently pointed key to compare it
                // with the key we are searching for.
                let current_key =
                    self.block.borrow().entry_at(current_offset).map(|(k, _, _)| k)?;

                // We use the offset found in the index that is just before
                // the one this key is in to iterate until we find the current key,
                // we stop searching and return the key just before the current one.
                let mut off = offsets[i] as usize;
                while let Some((k, _, next)) = self.block.borrow().entry_at(off) {
                    if current_key == k {
                        // We can stop as we found the key we were pointing at,
                        // we want the one just before.
                        break;
                    } else {
                        self.current_offset = Some(off);
                        off = next;
                    }
                }

                self.current()
            }
            None => self.move_on_last(),
        }
    }

    /// Moves the cursor on the key lower than or equal to the given key in this block.
    pub fn move_on_key_lower_than_or_equal_to(&mut self, key: &[u8]) -> Option<(&[u8], &[u8])> {
        let offsets = self.block.borrow().index_offsets();
        let result = offsets.binary_search_by_key(&key, |off| {
            let (key, _, _) = self.block.borrow().entry_at(*off as usize).unwrap();
            &key
        });

        match result {
            Ok(i) => self.current_offset = Some(offsets[i] as usize),
            Err(i) => match i.checked_sub(1).and_then(|i| offsets.get(i)) {
                Some(off) => {
                    self.current_offset = None;
                    let mut off = *off as usize;
                    while let Some((k, _, next)) = self.block.borrow().entry_at(off) {
                        if k > key {
                            break;
                        }
                        self.current_offset = Some(off);
                        off = next;
                    }
                }
                None => self.current_offset = None,
            },
        }

        self.current()
    }

    /// Moves the cursor on the key greater than or equal to the given key in this block.
    pub fn move_on_key_greater_than_or_equal_to(&mut self, key: &[u8]) -> Option<(&[u8], &[u8])> {
        match self.move_on_key_lower_than_or_equal_to(key) {
            Some((k, v)) if k == key => {
                // This is a trick to make the compiler happy...
                // https://github.com/rust-lang/rust/issues/47680
                let k: &'static _ = unsafe { mem::transmute(k) };
                let v: &'static _ = unsafe { mem::transmute(v) };
                Some((k, v))
            }
            Some(_) => self.move_on_next(),
            None => self.move_on_first(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_writer::BlockWriter;

    #[test]
    #[cfg_attr(miri, ignore)]
    fn simple() {
        let mut writer = BlockWriter::new();

        for x in 0..2000u32 {
            let x = x.to_be_bytes();
            writer.insert(&x, &x);
        }

        let buffer = writer.finish();
        let mut final_buffer = Vec::new();
        final_buffer.extend_from_slice(&(buffer.as_ref().len() as u64).to_be_bytes());
        final_buffer.extend_from_slice(buffer.as_ref());

        let block = Block::new(&mut &final_buffer[..], CompressionType::None).unwrap();
        let mut cursor = BlockCursor::new(block);
        let mut x: u32 = 0;

        while let Some((k, v)) = cursor.move_on_next() {
            assert_eq!(k, x.to_be_bytes());
            assert_eq!(v, x.to_be_bytes());
            x += 1;
        }

        assert_eq!(x, 2000);
    }

    #[test]
    fn small_iter() {
        let mut bb = BlockWriter::new();

        for x in 0..500i32 {
            let x = x.to_be_bytes();
            bb.insert(&x, &x);
        }
        let buffer = bb.finish();
        let mut final_buffer = Vec::new();
        final_buffer.extend_from_slice(&(buffer.as_ref().len() as u64).to_be_bytes());
        final_buffer.extend_from_slice(buffer.as_ref());

        let block = Block::new(&mut &final_buffer[..], CompressionType::None).unwrap();
        let mut cursor = block.into_cursor();
        for n in 0..500i32 {
            let (k, v) = cursor.move_on_next().unwrap();
            let k = k.try_into().map(i32::from_be_bytes).unwrap();
            let v = v.try_into().map(i32::from_be_bytes).unwrap();
            assert_eq!(k, n);
            assert_eq!(v, n);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn easy_iter() {
        let mut bb = BlockWriter::new();

        for x in 0..2000i32 {
            let x = x.to_be_bytes();
            bb.insert(&x, &x);
        }
        let buffer = bb.finish();
        let mut final_buffer = Vec::new();
        final_buffer.extend_from_slice(&(buffer.as_ref().len() as u64).to_be_bytes());
        final_buffer.extend_from_slice(buffer.as_ref());

        let block = Block::new(&mut &final_buffer[..], CompressionType::None).unwrap();
        let mut cursor = block.into_cursor();
        for n in 0..2000i32 {
            let (k, v) = cursor.move_on_next().unwrap();
            let k = k.try_into().map(i32::from_be_bytes).unwrap();
            let v = v.try_into().map(i32::from_be_bytes).unwrap();
            assert_eq!(k, n);
            assert_eq!(v, n);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn easy_rev_iter() {
        let mut bb = BlockWriter::new();

        for x in 0..2000i32 {
            let x = x.to_be_bytes();
            bb.insert(&x, &x);
        }
        let buffer = bb.finish();
        let mut final_buffer = Vec::new();
        final_buffer.extend_from_slice(&(buffer.as_ref().len() as u64).to_be_bytes());
        final_buffer.extend_from_slice(buffer.as_ref());

        let block = Block::new(&mut &final_buffer[..], CompressionType::None).unwrap();
        let mut cursor = block.into_cursor();
        for n in (0..2000i32).rev() {
            let (k, v) = cursor.move_on_prev().unwrap();
            let k = k.try_into().map(i32::from_be_bytes).unwrap();
            let v = v.try_into().map(i32::from_be_bytes).unwrap();
            assert_eq!(k, n);
            assert_eq!(v, n);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn easy_move_on_key_greater_than_or_equal() {
        let mut bb = BlockWriter::new();
        let mut nums = Vec::new();
        for x in (10..2000i32).step_by(3) {
            nums.push(x);
            let x = x.to_be_bytes();
            bb.insert(&x, &x);
        }
        let buffer = bb.finish();
        let mut final_buffer = Vec::new();
        final_buffer.extend_from_slice(&(buffer.as_ref().len() as u64).to_be_bytes());
        final_buffer.extend_from_slice(buffer.as_ref());

        let block = Block::new(&mut &final_buffer[..], CompressionType::None).unwrap();
        let mut cursor = BlockCursor::new(&block);
        for n in 0..2020i32 {
            match nums.binary_search(&n) {
                Ok(i) => {
                    let n = nums[i];
                    let (k, _) =
                        cursor.move_on_key_greater_than_or_equal_to(&n.to_be_bytes()).unwrap();
                    let k = k.try_into().map(i32::from_be_bytes).unwrap();
                    assert_eq!(k, n);
                }
                Err(i) => {
                    let k = cursor
                        .move_on_key_greater_than_or_equal_to(&n.to_be_bytes())
                        .map(|(k, _)| k.try_into().map(i32::from_be_bytes).unwrap());
                    assert_eq!(k, nums.get(i).copied());
                }
            }
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn easy_move_on_key_lower_than_or_equal() {
        let mut bb = BlockWriter::new();
        let mut nums = Vec::new();
        for x in (10..2000i32).step_by(3) {
            nums.push(x);
            let x = x.to_be_bytes();
            bb.insert(&x, &x);
        }
        let buffer = bb.finish();
        let mut final_buffer = Vec::new();
        final_buffer.extend_from_slice(&(buffer.as_ref().len() as u64).to_be_bytes());
        final_buffer.extend_from_slice(buffer.as_ref());

        let block = Block::new(&mut &final_buffer[..], CompressionType::None).unwrap();
        let mut cursor = BlockCursor::new(&block);
        for n in 0..2020i32 {
            match nums.binary_search(&n) {
                Ok(i) => {
                    let n = nums[i];
                    let (k, _) =
                        cursor.move_on_key_lower_than_or_equal_to(&n.to_be_bytes()).unwrap();
                    let k = k.try_into().map(i32::from_be_bytes).unwrap();
                    assert_eq!(k, n);
                }
                Err(i) => {
                    let k = cursor
                        .move_on_key_lower_than_or_equal_to(&n.to_be_bytes())
                        .map(|(k, _)| k.try_into().map(i32::from_be_bytes).unwrap());
                    let expected = i.checked_sub(1).and_then(|i| nums.get(i)).copied();
                    assert_eq!(k, expected, "queried value {}", n);
                }
            }
        }
    }
}
