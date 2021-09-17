use std::borrow::Cow;
use std::convert::TryInto;
use std::io::{self, ErrorKind};
use std::mem::size_of;

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
    index_offsets: Vec<u32>,
}

impl Block {
    pub fn new<R: io::Read>(
        reader: &mut R,
        compression_type: CompressionType,
    ) -> Result<Option<Block>, Error> {
        let mut block_reader = Block {
            compression_type,
            buffer: Vec::new(),
            payload_size: 0,
            index_offsets: Vec::new(),
        };

        if block_reader.read_from(reader)? {
            Ok(Some(block_reader))
        } else {
            Ok(None)
        }
    }

    /// Returns `true` if it was able to read a new Block.
    pub fn read_from<R: io::Read>(&mut self, reader: &mut R) -> Result<bool, Error> {
        let block_len = match reader.read_u64::<BigEndian>() {
            Ok(block_len) => block_len,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(Error::from(e)),
        };

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

        let index_bytes_size = index_size * size_of::<u32>();
        let index_bytes =
            &self.buffer[buffer_len - size_of::<u32>() - index_bytes_size..][..index_bytes_size];
        let index_chunk_iter = index_bytes
            .chunks_exact(size_of::<u32>())
            .filter_map(|s| TryInto::try_into(s).ok())
            .map(u32::from_be_bytes);
        self.index_offsets.clear();
        self.index_offsets.extend(index_chunk_iter);
        self.payload_size = buffer_len - index_bytes_size - size_of::<u32>();

        Ok(true)
    }

    /// Returns the payload bytes.
    pub fn payload(&self) -> &[u8] {
        &self.buffer[..self.payload_size]
    }

    /// Returns the index offsets.
    pub fn index_offsets(&self) -> &[u32] {
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
}

#[derive(Clone)]
pub struct BlockCursor<'b> {
    block: &'b Block,
    current_offset: Option<usize>,
}

impl<'b> BlockCursor<'b> {
    pub fn new(block: &'b Block) -> BlockCursor<'b> {
        BlockCursor { block, current_offset: None }
    }

    /// Returns the currently pointed key/value or `None` if the cursor hasn't been seeked yet.
    pub fn current(&self) -> Option<(&'b [u8], &'b [u8])> {
        self.current_offset.and_then(|off| self.block.entry_at(off).map(|(k, v, _)| (k, v)))
    }

    /// Moves the cursor on the first key/value and returns the pair.
    pub fn move_on_first(&mut self) -> Option<(&'b [u8], &'b [u8])> {
        self.current_offset = self.block.index_offsets().first().map(|off| *off as usize);
        self.current()
    }

    /// Moves the cursor on the last key/value and returns the pair.
    pub fn move_on_last(&mut self) -> Option<(&'b [u8], &'b [u8])> {
        match self.block.index_offsets().last().map(|off| *off as usize) {
            Some(mut off) => {
                while let Some((_, _, next)) = self.block.entry_at(off) {
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
    pub fn move_on_next(&mut self) -> Option<(&'b [u8], &'b [u8])> {
        match self.current_offset.and_then(|off| self.block.entry_at(off)) {
            Some((_, _, next)) => {
                self.current_offset = Some(next);
                self.current()
            }
            None => None,
        }
    }

    /// Moves the cursor on the key preceding the currently pointed key.
    pub fn move_on_prev(&mut self) -> Option<(&'b [u8], &'b [u8])> {
        match self.current_offset {
            Some(current_offset) => {
                let offsets = self.block.index_offsets();
                // We go to the previous offset corresponding to the current pointed key.
                let i = offsets
                    .binary_search(&(current_offset as u32))
                    .unwrap_or_else(|x| x) // extract Err and Ok
                    .checked_sub(1)?;

                // We retrieve the currently pointed key to compare it
                // with the key we are searching for.
                let current_key = self.block.entry_at(current_offset).map(|(k, _, _)| k)?;

                // We use the offset found in the index that is just before
                // the one this key is in to iterate until we find the current key,
                // we stop searching and return the key just before the current one.
                let mut off = offsets[i] as usize;
                while let Some((k, _, next)) = self.block.entry_at(off) {
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
            None => None,
        }
    }

    /// Moves the cursor on the key lower than or equal to the given key in this block.
    pub fn move_on_key_lower_than_or_equal_to(
        &mut self,
        key: &[u8],
    ) -> Option<(&'b [u8], &'b [u8])> {
        let offsets = self.block.index_offsets();
        let result = offsets.binary_search_by_key(&key, |off| {
            let (key, _, _) = self.block.entry_at(*off as usize).unwrap();
            &key
        });

        match result {
            Ok(i) => self.current_offset = Some(offsets[i] as usize),
            Err(i) => match i.checked_sub(1).and_then(|i| offsets.get(i)) {
                Some(off) => {
                    self.current_offset = None;
                    let mut off = *off as usize;
                    while let Some((k, _, next)) = self.block.entry_at(off) {
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
    pub fn move_on_key_greater_than_or_equal_to(
        &mut self,
        key: &[u8],
    ) -> Option<(&'b [u8], &'b [u8])> {
        match self.move_on_key_lower_than_or_equal_to(key) {
            Some((k, v)) if k == key => Some((k, v)),
            Some(_) => self.move_on_next(),
            None => self.move_on_first(),
        }
    }
}

#[derive(Clone)]
pub struct BlockIter<'b> {
    cursor: BlockCursor<'b>,
    move_on_first: bool,
}

impl<'b> BlockIter<'b> {
    pub fn new(block: &'b Block) -> BlockIter<'b> {
        BlockIter { cursor: BlockCursor::new(block), move_on_first: true }
    }
}

impl<'b> Iterator for BlockIter<'b> {
    type Item = (&'b [u8], &'b [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.move_on_first {
            self.move_on_first = false;
            self.cursor.move_on_first()
        } else {
            self.cursor.move_on_next()
        }
    }
}

#[derive(Clone)]
pub struct BlockRevIter<'b> {
    cursor: BlockCursor<'b>,
    move_on_last: bool,
}

impl<'b> BlockRevIter<'b> {
    pub fn new(block: &'b Block) -> BlockRevIter<'b> {
        BlockRevIter { cursor: BlockCursor::new(block), move_on_last: true }
    }
}

impl<'b> Iterator for BlockRevIter<'b> {
    type Item = (&'b [u8], &'b [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.move_on_last {
            self.move_on_last = false;
            self.cursor.move_on_last()
        } else {
            self.cursor.move_on_prev()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_writer::BlockWriter;

    #[test]
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

        let block = Block::new(&mut &final_buffer[..], CompressionType::None).unwrap().unwrap();
        for ((k, v), n) in BlockIter::new(&block).zip(0..2000i32) {
            let k = k.try_into().map(i32::from_be_bytes).unwrap();
            let v = v.try_into().map(i32::from_be_bytes).unwrap();
            assert_eq!(k, n);
            assert_eq!(v, n);
        }
    }

    #[test]
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

        let block = Block::new(&mut &final_buffer[..], CompressionType::None).unwrap().unwrap();
        for ((k, v), n) in BlockRevIter::new(&block).zip((0..2000i32).rev()) {
            let k = k.try_into().map(i32::from_be_bytes).unwrap();
            let v = v.try_into().map(i32::from_be_bytes).unwrap();
            assert_eq!(k, n);
            assert_eq!(v, n);
        }
    }

    #[test]
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

        let block = Block::new(&mut &final_buffer[..], CompressionType::None).unwrap().unwrap();
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

        let block = Block::new(&mut &final_buffer[..], CompressionType::None).unwrap().unwrap();
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
                    assert_eq!(k, expected, "{}: {:?} != {:?}", n, k, expected);
                }
            }
        }
    }
}
