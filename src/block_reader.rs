use std::borrow::Cow;
use std::convert::TryInto;
use std::io::{self, ErrorKind};
use std::mem::size_of;

use byteorder::{BigEndian, ReadBytesExt};

use crate::compression::decompress;
use crate::varint::varint_decode32;
use crate::{CompressionType, Error};

#[derive(Clone)]
pub struct BlockReader {
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
    current_offset: Option<usize>,
    next_offset: usize,
}

impl BlockReader {
    pub fn new<R: io::Read>(
        reader: &mut R,
        compression_type: CompressionType,
    ) -> Result<Option<BlockReader>, Error> {
        let mut block_reader = BlockReader {
            compression_type,
            buffer: Vec::new(),
            payload_size: 0,
            index_offsets: Vec::new(),
            current_offset: None,
            next_offset: 0,
        };

        if block_reader.read_from(reader)? {
            Ok(Some(block_reader))
        } else {
            Ok(None)
        }
    }

    /// Returns `true` if it was able to read a new BlockReader.
    pub fn read_from<R: io::Read>(&mut self, reader: &mut R) -> Result<bool, Error> {
        let block_len = match reader.read_u64::<BigEndian>() {
            Ok(block_len) => block_len,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(Error::from(e)),
        };

        // We reset the cursor's position and decompress
        // the block into the cursor's buffer.
        self.current_offset = None;
        self.next_offset = 0;
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

    /// Returns the current key-value pair and the amount
    /// of bytes to advance to read the next one.
    pub fn current(&self) -> Option<(&[u8], &[u8], usize)> {
        let start_offset = self.current_offset?;
        let payload = &self.buffer[..self.payload_size];
        Self::entry(payload, start_offset)
    }

    fn entry(buffer: &[u8], start_offset: usize) -> Option<(&[u8], &[u8], usize)> {
        if start_offset >= buffer.len() {
            return None;
        }

        let mut offset = start_offset;

        // Read the key length.
        let mut key_len = 0;
        let len = varint_decode32(&buffer[offset..], &mut key_len);
        offset += len;

        // Read the value length.
        let mut val_len = 0;
        let len = varint_decode32(&buffer[offset..], &mut val_len);
        offset += len;

        // Read the key itself.
        let key = &buffer[offset..offset + key_len as usize];
        offset += key_len as usize;

        // Read the value itself.
        let val = &buffer[offset..offset + val_len as usize];
        offset += val_len as usize;

        Some((key, val, offset - start_offset))
    }

    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        let payload = &self.buffer[..self.payload_size];
        match Self::entry(payload, self.next_offset) {
            Some((key, value, offset)) => {
                self.current_offset = Some(self.next_offset);
                self.next_offset += offset;
                Some((key, value))
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_builder::{BlockBuilder, DEFAULT_RESTART_INTERVAL};

    #[test]
    fn easy_block() {
        let mut bb = BlockBuilder::new(DEFAULT_RESTART_INTERVAL);

        for x in 0..2000i32 {
            let x = x.to_be_bytes();
            bb.insert(&x, &x);
        }
        let buffer = bb.finish();
        let mut final_buffer = Vec::new();
        final_buffer.extend_from_slice(&(buffer.as_ref().len() as u64).to_be_bytes());
        final_buffer.extend_from_slice(buffer.as_ref());

        let mut br =
            BlockReader::new(&mut &final_buffer[..], CompressionType::None).unwrap().unwrap();
        let mut iter = 0..2000i32;
        while let Some((k, v)) = br.next() {
            let k = k.try_into().map(i32::from_be_bytes).unwrap();
            let v = v.try_into().map(i32::from_be_bytes).unwrap();
            let i = iter.next().unwrap();
            assert_eq!(k, i);
            assert_eq!(v, i);
        }
        assert!(iter.next().is_none());
    }
}
