use std::io::{Read, Seek, SeekFrom, Write};
use std::{io, mem};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::compression::CompressionType;
use crate::error::Error;

const METADATA_SIZE: usize = 17;
const MAGIC_V1: u32 = 0x76324D4C;

/// The format version of this file.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Hash)]
#[repr(u32)]
pub enum FileVersion {
    /// The first format version.
    FormatV1 = 0,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Metadata {
    pub file_version: FileVersion,
    pub index_block_offset: u64,
    pub compression_type: CompressionType,
    pub entries_count: u64,
}

impl Metadata {
    pub(crate) fn read_from<R: Read + Seek>(mut reader: R) -> Result<Metadata, Error> {
        // We seek at the end of the file to be able to read the magic number.
        let magic_size = mem::size_of::<u32>() as i64;
        reader.seek(SeekFrom::End(-magic_size))?;

        let file_version = match reader.read_u32::<LittleEndian>()? {
            MAGIC_V1 => FileVersion::FormatV1,
            _ => return Err(Error::InvalidFormatVersion),
        };

        // Then we seek just before the metadata block (metadata + magic).
        let footer_size = METADATA_SIZE as i64 + magic_size;
        reader.seek(SeekFrom::End(-footer_size))?;

        let index_block_offset = reader.read_u64::<LittleEndian>()?;
        let compression_type = reader.read_u8()?;
        let compression_type =
            CompressionType::from_u8(compression_type).ok_or(Error::InvalidCompressionType)?;
        let entries_count = reader.read_u64::<LittleEndian>()?;

        Ok(Metadata { file_version, index_block_offset, compression_type, entries_count })
    }

    pub(crate) fn write_into<W: Write>(&self, mut writer: W) -> io::Result<usize> {
        writer.write_u64::<LittleEndian>(self.index_block_offset)?;
        writer.write_u8(self.compression_type as u8)?;
        writer.write_u64::<LittleEndian>(self.entries_count)?;

        // Write the magic number at the end of the buffer.
        writer.write_u32::<LittleEndian>(MAGIC_V1)?;

        Ok(METADATA_SIZE + mem::size_of::<u32>())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn simple() {
        let metadata = Metadata {
            file_version: FileVersion::FormatV1,
            index_block_offset: 0,
            compression_type: CompressionType::None,
            entries_count: 0,
        };

        let mut cursor = Cursor::new(Vec::new());
        let _count = metadata.write_into(&mut cursor).unwrap();

        cursor.seek(SeekFrom::Start(0)).unwrap();
        let new_metadata = Metadata::read_from(&mut cursor).unwrap();

        assert_eq!(metadata, new_metadata);
    }
}
