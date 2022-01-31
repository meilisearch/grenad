use std::io::{Read, Seek, SeekFrom, Write};
use std::{io, mem};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::compression::CompressionType;
use crate::error::Error;

const METADATA_V1_SIZE: usize = 17;
const METADATA_V2_SIZE: usize = 18;
const MAGIC_V1: u32 = 0x76324D4C;
const MAGIC_V2: u32 = 0x6723D4C4;

/// The format version of this file.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone, Hash)]
#[repr(u32)]
pub enum FileVersion {
    /// The first format version.
    FormatV1 = 0,
    /// The second format version which brings a leveled index footer.
    FormatV2 = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Metadata {
    /// The version used to encode this grenad file.
    pub file_version: FileVersion,
    /// The offset at which the index footer starts in this file.
    pub index_block_offset: u64,
    /// The compression type used to compress the blocks in this file.
    pub compression_type: CompressionType,
    /// The number of entries present in this file.
    pub entries_count: u64,
    /// The number of levels/indirections the index footer have to reach the data blocks.
    pub index_levels: u8,
}

impl Metadata {
    pub(crate) fn read_from<R: Read + Seek>(mut reader: R) -> Result<Metadata, Error> {
        // We seek at the end of the file to be able to read the magic number.
        let magic_size = mem::size_of::<u32>() as i64;
        reader.seek(SeekFrom::End(-magic_size))?;

        let file_version = match reader.read_u32::<LittleEndian>()? {
            MAGIC_V1 => FileVersion::FormatV1,
            MAGIC_V2 => FileVersion::FormatV2,
            _ => return Err(Error::InvalidFormatVersion),
        };

        match file_version {
            FileVersion::FormatV1 => {
                // Then we seek just before the metadata block (metadata + magic).
                let footer_size = METADATA_V1_SIZE as i64 + magic_size;
                reader.seek(SeekFrom::End(-footer_size))?;

                let index_block_offset = reader.read_u64::<LittleEndian>()?;
                let compression_type = reader.read_u8()?;
                let compression_type = CompressionType::from_u8(compression_type)
                    .ok_or(Error::InvalidCompressionType)?;
                let entries_count = reader.read_u64::<LittleEndian>()?;

                Ok(Metadata {
                    file_version,
                    index_block_offset,
                    compression_type,
                    entries_count,
                    index_levels: 0,
                })
            }
            FileVersion::FormatV2 => {
                // Then we seek just before the metadata block (metadata + magic).
                let footer_size = METADATA_V2_SIZE as i64 + magic_size;
                reader.seek(SeekFrom::End(-footer_size))?;

                let index_block_offset = reader.read_u64::<LittleEndian>()?;
                let compression_type = reader.read_u8()?;
                let compression_type = CompressionType::from_u8(compression_type)
                    .ok_or(Error::InvalidCompressionType)?;
                let entries_count = reader.read_u64::<LittleEndian>()?;
                let index_levels = reader.read_u8()?;

                Ok(Metadata {
                    file_version,
                    index_block_offset,
                    compression_type,
                    entries_count,
                    index_levels,
                })
            }
        }
    }

    pub(crate) fn write_into<W: Write>(&self, mut writer: W) -> io::Result<usize> {
        match self.file_version {
            FileVersion::FormatV1 => {
                writer.write_u64::<LittleEndian>(self.index_block_offset)?;
                writer.write_u8(self.compression_type as u8)?;
                writer.write_u64::<LittleEndian>(self.entries_count)?;

                // Write the magic number at the end of the buffer.
                writer.write_u32::<LittleEndian>(MAGIC_V1)?;

                Ok(METADATA_V1_SIZE + mem::size_of::<u32>())
            }
            FileVersion::FormatV2 => {
                writer.write_u64::<LittleEndian>(self.index_block_offset)?;
                writer.write_u8(self.compression_type as u8)?;
                writer.write_u64::<LittleEndian>(self.entries_count)?;
                writer.write_u8(self.index_levels)?;

                // Write the magic number at the end of the buffer.
                writer.write_u32::<LittleEndian>(MAGIC_V2)?;

                Ok(METADATA_V2_SIZE + mem::size_of::<u32>())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::mem;

    use super::*;

    #[test]
    fn simple() {
        let metadata = Metadata {
            file_version: FileVersion::FormatV1,
            index_block_offset: 0,
            compression_type: CompressionType::None,
            entries_count: 0,
            index_levels: 0,
        };

        let mut cursor = Cursor::new(Vec::new());
        let _count = metadata.write_into(&mut cursor).unwrap();

        cursor.seek(SeekFrom::Start(0)).unwrap();
        let new_metadata = Metadata::read_from(&mut cursor).unwrap();

        assert_eq!(metadata, new_metadata);
    }

    #[test]
    fn check_metadata_v1_size() {
        let Metadata {
            file_version: _,
            index_block_offset,
            compression_type,
            entries_count,
            index_levels: _,
        } = Metadata {
            file_version: FileVersion::FormatV1,
            index_block_offset: 0,
            compression_type: CompressionType::None,
            entries_count: 0,
            index_levels: 0,
        };

        assert_eq!(
            METADATA_V1_SIZE,
            mem::size_of_val(&index_block_offset)
                + mem::size_of_val(&compression_type)
                + mem::size_of_val(&entries_count)
        );
    }

    #[test]
    fn check_metadata_v2_size() {
        let Metadata {
            file_version: _,
            index_block_offset,
            compression_type,
            entries_count,
            index_levels,
        } = Metadata {
            file_version: FileVersion::FormatV2,
            index_block_offset: 0,
            compression_type: CompressionType::None,
            entries_count: 0,
            index_levels: 0,
        };

        assert_eq!(
            METADATA_V2_SIZE,
            mem::size_of_val(&index_block_offset)
                + mem::size_of_val(&compression_type)
                + mem::size_of_val(&entries_count)
                + mem::size_of_val(&index_levels)
        );
    }
}
