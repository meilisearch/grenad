use std::borrow::Cow;
use std::error::Error;
use std::str::FromStr;
use std::{fmt, io};

/// The different supported types of compression.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum CompressionType {
    /// Do not compress the blocks.
    None = 0,
    /// Use the [`snap`] crate to de/compress the blocks.
    Snappy = 1,
    /// Use the [`flate2`] crate to de/compress the blocks.
    Zlib = 2,
    /// Use the [`lz4_flex`] crate to de/compress the blocks.
    Lz4 = 3,
    /// Use the [`zstd`] crate to de/compress the blocks.
    Zstd = 4,
}

impl CompressionType {
    pub(crate) fn from_u8(value: u8) -> Option<CompressionType> {
        match value {
            0 => Some(CompressionType::None),
            1 => Some(CompressionType::Snappy),
            2 => Some(CompressionType::Zlib),
            3 => Some(CompressionType::Lz4),
            4 => Some(CompressionType::Zstd),
            _ => None,
        }
    }
}

impl FromStr for CompressionType {
    type Err = InvalidCompressionType;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name {
            "snappy" => Ok(CompressionType::Snappy),
            "zlib" => Ok(CompressionType::Zlib),
            "lz4" => Ok(CompressionType::Lz4),
            "zstd" => Ok(CompressionType::Zstd),
            _ => Err(InvalidCompressionType),
        }
    }
}

impl Default for CompressionType {
    fn default() -> CompressionType {
        CompressionType::None
    }
}

/// An invalid compression type have been read and the block can't be de/compressed.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InvalidCompressionType;

impl fmt::Display for InvalidCompressionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("Invalid compression type")
    }
}

impl Error for InvalidCompressionType {}

pub fn decompress(type_: CompressionType, data: &[u8]) -> io::Result<Cow<[u8]>> {
    match type_ {
        CompressionType::None => Ok(Cow::Borrowed(data)),
        CompressionType::Zlib => zlib_decompress(data),
        CompressionType::Snappy => snappy_decompress(data),
        CompressionType::Lz4 => lz4_decompress(data),
        CompressionType::Zstd => zstd_decompress(data),
    }
}

pub fn compress(type_: CompressionType, level: u32, data: &[u8]) -> io::Result<Cow<[u8]>> {
    match type_ {
        CompressionType::None => Ok(Cow::Borrowed(data)),
        CompressionType::Zlib => zlib_compress(data, level),
        CompressionType::Snappy => snappy_compress(data, level),
        CompressionType::Lz4 => lz4_compress(data, level),
        CompressionType::Zstd => zstd_compress(data, level),
    }
}

// --------- zlib ---------

#[cfg(feature = "zlib")]
fn zlib_decompress(data: &[u8]) -> io::Result<Cow<[u8]>> {
    use std::io::Read;
    let mut decoder = flate2::read::ZlibDecoder::new(data);
    let mut buffer = Vec::new();
    decoder.read_to_end(&mut buffer)?;
    Ok(Cow::Owned(buffer))
}

#[cfg(not(feature = "zlib"))]
fn zlib_decompress(_data: &[u8]) -> io::Result<Cow<[u8]>> {
    Err(io::Error::new(io::ErrorKind::Other, "unsupported zlib decompression"))
}

#[cfg(feature = "zlib")]
fn zlib_compress(data: &[u8], level: u32) -> io::Result<Cow<[u8]>> {
    use std::io::Write;
    let compression = flate2::Compression::new(level);
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), compression);
    encoder.write_all(data)?;
    encoder.finish().map(Cow::Owned)
}

#[cfg(not(feature = "zlib"))]
fn zlib_compress(_data: &[u8], _level: u32) -> io::Result<Cow<[u8]>> {
    Err(io::Error::new(io::ErrorKind::Other, "unsupported zlib compression"))
}

// --------- snappy ---------

#[cfg(feature = "snappy")]
fn snappy_decompress(data: &[u8]) -> io::Result<Cow<[u8]>> {
    let mut decoder = snap::raw::Decoder::new();
    decoder.decompress_vec(data).map_err(Into::into).map(Cow::Owned)
}

#[cfg(not(feature = "snappy"))]
fn snappy_decompress(_data: &[u8]) -> io::Result<Cow<[u8]>> {
    Err(io::Error::new(io::ErrorKind::Other, "unsupported snappy decompression"))
}

#[cfg(feature = "snappy")]
fn snappy_compress(data: &[u8], _level: u32) -> io::Result<Cow<[u8]>> {
    let mut decoder = snap::raw::Encoder::new();
    decoder.compress_vec(data).map_err(Into::into).map(Cow::Owned)
}

#[cfg(not(feature = "snappy"))]
fn snappy_compress(_data: &[u8], _level: u32) -> io::Result<Cow<[u8]>> {
    Err(io::Error::new(io::ErrorKind::Other, "unsupported snappy compression"))
}

// --------- zstd ---------

#[cfg(feature = "zstd")]
fn zstd_decompress(data: &[u8]) -> io::Result<Cow<[u8]>> {
    let mut buffer = Vec::new();
    zstd::stream::copy_decode(data, &mut buffer)?;
    Ok(Cow::Owned(buffer))
}

#[cfg(not(feature = "zstd"))]
fn zstd_decompress(_data: &[u8]) -> io::Result<Cow<[u8]>> {
    Err(io::Error::new(io::ErrorKind::Other, "unsupported zstd decompression"))
}

#[cfg(feature = "zstd")]
fn zstd_compress(data: &[u8], level: u32) -> io::Result<Cow<[u8]>> {
    let mut buffer = Vec::new();
    zstd::stream::copy_encode(data, &mut buffer, level as i32)?;
    Ok(Cow::Owned(buffer))
}

#[cfg(not(feature = "zstd"))]
fn zstd_compress(_data: &[u8], _level: u32) -> io::Result<Cow<[u8]>> {
    Err(io::Error::new(io::ErrorKind::Other, "unsupported zstd compression"))
}

// --------- lz4 ---------

#[cfg(feature = "lz4")]
fn lz4_decompress(data: &[u8]) -> io::Result<Cow<[u8]>> {
    let mut buffer = Vec::new();
    let mut rdr = lz4_flex::frame::FrameDecoder::new(data);
    io::copy(&mut rdr, &mut buffer)?;
    Ok(Cow::Owned(buffer))
}

#[cfg(not(feature = "lz4"))]
fn lz4_decompress(_data: &[u8]) -> io::Result<Cow<[u8]>> {
    Err(io::Error::new(io::ErrorKind::Other, "unsupported lz4 decompression"))
}

#[cfg(feature = "lz4")]
fn lz4_compress(mut data: &[u8], _level: u32) -> io::Result<Cow<[u8]>> {
    let mut wtr = lz4_flex::frame::FrameEncoder::new(Vec::new());
    io::copy(&mut data, &mut wtr)?;
    wtr.finish().map(Cow::Owned).map_err(Into::into)
}

#[cfg(not(feature = "lz4"))]
fn lz4_compress(_data: &[u8], _level: u32) -> io::Result<Cow<[u8]>> {
    Err(io::Error::new(io::ErrorKind::Other, "unsupported lz4 compression"))
}

#[cfg(test)]
mod tests {

    #[test]
    #[cfg_attr(miri, ignore)]
    #[cfg(all(feature = "zlib", feature = "snappy", feature = "zstd", feature = "lz4"))]
    fn check_all_compressions() {
        use CompressionType::*;

        use super::*;

        let data = "hello world this is my string!!!";
        for ctype in [None, Zlib, Snappy, Zstd, Lz4] {
            let level = 0;
            let compressed = compress(ctype, level, data.as_bytes()).unwrap();
            let output = decompress(ctype, &compressed).unwrap();
            assert_eq!(output.as_ref(), data.as_bytes());
        }
    }
}
