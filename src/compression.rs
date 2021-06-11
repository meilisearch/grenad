use std::borrow::Cow;
use std::error::Error;
use std::str::FromStr;
use std::{fmt, io};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Snappy = 1,
    Zlib = 2,
    Lz4 = 3,
    Lz4hc = 4,
    Zstd = 5,
}

impl CompressionType {
    pub(crate) fn from_u8(value: u8) -> Option<CompressionType> {
        match value {
            0 => Some(CompressionType::None),
            1 => Some(CompressionType::Snappy),
            2 => Some(CompressionType::Zlib),
            3 => Some(CompressionType::Lz4),
            4 => Some(CompressionType::Lz4hc),
            5 => Some(CompressionType::Zstd),
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
            "lz4hc" => Ok(CompressionType::Lz4hc),
            "zstd" => Ok(CompressionType::Zstd),
            _ => Err(InvalidCompressionType),
        }
    }
}

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
        CompressionType::Zstd => zstd_decompress(data),
        other => {
            let error = format!("unsupported {:?} decompression", other);
            Err(io::Error::new(io::ErrorKind::Other, error))
        }
    }
}

pub fn compress(type_: CompressionType, level: u32, data: &[u8]) -> io::Result<Cow<[u8]>> {
    match type_ {
        CompressionType::None => Ok(Cow::Borrowed(data)),
        CompressionType::Zlib => zlib_compress(data, level),
        CompressionType::Snappy => snappy_compress(data, level),
        CompressionType::Zstd => zstd_compress(data, level),
        other => {
            let error = format!("unsupported {:?} decompression", other);
            Err(io::Error::new(io::ErrorKind::Other, error))
        }
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
