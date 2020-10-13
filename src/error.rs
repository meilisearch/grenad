use std::{fmt, io, error};

#[derive(Debug)]
pub enum Error<U=()> {
    Io(io::Error),
    Merge(U),
    InvalidCompressionType,
}

impl<U> Error<U> {
    pub(crate) fn convert_merge_error<V>(self) -> Error<V> {
        match self {
            Error::Io(io) => Error::Io(io),
            _ => panic!("cannot convert a merge error"),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(io) => write!(f, "{}", io),
            Error::Merge(_) => f.write_str("<user merge error>"),
            Error::InvalidCompressionType => f.write_str("invalid compression type"),
        }
    }
}

impl error::Error for Error { }

impl<U> From<io::Error> for Error<U> {
    fn from(err: io::Error) -> Error<U> {
        Error::Io(err)
    }
}
