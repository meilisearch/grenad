use std::convert::Infallible;
use std::{error, fmt, io};

#[derive(Debug)]
pub enum Error<U = Infallible> {
    Io(io::Error),
    Merge(U),
    InvalidCompressionType,
}

impl<U> Error<U> {
    pub(crate) fn convert_merge_error<V>(self) -> Error<V> {
        match self {
            Error::Io(io) => Error::Io(io),
            Error::InvalidCompressionType => Error::InvalidCompressionType,
            _ => panic!("cannot convert a merge error"),
        }
    }
}

impl<U: fmt::Display> fmt::Display for Error<U> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(io) => write!(f, "{}", io),
            Error::Merge(e) => write!(f, "merge error: {}", e),
            Error::InvalidCompressionType => f.write_str("invalid compression type"),
        }
    }
}

impl<U: fmt::Display + fmt::Debug> error::Error for Error<U> {}

impl<U> From<io::Error> for Error<U> {
    fn from(err: io::Error) -> Error<U> {
        Error::Io(err)
    }
}
