use std::io::{self, Write};

/// A wrapper for [`Write`] that counts the total number of bytes written successfully.
pub struct CountWrite<W> {
    inner: W,
    count: u64,
}

impl<W> CountWrite<W> {
    pub fn new(inner: W) -> CountWrite<W> {
        CountWrite { inner, count: 0 }
    }

    /// Returns the number of bytes successfull written so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Gets a reference to the underlying writer.
    pub fn as_ref(&self) -> &W {
        &self.inner
    }
}

impl<W: Write> CountWrite<W> {
    /// Flush the inner writer, extracts the inner writer, discarding this wrapper.
    pub fn into_inner(mut self) -> io::Result<W> {
        self.inner.flush()?;
        Ok(self.inner)
    }
}

impl<W: Write> Write for CountWrite<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.count += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
