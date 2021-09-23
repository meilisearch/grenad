use std::{io, mem};

use crate::{Error, ReaderCursor};

/// An iterator that is able to yield all the entries with
/// a key that starts with a given prefix.
#[derive(Clone)]
pub struct PrefixIter<R> {
    cursor: ReaderCursor<R>,
    move_on_first_prefix: bool,
    prefix: Vec<u8>,
}

impl<R: io::Read + io::Seek> PrefixIter<R> {
    /// Creates a new [`PrefixIter`] by consumming a [`ReaderCursor`] and a prefix.
    pub(crate) fn new(cursor: ReaderCursor<R>, prefix: Vec<u8>) -> PrefixIter<R> {
        PrefixIter { cursor, prefix, move_on_first_prefix: true }
    }

    /// Returns the next entry that starts with the given prefix.
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>, Error> {
        let entry = if self.move_on_first_prefix {
            self.move_on_first_prefix = false;
            self.cursor.move_on_key_greater_than_or_equal_to(&self.prefix)?
        } else {
            self.cursor.move_on_next()?
        };

        match entry {
            Some((key, val)) if key.starts_with(&self.prefix) => {
                // This is a trick to make the compiler happy...
                // https://github.com/rust-lang/rust/issues/47680
                let key: &'static _ = unsafe { mem::transmute(key) };
                let val: &'static _ = unsafe { mem::transmute(val) };
                Ok(Some((key, val)))
            }
            _otherwise => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;
    use std::io::Cursor;

    use rand::Rng;

    use crate::writer::Writer;
    use crate::Reader;

    #[test]
    fn simple_prefix() {
        let mut writer = Writer::memory();
        for x in (10..24000u32).step_by(3) {
            let x = x.to_be_bytes();
            writer.insert(&x, &x).unwrap();
        }

        let bytes = writer.into_inner().unwrap();
        assert_ne!(bytes.len(), 0);

        let reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();

        let mut rng = rand::thread_rng();
        for _ in 0..2000 {
            let num: u32 = rng.gen_range(0..=24020);
            let num_bytes = num.to_be_bytes();
            let prefix = &num_bytes[..rng.gen_range(0..=num_bytes.len())];

            let mut prefix_iter = reader.clone().into_prefix_iter(prefix.to_vec()).unwrap();
            while let Some((k, v)) = prefix_iter.next().unwrap() {
                let k = k.try_into().map(u32::from_be_bytes).unwrap();
                let v = v.try_into().map(u32::from_be_bytes).unwrap();
                assert_eq!(k, v);
                assert!(k.to_be_bytes().starts_with(prefix));
            }
        }
    }
}
