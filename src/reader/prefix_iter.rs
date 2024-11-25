use std::io;

use crate::ReaderCursor;

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
    #[allow(clippy::should_implement_trait)] // We return interior references
    pub fn next(&mut self) -> crate::Result<Option<(&[u8], &[u8])>> {
        let entry = if self.move_on_first_prefix {
            self.move_on_first_prefix = false;
            self.cursor.move_on_key_greater_than_or_equal_to(&self.prefix)?
        } else {
            self.cursor.move_on_next()?
        };

        match entry {
            Some((key, val)) if key.starts_with(&self.prefix) => Ok(Some((key, val))),
            _otherwise => Ok(None),
        }
    }
}

/// An iterator that is able to yield all the entries with
/// a key that starts with a given prefix in reverse order.
#[derive(Clone)]
pub struct RevPrefixIter<R> {
    cursor: ReaderCursor<R>,
    move_on_last_prefix: bool,
    prefix: Vec<u8>,
}

impl<R: io::Read + io::Seek> RevPrefixIter<R> {
    /// Creates a new [`RevPrefixIter`] by consumming a [`ReaderCursor`] and a prefix.
    pub(crate) fn new(cursor: ReaderCursor<R>, prefix: Vec<u8>) -> RevPrefixIter<R> {
        RevPrefixIter { cursor, prefix, move_on_last_prefix: true }
    }

    /// Returns the next entry that starts with the given prefix.
    #[allow(clippy::should_implement_trait)] // We return interior references
    pub fn next(&mut self) -> crate::Result<Option<(&[u8], &[u8])>> {
        let entry = if self.move_on_last_prefix {
            self.move_on_last_prefix = false;
            move_on_last_prefix(&mut self.cursor, self.prefix.clone())?
        } else {
            self.cursor.move_on_prev()?
        };

        match entry {
            Some((key, val)) if key.starts_with(&self.prefix) => Ok(Some((key, val))),
            _otherwise => Ok(None),
        }
    }
}

/// Moves the cursor on the last key that starts with the given prefix or before.
fn move_on_last_prefix<R: io::Read + io::Seek>(
    cursor: &mut ReaderCursor<R>,
    prefix: Vec<u8>,
) -> crate::Result<Option<(&[u8], &[u8])>> {
    match advance_key(prefix) {
        Some(next_prefix) => match cursor.move_on_key_lower_than_or_equal_to(&next_prefix)? {
            Some((k, _)) if k == next_prefix => cursor.move_on_prev(),
            _otherwise => Ok(cursor.current()),
        },
        None => cursor.move_on_last(),
    }
}

/// Returns a vector representing the key that is just **after** the one provided.
fn advance_key(mut bytes: Vec<u8>) -> Option<Vec<u8>> {
    while let Some(x) = bytes.last_mut() {
        if let Some(y) = x.checked_add(1) {
            *x = y;
            return Some(bytes);
        } else {
            bytes.pop();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;
    use std::io::Cursor;

    use rand::Rng;

    use super::*;
    use crate::writer::Writer;
    use crate::Reader;

    #[test]
    #[cfg_attr(miri, ignore)]
    fn simple_prefix() {
        let mut writer = Writer::memory();
        for x in (10..24000u32).step_by(3) {
            let x = x.to_be_bytes();
            writer.insert(x, x).unwrap();
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

    #[test]
    fn prefix_iter_with_byte_255() {
        // Create an ordered list of keys...
        let mut writer = Writer::memory();
        writer.insert([0, 0, 0, 254, 119, 111, 114, 108, 100], b"world").unwrap();
        writer.insert([0, 0, 0, 255, 104, 101, 108, 108, 111], b"hello").unwrap();
        writer.insert([0, 0, 0, 255, 119, 111, 114, 108, 100], b"world").unwrap();
        writer.insert([0, 0, 1, 000, 119, 111, 114, 108, 100], b"world").unwrap();

        let bytes = writer.into_inner().unwrap();
        let reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();

        // Lets check that we can prefix_iter on that sequence with the key "255".
        let mut iter = reader.into_prefix_iter([0, 0, 0, 255].to_vec()).unwrap();
        assert_eq!(
            iter.next().unwrap(),
            Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], &b"hello"[..]))
        );
        assert_eq!(
            iter.next().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], &b"world"[..]))
        );
        assert_eq!(iter.next().unwrap(), None);
    }

    #[test]
    fn rev_prefix_iter_with_byte_255() {
        // Create an ordered list of keys...
        let mut writer = Writer::memory();
        writer.insert([0, 0, 0, 254, 119, 111, 114, 108, 100], b"world").unwrap();
        writer.insert([0, 0, 0, 255, 104, 101, 108, 108, 111], b"hello").unwrap();
        writer.insert([0, 0, 0, 255, 119, 111, 114, 108, 100], b"world").unwrap();
        writer.insert([0, 0, 1], b"hello").unwrap();
        writer.insert([0, 0, 1, 0, 119, 111, 114, 108, 100], b"world").unwrap();

        let bytes = writer.into_inner().unwrap();
        let reader = Reader::new(Cursor::new(bytes.as_slice())).unwrap();

        // Lets check that we can prefix_iter on that sequence with the key "255".
        let mut iter = reader.into_rev_prefix_iter([0, 0, 0, 255].to_vec()).unwrap();
        assert_eq!(
            iter.next().unwrap(),
            Some((&[0, 0, 0, 255, 119, 111, 114, 108, 100][..], &b"world"[..]))
        );
        assert_eq!(
            iter.next().unwrap(),
            Some((&[0, 0, 0, 255, 104, 101, 108, 108, 111][..], &b"hello"[..]))
        );
        assert_eq!(iter.next().unwrap(), None);
    }

    #[test]
    fn simple_advance_key() {
        assert_eq!(advance_key(vec![0, 255]), Some(vec![1]));
        assert_eq!(advance_key(vec![255]), None);
        assert_eq!(advance_key(vec![254]), Some(vec![255]));
        assert_eq!(advance_key(vec![0, 255, 255]), Some(vec![1]));
        assert_eq!(advance_key(vec![255, 255, 255]), None);
    }
}
